package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
)

// invalidationSweepInterval is how often expired markers are dropped.
const invalidationSweepInterval = time.Minute

var (
	invalidationMarkerCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: build.ProjectName,
		Name:      "cache_invalidation_marker_count",
		Help:      "The number of per-entity invalidation markers held for the store, summed over the server's result caches.",
	}, []string{"store_id"})

	storeInvalidationCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "cache_store_invalidation_count",
		Help:      "The number of times every cache entry of the store was invalidated at once, labeled by reason.",
	}, []string{"store_id", "reason"})
)

// StoreInvalidationReason says why every cache entry of a store was
// invalidated at once.
type StoreInvalidationReason string

const (
	// StoreInvalidationFirstRead: no earlier changelog read of the store to
	// continue from, at process start or after the controller's state expired.
	StoreInvalidationFirstRead StoreInvalidationReason = "first_read"
	// StoreInvalidationError: a changelog read failed.
	StoreInvalidationError StoreInvalidationReason = "error"
	// StoreInvalidationTimeout: a changelog read ran out of time.
	StoreInvalidationTimeout StoreInvalidationReason = "timeout"
	// StoreInvalidationBudget: a changelog read found more changes than its
	// budget.
	StoreInvalidationBudget StoreInvalidationReason = "budget"
	// StoreInvalidationCap: the store's per-entity markers exceeded their
	// limit.
	StoreInvalidationCap StoreInvalidationReason = "cap"
)

// InvalidationMarkers holds the cache controller's invalidation markers for
// one result cache. Each store has a store-wide marker, which invalidates
// every entry of the store whose LastModified is before it, and per-entity
// markers under keys (InvalidIteratorByObjectRelationCacheKey,
// InvalidIteratorByUserObjectTypeCacheKey), each of which invalidates the
// entries it guards whose LastModified is before it.
//
// The markers live outside the size-bounded result cache because evicting
// one would revive the entries it invalidates. A marker is kept for ttl after
// its time instead, and the entries it guards expire within ttl of their
// LastModified (see EntryTTL), so no entry outlives a marker that
// invalidates it. What bounds their memory is limit: a store never holds
// more per-entity markers than that, because the one that would exceed it
// replaces them all with a store-wide marker at the latest of their times,
// which invalidates at least every entry they did.
type InvalidationMarkers struct {
	ttl   time.Duration
	limit int

	mu        sync.RWMutex
	stores    map[string]*storeMarkers
	nextSweep time.Time
}

// storeMarkers are one store's markers. Every per-entity marker is after the
// store-wide one, since one at or before it would invalidate nothing more.
type storeMarkers struct {
	store    time.Time
	entities map[keys.Key]time.Time
	count    prometheus.Gauge
}

// NewInvalidationMarkers returns an empty set of markers kept for ttl, which
// must be at least the lifetime of any entry they guard, holding at most
// limit per-entity markers per store.
func NewInvalidationMarkers(ttl time.Duration, limit int) *InvalidationMarkers {
	if limit <= 0 {
		panic(fmt.Sprintf("invalidation markers need a positive per-store limit, got %d", limit))
	}
	return &InvalidationMarkers{ttl: ttl, limit: limit, stores: map[string]*storeMarkers{}}
}

// Invalidate invalidates the store's entries guarded by key whose
// LastModified is before at. A later marker for the same key is kept.
func (m *InvalidationMarkers) Invalidate(storeID string, key keys.Key, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	s := m.markersOf(storeID)
	if prev, ok := s.entities[key]; ok {
		s.entities[key] = maxTime(prev, at)
	} else if at.After(s.store) {
		s.entities[key] = at
		s.count.Inc()
		if len(s.entities) > m.limit {
			latest := at
			for _, t := range s.entities {
				latest = maxTime(latest, t)
			}
			m.invalidateStore(storeID, s, latest, StoreInvalidationCap)
		}
	}
	m.sweep(at)
}

// InvalidateStore invalidates every entry of the store whose LastModified is
// before at.
func (m *InvalidationMarkers) InvalidateStore(storeID string, at time.Time, reason StoreInvalidationReason) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.invalidateStore(storeID, m.markersOf(storeID), at, reason)
	m.sweep(at)
}

// Invalidated reports whether an entry of the store with the given
// LastModified, guarded by the per-entity markers under guards, is invalid.
func (m *InvalidationMarkers) Invalidated(storeID string, lastModified time.Time, guards ...keys.Key) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, ok := m.stores[storeID]
	if !ok {
		return false
	}
	if lastModified.Before(s.store) {
		return true
	}
	for _, key := range guards {
		if at, ok := s.entities[key]; ok && lastModified.Before(at) {
			return true
		}
	}
	return false
}

// markersOf returns the store's markers, creating them if it has none. The
// caller holds m.mu for writing.
func (m *InvalidationMarkers) markersOf(storeID string) *storeMarkers {
	s, ok := m.stores[storeID]
	if !ok {
		s = &storeMarkers{entities: map[keys.Key]time.Time{}, count: invalidationMarkerCount.WithLabelValues(storeID)}
		m.stores[storeID] = s
	}
	return s
}

// invalidateStore raises the store-wide marker to at and drops the
// per-entity markers it makes redundant. The caller holds m.mu for writing.
func (m *InvalidationMarkers) invalidateStore(storeID string, s *storeMarkers, at time.Time, reason StoreInvalidationReason) {
	storeInvalidationCount.WithLabelValues(storeID, string(reason)).Inc()
	if !at.After(s.store) {
		return
	}
	s.store = at
	dropped := 0
	for key, t := range s.entities {
		if !t.After(at) {
			delete(s.entities, key)
			dropped++
		}
	}
	s.count.Sub(float64(dropped))
}

// sweep drops the markers expired at now, at most once per
// invalidationSweepInterval. The caller holds m.mu for writing.
func (m *InvalidationMarkers) sweep(now time.Time) {
	if now.Before(m.nextSweep) {
		return
	}
	for storeID, s := range m.stores {
		expired := 0
		for key, t := range s.entities {
			if t.Add(m.ttl).Before(now) {
				delete(s.entities, key)
				expired++
			}
		}
		s.count.Sub(float64(expired))
		if len(s.entities) == 0 && s.store.Add(m.ttl).Before(now) {
			delete(m.stores, storeID)
		}
	}
	m.nextSweep = now.Add(invalidationSweepInterval)
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// MustGuard panics unless the markers outlive entries of the given lifetime,
// counted from their LastModified. Anything else is a wiring error that would
// let invalidated entries turn valid again.
func (m *InvalidationMarkers) MustGuard(lifetime time.Duration) {
	if m == nil {
		panic("cache entries have no invalidation markers")
	}
	if lifetime > m.ttl {
		panic(fmt.Sprintf("cache entries living %s outlive their invalidation markers, kept %s", lifetime, m.ttl))
	}
}

// EntryTTL returns the TTL for caching, now, an entry with the given
// LastModified so that it expires lifetime after LastModified rather than
// after it was cached. A non-positive result means the entry must not be
// cached.
func EntryTTL(lastModified time.Time, lifetime time.Duration) time.Duration {
	return lifetime - time.Since(lastModified)
}
