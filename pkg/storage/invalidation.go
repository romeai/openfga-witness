package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/openfga/openfga/pkg/storage/cache/keys"
)

// invalidationSweepInterval is how often expired markers are dropped.
const invalidationSweepInterval = time.Minute

// InvalidationMarkers holds the cache controller's invalidation markers for
// one result cache. A marker under a key (InvalidIteratorCacheKey or one of
// its per-entity variants) invalidates every cache entry guarded by that key
// whose LastModified is before the marker.
//
// The markers live outside the size-bounded result cache because evicting
// one would revive the entries it invalidates. A marker is kept for ttl after
// its time instead, and the entries it guards expire within ttl of their
// LastModified (see EntryTTL), so no entry outlives a marker that
// invalidates it.
type InvalidationMarkers struct {
	ttl time.Duration

	mu        sync.RWMutex
	markers   map[keys.Key]time.Time
	nextSweep time.Time
}

// NewInvalidationMarkers returns an empty set of markers kept for ttl, which
// must be at least the lifetime of any entry they guard.
func NewInvalidationMarkers(ttl time.Duration) *InvalidationMarkers {
	return &InvalidationMarkers{ttl: ttl, markers: map[keys.Key]time.Time{}}
}

// Invalidate invalidates the entries guarded by key whose LastModified is
// before at. A later marker for the same key is kept.
func (m *InvalidationMarkers) Invalidate(key keys.Key, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if prev, ok := m.markers[key]; !ok || at.After(prev) {
		m.markers[key] = at
	}

	if at.Before(m.nextSweep) {
		return
	}
	for k, t := range m.markers {
		if t.Add(m.ttl).Before(at) {
			delete(m.markers, k)
		}
	}
	m.nextSweep = at.Add(invalidationSweepInterval)
}

// Invalidated reports whether an entry with the given LastModified, guarded
// by guards, is invalid.
func (m *InvalidationMarkers) Invalidated(lastModified time.Time, guards ...keys.Key) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, key := range guards {
		if at, ok := m.markers[key]; ok && lastModified.Before(at) {
			return true
		}
	}
	return false
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
