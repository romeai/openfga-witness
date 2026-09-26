package cachecontroller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/internal/telemetry"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

var (
	tracer = otel.Tracer("internal/cachecontroller")

	cacheTotalCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "cachecontroller_cache_total_count",
		Help:      "The total number of cache controller requests triggered by Check.",
	})

	cacheHitCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "cachecontroller_cache_hit_count",
		Help:      "The total number of cache controller requests triggered by Check within the cache controller TTL (i.e., no invalidation).",
	})

	cacheInvalidationCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "cachecontroller_cache_invalidation_count",
		Help:      "The total number of invalidation requests that invalidated iterator caches.",
	})

	findChangesAndInvalidateHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:                       build.ProjectName,
		Name:                            "cachecontroller_invalidation_duration_ms",
		Help:                            "The duration (in ms) required for cache controller to find changes and invalidate labeled by whether invalidation is required and buckets of changes size.",
		Buckets:                         []float64{5, 10, 25, 50, 100, 200, 500, 1000, 5000},
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"invalidation_type"})
)

// refreshTimeout bounds one read of a store's changelog, and so how long a
// request waits for it.
const refreshTimeout = time.Second

type CacheController interface {
	// DetermineInvalidationTime returns the time before which the store's
	// cached Check results are invalid. When the store's changelog was last
	// read more than the controller TTL ago, it first waits for a re-read,
	// shared with concurrent callers, so the returned time and the iterator
	// invalidation markers account for every write committed before the call
	// minus the TTL. It fails only when ctx ends while waiting.
	DetermineInvalidationTime(ctx context.Context, storeID string) (time.Time, error)
}

type NoopCacheController struct{}

// DetermineInvalidationTime returns the zero time: without a controller,
// cached entries are trusted for their TTL.
func (c *NoopCacheController) DetermineInvalidationTime(_ context.Context, _ string) (time.Time, error) {
	return time.Time{}, nil
}

func NewNoopCacheController() CacheController {
	return &NoopCacheController{}
}

// InMemoryCacheControllerOpt defines an option that can be used to change the behavior of InMemoryCacheController
// instance.
type InMemoryCacheControllerOpt func(*InMemoryCacheController)

// WithLogger sets the logger for InMemoryCacheController.
func WithLogger(logger logger.Logger) InMemoryCacheControllerOpt {
	return func(inm *InMemoryCacheController) {
		inm.logger = logger
	}
}

// InMemoryCacheController invalidates iterator cache (InMemoryCache) and
// sub-problem cache (CachedCheckResolver) entries that are older than the last
// write to their store, reading the store's changelog at most once per TTL.
// Its state lives outside the result cache, whose size pressure must not
// decide what is invalid.
type InMemoryCacheController struct {
	ds      storage.OpenFGADatastore
	markers *storage.InvalidationMarkers

	// ttl bounds the staleness of cached answers: a request never relies on a
	// changelog read that started more than ttl before it.
	ttl              time.Duration
	iteratorCacheTTL time.Duration
	// stateTTL is how long a store's state is kept after its last read. A
	// store without state is invalidated as a whole on its next read, so the
	// state only needs to outlive the cache entries it guards to spare them.
	stateTTL  time.Duration
	refreshes singleflight.Group
	logger    logger.Logger

	mu        sync.RWMutex
	stores    map[string]*storeState
	nextSweep time.Time
}

// storeState is what the controller knows about one store's changelog.
type storeState struct {
	// lastChange is the timestamp of the newest change seen in the changelog.
	lastChange time.Time
	// checkedAt is the start of the changelog read that produced this state.
	checkedAt time.Time
	// invalidatedAt is when a new change was last observed: Check results
	// stamped before it may predate that change.
	invalidatedAt time.Time
}

func NewCacheController(
	ds storage.OpenFGADatastore,
	markers *storage.InvalidationMarkers,
	ttl time.Duration,
	queryCacheTTL time.Duration,
	iteratorCacheTTL time.Duration,
	opts ...InMemoryCacheControllerOpt,
) CacheController {
	if markers == nil {
		panic("cache controller has no invalidation markers to write")
	}
	c := &InMemoryCacheController{
		ds:               ds,
		markers:          markers,
		ttl:              ttl,
		iteratorCacheTTL: iteratorCacheTTL,
		stateTTL:         max(ttl, queryCacheTTL, iteratorCacheTTL),
		logger:           logger.NewNoopLogger(),
		stores:           map[string]*storeState{},
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// state returns the store's state, or nil if it has none or it expired.
func (c *InMemoryCacheController) state(storeID string) *storeState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	st := c.stores[storeID]
	if st == nil || time.Since(st.checkedAt) > c.stateTTL {
		return nil
	}
	return st
}

func (c *InMemoryCacheController) setState(storeID string, st *storeState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stores[storeID] = st

	now := time.Now()
	if now.Before(c.nextSweep) {
		return
	}
	for id, s := range c.stores {
		if now.Sub(s.checkedAt) > c.stateTTL {
			delete(c.stores, id)
		}
	}
	c.nextSweep = now.Add(c.stateTTL)
}

// DetermineInvalidationTime see [CacheController].DetermineInvalidationTime.
func (c *InMemoryCacheController) DetermineInvalidationTime(ctx context.Context, storeID string) (time.Time, error) {
	ctx, span := tracer.Start(ctx, "cacheController.DetermineInvalidationTime")
	defer span.End()
	cacheTotalCounter.Inc()

	notBefore := time.Now().Add(-c.ttl)
	st := c.state(storeID)
	if st != nil && !st.checkedAt.Before(notBefore) {
		cacheHitCounter.Inc()
		span.SetAttributes(attribute.Bool("cached_within_ttl", true))
		return st.invalidatedAt, nil
	}

	// A refresh already in flight may have started before notBefore; the one
	// started after it completes cannot have.
	for st == nil || st.checkedAt.Before(notBefore) {
		link := trace.LinkFromContext(ctx)
		refreshed := c.refreshes.DoChan(storeID, func() (any, error) {
			return c.refresh(storeID, link), nil
		})
		select {
		case res := <-refreshed:
			st = res.Val.(*storeState)
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		}
	}
	return st.invalidatedAt, nil
}

// refresh reads the store's changelog and invalidates the cache entries the
// changes it finds may have made stale. It always yields a state: when the
// changelog cannot be read, or no earlier state says which changes the caches
// have seen, it invalidates all of the store's cache entries, which needs no
// read at all.
func (c *InMemoryCacheController) refresh(storeID string, caller trace.Link) *storeState {
	start := time.Now()
	ctx, span := tracer.Start(context.Background(), "cacheController.refresh", trace.WithLinks(caller))
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()

	prev := c.state(storeID)
	changes, err := c.readNewestChanges(ctx, storeID)
	observedAt := time.Now()

	st := &storeState{checkedAt: start, invalidatedAt: observedAt}
	invalidationType := "full"
	switch {
	case err != nil:
		telemetry.TraceError(span, err)
		c.logger.Error("cache controller could not read the changelog; invalidating every cache entry of the store",
			zap.String("store_id", storeID), zap.Error(err))
		if prev != nil {
			st.lastChange = prev.lastChange
		}
		c.markers.Invalidate(storage.InvalidIteratorCacheKey(storeID), observedAt)
	case prev == nil:
		if len(changes) > 0 {
			st.lastChange = changes[0].GetTimestamp().AsTime()
		}
		c.markers.Invalidate(storage.InvalidIteratorCacheKey(storeID), observedAt)
	case len(changes) == 0 || !changes[0].GetTimestamp().AsTime().After(prev.lastChange):
		invalidationType = "none"
		st.lastChange = prev.lastChange
		st.invalidatedAt = prev.invalidatedAt
	default:
		st.lastChange = changes[0].GetTimestamp().AsTime()
		invalidationType = c.invalidateChanged(storeID, changes, observedAt)
	}
	c.setState(storeID, st)

	if invalidationType != "none" {
		cacheInvalidationCounter.Inc()
	}
	c.logger.Debug("InMemoryCacheController refresh",
		zap.String("store_id", storeID),
		zap.Time("lastChangeTime", st.lastChange),
		zap.String("invalidationType", invalidationType))
	span.SetAttributes(attribute.String("invalidationType", invalidationType))
	findChangesAndInvalidateHistogram.WithLabelValues(invalidationType).Observe(float64(time.Since(start).Milliseconds()))
	return st
}

// readNewestChanges returns the newest page of the store's changelog, newest
// first, and no changes when the changelog is empty.
func (c *InMemoryCacheController) readNewestChanges(ctx context.Context, storeID string) ([]*openfgav1.TupleChange, error) {
	opts := storage.ReadChangesOptions{
		SortDesc: true,
		Pagination: storage.PaginationOptions{
			PageSize: storage.DefaultPageSize,
		},
	}
	changes, _, err := c.ds.ReadChanges(ctx, storeID, storage.ReadChangesFilter{}, opts)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(changes) == 0 {
		return nil, fmt.Errorf("ReadChanges on store %s returned no changes and no ErrNotFound", storeID)
	}
	return changes, nil
}

// invalidateChanged invalidates the iterators the changes may have made
// stale, newest first in changes, and returns the kind of invalidation.
func (c *InMemoryCacheController) invalidateChanged(storeID string, changes []*openfgav1.TupleChange, ts time.Time) string {
	lastIteratorInvalidation := time.Now().Add(-c.iteratorCacheTTL)

	// Only changes newer than the iterator TTL can be missing from a cached
	// iterator. idx ends at the oldest such change.
	idx := len(changes) - 1
	for ; idx >= 0; idx-- {
		if changes[idx].GetTimestamp().AsTime().After(lastIteratorInvalidation) {
			break
		}
	}

	if idx == len(changes)-1 {
		// Even the oldest change read is recent, so older unread ones may be too.
		c.markers.Invalidate(storage.InvalidIteratorCacheKey(storeID), ts)
		return "full"
	}

	invalidationType := "none"
	if idx >= 0 {
		invalidationType = "partial"
	}
	for ; idx >= 0; idx-- {
		t := changes[idx].GetTupleKey()
		c.markers.Invalidate(storage.InvalidIteratorByObjectRelationCacheKey(storeID, t.GetObject(), t.GetRelation()), ts)
		// We invalidate all iterators for the tuple's user and object type, regardless of the relation.
		c.markers.Invalidate(storage.InvalidIteratorByUserObjectTypeCacheKey(storeID, t.GetUser(), tuple.GetType(t.GetObject())), ts)
	}
	return invalidationType
}
