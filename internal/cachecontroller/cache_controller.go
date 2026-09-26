package cachecontroller

import (
	"context"
	"errors"
	"fmt"
	"math"
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
type InMemoryCacheController struct {
	ds    storage.OpenFGADatastore
	cache storage.InMemoryCache[any]

	// ttl bounds the staleness of cached answers: a request never relies on a
	// changelog read that started more than ttl before it.
	ttl              time.Duration
	queryCacheTTL    time.Duration
	iteratorCacheTTL time.Duration
	refreshes        singleflight.Group
	logger           logger.Logger
}

func NewCacheController(
	ds storage.OpenFGADatastore,
	cache storage.InMemoryCache[any],
	ttl time.Duration,
	queryCacheTTL time.Duration,
	iteratorCacheTTL time.Duration,
	opts ...InMemoryCacheControllerOpt,
) CacheController {
	c := &InMemoryCacheController{
		ds:               ds,
		cache:            cache,
		ttl:              ttl,
		queryCacheTTL:    queryCacheTTL,
		iteratorCacheTTL: iteratorCacheTTL,
		logger:           logger.NewNoopLogger(),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// DetermineInvalidationTime see [CacheController].DetermineInvalidationTime.
func (c *InMemoryCacheController) DetermineInvalidationTime(ctx context.Context, storeID string) (time.Time, error) {
	ctx, span := tracer.Start(ctx, "cacheController.DetermineInvalidationTime")
	defer span.End()
	cacheTotalCounter.Inc()

	notBefore := time.Now().Add(-c.ttl)
	entry, _ := c.cache.Get(storage.ChangelogCacheKey(storeID)).(*storage.ChangelogCacheEntry)
	if entry != nil && !entry.LastChecked.Before(notBefore) {
		cacheHitCounter.Inc()
		span.SetAttributes(attribute.Bool("cached_within_ttl", true))
		return entry.InvalidatedAt, nil
	}

	// A refresh already in flight may have started before notBefore; the one
	// started after it completes cannot have.
	for entry == nil || entry.LastChecked.Before(notBefore) {
		link := trace.LinkFromContext(ctx)
		refreshed := c.refreshes.DoChan(storeID, func() (any, error) {
			return c.refresh(storeID, link), nil
		})
		select {
		case res := <-refreshed:
			entry = res.Val.(*storage.ChangelogCacheEntry)
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		}
	}
	return entry.InvalidatedAt, nil
}

// refresh reads the store's changelog and invalidates the cache entries the
// changes it finds may have made stale. It always yields an entry: when the
// changelog cannot be read, or no earlier entry says which changes the caches
// have seen, it invalidates all of the store's cache entries, which needs no
// read at all.
func (c *InMemoryCacheController) refresh(storeID string, caller trace.Link) *storage.ChangelogCacheEntry {
	start := time.Now()
	ctx, span := tracer.Start(context.Background(), "cacheController.refresh", trace.WithLinks(caller))
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()

	changelogCacheKey := storage.ChangelogCacheKey(storeID)
	prev, _ := c.cache.Get(changelogCacheKey).(*storage.ChangelogCacheEntry)

	changes, err := c.readNewestChanges(ctx, storeID)
	observedAt := time.Now()

	entry := &storage.ChangelogCacheEntry{LastChecked: start, InvalidatedAt: observedAt}
	invalidationType := "full"
	switch {
	case err != nil:
		telemetry.TraceError(span, err)
		c.logger.Error("cache controller could not read the changelog; invalidating every cache entry of the store",
			zap.String("store_id", storeID), zap.Error(err))
		if prev != nil {
			entry.LastModified = prev.LastModified
		}
		c.invalidateIteratorCache(storeID, observedAt)
	case prev == nil:
		if len(changes) > 0 {
			entry.LastModified = changes[0].GetTimestamp().AsTime()
		}
		c.invalidateIteratorCache(storeID, observedAt)
	case len(changes) == 0 || !changes[0].GetTimestamp().AsTime().After(prev.LastModified):
		invalidationType = "none"
		entry.LastModified = prev.LastModified
		entry.InvalidatedAt = prev.InvalidatedAt
	default:
		entry.LastModified = changes[0].GetTimestamp().AsTime()
		invalidationType = c.invalidateChanged(storeID, changes, observedAt)
	}

	// The entry only matters while cached Check results that it could
	// invalidate live.
	c.cache.Set(changelogCacheKey, entry, c.queryCacheTTL)

	if invalidationType != "none" {
		cacheInvalidationCounter.Inc()
	}
	c.logger.Debug("InMemoryCacheController refresh",
		zap.String("store_id", storeID),
		zap.Time("lastChangeTime", entry.LastModified),
		zap.String("invalidationType", invalidationType))
	span.SetAttributes(attribute.String("invalidationType", invalidationType))
	findChangesAndInvalidateHistogram.WithLabelValues(invalidationType).Observe(float64(time.Since(start).Milliseconds()))
	return entry
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
		c.invalidateIteratorCache(storeID, ts)
		return "full"
	}

	invalidationType := "none"
	if idx >= 0 {
		invalidationType = "partial"
	}
	for ; idx >= 0; idx-- {
		t := changes[idx].GetTupleKey()
		c.invalidateIteratorCacheByObjectRelation(storeID, t.GetObject(), t.GetRelation(), ts)
		// We invalidate all iterators for the tuple's user and object type, regardless of the relation.
		c.invalidateIteratorCacheByUserAndObjectType(storeID, t.GetUser(), tuple.GetType(t.GetObject()), ts)
	}
	return invalidationType
}

// invalidateIteratorCache writes a new key to the cache with a very long TTL.
// An alternative implementation could delete invalid keys, but this approach is faster (see storagewrappers.findInCache).
func (c *InMemoryCacheController) invalidateIteratorCache(storeID string, ts time.Time) {
	c.cache.Set(storage.InvalidIteratorCacheKey(storeID), &storage.InvalidEntityCacheEntry{LastModified: ts}, math.MaxInt)
}

// invalidateIteratorCacheByObjectRelation writes a new key to the cache.
// An alternative implementation could delete invalid keys, but this approach is faster (see storagewrappers.findInCache).
func (c *InMemoryCacheController) invalidateIteratorCacheByObjectRelation(storeID, object, relation string, ts time.Time) {
	c.cache.Set(storage.InvalidIteratorByObjectRelationCacheKey(storeID, object, relation), &storage.InvalidEntityCacheEntry{LastModified: ts}, c.iteratorCacheTTL)
}

// invalidateIteratorCacheByUserAndObjectType writes a new key to the cache.
// An alternative implementation could delete invalid keys, but this approach is faster (see storagewrappers.findInCache).
func (c *InMemoryCacheController) invalidateIteratorCacheByUserAndObjectType(storeID, user, objectType string, ts time.Time) {
	c.cache.Set(storage.InvalidIteratorByUserObjectTypeCacheKey(storeID, user, objectType), &storage.InvalidEntityCacheEntry{LastModified: ts}, c.iteratorCacheTTL)
}
