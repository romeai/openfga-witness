package cachecontroller

import (
	"context"
	"errors"
	"fmt"
	"maps"
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

// refreshTimeout bounds one read of a store's changelog, datastore
// connection waits included, and so how long a request waits for it.
const refreshTimeout = time.Second

const changelogPageSize = 1000

// changelogSettleMargin is how far before the previous read's start each read
// goes back. A change's changelog position and timestamp are taken before its
// transaction commits, so it can become visible after a newer change was
// read. The margin must exceed the longest write transaction plus the clock
// skew between OpenFGA and the datastore.
const changelogSettleMargin = time.Minute

// changelogBudgetRate is the sustained write rate, in changes per second, up
// to which a store read at least once per TTL has its changes invalidated one
// by one. Each read reads every change back to changelogSettleMargin before
// the previous read's start, the ones it already processed included, so its
// change budget is this rate over the TTL plus the margin.
const changelogBudgetRate = 100

// errChangelogBudget reports more changes back to a read's horizon than its
// change budget, the already processed ones within the margin included.
var errChangelogBudget = errors.New("changelog change budget exhausted")

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
	ttl time.Duration
	// stateTTL is how long a store's state is kept after its last read. A
	// store without state is invalidated as a whole on its next read, so the
	// state only needs to outlive the cache entries it guards to spare them.
	stateTTL time.Duration
	// changeBudget is the most changes one read processes; a read that finds
	// more invalidates the whole store instead.
	changeBudget int

	refreshes singleflight.Group
	logger    logger.Logger

	mu        sync.RWMutex
	stores    map[string]*storeState
	nextSweep time.Time
}

// storeState is what the controller knows about one store's changelog.
type storeState struct {
	// checkedAt is the start of the changelog read that produced this state.
	checkedAt time.Time
	// invalidatedAt is when a new change was last observed: Check results
	// stamped before it may predate that change.
	invalidatedAt time.Time
	// processed counts the processed changes the next read reads again,
	// those within changelogSettleMargin before checkedAt.
	processed map[changeKey]int
}

// changeKey identifies a change by everything the controller reads of it.
// Changes sharing a key invalidate the same entries, so counting them is
// enough to tell the ones already processed from new ones.
type changeKey struct {
	timestamp int64
	operation openfgav1.TupleOperation
	object    string
	relation  string
	user      string
}

func keyOf(change *openfgav1.TupleChange) changeKey {
	tk := change.GetTupleKey()
	return changeKey{
		timestamp: change.GetTimestamp().AsTime().UnixNano(),
		operation: change.GetOperation(),
		object:    tk.GetObject(),
		relation:  tk.GetRelation(),
		user:      tk.GetUser(),
	}
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
		ds:           ds,
		markers:      markers,
		ttl:          ttl,
		stateTTL:     max(ttl, queryCacheTTL, iteratorCacheTTL),
		changeBudget: int((ttl + changelogSettleMargin) * changelogBudgetRate / time.Second),
		logger:       logger.NewNoopLogger(),
		stores:       map[string]*storeState{},
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

// refresh reads the changes to the store's changelog since the previous
// read and invalidates the cache entries each may have made stale. It always
// yields a state: when the changes cannot all be read (error, timeout, change
// budget), or no previous state says which changes the caches have seen, it
// invalidates all of the store's cache entries, which needs no read at all.
func (c *InMemoryCacheController) refresh(storeID string, caller trace.Link) *storeState {
	start := time.Now()
	ctx, span := tracer.Start(context.Background(), "cacheController.refresh", trace.WithLinks(caller))
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()

	prev := c.state(storeID)
	since := start
	if prev != nil {
		since = prev.checkedAt
	}
	changes, err := c.readChangesSince(ctx, storeID, since.Add(-changelogSettleMargin))
	observedAt := time.Now()

	st := &storeState{checkedAt: start, invalidatedAt: observedAt, processed: map[changeKey]int{}}
	invalidationType := "full"
	switch {
	case errors.Is(err, errChangelogBudget):
		c.logger.Warn("cache controller found more changes than its changelog change budget; invalidating every cache entry of the store",
			zap.String("store_id", storeID), zap.Int("change_budget", c.changeBudget))
		c.markers.InvalidateStore(storeID, observedAt, storage.StoreInvalidationBudget)
	case err != nil:
		reason := storage.StoreInvalidationError
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			reason = storage.StoreInvalidationTimeout
		}
		telemetry.TraceError(span, err)
		c.logger.Error("cache controller could not read the changelog; invalidating every cache entry of the store",
			zap.String("store_id", storeID), zap.String("reason", string(reason)), zap.Error(err))
		c.markers.InvalidateStore(storeID, observedAt, reason)
	case prev == nil:
		st.recordProcessed(changes)
		c.markers.InvalidateStore(storeID, observedAt, storage.StoreInvalidationFirstRead)
	default:
		st.recordProcessed(changes)
		invalidationType = "none"
		st.invalidatedAt = prev.invalidatedAt
		if fresh := prev.unprocessed(changes); len(fresh) > 0 {
			invalidationType = "partial"
			st.invalidatedAt = observedAt
			for _, change := range fresh {
				tk := change.GetTupleKey()
				c.markers.Invalidate(storeID, storage.InvalidIteratorByObjectRelationCacheKey(storeID, tk.GetObject(), tk.GetRelation()), observedAt)
				// Iterators by user cover every relation of the object type.
				c.markers.Invalidate(storeID, storage.InvalidIteratorByUserObjectTypeCacheKey(storeID, tk.GetUser(), tuple.GetType(tk.GetObject())), observedAt)
			}
		}
	}
	c.setState(storeID, st)

	if invalidationType != "none" {
		cacheInvalidationCounter.Inc()
	}
	c.logger.Debug("InMemoryCacheController refresh",
		zap.String("store_id", storeID),
		zap.Int("changes", len(changes)),
		zap.String("invalidationType", invalidationType))
	span.SetAttributes(attribute.String("invalidationType", invalidationType))
	findChangesAndInvalidateHistogram.WithLabelValues(invalidationType).Observe(float64(time.Since(start).Milliseconds()))
	return st
}

// recordProcessed records the changes the next read reads again.
func (st *storeState) recordProcessed(changes []*openfgav1.TupleChange) {
	horizon := st.checkedAt.Add(-changelogSettleMargin)
	for _, change := range changes {
		if !change.GetTimestamp().AsTime().Before(horizon) {
			st.processed[keyOf(change)]++
		}
	}
}

// unprocessed returns the changes this state has not processed yet.
func (st *storeState) unprocessed(changes []*openfgav1.TupleChange) []*openfgav1.TupleChange {
	remaining := maps.Clone(st.processed)
	var fresh []*openfgav1.TupleChange
	for _, change := range changes {
		key := keyOf(change)
		if remaining[key] > 0 {
			remaining[key]--
			continue
		}
		fresh = append(fresh, change)
	}
	return fresh
}

// readChangesSince returns the store's changes with timestamps at or after
// horizon, newest first. It fails with errChangelogBudget when there are more
// than changeBudget of them.
func (c *InMemoryCacheController) readChangesSince(ctx context.Context, storeID string, horizon time.Time) ([]*openfgav1.TupleChange, error) {
	var changes []*openfgav1.TupleChange
	from := ""
	for {
		opts := storage.ReadChangesOptions{
			SortDesc:   true,
			Pagination: storage.PaginationOptions{PageSize: changelogPageSize, From: from},
		}
		page, token, err := c.ds.ReadChanges(ctx, storeID, storage.ReadChangesFilter{}, opts)
		if errors.Is(err, storage.ErrNotFound) {
			return changes, nil
		}
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			return nil, fmt.Errorf("ReadChanges on store %s returned no changes and no ErrNotFound", storeID)
		}
		for _, change := range page {
			if change.GetTimestamp().AsTime().Before(horizon) {
				return changes, nil
			}
			if len(changes) == c.changeBudget {
				return nil, errChangelogBudget
			}
			changes = append(changes, change)
		}
		if token == "" || token == from {
			return nil, fmt.Errorf("ReadChanges on store %s returned continuation token %q after %q", storeID, token, from)
		}
		from = token
	}
}
