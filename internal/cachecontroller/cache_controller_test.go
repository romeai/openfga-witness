package cachecontroller

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
	"github.com/openfga/openfga/pkg/tuple"
)

const storeID = "store"

// changelogDatastore serves a changelog the test controls, newest first and
// in pages, the way the SQL datastores serve ReadChanges to the controller.
type changelogDatastore struct {
	storage.OpenFGADatastore

	mu      sync.Mutex
	changes []*openfgav1.TupleChange // oldest first
	err     error
	reads   int
	// release, when set, holds every ReadChanges until it is closed.
	release chan struct{}
	// fixedToken, when set, is returned as every page's continuation token.
	fixedToken string
}

func (d *changelogDatastore) write(ts time.Time, object, user string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	change := &openfgav1.TupleChange{
		TupleKey:  tuple.NewTupleKey(object, "viewer", user),
		Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
		Timestamp: timestamppb.New(ts),
	}
	at, _ := slices.BinarySearchFunc(d.changes, ts, func(c *openfgav1.TupleChange, ts time.Time) int {
		return c.GetTimestamp().AsTime().Compare(ts)
	})
	d.changes = slices.Insert(d.changes, at, change)
}

func (d *changelogDatastore) readCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.reads
}

func (d *changelogDatastore) ReadChanges(ctx context.Context, _ string, _ storage.ReadChangesFilter, options storage.ReadChangesOptions) ([]*openfgav1.TupleChange, string, error) {
	if d.release != nil {
		select {
		case <-d.release:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.reads++
	if !options.SortDesc || options.Pagination.PageSize <= 0 {
		return nil, "", errors.New("unexpected ReadChanges arguments")
	}
	if d.err != nil {
		return nil, "", d.err
	}
	end := len(d.changes)
	if options.Pagination.From != "" {
		var err error
		if end, err = strconv.Atoi(options.Pagination.From); err != nil {
			return nil, "", err
		}
	}
	start := max(0, end-options.Pagination.PageSize)
	if start == end {
		return nil, "", storage.ErrNotFound
	}
	page := slices.Clone(d.changes[start:end])
	slices.Reverse(page)
	if d.fixedToken != "" {
		return page, d.fixedToken, nil
	}
	return page, strconv.Itoa(start), nil
}

type testController struct {
	*InMemoryCacheController
	ds      *changelogDatastore
	markers *storage.InvalidationMarkers
}

func newTestController(t *testing.T, ttl time.Duration) testController {
	return newTestControllerWithMarkers(t, ttl, time.Hour, 100_000)
}

func newTestControllerWithCacheTTL(t *testing.T, ttl, cacheTTL time.Duration) testController {
	return newTestControllerWithMarkers(t, ttl, cacheTTL, 100_000)
}

func newTestControllerWithMarkers(t *testing.T, ttl, cacheTTL time.Duration, markerLimit int) testController {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := &changelogDatastore{}
	markers := storage.NewInvalidationMarkers(cacheTTL, markerLimit)
	c := NewCacheController(ds, markers, ttl, cacheTTL, cacheTTL).(*InMemoryCacheController)
	return testController{InMemoryCacheController: c, ds: ds, markers: markers}
}

// invalidates reports whether a cache entry stamped at stamp and guarded by
// key is invalid.
func (c testController) invalidates(key keys.Key, stamp time.Time) bool {
	return c.markers.Invalidated(storeID, stamp, key)
}

// invalidatesStore reports whether the store-wide marker invalidates every
// cache entry stamped at stamp.
func (c testController) invalidatesStore(stamp time.Time) bool {
	return c.markers.Invalidated(storeID, stamp)
}

// storeInvalidations reads the store-wide invalidation counter for reason
// the way Prometheus scrapes it.
func storeInvalidations(t *testing.T, reason storage.StoreInvalidationReason) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "openfga_cache_store_invalidation_count" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["store_id"] == storeID && labels["reason"] == string(reason) {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func objectRelationKey(object string) keys.Key {
	return storage.InvalidIteratorByObjectRelationCacheKey(storeID, object, "viewer")
}

func userObjectTypeKey(user, objectType string) keys.Key {
	return storage.InvalidIteratorByUserObjectTypeCacheKey(storeID, user, objectType)
}

func (c testController) determine(t *testing.T) time.Time {
	t.Helper()
	ts, err := c.DetermineInvalidationTime(context.Background(), storeID)
	require.NoError(t, err)
	return ts
}

func TestNoopCacheController_DetermineInvalidationTime(t *testing.T) {
	ts, err := NewNoopCacheController().DetermineInvalidationTime(context.Background(), storeID)
	require.NoError(t, err)
	require.Zero(t, ts)
}

func TestInMemoryCacheController_DetermineInvalidationTime(t *testing.T) {
	t.Run("first_call_waits_and_invalidates_the_whole_store", func(t *testing.T) {
		c := newTestController(t, time.Hour)
		c.ds.write(time.Now().Add(-time.Minute), "document:1", "user:anne")
		firstReads := storeInvalidations(t, storage.StoreInvalidationFirstRead)

		before := time.Now()
		got := c.determine(t)

		require.Equal(t, 1, c.ds.readCount())
		require.False(t, got.Before(before), "entries cached before the first read must not be trusted")
		require.True(t, c.invalidatesStore(before))
		require.InDelta(t, firstReads+1, storeInvalidations(t, storage.StoreInvalidationFirstRead), 0)
	})

	t.Run("within_ttl_answers_without_reading", func(t *testing.T) {
		c := newTestController(t, time.Hour)
		first := c.determine(t)
		c.ds.write(time.Now(), "document:1", "user:anne")

		require.Equal(t, first, c.determine(t))
		require.Equal(t, 1, c.ds.readCount())
	})

	t.Run("write_after_quiet_period_is_seen_by_the_next_call", func(t *testing.T) {
		ttl := 20 * time.Millisecond
		c := newTestController(t, ttl)
		c.ds.write(time.Now().Add(-2*time.Hour), "document:0", "user:bob")
		c.determine(t)
		time.Sleep(2 * ttl)

		written := time.Now()
		c.ds.write(written, "document:1", "user:anne")
		got := c.determine(t)

		require.Equal(t, 2, c.ds.readCount())
		require.True(t, got.After(written), "a Check result cached before the write must be invalid")
		require.True(t, c.invalidates(objectRelationKey("document:1"), written))
		require.True(t, c.invalidates(userObjectTypeKey("user:anne", "document"), written))
		require.False(t, c.invalidatesStore(written), "one change must not invalidate the whole store")
	})

	t.Run("change_is_invalidated_from_when_it_was_observed", func(t *testing.T) {
		// A change's timestamp is taken when its transaction starts, so results
		// computed between that and its commit miss it despite being newer.
		ttl := 20 * time.Millisecond
		c := newTestController(t, ttl)
		c.ds.write(time.Now().Add(-2*time.Hour), "document:0", "user:bob")
		c.determine(t)
		time.Sleep(2 * ttl)

		computedAfterChangeTimestamp := time.Now()
		c.ds.write(computedAfterChangeTimestamp.Add(-time.Second), "document:1", "user:anne")
		got := c.determine(t)

		require.True(t, got.After(computedAfterChangeTimestamp))
	})

	t.Run("no_new_change_keeps_the_invalidation_time", func(t *testing.T) {
		ttl := 20 * time.Millisecond
		c := newTestController(t, ttl)
		c.ds.write(time.Now().Add(-2*time.Hour), "document:0", "user:bob")
		first := c.determine(t)
		time.Sleep(2 * ttl)

		require.Equal(t, first, c.determine(t))
		require.Equal(t, 2, c.ds.readCount())
	})

	t.Run("empty_changelog_is_not_an_error", func(t *testing.T) {
		ttl := 20 * time.Millisecond
		c := newTestController(t, ttl)
		first := c.determine(t)
		time.Sleep(2 * ttl)

		require.Equal(t, first, c.determine(t))
		require.False(t, c.invalidatesStore(first))
	})

	t.Run("concurrent_callers_share_one_read", func(t *testing.T) {
		c := newTestController(t, time.Hour)
		c.ds.release = make(chan struct{})

		const callers = 10
		type result struct {
			ts  time.Time
			err error
		}
		results := make(chan result, callers)
		var wg sync.WaitGroup
		for range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ts, err := c.DetermineInvalidationTime(context.Background(), storeID)
				results <- result{ts, err}
			}()
		}
		time.Sleep(50 * time.Millisecond)
		close(c.ds.release)
		wg.Wait()
		close(results)

		require.Equal(t, 1, c.ds.readCount())
		first := <-results
		require.NoError(t, first.err)
		for r := range results {
			require.NoError(t, r.err)
			require.Equal(t, first.ts, r.ts)
		}
	})

	t.Run("caller_stops_waiting_when_its_context_ends", func(t *testing.T) {
		c := newTestController(t, time.Hour)
		c.ds.release = make(chan struct{})

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := c.DetermineInvalidationTime(ctx, storeID)
		require.ErrorIs(t, err, context.Canceled)

		close(c.ds.release)
		require.Eventually(t, func() bool {
			return c.state(storeID) != nil
		}, time.Second, time.Millisecond)
	})

	t.Run("read_error_invalidates_the_whole_store", func(t *testing.T) {
		ttl := 20 * time.Millisecond
		c := newTestController(t, ttl)
		c.ds.write(time.Now().Add(-2*time.Hour), "document:0", "user:bob")
		c.determine(t)
		time.Sleep(2 * ttl)
		c.ds.err = errors.New("datastore unavailable")
		errorsBefore := storeInvalidations(t, storage.StoreInvalidationError)
		timeoutsBefore := storeInvalidations(t, storage.StoreInvalidationTimeout)

		before := time.Now()
		got := c.determine(t)

		require.False(t, got.Before(before))
		require.True(t, c.invalidatesStore(before))
		require.InDelta(t, errorsBefore+1, storeInvalidations(t, storage.StoreInvalidationError), 0)
		require.InDelta(t, timeoutsBefore, storeInvalidations(t, storage.StoreInvalidationTimeout), 0)
	})

	t.Run("read_timeout_invalidates_the_whole_store", func(t *testing.T) {
		ttl := 20 * time.Millisecond
		c := newTestController(t, ttl)
		c.determine(t)
		time.Sleep(2 * ttl)
		c.ds.release = make(chan struct{})
		t.Cleanup(func() { close(c.ds.release) })
		errorsBefore := storeInvalidations(t, storage.StoreInvalidationError)
		timeoutsBefore := storeInvalidations(t, storage.StoreInvalidationTimeout)

		before := time.Now()
		got := c.determine(t)

		require.Less(t, time.Since(before), refreshTimeout+500*time.Millisecond)
		require.False(t, got.Before(before))
		require.True(t, c.invalidatesStore(before))
		require.InDelta(t, timeoutsBefore+1, storeInvalidations(t, storage.StoreInvalidationTimeout), 0)
		require.InDelta(t, errorsBefore, storeInvalidations(t, storage.StoreInvalidationError), 0)
	})

	t.Run("expired_state_invalidates_the_whole_store", func(t *testing.T) {
		cacheTTL := 20 * time.Millisecond
		c := newTestControllerWithCacheTTL(t, cacheTTL, cacheTTL)
		c.ds.write(time.Now().Add(-2*time.Hour), "document:0", "user:bob")
		c.determine(t)
		time.Sleep(2 * cacheTTL)

		before := time.Now()
		got := c.determine(t)

		require.False(t, got.Before(before))
		require.True(t, c.invalidatesStore(before))
	})

	t.Run("expired_states_are_swept", func(t *testing.T) {
		cacheTTL := 20 * time.Millisecond
		c := newTestControllerWithCacheTTL(t, cacheTTL, cacheTTL)
		c.determine(t)
		time.Sleep(2 * cacheTTL)
		_, err := c.DetermineInvalidationTime(context.Background(), "other")
		require.NoError(t, err)

		c.mu.RLock()
		defer c.mu.RUnlock()
		require.NotContains(t, c.stores, storeID)
	})
}

func TestInMemoryCacheController_IncrementalChangelog(t *testing.T) {
	ttl := 20 * time.Millisecond

	t.Run("changes_beyond_one_page_are_invalidated_individually", func(t *testing.T) {
		c := newTestController(t, ttl)
		c.determine(t)
		time.Sleep(2 * ttl)

		const writes = 2*changelogPageSize + 50
		written := time.Now()
		for i := range writes {
			c.ds.write(written.Add(time.Duration(i)*time.Microsecond), "document:"+strconv.Itoa(i), "user:anne")
		}
		cachedBeforeObserved := time.Now()
		got := c.determine(t)

		require.True(t, got.After(cachedBeforeObserved))
		require.False(t, c.invalidatesStore(cachedBeforeObserved), "changes within the change budget must not invalidate the whole store")
		for i := range writes {
			require.True(t, c.invalidates(objectRelationKey("document:"+strconv.Itoa(i)), cachedBeforeObserved), "document:%d", i)
		}
		require.True(t, c.invalidates(userObjectTypeKey("user:anne", "document"), cachedBeforeObserved))
		require.False(t, c.invalidates(objectRelationKey("document:unwritten"), cachedBeforeObserved))
		require.False(t, c.invalidates(userObjectTypeKey("user:bob", "document"), cachedBeforeObserved))
	})

	t.Run("reads_stop_at_the_previous_read", func(t *testing.T) {
		c := newTestController(t, ttl)
		old := time.Now().Add(-2 * time.Hour)
		for i := range 500 {
			c.ds.write(old.Add(time.Duration(i)*time.Microsecond), "document:"+strconv.Itoa(i), "user:bob")
		}
		c.determine(t)
		require.Equal(t, 1, c.ds.readCount())
		time.Sleep(2 * ttl)

		c.ds.write(time.Now(), "document:new", "user:anne")
		c.determine(t)
		require.Equal(t, 2, c.ds.readCount())
	})

	t.Run("processed_changes_are_not_invalidated_again", func(t *testing.T) {
		c := newTestController(t, ttl)
		c.determine(t)
		time.Sleep(2 * ttl)
		c.ds.write(time.Now(), "document:1", "user:anne")
		first := c.determine(t)
		cachedAfterObserved := time.Now()
		time.Sleep(2 * ttl)

		require.Equal(t, first, c.determine(t))
		require.False(t, c.invalidates(objectRelationKey("document:1"), cachedAfterObserved))
	})

	t.Run("change_committed_after_a_newer_one_was_read_is_invalidated", func(t *testing.T) {
		c := newTestController(t, ttl)
		c.determine(t)
		time.Sleep(2 * ttl)
		c.ds.write(time.Now(), "document:1", "user:anne")
		c.determine(t)
		time.Sleep(2 * ttl)

		// Its transaction started before document:1's but committed after
		// the controller read the changelog.
		c.ds.write(time.Now().Add(-5*time.Second), "document:2", "user:anne")
		cachedBeforeObserved := time.Now()
		got := c.determine(t)

		require.True(t, got.After(cachedBeforeObserved))
		require.True(t, c.invalidates(objectRelationKey("document:2"), cachedBeforeObserved))
		require.False(t, c.invalidatesStore(cachedBeforeObserved))
	})

	t.Run("changes_re_read_within_the_margin_count_against_a_budget_sized_for_them", func(t *testing.T) {
		c := newTestController(t, ttl)
		c.determine(t)
		time.Sleep(2 * ttl)
		processed := c.changeBudget / 2
		written := time.Now()
		for i := range processed {
			c.ds.write(written.Add(time.Duration(i)*time.Microsecond), "document:old"+strconv.Itoa(i), "user:anne")
		}
		c.determine(t)
		time.Sleep(2 * ttl)

		written = time.Now()
		for i := range c.changeBudget - processed {
			c.ds.write(written.Add(time.Duration(i)*time.Microsecond), "document:new"+strconv.Itoa(i), "user:anne")
		}
		cachedBeforeObserved := time.Now()
		c.determine(t)

		require.False(t, c.invalidatesStore(cachedBeforeObserved), "a read of the TTL plus the margin at the budget rate must not invalidate the whole store")
		require.True(t, c.invalidates(objectRelationKey("document:new0"), cachedBeforeObserved))
		require.False(t, c.invalidates(objectRelationKey("document:old0"), cachedBeforeObserved), "processed changes must not be invalidated again")
	})

	t.Run("budget_covers_the_ttl_and_the_margin_at_the_budget_rate", func(t *testing.T) {
		c := newTestController(t, 10*time.Second)
		require.Equal(t, 7000, c.changeBudget)
	})

	t.Run("exhausted_change_budget_invalidates_the_whole_store_and_is_logged", func(t *testing.T) {
		c := newTestController(t, ttl)
		core, logs := observer.New(zap.WarnLevel)
		c.logger = &logger.ZapLogger{Logger: zap.New(core)}
		c.determine(t)
		time.Sleep(2 * ttl)
		budgetsBefore := storeInvalidations(t, storage.StoreInvalidationBudget)

		written := time.Now()
		for i := range c.changeBudget + 1 {
			c.ds.write(written.Add(time.Duration(i)*time.Microsecond), "document:"+strconv.Itoa(i), "user:anne")
		}
		cachedBeforeObserved := time.Now()
		got := c.determine(t)

		require.True(t, got.After(cachedBeforeObserved))
		require.True(t, c.invalidatesStore(cachedBeforeObserved))
		require.Equal(t, 1, logs.FilterMessageSnippet("change budget").Len())
		require.InDelta(t, budgetsBefore+1, storeInvalidations(t, storage.StoreInvalidationBudget), 0)
		pagesToExceed := (c.changeBudget + changelogPageSize) / changelogPageSize
		require.Equal(t, 1+pagesToExceed, c.ds.readCount(), "the read stops at the first change past the budget")
	})

	t.Run("changes_past_the_marker_limit_invalidate_the_whole_store", func(t *testing.T) {
		const markerLimit = 10
		c := newTestControllerWithMarkers(t, ttl, time.Hour, markerLimit)
		c.determine(t)
		time.Sleep(2 * ttl)
		capsBefore := storeInvalidations(t, storage.StoreInvalidationCap)

		written := time.Now()
		for i := range markerLimit {
			c.ds.write(written.Add(time.Duration(i)*time.Microsecond), "document:"+strconv.Itoa(i), "user:anne")
		}
		cachedBeforeObserved := time.Now()
		got := c.determine(t)

		require.True(t, c.invalidatesStore(cachedBeforeObserved), "%d changes write more than %d markers", markerLimit, markerLimit)
		require.False(t, c.invalidatesStore(got), "the store is invalidated from when the changes were observed")
		require.InDelta(t, capsBefore+1, storeInvalidations(t, storage.StoreInvalidationCap), 0)
	})

	t.Run("repeated_continuation_token_invalidates_the_whole_store", func(t *testing.T) {
		c := newTestController(t, ttl)
		c.determine(t)
		time.Sleep(2 * ttl)
		written := time.Now()
		for i := range changelogPageSize + 1 {
			c.ds.write(written.Add(time.Duration(i)*time.Microsecond), "document:"+strconv.Itoa(i), "user:anne")
		}
		c.ds.fixedToken = "stuck"
		cachedBeforeObserved := time.Now()
		c.determine(t)

		require.True(t, c.invalidatesStore(cachedBeforeObserved))
	})
}
