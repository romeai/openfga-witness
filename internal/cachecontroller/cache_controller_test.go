package cachecontroller

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
	storagetest "github.com/openfga/openfga/pkg/storage/test"
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

func (d *changelogDatastore) ReadChanges(ctx context.Context, store string, _ storage.ReadChangesFilter, options storage.ReadChangesOptions) ([]*openfgav1.TupleChange, string, error) {
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
	if store != storeID || !options.SortDesc || options.Pagination.PageSize <= 0 {
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
	return page, strconv.Itoa(start), nil
}

type testController struct {
	*InMemoryCacheController
	ds    *changelogDatastore
	cache *storagetest.MapCache
}

func newTestController(t *testing.T, ttl time.Duration) testController {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := &changelogDatastore{}
	cache := storagetest.NewMapCache()
	c := NewCacheController(ds, cache, ttl, time.Hour, time.Hour).(*InMemoryCacheController)
	return testController{InMemoryCacheController: c, ds: ds, cache: cache}
}

// invalidatedAt returns when the controller last invalidated key, or the zero time.
func (c testController) invalidatedAt(key keys.Key) time.Time {
	entry, _ := c.cache.Get(key).(*storage.InvalidEntityCacheEntry)
	if entry == nil {
		return time.Time{}
	}
	return entry.LastModified
}

func storeKey() keys.Key {
	return storage.InvalidIteratorCacheKey(storeID)
}

func objectRelationKey(object, relation string) keys.Key {
	return storage.InvalidIteratorByObjectRelationCacheKey(storeID, object, relation)
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

		before := time.Now()
		got := c.determine(t)

		require.Equal(t, 1, c.ds.readCount())
		require.False(t, got.Before(before), "entries cached before the first read must not be trusted")
		require.False(t, c.invalidatedAt(storeKey()).Before(before))
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
		require.True(t, c.invalidatedAt(objectRelationKey("document:1", "viewer")).After(written))
		require.True(t, c.invalidatedAt(userObjectTypeKey("user:anne", "document")).After(written))
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
		require.Equal(t, first, c.invalidatedAt(storeKey()))
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
			return c.cache.Get(storage.ChangelogCacheKey(storeID)) != nil
		}, time.Second, time.Millisecond)
	})

	t.Run("read_error_invalidates_the_whole_store", func(t *testing.T) {
		ttl := 20 * time.Millisecond
		c := newTestController(t, ttl)
		c.ds.write(time.Now().Add(-2*time.Hour), "document:0", "user:bob")
		c.determine(t)
		time.Sleep(2 * ttl)
		c.ds.err = errors.New("datastore unavailable")

		before := time.Now()
		got := c.determine(t)

		require.False(t, got.Before(before))
		require.False(t, c.invalidatedAt(storeKey()).Before(before))
	})

	t.Run("read_timeout_invalidates_the_whole_store", func(t *testing.T) {
		ttl := 20 * time.Millisecond
		c := newTestController(t, ttl)
		c.determine(t)
		time.Sleep(2 * ttl)
		c.ds.release = make(chan struct{})
		t.Cleanup(func() { close(c.ds.release) })

		before := time.Now()
		got := c.determine(t)

		require.Less(t, time.Since(before), refreshTimeout+500*time.Millisecond)
		require.False(t, got.Before(before))
		require.False(t, c.invalidatedAt(storeKey()).Before(before))
	})

	t.Run("evicted_state_invalidates_the_whole_store", func(t *testing.T) {
		c := newTestController(t, time.Hour)
		c.ds.write(time.Now().Add(-2*time.Hour), "document:0", "user:bob")
		c.determine(t)
		c.cache.Delete(storage.ChangelogCacheKey(storeID))

		before := time.Now()
		got := c.determine(t)

		require.False(t, got.Before(before))
		require.False(t, c.invalidatedAt(storeKey()).Before(before))
	})
}
