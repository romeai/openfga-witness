package storage

import (
	"strconv"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage/cache/keys"
)

func TestInvalidationMarkers(t *testing.T) {
	now := time.Now()
	entityOf := func(store, object string) keys.Key {
		return InvalidIteratorByObjectRelationCacheKey(store, object, "viewer")
	}
	markerCount := func(store string) float64 {
		return testutil.ToFloat64(invalidationMarkerCount.WithLabelValues(store))
	}
	storeInvalidations := func(store string, reason StoreInvalidationReason) float64 {
		return testutil.ToFloat64(storeInvalidationCount.WithLabelValues(store, string(reason)))
	}

	t.Run("entity_marker_invalidates_the_entries_it_guards_stamped_before_it", func(t *testing.T) {
		store := ulid.Make().String()
		m := NewInvalidationMarkers(time.Hour, 10)
		m.Invalidate(store, entityOf(store, "document:1"), now)

		require.True(t, m.Invalidated(store, now.Add(-time.Nanosecond), entityOf(store, "document:1")))
		require.False(t, m.Invalidated(store, now, entityOf(store, "document:1")))
		require.False(t, m.Invalidated(store, now.Add(-time.Nanosecond), entityOf(store, "document:2")))
		require.False(t, m.Invalidated(store, now.Add(-time.Nanosecond)))
	})

	t.Run("store_marker_invalidates_every_entry_of_its_store_only", func(t *testing.T) {
		store, other := ulid.Make().String(), ulid.Make().String()
		m := NewInvalidationMarkers(time.Hour, 10)
		m.InvalidateStore(store, now, StoreInvalidationError)

		require.True(t, m.Invalidated(store, now.Add(-time.Second)))
		require.True(t, m.Invalidated(store, now.Add(-time.Second), entityOf(store, "document:1")))
		require.False(t, m.Invalidated(store, now))
		require.False(t, m.Invalidated(other, now.Add(-time.Second), entityOf(other, "document:1")))
		require.InDelta(t, 1, storeInvalidations(store, StoreInvalidationError), 0)
	})

	t.Run("earlier_marker_does_not_replace_a_later_one", func(t *testing.T) {
		store := ulid.Make().String()
		m := NewInvalidationMarkers(time.Hour, 10)
		m.Invalidate(store, entityOf(store, "document:1"), now)
		m.Invalidate(store, entityOf(store, "document:1"), now.Add(-time.Minute))
		m.InvalidateStore(store, now.Add(time.Minute), StoreInvalidationError)
		m.InvalidateStore(store, now, StoreInvalidationError)

		require.True(t, m.Invalidated(store, now.Add(time.Second)))
	})

	t.Run("store_marker_drops_the_entity_markers_it_dominates", func(t *testing.T) {
		store := ulid.Make().String()
		m := NewInvalidationMarkers(time.Hour, 10)
		m.Invalidate(store, entityOf(store, "document:1"), now.Add(-time.Second))
		m.Invalidate(store, entityOf(store, "document:2"), now)
		m.Invalidate(store, entityOf(store, "document:3"), now.Add(time.Second))
		m.InvalidateStore(store, now, StoreInvalidationFirstRead)

		require.Equal(t, map[keys.Key]time.Time{entityOf(store, "document:3"): now.Add(time.Second)}, m.stores[store].entities)
		require.InDelta(t, 1, markerCount(store), 0)
		require.True(t, m.Invalidated(store, now.Add(time.Nanosecond), entityOf(store, "document:3")))
	})

	t.Run("entity_marker_the_store_marker_dominates_is_not_kept", func(t *testing.T) {
		store := ulid.Make().String()
		m := NewInvalidationMarkers(time.Hour, 10)
		m.InvalidateStore(store, now, StoreInvalidationFirstRead)
		m.Invalidate(store, entityOf(store, "document:1"), now)
		m.Invalidate(store, entityOf(store, "document:2"), now.Add(-time.Second))

		require.Empty(t, m.stores[store].entities)
		require.Zero(t, markerCount(store))
	})

	t.Run("entity_markers_beyond_the_limit_collapse_into_a_store_marker", func(t *testing.T) {
		store, other := ulid.Make().String(), ulid.Make().String()
		const limit = 1000
		m := NewInvalidationMarkers(time.Hour, limit)
		m.Invalidate(other, entityOf(other, "document:0"), now)
		for i := range limit {
			m.Invalidate(store, entityOf(store, "document:"+strconv.Itoa(i)), now)
		}
		require.Len(t, m.stores[store].entities, limit)
		require.False(t, m.Invalidated(store, now.Add(-time.Second)), "the limit itself must not collapse the markers")
		require.InDelta(t, limit, markerCount(store), 0)

		observedAt := now.Add(time.Second)
		for i := range 2 * limit {
			m.Invalidate(store, entityOf(store, "document:new"+strconv.Itoa(i)), observedAt)
			require.LessOrEqual(t, len(m.stores[store].entities), limit)
		}

		require.True(t, m.Invalidated(store, observedAt.Add(-time.Nanosecond)), "the store marker must cover every collapsed marker")
		require.False(t, m.Invalidated(store, observedAt, entityOf(store, "document:new0")))
		require.Empty(t, m.stores[store].entities, "markers at the store marker's time are redundant")
		require.Zero(t, markerCount(store))
		require.InDelta(t, 1, storeInvalidations(store, StoreInvalidationCap), 0)
		require.Len(t, m.stores[other].entities, 1, "another store's markers must not collapse")
	})

	t.Run("collapse_covers_the_latest_entity_marker", func(t *testing.T) {
		store := ulid.Make().String()
		m := NewInvalidationMarkers(time.Hour, 2)
		m.Invalidate(store, entityOf(store, "document:1"), now.Add(time.Minute))
		m.Invalidate(store, entityOf(store, "document:2"), now)
		m.Invalidate(store, entityOf(store, "document:3"), now)

		require.True(t, m.Invalidated(store, now.Add(time.Minute-time.Nanosecond)))
		require.Empty(t, m.stores[store].entities)
	})

	t.Run("expired_markers_are_swept", func(t *testing.T) {
		store, stale := ulid.Make().String(), ulid.Make().String()
		m := NewInvalidationMarkers(time.Hour, 10)
		m.InvalidateStore(stale, now.Add(-2*time.Hour), StoreInvalidationFirstRead)
		m.Invalidate(store, entityOf(store, "document:1"), now.Add(-2*time.Hour))
		m.Invalidate(store, entityOf(store, "document:2"), now.Add(-30*time.Minute))
		m.Invalidate(store, entityOf(store, "document:3"), now)

		require.Len(t, m.stores[store].entities, 2)
		require.NotContains(t, m.stores, stale)
		require.InDelta(t, 2, markerCount(store), 0)
		require.True(t, m.Invalidated(store, now.Add(-time.Hour), entityOf(store, "document:2")))
	})

	t.Run("non_positive_limit_panics", func(t *testing.T) {
		require.Panics(t, func() { NewInvalidationMarkers(time.Hour, 0) })
	})

	t.Run("must_guard_entries_that_outlive_markers_panics", func(t *testing.T) {
		m := NewInvalidationMarkers(time.Hour, 10)
		require.NotPanics(t, func() { m.MustGuard(time.Hour) })
		require.Panics(t, func() { m.MustGuard(time.Hour + time.Nanosecond) })
		require.Panics(t, func() { (*InvalidationMarkers)(nil).MustGuard(time.Second) })
	})
}

func TestEntryTTL(t *testing.T) {
	stamp := time.Now().Add(-time.Minute)

	ttl := EntryTTL(stamp, time.Hour)
	require.LessOrEqual(t, ttl, 59*time.Minute)
	require.Greater(t, ttl, 58*time.Minute)

	require.LessOrEqual(t, EntryTTL(stamp, time.Minute), time.Duration(0))
}
