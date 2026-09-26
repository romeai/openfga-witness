package storage

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInvalidationMarkers(t *testing.T) {
	store := InvalidIteratorCacheKey("store")
	entity := InvalidIteratorByObjectRelationCacheKey("store", "document:1", "viewer")
	other := InvalidIteratorByObjectRelationCacheKey("store", "document:2", "viewer")
	now := time.Now()

	t.Run("marker_invalidates_entries_stamped_before_it", func(t *testing.T) {
		m := NewInvalidationMarkers(time.Hour)
		m.Invalidate(entity, now)

		require.True(t, m.Invalidated(now.Add(-time.Nanosecond), entity))
		require.False(t, m.Invalidated(now, entity))
		require.False(t, m.Invalidated(now.Add(-time.Nanosecond), other))
	})

	t.Run("any_guard_invalidates", func(t *testing.T) {
		m := NewInvalidationMarkers(time.Hour)
		m.Invalidate(store, now)

		require.True(t, m.Invalidated(now.Add(-time.Second), other, store))
		require.False(t, m.Invalidated(now.Add(-time.Second), other))
	})

	t.Run("earlier_marker_does_not_replace_a_later_one", func(t *testing.T) {
		m := NewInvalidationMarkers(time.Hour)
		m.Invalidate(entity, now)
		m.Invalidate(entity, now.Add(-time.Minute))

		require.True(t, m.Invalidated(now.Add(-time.Second), entity))
	})

	t.Run("markers_are_not_bounded_in_number", func(t *testing.T) {
		m := NewInvalidationMarkers(time.Hour)
		const count = 100_000
		for i := range count {
			m.Invalidate(InvalidIteratorByObjectRelationCacheKey("store", "document:"+strconv.Itoa(i), "viewer"), now)
		}
		for i := range count {
			require.True(t, m.Invalidated(now.Add(-time.Second), InvalidIteratorByObjectRelationCacheKey("store", "document:"+strconv.Itoa(i), "viewer")))
		}
	})

	t.Run("expired_markers_are_swept", func(t *testing.T) {
		m := NewInvalidationMarkers(time.Hour)
		m.Invalidate(entity, now.Add(-2*time.Hour))
		m.Invalidate(other, now.Add(-30*time.Minute))
		m.Invalidate(store, now)

		require.Len(t, m.markers, 2)
		require.False(t, m.Invalidated(now.Add(-3*time.Hour), entity))
		require.True(t, m.Invalidated(now.Add(-time.Hour), other))
	})

	t.Run("must_guard_entries_that_outlive_markers_panics", func(t *testing.T) {
		m := NewInvalidationMarkers(time.Hour)
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
