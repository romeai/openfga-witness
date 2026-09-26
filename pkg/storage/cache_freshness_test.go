package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCacheFreshness(t *testing.T) {
	start := time.Now()

	t.Run("stamps_start_without_consumed_entries", func(t *testing.T) {
		_, f := ContextWithCacheFreshness(context.Background())
		require.True(t, f.Stamp(start).Equal(start))
	})

	t.Run("stamps_start_when_consumed_entries_are_newer", func(t *testing.T) {
		ctx, f := ContextWithCacheFreshness(context.Background())
		ObserveCacheEntry(ctx, start.Add(time.Second))
		require.True(t, f.Stamp(start).Equal(start))
	})

	t.Run("stamps_the_oldest_consumed_entry", func(t *testing.T) {
		ctx, f := ContextWithCacheFreshness(context.Background())
		ObserveCacheEntry(ctx, start.Add(-time.Minute))
		ObserveCacheEntry(ctx, start.Add(-time.Hour))
		ObserveCacheEntry(ctx, start.Add(-time.Second))
		require.True(t, f.Stamp(start).Equal(start.Add(-time.Hour)))
	})

	t.Run("a_zero_stamp_is_the_oldest", func(t *testing.T) {
		ctx, f := ContextWithCacheFreshness(context.Background())
		ObserveCacheEntry(ctx, start.Add(-time.Hour))
		ObserveCacheEntry(ctx, time.Time{})
		require.True(t, f.Stamp(start).IsZero())
	})

	t.Run("nested_entries_age_enclosing_computations_only", func(t *testing.T) {
		outerCtx, outer := ContextWithCacheFreshness(context.Background())
		innerCtx, inner := ContextWithCacheFreshness(outerCtx)
		_, sibling := ContextWithCacheFreshness(outerCtx)

		ObserveCacheEntry(innerCtx, start.Add(-time.Hour))
		ObserveCacheEntry(outerCtx, start.Add(-time.Minute))

		require.True(t, inner.Stamp(start).Equal(start.Add(-time.Hour)))
		require.True(t, outer.Stamp(start).Equal(start.Add(-time.Hour)))
		require.True(t, sibling.Stamp(start).Equal(start))
	})

	t.Run("untracked_context_is_a_no_op", func(t *testing.T) {
		require.NotPanics(t, func() { ObserveCacheEntry(context.Background(), start) })
	})

	t.Run("concurrent_observations_keep_the_oldest", func(t *testing.T) {
		ctx, f := ContextWithCacheFreshness(context.Background())
		var wg sync.WaitGroup
		for i := range 100 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ObserveCacheEntry(ctx, start.Add(-time.Duration(i)*time.Second))
			}()
		}
		wg.Wait()
		require.True(t, f.Stamp(start).Equal(start.Add(-99*time.Second)))
	})
}
