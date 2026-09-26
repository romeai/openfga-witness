package storage

import (
	"context"
	"sync"
	"time"
)

// CacheFreshness tracks the oldest LastModified among the cache entries a
// cached computation (a Check subproblem) consumed, directly or through
// nested computations. A cache entry reflects no write newer than its
// LastModified, so neither does anything derived from it: a derived result
// stamped later would outlive the invalidation of its own inputs.
type CacheFreshness struct {
	parent *CacheFreshness

	mu       sync.Mutex
	consumed bool
	oldest   time.Time
}

type cacheFreshnessContextKey struct{}

// ContextWithCacheFreshness returns a context carrying a new tracker nested
// in the context's current one, so entries consumed under it also age every
// enclosing computation.
func ContextWithCacheFreshness(ctx context.Context) (context.Context, *CacheFreshness) {
	parent, _ := ctx.Value(cacheFreshnessContextKey{}).(*CacheFreshness)
	f := &CacheFreshness{parent: parent}
	return context.WithValue(ctx, cacheFreshnessContextKey{}, f), f
}

// ObserveCacheEntry records that the computations tracked by ctx consumed
// data as of lastModified: a cache entry, or anything else that was not
// read after the computation started.
func ObserveCacheEntry(ctx context.Context, lastModified time.Time) {
	f, _ := ctx.Value(cacheFreshnessContextKey{}).(*CacheFreshness)
	for ; f != nil; f = f.parent {
		f.mu.Lock()
		if !f.consumed || lastModified.Before(f.oldest) {
			f.consumed = true
			f.oldest = lastModified
		}
		f.mu.Unlock()
	}
}

// Stamp returns the LastModified for a result computed since start: the
// oldest consumed cache entry if that is older, else start itself, never the
// completion time, since a write committed while the computation ran may be
// missing from its result.
func (f *CacheFreshness) Stamp(start time.Time) time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.consumed && f.oldest.Before(start) {
		return f.oldest
	}
	return start
}
