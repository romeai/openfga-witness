package storagewrappers

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func testCacheKey(s string) keys.Key {
	var b keys.Builder
	b.EncodeString(s)
	return b.Key()
}

func TestFindInCache(t *testing.T) {
	ctx := context.Background()

	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	maxSize := 10
	ttl := 5 * time.Hour
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}
	ds := NewCachedDatastore(ctx, mockDatastore, mockCache, maxSize, ttl, sf, wg)

	storeID := ulid.Make().String()
	var kb keys.Builder
	kb.EncodeString("key")
	key := kb.Key()
	invalidEntityKeys := []keys.Key{storage.InvalidIteratorByObjectRelationCacheKey(storeID, "object", "relation")}

	t.Run("cache_miss", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).Return(nil),
		)
		_, ok := findInCache(ds.cache, key, storage.InvalidIteratorCacheKey(storeID), invalidEntityKeys)
		require.False(t, ok)
	})
	t.Run("cache_hit_no_invalid", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*storage.TupleRecord{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKeys[0]).Return(nil),
		)
		_, ok := findInCache(ds.cache, key, storage.InvalidIteratorCacheKey(storeID), invalidEntityKeys)
		require.True(t, ok)
	})
	t.Run("cache_hit_bad_result", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).Return("invalid"),
		)
		_, ok := findInCache(ds.cache, key, storage.InvalidIteratorCacheKey(storeID), invalidEntityKeys)
		require.False(t, ok)
	})
	t.Run("cache_hit_invalid", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*storage.TupleRecord{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).
				Return(&storage.InvalidEntityCacheEntry{LastModified: time.Now().Add(5 * time.Second)}),
			mockCache.EXPECT().Delete(key),
		)
		_, ok := findInCache(ds.cache, key, storage.InvalidIteratorCacheKey(storeID), invalidEntityKeys)
		require.False(t, ok)
	})
	t.Run("cache_hit_stale_invalid", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*storage.TupleRecord{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).
				Return(&storage.InvalidEntityCacheEntry{LastModified: time.Now().Add(-5 * time.Second)}),
			mockCache.EXPECT().Get(invalidEntityKeys[0]).Return(nil),
		)
		_, ok := findInCache(ds.cache, key, storage.InvalidIteratorCacheKey(storeID), invalidEntityKeys)
		require.True(t, ok)
	})
	t.Run("cache_hit_invalidation_incorrect_type", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*storage.TupleRecord{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).
				Return("invalid"),
			mockCache.EXPECT().Get(invalidEntityKeys[0]).Return(nil),
		)
		_, ok := findInCache(ds.cache, key, storage.InvalidIteratorCacheKey(storeID), invalidEntityKeys)
		require.True(t, ok)
	})
	t.Run("cache_hit_invalid_entity", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*storage.TupleRecord{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKeys[0]).
				Return(&storage.InvalidEntityCacheEntry{LastModified: time.Now().Add(5 * time.Second)}),
			mockCache.EXPECT().Delete(key),
		)
		_, ok := findInCache(ds.cache, key, storage.InvalidIteratorCacheKey(storeID), invalidEntityKeys)
		require.False(t, ok)
	})
	t.Run("cache_hit_invalid_entity_stale", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*storage.TupleRecord{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKeys[0]).
				Return(&storage.InvalidEntityCacheEntry{LastModified: time.Now().Add(-5 * time.Second)}),
		)
		_, ok := findInCache(ds.cache, key, storage.InvalidIteratorCacheKey(storeID), invalidEntityKeys)
		require.True(t, ok)
	})
	t.Run("cache_hit_invalid_entity_stale_invalid", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(key).
				Return(&storage.TupleIteratorCacheEntry{Tuples: []*storage.TupleRecord{}, LastModified: time.Now()}),
			mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKeys[0]).
				Return("invalid"),
		)
		_, ok := findInCache(ds.cache, key, storage.InvalidIteratorCacheKey(storeID), invalidEntityKeys)
		require.True(t, ok)
	})
}

func TestReadStartingWithUser(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	maxSize := 10
	ttl := 5 * time.Hour
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}
	ds := NewCachedDatastore(ctx, mockDatastore, mockCache, maxSize, ttl, sf, wg)

	storeID := ulid.Make().String()

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:1"),
		tuple.NewTupleKey("document:2", "viewer", "user:2"),
		tuple.NewTupleKey("document:3", "viewer", "user:3"),
		tuple.NewTupleKey("document:4", "viewer", "user:4"),
		tuple.NewTupleKey("document:5", "viewer", "user:*"),
	}
	var tuples []*openfgav1.Tuple
	var cachedTuples []*storage.TupleRecord
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
		_, objectID := tuple.SplitObject(tk.GetObject())
		_, userObjectID, userRelation := tuple.ToUserParts(tk.GetUser())
		cachedTuples = append(cachedTuples, &storage.TupleRecord{
			ObjectID:       objectID,
			Relation:       tk.GetRelation(),
			UserObjectType: "",
			UserObjectID:   userObjectID,
			UserRelation:   userRelation,
			InsertedAt:     ts.AsTime(),
		})
	}

	options := storage.ReadStartingWithUserOptions{}
	filter := storage.ReadStartingWithUserFilter{
		ObjectType: "document",
		Relation:   "viewer",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:5"},
			{Object: "user:*"},
		},
		ObjectIDs: storage.NewSortedSet("1"),
	}

	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	invalidEntityKeys := invalidIteratorByUserObjectTypeKeys(storeID, []string{"user:5", "user:*"}, filter.ObjectType)

	t.Run("cache_miss", func(t *testing.T) {
		cacheKey := storage.ReadStartingWithUserKey(storeID, filter) // first find to determine cache miss
		gomock.InOrder(
			mockCache.EXPECT().Get(cacheKey).Return(nil),
			mockDatastore.EXPECT().
				ReadStartingWithUser(gomock.Any(), storeID, filter, options).
				Return(storage.NewStaticTupleIterator(tuples), nil),
			mockCache.EXPECT().Get(cacheKey).Return(nil),                                 // find while stopping
			mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil), // check if store invalidated before writing
			mockCache.EXPECT().Get(invalidEntityKeys[0]).Return(nil),                     // check if entity invalidated before writing
			mockCache.EXPECT().Get(invalidEntityKeys[1]).Return(nil),                     // check if entity invalidated before writing
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), ttl).DoAndReturn(func(_ keys.Key, entry *storage.TupleIteratorCacheEntry, _ time.Duration) {
				if diff := cmp.Diff(cachedTuples, entry.Tuples, cmpOpts...); diff != "" {
					t.Fatalf("mismatch (-want +got):\n%s", diff)
				}
			}),
		)

		iter, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
		require.NoError(t, err)

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		iter.Stop() // has to be sync otherwise the assertion fails
		i, ok := iter.(*cachedIterator)
		require.True(t, ok)
		i.wg.Wait()

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("cache_hit", func(t *testing.T) {
		t.Run("without_user_filter_relation", func(t *testing.T) {
			gomock.InOrder(
				mockCache.EXPECT().Get(gomock.Any()).Return(&storage.TupleIteratorCacheEntry{Tuples: cachedTuples}),
				mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil),
				mockCache.EXPECT().Get(invalidEntityKeys[0]).Return(nil),
				mockCache.EXPECT().Get(invalidEntityKeys[1]).Return(nil),
			)

			iter, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
			require.NoError(t, err)

			var actual []*openfgav1.Tuple

			for {
				tuple, err := iter.Next(ctx)
				if err != nil {
					if errors.Is(err, storage.ErrIteratorDone) {
						break
					}
					require.Fail(t, "no error was expected")
					break
				}
				actual = append(actual, tuple)
			}

			if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})

		t.Run("with_user_filter_relation", func(t *testing.T) {
			filterWithUserRelation := storage.ReadStartingWithUserFilter{
				ObjectType: "document",
				Relation:   "viewer",
				UserFilter: []*openfgav1.ObjectRelation{
					{Object: "user:5", Relation: "viewer"}, // one with Relation
					{Object: "user:*"},
				},
				ObjectIDs: storage.NewSortedSet("1"),
			}
			invalidEntityKeysWithRelation := invalidIteratorByUserObjectTypeKeys(
				storeID, []string{"user:5#viewer", "user:*"}, filterWithUserRelation.ObjectType,
			)
			gomock.InOrder(
				mockCache.EXPECT().Get(gomock.Any()).Return(&storage.TupleIteratorCacheEntry{Tuples: cachedTuples}),
				mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil),

				// These should not be found, the cache_controller does not include relations
				// in the invalidation record keys when calling invalidateIteratorCacheByUserAndObjectType
				mockCache.EXPECT().Get(invalidEntityKeysWithRelation[0]).Return(nil),
				mockCache.EXPECT().Get(invalidEntityKeysWithRelation[1]).Return(nil),
			)

			iter, err := ds.ReadStartingWithUser(ctx, storeID, filterWithUserRelation, options)
			require.NoError(t, err)

			var actual []*openfgav1.Tuple

			for {
				tuple, err := iter.Next(ctx)
				if err != nil {
					if errors.Is(err, storage.ErrIteratorDone) {
						break
					}
					require.Fail(t, "no error was expected")
					break
				}
				actual = append(actual, tuple)
			}

			if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})
	})

	t.Run("cache_empty_response", func(t *testing.T) {
		cacheKey := storage.ReadStartingWithUserKey(storeID, filter) // first find to determine cache miss
		gomock.InOrder(
			mockCache.EXPECT().Get(cacheKey),
			mockDatastore.EXPECT().
				ReadStartingWithUser(gomock.Any(), storeID, filter, options).
				Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil),
			mockCache.EXPECT().Get(cacheKey).Return(nil),                                 // find while stopping
			mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil), // check if store invalidated before writing
			mockCache.EXPECT().Get(invalidEntityKeys[0]).Return(nil),                     // check if entity invalidated before writing
			mockCache.EXPECT().Get(invalidEntityKeys[1]).Return(nil),                     // check if entity invalidated before writing
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), ttl).DoAndReturn(func(_ keys.Key, entry *storage.TupleIteratorCacheEntry, _ time.Duration) {
				require.Empty(t, entry.Tuples)
			}),
		)

		iter, err := ds.ReadStartingWithUser(ctx, storeID, filter, options)
		require.NoError(t, err)

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		iter.Stop() // has to be sync otherwise the assertion fails
		i, ok := iter.(*cachedIterator)
		require.True(t, ok)
		i.wg.Wait()

		require.Empty(t, actual)
	})

	t.Run("higher_consistency", func(t *testing.T) {
		opts := storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
			},
		}

		gomock.InOrder(
			mockDatastore.EXPECT().
				ReadStartingWithUser(gomock.Any(), storeID, filter, opts).
				Return(storage.NewStaticTupleIterator(tuples), nil),
		)

		iter, err := ds.ReadStartingWithUser(ctx, storeID, filter, opts)
		require.NoError(t, err)
		defer iter.Stop()

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestReadUsersetTuples(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	maxSize := 10
	ttl := 5 * time.Hour
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}
	ds := NewCachedDatastore(ctx, mockDatastore, mockCache, maxSize, ttl, sf, wg)

	storeID := ulid.Make().String()

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:1"),
		tuple.NewTupleKey("document:1", "viewer", "user:2"),
		tuple.NewTupleKey("document:1", "viewer", "user:3"),
		tuple.NewTupleKey("document:1", "viewer", "user:4"),
		tuple.NewTupleKey("document:1", "viewer", "user:*"),
		tuple.NewTupleKey("document:1", "viewer", "company:1#viewer"),
	}
	var tuples []*openfgav1.Tuple
	var cachedTuples []*storage.TupleRecord
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
		userObjectType, userObjectID, userRelation := tuple.ToUserParts(tk.GetUser())
		cachedTuples = append(cachedTuples, &storage.TupleRecord{
			UserObjectType: userObjectType,
			UserObjectID:   userObjectID,
			UserRelation:   userRelation,
			InsertedAt:     ts.AsTime(),
		})
	}

	options := storage.ReadUsersetTuplesOptions{}
	filter := storage.ReadUsersetTuplesFilter{
		Object:   "document:1",
		Relation: "viewer",
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			typesystem.DirectRelationReference("company", "viewer"),
			typesystem.WildcardRelationReference("user"),
		},
	}

	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	invalidEntityKey := storage.InvalidIteratorByObjectRelationCacheKey(storeID, filter.Object, filter.Relation)
	cacheKey := storage.ReadUsersetTuplesKey(storeID, filter)

	t.Run("cache_miss", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(gomock.Any()),
			mockDatastore.EXPECT().
				ReadUsersetTuples(gomock.Any(), storeID, filter, options).
				Return(storage.NewStaticTupleIterator(tuples), nil),
			mockCache.EXPECT().Get(cacheKey).Return(nil),                                 // find while stopping
			mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil), // check if store invalidated before writing
			mockCache.EXPECT().Get(invalidEntityKey).Return(nil),                         // check if entity invalidated before writing
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), ttl).DoAndReturn(func(_ keys.Key, entry *storage.TupleIteratorCacheEntry, _ time.Duration) {
				if diff := cmp.Diff(cachedTuples, entry.Tuples, cmpOpts...); diff != "" {
					t.Fatalf("mismatch (-want +got):\n%s", diff)
				}
			}),
		)

		iter, err := ds.ReadUsersetTuples(ctx, storeID, filter, options)
		require.NoError(t, err)

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		iter.Stop() // has to be sync otherwise the assertion fails
		i, ok := iter.(*cachedIterator)
		require.True(t, ok)
		i.wg.Wait()

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("cache_hit", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(cacheKey).Return(&storage.TupleIteratorCacheEntry{Tuples: cachedTuples}),
			mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKey).Return(nil),
		)

		iter, err := ds.ReadUsersetTuples(ctx, storeID, filter, options)
		require.NoError(t, err)
		defer iter.Stop()

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("cache_empty_response", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(cacheKey).Return(nil),
			mockDatastore.EXPECT().
				ReadUsersetTuples(gomock.Any(), storeID, filter, options).
				Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil),
			mockCache.EXPECT().Get(cacheKey).Return(nil),
			mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKey).Return(nil),
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), ttl).DoAndReturn(func(_ keys.Key, entry *storage.TupleIteratorCacheEntry, _ time.Duration) {
				require.Empty(t, entry.Tuples)
			}),
		)

		iter, err := ds.ReadUsersetTuples(ctx, storeID, filter, options)
		require.NoError(t, err)

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		iter.Stop() // has to be sync otherwise the assertion fails
		i, ok := iter.(*cachedIterator)
		require.True(t, ok)
		i.wg.Wait()

		require.Empty(t, actual)
	})

	t.Run("higher_consistency", func(t *testing.T) {
		opts := storage.ReadUsersetTuplesOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
			},
		}

		gomock.InOrder(
			mockDatastore.EXPECT().
				ReadUsersetTuples(gomock.Any(), storeID, filter, opts).
				Return(storage.NewStaticTupleIterator(tuples), nil),
		)

		iter, err := ds.ReadUsersetTuples(ctx, storeID, filter, opts)
		require.NoError(t, err)
		defer iter.Stop()

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestRead(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	maxSize := 10
	ttl := 5 * time.Hour
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}
	ds := NewCachedDatastore(ctx, mockDatastore, mockCache, maxSize, ttl, sf, wg)

	storeID := ulid.Make().String()

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("license:1", "owner", "company:1"),
		tuple.NewTupleKey("license:1", "owner", "company:2"),
	}
	var tuples []*openfgav1.Tuple
	var cachedTuples []*storage.TupleRecord
	for _, tk := range tks {
		ts := timestamppb.New(time.Now())
		tuples = append(tuples, &openfgav1.Tuple{Key: tk, Timestamp: ts})
		userObjectType, userObjectID, userRelation := tuple.ToUserParts(tk.GetUser())
		cachedTuples = append(cachedTuples, &storage.TupleRecord{
			UserObjectType: userObjectType,
			UserObjectID:   userObjectID,
			UserRelation:   userRelation,
			InsertedAt:     ts.AsTime(),
		})
	}

	tk := tuple.NewTupleKey("license:1", "owner", "")
	filter := storage.ReadFilter{
		Object:   "license:1",
		Relation: "owner",
		User:     "",
	}

	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	invalidEntityKey := storage.InvalidIteratorByObjectRelationCacheKey(storeID, tk.GetObject(), tk.GetRelation())
	cacheKey := storage.ReadKey(storeID, filter)

	t.Run("cache_miss", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(cacheKey).Return(nil),
			mockDatastore.EXPECT().
				Read(gomock.Any(), storeID, filter, storage.ReadOptions{}).
				Return(storage.NewStaticTupleIterator(tuples), nil),
			mockCache.EXPECT().Get(cacheKey).Return(nil),
			mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKey).Return(nil),
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), ttl).DoAndReturn(func(_ keys.Key, entry *storage.TupleIteratorCacheEntry, _ time.Duration) {
				if diff := cmp.Diff(cachedTuples, entry.Tuples, cmpOpts...); diff != "" {
					t.Fatalf("mismatch (-want +got):\n%s", diff)
				}
			}),
		)

		iter, err := ds.Read(ctx, storeID, filter, storage.ReadOptions{})
		require.NoError(t, err)

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		iter.Stop() // has to be sync otherwise the assertion fails
		i, ok := iter.(*cachedIterator)
		require.True(t, ok)
		i.wg.Wait()

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("cache_hit", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(cacheKey).Return(&storage.TupleIteratorCacheEntry{Tuples: cachedTuples}),
			mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKey).Return(nil),
		)

		iter, err := ds.Read(ctx, storeID, filter, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter.Stop()

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("cache_empty_response", func(t *testing.T) {
		gomock.InOrder(
			mockCache.EXPECT().Get(cacheKey),
			mockDatastore.EXPECT().
				Read(gomock.Any(), storeID, filter, storage.ReadOptions{}).
				Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil),
			mockCache.EXPECT().Get(cacheKey),
			mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(storeID)).Return(nil),
			mockCache.EXPECT().Get(invalidEntityKey).Return(nil),
			mockCache.EXPECT().Set(gomock.Any(), gomock.Any(), ttl).DoAndReturn(func(_ keys.Key, entry *storage.TupleIteratorCacheEntry, _ time.Duration) {
				require.Empty(t, entry.Tuples)
			}),
		)

		iter, err := ds.Read(ctx, storeID, filter, storage.ReadOptions{})
		require.NoError(t, err)

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		iter.Stop() // has to be sync otherwise the assertion fails
		i, ok := iter.(*cachedIterator)
		require.True(t, ok)
		i.wg.Wait()

		require.Empty(t, actual)
	})

	t.Run("higher_consistency", func(t *testing.T) {
		opts := storage.ReadOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
			},
		}

		gomock.InOrder(
			mockDatastore.EXPECT().
				Read(gomock.Any(), storeID, filter, opts).
				Return(storage.NewStaticTupleIterator(tuples), nil),
		)

		iter, err := ds.Read(ctx, storeID, filter, opts)
		require.NoError(t, err)
		defer iter.Stop()

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("tuple_key_is_not_from_ttu", func(t *testing.T) {
		invalidObjectFilter := storage.ReadFilter{
			Object:   "invalid",
			Relation: filter.Relation,
			User:     filter.User,
		}

		gomock.InOrder(
			mockDatastore.EXPECT().
				Read(gomock.Any(), storeID, invalidObjectFilter, storage.ReadOptions{}).
				Return(storage.NewStaticTupleIterator(tuples), nil),
		)

		iter, err := ds.Read(ctx, storeID, invalidObjectFilter, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter.Stop()

		var actual []*openfgav1.Tuple

		for {
			tuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tuple)
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestReadUserTuple(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	storeID := ulid.Make().String()
	tk := tuple.NewTupleKey("document:1", "viewer", "user:anne")
	stored := &openfgav1.Tuple{Key: tk, Timestamp: timestamppb.New(time.Now())}
	filter := storage.ReadUserTupleFilter{Object: tk.GetObject(), Relation: tk.GetRelation(), User: tk.GetUser()}
	defaultOpts := storage.ReadUserTupleOptions{}
	higherOpts := storage.ReadUserTupleOptions{
		Consistency: storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY},
	}
	objectRelationMarker := storage.InvalidIteratorByObjectRelationCacheKey(storeID, tk.GetObject(), tk.GetRelation())
	storeMarker := storage.InvalidIteratorCacheKey(storeID)
	ttl := 5 * time.Hour

	setup := func(t *testing.T) (*CachedDatastore, *mocks.MockOpenFGADatastore, storage.InMemoryCache[any]) {
		cache, err := storage.NewInMemoryLRUCache[any]()
		require.NoError(t, err)
		t.Cleanup(cache.Stop)
		inner := mocks.NewMockOpenFGADatastore(gomock.NewController(t))
		return NewCachedDatastore(ctx, inner, cache, 10, ttl, &singleflight.Group{}, &sync.WaitGroup{}), inner, cache
	}
	requireFound := func(t *testing.T, got *openfgav1.Tuple, err error) {
		t.Helper()
		require.NoError(t, err)
		if diff := cmp.Diff(stored, got, protocmp.Transform()); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	}
	markAt := func(cache storage.InMemoryCache[any], marker keys.Key, at time.Time) {
		cache.Set(marker, &storage.InvalidEntityCacheEntry{LastModified: at}, ttl)
	}

	t.Run("found_is_served_from_cache_stamped_at_query_start", func(t *testing.T) {
		ds, inner, cache := setup(t)
		var queriedAfter time.Time
		inner.EXPECT().ReadUserTuple(gomock.Any(), storeID, filter, defaultOpts).
			DoAndReturn(func(context.Context, string, storage.ReadUserTupleFilter, storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
				queriedAfter = time.Now()
				return stored, nil
			}).Times(1)

		before := time.Now()
		got, err := ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
		requireFound(t, got, err)

		entry, ok := cache.Get(storage.ReadUserTupleKey(storeID, filter)).(*storage.UserTupleCacheEntry)
		require.True(t, ok)
		require.False(t, entry.LastModified.Before(before))
		require.False(t, entry.LastModified.After(queriedAfter))

		got, err = ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
		requireFound(t, got, err)
	})

	t.Run("hits_return_independent_copies", func(t *testing.T) {
		ds, inner, _ := setup(t)
		inner.EXPECT().ReadUserTuple(gomock.Any(), storeID, filter, defaultOpts).Return(proto.Clone(stored), nil).Times(1)

		first, err := ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
		require.NoError(t, err)
		first.GetKey().User = "user:mallory"

		got, err := ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
		requireFound(t, got, err)
		got.GetKey().User = "user:mallory"

		got, err = ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
		requireFound(t, got, err)
	})

	t.Run("not_found_is_served_from_cache", func(t *testing.T) {
		ds, inner, _ := setup(t)
		inner.EXPECT().ReadUserTuple(gomock.Any(), storeID, filter, defaultOpts).Return(nil, storage.ErrNotFound).Times(1)

		for range 2 {
			_, err := ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
			require.ErrorIs(t, err, storage.ErrNotFound)
		}
	})

	t.Run("conditions_are_part_of_the_key", func(t *testing.T) {
		ds, inner, _ := setup(t)
		conditioned := filter
		conditioned.Conditions = []string{""}
		gomock.InOrder(
			inner.EXPECT().ReadUserTuple(gomock.Any(), storeID, filter, defaultOpts).Return(stored, nil),
			inner.EXPECT().ReadUserTuple(gomock.Any(), storeID, conditioned, defaultOpts).Return(nil, storage.ErrNotFound),
		)

		got, err := ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
		requireFound(t, got, err)
		_, err = ds.ReadUserTuple(ctx, storeID, conditioned, defaultOpts)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	for name, marker := range map[string]keys.Key{
		"object_relation_marker": objectRelationMarker,
		"store_marker":           storeMarker,
	} {
		t.Run(name+"_newer_than_the_entry_invalidates_it", func(t *testing.T) {
			ds, inner, cache := setup(t)
			gomock.InOrder(
				inner.EXPECT().ReadUserTuple(gomock.Any(), storeID, filter, defaultOpts).Return(nil, storage.ErrNotFound),
				inner.EXPECT().ReadUserTuple(gomock.Any(), storeID, filter, defaultOpts).Return(stored, nil),
			)

			_, err := ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
			require.ErrorIs(t, err, storage.ErrNotFound)

			entry, ok := cache.Get(storage.ReadUserTupleKey(storeID, filter)).(*storage.UserTupleCacheEntry)
			require.True(t, ok)
			markAt(cache, marker, entry.LastModified.Add(time.Nanosecond))

			got, err := ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
			requireFound(t, got, err)
			got, err = ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
			requireFound(t, got, err)
		})

		t.Run(name+"_older_than_the_entry_keeps_it", func(t *testing.T) {
			ds, inner, cache := setup(t)
			markAt(cache, marker, time.Now().Add(-time.Second))
			inner.EXPECT().ReadUserTuple(gomock.Any(), storeID, filter, defaultOpts).Return(stored, nil).Times(1)

			for range 2 {
				got, err := ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
				requireFound(t, got, err)
			}
		})
	}

	t.Run("markers_of_other_objects_and_relations_keep_the_entry", func(t *testing.T) {
		ds, inner, cache := setup(t)
		inner.EXPECT().ReadUserTuple(gomock.Any(), storeID, filter, defaultOpts).Return(stored, nil).Times(1)

		got, err := ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
		requireFound(t, got, err)

		now := time.Now()
		markAt(cache, storage.InvalidIteratorByObjectRelationCacheKey(storeID, "document:2", tk.GetRelation()), now)
		markAt(cache, storage.InvalidIteratorByObjectRelationCacheKey(storeID, tk.GetObject(), "editor"), now)
		markAt(cache, storage.InvalidIteratorCacheKey(ulid.Make().String()), now)

		got, err = ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
		requireFound(t, got, err)
	})

	t.Run("invalidation_during_the_query_is_not_cached", func(t *testing.T) {
		ds, inner, cache := setup(t)
		gomock.InOrder(
			inner.EXPECT().ReadUserTuple(gomock.Any(), storeID, filter, defaultOpts).
				DoAndReturn(func(context.Context, string, storage.ReadUserTupleFilter, storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
					// The controller observes a write that this query may not have seen.
					markAt(cache, objectRelationMarker, time.Now())
					return nil, storage.ErrNotFound
				}),
			inner.EXPECT().ReadUserTuple(gomock.Any(), storeID, filter, defaultOpts).Return(stored, nil),
		)

		_, err := ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
		require.ErrorIs(t, err, storage.ErrNotFound)
		require.Nil(t, cache.Get(storage.ReadUserTupleKey(storeID, filter)))

		got, err := ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
		requireFound(t, got, err)
	})

	t.Run("higher_consistency_bypasses_the_cache", func(t *testing.T) {
		ds, inner, cache := setup(t)
		gomock.InOrder(
			inner.EXPECT().ReadUserTuple(gomock.Any(), storeID, filter, higherOpts).Return(stored, nil),
			inner.EXPECT().ReadUserTuple(gomock.Any(), storeID, filter, defaultOpts).Return(nil, storage.ErrNotFound),
			inner.EXPECT().ReadUserTuple(gomock.Any(), storeID, filter, higherOpts).Return(stored, nil),
		)

		got, err := ds.ReadUserTuple(ctx, storeID, filter, higherOpts)
		requireFound(t, got, err)
		require.Nil(t, cache.Get(storage.ReadUserTupleKey(storeID, filter)), "a HIGHER_CONSISTENCY read populated the cache")

		_, err = ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
		require.ErrorIs(t, err, storage.ErrNotFound)

		got, err = ds.ReadUserTuple(ctx, storeID, filter, higherOpts)
		requireFound(t, got, err)

		_, err = ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("datastore_errors_are_not_cached", func(t *testing.T) {
		ds, inner, _ := setup(t)
		boom := errors.New("connection reset")
		gomock.InOrder(
			inner.EXPECT().ReadUserTuple(gomock.Any(), storeID, filter, defaultOpts).Return(nil, boom),
			inner.EXPECT().ReadUserTuple(gomock.Any(), storeID, filter, defaultOpts).Return(stored, nil),
		)

		_, err := ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
		require.ErrorIs(t, err, boom)
		got, err := ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
		requireFound(t, got, err)
	})

	t.Run("nil_tuple_without_error_fails", func(t *testing.T) {
		ds, inner, cache := setup(t)
		inner.EXPECT().ReadUserTuple(gomock.Any(), storeID, filter, defaultOpts).Return(nil, nil)

		_, err := ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
		require.ErrorContains(t, err, "neither a tuple nor ErrNotFound")
		require.Nil(t, cache.Get(storage.ReadUserTupleKey(storeID, filter)))
	})

	t.Run("foreign_entry_under_the_key_fails", func(t *testing.T) {
		ds, _, cache := setup(t)
		cache.Set(storage.ReadUserTupleKey(storeID, filter), &storage.TupleIteratorCacheEntry{}, ttl)

		_, err := ds.ReadUserTuple(ctx, storeID, filter, defaultOpts)
		require.ErrorContains(t, err, "*storage.TupleIteratorCacheEntry")
	})
}

func TestDatastoreIteratorError(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	maxSize := 10
	ttl := 5 * time.Hour
	sf := &singleflight.Group{}
	wg := &sync.WaitGroup{}
	ds := NewCachedDatastore(ctx, mockDatastore, mockCache, maxSize, ttl, sf, wg)

	storeID := ulid.Make().String()

	filter := storage.ReadFilter{
		Object:   "license:1",
		Relation: "owner",
		User:     "",
	}

	gomock.InOrder(
		mockCache.EXPECT().Get(gomock.Any()),
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, filter, storage.ReadOptions{}).
			Return(nil, storage.ErrNotFound),
	)

	_, err := ds.Read(ctx, storeID, filter, storage.ReadOptions{})
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func TestCachedIterator_IsOrdered(t *testing.T) {
	inner := storage.NewStaticTupleIterator([]*openfgav1.Tuple{})
	iter := &cachedIterator{iter: inner}
	defer iter.Stop()
	require.True(t, iter.IsOrdered())
}

func TestCachedIterator(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ctx := context.Background()

	tuples := []*openfgav1.Tuple{
		{
			Key:       tuple.NewTupleKey("document:doc1", "viewer", "bill"),
			Timestamp: timestamppb.New(time.Now()),
		},
		{
			Key:       tuple.NewTupleKey("document:doc2", "editor", "bob"),
			Timestamp: timestamppb.New(time.Now()),
		},
	}

	cachedTuples := []*storage.TupleRecord{
		{
			ObjectID:       "doc1",
			ObjectType:     "document",
			Relation:       "viewer",
			UserObjectType: "",
			UserObjectID:   "bill",
			UserRelation:   "",
			InsertedAt:     tuples[0].GetTimestamp().AsTime(),
		},
		{
			ObjectID:       "doc2",
			ObjectType:     "document",
			Relation:       "editor",
			UserObjectType: "",
			UserObjectID:   "bob",
			UserRelation:   "",
			InsertedAt:     tuples[1].GetTimestamp().AsTime(),
		},
	}

	cmpOpts := []cmp.Option{
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}

	t.Run("next_yielding_error_discards_results", func(t *testing.T) {
		maxCacheSize := 1
		cacheKey := testCacheKey("cache-key")
		ttl := 5 * time.Hour
		cache, err := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		require.NoError(t, err)
		defer cache.Stop()

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              mocks.NewErrorTupleIterator(tuples),
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidEntityKeys: []keys.Key{},
			cache:             cache,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			objectType:        "",
			objectID:          "",
			relation:          "",
			userType:          "",
			logger:            logger.NewNoopLogger(),
		}

		_, err = iter.Next(ctx)
		require.NoError(t, err)

		_, err = iter.Next(ctx)
		require.Error(t, err)

		iter.Stop()
		require.Nil(t, iter.tuples)
		cachedResults := cache.Get(cacheKey)
		require.Nil(t, cachedResults)
	})

	t.Run("next_at_max_discards_results", func(t *testing.T) {
		maxCacheSize := 1
		cacheKey := testCacheKey("cache-key")
		ttl := 5 * time.Hour
		cache, err := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		require.NoError(t, err)
		defer cache.Stop()

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              storage.NewStaticTupleIterator(tuples),
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidEntityKeys: []keys.Key{},
			cache:             cache,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			objectType:        "",
			objectID:          "",
			relation:          "",
			userType:          "",
			logger:            logger.NewNoopLogger(),
		}

		_, err = iter.Next(ctx)
		require.NoError(t, err)

		require.Nil(t, iter.tuples)
	})

	t.Run("calling_stop_doesnt_cache_due_to_size_foreground", func(t *testing.T) {
		maxCacheSize := 1
		cacheKey := testCacheKey("cache-key")
		ttl := 5 * time.Hour
		cache, err := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		require.NoError(t, err)
		defer cache.Stop()

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              storage.NewStaticTupleIterator(tuples),
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidEntityKeys: []keys.Key{},
			cache:             cache,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			objectType:        "",
			objectID:          "",
			relation:          "",
			userType:          "",
			logger:            logger.NewNoopLogger(),
		}

		var actual []*openfgav1.Tuple

		for {
			tk, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tk)
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}

		iter.Stop()
		iter.wg.Wait()

		cachedResults := cache.Get(cacheKey)
		require.Nil(t, cachedResults)
		require.Nil(t, iter.tuples)
		require.Nil(t, iter.records)
	})

	t.Run("calling_stop_doesnt_cache_due_to_size_background", func(t *testing.T) {
		maxCacheSize := 1
		cacheKey := testCacheKey("cache-key")
		ttl := 5 * time.Hour
		cache, err := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		require.NoError(t, err)
		defer cache.Stop()

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              mocks.NewErrorTupleIterator(tuples),
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidEntityKeys: []keys.Key{},
			cache:             cache,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			objectType:        "",
			objectID:          "",
			relation:          "",
			userType:          "",
			logger:            logger.NewNoopLogger(),
		}

		iter.Stop()
		iter.wg.Wait()

		cachedResults := cache.Get(cacheKey)
		require.Nil(t, cachedResults)
		require.Nil(t, iter.tuples)
		require.Nil(t, iter.records)
	})

	t.Run("calling_stop_caches_in_foreground", func(t *testing.T) {
		maxCacheSize := 10
		cacheKey := testCacheKey("cache-key")
		ttl := 5 * time.Hour
		cache, err := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		require.NoError(t, err)
		defer cache.Stop()

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              storage.NewStaticTupleIterator(tuples),
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidEntityKeys: []keys.Key{},
			cache:             cache,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			objectType:        "",
			objectID:          "",
			relation:          "",
			userType:          "",
			logger:            logger.NewNoopLogger(),
		}

		var actual []*openfgav1.Tuple

		for {
			tk, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.Fail(t, "no error was expected")
				break
			}

			actual = append(actual, tk)
		}

		if diff := cmp.Diff(tuples, actual, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}

		iter.Stop()
		iter.wg.Wait()

		cachedResults := cache.Get(cacheKey)
		require.NotNil(t, cachedResults)
		require.Nil(t, iter.tuples)
		require.Nil(t, iter.records)

		entry := cachedResults.(*storage.TupleIteratorCacheEntry)

		if diff := cmp.Diff(cachedTuples, entry.Tuples, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("calling_stop_caches_in_background", func(t *testing.T) {
		maxCacheSize := 10
		cacheKey := testCacheKey("cache-key")
		ttl := 5 * time.Hour
		cache, err := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		require.NoError(t, err)
		defer cache.Stop()

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              storage.NewStaticTupleIterator(tuples),
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidEntityKeys: []keys.Key{},
			cache:             cache,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			objectType:        "",
			objectID:          "",
			relation:          "",
			userType:          "",
			logger:            logger.NewNoopLogger(),
		}

		iter.Stop()
		iter.wg.Wait()

		cachedResults := cache.Get(cacheKey)
		require.NotNil(t, cachedResults)
		require.Nil(t, iter.tuples)
		require.Nil(t, iter.records)

		entry := cachedResults.(*storage.TupleIteratorCacheEntry)

		if diff := cmp.Diff(cachedTuples, entry.Tuples, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("parent_context_cancelled_still_caches_in_background", func(t *testing.T) {
		maxCacheSize := 10
		cacheKey := testCacheKey("cache-key")
		ttl := 5 * time.Hour
		cache, err := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		require.NoError(t, err)
		defer cache.Stop()

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              storage.NewStaticTupleIterator(tuples),
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidEntityKeys: []keys.Key{},
			cache:             cache,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			objectType:        "",
			objectID:          "",
			relation:          "",
			userType:          "",
			logger:            logger.NewNoopLogger(),
		}

		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = iter.Next(cancelledCtx)
		require.ErrorIs(t, err, context.Canceled)

		iter.Stop()
		iter.wg.Wait()

		cachedResults := cache.Get(cacheKey)
		require.NotNil(t, cachedResults)
		require.Nil(t, iter.tuples)
		require.Nil(t, iter.records)

		entry := cachedResults.(*storage.TupleIteratorCacheEntry)

		if diff := cmp.Diff(cachedTuples, entry.Tuples, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("prevent_draining_if_already_cached", func(t *testing.T) {
		maxCacheSize := 10
		cacheKey := testCacheKey("cache-key")
		ttl := 5 * time.Hour
		store := ulid.Make().String()
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockCache := mocks.NewMockInMemoryCache[any](mockController)
		tupleRecord := &storage.TupleIteratorCacheEntry{
			Tuples:       cachedTuples,
			LastModified: time.Now().Add(-1 * time.Second),
		}
		gomock.InOrder(
			mockCache.EXPECT().Get(cacheKey).Return(tupleRecord),
			mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(store)).Return(nil),
		)

		var wg sync.WaitGroup

		mockedIter := &mockCalledTupleIterator{
			iter: storage.NewStaticTupleIterator(tuples),
		}

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              mockedIter,
			store:             store,
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidStoreKey:   storage.InvalidIteratorCacheKey(store),
			invalidEntityKeys: []keys.Key{},
			cache:             mockCache,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			objectType:        "",
			objectID:          "",
			relation:          "",
			userType:          "",
			logger:            logger.NewNoopLogger(),
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			iter.Stop()
		}()

		wg.Wait()
		iter.wg.Wait()

		require.Zero(t, mockedIter.nextCalled)
		require.Nil(t, iter.tuples)
		require.Nil(t, iter.records)
	})

	t.Run("cache_entry_last_modified_uses_query_start_time", func(t *testing.T) {
		// The race: iterator starts at t0, the pre-write guard passes (no invalidation
		// entry yet), then a write is recorded at t1 between the guard and flush().
		// With the fix, LastModified = initializedAt = t0 < t1, so findInCache rejects
		// the stale entry. We exercise the real race by injecting the invalidation entry
		// inside the cache's Set call, before it returns to flush().
		maxCacheSize := 10
		cacheKey := testCacheKey("cache-key")
		ttl := 5 * time.Hour
		store := ulid.Make().String()
		invalidEntityKey := storage.InvalidIteratorByObjectRelationCacheKey(store, "document:1", "viewer")

		inner, err := storage.NewInMemoryLRUCache([]storage.InMemoryLRUCacheOpt[any]{
			storage.WithMaxCacheSize[any](int64(100)),
		}...)
		require.NoError(t, err)
		defer inner.Stop()

		t0 := time.Now().Add(-2 * time.Second)
		t1 := time.Now().Add(-1 * time.Second) // write between guard and flush

		// interceptingCache injects the invalidation entry when flush() calls Set for
		// the tuple cache key, simulating a concurrent write that races the flush.
		intercepting := &interceptingCache{
			InMemoryCache: inner,
			onSet: func(k keys.Key) {
				if k == cacheKey {
					inner.Set(invalidEntityKey, &storage.InvalidEntityCacheEntry{LastModified: t1}, ttl)
				}
			},
		}

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              storage.NewStaticTupleIterator(tuples),
			store:             store,
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidStoreKey:   storage.InvalidIteratorCacheKey(store),
			invalidEntityKeys: []keys.Key{invalidEntityKey},
			cache:             intercepting,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			initializedAt:     t0,
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			logger:            logger.NewNoopLogger(),
		}

		// Fully drain so flush() is called in the foreground (iterator already consumed).
		for {
			_, err := iter.Next(ctx)
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}
			require.NoError(t, err)
		}

		// No invalidation entry in cache yet, so the pre-write guard passes.
		iter.Stop()
		iter.wg.Wait()

		// The cache entry must have been written with LastModified = t0 (initializedAt).
		raw := inner.Get(cacheKey)
		require.NotNil(t, raw, "expected cache entry to be written")
		entry := raw.(*storage.TupleIteratorCacheEntry)
		require.Equal(t, t0, entry.LastModified)

		// findInCache must reject it: entry.LastModified (t0) < invalidation (t1).
		_, ok := findInCache(intercepting, cacheKey, storage.InvalidIteratorCacheKey(store), []keys.Key{invalidEntityKey})
		require.False(t, ok, "stale cache entry should be invalidated because write occurred after query start")
	})

	t.Run("prevent_draining_if_queried_before_invalidation_time", func(t *testing.T) {
		maxCacheSize := 10
		cacheKey := testCacheKey("cache-key")
		ttl := 5 * time.Hour
		store := ulid.Make().String()
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockCache := mocks.NewMockInMemoryCache[any](mockController)

		gomock.InOrder(
			mockCache.EXPECT().Get(cacheKey).Return(nil),
			mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(store)).Return(&storage.InvalidEntityCacheEntry{
				LastModified: time.Now().Add(1 * time.Minute),
			}),
		)

		var wg sync.WaitGroup

		mockedIter := &mockCalledTupleIterator{
			iter: storage.NewStaticTupleIterator(tuples),
		}

		iter := &cachedIterator{
			ctx:               ctx,
			iter:              mockedIter,
			store:             store,
			operation:         "operation",
			tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
			cacheKey:          cacheKey,
			invalidStoreKey:   storage.InvalidIteratorCacheKey(store),
			invalidEntityKeys: []keys.Key{},
			cache:             mockCache,
			maxResultSize:     maxCacheSize,
			ttl:               ttl,
			initializedAt:     time.Now(),
			sf:                &singleflight.Group{},
			wg:                &sync.WaitGroup{},
			objectType:        "",
			objectID:          "",
			relation:          "",
			userType:          "",
			logger:            logger.NewNoopLogger(),
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			iter.Stop()
		}()

		wg.Wait()
		iter.wg.Wait()

		require.Zero(t, mockedIter.nextCalled)
		require.Nil(t, iter.tuples)
		require.Nil(t, iter.records)
	})

	t.Run("prevent_draining_on_the_same_iterator_across_concurrent_requests", func(t *testing.T) {
		maxCacheSize := 10
		cacheKey := testCacheKey("cache-key")
		ttl := 5 * time.Hour
		store := ulid.Make().String()
		for i := 0; i < 100; i++ {
			mockController := gomock.NewController(t)
			defer mockController.Finish()

			mockCache := mocks.NewMockInMemoryCache[any](mockController)

			mockCache.EXPECT().Get(cacheKey).AnyTimes().Return(nil)
			mockCache.EXPECT().Get(storage.InvalidIteratorCacheKey(store)).AnyTimes().Return(nil)
			mockCache.EXPECT().Set(cacheKey, gomock.Any(), ttl).AnyTimes()
			mockCache.EXPECT().Delete(gomock.Any()).AnyTimes()

			sf := &singleflight.Group{}

			var wg sync.WaitGroup

			mockedIter1 := &mockCalledTupleIterator{
				iter: storage.NewStaticTupleIterator(tuples),
			}

			iter1 := &cachedIterator{
				ctx:               ctx,
				iter:              mockedIter1,
				operation:         "operation",
				tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
				cacheKey:          cacheKey,
				invalidStoreKey:   storage.InvalidIteratorCacheKey(store),
				invalidEntityKeys: []keys.Key{},
				cache:             mockCache,
				maxResultSize:     maxCacheSize,
				ttl:               ttl,
				sf:                sf,
				wg:                &sync.WaitGroup{},
				objectType:        "",
				objectID:          "",
				relation:          "",
				userType:          "",
				logger:            logger.NewNoopLogger(),
			}

			mockedIter2 := &mockCalledTupleIterator{
				iter: storage.NewStaticTupleIterator(tuples),
			}

			iter2 := &cachedIterator{
				ctx:               ctx,
				iter:              mockedIter2,
				operation:         "operation",
				tuples:            make([]*openfgav1.Tuple, 0, maxCacheSize),
				cacheKey:          cacheKey,
				invalidStoreKey:   storage.InvalidIteratorCacheKey(store),
				invalidEntityKeys: []keys.Key{},
				cache:             mockCache,
				maxResultSize:     maxCacheSize,
				ttl:               ttl,
				sf:                sf,
				wg:                &sync.WaitGroup{},
				objectType:        "",
				objectID:          "",
				relation:          "",
				userType:          "",
				logger:            logger.NewNoopLogger(),
			}

			wg.Add(2)

			go func() {
				defer wg.Done()

				iter1.Stop()
				iter1.wg.Wait()
			}()

			go func() {
				defer wg.Done()

				iter2.Stop()
				iter2.wg.Wait()
			}()

			wg.Wait()

			require.GreaterOrEqual(t, mockedIter1.nextCalled, 0)
			require.GreaterOrEqual(t, mockedIter2.nextCalled, 0)
			require.GreaterOrEqual(t, mockedIter1.nextCalled+mockedIter2.nextCalled, 3)
		}
	})
}

type mockCalledTupleIterator struct {
	iter        storage.TupleIterator
	nextCalled  int
	headCalled  int
	closeCalled int
}

func (s *mockCalledTupleIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	s.nextCalled++
	return s.iter.Next(ctx)
}

func (s *mockCalledTupleIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	s.headCalled++
	return s.iter.Head(ctx)
}

func (s *mockCalledTupleIterator) Stop() {
	s.closeCalled++
	s.iter.Stop()
}

func (s *mockCalledTupleIterator) IsOrdered() bool { return s.iter.IsOrdered() }

// interceptingCache wraps an InMemoryCache and calls onSet before each Set,
// allowing tests to inject side effects that race with a cache write.
type interceptingCache struct {
	storage.InMemoryCache[any]
	onSet func(keys.Key)
}

func (c *interceptingCache) Set(k keys.Key, v any, ttl time.Duration) {
	c.onSet(k)
	c.InMemoryCache.Set(k, v, ttl)
}

// invalidIteratorByUserObjectTypeKeys is a test helper that returns one
// invalidation cache key per user, mirroring the production fan-out previously
// performed by storage.GetInvalidIteratorByUserObjectTypeCacheKeys.
func invalidIteratorByUserObjectTypeKeys(storeID string, users []string, objectType string) []keys.Key {
	result := make([]keys.Key, 0, len(users))
	for _, u := range users {
		result = append(result, storage.InvalidIteratorByUserObjectTypeCacheKey(storeID, u, objectType))
	}
	return result
}
