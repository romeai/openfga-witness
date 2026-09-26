package shared

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/singleflight"

	"github.com/openfga/openfga/internal/cachecontroller"
	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
)

func TestSharedDatastoreResources(t *testing.T) {
	sharedCtx := context.Background()
	sharedSf := &singleflight.Group{}
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	t.Run("without", func(t *testing.T) {
		settings := config.CacheSettings{}
		s, err := NewSharedDatastoreResources(sharedCtx, sharedSf, mockDatastore, settings)
		require.NoError(t, err)
		t.Cleanup(s.Close)

		require.Equal(t, sharedCtx, s.ServerCtx)
		require.Equal(t, sharedSf, s.SingleflightGroup)
		require.Nil(t, s.CheckCache)
		require.Nil(t, s.CheckCacheInvalidations)
		require.NotNil(t, s.WaitGroup)
		require.NotNil(t, s.CacheController)
		_, ok := s.CacheController.(*cachecontroller.NoopCacheController)
		require.True(t, ok)
	})

	t.Run("with_cache", func(t *testing.T) {
		settings := config.CacheSettings{
			CheckCacheLimit:           1,
			CheckIteratorCacheEnabled: true,
		}

		s, err := NewSharedDatastoreResources(sharedCtx, sharedSf, mockDatastore, settings)
		require.NoError(t, err)
		t.Cleanup(s.Close)

		require.NotNil(t, s.CheckCache)
		require.NotNil(t, s.ShadowCheckCache)
		require.NotNil(t, s.CheckCacheInvalidations)
		require.NotNil(t, s.ShadowCheckCacheInvalidations)
		require.NotSame(t, s.CheckCacheInvalidations, s.ShadowCheckCacheInvalidations, "each cache has its own markers")
	})

	t.Run("markers_outlive_every_iterator_entry", func(t *testing.T) {
		settings := config.CacheSettings{
			CheckCacheLimit:                    1,
			CheckIteratorCacheEnabled:          true,
			CheckIteratorCacheTTL:              time.Hour,
			ListObjectsIteratorCacheEnabled:    true,
			ListObjectsIteratorCacheMaxResults: 1,
			ListObjectsIteratorCacheTTL:        2 * time.Hour,
			CacheTTLJitterPercentage:           10,
		}

		s, err := NewSharedDatastoreResources(sharedCtx, sharedSf, mockDatastore, settings)
		require.NoError(t, err)
		t.Cleanup(s.Close)

		longest := 2*time.Hour + 12*time.Minute
		for _, markers := range []*storage.InvalidationMarkers{s.CheckCacheInvalidations, s.ShadowCheckCacheInvalidations} {
			require.NotPanics(t, func() { markers.MustGuard(longest) })
			require.Panics(t, func() { markers.MustGuard(longest + time.Nanosecond) })
		}
	})

	t.Run("with_cache_controller", func(t *testing.T) {
		settings := config.CacheSettings{
			CheckCacheLimit:           1,
			CheckIteratorCacheEnabled: true,
			CacheControllerEnabled:    true,
		}

		s, err := NewSharedDatastoreResources(sharedCtx, sharedSf, mockDatastore, settings)
		require.NoError(t, err)
		t.Cleanup(s.Close)

		require.NotNil(t, s.CacheController)
		require.NotNil(t, s.ShadowCacheController)
		_, ok := s.CacheController.(*cachecontroller.InMemoryCacheController)
		require.True(t, ok)
	})

	t.Run("with_shadow_cache", func(t *testing.T) {
		settings := config.CacheSettings{
			CheckCacheLimit:           1,
			CheckIteratorCacheEnabled: true,
		}

		s, err := NewSharedDatastoreResources(sharedCtx, sharedSf, mockDatastore, settings)
		require.NoError(t, err)
		t.Cleanup(s.Close)

		require.NotNil(t, s.ShadowCheckCache)
		require.NotEqual(t, s.CheckCache, s.ShadowCheckCache)
	})

	t.Run("with_shadow_cache_controller", func(t *testing.T) {
		settings := config.CacheSettings{
			CheckCacheLimit:           1,
			CheckIteratorCacheEnabled: true,
			CacheControllerEnabled:    true,
		}

		s, err := NewSharedDatastoreResources(sharedCtx, sharedSf, mockDatastore, settings)
		require.NoError(t, err)
		t.Cleanup(s.Close)

		require.NotNil(t, s.ShadowCacheController)
		_, ok := s.ShadowCacheController.(*cachecontroller.InMemoryCacheController)
		require.True(t, ok)
		require.NotSame(t, s.CacheController, s.ShadowCacheController)
	})

	t.Run("with_custom_logger", func(t *testing.T) {
		settings := config.CacheSettings{
			CheckCacheLimit:           1,
			CheckIteratorCacheEnabled: true,
			CacheControllerEnabled:    true,
		}

		mockLogger := mockstorage.NewMockLogger(mockController)

		s, err := NewSharedDatastoreResources(sharedCtx, sharedSf, mockDatastore, settings,
			WithLogger(mockLogger))
		require.NoError(t, err)
		t.Cleanup(s.Close)

		require.Equal(t, mockLogger, s.Logger)
	})

	t.Run("with_custom_cache_controller", func(t *testing.T) {
		settings := config.CacheSettings{
			CheckCacheLimit:           1,
			CheckIteratorCacheEnabled: true,
			CacheControllerEnabled:    true, // Enables cache controller creation, but should not overwrite custom one
		}

		customController := mockstorage.NewMockCacheController(mockController)

		s, err := NewSharedDatastoreResources(sharedCtx, sharedSf, mockDatastore, settings,
			WithCacheController(customController))
		require.NoError(t, err)
		t.Cleanup(s.Close)

		require.Equal(t, customController, s.CacheController)
	})

	t.Run("with_custom_shadow_cache_controller", func(t *testing.T) {
		settings := config.CacheSettings{
			CheckCacheLimit:           1,
			CheckIteratorCacheEnabled: true,
			CacheControllerEnabled:    true, // Enables shadow controller creation, but should not overwrite custom one
		}

		customShadowController := mockstorage.NewMockCacheController(mockController)

		s, err := NewSharedDatastoreResources(sharedCtx, sharedSf, mockDatastore, settings,
			WithShadowCacheController(customShadowController))
		require.NoError(t, err)
		t.Cleanup(s.Close)

		require.Equal(t, customShadowController, s.ShadowCacheController)
	})
}
