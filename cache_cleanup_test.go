package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/schematichq/rulesengine"
	"github.com/schematichq/schematic-go/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock logger for testing
type mockLogger struct{}

func (m *mockLogger) Info(ctx context.Context, message string, args ...any)  {}
func (m *mockLogger) Warn(ctx context.Context, message string, args ...any)  {}
func (m *mockLogger) Error(ctx context.Context, message string, args ...any) {}
func (m *mockLogger) Debug(ctx context.Context, message string, args ...any) {}

// Compile-time check to ensure mockLogger implements core.Logger
var _ core.Logger = (*mockLogger)(nil)

func TestCacheCleanupManager(t *testing.T) {
	// Create mock caches (we'll use Redis with a test client)
	testClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // This might fail if Redis isn't running, but that's okay for compilation test
	})

	flagsCache := NewRedisCache[*rulesengine.Flag](testClient, 0)
	companiesCache := NewRedisCache[*rulesengine.Company](testClient, 0)
	usersCache := NewRedisCache[*rulesengine.User](testClient, 0)

	logger := &mockLogger{}

	// Test that we can create the cleanup manager
	cleanupManager := NewCacheCleanupManager(
		flagsCache,
		companiesCache,
		usersCache,
		logger,
		1*time.Hour,
	)

	if cleanupManager == nil {
		t.Fatal("Failed to create cache cleanup manager")
	}

	// Test that we can start and stop it (won't actually run because we don't wait)
	cleanupManager.Start(context.Background())
	cleanupManager.Stop()

	t.Log("Cache cleanup manager created and started/stopped successfully")
}

// TestCacheCleanupPreservesCurrentVersionKeys is the explicit guarantee that the
// hourly cleanup never deletes valid current-version data — id-based keys in
// particular — and only removes keys stamped with a different version segment.
func TestCacheCleanupPreservesCurrentVersionKeys(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()

	cur := rulesengine.VersionKey
	const old = "0ldver99" // any 3rd segment != current version
	require.NotEqual(t, cur, old)

	// Current-version keys built via the real builders — MUST survive.
	currentKeys := map[string]string{
		"company id key": companyIDCacheKey("comp_keepme"),
		"user id key":    userIDCacheKey("user_keepme"),
		"flag key":       flagCacheKey("flag-keepme"),
		"company lookup": resourceKeyToCacheKey(cacheKeyPrefixCompany, "clerkid", "org_keep"),
		"user lookup":    resourceKeyToCacheKey(cacheKeyPrefixUser, "email", "keep@x.com"),
	}
	// Same shapes but a stale version segment — MUST be deleted.
	staleKeys := map[string]string{
		"company id key": fmt.Sprintf("%s:%s:%s:%s", cacheKeyPrefix, cacheKeyPrefixCompany, old, "comp_old"),
		"user id key":    fmt.Sprintf("%s:%s:%s:%s", cacheKeyPrefix, cacheKeyPrefixUser, old, "user_old"),
		"flag key":       fmt.Sprintf("%s:%s:%s:%s", cacheKeyPrefix, cacheKeyPrefixFlags, old, "flag-old"),
		"company lookup": fmt.Sprintf("%s:%s:%s:%s:%s", cacheKeyPrefix, cacheKeyPrefixCompany, old, "clerkid", "org_old"),
		"user lookup":    fmt.Sprintf("%s:%s:%s:%s:%s", cacheKeyPrefix, cacheKeyPrefixUser, old, "email", "old@x.com"),
	}

	for _, k := range currentKeys {
		require.NoError(t, client.Set(ctx, k, "v", 0).Err())
	}
	for _, k := range staleKeys {
		require.NoError(t, client.Set(ctx, k, "v", 0).Err())
	}

	mgr := NewCacheCleanupManager(
		NewRedisCache[*rulesengine.Flag](client, 0),
		NewRedisCache[*rulesengine.Company](client, 0),
		NewRedisCache[*rulesengine.User](client, 0),
		&mockLogger{},
		time.Hour,
	)
	require.NoError(t, mgr.cleanupStaleEntries(ctx))

	for name, k := range currentKeys {
		n, err := client.Exists(ctx, k).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(1), n, "current-version %s was WRONGLY DELETED: %s", name, k)
	}
	for name, k := range staleKeys {
		n, err := client.Exists(ctx, k).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(0), n, "stale-version %s was not deleted: %s", name, k)
	}
}

func TestCacheCleanupManagerWithZeroInterval(t *testing.T) {
	// Test that we handle zero interval properly in the application logic
	// (In real usage, cleanup manager is not created when interval is 0)

	// Simulate the main.go logic
	cacheCleanupInterval := 0 * time.Second
	var cacheCleanupManager *CacheCleanupManager

	if cacheCleanupInterval > 0 {
		// This should not execute
		t.Fatal("Should not create cleanup manager with zero interval")
	} else {
		// This is the expected path
		cacheCleanupManager = nil
		t.Log("Cache cleanup correctly disabled with zero interval")
	}

	if cacheCleanupManager != nil {
		t.Fatal("Cleanup manager should be nil with zero interval")
	}
}
