package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/schematichq/rulesengine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCacheWithNoExpiration(t *testing.T) {
	// Start a mini Redis server for testing
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	// Create cache with default TTL of 1 second
	cache := NewRedisCache[string](client, 1*time.Second)
	ctx := context.Background()

	// Test 1: Set with 1 second TTL (should expire)
	err = cache.Set(ctx, "key_with_ttl", "value_with_ttl", 1*time.Second)
	require.NoError(t, err)

	// Verify key exists initially
	value, err := cache.Get(ctx, "key_with_ttl")
	require.NoError(t, err)
	assert.Equal(t, "value_with_ttl", value)

	// Test 2: Set with no expiration (TTL = 0)
	err = cache.Set(ctx, "key_no_expiration", "value_no_expiration", 0)
	require.NoError(t, err)

	// Verify key exists
	value, err = cache.Get(ctx, "key_no_expiration")
	require.NoError(t, err)
	assert.Equal(t, "value_no_expiration", value)

	// Fast forward time in mini Redis to simulate expiration
	mr.FastForward(2 * time.Second)

	// Test 3: Key with TTL should be expired (not found)
	_, err = cache.Get(ctx, "key_with_ttl")
	assert.Error(t, err) // Should be redis.Nil error

	// Test 4: Key without expiration should still exist
	value, err = cache.Get(ctx, "key_no_expiration")
	require.NoError(t, err)
	assert.Equal(t, "value_no_expiration", value)

	// Test 5: Set with custom TTL
	err = cache.Set(ctx, "key_custom_ttl", "value_custom_ttl", 5*time.Second)
	require.NoError(t, err)

	// Verify key exists
	value, err = cache.Get(ctx, "key_custom_ttl")
	require.NoError(t, err)
	assert.Equal(t, "value_custom_ttl", value)

	// Check TTL in Redis directly
	ttl := client.TTL(ctx, "key_no_expiration").Val()
	assert.Equal(t, time.Duration(-1), ttl) // -1 means no expiration

	ttl = client.TTL(ctx, "key_custom_ttl").Val()
	assert.True(t, ttl > 0 && ttl <= 5*time.Second) // Should have TTL
}

func TestCompanyIDCacheKey(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		expected string
	}{
		{
			name:     "Standard company ID",
			id:       "comp_abc123",
			expected: "schematic:company:comp_abc123",
		},
		{
			name:     "UUID-style company ID",
			id:       "550e8400-e29b-41d4-a716-446655440000",
			expected: "schematic:company:550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:     "Empty ID",
			id:       "",
			expected: "schematic:company:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := companyIDCacheKey(tt.id)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCompanyIDCacheKey_HasThreeSegments(t *testing.T) {
	// ID-based keys must have exactly 3 colon-separated segments
	// so the cleanup manager correctly skips them
	key := companyIDCacheKey("comp_abc123")
	parts := splitCacheKey(key)
	assert.Equal(t, 3, len(parts), "ID-based key should have exactly 3 segments: %s", key)
}

func TestResourceKeyToCacheKey_HasFiveSegments(t *testing.T) {
	// Versioned lookup keys must have 5 colon-separated segments
	key := resourceKeyToCacheKey(cacheKeyPrefixCompany, "domain", "test.com")
	parts := splitCacheKey(key)
	assert.Equal(t, 5, len(parts), "Lookup key should have exactly 5 segments: %s", key)
	assert.Equal(t, rulesengine.VersionKey, parts[2], "Third segment should be the version key")
}

func splitCacheKey(key string) []string {
	var parts []string
	current := ""
	for _, c := range key {
		if c == ':' {
			parts = append(parts, current)
			current = ""
		} else {
			current += string(c)
		}
	}
	parts = append(parts, current)
	return parts
}

func TestDeleteStaleKeysFromRedis_SkipsIDBasedKeys(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()

	currentVersion := rulesengine.VersionKey
	oldVersion := "old_version_12345"

	// Populate Redis with various key types:
	// 1. Current version lookup key (should be kept)
	currentLookupKey := fmt.Sprintf("schematic:company:%s:domain:test.com", currentVersion)
	client.Set(ctx, currentLookupKey, `"comp_abc123"`, 0)

	// 2. Old version lookup key (should be deleted)
	oldLookupKey := fmt.Sprintf("schematic:company:%s:domain:test.com", oldVersion)
	client.Set(ctx, oldLookupKey, `"comp_abc123"`, 0)

	// 3. ID-based key with 3 segments (should be SKIPPED, not deleted)
	idKey := "schematic:company:comp_abc123"
	client.Set(ctx, idKey, `{"id":"comp_abc123"}`, 0)

	// Verify all keys exist
	keys, err := client.Keys(ctx, "schematic:company:*").Result()
	require.NoError(t, err)
	assert.Equal(t, 3, len(keys))

	// Run the cleanup
	logger := &mockLogger{}
	flagsCache := NewRedisCache[*rulesengine.Flag](client, 0)
	companiesCache := NewRedisCache[*rulesengine.Company](client, 0)
	usersCache := NewRedisCache[*rulesengine.User](client, 0)
	cleanupManager := NewCacheCleanupManager(flagsCache, companiesCache, usersCache, logger, 1*time.Hour)

	deletedCount, err := cleanupManager.deleteStaleKeysFromRedis(
		ctx, client, "schematic:company:*", currentVersion,
	)
	require.NoError(t, err)

	// Only the old version lookup key should be deleted
	assert.Equal(t, 1, deletedCount, "Should delete only the old version lookup key")

	// Verify current version lookup key still exists
	exists := client.Exists(ctx, currentLookupKey).Val()
	assert.Equal(t, int64(1), exists, "Current version lookup key should still exist")

	// Verify ID-based key still exists (was skipped)
	exists = client.Exists(ctx, idKey).Val()
	assert.Equal(t, int64(1), exists, "ID-based key should still exist (skipped by cleanup)")

	// Verify old version lookup key was deleted
	exists = client.Exists(ctx, oldLookupKey).Val()
	assert.Equal(t, int64(0), exists, "Old version lookup key should be deleted")
}

func TestCompanyIDBasedCaching_Integration(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()
	cacheTTL := 5 * time.Minute

	companiesCache := NewRedisBatchCache[*rulesengine.Company](client, cacheTTL)
	companyLookupCache := NewRedisBatchCache[string](client, cacheTTL)

	company := &rulesengine.Company{
		ID:            "comp_abc123",
		AccountID:     "acct_001",
		EnvironmentID: "env_001",
		Keys: map[string]string{
			"domain":     "test.com",
			"company_id": "acme-corp",
		},
	}

	// Write using the ID-based keyspace pattern
	idKey := companyIDCacheKey(company.ID)
	err = companiesCache.Set(ctx, idKey, company, cacheTTL)
	require.NoError(t, err)

	for key, value := range company.Keys {
		lookupKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
		err = companyLookupCache.Set(ctx, lookupKey, company.ID, cacheTTL)
		require.NoError(t, err)
	}

	// Verify: direct lookup by ID key
	resolvedCompany, err := companiesCache.Get(ctx, idKey)
	require.NoError(t, err)
	assert.Equal(t, company.ID, resolvedCompany.ID)
	assert.Equal(t, company.Keys, resolvedCompany.Keys)

	// Verify: two-step lookup via each key-value pair
	for key, value := range company.Keys {
		lookupKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)

		// Step 1: Get company ID from lookup key
		companyID, err := companyLookupCache.Get(ctx, lookupKey)
		require.NoError(t, err)
		assert.Equal(t, company.ID, companyID, "Lookup key %s should resolve to company ID", lookupKey)

		// Step 2: Get company from ID key
		resolvedIDKey := companyIDCacheKey(companyID)
		resolvedCompany, err := companiesCache.Get(ctx, resolvedIDKey)
		require.NoError(t, err)
		assert.Equal(t, company.ID, resolvedCompany.ID)
		assert.Equal(t, company.Keys, resolvedCompany.Keys)
	}

	// Verify: single key-value pair resolves without needing all keys
	domainLookupKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, "domain", "test.com")
	domainCompanyID, err := companyLookupCache.Get(ctx, domainLookupKey)
	require.NoError(t, err)
	assert.Equal(t, company.ID, domainCompanyID)
	domainCompany, err := companiesCache.Get(ctx, companyIDCacheKey(domainCompanyID))
	require.NoError(t, err)
	assert.Equal(t, company.ID, domainCompany.ID)

	companyIDLookupKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, "company_id", "acme-corp")
	cidCompanyID, err := companyLookupCache.Get(ctx, companyIDLookupKey)
	require.NoError(t, err)
	assert.Equal(t, company.ID, cidCompanyID)
	cidCompany, err := companiesCache.Get(ctx, companyIDCacheKey(cidCompanyID))
	require.NoError(t, err)
	assert.Equal(t, company.ID, cidCompany.ID)

	// Both lookups resolve to the same underlying company object
	assert.Equal(t, domainCompany.ID, cidCompany.ID)
	assert.Equal(t, domainCompany.Keys, cidCompany.Keys)

	// Verify the ID key is stored once, not per-lookup-key
	allKeys, err := client.Keys(ctx, "schematic:company:*").Result()
	require.NoError(t, err)

	idKeyCount := 0
	lookupKeyCount := 0
	for _, k := range allKeys {
		if k == idKey {
			idKeyCount++
		} else {
			lookupKeyCount++
		}
	}
	assert.Equal(t, 1, idKeyCount, "Should have exactly one ID-based key")
	assert.Equal(t, len(company.Keys), lookupKeyCount, "Should have one lookup key per company key")
}

func TestCompanyDelete_IDBasedKeyspace_Integration(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()
	cacheTTL := 5 * time.Minute

	companiesCache := NewRedisBatchCache[*rulesengine.Company](client, cacheTTL)
	companyLookupCache := NewRedisBatchCache[string](client, cacheTTL)

	company := &rulesengine.Company{
		ID: "comp_to_delete",
		Keys: map[string]string{
			"domain":     "delete-me.com",
			"company_id": "doomed-corp",
		},
	}

	// Write company with ID-based keyspace
	idKey := companyIDCacheKey(company.ID)
	err = companiesCache.Set(ctx, idKey, company, cacheTTL)
	require.NoError(t, err)

	var lookupKeys []string
	for key, value := range company.Keys {
		lookupKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
		lookupKeys = append(lookupKeys, lookupKey)
		err = companyLookupCache.Set(ctx, lookupKey, company.ID, cacheTTL)
		require.NoError(t, err)
	}

	// Delete: ID key via companiesCache, lookup keys via companyLookupCache
	err = companiesCache.Delete(ctx, idKey)
	require.NoError(t, err)

	for _, lookupKey := range lookupKeys {
		err = companyLookupCache.Delete(ctx, lookupKey)
		require.NoError(t, err)
	}

	// Verify all keys are gone
	allKeys, err := client.Keys(ctx, "schematic:company:*").Result()
	require.NoError(t, err)
	assert.Equal(t, 0, len(allKeys), "All company keys should be deleted")
}

func TestBatchCacheCompanies_IDBasedKeyspace_Integration(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()
	cacheTTL := 5 * time.Minute

	companiesCache := NewRedisBatchCache[*rulesengine.Company](client, cacheTTL)
	companyLookupCache := NewRedisBatchCache[string](client, cacheTTL)

	companies := []*rulesengine.Company{
		{
			ID:   "comp_1",
			Keys: map[string]string{"company_id": "alpha"},
		},
		{
			ID:   "comp_2",
			Keys: map[string]string{"company_id": "beta", "domain": "beta.com"},
		},
	}

	// Build batch maps (same pattern as async_handler.go batchCacheCompanies)
	idItems := make(map[string]*rulesengine.Company)
	lookupItems := make(map[string]string)
	for _, company := range companies {
		idItems[companyIDCacheKey(company.ID)] = company
		for key, value := range company.Keys {
			lookupKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
			lookupItems[lookupKey] = company.ID
		}
	}

	// BatchSet both maps
	err = companiesCache.BatchSet(ctx, idItems, cacheTTL)
	require.NoError(t, err)
	err = companyLookupCache.BatchSet(ctx, lookupItems, cacheTTL)
	require.NoError(t, err)

	// Verify: 2 ID keys + 3 lookup keys = 5 total
	allKeys, err := client.Keys(ctx, "schematic:company:*").Result()
	require.NoError(t, err)
	assert.Equal(t, 5, len(allKeys))

	// Verify two-step resolution for each company
	for _, company := range companies {
		for key, value := range company.Keys {
			lookupKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
			companyID, err := companyLookupCache.Get(ctx, lookupKey)
			require.NoError(t, err)
			assert.Equal(t, company.ID, companyID)

			resolved, err := companiesCache.Get(ctx, companyIDCacheKey(companyID))
			require.NoError(t, err)
			assert.Equal(t, company.ID, resolved.ID)
		}
	}
}
