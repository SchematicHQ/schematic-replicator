package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/schematichq/rulesengine"
	"github.com/schematichq/schematic-go/core"
)

// Cache key builders (matching schematic-go datastream client functionality)

// flagCacheKey generates a cache key for a flag
// Format: schematic:flags:{version}:{flagKey}
func flagCacheKey(key string) string {
	return fmt.Sprintf("%s:%s:%s:%s", cacheKeyPrefix, cacheKeyPrefixFlags, rulesengine.VersionKey, strings.ToLower(key))
}

// resourceKeyToCacheKey generates a cache key for a resource (company/user) with key-value pair
// Format: schematic:{resourceType}:{version}:{key}:{value}
func resourceKeyToCacheKey(resourceType string, key string, value string) string {
	return fmt.Sprintf("%s:%s:%s:%s:%s", cacheKeyPrefix, resourceType, rulesengine.VersionKey, strings.ToLower(key), strings.ToLower(value))
}

// CacheProvider is a generic interface for caching operations
type CacheProvider[T any] interface {
	Get(ctx context.Context, key string) (T, error)
	Set(ctx context.Context, key string, value T, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	DeleteMissing(ctx context.Context, keys []string) error
	// DeleteByPattern deletes all keys matching a pattern (Redis SCAN + DEL)
	DeleteByPattern(ctx context.Context, pattern string) (int, error)
}

// BatchCacheProvider extends CacheProvider with batch operations for better performance
type BatchCacheProvider[T any] interface {
	CacheProvider[T]
	BatchSet(ctx context.Context, items map[string]T, ttl time.Duration) error
	BatchDelete(ctx context.Context, keys []string) error
}

// redisCache is a Redis implementation of CacheProvider
type redisCache[T any] struct {
	client redis.Cmdable
	ttl    time.Duration
}

// NewRedisCache creates a new Redis cache provider
func NewRedisCache[T any](client redis.Cmdable, ttl time.Duration) CacheProvider[T] {
	return &redisCache[T]{
		client: client,
		ttl:    ttl,
	}
}

// NewRedisBatchCache creates a new Redis cache provider that implements BatchCacheProvider
func NewRedisBatchCache[T any](client redis.Cmdable, ttl time.Duration) BatchCacheProvider[T] {
	return &redisCache[T]{
		client: client,
		ttl:    ttl,
	}
}

// Get retrieves a value from Redis cache
func (r *redisCache[T]) Get(ctx context.Context, key string) (T, error) {
	var zero T

	val, err := r.client.Get(ctx, key).Result()
	if err != nil {
		return zero, err
	}

	var result T
	err = json.Unmarshal([]byte(val), &result)
	if err != nil {
		return zero, err
	}

	return result, nil
}

// Set stores a value in Redis cache with the specified TTL
func (r *redisCache[T]) Set(ctx context.Context, key string, value T, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value for key %s: %w", key, err)
	}

	// Use provided TTL, or 0 for no expiration if ttl is 0
	err = r.client.Set(ctx, key, data, ttl).Err()
	if err != nil {
		return fmt.Errorf("failed to set key %s in Redis: %w", key, err)
	}

	// Debug: Verify the key was actually set
	exists := r.client.Exists(ctx, key).Val()
	if exists != 1 {
		return fmt.Errorf("key %s was not found in Redis after setting", key)
	}

	return nil
}

// Delete removes a value from Redis cache
func (r *redisCache[T]) Delete(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

// DeleteMissing removes keys that are not in the provided list
func (r *redisCache[T]) DeleteMissing(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	// Extract pattern from first key to match entity type and version
	// For cache keys like "schematic:flags:34717044:flag_key", we want "schematic:flags:34717044:*"
	// For cache keys like "schematic:company:34717044:key:value", we want "schematic:company:34717044:*"
	pattern := ""
	if len(keys) > 0 {
		parts := strings.Split(keys[0], ":")
		if len(parts) >= 3 {
			// Build pattern: prefix:entityType:version:*
			pattern = strings.Join(parts[:3], ":") + ":*"
		}
	}

	if pattern == "" {
		return nil
	}

	// Get all existing keys matching the pattern
	existingKeys, err := r.client.Keys(ctx, pattern).Result()
	if err != nil {
		return err
	}

	// Create a map for fast lookup of keys to keep
	keepKeys := make(map[string]bool)
	for _, key := range keys {
		keepKeys[key] = true
	}

	// Find keys to delete
	var keysToDelete []string
	for _, existingKey := range existingKeys {
		if !keepKeys[existingKey] {
			keysToDelete = append(keysToDelete, existingKey)
		}
	}

	if len(keysToDelete) > 0 {
		var deletionErrors []error

		for _, key := range keysToDelete {
			if err := r.client.Del(ctx, key).Err(); err != nil {
				deletionErrors = append(deletionErrors, fmt.Errorf("failed to delete key %s: %w", key, err))
			}
		}

		// Return first error if any occurred, but continue processing all keys
		if len(deletionErrors) > 0 {
			return fmt.Errorf("failed to delete %d/%d keys, first error: %w", len(deletionErrors), len(keysToDelete), deletionErrors[0])
		}
	}

	return nil
}

// DeleteByPattern deletes all keys matching a pattern and returns the count of deleted keys
func (r *redisCache[T]) DeleteByPattern(ctx context.Context, pattern string) (int, error) {
	// Use SCAN to find all keys matching the pattern (more efficient than KEYS for large datasets)
	var cursor uint64
	var allKeys []string

	for {
		keys, nextCursor, err := r.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return 0, fmt.Errorf("failed to scan keys with pattern %s: %w", pattern, err)
		}

		allKeys = append(allKeys, keys...)
		cursor = nextCursor

		if cursor == 0 {
			break
		}
	}

	// Delete keys in batches
	if len(allKeys) > 0 {
		err := r.client.Del(ctx, allKeys...).Err()
		if err != nil {
			return 0, fmt.Errorf("failed to delete keys matching pattern %s: %w", pattern, err)
		}
	}

	return len(allKeys), nil
}

// BatchSet implements BatchCacheProvider by using Redis pipelining for better performance
func (r *redisCache[T]) BatchSet(ctx context.Context, items map[string]T, ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}

	pipe := r.client.Pipeline()

	for key, value := range items {
		jsonData, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal value for key %s: %w", key, err)
		}

		if ttl > 0 {
			pipe.SetEx(ctx, key, jsonData, ttl)
		} else {
			pipe.Set(ctx, key, jsonData, 0)
		}
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute batch set: %w", err)
	}

	return nil
}

// BatchDelete implements BatchCacheProvider by using Redis pipelining for better performance
func (r *redisCache[T]) BatchDelete(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	pipe := r.client.Pipeline()

	for _, key := range keys {
		pipe.Del(ctx, key)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute batch delete: %w", err)
	}

	return nil
}

// CacheCleanupManager handles periodic cleanup of stale cache entries
type CacheCleanupManager struct {
	flagsCache     CacheProvider[*rulesengine.Flag]
	companiesCache CacheProvider[*rulesengine.Company]
	usersCache     CacheProvider[*rulesengine.User]
	logger         core.Logger
	cleanupTicker  *time.Ticker
	stopChan       chan struct{}
}

// NewCacheCleanupManager creates a new cache cleanup manager
func NewCacheCleanupManager(
	flagsCache CacheProvider[*rulesengine.Flag],
	companiesCache CacheProvider[*rulesengine.Company],
	usersCache CacheProvider[*rulesengine.User],
	logger core.Logger,
	cleanupInterval time.Duration,
) *CacheCleanupManager {
	return &CacheCleanupManager{
		flagsCache:     flagsCache,
		companiesCache: companiesCache,
		usersCache:     usersCache,
		logger:         logger,
		cleanupTicker:  time.NewTicker(cleanupInterval),
		stopChan:       make(chan struct{}),
	}
}

// Start begins the periodic cache cleanup process
func (c *CacheCleanupManager) Start(ctx context.Context) {
	go func() {
		c.logger.Info(ctx, "Starting cache cleanup manager")
		defer c.logger.Info(ctx, "Cache cleanup manager stopped")

		for {
			select {
			case <-c.cleanupTicker.C:
				if err := c.cleanupStaleEntries(ctx); err != nil {
					c.logger.Error(ctx, fmt.Sprintf("Cache cleanup failed: %v", err))
				}
			case <-c.stopChan:
				return
			}
		}
	}()
}

// Stop stops the periodic cache cleanup process
func (c *CacheCleanupManager) Stop() {
	c.cleanupTicker.Stop()
	close(c.stopChan)
}

// cleanupStaleEntries removes cache entries with outdated version keys
func (c *CacheCleanupManager) cleanupStaleEntries(ctx context.Context) error {
	currentVersion := rulesengine.VersionKey

	// Define patterns for different cache entry types with old versions
	// We'll scan for all entries and delete those not matching current version
	patterns := []struct {
		name    string
		pattern string
		cache   interface{}
	}{
		{
			name:    "flags",
			pattern: fmt.Sprintf("%s:%s:*", cacheKeyPrefix, cacheKeyPrefixFlags),
			cache:   c.flagsCache,
		},
		{
			name:    "companies",
			pattern: fmt.Sprintf("%s:%s:*", cacheKeyPrefix, cacheKeyPrefixCompany),
			cache:   c.companiesCache,
		},
		{
			name:    "users",
			pattern: fmt.Sprintf("%s:%s:*", cacheKeyPrefix, cacheKeyPrefixUser),
			cache:   c.usersCache,
		},
	}

	totalDeleted := 0

	for _, p := range patterns {
		// Create pattern to match stale entries (all except current version)
		stalePattern := fmt.Sprintf("%s:%s:*", cacheKeyPrefix, p.name)

		// Clean up stale entries for this cache type
		deletedCount, err := c.deleteStaleKeysForCache(ctx, stalePattern, currentVersion)

		if err != nil {
			c.logger.Error(ctx, fmt.Sprintf("Failed to cleanup %s cache: %v", p.name, err))
			continue
		}

		if deletedCount > 0 {
			c.logger.Info(ctx, fmt.Sprintf("Cleaned up %d stale %s cache entries", deletedCount, p.name))
			totalDeleted += deletedCount
		}
	}

	if totalDeleted > 0 {
		c.logger.Info(ctx, fmt.Sprintf("Cache cleanup completed: removed %d stale entries total", totalDeleted))
	} else {
		c.logger.Debug(ctx, "Cache cleanup completed: no stale entries found")
	}

	return nil
}

// deleteStaleKeysForCache deletes stale keys for different cache types
func (c *CacheCleanupManager) deleteStaleKeysForCache(
	ctx context.Context,
	pattern string,
	currentVersion string,
) (int, error) {
	// Get Redis client from any of the caches (they should all be Redis)
	var client redis.Cmdable

	// Cast to get access to Redis client - try flags cache first
	if redisCache, ok := c.flagsCache.(*redisCache[*rulesengine.Flag]); ok {
		client = redisCache.client
	} else {
		c.logger.Warn(ctx, "Cache cleanup not supported for non-Redis cache implementations")
		return 0, nil
	}

	return c.deleteStaleKeysFromRedis(ctx, client, pattern, currentVersion)
}

// deleteStaleKeysFromRedis scans Redis for keys and deletes those with old versions
func (c *CacheCleanupManager) deleteStaleKeysFromRedis(
	ctx context.Context,
	client redis.Cmdable,
	pattern string,
	currentVersion string,
) (int, error) {
	var cursor uint64
	var staleKeys []string

	for {
		keys, nextCursor, err := client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return 0, fmt.Errorf("failed to scan keys: %w", err)
		}

		// Filter keys to find stale ones (not matching current version)
		for _, key := range keys {
			parts := strings.Split(key, ":")
			if len(parts) >= 3 {
				keyVersion := parts[2] // Version is the 3rd part: schematic:type:version:...
				if keyVersion != currentVersion {
					staleKeys = append(staleKeys, key)
				}
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	// Delete stale keys in batches
	if len(staleKeys) > 0 {
		err := client.Del(ctx, staleKeys...).Err()
		if err != nil {
			return 0, fmt.Errorf("failed to delete stale keys: %w", err)
		}
	}

	return len(staleKeys), nil
}
