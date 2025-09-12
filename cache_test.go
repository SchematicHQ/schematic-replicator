package main

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
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
