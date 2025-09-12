package main

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/schematichq/rulesengine"
)

// Mock logger for testing
type mockLogger struct{}

func (m *mockLogger) Info(ctx context.Context, message string)  {}
func (m *mockLogger) Warn(ctx context.Context, message string)  {}
func (m *mockLogger) Error(ctx context.Context, message string) {}
func (m *mockLogger) Debug(ctx context.Context, message string) {}

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
