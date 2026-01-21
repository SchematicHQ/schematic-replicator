package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

func setupRedisClient() redis.Cmdable {
	// Check if we're using cluster mode
	clusterMode := os.Getenv("REDIS_CLUSTER_MODE")
	if strings.ToLower(clusterMode) == "true" {
		return setupRedisCluster()
	}
	return setupRedisSingle()
}

func setupRedisSingle() *redis.Client {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	password := os.Getenv("REDIS_PASSWORD")

	dbStr := os.Getenv("REDIS_DB")
	db := 0
	if dbStr != "" {
		if parsed, err := strconv.Atoi(dbStr); err == nil {
			db = parsed
		}
	}

	opts := &redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	}

	// Disable maintenance mode notifications by default (Redis Cloud/Enterprise feature)
	if os.Getenv("REDIS_ENABLE_MAINTENANCE_NOTIFICATIONS") != "true" {
		opts.MaintNotificationsConfig = &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		}
	}

	// Optional TLS configuration
	if os.Getenv("REDIS_TLS") == "true" {
		opts.TLSConfig = &tls.Config{}
	}

	return redis.NewClient(opts)
}

func setupRedisCluster() *redis.ClusterClient {
	addrsStr := os.Getenv("REDIS_CLUSTER_ADDRS")
	if addrsStr == "" {
		addrsStr = "localhost:7000,localhost:7001,localhost:7002"
	}

	addrs := strings.Split(addrsStr, ",")
	for i, addr := range addrs {
		addrs[i] = strings.TrimSpace(addr)
	}

	password := os.Getenv("REDIS_PASSWORD")

	opts := &redis.ClusterOptions{
		Addrs:    addrs,
		Password: password,
	}

	// Disable maintenance mode notifications by default (Redis Cloud/Enterprise feature)
	if os.Getenv("REDIS_ENABLE_MAINTENANCE_NOTIFICATIONS") != "true" {
		opts.MaintNotificationsConfig = &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		}
	}

	// Optional TLS configuration
	if os.Getenv("REDIS_TLS") == "true" {
		opts.TLSConfig = &tls.Config{}
	}

	return redis.NewClusterClient(opts)
}

func testRedisConnection(client redis.Cmdable, logger *SchematicLogger) error {
	ctx := context.Background()
	err := client.Ping(ctx).Err()
	if err != nil {
		logger.Error(ctx, "Failed to connect to Redis: "+err.Error())
		return err
	}
	logger.Info(ctx, "Successfully connected to Redis")

	// Test setting and getting a key to verify operations
	testKey := "test:connection:verify"
	testValue := "connected"

	err = client.Set(ctx, testKey, testValue, time.Minute).Err()
	if err != nil {
		logger.Error(ctx, fmt.Sprintf("Failed to set test key: %v", err))
		return err
	}

	retrievedValue, err := client.Get(ctx, testKey).Result()
	if err != nil {
		logger.Error(ctx, fmt.Sprintf("Failed to get test key: %v", err))
		return err
	}

	if retrievedValue != testValue {
		logger.Error(ctx, fmt.Sprintf("Test key value mismatch: expected %s, got %s", testValue, retrievedValue))
		return fmt.Errorf("test key value mismatch")
	}

	// Clean up test key
	client.Del(ctx, testKey)

	logger.Info(ctx, "Redis connection and operations verified successfully")
	return nil
}
