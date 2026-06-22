package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// RedisConfig holds the Redis connection settings. main populates it from the
// environment; nothing else in the package reads the env.
type RedisConfig struct {
	ClusterMode              bool
	Addr                     string   // single-node address ("" → localhost:6379)
	ClusterAddrs             []string // cluster addresses (empty → localhost:7000-7002)
	Password                 string
	DB                       int
	TLS                      bool
	MaintenanceNotifications bool // Redis Cloud/Enterprise feature; disabled unless true
}

func setupRedisClient(cfg RedisConfig) redis.Cmdable {
	if cfg.ClusterMode {
		return setupRedisCluster(cfg)
	}
	return setupRedisSingle(cfg)
}

func setupRedisSingle(cfg RedisConfig) *redis.Client {
	addr := cfg.Addr
	if addr == "" {
		addr = "localhost:6379"
	}

	opts := &redis.Options{
		Addr:     addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	}

	// Disable maintenance mode notifications by default (Redis Cloud/Enterprise feature)
	if !cfg.MaintenanceNotifications {
		opts.MaintNotificationsConfig = &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		}
	}

	if cfg.TLS {
		opts.TLSConfig = &tls.Config{}
	}

	return redis.NewClient(opts)
}

func setupRedisCluster(cfg RedisConfig) *redis.ClusterClient {
	addrs := cfg.ClusterAddrs
	if len(addrs) == 0 {
		addrs = []string{"localhost:7000", "localhost:7001", "localhost:7002"}
	}

	opts := &redis.ClusterOptions{
		Addrs:    addrs,
		Password: cfg.Password,
	}

	// Disable maintenance mode notifications by default (Redis Cloud/Enterprise feature)
	if !cfg.MaintenanceNotifications {
		opts.MaintNotificationsConfig = &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		}
	}

	if cfg.TLS {
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
