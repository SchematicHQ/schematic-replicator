package main

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// defaultReplayCursorKey is the Redis key under which the replicator persists
// the stream ID of the last datastream message it processed. On reconnect it
// sends this back as ReplayFrom so the server replays only what was missed
// instead of the replicator doing a full reload. A replicator instance is
// scoped to one account, so a single fixed key suffices (override via
// REPLAY_CURSOR_KEY if multiple replicators share a Redis).
const defaultReplayCursorKey = "schematic:datastream:replay_cursor"

// ReplayCursor tracks the stream ID of the last datastream message processed.
//
// It is held in memory (authoritative while the process is alive — the common
// case is a server-side datastream reconnect, not a replicator restart) and
// flushed to Redis periodically and on shutdown so it survives a process
// restart.
//
// Recording happens at message-receive time. That is exact for the live
// reconnect case: messages arrive in stream order on one connection, and any
// not-yet-applied messages still sit in the in-memory worker queues and sort
// <= the cursor, so replaying from the cursor never drops them. Across a
// process crash the periodic flush lags the in-memory value, so replay
// re-delivers a small idempotent window that covers anything lost in flight. A
// production-hardened version may prefer to commit a low-water-mark of *applied*
// IDs instead; the server's conservative reload guard backstops either way.
type ReplayCursor struct {
	redis  redis.Cmdable
	key    string
	logger *SchematicLogger

	mu        sync.Mutex
	latest    string
	persisted string
}

// NewReplayCursor builds a cursor backed by the given Redis client.
func NewReplayCursor(redisClient redis.Cmdable, logger *SchematicLogger) *ReplayCursor {
	key := os.Getenv("REPLAY_CURSOR_KEY")
	if key == "" {
		key = defaultReplayCursorKey
	}
	return &ReplayCursor{redis: redisClient, key: key, logger: logger}
}

// Record advances the in-memory cursor to streamID. Stream IDs arrive in
// monotonic order on a single connection, so the latest received is the
// furthest the client has progressed. Empty IDs (snapshots, subscription
// confirmations) are ignored.
func (c *ReplayCursor) Record(streamID string) {
	if streamID == "" {
		return
	}
	c.mu.Lock()
	c.latest = streamID
	c.mu.Unlock()
}

// Get returns the current in-memory cursor, or "" if none recorded yet.
func (c *ReplayCursor) Get() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.latest
}

// Reset clears the cursor in memory and in Redis. Called after the server
// signals a reload so the next reconnect starts fresh rather than replaying a
// stale window.
func (c *ReplayCursor) Reset(ctx context.Context) {
	c.mu.Lock()
	c.latest = ""
	c.persisted = ""
	c.mu.Unlock()
	if c.redis == nil {
		return
	}
	if err := c.redis.Del(ctx, c.key).Err(); err != nil && c.logger != nil {
		c.logger.Warn(ctx, "Failed to clear replay cursor: "+err.Error())
	}
}

// Load reads any persisted cursor from Redis into memory on boot.
func (c *ReplayCursor) Load(ctx context.Context) {
	if c.redis == nil {
		return
	}
	val, err := c.redis.Get(ctx, c.key).Result()
	if errors.Is(err, redis.Nil) || val == "" {
		return
	}
	if err != nil {
		if c.logger != nil {
			c.logger.Warn(ctx, "Failed to load replay cursor: "+err.Error())
		}
		return
	}
	c.mu.Lock()
	c.latest = val
	c.persisted = val
	c.mu.Unlock()
	if c.logger != nil {
		c.logger.Info(ctx, "Loaded replay cursor from Redis: "+val)
	}
}

// Flush writes the in-memory cursor to Redis if it has advanced since the last
// flush.
func (c *ReplayCursor) Flush(ctx context.Context) {
	if c.redis == nil {
		return
	}
	c.mu.Lock()
	latest, persisted := c.latest, c.persisted
	c.mu.Unlock()
	if latest == "" || latest == persisted {
		return
	}
	if err := c.redis.Set(ctx, c.key, latest, 0).Err(); err != nil {
		if c.logger != nil {
			c.logger.Warn(ctx, "Failed to flush replay cursor: "+err.Error())
		}
		return
	}
	c.mu.Lock()
	c.persisted = latest
	c.mu.Unlock()
}

// StartFlusher periodically flushes the cursor until ctx is cancelled, then
// flushes once more so the latest value is durable on a clean shutdown.
func (c *ReplayCursor) StartFlusher(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				c.Flush(context.Background())
				return
			case <-ticker.C:
				c.Flush(ctx)
			}
		}
	}()
}
