package main

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// defaultReplayCursorKey is the Redis key under which the replicator persists
// the stream ID of the last datastream message it has durably applied. On
// reconnect it sends this back as ReplayFrom so the server replays only what
// was missed instead of the replicator doing a full reload. A replicator
// instance is scoped to one account, so a single fixed key suffices (override
// via REPLAY_CURSOR_KEY if multiple replicators share a Redis).
const defaultReplayCursorKey = "schematic:datastream:replay_cursor"

// defaultMaxPendingStreamIDs bounds the in-flight tracking set. It is far above
// the normal in-flight window (channel capacities + batch sizes); exceeding it
// means a message was received but never applied (a backpressure drop or a
// sustained apply failure) and the contiguous watermark has stalled. At that
// point narrow replay can no longer catch us up, so we escalate to a full
// reload rather than grow memory without bound.
const defaultMaxPendingStreamIDs = 50_000

// ReplayCursor tracks the stream ID of the last datastream message that has
// been durably applied to the cache, as a contiguous low-water-mark.
//
// Messages are received in stream order on a single connection (Track), but
// applied out of order across the company/user/flags worker pools (Complete).
// The committed cursor only advances over the contiguous prefix of applied
// messages, so it is never ahead of cache state: a crash or a dropped message
// leaves its stream ID uncommitted, and the next reconnect replays from the
// last *applied* position rather than skipping the gap.
//
// The committed value is held in memory (authoritative while the process is
// alive — the common case is a server-side datastream reconnect, not a
// replicator restart) and flushed to Redis periodically and on shutdown so it
// survives a process restart.
type ReplayCursor struct {
	redis  redis.Cmdable
	key    string
	logger *SchematicLogger

	maxPending int
	onOverflow func() // escalation when the watermark stalls (e.g. trigger reload); optional

	mu        sync.Mutex
	committed string          // highest contiguously-applied stream ID; replayed from and persisted
	persisted string          // last value flushed to Redis
	order     []string        // tracked-but-not-yet-committed stream IDs, in receive order
	applied   map[string]bool // stream ID -> applied yet?
}

// NewReplayCursor builds a cursor backed by the given Redis client.
// NewReplayCursor builds a cursor backed by the given Redis client. key is the
// Redis key to persist the cursor under; pass "" to use defaultReplayCursorKey.
// (Reading the override from the environment is main's responsibility.)
func NewReplayCursor(redisClient redis.Cmdable, logger *SchematicLogger, key string) *ReplayCursor {
	if key == "" {
		key = defaultReplayCursorKey
	}
	return &ReplayCursor{
		redis:      redisClient,
		key:        key,
		logger:     logger,
		maxPending: defaultMaxPendingStreamIDs,
		applied:    make(map[string]bool),
	}
}

// SetOverflowHandler wires the action taken when the in-flight tracking set
// exceeds maxPending (the contiguous watermark has stalled and narrow replay
// can no longer recover). Typically a full reload trigger.
func (c *ReplayCursor) SetOverflowHandler(fn func()) {
	c.mu.Lock()
	c.onOverflow = fn
	c.mu.Unlock()
}

// Track records that a message with streamID has been received and is in
// flight. It must be called in stream order (the datastream client delivers
// messages serially). Empty IDs (snapshots, subscription confirmations) and
// already-applied/older IDs (replay re-delivery) are ignored.
func (c *ReplayCursor) Track(streamID string) {
	if streamID == "" {
		return
	}

	var overflow func()
	c.mu.Lock()
	if _, exists := c.applied[streamID]; exists {
		c.mu.Unlock() // already in flight (duplicate delivery)
		return
	}
	if c.committed != "" && streamIDLessOrEqual(streamID, c.committed) {
		c.mu.Unlock() // already committed (replay re-delivery); would move the cursor backward
		return
	}
	c.applied[streamID] = false
	c.order = append(c.order, streamID)
	if len(c.order) > c.maxPending {
		overflow = c.onOverflow
		c.resetLocked()
	}
	c.mu.Unlock()

	if overflow != nil {
		if c.logger != nil {
			c.logger.Warn(context.Background(), "Replay cursor pending set overflowed; escalating to full reload")
		}
		overflow()
	}
}

// Complete records that the message with streamID has been applied to the
// cache, advancing the committed cursor over the contiguous prefix of applied
// messages. Safe to call concurrently from the worker pools.
func (c *ReplayCursor) Complete(streamID string) {
	if streamID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.applied[streamID]; !exists {
		return // not tracked (already committed, or never tracked)
	}
	c.applied[streamID] = true
	// Advance over the contiguous run of applied messages at the front.
	i := 0
	for i < len(c.order) && c.applied[c.order[i]] {
		c.committed = c.order[i]
		delete(c.applied, c.order[i])
		i++
	}
	if i > 0 {
		// Reslice into a fresh backing array so the consumed prefix is freed.
		c.order = append([]string(nil), c.order[i:]...)
	}
}

// Get returns the committed low-water-mark, or "" if nothing applied yet.
func (c *ReplayCursor) Get() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.committed
}

// Reset clears the cursor in memory and in Redis. Called after the server
// signals a reload so the next reconnect starts fresh rather than replaying a
// stale window.
func (c *ReplayCursor) Reset(ctx context.Context) {
	c.mu.Lock()
	c.resetLocked()
	c.mu.Unlock()
	if c.redis == nil {
		return
	}
	if err := c.redis.Del(ctx, c.key).Err(); err != nil && c.logger != nil {
		c.logger.Warn(ctx, "Failed to clear replay cursor: "+err.Error())
	}
}

// resetLocked clears in-memory state. Caller must hold c.mu.
func (c *ReplayCursor) resetLocked() {
	c.committed = ""
	c.persisted = ""
	c.order = nil
	c.applied = make(map[string]bool)
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
	c.committed = val
	c.persisted = val
	c.mu.Unlock()
	if c.logger != nil {
		c.logger.Info(ctx, "Loaded replay cursor from Redis: "+val)
	}
}

// Flush writes the committed cursor to Redis if it has advanced since the last
// flush.
func (c *ReplayCursor) Flush(ctx context.Context) {
	if c.redis == nil {
		return
	}
	c.mu.Lock()
	committed, persisted := c.committed, c.persisted
	c.mu.Unlock()
	if committed == "" || committed == persisted {
		return
	}
	if err := c.redis.Set(ctx, c.key, committed, 0).Err(); err != nil {
		if c.logger != nil {
			c.logger.Warn(ctx, "Failed to flush replay cursor: "+err.Error())
		}
		return
	}
	c.mu.Lock()
	c.persisted = committed
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

// streamIDLessOrEqual reports whether stream ID a sorts at or before b. Redis
// stream IDs have the form "<millis>-<seq>"; both parts are compared
// numerically. IDs that don't parse fall back to lexicographic comparison.
func streamIDLessOrEqual(a, b string) bool {
	if a == b {
		return true
	}
	aMs, aSeq, aOK := parseStreamID(a)
	bMs, bSeq, bOK := parseStreamID(b)
	if !aOK || !bOK {
		return a <= b
	}
	if aMs != bMs {
		return aMs < bMs
	}
	return aSeq <= bSeq
}

func parseStreamID(id string) (ms, seq uint64, ok bool) {
	left, right, found := strings.Cut(id, "-")
	if !found {
		return 0, 0, false
	}
	ms, err := strconv.ParseUint(left, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	seq, err = strconv.ParseUint(right, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return ms, seq, true
}
