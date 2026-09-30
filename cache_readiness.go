package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// defaultLoadCompleteKey is the Redis key recording that an initial load
// (flags, companies and users) has completed. Its value is the rules engine
// cache version the load was written under, so a marker left by a build with a
// different version never makes the new, empty key space look complete. It
// lives outside the schematic:{flags,company,user}:* namespaces, so cache
// cleanup and DeleteMissing never touch it.
const defaultLoadCompleteKey = "schematic:datastream:load_complete"

// CacheReadiness tracks whether the Redis cache is complete and servable for
// the current cache version. This is what /health reports as `ready`, and what
// SDKs in replicator mode gate on.
//
// It becomes ready once, in this process, both the bulk flags snapshot and the
// company/user load have been applied, or at startup when Redis already holds a
// load-complete marker for the current version (a restart with a warm cache).
// Once ready it stays ready: a datastream disconnect or a reload leaves the
// cache populated, so SDKs can keep evaluating from it. The state is held in
// memory so the health handler never waits on Redis.
//
// All methods are nil-safe, so handlers built without one (tests) need no
// guards.
type CacheReadiness struct {
	redis   redis.Cmdable
	key     string
	version string
	ttl     time.Duration // marker TTL; matches the cache TTL (0 = no expiry)
	logger  *SchematicLogger

	ready atomic.Bool

	mu             sync.Mutex
	flagsLoaded    bool // bulk flags snapshot applied in this process
	entitiesLoaded bool // company and user bulk load completed in this process
}

// NewCacheReadiness builds a tracker for the given cache version. ttl should be
// the cache TTL, so the marker does not outlive the entries it vouches for.
func NewCacheReadiness(redisClient redis.Cmdable, logger *SchematicLogger, version string, ttl time.Duration) *CacheReadiness {
	return &CacheReadiness{
		redis:   redisClient,
		key:     defaultLoadCompleteKey,
		version: version,
		ttl:     ttl,
		logger:  logger,
	}
}

// Load reads the persisted marker and reports ready if it was written for the
// current cache version. It only ever moves readiness from false to true.
func (r *CacheReadiness) Load(ctx context.Context) {
	if r == nil || r.redis == nil {
		return
	}
	val, err := r.redis.Get(ctx, r.key).Result()
	if errors.Is(err, redis.Nil) {
		return
	}
	if err != nil {
		if r.logger != nil {
			r.logger.Warn(ctx, "Failed to load cache load-complete marker: "+err.Error())
		}
		return
	}
	if val != r.version {
		if r.logger != nil {
			r.logger.Info(ctx, "Cache load-complete marker is for cache version "+val+", current is "+r.version+"; waiting for a fresh initial load")
		}
		return
	}
	if !r.ready.Swap(true) && r.logger != nil {
		r.logger.Info(ctx, "Cache load-complete marker found for cache version "+r.version+"; reporting ready")
	}
}

// IsReady reports whether the cache is complete for the current cache version.
func (r *CacheReadiness) IsReady() bool {
	return r != nil && r.ready.Load()
}

// MarkFlagsLoaded records that a bulk flags snapshot has been applied.
func (r *CacheReadiness) MarkFlagsLoaded(ctx context.Context) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.flagsLoaded = true
	r.mu.Unlock()
	r.maybeComplete(ctx)
}

// MarkEntitiesLoaded records that a company and user bulk load has completed
// successfully.
func (r *CacheReadiness) MarkEntitiesLoaded(ctx context.Context) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.entitiesLoaded = true
	r.mu.Unlock()
	r.maybeComplete(ctx)
}

// maybeComplete persists the marker and reports ready once both halves of the
// load have landed. Later completions (a reconnect's flags snapshot, a reload)
// rewrite the marker, which refreshes its TTL when one is set.
func (r *CacheReadiness) maybeComplete(ctx context.Context) {
	r.mu.Lock()
	complete := r.flagsLoaded && r.entitiesLoaded
	r.mu.Unlock()
	if !complete {
		return
	}

	if r.redis != nil {
		if err := r.redis.Set(ctx, r.key, r.version, r.ttl).Err(); err != nil && r.logger != nil {
			// The cache is still complete; only survival across a restart is lost
			// until the next completion rewrites the marker.
			r.logger.Warn(ctx, "Failed to persist cache load-complete marker: "+err.Error())
		}
	}
	if !r.ready.Swap(true) && r.logger != nil {
		r.logger.Info(ctx, "Initial load complete for cache version "+r.version+"; reporting ready")
	}
}

// discardUnvouchedReplayCursor resets a persisted replay cursor unless the
// cache is known complete for the current cache version. Resuming from a
// cursor skips the bulk load, which is only safe on a complete cache. A cursor
// without a current marker means the last initial load never finished (the
// cursor advances on live updates applied during the load), or the cache was
// written under another cache version, so the replicator must do a full load.
// Reports whether it discarded the cursor.
func discardUnvouchedReplayCursor(ctx context.Context, cursor *ReplayCursor, readiness *CacheReadiness, logger *SchematicLogger) bool {
	if cursor == nil || cursor.Get() == "" || readiness.IsReady() {
		return false
	}
	if logger != nil {
		logger.Info(ctx, "Replay cursor present but no completed initial load for this cache version; discarding cursor and doing a full load")
	}
	cursor.Reset(ctx)
	return true
}
