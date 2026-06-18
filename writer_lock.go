package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

// The replicator is a single-writer service: exactly one instance may consume
// the datastream and write the shared cache (and the replay cursor) for a given
// Redis. Running a second write instance would corrupt the cursor and
// double-write the cache. WriterLock enforces that contract with a Redis lease:
// the write instance acquires it at startup, heartbeats it, and releases it on
// shutdown; a crashed instance's lease expires by TTL so a replacement can take
// over. (Future read-only instances do not acquire this lock.)
const (
	defaultWriterLockKey = "schematic:datastream:writer_lock"
	defaultWriterLockTTL = 15 * time.Second
	// renew at ttl/renewDivisor so a couple of refreshes can fail before expiry.
	writerLockRenewDivisor = 3
)

// errWriterLockHeld means another write instance currently holds the lock.
var errWriterLockHeld = errors.New("writer lock held by another instance")

// refreshScript extends the TTL only if we still own the lock.
const writerLockRefreshScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("pexpire", KEYS[1], ARGV[2])
else
	return 0
end`

// releaseScript deletes the lock only if we still own it (never steals another
// owner's lock).
const writerLockReleaseScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
else
	return 0
end`

// WriterLock is a single-holder Redis lease identifying the active write
// instance.
type WriterLock struct {
	redis  redis.Cmdable
	key    string
	token  string
	ttl    time.Duration
	logger *SchematicLogger
}

// NewWriterLock builds a writer lock. The key and TTL are overridable via
// WRITER_LOCK_KEY / WRITER_LOCK_TTL.
func NewWriterLock(redisClient redis.Cmdable, logger *SchematicLogger) *WriterLock {
	key := os.Getenv("WRITER_LOCK_KEY")
	if key == "" {
		key = defaultWriterLockKey
	}
	ttl := defaultWriterLockTTL
	if v := os.Getenv("WRITER_LOCK_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			ttl = d
		}
	}
	return &WriterLock{
		redis:  redisClient,
		key:    key,
		token:  newWriterLockToken(),
		ttl:    ttl,
		logger: logger,
	}
}

// newWriterLockToken returns a process-unique owner token. Hostname and PID make
// it legible in Redis; the random suffix guarantees uniqueness across restarts.
func newWriterLockToken() string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(buf))
}

// Acquire blocks until it owns the lock or waitFor elapses. While another
// instance holds the lock it retries, so a rolling deploy where the previous
// writer is still shutting down (or crashed, with a lease yet to expire) is
// smoothed over. Returns errWriterLockHeld if the lock stays held for the whole
// window.
func (l *WriterLock) Acquire(ctx context.Context, waitFor time.Duration) error {
	retry := l.ttl / writerLockRenewDivisor
	if retry < time.Second {
		retry = time.Second
	}
	start := time.Now()
	for {
		ok, err := l.redis.SetNX(ctx, l.key, l.token, l.ttl).Result()
		if err != nil {
			return fmt.Errorf("acquire writer lock: %w", err)
		}
		if ok {
			l.logger.Info(ctx, fmt.Sprintf("Acquired writer lock %q (owner %s)", l.key, l.token))
			return nil
		}
		if time.Since(start) >= waitFor {
			return errWriterLockHeld
		}
		l.logger.Warn(ctx, fmt.Sprintf("Writer lock %q held by another instance; waiting...", l.key))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retry):
		}
	}
}

// KeepAlive renews the lease until ctx is cancelled. It calls onLost exactly
// once and returns if it confirms it no longer owns the lock, or if it has been
// unable to confirm ownership for longer than the TTL (so it never keeps writing
// while another instance may have taken over). onLost should trigger shutdown.
func (l *WriterLock) KeepAlive(ctx context.Context, onLost func()) {
	interval := l.ttl / writerLockRenewDivisor
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lastConfirmed := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			owned, err := l.refresh(ctx)
			switch {
			case err == nil && owned:
				lastConfirmed = time.Now()
			case err == nil && !owned:
				l.logger.Error(ctx, fmt.Sprintf("Lost writer lock %q (taken by another instance); stepping down", l.key))
				onLost()
				return
			default: // transient Redis error
				if time.Since(lastConfirmed) >= l.ttl {
					l.logger.Error(ctx, fmt.Sprintf("Could not confirm writer lock %q for %v; stepping down: %v", l.key, l.ttl, err))
					onLost()
					return
				}
				l.logger.Warn(ctx, fmt.Sprintf("Transient error refreshing writer lock %q: %v", l.key, err))
			}
		}
	}
}

func (l *WriterLock) refresh(ctx context.Context) (bool, error) {
	res, err := l.redis.Eval(ctx, writerLockRefreshScript, []string{l.key}, l.token, l.ttl.Milliseconds()).Result()
	if err != nil {
		return false, err
	}
	n, _ := res.(int64)
	return n == 1, nil
}

// Release relinquishes the lock if still owned, so a replacement can start
// immediately on a clean shutdown.
func (l *WriterLock) Release(ctx context.Context) {
	if _, err := l.redis.Eval(ctx, writerLockReleaseScript, []string{l.key}, l.token).Result(); err != nil && l.logger != nil {
		l.logger.Warn(ctx, fmt.Sprintf("Failed to release writer lock %q: %v", l.key, err))
	}
}
