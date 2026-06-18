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

func newTestWriterLock(t *testing.T, mr *miniredis.Miniredis, ttl time.Duration) *WriterLock {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return &WriterLock{
		redis:  client,
		key:    defaultWriterLockKey,
		token:  newWriterLockToken(),
		ttl:    ttl,
		logger: NewSchematicLogger(),
	}
}

func TestWriterLockAcquireBlocksSecondInstance(t *testing.T) {
	ctx := context.Background()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	first := newTestWriterLock(t, mr, 15*time.Second)
	require.NoError(t, first.Acquire(ctx, time.Second))

	// A second instance can't acquire while the first holds it.
	second := newTestWriterLock(t, mr, 15*time.Second)
	err = second.Acquire(ctx, 200*time.Millisecond)
	assert.ErrorIs(t, err, errWriterLockHeld)

	// After the first releases, the second can take over.
	first.Release(ctx)
	require.NoError(t, second.Acquire(ctx, time.Second))
}

func TestWriterLockReleaseOnlyByOwner(t *testing.T) {
	ctx := context.Background()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	owner := newTestWriterLock(t, mr, 15*time.Second)
	require.NoError(t, owner.Acquire(ctx, time.Second))

	// A non-owner releasing must not clear the owner's lock.
	other := newTestWriterLock(t, mr, 15*time.Second)
	other.Release(ctx)
	assert.True(t, mr.Exists(defaultWriterLockKey), "non-owner release must not delete the lock")

	owner.Release(ctx)
	assert.False(t, mr.Exists(defaultWriterLockKey))
}

func TestWriterLockRefreshExtendsOwnedLease(t *testing.T) {
	ctx := context.Background()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	l := newTestWriterLock(t, mr, 15*time.Second)
	require.NoError(t, l.Acquire(ctx, time.Second))

	owned, err := l.refresh(ctx)
	require.NoError(t, err)
	assert.True(t, owned)

	// Once the key is gone (e.g. expired / taken over), refresh reports not-owned.
	mr.Del(defaultWriterLockKey)
	owned, err = l.refresh(ctx)
	require.NoError(t, err)
	assert.False(t, owned)
}

func TestWriterLockKeepAliveStepsDownWhenLost(t *testing.T) {
	ctx := context.Background()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	l := newTestWriterLock(t, mr, 3*time.Second) // renew interval = 1s
	require.NoError(t, l.Acquire(ctx, time.Second))

	lost := make(chan struct{})
	keepCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go l.KeepAlive(keepCtx, func() { close(lost) })

	// Simulate another instance taking over.
	mr.Set(defaultWriterLockKey, "someone-else")

	select {
	case <-lost:
		// stepped down as required
	case <-time.After(3 * time.Second):
		t.Fatal("KeepAlive did not step down after losing the lock")
	}
}
