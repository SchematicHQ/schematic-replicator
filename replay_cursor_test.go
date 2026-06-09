package main

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestReplayCursor(t *testing.T) (*ReplayCursor, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return &ReplayCursor{redis: client, key: defaultReplayCursorKey, logger: NewSchematicLogger()}, mr
}

func TestReplayCursorRecordAndGet(t *testing.T) {
	c, _ := newTestReplayCursor(t)

	assert.Equal(t, "", c.Get())

	c.Record("100-0")
	assert.Equal(t, "100-0", c.Get())

	// Empty IDs (snapshots, confirmations) don't move the cursor.
	c.Record("")
	assert.Equal(t, "100-0", c.Get())

	c.Record("200-0")
	assert.Equal(t, "200-0", c.Get())
}

func TestReplayCursorFlushAndLoad(t *testing.T) {
	ctx := context.Background()
	c, mr := newTestReplayCursor(t)

	// Nothing recorded yet → nothing persisted.
	c.Flush(ctx)
	assert.False(t, mr.Exists(defaultReplayCursorKey))

	c.Record("150-3")
	c.Flush(ctx)
	got, err := mr.Get(defaultReplayCursorKey)
	require.NoError(t, err)
	assert.Equal(t, "150-3", got)

	// A fresh cursor sharing the same Redis loads the persisted value on boot.
	reloaded := &ReplayCursor{redis: c.redis, key: defaultReplayCursorKey, logger: NewSchematicLogger()}
	reloaded.Load(ctx)
	assert.Equal(t, "150-3", reloaded.Get())
}

func TestReplayCursorResetClearsMemoryAndRedis(t *testing.T) {
	ctx := context.Background()
	c, mr := newTestReplayCursor(t)

	c.Record("300-0")
	c.Flush(ctx)
	require.True(t, mr.Exists(defaultReplayCursorKey))

	c.Reset(ctx)
	assert.Equal(t, "", c.Get())
	assert.False(t, mr.Exists(defaultReplayCursorKey))
}
