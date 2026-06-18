package main

import (
	"context"
	"strconv"
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
	return NewReplayCursor(client, NewSchematicLogger()), mr
}

// The cursor commits at apply time (Complete), not receive time (Track), so a
// crash or drop never advances it past unapplied messages.
func TestReplayCursorCommitsOnComplete(t *testing.T) {
	c, _ := newTestReplayCursor(t)

	assert.Equal(t, "", c.Get())

	// Tracking alone does not move the committed cursor.
	c.Track("100-0")
	assert.Equal(t, "", c.Get())

	// Applying it does.
	c.Complete("100-0")
	assert.Equal(t, "100-0", c.Get())

	// Empty IDs (snapshots, confirmations) are ignored.
	c.Track("")
	c.Complete("")
	assert.Equal(t, "100-0", c.Get())

	c.Track("200-0")
	c.Complete("200-0")
	assert.Equal(t, "200-0", c.Get())
}

// Messages are applied out of order across the worker pools; the cursor must
// only advance over the contiguous applied prefix.
func TestReplayCursorAdvancesOverContiguousPrefix(t *testing.T) {
	c, _ := newTestReplayCursor(t)

	c.Track("100-0")
	c.Track("200-0")
	c.Track("300-0")

	// Completing the newest first must not advance past the gaps.
	c.Complete("300-0")
	assert.Equal(t, "", c.Get())

	c.Complete("100-0")
	assert.Equal(t, "100-0", c.Get())

	// Filling the gap advances over both 200 and the already-applied 300.
	c.Complete("200-0")
	assert.Equal(t, "300-0", c.Get())
}

// A dropped (never-applied) message holds the cursor at the gap so the next
// reconnect replays from there rather than skipping it.
func TestReplayCursorHoldsAtDroppedMessage(t *testing.T) {
	c, _ := newTestReplayCursor(t)

	c.Track("100-0")
	c.Track("200-0") // this one gets dropped — never completed
	c.Track("300-0")

	c.Complete("100-0")
	c.Complete("300-0")

	// Held behind the gap; replay resumes from 100-0 and re-delivers 200-0.
	assert.Equal(t, "100-0", c.Get())

	// Replay re-delivers and applies 200-0, unblocking the prefix.
	c.Track("200-0")
	c.Complete("200-0")
	assert.Equal(t, "300-0", c.Get())
}

// Replay re-delivers already-committed IDs; they must never move the cursor
// backward.
func TestReplayCursorIgnoresAlreadyCommitted(t *testing.T) {
	c, _ := newTestReplayCursor(t)

	c.Track("200-0")
	c.Complete("200-0")
	assert.Equal(t, "200-0", c.Get())

	// Older re-delivered IDs are ignored.
	c.Track("100-0")
	c.Complete("100-0")
	assert.Equal(t, "200-0", c.Get())

	// Re-delivering the committed ID itself is a no-op.
	c.Track("200-0")
	c.Complete("200-0")
	assert.Equal(t, "200-0", c.Get())
}

// Only the committed (applied) cursor is persisted — never a merely-received ID.
func TestReplayCursorFlushPersistsCommittedOnly(t *testing.T) {
	ctx := context.Background()
	c, mr := newTestReplayCursor(t)

	// Received but not applied → nothing persisted.
	c.Track("150-3")
	c.Flush(ctx)
	assert.False(t, mr.Exists(defaultReplayCursorKey))

	c.Complete("150-3")
	c.Flush(ctx)
	got, err := mr.Get(defaultReplayCursorKey)
	require.NoError(t, err)
	assert.Equal(t, "150-3", got)

	// A fresh cursor sharing the same Redis loads the persisted value on boot.
	reloaded := NewReplayCursor(c.redis, NewSchematicLogger())
	reloaded.Load(ctx)
	assert.Equal(t, "150-3", reloaded.Get())
}

func TestReplayCursorResetClearsMemoryAndRedis(t *testing.T) {
	ctx := context.Background()
	c, mr := newTestReplayCursor(t)

	c.Track("300-0")
	c.Complete("300-0")
	c.Flush(ctx)
	require.True(t, mr.Exists(defaultReplayCursorKey))

	c.Reset(ctx)
	assert.Equal(t, "", c.Get())
	assert.False(t, mr.Exists(defaultReplayCursorKey))
}

// A stalled watermark must not grow memory without bound: once the pending set
// overflows, the cursor resets and escalates to the overflow handler.
func TestReplayCursorOverflowEscalates(t *testing.T) {
	c, _ := newTestReplayCursor(t)
	c.maxPending = 3

	var escalated int
	c.SetOverflowHandler(func() { escalated++ })

	// Track a gap (never completed) followed by enough messages to overflow.
	for i := 0; i < 4; i++ {
		c.Track("10" + strconv.Itoa(i) + "-0")
	}

	assert.Equal(t, 1, escalated, "overflow must escalate to reload")
	assert.Equal(t, "", c.Get(), "overflow resets the cursor")
}

func TestStreamIDLessOrEqual(t *testing.T) {
	assert.True(t, streamIDLessOrEqual("100-0", "100-0"))
	assert.True(t, streamIDLessOrEqual("100-0", "100-1"))
	assert.True(t, streamIDLessOrEqual("100-5", "200-0"))
	assert.False(t, streamIDLessOrEqual("200-0", "100-9"))
	assert.False(t, streamIDLessOrEqual("100-1", "100-0"))
	// Numeric, not lexicographic: "90" < "100".
	assert.True(t, streamIDLessOrEqual("90-0", "100-0"))
	// Non-conforming IDs fall back to lexicographic comparison.
	assert.True(t, streamIDLessOrEqual("abc", "abd"))
}
