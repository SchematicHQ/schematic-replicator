package main

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/schematichq/rulesengine"
	schematicdatastreamws "github.com/schematichq/schematic-datastream-ws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func newTestReplayCursor(t *testing.T) (*ReplayCursor, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewReplayCursor(client, NewSchematicLogger(), ""), mr
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
	reloaded := NewReplayCursor(c.redis, NewSchematicLogger(), "")
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

// When replay is disabled (REPLAY_DISABLED=true), main wires no cursor onto the
// connection-ready handler, so it must never set ReplayFrom and the server falls
// back to its pre-replay full-reload behavior. This verifies that invariant
// (nil cursor → nil ReplayFrom) alongside the enabled path.
func TestReplayFromReflectsCursorState(t *testing.T) {
	h := &AsyncConnectionReadyHandler{}

	// Replay disabled: no cursor → no ReplayFrom.
	assert.Nil(t, h.replayFrom(), "no cursor must yield no ReplayFrom")

	// Cursor attached but nothing committed yet → still no ReplayFrom (fresh
	// start → full load).
	cursor := NewReplayCursor(nil, NewSchematicLogger(), "")
	h.replayCursor = cursor
	assert.Nil(t, h.replayFrom(), "uncommitted cursor must yield no ReplayFrom")

	// Once a message is applied, ReplayFrom carries the committed position.
	cursor.Track("100-0")
	cursor.Complete("100-0")
	got := h.replayFrom()
	require.NotNil(t, got)
	assert.Equal(t, "100-0", *got)
}

// newApplyHandler builds a handler with a mock company cache and an in-memory
// replay cursor, for testing which messages advance the cursor.
func newApplyHandler(t *testing.T) (*AsyncReplicatorMessageHandler, *MockBatchCacheProvider[*rulesengine.Company], *ReplayCursor) {
	t.Helper()
	companyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
	companyLookupCache := NewMockBatchCacheProvider[string]()
	companyLookupCache.On("BatchSet", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	companyLookupCache.On("BatchDelete", mock.Anything, mock.Anything).Return(nil)

	h := NewAsyncReplicatorMessageHandler(
		companyCache,
		NewMockBatchCacheProvider[*rulesengine.User](),
		NewMockCacheProvider[*rulesengine.Flag](),
		companyLookupCache,
		NewMockBatchCacheProvider[string](),
		NewSchematicLogger(),
		5*time.Minute,
		createTestAsyncConfig(),
	)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		h.Shutdown(ctx)
	})
	cursor := NewReplayCursor(nil, NewSchematicLogger(), "")
	h.SetReplayCursor(cursor)
	return h, companyCache, cursor
}

func partialCompanyJob(id, streamID string) *MessageJob {
	eid, sid := id, streamID
	return &MessageJob{
		Message: &schematicdatastreamws.DataStreamResp{
			EntityType:  string(schematicdatastreamws.EntityTypeCompany),
			MessageType: schematicdatastreamws.MessageTypePartial,
			Data:        json.RawMessage(`{"base_plan_id":"plan_x"}`),
			EntityID:    &eid,
			StreamID:    &sid,
		},
		EntityKey: id,
	}
}

func fullCompanyJob(t *testing.T, id, streamID, basePlan string) *MessageJob {
	t.Helper()
	c := &rulesengine.Company{ID: id, Keys: map[string]string{"key": "val-" + id}}
	if basePlan != "" {
		c.BasePlanID = &basePlan
	}
	data, err := json.Marshal(c)
	require.NoError(t, err)
	eid, sid := id, streamID
	return &MessageJob{
		Message: &schematicdatastreamws.DataStreamResp{
			EntityType:  string(schematicdatastreamws.EntityTypeCompany),
			MessageType: schematicdatastreamws.MessageTypeFull,
			Data:        data,
			EntityID:    &eid,
			StreamID:    &sid,
		},
		EntityKey: id,
	}
}

// A partial for a genuinely-absent entity is a correct no-op and must advance
// the cursor (it isn't something replay can recover, and the missing base, if it
// should exist, holds the cursor at its own slot).
func TestPartialGenuineMissCommitsAsNoOp(t *testing.T) {
	h, companyCache, cursor := newApplyHandler(t)
	companyCache.On("Get", mock.Anything, mock.Anything).Return((*rulesengine.Company)(nil), redis.Nil)

	cursor.Track("100-0")
	h.processBatchedCompanyMessages(context.Background(), []*MessageJob{partialCompanyJob("c1", "100-0")})

	assert.Equal(t, "100-0", cursor.Get(), "a genuine miss must not stall the cursor")
}

// A partial whose base read fails transiently (cache present but unreadable)
// must NOT advance the cursor, so the next reconnect's replay re-delivers it.
func TestPartialReadErrorHoldsCursor(t *testing.T) {
	h, companyCache, cursor := newApplyHandler(t)
	companyCache.On("Get", mock.Anything, mock.Anything).Return((*rulesengine.Company)(nil), errors.New("redis unavailable"))

	cursor.Track("100-0")
	h.processBatchedCompanyMessages(context.Background(), []*MessageJob{partialCompanyJob("c1", "100-0")})

	assert.Equal(t, "", cursor.Get(), "a transient read error must hold the cursor for replay")
}

// A failed cache write must hold the cursor.
func TestWriteFailureHoldsCursor(t *testing.T) {
	h, companyCache, cursor := newApplyHandler(t)
	companyCache.On("Get", mock.Anything, mock.Anything).Return((*rulesengine.Company)(nil), redis.Nil)
	companyCache.On("BatchSet", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("write failed"))

	cursor.Track("100-0")
	h.processBatchedCompanyMessages(context.Background(), []*MessageJob{fullCompanyJob(t, "c1", "100-0", "v1")})

	assert.Equal(t, "", cursor.Get(), "a failed cache write must hold the cursor for replay")
}

// A read failure followed by a full for the same entity in the same batch is
// superseded by the full and commits.
func TestReadErrorThenFullCommits(t *testing.T) {
	h, companyCache, cursor := newApplyHandler(t)
	companyCache.On("Get", mock.Anything, mock.Anything).Return((*rulesengine.Company)(nil), errors.New("blip"))
	companyCache.On("BatchSet", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	cursor.Track("100-0")
	cursor.Track("200-0")
	h.processBatchedCompanyMessages(context.Background(), []*MessageJob{
		partialCompanyJob("c1", "100-0"),
		fullCompanyJob(t, "c1", "200-0", "v1"),
	})

	assert.Equal(t, "200-0", cursor.Get(), "a later full supersedes the read failure and commits the batch")
}

// Drives a message through HandleMessage (Track) and batch processing
// (Complete) to prove the cursor commits end-to-end only after apply.
func TestReplayCursorCommitsThroughHandler(t *testing.T) {
	logger := NewSchematicLogger()
	companyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
	userCache := NewMockBatchCacheProvider[*rulesengine.User]()
	flagCache := NewMockCacheProvider[*rulesengine.Flag]()
	companyLookupCache := NewMockBatchCacheProvider[string]()
	userLookupCache := NewMockBatchCacheProvider[string]()

	companyCache.On("Get", mock.Anything, mock.Anything).Return((*rulesengine.Company)(nil), redis.Nil)
	companyCache.On("BatchSet", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	companyLookupCache.On("BatchSet", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	handler := NewAsyncReplicatorMessageHandler(
		companyCache, userCache, flagCache, companyLookupCache, userLookupCache,
		logger, 5*time.Minute, createTestAsyncConfig(),
	)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		handler.Shutdown(ctx)
	}()

	cursor := NewReplayCursor(nil, logger, "") // in-memory only; no Redis needed
	handler.SetReplayCursor(cursor)

	company := createAsyncTestCompany(t)
	companyData, err := json.Marshal(company)
	require.NoError(t, err)

	ctx := context.Background()
	streamIDs := []string{"100-0", "200-0"} // BatchSize is 2 in the test config
	for _, sid := range streamIDs {
		msg := &schematicdatastreamws.DataStreamResp{
			EntityType:  string(schematicdatastreamws.EntityTypeCompany),
			MessageType: schematicdatastreamws.MessageTypeFull,
			Data:        companyData,
			StreamID:    &sid,
		}
		require.NoError(t, handler.HandleMessage(ctx, msg))
	}

	// Cursor must not commit until the batch is applied, then commit to the last.
	require.Eventually(t, func() bool { return cursor.Get() == "200-0" }, time.Second, 5*time.Millisecond,
		"cursor should commit to the last applied stream ID")
}
