package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/schematichq/rulesengine"
	schematicdatastreamws "github.com/schematichq/schematic-datastream-ws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

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
	cursor := NewReplayCursor(nil, NewSchematicLogger())
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
