package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/schematichq/rulesengine"
	schematicdatastreamws "github.com/schematichq/schematic-datastream-ws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func newOrderingHandler(t *testing.T) (*AsyncReplicatorMessageHandler, *MockBatchCacheProvider[*rulesengine.Company]) {
	t.Helper()
	companyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
	companyLookupCache := NewMockBatchCacheProvider[string]()
	// Permissive lookup-cache expectations; the ID cache is asserted per test.
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
	return h, companyCache
}

func companyJob(t *testing.T, id string, mt schematicdatastreamws.MessageType, basePlan string) *MessageJob {
	t.Helper()
	c := &rulesengine.Company{ID: id, Keys: map[string]string{"key": "val-" + id}}
	if basePlan != "" {
		c.BasePlanID = &basePlan
	}
	data, err := json.Marshal(c)
	require.NoError(t, err)
	eid := id
	return &MessageJob{
		Message: &schematicdatastreamws.DataStreamResp{
			EntityType:  string(schematicdatastreamws.EntityTypeCompany),
			MessageType: mt,
			Data:        data,
			EntityID:    &eid,
		},
		EntityKey: id,
	}
}

// Two fulls for the same company in one batch must collapse to the last one,
// not be applied in arbitrary order.
func TestCompanyBatchCollapsesToLastFull(t *testing.T) {
	h, companyCache := newOrderingHandler(t)

	var cached map[string]*rulesengine.Company
	companyCache.On("BatchSet", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { cached = args.Get(1).(map[string]*rulesengine.Company) }).
		Return(nil)

	jobs := []*MessageJob{
		companyJob(t, "c1", schematicdatastreamws.MessageTypeFull, "v1"),
		companyJob(t, "c1", schematicdatastreamws.MessageTypeFull, "v2"),
	}
	h.processBatchedCompanyMessages(context.Background(), jobs)

	companyCache.AssertNotCalled(t, "BatchDelete", mock.Anything, mock.Anything)
	require.Len(t, cached, 1)
	got := cached[companyIDCacheKey("c1")]
	require.NotNil(t, got)
	require.NotNil(t, got.BasePlanID)
	assert.Equal(t, "v2", *got.BasePlanID, "last full in the batch must win")
}

// full → delete in one batch must leave the company deleted, not cached.
func TestCompanyBatchFullThenDeleteDeletes(t *testing.T) {
	h, companyCache := newOrderingHandler(t)
	companyCache.On("BatchDelete", mock.Anything, mock.Anything).Return(nil)

	jobs := []*MessageJob{
		companyJob(t, "c1", schematicdatastreamws.MessageTypeFull, "v1"),
		companyJob(t, "c1", schematicdatastreamws.MessageTypeDelete, ""),
	}
	h.processBatchedCompanyMessages(context.Background(), jobs)

	companyCache.AssertNotCalled(t, "BatchSet", mock.Anything, mock.Anything, mock.Anything)
	companyCache.AssertCalled(t, "BatchDelete", mock.Anything, mock.Anything)
}

// delete → full in one batch (re-create) must leave the company present.
func TestCompanyBatchDeleteThenFullCaches(t *testing.T) {
	h, companyCache := newOrderingHandler(t)
	companyCache.On("BatchSet", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	jobs := []*MessageJob{
		companyJob(t, "c1", schematicdatastreamws.MessageTypeDelete, ""),
		companyJob(t, "c1", schematicdatastreamws.MessageTypeFull, "v2"),
	}
	h.processBatchedCompanyMessages(context.Background(), jobs)

	companyCache.AssertNotCalled(t, "BatchDelete", mock.Anything, mock.Anything)
	companyCache.AssertCalled(t, "BatchSet", mock.Anything, mock.Anything, mock.Anything)
}

func TestEntityKeyForMessage(t *testing.T) {
	eid := "explicit-id"
	sid := "5000-0"
	// EntityID wins even when a StreamID is also present (the common case for
	// company/user messages).
	withEntityID := &schematicdatastreamws.DataStreamResp{EntityID: &eid, StreamID: &sid}
	assert.Equal(t, "explicit-id", entityKeyForMessage(withEntityID), "EntityID wins when set")

	// No EntityID: fall back to the unique StreamID (never parses the payload).
	streamOnly := &schematicdatastreamws.DataStreamResp{StreamID: &sid, Data: json.RawMessage(`{"id":"from-data"}`)}
	assert.Equal(t, "5000-0", entityKeyForMessage(streamOnly), "falls back to StreamID, not the payload id")

	// Neither present: empty key (hashes to shard 0).
	none := &schematicdatastreamws.DataStreamResp{Data: json.RawMessage(`{"id":"from-data"}`)}
	assert.Equal(t, "", entityKeyForMessage(none))
}

func TestShardForKeyIsStableAndBounded(t *testing.T) {
	const n = 4
	first := shardForKey("company-abc", n)
	for i := 0; i < 100; i++ {
		assert.Equal(t, first, shardForKey("company-abc", n), "same key must always map to the same shard")
	}
	for _, k := range []string{"a", "b", "c", "xyz", "company-123"} {
		s := shardForKey(k, n)
		assert.GreaterOrEqual(t, s, 0)
		assert.Less(t, s, n)
	}
	assert.Equal(t, 0, shardForKey("", n), "empty key maps to shard 0")
	assert.Equal(t, 0, shardForKey("anything", 1))
}
