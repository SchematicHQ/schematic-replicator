package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/schematichq/rulesengine"
	schematicdatastreamws "github.com/schematichq/schematic-datastream-ws"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Drives a message through HandleMessage (Track) and batch processing
// (Complete) to prove the cursor commits end-to-end only after apply.
func TestReplayCursorCommitsThroughHandler(t *testing.T) {
	logger := NewSchematicLogger()
	companyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
	userCache := NewMockBatchCacheProvider[*rulesengine.User]()
	flagCache := NewMockCacheProvider[*rulesengine.Flag]()
	companyLookupCache := NewMockBatchCacheProvider[string]()
	userLookupCache := NewMockBatchCacheProvider[string]()

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

	cursor := NewReplayCursor(nil, logger) // in-memory only; no Redis needed
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
