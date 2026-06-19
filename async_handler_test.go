package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

// MockBatchCacheProvider extends MockCacheProvider with batch operations
type MockBatchCacheProvider[T any] struct {
	MockCacheProvider[T]
}

func NewMockBatchCacheProvider[T any]() *MockBatchCacheProvider[T] {
	return &MockBatchCacheProvider[T]{
		MockCacheProvider: MockCacheProvider[T]{},
	}
}

func (m *MockBatchCacheProvider[T]) BatchSet(ctx context.Context, items map[string]T, ttl time.Duration) error {
	args := m.Called(ctx, items, ttl)
	return args.Error(0)
}

func (m *MockBatchCacheProvider[T]) BatchDelete(ctx context.Context, keys []string) error {
	args := m.Called(ctx, keys)
	return args.Error(0)
}

// Test helper functions
func createAsyncTestCompany(t *testing.T) *rulesengine.Company {
	t.Helper()
	return normalizeFixture(t, &rulesengine.Company{
		ID:            "async-company-123",
		AccountID:     "account-456",
		EnvironmentID: "env-789",
		Keys: map[string]string{
			"id":     "async-company-123",
			"domain": "asynctest.com",
		},
		Traits: []*rulesengine.Trait{
			{
				Value: "Enterprise",
				TraitDefinition: &rulesengine.TraitDefinition{
					ID:         "plan",
					EntityType: rulesengine.EntityTypeCompany,
				},
			},
		},
	})
}

func createAsyncTestUser(t *testing.T) *rulesengine.User {
	t.Helper()
	return normalizeFixture(t, &rulesengine.User{
		ID:            "async-user-123",
		AccountID:     "account-456",
		EnvironmentID: "env-789",
		Keys: map[string]string{
			"id":    "async-user-123",
			"email": "asynctest@example.com",
		},
		Traits: []*rulesengine.Trait{
			{
				Value: "Premium",
				TraitDefinition: &rulesengine.TraitDefinition{
					ID:         "tier",
					EntityType: rulesengine.EntityTypeUser,
				},
			},
		},
	})
}

func createAsyncTestFlag(t *testing.T) *rulesengine.Flag {
	t.Helper()
	return normalizeFixture(t, &rulesengine.Flag{
		ID:            "async-flag-123",
		AccountID:     "account-456",
		EnvironmentID: "env-789",
		Key:           "async-test-feature",
		DefaultValue:  false,
		Rules: []*rulesengine.Rule{
			{
				ID:            "async-rule-123",
				AccountID:     "account-456",
				EnvironmentID: "env-789",
				Name:          "Async Enterprise Rule",
				Priority:      100,
				Value:         true,
			},
		},
	})
}

func createTestAsyncConfig() AsyncConfig {
	return AsyncConfig{
		NumWorkers:              2, // Use small number for tests
		CompanyChannelSize:      10,
		UserChannelSize:         10,
		FlagsChannelSize:        10,
		BatchSize:               2, // Small batch for faster testing
		BatchTimeout:            50 * time.Millisecond,
		CircuitBreakerThreshold: 3,
		CircuitBreakerTimeout:   1 * time.Second,
	}
}

func TestAsyncReplicatorMessageHandler_Creation(t *testing.T) {
	t.Run("creates handler with correct configuration", func(t *testing.T) {
		logger := NewSchematicLogger()
		mockCompanyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
		mockUserCache := NewMockBatchCacheProvider[*rulesengine.User]()
		mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()
		config := createTestAsyncConfig()

		mockCompanyLookupCache := NewMockBatchCacheProvider[string]()
		mockUserLookupCache := NewMockBatchCacheProvider[string]()
		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
			mockCompanyLookupCache,
			mockUserLookupCache,
			logger,
			5*time.Minute,
			config,
		)

		assert.NotNil(t, handler)
		assert.Equal(t, config.NumWorkers, handler.numWorkers)
		assert.Equal(t, config.BatchSize, handler.batchSize)
		assert.Equal(t, config.BatchTimeout, handler.batchTimeout)

		// Test graceful shutdown
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := handler.Shutdown(ctx)
		assert.NoError(t, err)
	})
}

func TestAsyncReplicatorMessageHandler_MessageRouting(t *testing.T) {
	tests := []struct {
		name        string
		entityType  string
		expectRoute bool
	}{
		{"company message", string(schematicdatastreamws.EntityTypeCompany), true},
		{"user message", string(schematicdatastreamws.EntityTypeUser), true},
		{"single flag message", string(schematicdatastreamws.EntityTypeFlag), true},
		{"bulk flags message", string(schematicdatastreamws.EntityTypeFlags), true},
		{"companies subscription", string(schematicdatastreamws.EntityTypeCompanies), true},
		{"users subscription", string(schematicdatastreamws.EntityTypeUsers), true},
		{"unknown entity type", "unknown", true}, // Should handle gracefully
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := NewSchematicLogger()
			mockCompanyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
			mockUserCache := NewMockBatchCacheProvider[*rulesengine.User]()
			mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()
			config := createTestAsyncConfig()

			mockCompanyLookupCache := NewMockBatchCacheProvider[string]()
			mockUserLookupCache := NewMockBatchCacheProvider[string]()
			handler := NewAsyncReplicatorMessageHandler(
				mockCompanyCache,
				mockUserCache,
				mockFlagCache,
				mockCompanyLookupCache,
				mockUserLookupCache,
				logger,
				5*time.Minute,
				config,
			)
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()
				handler.Shutdown(ctx)
			}()

			var data json.RawMessage
			if tt.entityType == string(schematicdatastreamws.EntityTypeFlags) {
				// Bulk flags expects an array, not an object
				data = json.RawMessage(`[]`)
			} else {
				data = json.RawMessage(`{}`)
			}

			message := &schematicdatastreamws.DataStreamResp{
				EntityType:  tt.entityType,
				MessageType: schematicdatastreamws.MessageTypeFull,
				Data:        data,
			}

			ctx := context.Background()
			err := handler.HandleMessage(ctx, message)

			if tt.expectRoute {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAsyncReplicatorMessageHandler_CompanyBatchProcessing(t *testing.T) {
	t.Run("processes company messages in batches", func(t *testing.T) {
		logger := NewSchematicLogger()
		mockCompanyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
		mockUserCache := NewMockBatchCacheProvider[*rulesengine.User]()
		mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()
		config := createTestAsyncConfig()

		mockCompanyLookupCache := NewMockBatchCacheProvider[string]()
		mockUserLookupCache := NewMockBatchCacheProvider[string]()
		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
			mockCompanyLookupCache,
			mockUserLookupCache,
			logger,
			5*time.Minute,
			config,
		)
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			handler.Shutdown(ctx)
		}()

		company := createAsyncTestCompany(t)
		companyData, err := json.Marshal(company)
		require.NoError(t, err)

		// Set up expectation for batch operation on ID keys
		mockCompanyCache.On("BatchSet", mock.Anything, mock.MatchedBy(func(items map[string]*rulesengine.Company) bool {
			// Should contain exactly 1 ID key per unique company
			idKey := companyIDCacheKey(company.ID)
			comp, exists := items[idKey]
			return exists && comp.ID == company.ID
		}), 5*time.Minute).Return(nil)

		// Set up expectation for batch operation on lookup keys
		mockCompanyLookupCache.On("BatchSet", mock.Anything, mock.MatchedBy(func(items map[string]string) bool {
			if len(items) != len(company.Keys) {
				return false
			}
			for key, value := range company.Keys {
				cacheKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
				if id, exists := items[cacheKey]; !exists || id != company.ID {
					return false
				}
			}
			return true
		}), 5*time.Minute).Return(nil)

		// Send multiple messages to trigger batching
		ctx := context.Background()
		for i := 0; i < config.BatchSize; i++ {
			message := &schematicdatastreamws.DataStreamResp{
				EntityType:  string(schematicdatastreamws.EntityTypeCompany),
				MessageType: schematicdatastreamws.MessageTypeFull,
				Data:        companyData,
			}

			err := handler.HandleMessage(ctx, message)
			assert.NoError(t, err)
		}

		// Wait for batch processing
		time.Sleep(100 * time.Millisecond)

		mockCompanyCache.AssertExpectations(t)
		mockCompanyLookupCache.AssertExpectations(t)
	})

	t.Run("handles company deletion messages", func(t *testing.T) {
		logger := NewSchematicLogger()
		mockCompanyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
		mockUserCache := NewMockBatchCacheProvider[*rulesengine.User]()
		mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()
		config := createTestAsyncConfig()

		mockCompanyLookupCache := NewMockBatchCacheProvider[string]()
		mockUserLookupCache := NewMockBatchCacheProvider[string]()
		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
			mockCompanyLookupCache,
			mockUserLookupCache,
			logger,
			5*time.Minute,
			config,
		)
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			handler.Shutdown(ctx)
		}()

		company := createAsyncTestCompany(t)
		companyData, err := json.Marshal(company)
		require.NoError(t, err)

		// Set up expectation for batch delete on ID keys
		mockCompanyCache.On("BatchDelete", mock.Anything, mock.MatchedBy(func(keys []string) bool {
			expectedIDKey := companyIDCacheKey(company.ID)
			for _, key := range keys {
				if key == expectedIDKey {
					return true
				}
			}
			return false
		})).Return(nil)

		// Set up expectation for batch delete on lookup keys
		mockCompanyLookupCache.On("BatchDelete", mock.Anything, mock.MatchedBy(func(keys []string) bool {
			if len(keys) != len(company.Keys) {
				return false
			}
			expectedKeys := make(map[string]bool)
			for key, value := range company.Keys {
				cacheKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
				expectedKeys[cacheKey] = true
			}
			for _, key := range keys {
				if !expectedKeys[key] {
					return false
				}
			}
			return true
		})).Return(nil)

		// Send delete messages
		ctx := context.Background()
		for i := 0; i < config.BatchSize; i++ {
			message := &schematicdatastreamws.DataStreamResp{
				EntityType:  string(schematicdatastreamws.EntityTypeCompany),
				MessageType: schematicdatastreamws.MessageTypeDelete,
				Data:        companyData,
			}

			err := handler.HandleMessage(ctx, message)
			assert.NoError(t, err)
		}

		// Wait for batch processing
		time.Sleep(100 * time.Millisecond)

		mockCompanyCache.AssertExpectations(t)
		mockCompanyLookupCache.AssertExpectations(t)
	})
}

func TestAsyncReplicatorMessageHandler_UserBatchProcessing(t *testing.T) {
	t.Run("processes user messages in batches", func(t *testing.T) {
		logger := NewSchematicLogger()
		mockCompanyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
		mockUserCache := NewMockBatchCacheProvider[*rulesengine.User]()
		mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()
		config := createTestAsyncConfig()

		mockCompanyLookupCache := NewMockBatchCacheProvider[string]()
		mockUserLookupCache := NewMockBatchCacheProvider[string]()
		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
			mockCompanyLookupCache,
			mockUserLookupCache,
			logger,
			5*time.Minute,
			config,
		)
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			handler.Shutdown(ctx)
		}()

		user := createAsyncTestUser(t)
		userData, err := json.Marshal(user)
		require.NoError(t, err)

		// Set up expectation for batch operation
		// Use mock.MatchedBy to handle batch items
		// Expect BatchSet on user cache with ID-based key (deduplicated)
		mockUserCache.On("BatchSet", mock.Anything, mock.MatchedBy(func(items map[string]*rulesengine.User) bool {
			if len(items) != 1 {
				return false
			}
			idKey := userIDCacheKey(user.ID)
			u, exists := items[idKey]
			return exists && u.ID == user.ID
		}), 5*time.Minute).Return(nil)

		// Expect BatchSet on user lookup cache with lookup keys pointing to user ID
		mockUserLookupCache.On("BatchSet", mock.Anything, mock.MatchedBy(func(items map[string]string) bool {
			if len(items) != len(user.Keys) {
				return false
			}
			for key, value := range user.Keys {
				lookupKey := resourceKeyToCacheKey(cacheKeyPrefixUser, key, value)
				if id, exists := items[lookupKey]; !exists || id != user.ID {
					return false
				}
			}
			return true
		}), 5*time.Minute).Return(nil)

		// Send multiple messages to trigger batching
		ctx := context.Background()
		for i := 0; i < config.BatchSize; i++ {
			message := &schematicdatastreamws.DataStreamResp{
				EntityType:  string(schematicdatastreamws.EntityTypeUser),
				MessageType: schematicdatastreamws.MessageTypeFull,
				Data:        userData,
			}

			err := handler.HandleMessage(ctx, message)
			assert.NoError(t, err)
		}

		// Wait for batch processing
		time.Sleep(100 * time.Millisecond)

		mockUserCache.AssertExpectations(t)
		mockUserLookupCache.AssertExpectations(t)
	})
}

func TestAsyncReplicatorMessageHandler_FlagProcessing(t *testing.T) {
	t.Run("processes single flag messages", func(t *testing.T) {
		logger := NewSchematicLogger()
		mockCompanyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
		mockUserCache := NewMockBatchCacheProvider[*rulesengine.User]()
		mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()
		config := createTestAsyncConfig()

		mockCompanyLookupCache := NewMockBatchCacheProvider[string]()
		mockUserLookupCache := NewMockBatchCacheProvider[string]()
		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
			mockCompanyLookupCache,
			mockUserLookupCache,
			logger,
			5*time.Minute,
			config,
		)
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			handler.Shutdown(ctx)
		}()

		flag := createAsyncTestFlag(t)
		flagData, err := json.Marshal(flag)
		require.NoError(t, err)

		// Set up expectation for single flag caching. Match on identity rather
		// than the exact struct: the handler caches the round-tripped flag, which
		// is functionally equal but not reflect.DeepEqual to the original (e.g.
		// Rules is a JSONSlice whose nil/empty representation differs).
		cacheKey := flagCacheKey(flag.Key)
		mockFlagCache.On("Set", mock.Anything, cacheKey, mock.MatchedBy(func(f *rulesengine.Flag) bool {
			return f != nil && f.ID == flag.ID && f.Key == flag.Key
		}), 5*time.Minute).Return(nil)

		message := &schematicdatastreamws.DataStreamResp{
			EntityType:  string(schematicdatastreamws.EntityTypeFlag),
			MessageType: schematicdatastreamws.MessageTypeFull,
			Data:        flagData,
		}

		ctx := context.Background()
		err = handler.HandleMessage(ctx, message)
		assert.NoError(t, err)

		// Wait for processing
		time.Sleep(100 * time.Millisecond)

		mockFlagCache.AssertExpectations(t)
	})

	t.Run("processes bulk flags messages with DeleteMissing", func(t *testing.T) {
		logger := NewSchematicLogger()
		mockCompanyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
		mockUserCache := NewMockBatchCacheProvider[*rulesengine.User]()
		mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()
		config := createTestAsyncConfig()

		mockCompanyLookupCache := NewMockBatchCacheProvider[string]()
		mockUserLookupCache := NewMockBatchCacheProvider[string]()
		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
			mockCompanyLookupCache,
			mockUserLookupCache,
			logger,
			5*time.Minute,
			config,
		)
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			handler.Shutdown(ctx)
		}()

		flags := []*rulesengine.Flag{createAsyncTestFlag(t)}
		flagsData, err := json.Marshal(flags)
		require.NoError(t, err)

		// Set up expectations for bulk flag processing. Match on identity (the
		// handler caches the round-tripped flag; see the single-flag test).
		cacheKey := flagCacheKey(flags[0].Key)
		mockFlagCache.On("Set", mock.Anything, cacheKey, mock.MatchedBy(func(f *rulesengine.Flag) bool {
			return f != nil && f.ID == flags[0].ID && f.Key == flags[0].Key
		}), 5*time.Minute).Return(nil)
		mockFlagCache.On("DeleteMissing", mock.Anything, []string{cacheKey}).Return(nil)

		message := &schematicdatastreamws.DataStreamResp{
			EntityType:  string(schematicdatastreamws.EntityTypeFlags),
			MessageType: schematicdatastreamws.MessageTypeFull,
			Data:        flagsData,
		}

		ctx := context.Background()
		err = handler.HandleMessage(ctx, message)
		assert.NoError(t, err)

		// Wait for processing
		time.Sleep(100 * time.Millisecond)

		mockFlagCache.AssertExpectations(t)
	})

	t.Run("handles single flag deletion", func(t *testing.T) {
		logger := NewSchematicLogger()
		mockCompanyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
		mockUserCache := NewMockBatchCacheProvider[*rulesengine.User]()
		mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()
		config := createTestAsyncConfig()

		mockCompanyLookupCache := NewMockBatchCacheProvider[string]()
		mockUserLookupCache := NewMockBatchCacheProvider[string]()
		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
			mockCompanyLookupCache,
			mockUserLookupCache,
			logger,
			5*time.Minute,
			config,
		)
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			handler.Shutdown(ctx)
		}()

		deleteData := struct {
			Key string `json:"key"`
		}{
			Key: "async-test-feature",
		}
		deleteDataBytes, err := json.Marshal(deleteData)
		require.NoError(t, err)

		// Set up expectation for flag deletion
		cacheKey := flagCacheKey(deleteData.Key)
		mockFlagCache.On("Delete", mock.Anything, cacheKey).Return(nil)

		message := &schematicdatastreamws.DataStreamResp{
			EntityType:  string(schematicdatastreamws.EntityTypeFlag),
			MessageType: schematicdatastreamws.MessageTypeDelete,
			Data:        deleteDataBytes,
		}

		ctx := context.Background()
		err = handler.HandleMessage(ctx, message)
		assert.NoError(t, err)

		// Wait for processing
		time.Sleep(100 * time.Millisecond)

		mockFlagCache.AssertExpectations(t)
	})
}

func TestAsyncReplicatorMessageHandler_CircuitBreaker(t *testing.T) {
	t.Run("circuit breaker opens on failures and closes on successes", func(t *testing.T) {
		logger := NewSchematicLogger()
		mockCompanyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
		mockUserCache := NewMockBatchCacheProvider[*rulesengine.User]()
		mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()

		// Use config with low threshold for faster testing
		config := createTestAsyncConfig()
		config.CircuitBreakerThreshold = 2
		config.CircuitBreakerTimeout = 100 * time.Millisecond

		mockCompanyLookupCache := NewMockBatchCacheProvider[string]()
		mockUserLookupCache := NewMockBatchCacheProvider[string]()
		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
			mockCompanyLookupCache,
			mockUserLookupCache,
			logger,
			5*time.Minute,
			config,
		)
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			handler.Shutdown(ctx)
		}()

		// Test circuit breaker functionality
		cb := handler.redisCircuitBreaker

		// Initially should be closed
		assert.Equal(t, CircuitClosed, cb.GetState())
		assert.True(t, cb.CanExecute())

		// Record failures to open circuit
		for i := 0; i < config.CircuitBreakerThreshold; i++ {
			cb.RecordFailure()
		}
		assert.Equal(t, CircuitOpen, cb.GetState())
		assert.False(t, cb.CanExecute())

		// Wait for timeout and check half-open state
		time.Sleep(config.CircuitBreakerTimeout + 10*time.Millisecond)
		assert.True(t, cb.CanExecute()) // Should allow execution in half-open state
		assert.Equal(t, CircuitHalfOpen, cb.GetState())

		// Record successes to close circuit
		for i := 0; i < 3; i++ { // Requires 3 successes to close
			cb.RecordSuccess()
		}
		assert.Equal(t, CircuitClosed, cb.GetState())
	})
}

func TestAsyncReplicatorMessageHandler_Metrics(t *testing.T) {
	t.Run("tracks processed and dropped message metrics", func(t *testing.T) {
		logger := NewSchematicLogger()
		mockCompanyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
		mockUserCache := NewMockBatchCacheProvider[*rulesengine.User]()
		mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()

		// Use small channel size to test backpressure
		config := createTestAsyncConfig()
		config.CompanyChannelSize = 1

		mockCompanyLookupCache := NewMockBatchCacheProvider[string]()
		mockUserLookupCache := NewMockBatchCacheProvider[string]()
		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
			mockCompanyLookupCache,
			mockUserLookupCache,
			logger,
			5*time.Minute,
			config,
		)
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			handler.Shutdown(ctx)
		}()

		company := createAsyncTestCompany(t)
		companyData, err := json.Marshal(company)
		require.NoError(t, err)

		message := &schematicdatastreamws.DataStreamResp{
			EntityType:  string(schematicdatastreamws.EntityTypeCompany),
			MessageType: schematicdatastreamws.MessageTypeFull,
			Data:        companyData,
		}

		// Set up mock expectations (message might be processed)
		mockCompanyCache.On("BatchSet", mock.Anything, mock.MatchedBy(func(items map[string]*rulesengine.Company) bool {
			return len(items) > 0
		}), 5*time.Minute).Return(nil).Maybe()
		mockCompanyLookupCache.On("BatchSet", mock.Anything, mock.MatchedBy(func(items map[string]string) bool {
			return len(items) > 0
		}), 5*time.Minute).Return(nil).Maybe()

		ctx := context.Background()

		// Send first message (should be processed)
		err = handler.HandleMessage(ctx, message)
		assert.NoError(t, err)

		// Wait a bit for processing
		time.Sleep(50 * time.Millisecond)

		// Check initial metrics
		processed, _, dropped := handler.GetMetrics()
		assert.Equal(t, int64(1), processed)
		assert.Equal(t, int64(0), dropped)
	})
}

func TestAsyncReplicatorMessageHandler_GracefulShutdown(t *testing.T) {
	t.Run("shuts down gracefully and processes remaining messages", func(t *testing.T) {
		logger := NewSchematicLogger()
		mockCompanyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
		mockUserCache := NewMockBatchCacheProvider[*rulesengine.User]()
		mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()
		config := createTestAsyncConfig()

		mockCompanyLookupCache := NewMockBatchCacheProvider[string]()
		mockUserLookupCache := NewMockBatchCacheProvider[string]()
		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
			mockCompanyLookupCache,
			mockUserLookupCache,
			logger,
			5*time.Minute,
			config,
		)

		// Add some messages
		company := createAsyncTestCompany(t)
		companyData, err := json.Marshal(company)
		require.NoError(t, err)

		// Set up mock expectations
		mockCompanyCache.On("BatchSet", mock.Anything, mock.MatchedBy(func(items map[string]*rulesengine.Company) bool {
			return len(items) > 0
		}), 5*time.Minute).Return(nil).Maybe()
		mockCompanyLookupCache.On("BatchSet", mock.Anything, mock.MatchedBy(func(items map[string]string) bool {
			return len(items) > 0
		}), 5*time.Minute).Return(nil).Maybe()

		message := &schematicdatastreamws.DataStreamResp{
			EntityType:  string(schematicdatastreamws.EntityTypeCompany),
			MessageType: schematicdatastreamws.MessageTypeFull,
			Data:        companyData,
		}

		ctx := context.Background()
		err = handler.HandleMessage(ctx, message)
		assert.NoError(t, err)

		// Shutdown with reasonable timeout
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err = handler.Shutdown(shutdownCtx)
		assert.NoError(t, err)

		// Verify cannot handle messages after shutdown
		err = handler.HandleMessage(ctx, message)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "shutting down")
	})

	t.Run("handles shutdown timeout gracefully", func(t *testing.T) {
		logger := NewSchematicLogger()
		mockCompanyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
		mockUserCache := NewMockBatchCacheProvider[*rulesengine.User]()
		mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()

		// Use larger config with more workers to increase shutdown time
		config := createTestAsyncConfig()
		config.NumWorkers = 10
		config.BatchTimeout = 1 * time.Second // Longer batch timeout

		mockCompanyLookupCache := NewMockBatchCacheProvider[string]()
		mockUserLookupCache := NewMockBatchCacheProvider[string]()
		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
			mockCompanyLookupCache,
			mockUserLookupCache,
			logger,
			5*time.Minute,
			config,
		)

		// Use very short timeout to test timeout scenario
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		err := handler.Shutdown(shutdownCtx)
		// Should return context deadline exceeded error or complete successfully
		// (Since Go runtime is fast, we just check it handles the timeout properly)
		if err != nil {
			assert.Equal(t, context.DeadlineExceeded, err)
		} else {
			// Shutdown completed within timeout, which is also acceptable
			t.Log("Shutdown completed within timeout")
		}
	})
}

func TestAsyncReplicatorMessageHandler_ConcurrentAccess(t *testing.T) {
	t.Run("handles concurrent message processing safely", func(t *testing.T) {
		logger := NewSchematicLogger()
		mockCompanyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
		mockUserCache := NewMockBatchCacheProvider[*rulesengine.User]()
		mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()
		config := createTestAsyncConfig()
		config.CompanyChannelSize = 100 // Larger buffer for concurrency test

		mockCompanyLookupCache := NewMockBatchCacheProvider[string]()
		mockUserLookupCache := NewMockBatchCacheProvider[string]()
		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
			mockCompanyLookupCache,
			mockUserLookupCache,
			logger,
			5*time.Minute,
			config,
		)
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			handler.Shutdown(ctx)
		}()

		company := createAsyncTestCompany(t)
		companyData, err := json.Marshal(company)
		require.NoError(t, err)

		message := &schematicdatastreamws.DataStreamResp{
			EntityType:  string(schematicdatastreamws.EntityTypeCompany),
			MessageType: schematicdatastreamws.MessageTypeFull,
			Data:        companyData,
		}

		// Set up mock expectations (may be called multiple times due to batching)
		mockCompanyCache.On("BatchSet", mock.Anything, mock.MatchedBy(func(items map[string]*rulesengine.Company) bool {
			return len(items) > 0
		}), 5*time.Minute).Return(nil).Maybe()
		mockCompanyLookupCache.On("BatchSet", mock.Anything, mock.MatchedBy(func(items map[string]string) bool {
			return len(items) > 0
		}), 5*time.Minute).Return(nil).Maybe()

		// Send messages concurrently
		var wg sync.WaitGroup
		var successCount int64
		numGoroutines := 10
		messagesPerGoroutine := 5

		ctx := context.Background()
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < messagesPerGoroutine; j++ {
					if err := handler.HandleMessage(ctx, message); err == nil {
						atomic.AddInt64(&successCount, 1)
					}
				}
			}()
		}

		wg.Wait()

		// Wait for processing to complete
		time.Sleep(200 * time.Millisecond)

		// Check that most messages were processed successfully
		totalExpected := int64(numGoroutines * messagesPerGoroutine)
		assert.True(t, atomic.LoadInt64(&successCount) > totalExpected/2,
			"Expected at least half of messages to be processed successfully")

		processed, _, _ := handler.GetMetrics()
		assert.True(t, processed > 0, "Expected some messages to be processed")
	})
}

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

// newTestLoader builds a loader with no real API/cache dependencies. Callers
// override loadCompaniesFn/loadUsersFn to observe load runs.
func newTestLoader(t *testing.T) *AsyncInitialLoader {
	t.Helper()
	return NewAsyncInitialLoader(nil, nil, nil, nil, nil, NewSchematicLogger(), time.Minute, DefaultAsyncLoaderConfig())
}

func waitIdle(t *testing.T, al *AsyncInitialLoader) {
	t.Helper()
	require.Eventually(t, func() bool { return !al.loadingActive() }, time.Second, time.Millisecond, "load run did not finish")
}

// TestStartAsyncLoadingOnlyRunsOnce verifies the once-guard: OnConnectionReady
// calls StartAsyncLoading on every reconnect, but only the first call may
// trigger a bulk load — reconnects rely on replay instead.
func TestStartAsyncLoadingOnlyRunsOnce(t *testing.T) {
	ctx := context.Background()
	al := newTestLoader(t)

	var companyLoads, userLoads int32
	al.loadCompaniesFn = func(context.Context) error { atomic.AddInt32(&companyLoads, 1); return nil }
	al.loadUsersFn = func(context.Context) error { atomic.AddInt32(&userLoads, 1); return nil }

	al.StartAsyncLoading(ctx)
	waitIdle(t, al)
	al.StartAsyncLoading(ctx)
	al.StartAsyncLoading(ctx)
	waitIdle(t, al)

	assert.Equal(t, int32(1), atomic.LoadInt32(&companyLoads), "StartAsyncLoading must load only once")
	assert.Equal(t, int32(1), atomic.LoadInt32(&userLoads), "StartAsyncLoading must load only once")
}

// TestReloadForcesFreshLoad is the regression test for the dead reload path:
// Reload must trigger a real bulk load even after the initial load has run.
func TestReloadForcesFreshLoad(t *testing.T) {
	ctx := context.Background()
	al := newTestLoader(t)

	var companyLoads, userLoads int32
	al.loadCompaniesFn = func(context.Context) error { atomic.AddInt32(&companyLoads, 1); return nil }
	al.loadUsersFn = func(context.Context) error { atomic.AddInt32(&userLoads, 1); return nil }

	al.StartAsyncLoading(ctx)
	waitIdle(t, al)

	al.Reload(ctx)
	waitIdle(t, al)
	al.Reload(ctx)
	waitIdle(t, al)

	// Initial load + two reloads = three runs.
	assert.Equal(t, int32(3), atomic.LoadInt32(&companyLoads))
	assert.Equal(t, int32(3), atomic.LoadInt32(&userLoads))
}

// TestReloadSkippedWhileLoadInProgress verifies overlapping runs are guarded so
// two loads never write the same caches concurrently.
func TestReloadSkippedWhileLoadInProgress(t *testing.T) {
	ctx := context.Background()
	al := newTestLoader(t)

	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(1)
	var startedOnce sync.Once
	var companyLoads int32
	al.loadCompaniesFn = func(context.Context) error {
		atomic.AddInt32(&companyLoads, 1)
		startedOnce.Do(started.Done)
		<-release // block until the test lets the run finish
		return nil
	}
	al.loadUsersFn = func(context.Context) error { return nil }

	al.Reload(ctx) // first run begins and blocks in loadCompaniesFn
	started.Wait() // ensure the run is in progress
	al.Reload(ctx) // second run must be skipped while the first is active
	assert.True(t, al.loadingActive())

	close(release)
	waitIdle(t, al)
	assert.Equal(t, int32(1), atomic.LoadInt32(&companyLoads), "overlapping reload must be skipped")
}

// --- Always-on pipeline validation (independent of replay) ---
// These exercise the sharded routing + in-order batch collapse through the real
// HandleMessage entry point against a miniredis-backed cache, with NO replay
// cursor attached (the REPLAY_DISABLED path). They confirm the message-pipeline
// rewrite is correct on its own, for clients that never use replay.

func newRealCacheHandler(t *testing.T) (*AsyncReplicatorMessageHandler, BatchCacheProvider[*rulesengine.Company], context.Context) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Buffers sized so the tests don't trip backpressure (which would legitimately
	// drop messages); we're validating the routing/collapse, not the drop policy.
	cfg := AsyncConfig{
		NumWorkers:              4,
		CompanyChannelSize:      500,
		UserChannelSize:         500,
		FlagsChannelSize:        500,
		BatchSize:               10,
		BatchTimeout:            20 * time.Millisecond,
		CircuitBreakerThreshold: 5,
		CircuitBreakerTimeout:   time.Second,
	}
	companies := NewRedisBatchCache[*rulesengine.Company](client, time.Minute)
	h := NewAsyncReplicatorMessageHandler(
		companies,
		NewRedisBatchCache[*rulesengine.User](client, time.Minute),
		NewRedisBatchCache[*rulesengine.Flag](client, time.Minute),
		NewRedisBatchCache[string](client, time.Minute),
		NewRedisBatchCache[string](client, time.Minute),
		NewSchematicLogger(),
		time.Minute,
		cfg,
	)
	// No replay cursor: this is the replay-disabled / always-on path.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		h.Shutdown(ctx)
	})
	return h, companies, context.Background()
}

func sendCompanyMsg(t *testing.T, h *AsyncReplicatorMessageHandler, ctx context.Context, mt schematicdatastreamws.MessageType, id, basePlan string) {
	t.Helper()
	var data json.RawMessage
	if mt == schematicdatastreamws.MessageTypePartial {
		data = json.RawMessage(fmt.Sprintf(`{"base_plan_id":%q}`, basePlan))
	} else {
		c := &rulesengine.Company{ID: id, Keys: map[string]string{"key": "k-" + id}}
		if basePlan != "" {
			c.BasePlanID = &basePlan
		}
		b, err := json.Marshal(c)
		require.NoError(t, err)
		data = b
	}
	eid := id
	require.NoError(t, h.HandleMessage(ctx, &schematicdatastreamws.DataStreamResp{
		EntityType:  string(schematicdatastreamws.EntityTypeCompany),
		MessageType: mt,
		Data:        data,
		EntityID:    &eid,
	}))
}

// Full→full, full→partial, and full→delete sequences for the same entity must
// produce the correct final cache state through the sharded async pipeline.
func TestShardedPipelineAppliesInOrderWithoutReplay(t *testing.T) {
	h, companies, ctx := newRealCacheHandler(t)

	sendCompanyMsg(t, h, ctx, schematicdatastreamws.MessageTypeFull, "comp_a", "v1")
	sendCompanyMsg(t, h, ctx, schematicdatastreamws.MessageTypeFull, "comp_a", "v2") // last write wins
	sendCompanyMsg(t, h, ctx, schematicdatastreamws.MessageTypeFull, "comp_b", "v1")
	sendCompanyMsg(t, h, ctx, schematicdatastreamws.MessageTypePartial, "comp_b", "v3") // merged onto v1
	sendCompanyMsg(t, h, ctx, schematicdatastreamws.MessageTypeFull, "comp_c", "v1")
	sendCompanyMsg(t, h, ctx, schematicdatastreamws.MessageTypeDelete, "comp_c", "") // removed

	require.Eventually(t, func() bool {
		a, _ := companies.Get(ctx, companyIDCacheKey("comp_a"))
		b, _ := companies.Get(ctx, companyIDCacheKey("comp_b"))
		_, cErr := companies.Get(ctx, companyIDCacheKey("comp_c"))
		return a != nil && a.BasePlanID != nil && *a.BasePlanID == "v2" &&
			b != nil && b.BasePlanID != nil && *b.BasePlanID == "v3" &&
			errors.Is(cErr, redis.Nil)
	}, 2*time.Second, 10*time.Millisecond, "sharded pipeline must apply each entity's ops in order")
}

// Sharding across workers must not drop or cross-contaminate entities.
func TestShardedPipelineHandlesManyEntitiesWithoutLoss(t *testing.T) {
	h, companies, ctx := newRealCacheHandler(t)
	const n = 30
	for i := 0; i < n; i++ {
		sendCompanyMsg(t, h, ctx, schematicdatastreamws.MessageTypeFull, fmt.Sprintf("comp_%02d", i), fmt.Sprintf("v%d", i))
	}
	require.Eventually(t, func() bool {
		for i := 0; i < n; i++ {
			c, err := companies.Get(ctx, companyIDCacheKey(fmt.Sprintf("comp_%02d", i)))
			if err != nil || c == nil || c.BasePlanID == nil || *c.BasePlanID != fmt.Sprintf("v%d", i) {
				return false
			}
		}
		return true
	}, 2*time.Second, 10*time.Millisecond, "every entity must be cached with its own value")
}

// When a persisted cursor lets the replicator resume via replay, OnConnectionReady
// calls MarkStarted instead of StartAsyncLoading: no bulk load on startup, and
// reconnects stay skipped too — but the aged-out fallback (forced Reload) must
// still run a full load.
func TestMarkStartedSkipsLoadButAllowsReload(t *testing.T) {
	ctx := context.Background()
	al := newTestLoader(t)
	var loads int32
	al.loadCompaniesFn = func(context.Context) error { atomic.AddInt32(&loads, 1); return nil }
	al.loadUsersFn = func(context.Context) error { return nil }

	// Resume-via-replay path: mark started without loading.
	al.MarkStarted()
	// A subsequent (re)connect must not trigger a bulk load.
	al.StartAsyncLoading(ctx)
	waitIdle(t, al)
	assert.Equal(t, int32(0), atomic.LoadInt32(&loads), "MarkStarted + reconnect must not bulk-load")

	// But an aged-out cursor (server MessageTypeReload) still forces a full reload.
	al.Reload(ctx)
	waitIdle(t, al)
	assert.Equal(t, int32(1), atomic.LoadInt32(&loads), "forced reload must still run after MarkStarted")
}

// A deleted company must be fully evicted — both its id-key and its lookup keys
// — otherwise clients keep resolving a deleted company by key and serving it
// (especially during an outage, since there's no TTL). This drives a full then a
// delete through the real handler + a miniredis cache and asserts both key kinds
// are gone.
func TestCompanyDeleteEvictsIdAndLookupKeys(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	cfg := createTestAsyncConfig()
	cfg.NumWorkers = 2
	cfg.CompanyChannelSize = 100
	h := NewAsyncReplicatorMessageHandler(
		NewRedisBatchCache[*rulesengine.Company](client, time.Minute),
		NewRedisBatchCache[*rulesengine.User](client, time.Minute),
		NewRedisBatchCache[*rulesengine.Flag](client, time.Minute),
		NewRedisBatchCache[string](client, time.Minute),
		NewRedisBatchCache[string](client, time.Minute),
		NewSchematicLogger(), time.Minute, cfg,
	)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		h.Shutdown(ctx)
	})

	ctx := context.Background()
	company := &rulesengine.Company{ID: "comp_del", Keys: map[string]string{"clerkid": "org_DEL"}}
	data, err := json.Marshal(company)
	require.NoError(t, err)
	eid := company.ID
	msg := func(mt schematicdatastreamws.MessageType) *schematicdatastreamws.DataStreamResp {
		return &schematicdatastreamws.DataStreamResp{
			EntityType:  string(schematicdatastreamws.EntityTypeCompany),
			MessageType: mt,
			Data:        data,
			EntityID:    &eid,
		}
	}

	idKey := companyIDCacheKey("comp_del")
	lookupKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, "clerkid", "org_DEL")

	// Full → both keys present.
	require.NoError(t, h.HandleMessage(ctx, msg(schematicdatastreamws.MessageTypeFull)))
	require.Eventually(t, func() bool {
		return mr.Exists(idKey) && mr.Exists(lookupKey)
	}, 2*time.Second, 10*time.Millisecond, "full should populate id-key and lookup-key")

	// Delete → both keys evicted.
	require.NoError(t, h.HandleMessage(ctx, msg(schematicdatastreamws.MessageTypeDelete)))
	require.Eventually(t, func() bool {
		return !mr.Exists(idKey) && !mr.Exists(lookupKey)
	}, 2*time.Second, 10*time.Millisecond, "delete must evict both the id-key and the lookup-key")
}
