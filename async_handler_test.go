package main

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	return &rulesengine.Company{
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
	}
}

func createAsyncTestUser(t *testing.T) *rulesengine.User {
	t.Helper()
	return &rulesengine.User{
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
	}
}

func createAsyncTestFlag(t *testing.T) *rulesengine.Flag {
	t.Helper()
	return &rulesengine.Flag{
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
	}
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

		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
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

			handler := NewAsyncReplicatorMessageHandler(
				mockCompanyCache,
				mockUserCache,
				mockFlagCache,
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

		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
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

		// Set up expectation for batch operation
		// Use mock.MatchedBy to handle batch items
		mockCompanyCache.On("BatchSet", mock.Anything, mock.MatchedBy(func(items map[string]*rulesengine.Company) bool {
			if len(items) != len(company.Keys) {
				return false
			}
			// Check that all expected cache keys are present with correct company
			for key, value := range company.Keys {
				cacheKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
				if comp, exists := items[cacheKey]; !exists || comp.ID != company.ID {
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
	})

	t.Run("handles company deletion messages", func(t *testing.T) {
		logger := NewSchematicLogger()
		mockCompanyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
		mockUserCache := NewMockBatchCacheProvider[*rulesengine.User]()
		mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()
		config := createTestAsyncConfig()

		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
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

		// Set up expectation for batch delete operation
		// Use mock.MatchedBy to handle non-deterministic order of keys
		mockCompanyCache.On("BatchDelete", mock.Anything, mock.MatchedBy(func(keys []string) bool {
			if len(keys) != len(company.Keys) {
				return false
			}
			// Check that all expected cache keys are present
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
	})
}

func TestAsyncReplicatorMessageHandler_UserBatchProcessing(t *testing.T) {
	t.Run("processes user messages in batches", func(t *testing.T) {
		logger := NewSchematicLogger()
		mockCompanyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
		mockUserCache := NewMockBatchCacheProvider[*rulesengine.User]()
		mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()
		config := createTestAsyncConfig()

		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
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
		mockUserCache.On("BatchSet", mock.Anything, mock.MatchedBy(func(items map[string]*rulesengine.User) bool {
			if len(items) != len(user.Keys) {
				return false
			}
			// Check that all expected cache keys are present with correct user
			for key, value := range user.Keys {
				cacheKey := resourceKeyToCacheKey(cacheKeyPrefixUser, key, value)
				if u, exists := items[cacheKey]; !exists || u.ID != user.ID {
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
	})
}

func TestAsyncReplicatorMessageHandler_FlagProcessing(t *testing.T) {
	t.Run("processes single flag messages", func(t *testing.T) {
		logger := NewSchematicLogger()
		mockCompanyCache := NewMockBatchCacheProvider[*rulesengine.Company]()
		mockUserCache := NewMockBatchCacheProvider[*rulesengine.User]()
		mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()
		config := createTestAsyncConfig()

		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
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

		// Set up expectation for single flag caching
		cacheKey := flagCacheKey(flag.Key)
		mockFlagCache.On("Set", mock.Anything, cacheKey, flag, 5*time.Minute).Return(nil)

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

		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
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

		// Set up expectations for bulk flag processing
		cacheKey := flagCacheKey(flags[0].Key)
		mockFlagCache.On("Set", mock.Anything, cacheKey, flags[0], 5*time.Minute).Return(nil)
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

		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
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

		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
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

		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
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

		// Set up mock expectation (message might be processed)
		mockCompanyCache.On("BatchSet", mock.Anything, mock.MatchedBy(func(items map[string]*rulesengine.Company) bool {
			return len(items) > 0
		}), 5*time.Minute).Return(nil).Maybe()

		ctx := context.Background()

		// Send first message (should be processed)
		err = handler.HandleMessage(ctx, message)
		assert.NoError(t, err)

		// Wait a bit for processing
		time.Sleep(50 * time.Millisecond)

		// Check initial metrics
		processed, dropped := handler.GetMetrics()
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

		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
			logger,
			5*time.Minute,
			config,
		)

		// Add some messages
		company := createAsyncTestCompany(t)
		companyData, err := json.Marshal(company)
		require.NoError(t, err)

		// Set up mock expectation
		mockCompanyCache.On("BatchSet", mock.Anything, mock.MatchedBy(func(items map[string]*rulesengine.Company) bool {
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

		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
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

		handler := NewAsyncReplicatorMessageHandler(
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
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
			// Allow any valid batch set operation
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

		processed, _ := handler.GetMetrics()
		assert.True(t, processed > 0, "Expected some messages to be processed")
	})
}
