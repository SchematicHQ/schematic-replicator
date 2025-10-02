package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/schematichq/rulesengine"
	schematicdatastreamws "github.com/schematichq/schematic-datastream-ws"
	schematicgo "github.com/schematichq/schematic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockCacheProvider is a mock implementation of CacheProvider for testing
type MockCacheProvider[T any] struct {
	mock.Mock
}

func NewMockCacheProvider[T any]() *MockCacheProvider[T] {
	return &MockCacheProvider[T]{}
}

func (m *MockCacheProvider[T]) Get(ctx context.Context, key string) (T, error) {
	args := m.Called(ctx, key)
	return args.Get(0).(T), args.Error(1)
}

func (m *MockCacheProvider[T]) Set(ctx context.Context, key string, value T, ttl time.Duration) error {
	args := m.Called(ctx, key, value, ttl)
	return args.Error(0)
}

func (m *MockCacheProvider[T]) Delete(ctx context.Context, key string) error {
	args := m.Called(ctx, key)
	return args.Error(0)
}

func (m *MockCacheProvider[T]) DeleteMissing(ctx context.Context, keys []string) error {
	args := m.Called(ctx, keys)
	return args.Error(0)
}

func (m *MockCacheProvider[T]) DeleteByPattern(ctx context.Context, pattern string) (int, error) {
	args := m.Called(ctx, pattern)
	return args.Int(0), args.Error(1)
}

// Test data helpers
func createTestCompany(t *testing.T) *rulesengine.Company {
	t.Helper()
	return &rulesengine.Company{
		ID:            "company-123",
		AccountID:     "account-456",
		EnvironmentID: "env-789",
		Keys: map[string]string{
			"id":     "company-123",
			"domain": "test.com",
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

func createTestUser(t *testing.T) *rulesengine.User {
	t.Helper()
	return &rulesengine.User{
		ID:            "user-123",
		AccountID:     "account-456",
		EnvironmentID: "env-789",
		Keys: map[string]string{
			"id":    "user-123",
			"email": "test@example.com",
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

func createTestFlag(t *testing.T) *rulesengine.Flag {
	t.Helper()
	return &rulesengine.Flag{
		ID:            "flag-123",
		AccountID:     "account-456",
		EnvironmentID: "env-789",
		Key:           "test-feature",
		DefaultValue:  false,
		Rules: []*rulesengine.Rule{
			{
				ID:            "rule-123",
				AccountID:     "account-456",
				EnvironmentID: "env-789",
				Name:          "Enterprise Rule",
				Priority:      100,
				Value:         true,
			},
		},
	}
}

func TestReplicatorMessageHandler_HandleCompanyMessage(t *testing.T) {
	tests := []struct {
		name        string
		messageType schematicdatastreamws.MessageType
		company     *rulesengine.Company
		expectError bool
	}{
		{
			name:        "Handle company creation message",
			messageType: schematicdatastreamws.MessageTypeFull,
			company:     createTestCompany(t),
			expectError: false,
		},
		{
			name:        "Handle company update message",
			messageType: schematicdatastreamws.MessageTypePartial,
			company:     createTestCompany(t),
			expectError: false,
		},
		{
			name:        "Handle company deletion message",
			messageType: schematicdatastreamws.MessageTypeDelete,
			company:     createTestCompany(t),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mocks
			logger := NewSchematicLogger()
			mockCompanyCache := NewMockCacheProvider[*rulesengine.Company]()
			mockUserCache := NewMockCacheProvider[*rulesengine.User]()
			mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()

			// Create handler
			handler := NewReplicatorMessageHandler(
				logger,
				mockCompanyCache,
				mockUserCache,
				mockFlagCache,
				5*time.Minute,
			)

			// Prepare test data
			companyData, err := json.Marshal(tt.company)
			assert.NoError(t, err)

			message := &schematicdatastreamws.DataStreamResp{
				EntityType:  string(schematicdatastreamws.EntityTypeCompany),
				MessageType: tt.messageType,
				Data:        json.RawMessage(companyData),
			}

			// Setup expectations
			if tt.messageType == schematicdatastreamws.MessageTypeDelete {
				// For delete operations, expect Delete calls for each key
				for key, value := range tt.company.Keys {
					cacheKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
					mockCompanyCache.On("Delete", mock.Anything, cacheKey).Return(nil)
				}
			} else {
				// For create/update operations, expect Set calls for each key
				for key, value := range tt.company.Keys {
					cacheKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
					mockCompanyCache.On("Set", mock.Anything, cacheKey, tt.company, 5*time.Minute).Return(nil)
				}
			}

			// Execute test
			ctx := context.Background()
			err = handler.HandleMessage(ctx, message)

			// Verify results
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// Verify mock expectations
			mockCompanyCache.AssertExpectations(t)
		})
	}
}

func TestReplicatorMessageHandler_HandleUserMessage(t *testing.T) {
	tests := []struct {
		name        string
		messageType schematicdatastreamws.MessageType
		user        *rulesengine.User
		expectError bool
	}{
		{
			name:        "Handle user creation message",
			messageType: schematicdatastreamws.MessageTypeFull,
			user:        createTestUser(t),
			expectError: false,
		},
		{
			name:        "Handle user update message",
			messageType: schematicdatastreamws.MessageTypePartial,
			user:        createTestUser(t),
			expectError: false,
		},
		{
			name:        "Handle user deletion message",
			messageType: schematicdatastreamws.MessageTypeDelete,
			user:        createTestUser(t),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mocks
			logger := NewSchematicLogger()
			mockCompanyCache := NewMockCacheProvider[*rulesengine.Company]()
			mockUserCache := NewMockCacheProvider[*rulesengine.User]()
			mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()

			// Create handler
			handler := NewReplicatorMessageHandler(
				logger,
				mockCompanyCache,
				mockUserCache,
				mockFlagCache,
				5*time.Minute,
			)

			// Prepare test data
			userData, err := json.Marshal(tt.user)
			assert.NoError(t, err)

			message := &schematicdatastreamws.DataStreamResp{
				EntityType:  string(schematicdatastreamws.EntityTypeUser),
				MessageType: tt.messageType,
				Data:        json.RawMessage(userData),
			}

			// Setup expectations based on message type
			if tt.messageType == schematicdatastreamws.MessageTypeDelete {
				// For delete operations, expect Delete calls for all user keys
				for key, value := range tt.user.Keys {
					cacheKey := resourceKeyToCacheKey(cacheKeyPrefixUser, key, value)
					mockUserCache.On("Delete", mock.Anything, cacheKey).Return(nil)
				}
			} else {
				// For create/update operations, expect Set calls for all user keys
				for key, value := range tt.user.Keys {
					cacheKey := resourceKeyToCacheKey(cacheKeyPrefixUser, key, value)
					mockUserCache.On("Set", mock.Anything, cacheKey, tt.user, 5*time.Minute).Return(nil)
				}
			}

			// Execute test
			ctx := context.Background()
			err = handler.HandleMessage(ctx, message)

			// Verify results
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// Verify mock expectations
			mockUserCache.AssertExpectations(t)
		})
	}
}

func TestReplicatorMessageHandler_HandleFlagMessage(t *testing.T) {
	tests := []struct {
		name        string
		messageType schematicdatastreamws.MessageType
		flag        *rulesengine.Flag
		expectError bool
	}{
		{
			name:        "Handle single flag creation message",
			messageType: schematicdatastreamws.MessageTypeFull,
			flag:        createTestFlag(t),
			expectError: false,
		},
		{
			name:        "Handle single flag update message",
			messageType: schematicdatastreamws.MessageTypePartial,
			flag:        createTestFlag(t),
			expectError: false,
		},
		{
			name:        "Handle single flag deletion message",
			messageType: schematicdatastreamws.MessageTypeDelete,
			flag:        createTestFlag(t),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mocks
			logger := NewSchematicLogger()
			mockCompanyCache := NewMockCacheProvider[*rulesengine.Company]()
			mockUserCache := NewMockCacheProvider[*rulesengine.User]()
			mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()

			// Create handler
			handler := NewReplicatorMessageHandler(
				logger,
				mockCompanyCache,
				mockUserCache,
				mockFlagCache,
				5*time.Minute,
			)

			// Prepare test data
			var message *schematicdatastreamws.DataStreamResp

			if tt.messageType == schematicdatastreamws.MessageTypeDelete {
				// For delete messages, we expect just the flag key/ID
				deleteData := map[string]string{
					"key": tt.flag.Key,
					"id":  tt.flag.ID,
				}
				flagData, err := json.Marshal(deleteData)
				assert.NoError(t, err)

				message = &schematicdatastreamws.DataStreamResp{
					EntityType:  string(schematicdatastreamws.EntityTypeFlag),
					MessageType: tt.messageType,
					Data:        json.RawMessage(flagData),
				}
			} else {
				// For create/update messages, we expect the full flag
				flagData, err := json.Marshal(tt.flag)
				assert.NoError(t, err)

				message = &schematicdatastreamws.DataStreamResp{
					EntityType:  string(schematicdatastreamws.EntityTypeFlag),
					MessageType: tt.messageType,
					Data:        json.RawMessage(flagData),
				}
			}

			// Setup expectations
			if tt.messageType == schematicdatastreamws.MessageTypeDelete {
				// For delete operations, expect Delete call for the flag
				cacheKey := flagCacheKey(tt.flag.Key)
				mockFlagCache.On("Delete", mock.Anything, cacheKey).Return(nil)
			} else {
				// For create/update operations, expect Set call for the flag
				// Note: Single flag processing should NOT call DeleteMissing
				cacheKey := flagCacheKey(tt.flag.Key)
				mockFlagCache.On("Set", mock.Anything, cacheKey, tt.flag, 5*time.Minute).Return(nil)
			}

			// Execute test
			ctx := context.Background()
			err := handler.HandleMessage(ctx, message)

			// Verify results
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// Verify mock expectations
			mockFlagCache.AssertExpectations(t)
		})
	}
}

func TestReplicatorMessageHandler_HandleFlagsMessage(t *testing.T) {
	t.Run("Handle bulk flags message with DeleteMissing", func(t *testing.T) {
		// Create mocks
		logger := NewSchematicLogger()
		mockCompanyCache := NewMockCacheProvider[*rulesengine.Company]()
		mockUserCache := NewMockCacheProvider[*rulesengine.User]()
		mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()

		// Create handler
		handler := NewReplicatorMessageHandler(
			logger,
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
			5*time.Minute,
		)

		// Prepare test data - multiple flags
		flags := []*rulesengine.Flag{
			createTestFlag(t),
			{
				ID:            "flag-456",
				AccountID:     "account-456",
				EnvironmentID: "env-789",
				Key:           "another-feature",
				DefaultValue:  true,
			},
		}

		flagsData, err := json.Marshal(flags)
		assert.NoError(t, err)

		message := &schematicdatastreamws.DataStreamResp{
			EntityType:  string(schematicdatastreamws.EntityTypeFlags),
			MessageType: schematicdatastreamws.MessageTypeFull,
			Data:        json.RawMessage(flagsData),
		}

		// Setup expectations for bulk flags processing
		var expectedCacheKeys []string
		for _, flag := range flags {
			cacheKey := flagCacheKey(flag.Key)
			expectedCacheKeys = append(expectedCacheKeys, cacheKey)
			mockFlagCache.On("Set", mock.Anything, cacheKey, flag, 5*time.Minute).Return(nil)
		}

		// Important: Bulk flags processing should call DeleteMissing
		mockFlagCache.On("DeleteMissing", mock.Anything, expectedCacheKeys).Return(nil)

		// Execute test
		ctx := context.Background()
		err = handler.HandleMessage(ctx, message)

		// Verify results
		assert.NoError(t, err)

		// Verify mock expectations - this ensures DeleteMissing was called for bulk processing
		mockFlagCache.AssertExpectations(t)
	})
}

func TestReplicatorMessageHandler_SingleVsBulkFlagProcessing(t *testing.T) {
	t.Run("Single flag processing does NOT call DeleteMissing", func(t *testing.T) {
		// Create mocks
		logger := NewSchematicLogger()
		mockCompanyCache := NewMockCacheProvider[*rulesengine.Company]()
		mockUserCache := NewMockCacheProvider[*rulesengine.User]()
		mockFlagCache := NewMockCacheProvider[*rulesengine.Flag]()

		// Create handler
		handler := NewReplicatorMessageHandler(
			logger,
			mockCompanyCache,
			mockUserCache,
			mockFlagCache,
			5*time.Minute,
		)

		// Single flag message
		flag := createTestFlag(t)
		flagData, err := json.Marshal(flag)
		assert.NoError(t, err)

		message := &schematicdatastreamws.DataStreamResp{
			EntityType:  string(schematicdatastreamws.EntityTypeFlag),
			MessageType: schematicdatastreamws.MessageTypeFull,
			Data:        json.RawMessage(flagData),
		}

		// Setup expectations - only Set, no DeleteMissing
		cacheKey := flagCacheKey(flag.Key)
		mockFlagCache.On("Set", mock.Anything, cacheKey, flag, 5*time.Minute).Return(nil)
		// Note: We don't set up DeleteMissing expectation - it should NOT be called

		// Execute test
		ctx := context.Background()
		err = handler.HandleMessage(ctx, message)

		// Verify results
		assert.NoError(t, err)

		// Verify mock expectations - ensures DeleteMissing was NOT called
		mockFlagCache.AssertExpectations(t)
	})
}

// Test the convertToRulesEngineCompany function specifically for credit balance mapping
func TestConvertToRulesEngineCompany_CreditBalances(t *testing.T) {
	// Test data with credit balances
	companyData := &schematicgo.CompanyDetailResponseData{
		ID:            "company-123",
		EnvironmentID: "env-789",
		Name:          "Test Company",
		BillingCreditBalances: map[string]float64{
			"credit-1": 100.50,
			"credit-2": 250.75,
			"credit-3": 0.00,
		},
	}

	// Convert to rulesengine company
	company := convertToRulesEngineCompany(companyData)

	// Verify basic fields
	assert.Equal(t, "company-123", company.ID)
	assert.Equal(t, "env-789", company.EnvironmentID)

	// Verify credit balances are correctly mapped
	assert.NotNil(t, company.CreditBalances)
	assert.Equal(t, 3, len(company.CreditBalances))
	assert.Equal(t, 100.50, company.CreditBalances["credit-1"])
	assert.Equal(t, 250.75, company.CreditBalances["credit-2"])
	assert.Equal(t, 0.00, company.CreditBalances["credit-3"])
}

// Test with nil credit balances
func TestConvertToRulesEngineCompany_NilCreditBalances(t *testing.T) {
	companyData := &schematicgo.CompanyDetailResponseData{
		ID:                    "company-123",
		EnvironmentID:         "env-789",
		Name:                  "Test Company",
		BillingCreditBalances: nil,
	}

	company := convertToRulesEngineCompany(companyData)

	// Verify credit balances is initialized but empty
	assert.NotNil(t, company.CreditBalances)
	assert.Equal(t, 0, len(company.CreditBalances))
}
