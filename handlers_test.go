package main

import (
	"context"
	"encoding/json"
	"fmt"
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

// testHarness bundles the mocks and handler used by every message-handler test.
type testHarness struct {
	handler            *ReplicatorMessageHandler
	companyCache       *MockCacheProvider[*rulesengine.Company]
	companyLookupCache *MockCacheProvider[string]
	userCache          *MockCacheProvider[*rulesengine.User]
	userLookupCache    *MockCacheProvider[string]
	flagCache          *MockCacheProvider[*rulesengine.Flag]
}

func newTestHarness(t *testing.T) *testHarness {
	t.Helper()
	h := &testHarness{
		companyCache:       NewMockCacheProvider[*rulesengine.Company](),
		companyLookupCache: NewMockCacheProvider[string](),
		userCache:          NewMockCacheProvider[*rulesengine.User](),
		userLookupCache:    NewMockCacheProvider[string](),
		flagCache:          NewMockCacheProvider[*rulesengine.Flag](),
	}
	h.handler = NewReplicatorMessageHandler(
		NewSchematicLogger(),
		h.companyCache,
		h.companyLookupCache,
		h.userCache,
		h.userLookupCache,
		h.flagCache,
		5*time.Minute,
	)
	return h
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
			name:        "Handle company partial message (no existing cache)",
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
			h := newTestHarness(t)

			companyData, err := json.Marshal(tt.company)
			assert.NoError(t, err)

			message := &schematicdatastreamws.DataStreamResp{
				EntityType:  string(schematicdatastreamws.EntityTypeCompany),
				MessageType: tt.messageType,
				Data:        json.RawMessage(companyData),
			}
			// Partial messages identify the cached entity via top-level entity_id.
			if tt.messageType == schematicdatastreamws.MessageTypePartial {
				id := tt.company.ID
				message.EntityID = &id
			}

			// Setup expectations
			if tt.messageType == schematicdatastreamws.MessageTypeDelete {
				idKey := companyIDCacheKey(tt.company.ID)
				h.companyCache.On("Delete", mock.Anything, idKey).Return(nil)
				for key, value := range tt.company.Keys {
					cacheKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
					h.companyLookupCache.On("Delete", mock.Anything, cacheKey).Return(nil)
				}
			} else if tt.messageType == schematicdatastreamws.MessageTypePartial {
				// Partial with cache miss: expect Get (not found), skip without Set
				idKey := companyIDCacheKey(tt.company.ID)
				var nilCompany *rulesengine.Company
				h.companyCache.On("Get", mock.Anything, idKey).Return(nilCompany, fmt.Errorf("not found"))
			} else {
				idKey := companyIDCacheKey(tt.company.ID)
				h.companyCache.On("Set", mock.Anything, idKey, tt.company, 5*time.Minute).Return(nil)
				for key, value := range tt.company.Keys {
					cacheKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
					h.companyLookupCache.On("Set", mock.Anything, cacheKey, tt.company.ID, 5*time.Minute).Return(nil)
				}
			}

			ctx := context.Background()
			err = h.handler.HandleMessage(ctx, message)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			h.companyCache.AssertExpectations(t)
			h.companyLookupCache.AssertExpectations(t)
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
			name:        "Handle user partial message (no existing cache)",
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
			h := newTestHarness(t)

			userData, err := json.Marshal(tt.user)
			assert.NoError(t, err)

			message := &schematicdatastreamws.DataStreamResp{
				EntityType:  string(schematicdatastreamws.EntityTypeUser),
				MessageType: tt.messageType,
				Data:        json.RawMessage(userData),
			}
			// Partial messages identify the cached entity via top-level entity_id.
			if tt.messageType == schematicdatastreamws.MessageTypePartial {
				id := tt.user.ID
				message.EntityID = &id
			}

			if tt.messageType == schematicdatastreamws.MessageTypeDelete {
				idKey := userIDCacheKey(tt.user.ID)
				h.userCache.On("Delete", mock.Anything, idKey).Return(nil)
				for key, value := range tt.user.Keys {
					cacheKey := resourceKeyToCacheKey(cacheKeyPrefixUser, key, value)
					h.userLookupCache.On("Delete", mock.Anything, cacheKey).Return(nil)
				}
			} else if tt.messageType == schematicdatastreamws.MessageTypePartial {
				idKey := userIDCacheKey(tt.user.ID)
				var nilUser *rulesengine.User
				h.userCache.On("Get", mock.Anything, idKey).Return(nilUser, fmt.Errorf("not found"))
			} else {
				idKey := userIDCacheKey(tt.user.ID)
				h.userCache.On("Set", mock.Anything, idKey, tt.user, 5*time.Minute).Return(nil)
				for key, value := range tt.user.Keys {
					cacheKey := resourceKeyToCacheKey(cacheKeyPrefixUser, key, value)
					h.userLookupCache.On("Set", mock.Anything, cacheKey, tt.user.ID, 5*time.Minute).Return(nil)
				}
			}

			ctx := context.Background()
			err = h.handler.HandleMessage(ctx, message)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			h.userCache.AssertExpectations(t)
			h.userLookupCache.AssertExpectations(t)
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
			h := newTestHarness(t)

			var message *schematicdatastreamws.DataStreamResp
			if tt.messageType == schematicdatastreamws.MessageTypeDelete {
				deleteData := map[string]string{"key": tt.flag.Key, "id": tt.flag.ID}
				flagData, err := json.Marshal(deleteData)
				assert.NoError(t, err)
				message = &schematicdatastreamws.DataStreamResp{
					EntityType:  string(schematicdatastreamws.EntityTypeFlag),
					MessageType: tt.messageType,
					Data:        json.RawMessage(flagData),
				}
			} else {
				flagData, err := json.Marshal(tt.flag)
				assert.NoError(t, err)
				message = &schematicdatastreamws.DataStreamResp{
					EntityType:  string(schematicdatastreamws.EntityTypeFlag),
					MessageType: tt.messageType,
					Data:        json.RawMessage(flagData),
				}
			}

			if tt.messageType == schematicdatastreamws.MessageTypeDelete {
				cacheKey := flagCacheKey(tt.flag.Key)
				h.flagCache.On("Delete", mock.Anything, cacheKey).Return(nil)
			} else {
				cacheKey := flagCacheKey(tt.flag.Key)
				h.flagCache.On("Set", mock.Anything, cacheKey, tt.flag, 5*time.Minute).Return(nil)
			}

			ctx := context.Background()
			err := h.handler.HandleMessage(ctx, message)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			h.flagCache.AssertExpectations(t)
		})
	}
}

func TestReplicatorMessageHandler_HandleFlagsMessage(t *testing.T) {
	t.Run("Handle bulk flags message with DeleteMissing", func(t *testing.T) {
		h := newTestHarness(t)

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

		var expectedCacheKeys []string
		for _, flag := range flags {
			cacheKey := flagCacheKey(flag.Key)
			expectedCacheKeys = append(expectedCacheKeys, cacheKey)
			h.flagCache.On("Set", mock.Anything, cacheKey, flag, 5*time.Minute).Return(nil)
		}
		h.flagCache.On("DeleteMissing", mock.Anything, expectedCacheKeys).Return(nil)

		ctx := context.Background()
		err = h.handler.HandleMessage(ctx, message)
		assert.NoError(t, err)
		h.flagCache.AssertExpectations(t)
	})
}

func TestReplicatorMessageHandler_SingleVsBulkFlagProcessing(t *testing.T) {
	t.Run("Single flag processing does NOT call DeleteMissing", func(t *testing.T) {
		h := newTestHarness(t)

		flag := createTestFlag(t)
		flagData, err := json.Marshal(flag)
		assert.NoError(t, err)

		message := &schematicdatastreamws.DataStreamResp{
			EntityType:  string(schematicdatastreamws.EntityTypeFlag),
			MessageType: schematicdatastreamws.MessageTypeFull,
			Data:        json.RawMessage(flagData),
		}

		cacheKey := flagCacheKey(flag.Key)
		h.flagCache.On("Set", mock.Anything, cacheKey, flag, 5*time.Minute).Return(nil)

		ctx := context.Background()
		err = h.handler.HandleMessage(ctx, message)
		assert.NoError(t, err)
		h.flagCache.AssertExpectations(t)
	})
}

func TestPartialCompanyMergesWithExistingCache(t *testing.T) {
	h := newTestHarness(t)

	existing := createTestCompany(t)
	idKey := companyIDCacheKey(existing.ID)
	h.companyCache.On("Get", mock.Anything, idKey).Return(existing, nil)

	// Wire shape from the API: data is wrapped under the field name (e.g.
	// "traits"), no top-level id. Cache lookup uses the envelope's entity_id.
	id := existing.ID
	partialData := json.RawMessage(`{"traits":[{"value":"Startup","trait_definition":{"id":"plan","entity_type":"company"}}]}`)
	message := &schematicdatastreamws.DataStreamResp{
		EntityType:  string(schematicdatastreamws.EntityTypeCompany),
		EntityID:    &id,
		MessageType: schematicdatastreamws.MessageTypePartial,
		Data:        partialData,
	}

	h.companyCache.On("Set", mock.Anything, idKey, mock.MatchedBy(func(c *rulesengine.Company) bool {
		return c.ID == "company-123" &&
			c.AccountID == "account-456" &&
			c.EnvironmentID == "env-789" &&
			len(c.Traits) == 1 &&
			c.Traits[0].Value == "Startup" &&
			len(c.Keys) == 2
	}), 5*time.Minute).Return(nil)

	for key, value := range existing.Keys {
		cacheKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
		h.companyLookupCache.On("Set", mock.Anything, cacheKey, existing.ID, 5*time.Minute).Return(nil)
	}

	ctx := context.Background()
	err := h.handler.HandleMessage(ctx, message)
	assert.NoError(t, err)
	h.companyCache.AssertExpectations(t)
	h.companyLookupCache.AssertExpectations(t)
}

func TestPartialUserMergesWithExistingCache(t *testing.T) {
	h := newTestHarness(t)

	existing := createTestUser(t)
	idKey := userIDCacheKey(existing.ID)
	h.userCache.On("Get", mock.Anything, idKey).Return(existing, nil)

	// Wire shape from the API: data is wrapped under the field name (e.g.
	// "traits"), no top-level id. Cache lookup uses the envelope's entity_id.
	id := existing.ID
	partialData := json.RawMessage(`{"traits":[{"value":"Free","trait_definition":{"id":"tier","entity_type":"user"}}]}`)
	message := &schematicdatastreamws.DataStreamResp{
		EntityType:  string(schematicdatastreamws.EntityTypeUser),
		EntityID:    &id,
		MessageType: schematicdatastreamws.MessageTypePartial,
		Data:        partialData,
	}

	h.userCache.On("Set", mock.Anything, idKey, mock.MatchedBy(func(u *rulesengine.User) bool {
		return u.ID == "user-123" &&
			u.AccountID == "account-456" &&
			u.EnvironmentID == "env-789" &&
			len(u.Traits) == 1 &&
			u.Traits[0].Value == "Free" &&
			len(u.Keys) == 2
	}), 5*time.Minute).Return(nil)

	for key, value := range existing.Keys {
		cacheKey := resourceKeyToCacheKey(cacheKeyPrefixUser, key, value)
		h.userLookupCache.On("Set", mock.Anything, cacheKey, existing.ID, 5*time.Minute).Return(nil)
	}

	ctx := context.Background()
	err := h.handler.HandleMessage(ctx, message)
	assert.NoError(t, err)
	h.userCache.AssertExpectations(t)
	h.userLookupCache.AssertExpectations(t)
}

func TestFullThenPartialCompany(t *testing.T) {
	h := newTestHarness(t)

	company := createTestCompany(t)
	idKey := companyIDCacheKey(company.ID)

	// Step 1: Full message
	fullData, err := json.Marshal(company)
	assert.NoError(t, err)

	fullMsg := &schematicdatastreamws.DataStreamResp{
		EntityType:  string(schematicdatastreamws.EntityTypeCompany),
		MessageType: schematicdatastreamws.MessageTypeFull,
		Data:        json.RawMessage(fullData),
	}

	h.companyCache.On("Set", mock.Anything, idKey, company, 5*time.Minute).Return(nil).Once()
	for key, value := range company.Keys {
		cacheKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
		h.companyLookupCache.On("Set", mock.Anything, cacheKey, company.ID, 5*time.Minute).Return(nil)
	}

	ctx := context.Background()
	err = h.handler.HandleMessage(ctx, fullMsg)
	assert.NoError(t, err)

	// Step 2: Partial message — Get returns the full company
	h.companyCache.On("Get", mock.Anything, idKey).Return(company, nil)

	id := company.ID
	partialData := json.RawMessage(`{"traits":[]}`)
	partialMsg := &schematicdatastreamws.DataStreamResp{
		EntityType:  string(schematicdatastreamws.EntityTypeCompany),
		EntityID:    &id,
		MessageType: schematicdatastreamws.MessageTypePartial,
		Data:        partialData,
	}

	h.companyCache.On("Set", mock.Anything, idKey, mock.MatchedBy(func(c *rulesengine.Company) bool {
		return c.ID == "company-123" &&
			c.AccountID == "account-456" &&
			len(c.Traits) == 0 &&
			len(c.Keys) == 2
	}), 5*time.Minute).Return(nil).Once()

	err = h.handler.HandleMessage(ctx, partialMsg)
	assert.NoError(t, err)
	h.companyCache.AssertExpectations(t)
}

func TestPartialThenFullCompany(t *testing.T) {
	h := newTestHarness(t)

	company := createTestCompany(t)
	idKey := companyIDCacheKey(company.ID)

	// Step 1: Partial with cache miss — skipped
	id := company.ID
	partialData := json.RawMessage(`{"traits":[]}`)
	partialMsg := &schematicdatastreamws.DataStreamResp{
		EntityType:  string(schematicdatastreamws.EntityTypeCompany),
		EntityID:    &id,
		MessageType: schematicdatastreamws.MessageTypePartial,
		Data:        partialData,
	}

	var nilCompany *rulesengine.Company
	h.companyCache.On("Get", mock.Anything, idKey).Return(nilCompany, fmt.Errorf("not found")).Once()

	ctx := context.Background()
	err := h.handler.HandleMessage(ctx, partialMsg)
	assert.NoError(t, err)

	// Step 2: Full message overwrites everything
	fullData, err := json.Marshal(company)
	assert.NoError(t, err)

	fullMsg := &schematicdatastreamws.DataStreamResp{
		EntityType:  string(schematicdatastreamws.EntityTypeCompany),
		MessageType: schematicdatastreamws.MessageTypeFull,
		Data:        json.RawMessage(fullData),
	}

	h.companyCache.On("Set", mock.Anything, idKey, company, 5*time.Minute).Return(nil).Once()
	for key, value := range company.Keys {
		cacheKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
		h.companyLookupCache.On("Set", mock.Anything, cacheKey, company.ID, 5*time.Minute).Return(nil)
	}

	err = h.handler.HandleMessage(ctx, fullMsg)
	assert.NoError(t, err)
	h.companyCache.AssertExpectations(t)
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
