package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/schematichq/rulesengine"
	"github.com/schematichq/rulesengine/typeconvert"
	schematicdatastreamws "github.com/schematichq/schematic-datastream-ws"
	schematicgo "github.com/schematichq/schematic-go"
	"github.com/schematichq/schematic-go/client"
)

// ReplicatorMessageHandler implements the MessageHandler interface
type ReplicatorMessageHandler struct {
	logger         *SchematicLogger
	companiesCache CacheProvider[*rulesengine.Company]
	usersCache     CacheProvider[*rulesengine.User]
	flagsCache     CacheProvider[*rulesengine.Flag]
	cacheTTL       time.Duration

	// Mutexes for cache operations (matching schematic-go pattern)
	flagsMu   sync.RWMutex
	companyMu sync.RWMutex
	userMu    sync.RWMutex
}

// NewReplicatorMessageHandler creates a new message handler
func NewReplicatorMessageHandler(
	logger *SchematicLogger,
	companiesCache CacheProvider[*rulesengine.Company],
	usersCache CacheProvider[*rulesengine.User],
	flagsCache CacheProvider[*rulesengine.Flag],
	cacheTTL time.Duration,
) *ReplicatorMessageHandler {
	return &ReplicatorMessageHandler{
		logger:         logger,
		companiesCache: companiesCache,
		usersCache:     usersCache,
		flagsCache:     flagsCache,
		cacheTTL:       cacheTTL,
	}
}

// HandleMessage implements the MessageHandler interface
func (h *ReplicatorMessageHandler) HandleMessage(ctx context.Context, message *schematicdatastreamws.DataStreamResp) error {
	if message.Data == nil {
		h.logger.Debug(ctx, "Received empty message from datastream")
		return nil
	}

	// Handle error messages
	if message.MessageType == schematicdatastreamws.MessageTypeError {
		return h.handleErrorMessage(ctx, message)
	}

	// Route message based on entity type
	switch message.EntityType {
	case string(schematicdatastreamws.EntityTypeFlags):
		return h.handleFlagsMessage(ctx, message)
	case string(schematicdatastreamws.EntityTypeFlag):
		return h.handleFlagMessage(ctx, message)
	case string(schematicdatastreamws.EntityTypeCompany):
		return h.handleCompanyMessage(ctx, message)
	case string(schematicdatastreamws.EntityTypeUser):
		return h.handleUserMessage(ctx, message)
	case string(schematicdatastreamws.EntityTypeCompanies):
		return h.handleCompaniesMessage(ctx, message)
	case string(schematicdatastreamws.EntityTypeUsers):
		return h.handleUsersMessage(ctx, message)
	default:
		h.logger.Debug(ctx, fmt.Sprintf("Unhandled entity type: %s", message.EntityType))
		return nil
	}
}

// Error message handling
func (h *ReplicatorMessageHandler) handleErrorMessage(ctx context.Context, message *schematicdatastreamws.DataStreamResp) error {
	var respError schematicdatastreamws.DataStreamError
	if err := json.Unmarshal(message.Data, &respError); err != nil {
		h.logger.Error(ctx, fmt.Sprintf("Failed to unmarshal error message: %v", err))
		return err
	}

	h.logger.Error(ctx, fmt.Sprintf("Datastream error: %s", respError.Error))
	return fmt.Errorf("datastream error: %s", respError.Error)
}

// Flags handling
func (h *ReplicatorMessageHandler) handleFlagsMessage(ctx context.Context, message *schematicdatastreamws.DataStreamResp) error {
	switch message.MessageType {
	case schematicdatastreamws.MessageTypeFull, schematicdatastreamws.MessageTypePartial:
		// For flags, we expect an array of rulesengine.Flag
		var flags []*rulesengine.Flag
		if err := json.Unmarshal(message.Data, &flags); err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to unmarshal flags data: %v", err))
			return err
		}

		// Lock for cache operations (matching schematic-go pattern)
		h.flagsMu.Lock()
		var cacheKeys []string
		for _, flag := range flags {
			if flag != nil {
				cacheKey := flagCacheKey(flag.Key)
				if err := h.flagsCache.Set(ctx, cacheKey, flag, h.cacheTTL); err != nil {
					h.logger.Error(ctx, fmt.Sprintf("Failed to cache flag %s: %v", flag.Key, err))
				} else {
					h.logger.Debug(ctx, fmt.Sprintf("Cached flag: %s", flag.Key))
				}
				cacheKeys = append(cacheKeys, cacheKey)
			}
		}

		// Delete missing flags (matching schematic-go behavior)
		if err := h.flagsCache.DeleteMissing(ctx, cacheKeys); err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to delete missing flags: %v", err))
		}
		h.flagsMu.Unlock()

		h.logger.Info(ctx, fmt.Sprintf("Successfully cached %d flags", len(flags)))

	case schematicdatastreamws.MessageTypeDelete:
		// For delete, we expect just the flag key or ID
		var deleteData struct {
			Key string `json:"key,omitempty"`
			ID  string `json:"id,omitempty"`
		}
		if err := json.Unmarshal(message.Data, &deleteData); err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to unmarshal flag delete data: %v", err))
			return err
		}

		flagKey := deleteData.Key
		if flagKey == "" {
			flagKey = deleteData.ID
		}

		if flagKey != "" {
			h.flagsMu.Lock()
			cacheKey := flagCacheKey(flagKey)
			if err := h.flagsCache.Delete(ctx, cacheKey); err != nil {
				h.logger.Error(ctx, fmt.Sprintf("Failed to delete flag from cache: %v", err))
			} else {
				h.logger.Debug(ctx, fmt.Sprintf("Deleted flag from cache: %s", flagKey))
			}
			h.flagsMu.Unlock()
		}
	}

	return nil
}

// handleFlagMessage handles single flag messages (EntityTypeFlag)
func (h *ReplicatorMessageHandler) handleFlagMessage(ctx context.Context, message *schematicdatastreamws.DataStreamResp) error {
	switch message.MessageType {
	case schematicdatastreamws.MessageTypeFull, schematicdatastreamws.MessageTypePartial:
		// For single flag, we expect a single rulesengine.Flag
		var flag *rulesengine.Flag
		if err := json.Unmarshal(message.Data, &flag); err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to unmarshal single flag data: %v", err))
			return err
		}

		if flag != nil {
			// Lock for cache operations (matching schematic-go pattern)
			h.flagsMu.Lock()
			cacheKey := flagCacheKey(flag.Key)
			if err := h.flagsCache.Set(ctx, cacheKey, flag, h.cacheTTL); err != nil {
				h.logger.Error(ctx, fmt.Sprintf("Failed to cache single flag %s: %v", flag.Key, err))
			} else {
				h.logger.Debug(ctx, fmt.Sprintf("Cached single flag: %s", flag.Key))
			}
			h.flagsMu.Unlock()
		}

	case schematicdatastreamws.MessageTypeDelete:
		// For delete, we expect just the flag key or ID
		var deleteData struct {
			Key string `json:"key,omitempty"`
			ID  string `json:"id,omitempty"`
		}
		if err := json.Unmarshal(message.Data, &deleteData); err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to unmarshal single flag delete data: %v", err))
			return err
		}

		flagKey := deleteData.Key
		if flagKey == "" {
			flagKey = deleteData.ID
		}

		if flagKey != "" {
			h.flagsMu.Lock()
			cacheKey := flagCacheKey(flagKey)
			if err := h.flagsCache.Delete(ctx, cacheKey); err != nil {
				h.logger.Error(ctx, fmt.Sprintf("Failed to delete single flag from cache: %v", err))
			} else {
				h.logger.Debug(ctx, fmt.Sprintf("Deleted single flag from cache: %s", flagKey))
			}
			h.flagsMu.Unlock()
		}

	default:
		h.logger.Debug(ctx, fmt.Sprintf("Unhandled single flag message type: %s", message.MessageType))
	}

	return nil
}

// Company handling
func (h *ReplicatorMessageHandler) handleCompanyMessage(ctx context.Context, message *schematicdatastreamws.DataStreamResp) error {
	h.logger.Debug(ctx, fmt.Sprintf("Received company message: %v", message))
	switch message.MessageType {
	case schematicdatastreamws.MessageTypeFull, schematicdatastreamws.MessageTypePartial:
		// Data comes as rulesengine.Company directly
		var company *rulesengine.Company
		if err := json.Unmarshal(message.Data, &company); err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to unmarshal company data: %v", err))
			return err
		}

		if company == nil {
			return nil
		}

		// Lock for cache operations and cache company for all keys (matching schematic-go pattern)
		h.companyMu.Lock()
		cacheResults := h.cacheCompanyForKeys(ctx, company)
		h.companyMu.Unlock()

		// Check for cache errors and log warnings (non-blocking like schematic-go)
		for cacheKey, cacheErr := range cacheResults {
			if cacheErr != nil {
				h.logger.Warn(ctx, fmt.Sprintf("Cache error for company key '%s': %v", cacheKey, cacheErr))
			}
		}

		if len(cacheResults) > 0 {
			h.logger.Debug(ctx, fmt.Sprintf("Cached company with %d keys: %s", len(cacheResults), company.ID))
		}

	case schematicdatastreamws.MessageTypeDelete:
		// For delete, expect company data with keys (matching schematic-go)
		var company *rulesengine.Company
		if err := json.Unmarshal(message.Data, &company); err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to unmarshal company delete data: %v", err))
			return err
		}

		if company == nil {
			return nil
		}

		// Delete company from cache for all keys (matching schematic-go behavior)
		h.companyMu.Lock()
		for key, value := range company.Keys {
			companyKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
			if err := h.companiesCache.Delete(ctx, companyKey); err != nil {
				h.logger.Warn(ctx, fmt.Sprintf("Failed to delete company from cache for key '%s': %v", companyKey, err))
			}
		}
		h.companyMu.Unlock()

		h.logger.Debug(ctx, fmt.Sprintf("Deleted company from cache: %s", company.ID))
	}

	return nil
}

// Helper method to cache company for all keys (matching schematic-go pattern)
func (h *ReplicatorMessageHandler) cacheCompanyForKeys(ctx context.Context, company *rulesengine.Company) map[string]error {
	if company == nil || len(company.Keys) == 0 {
		return nil
	}

	// Map to track which cache keys were successfully cached and which ones failed
	cacheResults := make(map[string]error)

	// Try to cache the company for all keys
	for key, value := range company.Keys {
		companyKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
		err := h.companiesCache.Set(ctx, companyKey, company, h.cacheTTL)
		// Store the result for each cache key
		cacheResults[companyKey] = err
	}

	return cacheResults
}

// Helper method to cache user for all keys (matching schematic-go pattern)
func (h *ReplicatorMessageHandler) cacheUserForKeys(ctx context.Context, user *rulesengine.User) map[string]error {
	if user == nil || len(user.Keys) == 0 {
		return nil
	}

	// Map to track which cache keys were successfully cached and which ones failed
	cacheResults := make(map[string]error)

	// Try to cache the user for all keys
	for key, value := range user.Keys {
		userKey := resourceKeyToCacheKey(cacheKeyPrefixUser, key, value)
		err := h.usersCache.Set(ctx, userKey, user, h.cacheTTL)
		// Store the result for each cache key
		cacheResults[userKey] = err
	}

	return cacheResults
}

// User handling
func (h *ReplicatorMessageHandler) handleUserMessage(ctx context.Context, message *schematicdatastreamws.DataStreamResp) error {
	switch message.MessageType {
	case schematicdatastreamws.MessageTypeFull, schematicdatastreamws.MessageTypePartial:
		// Data comes as rulesengine.User directly
		var user *rulesengine.User
		if err := json.Unmarshal(message.Data, &user); err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to unmarshal user data: %v", err))
			return err
		}

		if user == nil {
			return nil
		}

		// Lock for cache operations and cache user for all keys (matching schematic-go pattern)
		h.userMu.Lock()
		cacheResults := h.cacheUserForKeys(ctx, user)
		h.userMu.Unlock()

		// Check for cache errors and log warnings (non-blocking like schematic-go)
		for cacheKey, cacheErr := range cacheResults {
			if cacheErr != nil {
				h.logger.Warn(ctx, fmt.Sprintf("Cache error for user key '%s': %v", cacheKey, cacheErr))
			}
		}

		if len(cacheResults) > 0 {
			h.logger.Debug(ctx, fmt.Sprintf("Cached user with %d keys: %s", len(cacheResults), user.ID))
		}

	case schematicdatastreamws.MessageTypeDelete:
		// For delete, expect user data with keys (matching schematic-go)
		var user *rulesengine.User
		if err := json.Unmarshal(message.Data, &user); err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to unmarshal user delete data: %v", err))
			return err
		}

		if user == nil {
			return nil
		}

		// Delete user from cache for all keys (matching schematic-go behavior)
		h.userMu.Lock()
		for key, value := range user.Keys {
			userKey := resourceKeyToCacheKey(cacheKeyPrefixUser, key, value)
			if err := h.usersCache.Delete(ctx, userKey); err != nil {
				h.logger.Warn(ctx, fmt.Sprintf("Failed to delete user from cache for key '%s': %v", userKey, err))
			}
		}
		h.userMu.Unlock()

		h.logger.Debug(ctx, fmt.Sprintf("Deleted user from cache: %s", user.ID))
	}

	return nil
}

// Companies handling (bulk subscription confirmation)
func (h *ReplicatorMessageHandler) handleCompaniesMessage(ctx context.Context, message *schematicdatastreamws.DataStreamResp) error {
	// For bulk company subscriptions, we typically get confirmation messages or individual company updates
	switch message.MessageType {
	case schematicdatastreamws.MessageTypeFull:
		// This is likely a confirmation message from the server
		var confirmationData map[string]interface{}
		if err := json.Unmarshal(message.Data, &confirmationData); err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to unmarshal companies confirmation data: %v", err))
			return err
		}
		h.logger.Info(ctx, fmt.Sprintf("Companies subscription confirmed: %v", confirmationData))
	default:
		h.logger.Debug(ctx, fmt.Sprintf("Unhandled companies message type: %s", message.MessageType))
	}
	return nil
}

// Users handling (bulk subscription confirmation)
func (h *ReplicatorMessageHandler) handleUsersMessage(ctx context.Context, message *schematicdatastreamws.DataStreamResp) error {
	// For bulk user subscriptions, we typically get confirmation messages or individual user updates
	switch message.MessageType {
	case schematicdatastreamws.MessageTypeFull:
		// This is likely a confirmation message from the server
		var confirmationData map[string]interface{}
		if err := json.Unmarshal(message.Data, &confirmationData); err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to unmarshal users confirmation data: %v", err))
			return err
		}
		h.logger.Info(ctx, fmt.Sprintf("Users subscription confirmed: %v", confirmationData))
	default:
		h.logger.Debug(ctx, fmt.Sprintf("Unhandled users message type: %s", message.MessageType))
	}
	return nil
}

// ConnectionReadyHandler implements the ConnectionReadyHandler interface for synchronous loading
type ConnectionReadyHandler struct {
	schematicClient *client.Client
	wsClient        *schematicdatastreamws.Client
	companiesCache  CacheProvider[*rulesengine.Company]
	usersCache      CacheProvider[*rulesengine.User]
	flagsCache      CacheProvider[*rulesengine.Flag]
	logger          *SchematicLogger
	cacheTTL        time.Duration
}

// NewConnectionReadyHandler creates a new connection ready handler with synchronous loading (default)
func NewConnectionReadyHandler(
	schematicClient *client.Client,
	wsClient *schematicdatastreamws.Client,
	companiesCache CacheProvider[*rulesengine.Company],
	usersCache CacheProvider[*rulesengine.User],
	flagsCache CacheProvider[*rulesengine.Flag],
	logger *SchematicLogger,
	cacheTTL time.Duration,
) *ConnectionReadyHandler {
	return &ConnectionReadyHandler{
		schematicClient: schematicClient,
		wsClient:        wsClient,
		companiesCache:  companiesCache,
		usersCache:      usersCache,
		flagsCache:      flagsCache,
		logger:          logger,
		cacheTTL:        cacheTTL,
	}
}

// SetWebSocketClient sets the WebSocket client after it's created
func (h *ConnectionReadyHandler) SetWebSocketClient(wsClient *schematicdatastreamws.Client) {
	h.wsClient = wsClient
}

// OnConnectionReady implements the ConnectionReadyHandler interface for synchronous loading
func (h *ConnectionReadyHandler) OnConnectionReady(ctx context.Context) error {
	// Synchronous mode: wait for all data to load before completing
	h.logger.Info(ctx, "Starting synchronous initial data loading")

	// 1. Load companies from Schematic API
	if err := h.loadAndCacheCompanies(ctx); err != nil {
		return fmt.Errorf("failed to load companies: %w", err)
	}

	// 2. Load users from Schematic API
	if err := h.loadAndCacheUsers(ctx); err != nil {
		return fmt.Errorf("failed to load users: %w", err)
	}

	// 3. Subscribe to company and user updates via websocket
	if err := h.subscribeToUpdates(ctx); err != nil {
		return fmt.Errorf("failed to subscribe to updates: %w", err)
	}

	// 4. Request flags data from datastream
	if err := h.requestFlagsData(ctx); err != nil {
		return fmt.Errorf("failed to request flags data: %w", err)
	}

	h.logger.Info(ctx, "Connection ready setup completed with synchronous loading")
	return nil
}

func (h *ConnectionReadyHandler) loadAndCacheCompanies(ctx context.Context) error {
	h.logger.Info(ctx, "Loading companies from Schematic API")

	pageSize := 100
	offset := 0
	totalCompanies := 0
	var allCacheKeys []string // Track all successfully cached keys for eviction

	for {
		h.logger.Debug(ctx, fmt.Sprintf("Fetching companies page: offset=%d, limit=%d", offset, pageSize))

		// Fetch companies page
		companiesResp, err := h.schematicClient.Companies.ListCompanies(ctx, &schematicgo.ListCompaniesRequest{
			Limit:  &pageSize,
			Offset: &offset,
		})
		if err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to fetch companies (offset=%d): %v", offset, err))
			return err
		}

		h.logger.Debug(ctx, fmt.Sprintf("Retrieved %d companies from page (offset=%d)", len(companiesResp.Data), offset))

		// Convert and cache all companies from this page
		for _, companyData := range companiesResp.Data {
			company := convertToRulesEngineCompany(companyData)

			h.logger.Debug(ctx, fmt.Sprintf("Company %s has %d keys: %v", company.ID, len(company.Keys), company.Keys))

			// Cache the company with all its key-value pairs (matching schematic-go behavior)
			cacheResults := h.cacheCompanyForKeys(ctx, company)
			for cacheKey, cacheErr := range cacheResults {
				if cacheErr != nil {
					h.logger.Error(ctx, fmt.Sprintf("Cache error for company %s key '%s': %v", company.ID, cacheKey, cacheErr))
				} else {
					h.logger.Debug(ctx, fmt.Sprintf("Successfully cached company key '%s'", cacheKey))
					allCacheKeys = append(allCacheKeys, cacheKey) // Track successfully cached keys
					// Additional debug: verify the key exists immediately
					if _, err := h.companiesCache.Get(ctx, cacheKey); err != nil {
						h.logger.Error(ctx, fmt.Sprintf("Key '%s' not found immediately after caching: %v", cacheKey, err))
					}
				}
			}

			if len(cacheResults) > 0 {
				h.logger.Debug(ctx, fmt.Sprintf("Cached company with %d keys: %s", len(cacheResults), company.ID))
			} else {
				h.logger.Warn(ctx, fmt.Sprintf("Company %s has no keys to cache", company.ID))
			}
		}

		totalCompanies += len(companiesResp.Data)

		// Check if we got fewer results than the page size, indicating we've reached the end
		if len(companiesResp.Data) < pageSize {
			h.logger.Debug(ctx, fmt.Sprintf("Reached end of companies list (got %d < %d)", len(companiesResp.Data), pageSize))
			break
		}

		// Move to next page
		offset += pageSize
	}

	// Evict any cached company keys that are not present in the retrieved dataset
	if len(allCacheKeys) > 0 {
		h.logger.Debug(ctx, fmt.Sprintf("Evicting missing company cache keys (keeping %d keys)", len(allCacheKeys)))
		if err := h.companiesCache.DeleteMissing(ctx, allCacheKeys); err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to evict missing company cache keys: %v", err))
		} else {
			h.logger.Debug(ctx, "Successfully evicted missing company cache keys")
		}
	}

	h.logger.Info(ctx, fmt.Sprintf("Successfully cached %d companies across all pages", totalCompanies))
	return nil
}

func (h *ConnectionReadyHandler) loadAndCacheUsers(ctx context.Context) error {
	h.logger.Info(ctx, "Loading users from Schematic API")

	pageSize := 100
	offset := 0
	totalUsers := 0
	var allCacheKeys []string // Track all successfully cached keys for eviction

	for {
		h.logger.Debug(ctx, fmt.Sprintf("Fetching users page: offset=%d, limit=%d", offset, pageSize))

		// Fetch users page
		usersResp, err := h.schematicClient.Companies.ListUsers(ctx, &schematicgo.ListUsersRequest{
			Limit:  &pageSize,
			Offset: &offset,
		})
		if err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to fetch users (offset=%d): %v", offset, err))
			return err
		}

		h.logger.Debug(ctx, fmt.Sprintf("Retrieved %d users from page (offset=%d)", len(usersResp.Data), offset))

		// Convert and cache all users from this page
		for _, userData := range usersResp.Data {
			user := convertToRulesEngineUser(userData)

			h.logger.Debug(ctx, fmt.Sprintf("User %s has %d keys: %v", user.ID, len(user.Keys), user.Keys))

			// Cache the user with all its key-value pairs (matching schematic-go behavior)
			cacheResults := h.cacheUserForKeys(ctx, user)
			for cacheKey, cacheErr := range cacheResults {
				if cacheErr != nil {
					h.logger.Error(ctx, fmt.Sprintf("Cache error for user %s key '%s': %v", user.ID, cacheKey, cacheErr))
				} else {
					h.logger.Debug(ctx, fmt.Sprintf("Successfully cached user key '%s'", cacheKey))
					allCacheKeys = append(allCacheKeys, cacheKey) // Track successfully cached keys
				}
			}

			if len(cacheResults) > 0 {
				h.logger.Debug(ctx, fmt.Sprintf("Cached user with %d keys: %s", len(cacheResults), user.ID))
			} else {
				h.logger.Warn(ctx, fmt.Sprintf("User %s has no keys to cache", user.ID))
			}
		}

		totalUsers += len(usersResp.Data)

		// Check if we got fewer results than the page size, indicating we've reached the end
		if len(usersResp.Data) < pageSize {
			h.logger.Debug(ctx, fmt.Sprintf("Reached end of users list (got %d < %d)", len(usersResp.Data), pageSize))
			break
		}

		// Move to next page
		offset += pageSize
	}

	// Evict any cached user keys that are not present in the retrieved dataset
	if len(allCacheKeys) > 0 {
		h.logger.Debug(ctx, fmt.Sprintf("Evicting missing user cache keys (keeping %d keys)", len(allCacheKeys)))
		if err := h.usersCache.DeleteMissing(ctx, allCacheKeys); err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to evict missing user cache keys: %v", err))
		} else {
			h.logger.Debug(ctx, "Successfully evicted missing user cache keys")
		}
	}

	h.logger.Info(ctx, fmt.Sprintf("Successfully cached %d users across all pages", totalUsers))
	return nil
}

func (h *ConnectionReadyHandler) subscribeToUpdates(ctx context.Context) error {
	h.logger.Info(ctx, "Subscribing to company and user updates")

	// Subscribe to all company updates using bulk subscription entity type
	companySubscription := &schematicdatastreamws.DataStreamBaseReq{
		Data: schematicdatastreamws.DataStreamReq{
			Action:     schematicdatastreamws.ActionStart,
			EntityType: schematicdatastreamws.EntityTypeCompanies, // Changed to EntityTypeCompanies for bulk subscription
		},
	}
	if err := h.wsClient.SendMessage(companySubscription); err != nil {
		h.logger.Error(ctx, fmt.Sprintf("Failed to subscribe to company updates: %v", err))
		return err
	}

	// Subscribe to all user updates using bulk subscription entity type
	userSubscription := &schematicdatastreamws.DataStreamBaseReq{
		Data: schematicdatastreamws.DataStreamReq{
			Action:     schematicdatastreamws.ActionStart,
			EntityType: schematicdatastreamws.EntityTypeUsers, // Changed to EntityTypeUsers for bulk subscription
		},
	}
	if err := h.wsClient.SendMessage(userSubscription); err != nil {
		h.logger.Error(ctx, fmt.Sprintf("Failed to subscribe to user updates: %v", err))
		return err
	}

	h.logger.Info(ctx, "Successfully subscribed to company and user updates")
	return nil
}

func (h *ConnectionReadyHandler) requestFlagsData(ctx context.Context) error {
	h.logger.Info(ctx, "Requesting flags data from datastream")

	// Request flags data using datastream format
	flagsRequest := &schematicdatastreamws.DataStreamBaseReq{
		Data: schematicdatastreamws.DataStreamReq{
			Action:     schematicdatastreamws.ActionStart,
			EntityType: schematicdatastreamws.EntityTypeFlags,
		},
	}
	if err := h.wsClient.SendMessage(flagsRequest); err != nil {
		h.logger.Error(ctx, fmt.Sprintf("Failed to request flags data: %v", err))
		return err
	}

	h.logger.Info(ctx, "Successfully requested flags data")
	return nil
}

func (h *ConnectionReadyHandler) cacheCompanyForKeys(ctx context.Context, company *rulesengine.Company) map[string]error {
	if company == nil || len(company.Keys) == 0 {
		return nil
	}

	cacheResults := make(map[string]error)

	for key, value := range company.Keys {
		companyKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
		err := h.companiesCache.Set(ctx, companyKey, company, h.cacheTTL)
		cacheResults[companyKey] = err
	}

	return cacheResults
}

func (h *ConnectionReadyHandler) cacheUserForKeys(ctx context.Context, user *rulesengine.User) map[string]error {
	if user == nil || len(user.Keys) == 0 {
		return nil
	}

	cacheResults := make(map[string]error)

	for key, value := range user.Keys {
		userKey := resourceKeyToCacheKey(cacheKeyPrefixUser, key, value)
		err := h.usersCache.Set(ctx, userKey, user, h.cacheTTL)
		cacheResults[userKey] = err
	}

	return cacheResults
}

// Conversion functions for API data to rulesengine types (used only for initial loading from Schematic API)

func convertToRulesEngineCompany(data *schematicgo.CompanyDetailResponseData) *rulesengine.Company {
	company := &rulesengine.Company{
		ID:                data.ID,
		AccountID:         "", // Not available from API response - would need to be set from context
		EnvironmentID:     data.EnvironmentID,
		Keys:              make(map[string]string),
		Traits:            make([]*rulesengine.Trait, 0),
		BillingProductIDs: make([]string, 0),
		PlanIDs:           make([]string, 0),
		CreditBalances:    make(map[string]float64),
		Metrics:           make([]*rulesengine.CompanyMetric, 0),
		Rules:             make([]*rulesengine.Rule, 0),
	}

	if data.Keys != nil {
		for _, keyData := range data.Keys {
			if keyData != nil {
				company.Keys[keyData.Key] = keyData.Value
			}
		}
	}

	company.Traits = convertCompanyTraits(data)

	if data.Plan != nil && data.Plan.ID != "" {
		company.BasePlanID = &data.Plan.ID
	}

	if data.BillingSubscriptions != nil {
		for _, subscription := range data.BillingSubscriptions {
			if subscription != nil && subscription.Products != nil {
				for _, product := range subscription.Products {
					if product != nil && product.ID != "" {
						company.BillingProductIDs = append(company.BillingProductIDs, product.ID)
					}
				}
			}
		}
	}

	if data.BillingSubscription != nil && data.BillingSubscription.Products != nil {
		for _, product := range data.BillingSubscription.Products {
			if product != nil && product.ID != "" {
				found := false
				for _, existingID := range company.BillingProductIDs {
					if existingID == product.ID {
						found = true
						break
					}
				}
				if !found {
					company.BillingProductIDs = append(company.BillingProductIDs, product.ID)
				}
			}
		}
	}

	if data.Plans != nil {
		for _, plan := range data.Plans {
			if plan != nil && plan.ID != "" {
				company.PlanIDs = append(company.PlanIDs, plan.ID)
			}
		}
	}

	if data.BillingSubscription != nil {
		company.Subscription = convertBillingSubscriptionToRulesEngine(data.BillingSubscription)
	}

	if data.BillingCreditBalances != nil {
		company.CreditBalances = data.BillingCreditBalances
	}

	company.Metrics = convertMetricsToRulesEngine(data.Metrics)

	company.Rules = convertRulesToRulesEngine(data.Rules)

	return company
}

func convertBillingSubscriptionToRulesEngine(billingSubscription *schematicgo.BillingSubscriptionView) *rulesengine.Subscription {
	if billingSubscription == nil {
		return nil
	}

	subscription := &rulesengine.Subscription{
		ID: billingSubscription.ID,
	}

	if billingSubscription.PeriodStart != 0 {
		subscription.PeriodStart = time.Unix(int64(billingSubscription.PeriodStart), 0)
	}
	if billingSubscription.PeriodEnd != 0 {
		subscription.PeriodEnd = time.Unix(int64(billingSubscription.PeriodEnd), 0)
	}

	return subscription
}

func convertToRulesEngineUser(data *schematicgo.UserDetailResponseData) *rulesengine.User {
	user := &rulesengine.User{
		ID:            data.ID,
		AccountID:     "", // Will need to be set from context or configuration
		EnvironmentID: data.EnvironmentID,
		Keys:          make(map[string]string),
		Traits:        make([]*rulesengine.Trait, 0),
		Rules:         make([]*rulesengine.Rule, 0),
	}

	if data.Keys != nil {
		for _, keyData := range data.Keys {
			if keyData != nil {
				user.Keys[keyData.Key] = keyData.Value
			}
		}
	}

	user.Traits = convertUserTraits(data)

	return user
}

func convertMetricsToRulesEngine(apiMetrics []*schematicgo.CompanyEventPeriodMetricsResponseData) []*rulesengine.CompanyMetric {
	if apiMetrics == nil {
		return make([]*rulesengine.CompanyMetric, 0)
	}

	metrics := make([]*rulesengine.CompanyMetric, 0, len(apiMetrics))
	for _, metric := range apiMetrics {
		if metric != nil {
			companyMetric := &rulesengine.CompanyMetric{
				AccountID:     metric.AccountID,
				EnvironmentID: metric.EnvironmentID,
				CompanyID:     metric.CompanyID,
				EventSubtype:  metric.EventSubtype,
				Value:         int64(metric.Value),
				CreatedAt:     metric.CreatedAt,
				ValidUntil:    metric.ValidUntil,
			}

			switch metric.Period {
			case "current_day":
				companyMetric.Period = rulesengine.MetricPeriodCurrentDay
			case "current_month":
				companyMetric.Period = rulesengine.MetricPeriodCurrentMonth
			case "all_time":
				companyMetric.Period = rulesengine.MetricPeriodAllTime
			default:
				companyMetric.Period = rulesengine.MetricPeriodAllTime
			}

			switch metric.MonthReset {
			case "first":
				companyMetric.MonthReset = rulesengine.MetricPeriodMonthResetFirst
			case "billing_cycle":
				companyMetric.MonthReset = rulesengine.MetricPeriodMonthResetBilling
			default:
				companyMetric.MonthReset = rulesengine.MetricPeriodMonthResetFirst
			}

			metrics = append(metrics, companyMetric)
		}
	}

	return metrics
}

func convertRulesToRulesEngine(apiRules []*schematicgo.Rule) []*rulesengine.Rule {
	if apiRules == nil {
		return make([]*rulesengine.Rule, 0)
	}

	rules := make([]*rulesengine.Rule, 0)
	for _, rule := range apiRules {
		if rule != nil {
			rulesEngineRule := &rulesengine.Rule{
				ID:            rule.ID,
				EnvironmentID: rule.EnvironmentID,
				Name:          rule.Name,
				Priority:      int64(rule.Priority),
				Value:         rule.Value,
				FlagID:        rule.FlagID,
			}

			switch rule.RuleType {
			case "company_override":
				rulesEngineRule.RuleType = rulesengine.RuleTypeCompanyOverride
			case "company_override_usage_exceeded":
				rulesEngineRule.RuleType = rulesengine.RuleTypeCompanyOverrideUsageExceeded
			default:
				continue
			}

			rulesEngineRule.Conditions = convertToRulesEngineConditions(rule.Conditions)
			rulesEngineRule.ConditionGroups = convertToRulesEngineConditionGroups(rule.ConditionGroups)

			rules = append(rules, rulesEngineRule)
		}
	}

	return rules
}

func convertToRulesEngineConditions(apiConditions []*schematicgo.Condition) []*rulesengine.Condition {
	if apiConditions == nil {
		return make([]*rulesengine.Condition, 0)
	}

	conditions := make([]*rulesengine.Condition, 0, len(apiConditions))
	for _, apiCondition := range apiConditions {
		if apiCondition != nil {
			condition := &rulesengine.Condition{
				ID:            apiCondition.ID,
				AccountID:     apiCondition.AccountID,
				EnvironmentID: apiCondition.EnvironmentID,
				EventSubtype:  apiCondition.EventSubtype,
				ResourceIDs:   apiCondition.ResourceIDs,
				TraitValue:    apiCondition.TraitValue,
			}

			// Convert condition type string to enum
			if apiCondition.ConditionType != "" {
				condition.ConditionType = convertToRulesEngineConditionType(apiCondition.ConditionType)
			}

			// Convert metric value from *int to *int64
			if apiCondition.MetricValue != nil {
				metricValue := int64(*apiCondition.MetricValue)
				condition.MetricValue = &metricValue
			}

			// Convert operator string to enum
			if apiCondition.Operator != "" {
				condition.Operator = convertToRulesEngineOperator(apiCondition.Operator)
			}

			// Convert metric period
			if apiCondition.MetricPeriod != nil {
				condition.MetricPeriod = convertToRulesEngineMetricPeriod(*apiCondition.MetricPeriod)
			}

			// Convert metric period month reset
			if apiCondition.MetricPeriodMonthReset != nil {
				condition.MetricPeriodMonthReset = convertToRulesEngineMetricPeriodMonthReset(*apiCondition.MetricPeriodMonthReset)
			}

			// Convert consumption rate
			if apiCondition.ConsumptionRate != nil {
				condition.ConsumptionRate = apiCondition.ConsumptionRate
			}

			// Convert credit ID
			if apiCondition.CreditID != nil {
				condition.CreditID = apiCondition.CreditID
			}

			// Convert trait definition
			if apiCondition.TraitDefinition != nil {
				condition.TraitDefinition = convertToRulesEngineTraitDefinition(apiCondition.TraitDefinition)
			}

			// Convert comparison trait definition
			if apiCondition.ComparisonTraitDefinition != nil {
				condition.ComparisonTraitDefinition = convertToRulesEngineTraitDefinition(apiCondition.ComparisonTraitDefinition)
			}

			conditions = append(conditions, condition)
		}
	}

	return conditions
}

func convertToRulesEngineConditionGroups(apiConditionGroups []*schematicgo.ConditionGroup) []*rulesengine.ConditionGroup {
	if apiConditionGroups == nil {
		return make([]*rulesengine.ConditionGroup, 0)
	}

	conditionGroups := make([]*rulesengine.ConditionGroup, 0, len(apiConditionGroups))
	for _, apiGroup := range apiConditionGroups {
		if apiGroup != nil {
			conditionGroup := &rulesengine.ConditionGroup{}
			conditionGroup.Conditions = convertToRulesEngineConditions(apiGroup.Conditions)
			conditionGroups = append(conditionGroups, conditionGroup)
		}
	}

	return conditionGroups
}

func convertToRulesEngineOperator(operator string) typeconvert.ComparableOperator {
	switch operator {
	case "eq", "equals":
		return typeconvert.ComparableOperatorEquals
	case "ne", "not_equals":
		return typeconvert.ComparableOperatorNotEquals
	case "gt", "greater_than":
		return typeconvert.ComparableOperatorGt
	case "lt", "less_than":
		return typeconvert.ComparableOperatorLt
	case "gte", "greater_than_or_equal":
		return typeconvert.ComparableOperatorGte
	case "lte", "less_than_or_equal":
		return typeconvert.ComparableOperatorLte
	case "is_empty":
		return typeconvert.ComparableOperatorIsEmpty
	case "not_empty":
		return typeconvert.ComparableOperatorNotEmpty
	default:
		return typeconvert.ComparableOperatorEquals
	}
}

func convertToRulesEngineTraits(entityTraits []*schematicgo.EntityTraitDetailResponseData, entityType rulesengine.EntityType) []*rulesengine.Trait {
	if entityTraits == nil {
		return make([]*rulesengine.Trait, 0)
	}

	traits := make([]*rulesengine.Trait, 0, len(entityTraits))
	for _, entityTrait := range entityTraits {
		if entityTrait != nil && entityTrait.Definition != nil {
			rulesEngineTrait := &rulesengine.Trait{
				Value: entityTrait.Value,
				TraitDefinition: &rulesengine.TraitDefinition{
					ID:             entityTrait.Definition.ID,
					EntityType:     entityType,
					ComparableType: convertTraitTypeToComparableType(entityTrait.Definition.TraitType),
				},
			}
			traits = append(traits, rulesEngineTrait)
		}
	}

	return traits
}

func convertTraitTypeToComparableType(traitType string) typeconvert.ComparableType {
	switch traitType {
	case "boolean", "bool":
		return typeconvert.ComparableTypeBool
	case "number", "int", "integer":
		return typeconvert.ComparableTypeInt
	case "string":
		return typeconvert.ComparableTypeString
	case "date":
		return typeconvert.ComparableTypeDate
	default:
		return typeconvert.ComparableTypeString
	}
}

func convertCompanyTraits(data *schematicgo.CompanyDetailResponseData) []*rulesengine.Trait {
	if data == nil {
		return make([]*rulesengine.Trait, 0)
	}
	return convertToRulesEngineTraits(data.EntityTraits, rulesengine.EntityTypeCompany)
}

func convertUserTraits(data *schematicgo.UserDetailResponseData) []*rulesengine.Trait {
	if data == nil {
		return make([]*rulesengine.Trait, 0)
	}
	return convertToRulesEngineTraits(data.EntityTraits, rulesengine.EntityTypeUser)
}

func convertToRulesEngineConditionType(conditionType string) rulesengine.ConditionType {
	switch conditionType {
	case "trait":
		return rulesengine.ConditionTypeTrait
	case "company":
		return rulesengine.ConditionTypeCompany
	case "user":
		return rulesengine.ConditionTypeUser
	case "plan":
		return rulesengine.ConditionTypePlan
	case "base_plan":
		return rulesengine.ConditionTypeBasePlan
	case "metric":
		return rulesengine.ConditionTypeMetric
	case "billing_product":
		return rulesengine.ConditionTypeBillingProduct
	case "crm_product":
		return rulesengine.ConditionTypeCrmProduct
	case "credit":
		return rulesengine.ConditionTypeCredit
	}

	return ""
}

func convertToRulesEngineMetricPeriod(metricPeriod string) *rulesengine.MetricPeriod {
	var period rulesengine.MetricPeriod
	switch metricPeriod {
	case "all_time":
		period = rulesengine.MetricPeriodAllTime
	case "current_month":
		period = rulesengine.MetricPeriodCurrentMonth
	case "current_day":
		period = rulesengine.MetricPeriodCurrentDay
	case "current_week":
		period = rulesengine.MetricPeriodCurrentWeek
	default:
		period = rulesengine.MetricPeriodAllTime
	}
	return &period
}

func convertToRulesEngineMetricPeriodMonthReset(monthReset string) *rulesengine.MetricPeriodMonthReset {
	var reset rulesengine.MetricPeriodMonthReset
	switch monthReset {
	case "first_of_month":
		reset = rulesengine.MetricPeriodMonthResetFirst
	case "billing_cycle":
		reset = rulesengine.MetricPeriodMonthResetBilling
	default:
		reset = rulesengine.MetricPeriodMonthResetFirst
	}
	return &reset
}

func convertToRulesEngineTraitDefinition(apiTraitDef *schematicgo.TraitDefinition) *rulesengine.TraitDefinition {
	if apiTraitDef == nil {
		return nil
	}

	return &rulesengine.TraitDefinition{
		ID:             apiTraitDef.ID,
		ComparableType: convertTraitTypeToComparableType(apiTraitDef.ComparableType),
	}
}
