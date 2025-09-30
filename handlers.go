package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/schematichq/rulesengine"
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

// ConnectionReadyHandler implements the ConnectionReadyHandler interface
type ConnectionReadyHandler struct {
	schematicClient *client.Client
	wsClient        *schematicdatastreamws.Client
	companiesCache  CacheProvider[*rulesengine.Company]
	usersCache      CacheProvider[*rulesengine.User]
	flagsCache      CacheProvider[*rulesengine.Flag]
	logger          *SchematicLogger
	cacheTTL        time.Duration
}

// NewConnectionReadyHandler creates a new connection ready handler
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

// OnConnectionReady implements the ConnectionReadyHandler interface
func (h *ConnectionReadyHandler) OnConnectionReady(ctx context.Context) error {
	// 1. Load companies from Schematic API
	if err := h.loadAndCacheCompanies(ctx); err != nil {
		return err
	}

	// 2. Load users from Schematic API
	if err := h.loadAndCacheUsers(ctx); err != nil {
		return err
	}

	// 3. Subscribe to company and user updates via websocket
	if err := h.subscribeToUpdates(ctx); err != nil {
		return err
	}

	// 4. Request flags data from datastream
	if err := h.requestFlagsData(ctx); err != nil {
		return err
	}

	h.logger.Info(ctx, "Connection ready setup completed")
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

// Helper methods for ConnectionReadyHandler to cache entities

// Helper method to cache company for all keys (matching schematic-go pattern)
func (h *ConnectionReadyHandler) cacheCompanyForKeys(ctx context.Context, company *rulesengine.Company) map[string]error {
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
func (h *ConnectionReadyHandler) cacheUserForKeys(ctx context.Context, user *rulesengine.User) map[string]error {
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
	}

	// Convert keys - EntityKeyDetailResponseData has Key and Value fields
	if data.Keys != nil {
		for _, keyData := range data.Keys {
			if keyData != nil {
				company.Keys[keyData.Key] = keyData.Value
			}
		}
	}

	// Convert traits - Traits is a map[string]interface{}
	if data.Traits != nil {
		for traitName, traitValue := range data.Traits {
			rulesEngineTrait := &rulesengine.Trait{
				Value: fmt.Sprintf("%v", traitValue), // Convert interface{} to string
				TraitDefinition: &rulesengine.TraitDefinition{
					ID:         traitName, // Use trait name as ID for now
					EntityType: rulesengine.EntityTypeCompany,
				},
			}
			company.Traits = append(company.Traits, rulesEngineTrait)
		}
	}

	// Set BasePlanID from the primary plan (ID is a string, not a pointer)
	if data.Plan != nil && data.Plan.ID != "" {
		company.BasePlanID = &data.Plan.ID
	}

	// Extract BillingProductIDs from billing subscriptions
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

	// Also check the primary billing subscription
	if data.BillingSubscription != nil && data.BillingSubscription.Products != nil {
		for _, product := range data.BillingSubscription.Products {
			if product != nil && product.ID != "" {
				// Avoid duplicates by checking if it's already in the slice
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

	// Extract PlanIDs from the plans array (ID is a string, not a pointer)
	if data.Plans != nil {
		for _, plan := range data.Plans {
			if plan != nil && plan.ID != "" {
				company.PlanIDs = append(company.PlanIDs, plan.ID)
			}
		}
	}

	// Convert primary billing subscription to rulesengine.Subscription
	if data.BillingSubscription != nil {
		company.Subscription = convertBillingSubscriptionToRulesEngine(data.BillingSubscription)
	}

	// Set credit balances - BillingCreditBalances is already a map[string]float64
	if data.BillingCreditBalances != nil {
		company.CreditBalances = data.BillingCreditBalances
	}

	return company
}

// Helper function to convert billing subscription to rulesengine.Subscription
func convertBillingSubscriptionToRulesEngine(billingSubscription *schematicgo.BillingSubscriptionView) *rulesengine.Subscription {
	if billingSubscription == nil {
		return nil
	}

	subscription := &rulesengine.Subscription{
		ID: billingSubscription.ID,
	}

	// Set period start and end (these are int timestamps, convert to time.Time)
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
	}

	// Convert keys - EntityKeyDetailResponseData has Key and Value fields
	if data.Keys != nil {
		for _, keyData := range data.Keys {
			if keyData != nil {
				user.Keys[keyData.Key] = keyData.Value
			}
		}
	}

	// Convert traits - Traits is a map[string]interface{}
	if data.Traits != nil {
		for traitName, traitValue := range data.Traits {
			rulesEngineTrait := &rulesengine.Trait{
				Value: fmt.Sprintf("%v", traitValue), // Convert interface{} to string
				TraitDefinition: &rulesengine.TraitDefinition{
					ID:         traitName, // Use trait name as ID for now
					EntityType: rulesengine.EntityTypeUser,
				},
			}
			user.Traits = append(user.Traits, rulesEngineTrait)
		}
	}

	return user
}
