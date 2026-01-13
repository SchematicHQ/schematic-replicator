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

// CircuitBreakerState represents the state of a circuit breaker
type CircuitBreakerState int

const (
	CircuitClosed CircuitBreakerState = iota
	CircuitOpen
	CircuitHalfOpen
)

// CircuitBreaker provides fail-fast behavior for external dependencies
type CircuitBreaker struct {
	state        CircuitBreakerState
	failureCount int
	successCount int
	threshold    int
	timeout      time.Duration
	mu           sync.RWMutex
	lastFailure  time.Time
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:     CircuitClosed,
		threshold: threshold,
		timeout:   timeout,
	}
}

// CanExecute checks if operations can be executed
func (cb *CircuitBreaker) CanExecute() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if time.Since(cb.lastFailure) > cb.timeout {
			cb.state = CircuitHalfOpen
			cb.successCount = 0
			return true
		}
		return false
	case CircuitHalfOpen:
		return true
	}
	return false
}

// RecordSuccess records a successful operation
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failureCount = 0

	if cb.state == CircuitHalfOpen {
		cb.successCount++
		if cb.successCount >= 3 { // Require 3 successes to close
			cb.state = CircuitClosed
		}
	}
}

// RecordFailure records a failed operation
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failureCount++
	cb.lastFailure = time.Now()

	if cb.failureCount >= cb.threshold {
		cb.state = CircuitOpen
	}
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// MessageJob represents a message with metadata for async processing
type MessageJob struct {
	Message   *schematicdatastreamws.DataStreamResp
	Timestamp time.Time
	Retries   int
}

// AsyncReplicatorMessageHandler handles messages asynchronously with worker pools
type AsyncReplicatorMessageHandler struct {
	// Message channels for each entity type
	companyMsgChan chan *MessageJob
	userMsgChan    chan *MessageJob
	flagsMsgChan   chan *MessageJob

	// Worker pool controls
	numWorkers int
	workerWg   sync.WaitGroup
	shutdown   chan struct{}
	shutdownMu sync.RWMutex

	// Circuit breakers
	redisCircuitBreaker *CircuitBreaker

	// Original fields
	companiesCache CacheProvider[*rulesengine.Company]
	usersCache     CacheProvider[*rulesengine.User]
	flagsCache     CacheProvider[*rulesengine.Flag]
	companyMu      sync.RWMutex
	userMu         sync.RWMutex
	flagsMu        sync.RWMutex
	logger         *SchematicLogger
	cacheTTL       time.Duration

	// Configuration
	batchSize    int
	batchTimeout time.Duration

	// Metrics
	processedMessages int64
	droppedMessages   int64
	metricsMu         sync.RWMutex
}

// AsyncConfig holds configuration for the async handler
type AsyncConfig struct {
	NumWorkers              int
	CompanyChannelSize      int
	UserChannelSize         int
	FlagsChannelSize        int
	BatchSize               int
	BatchTimeout            time.Duration
	CircuitBreakerThreshold int
	CircuitBreakerTimeout   time.Duration
}

// DefaultAsyncConfig returns sensible defaults for async processing
// Optimized for low latency and variable customer infrastructure
func DefaultAsyncConfig() AsyncConfig {
	return AsyncConfig{
		NumWorkers:              0,                     // 0 = auto-detect CPU cores (overridden by NUM_WORKERS env var)
		CompanyChannelSize:      200,                   // Smaller buffers for lower latency
		UserChannelSize:         200,                   // Smaller buffers for lower latency
		FlagsChannelSize:        50,                    // Proportionally smaller
		BatchSize:               5,                     // Smaller batches for lower latency
		BatchTimeout:            10 * time.Millisecond, // More responsive for latency priority
		CircuitBreakerThreshold: 3,                     // Fail faster in customer environments
		CircuitBreakerTimeout:   15 * time.Second,      // Recover faster
	}
}

// NewAsyncReplicatorMessageHandler creates a new async message handler
func NewAsyncReplicatorMessageHandler(
	companiesCache CacheProvider[*rulesengine.Company],
	usersCache CacheProvider[*rulesengine.User],
	flagsCache CacheProvider[*rulesengine.Flag],
	logger *SchematicLogger,
	cacheTTL time.Duration,
	config AsyncConfig,
) *AsyncReplicatorMessageHandler {

	h := &AsyncReplicatorMessageHandler{
		// Buffered channels to prevent blocking
		companyMsgChan: make(chan *MessageJob, config.CompanyChannelSize),
		userMsgChan:    make(chan *MessageJob, config.UserChannelSize),
		flagsMsgChan:   make(chan *MessageJob, config.FlagsChannelSize),

		numWorkers:     config.NumWorkers,
		shutdown:       make(chan struct{}),
		companiesCache: companiesCache,
		usersCache:     usersCache,
		flagsCache:     flagsCache,
		logger:         logger,
		cacheTTL:       cacheTTL,

		batchSize:    config.BatchSize,
		batchTimeout: config.BatchTimeout,

		redisCircuitBreaker: NewCircuitBreaker(config.CircuitBreakerThreshold, config.CircuitBreakerTimeout),
	}

	// Start worker pools for each entity type
	h.startWorkerPools()

	return h
}

// startWorkerPools initializes worker pools for async processing
func (h *AsyncReplicatorMessageHandler) startWorkerPools() {
	ctx := context.Background()

	// Company workers
	for i := 0; i < h.numWorkers; i++ {
		h.workerWg.Add(1)
		go h.companyWorker(ctx, i)
	}

	// User workers
	for i := 0; i < h.numWorkers; i++ {
		h.workerWg.Add(1)
		go h.userWorker(ctx, i)
	}

	// Flags workers (fewer needed, less frequent updates)
	flagsWorkers := h.numWorkers / 2
	if flagsWorkers < 1 {
		flagsWorkers = 1
	}
	for i := 0; i < flagsWorkers; i++ {
		h.workerWg.Add(1)
		go h.flagsWorker(ctx, i)
	}

	h.logger.Info(context.Background(), fmt.Sprintf("Started worker pools: %d company workers, %d user workers, %d flags workers",
		h.numWorkers, h.numWorkers, flagsWorkers))
}

// HandleMessage dispatches messages to appropriate worker pools (non-blocking)
func (h *AsyncReplicatorMessageHandler) HandleMessage(ctx context.Context, message *schematicdatastreamws.DataStreamResp) error {
	h.shutdownMu.RLock()
	defer h.shutdownMu.RUnlock()

	select {
	case <-h.shutdown:
		return fmt.Errorf("handler is shutting down")
	default:
	}

	job := &MessageJob{
		Message:   message,
		Timestamp: time.Now(),
		Retries:   0,
	}

	// Route to appropriate worker pool based on entity type
	switch message.EntityType {
	case string(schematicdatastreamws.EntityTypeCompany):
		return h.queueCompanyMessage(ctx, job)
	case string(schematicdatastreamws.EntityTypeUser):
		return h.queueUserMessage(ctx, job)
	case string(schematicdatastreamws.EntityTypeFlags), string(schematicdatastreamws.EntityTypeFlag):
		return h.queueFlagsMessage(ctx, job)
	case string(schematicdatastreamws.EntityTypeCompanies):
		// Bulk company subscription confirmation
		h.logger.Info(ctx, "Received companies subscription confirmation")
		return nil
	case string(schematicdatastreamws.EntityTypeUsers):
		// Bulk user subscription confirmation
		h.logger.Info(ctx, "Received users subscription confirmation")
		return nil
	default:
		h.logger.Warn(ctx, fmt.Sprintf("Unknown entity type: %s", message.EntityType))
		return nil
	}
}

// queueCompanyMessage queues a company message with backpressure handling
func (h *AsyncReplicatorMessageHandler) queueCompanyMessage(ctx context.Context, job *MessageJob) error {
	select {
	case h.companyMsgChan <- job:
		h.incrementProcessedMessages()
		return nil
	default:
		// Channel full - implement backpressure strategy
		h.logger.Warn(ctx, "Company message channel full, dropping oldest message")
		select {
		case <-h.companyMsgChan: // Remove oldest
			h.incrementDroppedMessages()
		default:
		}
		select {
		case h.companyMsgChan <- job: // Try again
			h.incrementProcessedMessages()
			return nil
		default:
			h.incrementDroppedMessages()
			return fmt.Errorf("failed to queue company message after backpressure")
		}
	}
}

// queueUserMessage queues a user message with backpressure handling
func (h *AsyncReplicatorMessageHandler) queueUserMessage(ctx context.Context, job *MessageJob) error {
	select {
	case h.userMsgChan <- job:
		h.incrementProcessedMessages()
		return nil
	default:
		h.logger.Warn(ctx, "User message channel full, dropping oldest message")
		select {
		case <-h.userMsgChan:
			h.incrementDroppedMessages()
		default:
		}
		select {
		case h.userMsgChan <- job:
			h.incrementProcessedMessages()
			return nil
		default:
			h.incrementDroppedMessages()
			return fmt.Errorf("failed to queue user message after backpressure")
		}
	}
}

// queueFlagsMessage queues a flags message with backpressure handling
func (h *AsyncReplicatorMessageHandler) queueFlagsMessage(ctx context.Context, job *MessageJob) error {
	select {
	case h.flagsMsgChan <- job:
		h.incrementProcessedMessages()
		return nil
	default:
		h.logger.Warn(ctx, "Flags message channel full, dropping oldest message")
		select {
		case <-h.flagsMsgChan:
			h.incrementDroppedMessages()
		default:
		}
		select {
		case h.flagsMsgChan <- job:
			h.incrementProcessedMessages()
			return nil
		default:
			h.incrementDroppedMessages()
			return fmt.Errorf("failed to queue flags message after backpressure")
		}
	}
}

// Shutdown gracefully shuts down the async handler
func (h *AsyncReplicatorMessageHandler) Shutdown(ctx context.Context) error {
	h.shutdownMu.Lock()
	defer h.shutdownMu.Unlock()

	h.logger.Info(ctx, "Shutting down async message handler")

	// Signal shutdown to all workers
	close(h.shutdown)

	// Close message channels
	close(h.companyMsgChan)
	close(h.userMsgChan)
	close(h.flagsMsgChan)

	// Wait for all workers to finish with timeout
	done := make(chan struct{})
	go func() {
		h.workerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		h.logger.Info(ctx, "All workers shut down successfully")
		return nil
	case <-ctx.Done():
		h.logger.Warn(ctx, "Shutdown timeout reached, some workers may still be running")
		return ctx.Err()
	}
}

// GetMetrics returns current processing metrics
func (h *AsyncReplicatorMessageHandler) GetMetrics() (processed, dropped int64) {
	h.metricsMu.RLock()
	defer h.metricsMu.RUnlock()
	return h.processedMessages, h.droppedMessages
}

// incrementProcessedMessages atomically increments the processed counter
func (h *AsyncReplicatorMessageHandler) incrementProcessedMessages() {
	h.metricsMu.Lock()
	defer h.metricsMu.Unlock()
	h.processedMessages++
}

// incrementDroppedMessages atomically increments the dropped counter
func (h *AsyncReplicatorMessageHandler) incrementDroppedMessages() {
	h.metricsMu.Lock()
	defer h.metricsMu.Unlock()
	h.droppedMessages++
}

// companyWorker processes company messages in batches
func (h *AsyncReplicatorMessageHandler) companyWorker(ctx context.Context, workerID int) {
	defer h.workerWg.Done()

	batchBuffer := make([]*MessageJob, 0, h.batchSize)
	batchTimer := time.NewTimer(h.batchTimeout)
	defer batchTimer.Stop()

	h.logger.Info(ctx, fmt.Sprintf("Company worker %d started", workerID))

	for {
		select {
		case <-h.shutdown:
			// Process remaining messages in buffer before shutdown
			if len(batchBuffer) > 0 {
				h.processBatchedCompanyMessages(ctx, batchBuffer)
			}
			h.logger.Info(ctx, fmt.Sprintf("Company worker %d shutting down", workerID))
			return

		case job, ok := <-h.companyMsgChan:
			if !ok {
				// Channel closed, process remaining buffer and exit
				if len(batchBuffer) > 0 {
					h.processBatchedCompanyMessages(ctx, batchBuffer)
				}
				h.logger.Info(ctx, fmt.Sprintf("Company worker %d channel closed", workerID))
				return
			}

			batchBuffer = append(batchBuffer, job)

			// Process batch when full
			if len(batchBuffer) >= h.batchSize {
				h.processBatchedCompanyMessages(ctx, batchBuffer)
				batchBuffer = batchBuffer[:0]
				batchTimer.Reset(h.batchTimeout)
			}

		case <-batchTimer.C:
			// Process batch on timeout
			if len(batchBuffer) > 0 {
				h.processBatchedCompanyMessages(ctx, batchBuffer)
				batchBuffer = batchBuffer[:0]
			}
			batchTimer.Reset(h.batchTimeout)
		}
	}
}

// userWorker processes user messages in batches
func (h *AsyncReplicatorMessageHandler) userWorker(ctx context.Context, workerID int) {
	defer h.workerWg.Done()

	batchBuffer := make([]*MessageJob, 0, h.batchSize)
	batchTimer := time.NewTimer(h.batchTimeout)
	defer batchTimer.Stop()

	h.logger.Info(ctx, fmt.Sprintf("User worker %d started", workerID))

	for {
		select {
		case <-h.shutdown:
			if len(batchBuffer) > 0 {
				h.processBatchedUserMessages(ctx, batchBuffer)
			}
			h.logger.Info(ctx, fmt.Sprintf("User worker %d shutting down", workerID))
			return

		case job, ok := <-h.userMsgChan:
			if !ok {
				if len(batchBuffer) > 0 {
					h.processBatchedUserMessages(ctx, batchBuffer)
				}
				h.logger.Info(ctx, fmt.Sprintf("User worker %d channel closed", workerID))
				return
			}

			batchBuffer = append(batchBuffer, job)

			if len(batchBuffer) >= h.batchSize {
				h.processBatchedUserMessages(ctx, batchBuffer)
				batchBuffer = batchBuffer[:0]
				batchTimer.Reset(h.batchTimeout)
			}

		case <-batchTimer.C:
			if len(batchBuffer) > 0 {
				h.processBatchedUserMessages(ctx, batchBuffer)
				batchBuffer = batchBuffer[:0]
			}
			batchTimer.Reset(h.batchTimeout)
		}
	}
}

// flagsWorker processes flags messages in batches
func (h *AsyncReplicatorMessageHandler) flagsWorker(ctx context.Context, workerID int) {
	defer h.workerWg.Done()

	batchBuffer := make([]*MessageJob, 0, h.batchSize)
	batchTimer := time.NewTimer(h.batchTimeout)
	defer batchTimer.Stop()

	h.logger.Info(ctx, fmt.Sprintf("Flags worker %d started", workerID))

	for {
		select {
		case <-h.shutdown:
			if len(batchBuffer) > 0 {
				h.processBatchedFlagsMessages(ctx, batchBuffer)
			}
			h.logger.Info(ctx, fmt.Sprintf("Flags worker %d shutting down", workerID))
			return

		case job, ok := <-h.flagsMsgChan:
			if !ok {
				if len(batchBuffer) > 0 {
					h.processBatchedFlagsMessages(ctx, batchBuffer)
				}
				h.logger.Info(ctx, fmt.Sprintf("Flags worker %d channel closed", workerID))
				return
			}

			batchBuffer = append(batchBuffer, job)

			if len(batchBuffer) >= h.batchSize {
				h.processBatchedFlagsMessages(ctx, batchBuffer)
				batchBuffer = batchBuffer[:0]
				batchTimer.Reset(h.batchTimeout)
			}

		case <-batchTimer.C:
			if len(batchBuffer) > 0 {
				h.processBatchedFlagsMessages(ctx, batchBuffer)
				batchBuffer = batchBuffer[:0]
			}
			batchTimer.Reset(h.batchTimeout)
		}
	}
}

// processBatchedCompanyMessages processes a batch of company messages with circuit breaker
func (h *AsyncReplicatorMessageHandler) processBatchedCompanyMessages(ctx context.Context, jobs []*MessageJob) {
	if len(jobs) == 0 {
		return
	}

	// Check circuit breaker before proceeding
	if !h.redisCircuitBreaker.CanExecute() {
		h.logger.Warn(ctx, fmt.Sprintf("Circuit breaker open, dropping %d company messages", len(jobs)))
		return
	}

	// Group operations by type for better batching
	var creates []*rulesengine.Company
	var deletes []*rulesengine.Company

	for _, job := range jobs {
		company, err := h.parseCompanyMessage(job.Message)
		if err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to parse company message: %v", err))
			continue
		}

		if company == nil {
			continue
		}

		switch job.Message.MessageType {
		case schematicdatastreamws.MessageTypeFull, schematicdatastreamws.MessageTypePartial:
			creates = append(creates, company)
		case schematicdatastreamws.MessageTypeDelete:
			deletes = append(deletes, company)
		}
	}

	// Batch create operations
	if len(creates) > 0 {
		if err := h.batchCacheCompanies(ctx, creates); err != nil {
			h.redisCircuitBreaker.RecordFailure()
			h.logger.Error(ctx, fmt.Sprintf("Batch company cache failed: %v", err))
		} else {
			h.redisCircuitBreaker.RecordSuccess()
			h.logger.Debug(ctx, fmt.Sprintf("Successfully cached %d companies", len(creates)))
		}
	}

	// Batch delete operations
	if len(deletes) > 0 {
		if err := h.batchDeleteCompanies(ctx, deletes); err != nil {
			h.redisCircuitBreaker.RecordFailure()
			h.logger.Error(ctx, fmt.Sprintf("Batch company delete failed: %v", err))
		} else {
			h.redisCircuitBreaker.RecordSuccess()
			h.logger.Debug(ctx, fmt.Sprintf("Successfully deleted %d companies", len(deletes)))
		}
	}
}

// processBatchedUserMessages processes a batch of user messages with circuit breaker
func (h *AsyncReplicatorMessageHandler) processBatchedUserMessages(ctx context.Context, jobs []*MessageJob) {
	if len(jobs) == 0 {
		return
	}

	if !h.redisCircuitBreaker.CanExecute() {
		h.logger.Warn(ctx, fmt.Sprintf("Circuit breaker open, dropping %d user messages", len(jobs)))
		return
	}

	var creates []*rulesengine.User
	var deletes []*rulesengine.User

	for _, job := range jobs {
		user, err := h.parseUserMessage(job.Message)
		if err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to parse user message: %v", err))
			continue
		}

		if user == nil {
			continue
		}

		switch job.Message.MessageType {
		case schematicdatastreamws.MessageTypeFull, schematicdatastreamws.MessageTypePartial:
			creates = append(creates, user)
		case schematicdatastreamws.MessageTypeDelete:
			deletes = append(deletes, user)
		}
	}

	if len(creates) > 0 {
		if err := h.batchCacheUsers(ctx, creates); err != nil {
			h.redisCircuitBreaker.RecordFailure()
			h.logger.Error(ctx, fmt.Sprintf("Batch user cache failed: %v", err))
		} else {
			h.redisCircuitBreaker.RecordSuccess()
			h.logger.Debug(ctx, fmt.Sprintf("Successfully cached %d users", len(creates)))
		}
	}

	if len(deletes) > 0 {
		if err := h.batchDeleteUsers(ctx, deletes); err != nil {
			h.redisCircuitBreaker.RecordFailure()
			h.logger.Error(ctx, fmt.Sprintf("Batch user delete failed: %v", err))
		} else {
			h.redisCircuitBreaker.RecordSuccess()
			h.logger.Debug(ctx, fmt.Sprintf("Successfully deleted %d users", len(deletes)))
		}
	}
}

// processBatchedFlagsMessages processes a batch of flags messages with circuit breaker
func (h *AsyncReplicatorMessageHandler) processBatchedFlagsMessages(ctx context.Context, jobs []*MessageJob) {
	if len(jobs) == 0 {
		return
	}

	if !h.redisCircuitBreaker.CanExecute() {
		h.logger.Warn(ctx, fmt.Sprintf("Circuit breaker open, dropping %d flags messages", len(jobs)))
		return
	}

	// Group jobs by entity type for different processing
	var bulkFlagsJobs []*MessageJob  // EntityTypeFlags
	var singleFlagJobs []*MessageJob // EntityTypeFlag

	for _, job := range jobs {
		switch job.Message.EntityType {
		case string(schematicdatastreamws.EntityTypeFlags):
			bulkFlagsJobs = append(bulkFlagsJobs, job)
		case string(schematicdatastreamws.EntityTypeFlag):
			singleFlagJobs = append(singleFlagJobs, job)
		}
	}

	// Process bulk flags messages (with missing flag deletion)
	for _, job := range bulkFlagsJobs {
		if err := h.processBulkFlagsMessage(ctx, job.Message); err != nil {
			h.redisCircuitBreaker.RecordFailure()
			h.logger.Error(ctx, fmt.Sprintf("Failed to process bulk flags message: %v", err))
		} else {
			h.redisCircuitBreaker.RecordSuccess()
		}
	}

	// Process single flag messages (without missing flag deletion)
	for _, job := range singleFlagJobs {
		if err := h.processSingleFlagMessage(ctx, job.Message); err != nil {
			h.redisCircuitBreaker.RecordFailure()
			h.logger.Error(ctx, fmt.Sprintf("Failed to process single flag message: %v", err))
		} else {
			h.redisCircuitBreaker.RecordSuccess()
		}
	}
}

// parseCompanyMessage parses a company message from DataStreamResp
func (h *AsyncReplicatorMessageHandler) parseCompanyMessage(message *schematicdatastreamws.DataStreamResp) (*rulesengine.Company, error) {
	var company *rulesengine.Company
	if err := json.Unmarshal(message.Data, &company); err != nil {
		return nil, fmt.Errorf("failed to unmarshal company data: %w", err)
	}
	return company, nil
}

// parseUserMessage parses a user message from DataStreamResp
func (h *AsyncReplicatorMessageHandler) parseUserMessage(message *schematicdatastreamws.DataStreamResp) (*rulesengine.User, error) {
	var user *rulesengine.User
	if err := json.Unmarshal(message.Data, &user); err != nil {
		return nil, fmt.Errorf("failed to unmarshal user data: %w", err)
	}
	return user, nil
}

// batchCacheCompanies caches multiple companies using batch operations when possible
func (h *AsyncReplicatorMessageHandler) batchCacheCompanies(ctx context.Context, companies []*rulesengine.Company) error {
	if len(companies) == 0 {
		return nil
	}

	// Build batch of all cache keys and values
	batchItems := make(map[string]*rulesengine.Company)

	for _, company := range companies {
		if company == nil || len(company.Keys) == 0 {
			continue
		}

		// Create cache entry for each key-value pair
		for key, value := range company.Keys {
			cacheKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
			batchItems[cacheKey] = company
		}
	}

	if len(batchItems) == 0 {
		return nil
	}

	// Use Redis pipeline for batch operations if supported
	if batchCache, ok := h.companiesCache.(BatchCacheProvider[*rulesengine.Company]); ok {
		return batchCache.BatchSet(ctx, batchItems, h.cacheTTL)
	}

	// Fallback to individual operations if batch not supported
	h.companyMu.Lock()
	defer h.companyMu.Unlock()

	for cacheKey, company := range batchItems {
		if err := h.companiesCache.Set(ctx, cacheKey, company, h.cacheTTL); err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to cache company key %s: %v", cacheKey, err))
		}
	}

	return nil
}

// batchCacheUsers caches multiple users using batch operations when possible
func (h *AsyncReplicatorMessageHandler) batchCacheUsers(ctx context.Context, users []*rulesengine.User) error {
	if len(users) == 0 {
		return nil
	}

	batchItems := make(map[string]*rulesengine.User)

	for _, user := range users {
		if user == nil || len(user.Keys) == 0 {
			continue
		}

		for key, value := range user.Keys {
			cacheKey := resourceKeyToCacheKey(cacheKeyPrefixUser, key, value)
			batchItems[cacheKey] = user
		}
	}

	if len(batchItems) == 0 {
		return nil
	}

	if batchCache, ok := h.usersCache.(BatchCacheProvider[*rulesengine.User]); ok {
		return batchCache.BatchSet(ctx, batchItems, h.cacheTTL)
	}

	h.userMu.Lock()
	defer h.userMu.Unlock()

	for cacheKey, user := range batchItems {
		if err := h.usersCache.Set(ctx, cacheKey, user, h.cacheTTL); err != nil {
			h.logger.Error(ctx, fmt.Sprintf("Failed to cache user key %s: %v", cacheKey, err))
		}
	}

	return nil
}

// batchDeleteCompanies deletes multiple companies using batch operations when possible
func (h *AsyncReplicatorMessageHandler) batchDeleteCompanies(ctx context.Context, companies []*rulesengine.Company) error {
	if len(companies) == 0 {
		return nil
	}

	// Use a map to deduplicate keys
	keysMap := make(map[string]bool)

	for _, company := range companies {
		if company == nil || len(company.Keys) == 0 {
			continue
		}

		for key, value := range company.Keys {
			cacheKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
			keysMap[cacheKey] = true
		}
	}

	if len(keysMap) == 0 {
		return nil
	}

	// Convert map keys to slice
	keysToDelete := make([]string, 0, len(keysMap))
	for key := range keysMap {
		keysToDelete = append(keysToDelete, key)
	}

	if batchCache, ok := h.companiesCache.(BatchCacheProvider[*rulesengine.Company]); ok {
		return batchCache.BatchDelete(ctx, keysToDelete)
	}

	h.companyMu.Lock()
	defer h.companyMu.Unlock()

	for _, cacheKey := range keysToDelete {
		if err := h.companiesCache.Delete(ctx, cacheKey); err != nil {
			h.logger.Warn(ctx, fmt.Sprintf("Failed to delete company key %s: %v", cacheKey, err))
		}
	}

	return nil
}

// batchDeleteUsers deletes multiple users using batch operations when possible
func (h *AsyncReplicatorMessageHandler) batchDeleteUsers(ctx context.Context, users []*rulesengine.User) error {
	if len(users) == 0 {
		return nil
	}

	// Use a map to deduplicate keys
	keysMap := make(map[string]bool)

	for _, user := range users {
		if user == nil || len(user.Keys) == 0 {
			continue
		}

		for key, value := range user.Keys {
			cacheKey := resourceKeyToCacheKey(cacheKeyPrefixUser, key, value)
			keysMap[cacheKey] = true
		}
	}

	if len(keysMap) == 0 {
		return nil
	}

	// Convert map keys to slice
	keysToDelete := make([]string, 0, len(keysMap))
	for key := range keysMap {
		keysToDelete = append(keysToDelete, key)
	}

	if batchCache, ok := h.usersCache.(BatchCacheProvider[*rulesengine.User]); ok {
		return batchCache.BatchDelete(ctx, keysToDelete)
	}

	h.userMu.Lock()
	defer h.userMu.Unlock()

	for _, cacheKey := range keysToDelete {
		if err := h.usersCache.Delete(ctx, cacheKey); err != nil {
			h.logger.Warn(ctx, fmt.Sprintf("Failed to delete user key %s: %v", cacheKey, err))
		}
	}

	return nil
}

// processBulkFlagsMessage processes bulk flags messages (EntityTypeFlags) with missing flag deletion
func (h *AsyncReplicatorMessageHandler) processBulkFlagsMessage(ctx context.Context, message *schematicdatastreamws.DataStreamResp) error {
	switch message.MessageType {
	case schematicdatastreamws.MessageTypeFull:
		// Handle flags data (array of flags)
		var flags []*rulesengine.Flag
		if err := json.Unmarshal(message.Data, &flags); err != nil {
			return fmt.Errorf("failed to unmarshal flags data: %w", err)
		}

		h.flagsMu.Lock()
		defer h.flagsMu.Unlock()

		var cacheKeys []string
		for _, flag := range flags {
			if flag != nil && flag.Key != "" {
				cacheKey := flagCacheKey(flag.Key)
				if err := h.flagsCache.Set(ctx, cacheKey, flag, h.cacheTTL); err != nil {
					h.logger.Error(ctx, fmt.Sprintf("Failed to cache flag %s: %v", flag.Key, err))
				} else {
					h.logger.Debug(ctx, fmt.Sprintf("Cached flag: %s", flag.Key))
					cacheKeys = append(cacheKeys, cacheKey)
				}
			}
		}

		// Delete missing flags for bulk updates (matching schematic-go behavior)
		if len(cacheKeys) > 0 {
			if err := h.flagsCache.DeleteMissing(ctx, cacheKeys); err != nil {
				h.logger.Error(ctx, fmt.Sprintf("Failed to delete missing flags: %v", err))
			}
		}

		h.logger.Info(ctx, fmt.Sprintf("Successfully cached %d flags", len(flags)))

	default:
		h.logger.Debug(ctx, fmt.Sprintf("Unhandled bulk flags message type: %s", message.MessageType))
	}

	return nil
}

// processSingleFlagMessage processes single flag messages (EntityTypeFlag) without missing flag deletion
func (h *AsyncReplicatorMessageHandler) processSingleFlagMessage(ctx context.Context, message *schematicdatastreamws.DataStreamResp) error {
	switch message.MessageType {
	case schematicdatastreamws.MessageTypeFull, schematicdatastreamws.MessageTypePartial:
		// Handle single flag data
		var flag *rulesengine.Flag
		if err := json.Unmarshal(message.Data, &flag); err != nil {
			return fmt.Errorf("failed to unmarshal single flag data: %w", err)
		}

		h.flagsMu.Lock()
		defer h.flagsMu.Unlock()

		if flag != nil && flag.Key != "" {
			cacheKey := flagCacheKey(flag.Key)
			if err := h.flagsCache.Set(ctx, cacheKey, flag, h.cacheTTL); err != nil {
				h.logger.Error(ctx, fmt.Sprintf("Failed to cache flag %s: %v", flag.Key, err))
			} else {
				h.logger.Debug(ctx, fmt.Sprintf("Cached single flag: %s", flag.Key))
			}
		}

	case schematicdatastreamws.MessageTypeDelete:
		// Handle single flag deletion
		var deleteData struct {
			Key string `json:"key,omitempty"`
			ID  string `json:"id,omitempty"`
		}
		if err := json.Unmarshal(message.Data, &deleteData); err != nil {
			return fmt.Errorf("failed to unmarshal single flag delete data: %w", err)
		}

		flagKey := deleteData.Key
		if flagKey == "" {
			flagKey = deleteData.ID
		}

		if flagKey != "" {
			h.flagsMu.Lock()
			defer h.flagsMu.Unlock()

			cacheKey := flagCacheKey(flagKey)
			if err := h.flagsCache.Delete(ctx, cacheKey); err != nil {
				h.logger.Error(ctx, fmt.Sprintf("Failed to delete single flag from cache: %v", err))
			} else {
				h.logger.Debug(ctx, fmt.Sprintf("Deleted single flag from cache: %s", flagKey))
			}
		}

	default:
		h.logger.Debug(ctx, fmt.Sprintf("Unhandled single flag message type: %s", message.MessageType))
	}

	return nil
}

// AsyncInitialLoader provides asynchronous loading of companies and users
// This allows the connection to be established quickly while data loads in the background
type AsyncInitialLoader struct {
	schematicClient *client.Client
	companiesCache  CacheProvider[*rulesengine.Company]
	usersCache      CacheProvider[*rulesengine.User]
	logger          *SchematicLogger
	cacheTTL        time.Duration
	config          AsyncLoaderConfig

	// Circuit breaker for API calls
	apiCircuitBreaker *CircuitBreaker

	// Rate limiter for API calls
	rateLimiter *RateLimiter

	// Loading state tracking
	loadingMu          sync.RWMutex
	companiesLoaded    bool
	usersLoaded        bool
	companiesLoadError error
	usersLoadError     error

	// Completion channels
	companiesLoadChan chan struct{}
	usersLoadChan     chan struct{}

	// Metrics
	companiesLoadTime time.Duration
	usersLoadTime     time.Duration
	totalCompanies    int
	totalUsers        int

	// Concurrency tracking
	concurrencyLimit chan struct{} // Semaphore for concurrent requests
}

// RateLimiter provides rate limiting for API calls
type RateLimiter struct {
	ticker   *time.Ticker
	tokens   chan struct{}
	shutdown chan struct{}
	mu       sync.Mutex
}

// NewRateLimiter creates a new rate limiter with the specified requests per second
func NewRateLimiter(rps int) *RateLimiter {
	if rps <= 0 {
		rps = 10 // Default to 10 RPS
	}

	rl := &RateLimiter{
		ticker:   time.NewTicker(time.Second / time.Duration(rps)),
		tokens:   make(chan struct{}, rps),
		shutdown: make(chan struct{}),
	}

	// Fill initial tokens
	for i := 0; i < rps; i++ {
		select {
		case rl.tokens <- struct{}{}:
		default:
			// Token bucket full, stop filling
			return rl
		}
	}

	// Start token replenishment
	go rl.run()

	return rl
}

// Wait blocks until a token is available
func (rl *RateLimiter) Wait(ctx context.Context) error {
	select {
	case <-rl.tokens:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-rl.shutdown:
		return fmt.Errorf("rate limiter shutdown")
	}
}

// Close shuts down the rate limiter
func (rl *RateLimiter) Close() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	select {
	case <-rl.shutdown:
		return // already closed
	default:
		close(rl.shutdown)
		rl.ticker.Stop()
	}
}

// run replenishes tokens at the configured rate
func (rl *RateLimiter) run() {
	for {
		select {
		case <-rl.ticker.C:
			select {
			case rl.tokens <- struct{}{}:
			default:
				// Token bucket full, skip
			}
		case <-rl.shutdown:
			return
		}
	}
}

// AsyncLoaderConfig holds configuration for the async loader
type AsyncLoaderConfig struct {
	CircuitBreakerThreshold int
	CircuitBreakerTimeout   time.Duration
	PageSize                int
	// Concurrency settings
	MaxConcurrentRequests int // Maximum concurrent API requests (default: 5)
	RateLimitRPS          int // Rate limit in requests per second (default: 10)
}

// DefaultAsyncLoaderConfig returns sensible defaults for async loading
func DefaultAsyncLoaderConfig() AsyncLoaderConfig {
	return AsyncLoaderConfig{
		CircuitBreakerThreshold: 5,                // Allow more failures for initial loading
		CircuitBreakerTimeout:   30 * time.Second, // Longer timeout for recovery
		PageSize:                100,              // Standard page size
		MaxConcurrentRequests:   5,                // Conservative concurrency for rate limiting
		RateLimitRPS:            10,               // Conservative rate limit
	}
}

// NewAsyncInitialLoader creates a new async initial loader
func NewAsyncInitialLoader(
	schematicClient *client.Client,
	companiesCache CacheProvider[*rulesengine.Company],
	usersCache CacheProvider[*rulesengine.User],
	logger *SchematicLogger,
	cacheTTL time.Duration,
	config AsyncLoaderConfig,
) *AsyncInitialLoader {
	return &AsyncInitialLoader{
		schematicClient:   schematicClient,
		companiesCache:    companiesCache,
		usersCache:        usersCache,
		logger:            logger,
		cacheTTL:          cacheTTL,
		config:            config,
		apiCircuitBreaker: NewCircuitBreaker(config.CircuitBreakerThreshold, config.CircuitBreakerTimeout),
		rateLimiter:       NewRateLimiter(config.RateLimitRPS),
		companiesLoadChan: make(chan struct{}),
		usersLoadChan:     make(chan struct{}),
		concurrencyLimit:  make(chan struct{}, config.MaxConcurrentRequests),
	}
}

// StartAsyncLoading begins loading companies and users asynchronously
// Returns immediately, allowing the connection to be established without waiting
func (al *AsyncInitialLoader) StartAsyncLoading(ctx context.Context) {
	al.logger.Info(ctx, "Starting asynchronous initial data loading")

	// Start companies loading in background
	go func() {
		defer close(al.companiesLoadChan)

		startTime := time.Now()
		err := al.loadCompaniesAsync(ctx)
		loadTime := time.Since(startTime)

		al.loadingMu.Lock()
		al.companiesLoaded = true
		al.companiesLoadError = err
		al.companiesLoadTime = loadTime
		al.loadingMu.Unlock()

		if err != nil {
			al.logger.Error(ctx, fmt.Sprintf("Async companies loading failed after %v: %v", loadTime, err))
		} else {
			al.logger.Info(ctx, fmt.Sprintf("Async companies loading completed successfully in %v (%d companies)", loadTime, al.totalCompanies))
		}
	}()

	// Start users loading in background
	go func() {
		defer close(al.usersLoadChan)

		startTime := time.Now()
		err := al.loadUsersAsync(ctx)
		loadTime := time.Since(startTime)

		al.loadingMu.Lock()
		al.usersLoaded = true
		al.usersLoadError = err
		al.usersLoadTime = loadTime
		al.loadingMu.Unlock()

		if err != nil {
			al.logger.Error(ctx, fmt.Sprintf("Async users loading failed after %v: %v", loadTime, err))
		} else {
			al.logger.Info(ctx, fmt.Sprintf("Async users loading completed successfully in %v (%d users)", loadTime, al.totalUsers))
		}
	}()

	al.logger.Info(ctx, "Async initial data loading started in background")
}

// WaitForCompletion waits for both companies and users to finish loading
func (al *AsyncInitialLoader) WaitForCompletion(ctx context.Context) error {
	al.logger.Info(ctx, "Waiting for async initial data loading to complete")

	// Wait for both to complete or context timeout
	select {
	case <-al.companiesLoadChan:
		// Companies done, wait for users
		select {
		case <-al.usersLoadChan:
			// Both done
		case <-ctx.Done():
			return fmt.Errorf("context timeout waiting for users loading: %w", ctx.Err())
		}
	case <-al.usersLoadChan:
		// Users done, wait for companies
		select {
		case <-al.companiesLoadChan:
			// Both done
		case <-ctx.Done():
			return fmt.Errorf("context timeout waiting for companies loading: %w", ctx.Err())
		}
	case <-ctx.Done():
		return fmt.Errorf("context timeout waiting for initial loading: %w", ctx.Err())
	}

	// Clean up resources
	if al.rateLimiter != nil {
		al.rateLimiter.Close()
	}

	// Check for errors
	al.loadingMu.RLock()
	defer al.loadingMu.RUnlock()

	if al.companiesLoadError != nil && al.usersLoadError != nil {
		return fmt.Errorf("both companies and users loading failed: companies=%v, users=%v",
			al.companiesLoadError, al.usersLoadError)
	} else if al.companiesLoadError != nil {
		return fmt.Errorf("companies loading failed: %w", al.companiesLoadError)
	} else if al.usersLoadError != nil {
		return fmt.Errorf("users loading failed: %w", al.usersLoadError)
	}

	al.logger.Info(ctx, fmt.Sprintf("Async initial data loading completed successfully: %d companies in %v, %d users in %v",
		al.totalCompanies, al.companiesLoadTime, al.totalUsers, al.usersLoadTime))

	return nil
}

// GetLoadingStatus returns the current loading status
func (al *AsyncInitialLoader) GetLoadingStatus() (companiesLoaded, usersLoaded bool, companiesErr, usersErr error) {
	al.loadingMu.RLock()
	defer al.loadingMu.RUnlock()

	return al.companiesLoaded, al.usersLoaded, al.companiesLoadError, al.usersLoadError
}

// GetLoadingMetrics returns loading performance metrics
func (al *AsyncInitialLoader) GetLoadingMetrics() (companiesTime, usersTime time.Duration, totalCompanies, totalUsers int) {
	al.loadingMu.RLock()
	defer al.loadingMu.RUnlock()

	return al.companiesLoadTime, al.usersLoadTime, al.totalCompanies, al.totalUsers
}

// loadCompaniesAsync loads all companies with concurrent requests and rate limiting
func (al *AsyncInitialLoader) loadCompaniesAsync(ctx context.Context) error {
	al.logger.Info(ctx, "Starting async companies loading")

	if !al.apiCircuitBreaker.CanExecute() {
		err := fmt.Errorf("companies loading blocked by circuit breaker")
		al.apiCircuitBreaker.RecordFailure()
		return err
	}

	// Always use concurrent loading in async mode
	return al.loadCompaniesConcurrent(ctx)
}

// loadCompaniesConcurrent loads companies using concurrent API requests
func (al *AsyncInitialLoader) loadCompaniesConcurrent(ctx context.Context) error {
	// First, get the total count to plan pagination
	countResp, err := al.schematicClient.Companies.CountCompanies(ctx, &schematicgo.CountCompaniesRequest{})
	if err != nil {
		al.logger.Error(ctx, fmt.Sprintf("Failed to get companies count: %v", err))
		al.apiCircuitBreaker.RecordFailure()
		return err
	}

	totalCount := 0
	if countResp.Data != nil && countResp.Data.Count != nil {
		totalCount = int(*countResp.Data.Count)
	}
	al.logger.Info(ctx, fmt.Sprintf("Planning to load %d companies concurrently", totalCount))

	if totalCount == 0 {
		al.logger.Info(ctx, "No companies to load")
		return nil
	}

	pageSize := al.config.PageSize
	numPages := (totalCount + pageSize - 1) / pageSize

	// Create channels for coordinating concurrent requests
	type pageResult struct {
		offset int
		data   []*schematicgo.CompanyDetailResponseData
		err    error
	}

	results := make(chan pageResult, numPages)

	// Start concurrent page fetchers
	var wg sync.WaitGroup
	for i := 0; i < numPages; i++ {
		wg.Add(1)
		go func(pageIndex int) {
			defer wg.Done()

			offset := pageIndex * pageSize

			// Rate limiting and concurrency control
			select {
			case al.concurrencyLimit <- struct{}{}:
				defer func() { <-al.concurrencyLimit }()
			case <-ctx.Done():
				results <- pageResult{offset: offset, err: ctx.Err()}
				return
			}

			if err := al.rateLimiter.Wait(ctx); err != nil {
				results <- pageResult{offset: offset, err: err}
				return
			}

			// Circuit breaker check
			if !al.apiCircuitBreaker.CanExecute() {
				results <- pageResult{offset: offset, err: fmt.Errorf("circuit breaker open")}
				return
			}

			// Fetch the page
			companiesResp, err := al.schematicClient.Companies.ListCompanies(ctx, &schematicgo.ListCompaniesRequest{
				Limit:  &pageSize,
				Offset: &offset,
			})

			if err != nil {
				al.apiCircuitBreaker.RecordFailure()
				results <- pageResult{offset: offset, err: err}
				return
			}

			al.apiCircuitBreaker.RecordSuccess()
			results <- pageResult{offset: offset, data: companiesResp.Data}
		}(i)
	}

	// Wait for all requests to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Process results and cache companies
	var allCacheKeys []string
	processedCount := 0

	for result := range results {
		if result.err != nil {
			al.logger.Error(ctx, fmt.Sprintf("Failed to fetch companies page at offset %d: %v", result.offset, result.err))
			continue
		}

		for _, companyData := range result.data {
			company := convertToRulesEngineCompany(companyData)

			cacheResults := al.cacheCompanyForKeys(ctx, company)
			for cacheKey, cacheErr := range cacheResults {
				if cacheErr != nil {
					al.logger.Error(ctx, fmt.Sprintf("Cache error for company %s key '%s': %v", company.ID, cacheKey, cacheErr))
				} else {
					allCacheKeys = append(allCacheKeys, cacheKey)
				}
			}
		}

		processedCount += len(result.data)
		al.logger.Debug(ctx, fmt.Sprintf("Processed companies page at offset %d (%d companies)", result.offset, len(result.data)))
	}

	// Evict missing keys
	if len(allCacheKeys) > 0 {
		if err := al.companiesCache.DeleteMissing(ctx, allCacheKeys); err != nil {
			al.logger.Error(ctx, fmt.Sprintf("Failed to evict missing company cache keys: %v", err))
		}
	}

	al.totalCompanies = processedCount
	al.logger.Info(ctx, fmt.Sprintf("Successfully cached %d companies via concurrent async loading", processedCount))
	return nil
}

// loadUsersAsync loads all users with concurrent requests and rate limiting
func (al *AsyncInitialLoader) loadUsersAsync(ctx context.Context) error {
	al.logger.Info(ctx, "Starting async users loading")

	if !al.apiCircuitBreaker.CanExecute() {
		err := fmt.Errorf("users loading blocked by circuit breaker")
		al.apiCircuitBreaker.RecordFailure()
		return err
	}

	// Always use concurrent loading in async mode
	return al.loadUsersConcurrent(ctx)
}

// loadUsersConcurrent loads users using concurrent API requests
func (al *AsyncInitialLoader) loadUsersConcurrent(ctx context.Context) error {
	// First, get the total count to plan pagination
	countResp, err := al.schematicClient.Companies.CountUsers(ctx, &schematicgo.CountUsersRequest{})
	if err != nil {
		al.logger.Error(ctx, fmt.Sprintf("Failed to get users count: %v", err))
		al.apiCircuitBreaker.RecordFailure()
		return err
	}

	totalCount := 0
	if countResp.Data != nil && countResp.Data.Count != nil {
		totalCount = int(*countResp.Data.Count)
	}
	al.logger.Info(ctx, fmt.Sprintf("Planning to load %d users concurrently", totalCount))

	if totalCount == 0 {
		al.logger.Info(ctx, "No users to load")
		return nil
	}

	pageSize := al.config.PageSize
	numPages := (totalCount + pageSize - 1) / pageSize

	// Create channels for coordinating concurrent requests
	type pageResult struct {
		offset int
		data   []*schematicgo.UserDetailResponseData
		err    error
	}

	results := make(chan pageResult, numPages)

	// Start concurrent page fetchers
	var wg sync.WaitGroup
	for i := 0; i < numPages; i++ {
		wg.Add(1)
		go func(pageIndex int) {
			defer wg.Done()

			offset := pageIndex * pageSize

			// Rate limiting and concurrency control
			select {
			case al.concurrencyLimit <- struct{}{}:
				defer func() { <-al.concurrencyLimit }()
			case <-ctx.Done():
				results <- pageResult{offset: offset, err: ctx.Err()}
				return
			}

			if err := al.rateLimiter.Wait(ctx); err != nil {
				results <- pageResult{offset: offset, err: err}
				return
			}

			// Circuit breaker check
			if !al.apiCircuitBreaker.CanExecute() {
				results <- pageResult{offset: offset, err: fmt.Errorf("circuit breaker open")}
				return
			}

			// Fetch the page
			usersResp, err := al.schematicClient.Companies.ListUsers(ctx, &schematicgo.ListUsersRequest{
				Limit:  &pageSize,
				Offset: &offset,
			})

			if err != nil {
				al.apiCircuitBreaker.RecordFailure()
				results <- pageResult{offset: offset, err: err}
				return
			}

			al.apiCircuitBreaker.RecordSuccess()
			results <- pageResult{offset: offset, data: usersResp.Data}
		}(i)
	}

	// Wait for all requests to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Process results and cache users
	var allCacheKeys []string
	processedCount := 0

	for result := range results {
		if result.err != nil {
			al.logger.Error(ctx, fmt.Sprintf("Failed to fetch users page at offset %d: %v", result.offset, result.err))
			continue
		}

		for _, userData := range result.data {
			user := convertToRulesEngineUser(userData)

			cacheResults := al.cacheUserForKeys(ctx, user)
			for cacheKey, cacheErr := range cacheResults {
				if cacheErr != nil {
					al.logger.Error(ctx, fmt.Sprintf("Cache error for user %s key '%s': %v", user.ID, cacheKey, cacheErr))
				} else {
					allCacheKeys = append(allCacheKeys, cacheKey)
				}
			}
		}

		processedCount += len(result.data)
		al.logger.Debug(ctx, fmt.Sprintf("Processed users page at offset %d (%d users)", result.offset, len(result.data)))
	}

	// Evict missing keys
	if len(allCacheKeys) > 0 {
		if err := al.usersCache.DeleteMissing(ctx, allCacheKeys); err != nil {
			al.logger.Error(ctx, fmt.Sprintf("Failed to evict missing user cache keys: %v", err))
		}
	}

	al.totalUsers = processedCount
	al.logger.Info(ctx, fmt.Sprintf("Successfully cached %d users via concurrent async loading", processedCount))
	return nil
}

// cacheCompanyForKeys caches a company for all its key combinations (matching existing implementation)
func (al *AsyncInitialLoader) cacheCompanyForKeys(ctx context.Context, company *rulesengine.Company) map[string]error {
	if company == nil || len(company.Keys) == 0 {
		return nil
	}

	cacheResults := make(map[string]error)

	for key, value := range company.Keys {
		companyKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
		err := al.companiesCache.Set(ctx, companyKey, company, al.cacheTTL)
		cacheResults[companyKey] = err
	}

	return cacheResults
}

// cacheUserForKeys caches a user for all its key combinations (matching existing implementation)
func (al *AsyncInitialLoader) cacheUserForKeys(ctx context.Context, user *rulesengine.User) map[string]error {
	if user == nil || len(user.Keys) == 0 {
		return nil
	}

	cacheResults := make(map[string]error)

	for key, value := range user.Keys {
		userKey := resourceKeyToCacheKey(cacheKeyPrefixUser, key, value)
		err := al.usersCache.Set(ctx, userKey, user, al.cacheTTL)
		cacheResults[userKey] = err
	}

	return cacheResults
}

// AsyncConnectionReadyHandler implements the ConnectionReadyHandler interface for asynchronous loading
type AsyncConnectionReadyHandler struct {
	schematicClient *client.Client
	wsClient        *schematicdatastreamws.Client
	companiesCache  CacheProvider[*rulesengine.Company]
	usersCache      CacheProvider[*rulesengine.User]
	flagsCache      CacheProvider[*rulesengine.Flag]
	logger          *SchematicLogger
	cacheTTL        time.Duration
	asyncLoader     *AsyncInitialLoader
}

// NewAsyncConnectionReadyHandler creates a new async connection ready handler
func NewAsyncConnectionReadyHandler(
	schematicClient *client.Client,
	wsClient *schematicdatastreamws.Client,
	companiesCache CacheProvider[*rulesengine.Company],
	usersCache CacheProvider[*rulesengine.User],
	flagsCache CacheProvider[*rulesengine.Flag],
	logger *SchematicLogger,
	cacheTTL time.Duration,
	asyncLoaderConfig AsyncLoaderConfig,
) *AsyncConnectionReadyHandler {
	asyncLoader := NewAsyncInitialLoader(
		schematicClient,
		companiesCache,
		usersCache,
		logger,
		cacheTTL,
		asyncLoaderConfig,
	)

	return &AsyncConnectionReadyHandler{
		schematicClient: schematicClient,
		wsClient:        wsClient,
		companiesCache:  companiesCache,
		usersCache:      usersCache,
		flagsCache:      flagsCache,
		logger:          logger,
		cacheTTL:        cacheTTL,
		asyncLoader:     asyncLoader,
	}
}

// OnConnectionReady implements the ConnectionReadyHandler interface for asynchronous loading
func (h *AsyncConnectionReadyHandler) OnConnectionReady(ctx context.Context) error {
	// Async mode: start loading in background, establish connection quickly
	h.logger.Info(ctx, "Starting async initial data loading for fast connection setup")
	h.asyncLoader.StartAsyncLoading(ctx)

	// 1. Subscribe to updates immediately (WebSocket connection is ready)
	if err := h.subscribeToUpdates(ctx); err != nil {
		return fmt.Errorf("failed to subscribe to updates: %w", err)
	}

	// 2. Request flags data from datastream
	if err := h.requestFlagsData(ctx); err != nil {
		return fmt.Errorf("failed to request flags data: %w", err)
	}

	h.logger.Info(ctx, "Connection ready setup completed with async loading (companies/users loading in background)")
	return nil
}

// SetWebSocketClient sets the WebSocket client for AsyncConnectionReadyHandler
func (h *AsyncConnectionReadyHandler) SetWebSocketClient(wsClient *schematicdatastreamws.Client) {
	h.wsClient = wsClient
}

// WaitForAsyncLoading waits for async initial loading to complete
func (h *AsyncConnectionReadyHandler) WaitForAsyncLoading(ctx context.Context) error {
	return h.asyncLoader.WaitForCompletion(ctx)
}

// GetAsyncLoadingStatus returns the status of async loading
func (h *AsyncConnectionReadyHandler) GetAsyncLoadingStatus() (companiesLoaded, usersLoaded bool, companiesErr, usersErr error) {
	return h.asyncLoader.GetLoadingStatus()
}

// GetAsyncLoadingMetrics returns loading metrics
func (h *AsyncConnectionReadyHandler) GetAsyncLoadingMetrics() (companiesTime, usersTime time.Duration, totalCompanies, totalUsers int) {
	return h.asyncLoader.GetLoadingMetrics()
}

// subscribeToUpdates for AsyncConnectionReadyHandler
func (h *AsyncConnectionReadyHandler) subscribeToUpdates(ctx context.Context) error {
	h.logger.Info(ctx, "Subscribing to company and user updates")

	// Subscribe to all company updates using bulk subscription entity type
	companySubscription := &schematicdatastreamws.DataStreamBaseReq{
		Data: schematicdatastreamws.DataStreamReq{
			Action:     schematicdatastreamws.ActionStart,
			EntityType: schematicdatastreamws.EntityTypeCompanies,
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
			EntityType: schematicdatastreamws.EntityTypeUsers,
		},
	}
	if err := h.wsClient.SendMessage(userSubscription); err != nil {
		h.logger.Error(ctx, fmt.Sprintf("Failed to subscribe to user updates: %v", err))
		return err
	}

	h.logger.Info(ctx, "Successfully subscribed to company and user updates")
	return nil
}

// requestFlagsData for AsyncConnectionReadyHandler
func (h *AsyncConnectionReadyHandler) requestFlagsData(ctx context.Context) error {
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
