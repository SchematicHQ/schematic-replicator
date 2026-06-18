package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/schematichq/rulesengine"
	schematicdatastreamws "github.com/schematichq/schematic-datastream-ws"
	schematicgo "github.com/schematichq/schematic-go"
	"github.com/schematichq/schematic-go/client"
	"github.com/schematichq/schematic-go/datastream"
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
	// EntityKey identifies the entity this message mutates. Messages for the
	// same entity are routed to the same worker (see queue*Message) so they are
	// applied in arrival (stream) order.
	EntityKey string
}

// AsyncReplicatorMessageHandler handles messages asynchronously with worker pools
type AsyncReplicatorMessageHandler struct {
	// Message channels for each entity type. Companies and users are sharded by
	// entity key (one channel per worker) so a given entity is always handled by
	// the same worker, preserving per-entity ordering. Flags use a single worker
	// because bulk-flag snapshots affect the whole set and must stay ordered
	// against single-flag updates.
	companyMsgChans []chan *MessageJob
	userMsgChans    []chan *MessageJob
	flagsMsgChan    chan *MessageJob

	// Worker pool controls
	numWorkers int
	workerWg   sync.WaitGroup
	shutdown   chan struct{}
	shutdownMu sync.RWMutex

	// Circuit breakers
	redisCircuitBreaker *CircuitBreaker

	// Original fields
	companiesCache     CacheProvider[*rulesengine.Company]
	companyLookupCache BatchCacheProvider[string]
	usersCache         CacheProvider[*rulesengine.User]
	userLookupCache    BatchCacheProvider[string]
	flagsCache         CacheProvider[*rulesengine.Flag]
	companyMu          sync.RWMutex
	userMu             sync.RWMutex
	flagsMu            sync.RWMutex
	logger             *SchematicLogger
	cacheTTL           time.Duration

	// Configuration
	batchSize    int
	batchTimeout time.Duration

	// Metrics
	processedMessages int64
	appliedMessages   int64
	droppedMessages   int64
	metricsMu         sync.RWMutex

	// Replay support (optional; nil when replay is disabled)
	replayCursor *ReplayCursor
	reloadFunc   func(ctx context.Context)
}

// SetReplayCursor injects the cursor the handler records each message's stream
// ID into, so a reconnect can replay from the last processed message.
func (h *AsyncReplicatorMessageHandler) SetReplayCursor(cursor *ReplayCursor) {
	h.replayCursor = cursor
}

// SetReloadFunc wires the action taken when the server signals that the replay
// window has aged out and a full reload is required.
func (h *AsyncReplicatorMessageHandler) SetReloadFunc(fn func(ctx context.Context)) {
	h.reloadFunc = fn
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
	companyLookupCache BatchCacheProvider[string],
	userLookupCache BatchCacheProvider[string],
	logger *SchematicLogger,
	cacheTTL time.Duration,
	config AsyncConfig,
) *AsyncReplicatorMessageHandler {

	numWorkers := config.NumWorkers
	if numWorkers < 1 {
		numWorkers = 1
	}
	companyMsgChans := make([]chan *MessageJob, numWorkers)
	userMsgChans := make([]chan *MessageJob, numWorkers)
	for i := 0; i < numWorkers; i++ {
		companyMsgChans[i] = make(chan *MessageJob, config.CompanyChannelSize)
		userMsgChans[i] = make(chan *MessageJob, config.UserChannelSize)
	}

	h := &AsyncReplicatorMessageHandler{
		// Per-worker channels for companies/users (sharded by entity key);
		// a single buffered channel for flags.
		companyMsgChans: companyMsgChans,
		userMsgChans:    userMsgChans,
		flagsMsgChan:    make(chan *MessageJob, config.FlagsChannelSize),

		numWorkers:         numWorkers,
		shutdown:           make(chan struct{}),
		companiesCache:     companiesCache,
		companyLookupCache: companyLookupCache,
		usersCache:         usersCache,
		userLookupCache:    userLookupCache,
		flagsCache:         flagsCache,
		logger:             logger,
		cacheTTL:           cacheTTL,

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

	// A single flags worker keeps bulk-flag snapshots ordered against
	// single-flag updates (bulk replaces the whole set).
	h.workerWg.Add(1)
	go h.flagsWorker(ctx, 0)

	h.logger.Info(context.Background(), fmt.Sprintf("Started worker pools: %d company workers, %d user workers, 1 flags worker",
		h.numWorkers, h.numWorkers))
}

// entityKeyForMessage derives the stable per-entity routing/grouping key for a
// message. The server sets entity_id on every company and user message (full,
// partial, and delete alike), so EntityID is the key for the entity types that
// shard. We deliberately do not parse the payload here: this runs on the single
// ingestion goroutine, and the worker parses the payload again, so a fallback
// parse would scan large payloads twice on the hot path. When EntityID is absent
// (flags, snapshots, subscription confirmations) we fall back to the unique
// StreamID — never colliding two distinct entities onto one key — and finally to
// "" (all such messages hash to shard 0; flags don't shard regardless).
func entityKeyForMessage(message *schematicdatastreamws.DataStreamResp) string {
	if message.EntityID != nil && *message.EntityID != "" {
		return *message.EntityID
	}
	if message.StreamID != nil && *message.StreamID != "" {
		return *message.StreamID
	}
	return ""
}

// shardForKey maps an entity key to one of n worker shards. Empty keys map to
// shard 0.
func shardForKey(key string, n int) int {
	if n <= 1 || key == "" {
		return 0
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return int(hasher.Sum32() % uint32(n))
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

	// The server signals that our resume token aged out of its retention window
	// and a complete replay is no longer possible — drop the stale cursor and
	// fall back to a full reload.
	if message.MessageType == schematicdatastreamws.MessageTypeReload {
		h.logger.Warn(ctx, "Server signaled replay window aged out; triggering full reload")
		if h.replayCursor != nil {
			h.replayCursor.Reset(ctx)
		}
		if h.reloadFunc != nil {
			h.reloadFunc(ctx)
		}
		return nil
	}

	// Track the resume token so a reconnect can replay from the last message we
	// have applied. The cursor is committed at apply time (see Complete calls in
	// the batch processors), not here, so it never runs ahead of cache state.
	if h.replayCursor != nil && message.StreamID != nil {
		h.replayCursor.Track(*message.StreamID)
	}

	job := &MessageJob{
		Message:   message,
		Timestamp: time.Now(),
		Retries:   0,
		EntityKey: entityKeyForMessage(message),
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

// queueCompanyMessage queues a company message on its entity's shard with
// backpressure handling.
func (h *AsyncReplicatorMessageHandler) queueCompanyMessage(ctx context.Context, job *MessageJob) error {
	ch := h.companyMsgChans[shardForKey(job.EntityKey, h.numWorkers)]
	select {
	case ch <- job:
		h.incrementProcessedMessages()
		return nil
	default:
		// Channel full - implement backpressure strategy
		h.logger.Warn(ctx, "Company message channel full, dropping oldest message")
		select {
		case <-ch: // Remove oldest
			h.incrementDroppedMessages()
		default:
		}
		select {
		case ch <- job: // Try again
			h.incrementProcessedMessages()
			return nil
		default:
			h.incrementDroppedMessages()
			return fmt.Errorf("failed to queue company message after backpressure")
		}
	}
}

// queueUserMessage queues a user message on its entity's shard with
// backpressure handling.
func (h *AsyncReplicatorMessageHandler) queueUserMessage(ctx context.Context, job *MessageJob) error {
	ch := h.userMsgChans[shardForKey(job.EntityKey, h.numWorkers)]
	select {
	case ch <- job:
		h.incrementProcessedMessages()
		return nil
	default:
		h.logger.Warn(ctx, "User message channel full, dropping oldest message")
		select {
		case <-ch:
			h.incrementDroppedMessages()
		default:
		}
		select {
		case ch <- job:
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
	for _, ch := range h.companyMsgChans {
		close(ch)
	}
	for _, ch := range h.userMsgChans {
		close(ch)
	}
	close(h.flagsMsgChan)

	// Wait for all workers to finish with timeout
	done := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				h.logger.Error(ctx, fmt.Sprintf("Panic in shutdown wait goroutine: %v", r))
			}
		}()
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

// GetMetrics returns current processing metrics: messages queued, messages
// applied to the cache (committed), and messages dropped under backpressure.
func (h *AsyncReplicatorMessageHandler) GetMetrics() (processed, applied, dropped int64) {
	h.metricsMu.RLock()
	defer h.metricsMu.RUnlock()
	return h.processedMessages, h.appliedMessages, h.droppedMessages
}

// incrementAppliedMessages records messages that were applied to the cache.
func (h *AsyncReplicatorMessageHandler) incrementAppliedMessages(n int) {
	if n <= 0 {
		return
	}
	h.metricsMu.Lock()
	defer h.metricsMu.Unlock()
	h.appliedMessages += int64(n)
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

		case job, ok := <-h.companyMsgChans[workerID]:
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

		case job, ok := <-h.userMsgChans[workerID]:
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

// completeReplayJobs advances the replay cursor over a batch that has been
// processed. Called only once a batch reaches the end of processing, so a batch
// dropped early (circuit breaker open) leaves its stream IDs uncommitted and is
// recovered on the next reconnect's replay.
func (h *AsyncReplicatorMessageHandler) completeReplayJobs(jobs []*MessageJob) {
	h.incrementAppliedMessages(len(jobs))
	if h.replayCursor == nil {
		return
	}
	for _, job := range jobs {
		if job.Message != nil && job.Message.StreamID != nil {
			h.replayCursor.Complete(*job.Message.StreamID)
		}
	}
}

// completeCommittableJobs advances the replay cursor only for jobs whose entity
// was successfully applied (or was a genuine no-op). Jobs for an entity that
// failed to apply — a cache write error, or a partial whose base couldn't be
// read — are left uncommitted so the next reconnect's replay re-delivers them.
func (h *AsyncReplicatorMessageHandler) completeCommittableJobs(jobs []*MessageJob, committable map[string]bool) {
	applied := 0
	for _, job := range jobs {
		if !committable[job.EntityKey] {
			continue
		}
		applied++
		if h.replayCursor != nil && job.Message != nil && job.Message.StreamID != nil {
			h.replayCursor.Complete(*job.Message.StreamID)
		}
	}
	h.incrementAppliedMessages(applied)
}

// processBatchedCompanyMessages applies a batch of company messages. Because a
// worker only ever sees one entity's messages (sharded routing), the jobs for a
// given company arrive in stream order; we collapse each company's ops in that
// order to its final state, then issue one cache write or delete per company.
// This preserves per-entity ordering (a full/partial/delete sequence is never
// reordered by type) while still batching the resulting cache operations.
func (h *AsyncReplicatorMessageHandler) processBatchedCompanyMessages(ctx context.Context, jobs []*MessageJob) {
	if len(jobs) == 0 {
		return
	}

	// Check circuit breaker before proceeding
	if !h.redisCircuitBreaker.CanExecute() {
		h.logger.Warn(ctx, fmt.Sprintf("Circuit breaker open, dropping %d company messages", len(jobs)))
		return
	}

	type companyState struct {
		company    *rulesengine.Company // latest known value (for cache or lookup-key cleanup)
		present    bool                 // final op leaves the entity present vs deleted
		touched    bool                 // a valid op was applied
		readFailed bool                 // a partial couldn't read its base due to a cache error (not a genuine miss)
	}
	order := make([]string, 0, len(jobs))
	states := make(map[string]*companyState, len(jobs))
	stateFor := func(key string) *companyState {
		st, ok := states[key]
		if !ok {
			st = &companyState{}
			states[key] = st
			order = append(order, key)
		}
		return st
	}

	for _, job := range jobs {
		st := stateFor(job.EntityKey)
		switch job.Message.MessageType {
		case schematicdatastreamws.MessageTypeFull:
			company, err := h.parseCompanyMessage(job.Message)
			if err != nil || company == nil {
				if err != nil {
					h.logger.Error(ctx, fmt.Sprintf("Failed to parse company message: %v", err))
				}
				continue
			}
			st.company, st.present, st.touched, st.readFailed = company, true, true, false
		case schematicdatastreamws.MessageTypeDelete:
			company, err := h.parseCompanyMessage(job.Message)
			if err != nil || company == nil {
				if err != nil {
					h.logger.Error(ctx, fmt.Sprintf("Failed to parse company message: %v", err))
				}
				continue
			}
			st.company, st.present, st.touched, st.readFailed = company, false, true, false
		case schematicdatastreamws.MessageTypePartial:
			base := st.company
			if !st.present || base == nil {
				existing, existErr := h.companiesCache.Get(ctx, companyIDCacheKey(job.EntityKey))
				switch {
				case existErr == nil && existing != nil:
					base = existing
				case errors.Is(existErr, redis.Nil) || (existErr == nil && existing == nil):
					// Genuine absence: the base full was dropped/aged-out (its own
					// stream slot holds the cursor for replay) or the entity is
					// deleted/never-existed. Either way, skipping is a correct no-op.
					h.logger.Warn(ctx, fmt.Sprintf("No cached resource for partial company '%s', skipping", job.EntityKey))
					continue
				default:
					// The base may be present but unreadable (transient cache error).
					// Do not commit past this partial; hold so replay re-delivers it.
					h.logger.Error(ctx, fmt.Sprintf("Failed to read base for partial company '%s': %v", job.EntityKey, existErr))
					st.readFailed = true
					continue
				}
			}
			merged, mergeErr := datastream.PartialCompany(base, job.Message.Data)
			if mergeErr != nil {
				h.logger.Error(ctx, fmt.Sprintf("Failed to merge partial company '%s': %v", job.EntityKey, mergeErr))
				continue
			}
			st.company, st.present, st.touched, st.readFailed = merged, true, true, false
		}
	}

	var toCache, toDelete []*rulesengine.Company
	var cacheKeys, deleteKeys []string
	for _, key := range order {
		st := states[key]
		if !st.touched || st.company == nil {
			continue
		}
		if st.present {
			toCache = append(toCache, st.company)
			cacheKeys = append(cacheKeys, key)
		} else {
			toDelete = append(toDelete, st.company)
			deleteKeys = append(deleteKeys, key)
		}
	}

	// Track which entities we may advance the replay cursor for. A genuine no-op
	// (skipped partial with no base) is committable; an entity whose write fails
	// or whose base couldn't be read is held so replay re-delivers it.
	committable := make(map[string]bool, len(order))
	for _, key := range order {
		if st := states[key]; !st.touched && !st.readFailed {
			committable[key] = true
		}
	}

	if len(toCache) > 0 {
		if err := h.batchCacheCompanies(ctx, toCache); err != nil {
			h.redisCircuitBreaker.RecordFailure()
			h.logger.Error(ctx, fmt.Sprintf("Batch company cache failed: %v", err))
		} else {
			h.redisCircuitBreaker.RecordSuccess()
			h.logger.Debug(ctx, fmt.Sprintf("Successfully cached %d companies", len(toCache)))
			for _, key := range cacheKeys {
				committable[key] = true
			}
		}
	}

	if len(toDelete) > 0 {
		if err := h.batchDeleteCompanies(ctx, toDelete); err != nil {
			h.redisCircuitBreaker.RecordFailure()
			h.logger.Error(ctx, fmt.Sprintf("Batch company delete failed: %v", err))
		} else {
			h.redisCircuitBreaker.RecordSuccess()
			h.logger.Debug(ctx, fmt.Sprintf("Successfully deleted %d companies", len(toDelete)))
			for _, key := range deleteKeys {
				committable[key] = true
			}
		}
	}

	h.completeCommittableJobs(jobs, committable)
}

// processBatchedUserMessages applies a batch of user messages, collapsing each
// user's ops in arrival (stream) order to a final state. See
// processBatchedCompanyMessages for the rationale.
func (h *AsyncReplicatorMessageHandler) processBatchedUserMessages(ctx context.Context, jobs []*MessageJob) {
	if len(jobs) == 0 {
		return
	}

	if !h.redisCircuitBreaker.CanExecute() {
		h.logger.Warn(ctx, fmt.Sprintf("Circuit breaker open, dropping %d user messages", len(jobs)))
		return
	}

	type userState struct {
		user       *rulesengine.User
		present    bool
		touched    bool
		readFailed bool
	}
	order := make([]string, 0, len(jobs))
	states := make(map[string]*userState, len(jobs))
	stateFor := func(key string) *userState {
		st, ok := states[key]
		if !ok {
			st = &userState{}
			states[key] = st
			order = append(order, key)
		}
		return st
	}

	for _, job := range jobs {
		st := stateFor(job.EntityKey)
		switch job.Message.MessageType {
		case schematicdatastreamws.MessageTypeFull:
			user, err := h.parseUserMessage(job.Message)
			if err != nil || user == nil {
				if err != nil {
					h.logger.Error(ctx, fmt.Sprintf("Failed to parse user message: %v", err))
				}
				continue
			}
			st.user, st.present, st.touched, st.readFailed = user, true, true, false
		case schematicdatastreamws.MessageTypeDelete:
			user, err := h.parseUserMessage(job.Message)
			if err != nil || user == nil {
				if err != nil {
					h.logger.Error(ctx, fmt.Sprintf("Failed to parse user message: %v", err))
				}
				continue
			}
			st.user, st.present, st.touched, st.readFailed = user, false, true, false
		case schematicdatastreamws.MessageTypePartial:
			base := st.user
			if !st.present || base == nil {
				existing, existErr := h.usersCache.Get(ctx, userIDCacheKey(job.EntityKey))
				switch {
				case existErr == nil && existing != nil:
					base = existing
				case errors.Is(existErr, redis.Nil) || (existErr == nil && existing == nil):
					// Genuine absence — skipping is a correct no-op (see the
					// company path for the rationale).
					h.logger.Warn(ctx, fmt.Sprintf("No cached resource for partial user '%s', skipping", job.EntityKey))
					continue
				default:
					// Present but unreadable (transient cache error): hold so
					// replay re-delivers this partial.
					h.logger.Error(ctx, fmt.Sprintf("Failed to read base for partial user '%s': %v", job.EntityKey, existErr))
					st.readFailed = true
					continue
				}
			}
			merged, mergeErr := datastream.PartialUser(base, job.Message.Data)
			if mergeErr != nil {
				h.logger.Error(ctx, fmt.Sprintf("Failed to merge partial user '%s': %v", job.EntityKey, mergeErr))
				continue
			}
			st.user, st.present, st.touched, st.readFailed = merged, true, true, false
		}
	}

	var toCache, toDelete []*rulesengine.User
	var cacheKeys, deleteKeys []string
	for _, key := range order {
		st := states[key]
		if !st.touched || st.user == nil {
			continue
		}
		if st.present {
			toCache = append(toCache, st.user)
			cacheKeys = append(cacheKeys, key)
		} else {
			toDelete = append(toDelete, st.user)
			deleteKeys = append(deleteKeys, key)
		}
	}

	committable := make(map[string]bool, len(order))
	for _, key := range order {
		if st := states[key]; !st.touched && !st.readFailed {
			committable[key] = true
		}
	}

	if len(toCache) > 0 {
		if err := h.batchCacheUsers(ctx, toCache); err != nil {
			h.redisCircuitBreaker.RecordFailure()
			h.logger.Error(ctx, fmt.Sprintf("Batch user cache failed: %v", err))
		} else {
			h.redisCircuitBreaker.RecordSuccess()
			h.logger.Debug(ctx, fmt.Sprintf("Successfully cached %d users", len(toCache)))
			for _, key := range cacheKeys {
				committable[key] = true
			}
		}
	}

	if len(toDelete) > 0 {
		if err := h.batchDeleteUsers(ctx, toDelete); err != nil {
			h.redisCircuitBreaker.RecordFailure()
			h.logger.Error(ctx, fmt.Sprintf("Batch user delete failed: %v", err))
		} else {
			h.redisCircuitBreaker.RecordSuccess()
			h.logger.Debug(ctx, fmt.Sprintf("Successfully deleted %d users", len(toDelete)))
			for _, key := range deleteKeys {
				committable[key] = true
			}
		}
	}

	h.completeCommittableJobs(jobs, committable)
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

	// Apply flag messages in arrival (stream) order: a bulk-flags snapshot
	// replaces the whole set, so it must not be reordered relative to the
	// single-flag updates around it. The single flags worker keeps this order
	// across batches too.
	//
	// Only advance the replay cursor for messages that applied (or were no-ops);
	// a failed apply is held so the next reconnect's replay re-delivers it.
	var doneJobs []*MessageJob
	for _, job := range jobs {
		var err error
		switch job.Message.EntityType {
		case string(schematicdatastreamws.EntityTypeFlags):
			err = h.processBulkFlagsMessage(ctx, job.Message)
		case string(schematicdatastreamws.EntityTypeFlag):
			err = h.processSingleFlagMessage(ctx, job.Message)
		default:
			doneJobs = append(doneJobs, job) // not a flag message we handle → no-op
			continue
		}
		if err != nil {
			h.redisCircuitBreaker.RecordFailure()
			h.logger.Error(ctx, fmt.Sprintf("Failed to process flags message (%s): %v", job.Message.EntityType, err))
		} else {
			h.redisCircuitBreaker.RecordSuccess()
			doneJobs = append(doneJobs, job)
		}
	}

	h.completeReplayJobs(doneJobs)
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

// batchCacheCompanies caches multiple companies using batch operations when possible.
// Writes full company to ID keys and company ID strings to lookup keys.
func (h *AsyncReplicatorMessageHandler) batchCacheCompanies(ctx context.Context, companies []*rulesengine.Company) error {
	if len(companies) == 0 {
		return nil
	}

	// Build batch maps for ID keys (full company) and lookup keys (company ID string)
	idItems := make(map[string]*rulesengine.Company)
	lookupItems := make(map[string]string)

	for _, company := range companies {
		if company == nil || len(company.Keys) == 0 {
			continue
		}

		// ID-based primary key -> full company
		idKey := companyIDCacheKey(company.ID)
		idItems[idKey] = company

		// Versioned lookup keys -> company ID string
		for key, value := range company.Keys {
			lookupKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
			lookupItems[lookupKey] = company.ID
		}
	}

	// Batch write ID keys
	if len(idItems) > 0 {
		if batchCache, ok := h.companiesCache.(BatchCacheProvider[*rulesengine.Company]); ok {
			if err := batchCache.BatchSet(ctx, idItems, h.cacheTTL); err != nil {
				return fmt.Errorf("batch set company ID keys: %w", err)
			}
		} else {
			h.companyMu.Lock()
			for cacheKey, company := range idItems {
				if err := h.companiesCache.Set(ctx, cacheKey, company, h.cacheTTL); err != nil {
					h.logger.Error(ctx, fmt.Sprintf("Failed to cache company ID key %s: %v", cacheKey, err))
				}
			}
			h.companyMu.Unlock()
		}
	}

	// Batch write lookup keys
	if len(lookupItems) > 0 {
		if err := h.companyLookupCache.BatchSet(ctx, lookupItems, h.cacheTTL); err != nil {
			return fmt.Errorf("batch set company lookup keys: %w", err)
		}
	}

	return nil
}

// batchCacheUsers caches multiple users using batch operations when possible.
// Writes full user to ID keys and user ID strings to lookup keys.
func (h *AsyncReplicatorMessageHandler) batchCacheUsers(ctx context.Context, users []*rulesengine.User) error {
	if len(users) == 0 {
		return nil
	}

	// Build batch maps for ID keys (full user) and lookup keys (user ID string)
	idItems := make(map[string]*rulesengine.User)
	lookupItems := make(map[string]string)

	for _, user := range users {
		if user == nil || len(user.Keys) == 0 {
			continue
		}

		// ID-based primary key -> full user
		idKey := userIDCacheKey(user.ID)
		idItems[idKey] = user

		// Versioned lookup keys -> user ID string
		for key, value := range user.Keys {
			lookupKey := resourceKeyToCacheKey(cacheKeyPrefixUser, key, value)
			lookupItems[lookupKey] = user.ID
		}
	}

	// Batch write ID keys
	if len(idItems) > 0 {
		if batchCache, ok := h.usersCache.(BatchCacheProvider[*rulesengine.User]); ok {
			if err := batchCache.BatchSet(ctx, idItems, h.cacheTTL); err != nil {
				return fmt.Errorf("batch set user ID keys: %w", err)
			}
		} else {
			h.userMu.Lock()
			for cacheKey, user := range idItems {
				if err := h.usersCache.Set(ctx, cacheKey, user, h.cacheTTL); err != nil {
					h.logger.Error(ctx, fmt.Sprintf("Failed to cache user ID key %s: %v", cacheKey, err))
				}
			}
			h.userMu.Unlock()
		}
	}

	// Batch write lookup keys
	if len(lookupItems) > 0 {
		if err := h.userLookupCache.BatchSet(ctx, lookupItems, h.cacheTTL); err != nil {
			return fmt.Errorf("batch set user lookup keys: %w", err)
		}
	}

	return nil
}

// batchDeleteCompanies deletes multiple companies using batch operations when possible.
// Deletes both ID keys and lookup keys.
func (h *AsyncReplicatorMessageHandler) batchDeleteCompanies(ctx context.Context, companies []*rulesengine.Company) error {
	if len(companies) == 0 {
		return nil
	}

	// Collect ID keys and lookup keys separately
	idKeysMap := make(map[string]bool)
	lookupKeysMap := make(map[string]bool)

	for _, company := range companies {
		if company == nil {
			continue
		}

		// ID-based primary key
		idKeysMap[companyIDCacheKey(company.ID)] = true

		// Versioned lookup keys
		for key, value := range company.Keys {
			lookupKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
			lookupKeysMap[lookupKey] = true
		}
	}

	// Delete ID keys
	if len(idKeysMap) > 0 {
		idKeys := make([]string, 0, len(idKeysMap))
		for key := range idKeysMap {
			idKeys = append(idKeys, key)
		}
		if batchCache, ok := h.companiesCache.(BatchCacheProvider[*rulesengine.Company]); ok {
			if err := batchCache.BatchDelete(ctx, idKeys); err != nil {
				return fmt.Errorf("batch delete company ID keys: %w", err)
			}
		} else {
			h.companyMu.Lock()
			for _, k := range idKeys {
				if err := h.companiesCache.Delete(ctx, k); err != nil {
					h.logger.Warn(ctx, fmt.Sprintf("Failed to delete company ID key %s: %v", k, err))
				}
			}
			h.companyMu.Unlock()
		}
	}

	// Delete lookup keys
	if len(lookupKeysMap) > 0 {
		lookupKeys := make([]string, 0, len(lookupKeysMap))
		for key := range lookupKeysMap {
			lookupKeys = append(lookupKeys, key)
		}
		if err := h.companyLookupCache.BatchDelete(ctx, lookupKeys); err != nil {
			return fmt.Errorf("batch delete company lookup keys: %w", err)
		}
	}

	return nil
}

// batchDeleteUsers deletes multiple users using batch operations when possible.
// Deletes both ID keys and lookup keys.
func (h *AsyncReplicatorMessageHandler) batchDeleteUsers(ctx context.Context, users []*rulesengine.User) error {
	if len(users) == 0 {
		return nil
	}

	// Collect ID keys and lookup keys separately
	idKeysMap := make(map[string]bool)
	lookupKeysMap := make(map[string]bool)

	for _, user := range users {
		if user == nil {
			continue
		}

		// ID-based primary key
		idKeysMap[userIDCacheKey(user.ID)] = true

		// Versioned lookup keys
		for key, value := range user.Keys {
			lookupKey := resourceKeyToCacheKey(cacheKeyPrefixUser, key, value)
			lookupKeysMap[lookupKey] = true
		}
	}

	// Delete ID keys
	if len(idKeysMap) > 0 {
		idKeys := make([]string, 0, len(idKeysMap))
		for key := range idKeysMap {
			idKeys = append(idKeys, key)
		}
		if batchCache, ok := h.usersCache.(BatchCacheProvider[*rulesengine.User]); ok {
			if err := batchCache.BatchDelete(ctx, idKeys); err != nil {
				return fmt.Errorf("batch delete user ID keys: %w", err)
			}
		} else {
			h.userMu.Lock()
			for _, k := range idKeys {
				if err := h.usersCache.Delete(ctx, k); err != nil {
					h.logger.Warn(ctx, fmt.Sprintf("Failed to delete user ID key %s: %v", k, err))
				}
			}
			h.userMu.Unlock()
		}
	}

	// Delete lookup keys
	if len(lookupKeysMap) > 0 {
		lookupKeys := make([]string, 0, len(lookupKeysMap))
		for key := range lookupKeysMap {
			lookupKeys = append(lookupKeys, key)
		}
		if err := h.userLookupCache.BatchDelete(ctx, lookupKeys); err != nil {
			return fmt.Errorf("batch delete user lookup keys: %w", err)
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
	schematicClient    *client.Client
	companiesCache     CacheProvider[*rulesengine.Company]
	companyLookupCache CacheProvider[string]
	usersCache         CacheProvider[*rulesengine.User]
	userLookupCache    CacheProvider[string]
	logger             *SchematicLogger
	cacheTTL           time.Duration
	config             AsyncLoaderConfig

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
	started            bool // the initial load has been kicked off (guards StartAsyncLoading)
	loadingInProgress  bool // a load (initial or reload) is currently running

	// Completion channels. Recreated on every load run, so callers must snapshot
	// them under loadingMu before selecting on them.
	companiesLoadChan chan struct{}
	usersLoadChan     chan struct{}

	// Load implementations, injectable for tests. Default to the real
	// API-backed loaders in NewAsyncInitialLoader.
	loadCompaniesFn func(ctx context.Context) error
	loadUsersFn     func(ctx context.Context) error

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
	companyLookupCache CacheProvider[string],
	usersCache CacheProvider[*rulesengine.User],
	userLookupCache CacheProvider[string],
	logger *SchematicLogger,
	cacheTTL time.Duration,
	config AsyncLoaderConfig,
) *AsyncInitialLoader {
	loader := &AsyncInitialLoader{
		schematicClient:    schematicClient,
		companiesCache:     companiesCache,
		companyLookupCache: companyLookupCache,
		usersCache:         usersCache,
		userLookupCache:    userLookupCache,
		logger:             logger,
		cacheTTL:           cacheTTL,
		config:             config,
		apiCircuitBreaker:  NewCircuitBreaker(config.CircuitBreakerThreshold, config.CircuitBreakerTimeout),
		rateLimiter:        NewRateLimiter(config.RateLimitRPS),
		concurrencyLimit:   make(chan struct{}, config.MaxConcurrentRequests),
	}
	loader.loadCompaniesFn = loader.loadCompaniesAsync
	loader.loadUsersFn = loader.loadUsersAsync
	return loader
}

// StartAsyncLoading begins the initial load of companies and users exactly
// once. It returns immediately so the connection can be established without
// waiting. OnConnectionReady calls this on every (re)connect; subsequent calls
// are no-ops because reconnects rely on replay rather than another bulk load.
func (al *AsyncInitialLoader) StartAsyncLoading(ctx context.Context) {
	al.loadingMu.Lock()
	if al.started {
		al.loadingMu.Unlock()
		al.logger.Debug(ctx, "Initial data load already started; reconnects rely on replay")
		return
	}
	al.started = true
	al.loadingMu.Unlock()

	al.runLoad(ctx)
}

// Reload forces a fresh bulk load of companies and users, bypassing the
// once-only guard on StartAsyncLoading. It is invoked when the server signals
// MessageTypeReload (the replay window aged out), so the cache must be rebuilt
// from scratch. Like StartAsyncLoading it returns immediately and is a no-op
// while a load is already running.
func (al *AsyncInitialLoader) Reload(ctx context.Context) {
	al.runLoad(ctx)
}

// runLoad performs one full load run in the background. It guards against
// overlapping runs (a reload arriving mid-load, or repeated reload signals) so
// concurrent runs never write the same caches at once, and recreates the
// completion channels for this run.
func (al *AsyncInitialLoader) runLoad(ctx context.Context) {
	al.loadingMu.Lock()
	if al.loadingInProgress {
		al.loadingMu.Unlock()
		al.logger.Warn(ctx, "Initial data load already in progress; skipping duplicate run")
		return
	}
	al.loadingInProgress = true
	al.companiesLoaded = false
	al.usersLoaded = false
	al.companiesLoadError = nil
	al.usersLoadError = nil
	companiesChan := make(chan struct{})
	usersChan := make(chan struct{})
	al.companiesLoadChan = companiesChan
	al.usersLoadChan = usersChan
	al.loadingMu.Unlock()

	al.logger.Info(ctx, "Starting asynchronous data loading")

	// Start companies loading in background
	go func() {
		defer close(companiesChan)
		defer func() {
			if r := recover(); r != nil {
				al.logger.Error(ctx, fmt.Sprintf("Panic in companies loading goroutine: %v", r))
				al.loadingMu.Lock()
				al.companiesLoaded = true
				al.companiesLoadError = fmt.Errorf("panic during companies loading: %v", r)
				al.loadingMu.Unlock()
			}
		}()

		startTime := time.Now()
		err := al.loadCompaniesFn(ctx)
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
		defer close(usersChan)
		defer func() {
			if r := recover(); r != nil {
				al.logger.Error(ctx, fmt.Sprintf("Panic in users loading goroutine: %v", r))
				al.loadingMu.Lock()
				al.usersLoaded = true
				al.usersLoadError = fmt.Errorf("panic during users loading: %v", r)
				al.loadingMu.Unlock()
			}
		}()

		startTime := time.Now()
		err := al.loadUsersFn(ctx)
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

	// Clear the in-progress flag once both loads finish so a later reload can run.
	go func() {
		<-companiesChan
		<-usersChan
		al.loadingMu.Lock()
		al.loadingInProgress = false
		al.loadingMu.Unlock()
	}()

	al.logger.Info(ctx, "Async data loading started in background")
}

// loadingActive reports whether a load run is currently in progress.
func (al *AsyncInitialLoader) loadingActive() bool {
	al.loadingMu.RLock()
	defer al.loadingMu.RUnlock()
	return al.loadingInProgress
}

// WaitForCompletion waits for both companies and users to finish loading
func (al *AsyncInitialLoader) WaitForCompletion(ctx context.Context) error {
	al.logger.Info(ctx, "Waiting for async initial data loading to complete")

	// Snapshot the completion channels; runLoad recreates them on every run.
	al.loadingMu.RLock()
	companiesLoadChan, usersLoadChan := al.companiesLoadChan, al.usersLoadChan
	al.loadingMu.RUnlock()

	// Wait for both to complete or context timeout
	select {
	case <-companiesLoadChan:
		// Companies done, wait for users
		select {
		case <-usersLoadChan:
			// Both done
		case <-ctx.Done():
			return fmt.Errorf("context timeout waiting for users loading: %w", ctx.Err())
		}
	case <-usersLoadChan:
		// Users done, wait for companies
		select {
		case <-companiesLoadChan:
			// Both done
		case <-ctx.Done():
			return fmt.Errorf("context timeout waiting for companies loading: %w", ctx.Err())
		}
	case <-ctx.Done():
		return fmt.Errorf("context timeout waiting for initial loading: %w", ctx.Err())
	}

	// Note: the rate limiter is intentionally left open — the loader is reused
	// across reloads (MessageTypeReload), so it must outlive any single load run
	// and is torn down only when the process exits.

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
			limit64 := int64(pageSize)
			offset64 := int64(offset)
			companiesResp, err := al.schematicClient.Companies.ListCompanies(ctx, &schematicgo.ListCompaniesRequest{
				Limit:  &limit64,
				Offset: &offset64,
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
		defer func() {
			if r := recover(); r != nil {
				al.logger.Error(ctx, fmt.Sprintf("Panic in company results closer: %v", r))
			}
		}()
		wg.Wait()
		close(results)
	}()

	// Process results and cache companies
	var allLookupKeys []string
	var allIDKeys []string
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
					// Track lookup keys and ID keys separately for eviction
					if cacheKey == companyIDCacheKey(company.ID) {
						allIDKeys = append(allIDKeys, cacheKey)
					} else {
						allLookupKeys = append(allLookupKeys, cacheKey)
					}
				}
			}
		}

		processedCount += len(result.data)
		al.logger.Debug(ctx, fmt.Sprintf("Processed companies page at offset %d (%d companies)", result.offset, len(result.data)))
	}

	// Evict missing lookup keys
	if len(allLookupKeys) > 0 {
		if err := al.companyLookupCache.DeleteMissing(ctx, allLookupKeys); err != nil {
			al.logger.Error(ctx, fmt.Sprintf("Failed to evict missing company lookup keys: %v", err))
		}
	}

	// Evict missing ID keys
	if len(allIDKeys) > 0 {
		if err := al.companiesCache.DeleteMissing(ctx, allIDKeys); err != nil {
			al.logger.Error(ctx, fmt.Sprintf("Failed to evict missing company ID keys: %v", err))
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
			limit64 := int64(pageSize)
			offset64 := int64(offset)
			usersResp, err := al.schematicClient.Companies.ListUsers(ctx, &schematicgo.ListUsersRequest{
				Limit:  &limit64,
				Offset: &offset64,
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
		defer func() {
			if r := recover(); r != nil {
				al.logger.Error(ctx, fmt.Sprintf("Panic in user results closer: %v", r))
			}
		}()
		wg.Wait()
		close(results)
	}()

	// Process results and cache users
	var allLookupKeys []string
	var allIDKeys []string
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
					// Track lookup keys and ID keys separately for eviction
					if cacheKey == userIDCacheKey(user.ID) {
						allIDKeys = append(allIDKeys, cacheKey)
					} else {
						allLookupKeys = append(allLookupKeys, cacheKey)
					}
				}
			}
		}

		processedCount += len(result.data)
		al.logger.Debug(ctx, fmt.Sprintf("Processed users page at offset %d (%d users)", result.offset, len(result.data)))
	}

	// Evict missing lookup keys
	if len(allLookupKeys) > 0 {
		if err := al.userLookupCache.DeleteMissing(ctx, allLookupKeys); err != nil {
			al.logger.Error(ctx, fmt.Sprintf("Failed to evict missing user lookup keys: %v", err))
		}
	}

	// Evict missing ID keys
	if len(allIDKeys) > 0 {
		if err := al.usersCache.DeleteMissing(ctx, allIDKeys); err != nil {
			al.logger.Error(ctx, fmt.Sprintf("Failed to evict missing user ID keys: %v", err))
		}
	}

	al.totalUsers = processedCount
	al.logger.Info(ctx, fmt.Sprintf("Successfully cached %d users via concurrent async loading", processedCount))
	return nil
}

// cacheCompanyForKeys caches a company using ID-based deduplicated keyspace.
// Writes the full company to the ID key and the company ID string to each lookup key.
func (al *AsyncInitialLoader) cacheCompanyForKeys(ctx context.Context, company *rulesengine.Company) map[string]error {
	if company == nil || len(company.Keys) == 0 {
		return nil
	}

	cacheResults := make(map[string]error)

	// Write full company to ID-based primary key
	idKey := companyIDCacheKey(company.ID)
	cacheResults[idKey] = al.companiesCache.Set(ctx, idKey, company, al.cacheTTL)

	// Write company ID string to each versioned lookup key
	for key, value := range company.Keys {
		lookupKey := resourceKeyToCacheKey(cacheKeyPrefixCompany, key, value)
		cacheResults[lookupKey] = al.companyLookupCache.Set(ctx, lookupKey, company.ID, al.cacheTTL)
	}

	return cacheResults
}

// cacheUserForKeys caches a user using ID-based deduplicated keyspace.
// Writes the full user to the ID key and the user ID string to each lookup key.
func (al *AsyncInitialLoader) cacheUserForKeys(ctx context.Context, user *rulesengine.User) map[string]error {
	if user == nil || len(user.Keys) == 0 {
		return nil
	}

	cacheResults := make(map[string]error)

	// Write full user to ID-based primary key
	idKey := userIDCacheKey(user.ID)
	cacheResults[idKey] = al.usersCache.Set(ctx, idKey, user, al.cacheTTL)

	// Write user ID string to each versioned lookup key
	for key, value := range user.Keys {
		lookupKey := resourceKeyToCacheKey(cacheKeyPrefixUser, key, value)
		cacheResults[lookupKey] = al.userLookupCache.Set(ctx, lookupKey, user.ID, al.cacheTTL)
	}

	return cacheResults
}

// AsyncConnectionReadyHandler implements the ConnectionReadyHandler interface for asynchronous loading
type AsyncConnectionReadyHandler struct {
	schematicClient    *client.Client
	wsClient           *schematicdatastreamws.Client
	companiesCache     CacheProvider[*rulesengine.Company]
	companyLookupCache CacheProvider[string]
	usersCache         CacheProvider[*rulesengine.User]
	userLookupCache    CacheProvider[string]
	flagsCache         CacheProvider[*rulesengine.Flag]
	logger             *SchematicLogger
	cacheTTL           time.Duration
	asyncLoader        *AsyncInitialLoader
	replayCursor       *ReplayCursor // optional; nil when replay is disabled
	stats              *ReplayStats  // optional; counters for observability
}

// SetReplayCursor injects the cursor read on (re)connect to populate ReplayFrom
// on the subscribe requests.
func (h *AsyncConnectionReadyHandler) SetReplayCursor(cursor *ReplayCursor) {
	h.replayCursor = cursor
}

// SetStats injects the counters updated on connect/replay/reload.
func (h *AsyncConnectionReadyHandler) SetStats(stats *ReplayStats) {
	h.stats = stats
}

// replayFrom returns the resume token to attach to subscribe requests, or nil
// when there is no cursor yet (fresh start → full load).
func (h *AsyncConnectionReadyHandler) replayFrom() *string {
	if h.replayCursor == nil {
		return nil
	}
	if from := h.replayCursor.Get(); from != "" {
		return &from
	}
	return nil
}

// TriggerReload re-runs the bulk initial load. Invoked when the server signals
// MessageTypeReload (resume token aged out).
func (h *AsyncConnectionReadyHandler) TriggerReload(ctx context.Context) {
	h.logger.Info(ctx, "Reloading initial data after replay window aged out")
	h.stats.IncFullReloads()
	h.asyncLoader.Reload(ctx)
}

// NewAsyncConnectionReadyHandler creates a new async connection ready handler
func NewAsyncConnectionReadyHandler(
	schematicClient *client.Client,
	wsClient *schematicdatastreamws.Client,
	companiesCache CacheProvider[*rulesengine.Company],
	usersCache CacheProvider[*rulesengine.User],
	flagsCache CacheProvider[*rulesengine.Flag],
	companyLookupCache CacheProvider[string],
	userLookupCache CacheProvider[string],
	logger *SchematicLogger,
	cacheTTL time.Duration,
	asyncLoaderConfig AsyncLoaderConfig,
) *AsyncConnectionReadyHandler {
	asyncLoader := NewAsyncInitialLoader(
		schematicClient,
		companiesCache,
		companyLookupCache,
		usersCache,
		userLookupCache,
		logger,
		cacheTTL,
		asyncLoaderConfig,
	)

	return &AsyncConnectionReadyHandler{
		schematicClient:    schematicClient,
		wsClient:           wsClient,
		companiesCache:     companiesCache,
		companyLookupCache: companyLookupCache,
		usersCache:         usersCache,
		userLookupCache:    userLookupCache,
		flagsCache:         flagsCache,
		logger:             logger,
		cacheTTL:           cacheTTL,
		asyncLoader:        asyncLoader,
	}
}

// OnConnectionReady implements the ConnectionReadyHandler interface for asynchronous loading
func (h *AsyncConnectionReadyHandler) OnConnectionReady(ctx context.Context) error {
	// Async mode: start loading in background, establish connection quickly
	h.stats.IncConnections()
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

	// On a reconnect, replay missed changes from the last processed message
	// rather than relying solely on the bulk reload above.
	replayFrom := h.replayFrom()
	if replayFrom != nil {
		h.stats.IncReplaysEngaged()
		h.logger.Info(ctx, fmt.Sprintf("Replaying from stream ID %s", *replayFrom))
	} else {
		h.logger.Info(ctx, "No replay cursor; subscribing for a full load")
	}

	// Subscribe to all company updates using bulk subscription entity type
	companySubscription := &schematicdatastreamws.DataStreamBaseReq{
		Data: schematicdatastreamws.DataStreamReq{
			Action:     schematicdatastreamws.ActionStart,
			EntityType: schematicdatastreamws.EntityTypeCompanies,
			ReplayFrom: replayFrom,
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
			ReplayFrom: replayFrom,
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
			ReplayFrom: h.replayFrom(),
		},
	}
	if err := h.wsClient.SendMessage(flagsRequest); err != nil {
		h.logger.Error(ctx, fmt.Sprintf("Failed to request flags data: %v", err))
		return err
	}

	h.logger.Info(ctx, "Successfully requested flags data")
	return nil
}
