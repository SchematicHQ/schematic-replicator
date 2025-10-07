package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/schematichq/rulesengine"
	schematicdatastreamws "github.com/schematichq/schematic-datastream-ws"
	"github.com/schematichq/schematic-go/client"
	"github.com/schematichq/schematic-go/option"
)

const (
	defaultAPIURL               = "https://api.schematichq.com"
	apiKeyEnvVar                = "SCHEMATIC_API_KEY"
	defaultCacheTTL             = 0 * time.Second // Unlimited cache by default
	defaultCacheCleanupInterval = 1 * time.Hour   // Clean up stale cache entries every hour
	defaultHealthPort           = 8090
	cacheKeyPrefix              = "schematic"
	cacheKeyPrefixCompany       = "company"
	cacheKeyPrefixUser          = "user"
	cacheKeyPrefixFlags         = "flags"
)

// HealthServer provides health and readiness endpoints for container orchestration
type HealthServer struct {
	datastreamClient *schematicdatastreamws.Client
	redisClient      interface{}
	logger           *SchematicLogger
	server           *http.Server
	mu               sync.RWMutex
}

// HealthStatusType represents the overall health status
type HealthStatusType string

const (
	HealthStatusHealthy   HealthStatusType = "healthy"
	HealthStatusUnhealthy HealthStatusType = "unhealthy"
)

// ReadinessStatusType represents the readiness status
type ReadinessStatusType string

const (
	ReadinessStatusReady    ReadinessStatusType = "ready"
	ReadinessStatusNotReady ReadinessStatusType = "not_ready"
)

// ComponentStatusType represents individual component status
type ComponentStatusType string

const (
	ComponentStatusConnected        ComponentStatusType = "connected"
	ComponentStatusDisconnected     ComponentStatusType = "disconnected"
	ComponentStatusReady            ComponentStatusType = "ready"
	ComponentStatusConnectedLoading ComponentStatusType = "connected_loading"
	ComponentStatusNotReady         ComponentStatusType = "not_ready"
	ComponentStatusUnknown          ComponentStatusType = "unknown"
)

// HealthStatus represents the health status response
type HealthStatus struct {
	Status       HealthStatusType               `json:"status"`
	Ready        bool                           `json:"ready"`
	Connected    bool                           `json:"connected"`
	Components   map[string]ComponentStatusType `json:"components"`
	CacheVersion string                         `json:"cache_version"`
	Timestamp    time.Time                      `json:"timestamp"`
}

// NewHealthServer creates a new health server
func NewHealthServer(port int, datastreamClient *schematicdatastreamws.Client, redisClient interface{}, logger *SchematicLogger) *HealthServer {
	hs := &HealthServer{
		datastreamClient: datastreamClient,
		redisClient:      redisClient,
		logger:           logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", hs.healthHandler)
	mux.HandleFunc("/ready", hs.readinessHandler)

	hs.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	return hs
}

// Start starts the health server
func (hs *HealthServer) Start() {
	go func() {
		hs.logger.Info(context.Background(), fmt.Sprintf("Health server starting on port %s", hs.server.Addr))
		if err := hs.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			hs.logger.Error(context.Background(), fmt.Sprintf("Health server error: %v", err))
		}
	}()
}

// Stop stops the health server
func (hs *HealthServer) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := hs.server.Shutdown(ctx); err != nil {
		hs.logger.Error(context.Background(), fmt.Sprintf("Health server shutdown error: %v", err))
	}
}

// healthHandler handles the /health endpoint (liveness probe)
// Returns healthy if the process is alive and Redis is connected
func (hs *HealthServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	status := HealthStatus{
		Status:    HealthStatusHealthy,
		Ready:     false,
		Connected: false,
		Components: map[string]ComponentStatusType{
			"redis":      ComponentStatusConnected, // Redis is assumed healthy if we got this far (connection tested at startup)
			"datastream": ComponentStatusUnknown,
		},
		CacheVersion: rulesengine.GetVersionKey(),
		Timestamp:    time.Now(),
	}

	// Check datastream connection (for informational purposes)
	if hs.datastreamClient != nil {
		status.Connected = hs.datastreamClient.IsConnected()
		status.Ready = hs.datastreamClient.IsReady()

		if status.Connected {
			status.Components["datastream"] = ComponentStatusConnected
		} else {
			status.Components["datastream"] = ComponentStatusDisconnected
		}
	}

	// Liveness check: Process is healthy if Redis is working
	// Datastream connectivity issues don't make the process unhealthy (it can retry)
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(status); err != nil {
		hs.logger.Error(context.Background(), fmt.Sprintf("Failed to encode health status: %v", err))
	}
}

// readinessHandler handles the /ready endpoint (readiness probe)
// Returns ready only when connected to datastream and initial data is loaded
func (hs *HealthServer) readinessHandler(w http.ResponseWriter, r *http.Request) {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	ready := false
	connected := false

	if hs.datastreamClient != nil {
		connected = hs.datastreamClient.IsConnected()
		ready = hs.datastreamClient.IsReady()
	}

	status := HealthStatus{
		Status:    HealthStatusHealthy, // Will be updated based on readiness
		Ready:     ready,
		Connected: connected,
		Components: map[string]ComponentStatusType{
			"redis":      ComponentStatusConnected,
			"datastream": ComponentStatusNotReady,
		},
		CacheVersion: rulesengine.GetVersionKey(),
		Timestamp:    time.Now(),
	}

	if ready {
		// Fully ready: connected and initial data loaded
		status.Status = HealthStatusHealthy
		status.Components["datastream"] = ComponentStatusReady
		w.WriteHeader(http.StatusOK)
	} else if connected {
		// Connected but still loading initial data
		status.Status = HealthStatusHealthy // Still healthy, just not ready yet
		status.Components["datastream"] = ComponentStatusConnectedLoading
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		// Not connected at all
		status.Status = HealthStatusHealthy // Health endpoint should still return healthy
		status.Components["datastream"] = ComponentStatusDisconnected
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(status); err != nil {
		hs.logger.Error(context.Background(), fmt.Sprintf("Failed to encode readiness status: %v", err))
	}
}

func main() {

	// Get API key from environment
	apiKey := os.Getenv(apiKeyEnvVar)
	if apiKey == "" {
		log.Fatalf("Please set %s environment variable", apiKeyEnvVar)
	}

	// Get API base URL from environment or use default
	apiBaseURL := os.Getenv("SCHEMATIC_API_URL")
	if apiBaseURL == "" {
		apiBaseURL = defaultAPIURL
	}

	// Get datastream WebSocket URL from environment
	// If not set, the schematic-go datastream client will automatically
	// convert the API URL to a WebSocket URL (http->ws, https->wss, +/datastream path)
	datastreamURL := os.Getenv("SCHEMATIC_DATASTREAM_URL")
	if datastreamURL == "" {
		datastreamURL = apiBaseURL // Let the datastream client handle URL conversion
	}

	// Get cache TTL from environment
	// Default is unlimited cache (0). Override with CACHE_TTL env var (e.g., "5m", "1h", "30s")
	// Set CACHE_TTL="0" or CACHE_TTL="0s" for unlimited cache explicitly
	cacheTTL := defaultCacheTTL
	if ttlStr := os.Getenv("CACHE_TTL"); ttlStr != "" {
		if parsedTTL, err := time.ParseDuration(ttlStr); err == nil {
			cacheTTL = parsedTTL
		}
	}

	// Get cache cleanup interval from environment
	// Default is 1 hour. Override with CACHE_CLEANUP_INTERVAL env var (e.g., "30m", "2h")
	// Set to "0" or "0s" to disable cache cleanup (not recommended with unlimited cache)
	cacheCleanupInterval := defaultCacheCleanupInterval
	if cleanupStr := os.Getenv("CACHE_CLEANUP_INTERVAL"); cleanupStr != "" {
		if parsedInterval, err := time.ParseDuration(cleanupStr); err == nil {
			cacheCleanupInterval = parsedInterval
		}
	}

	// Create logger
	logger := NewSchematicLogger()

	// Set log level from environment
	if logLevelStr := os.Getenv("LOG_LEVEL"); logLevelStr != "" {
		switch strings.ToLower(logLevelStr) {
		case "debug":
			logger.SetLevel(LogLevelDebug)
		case "info":
			logger.SetLevel(LogLevelInfo)
		case "warn":
			logger.SetLevel(LogLevelWarn)
		case "error":
			logger.SetLevel(LogLevelError)
		}
	}

	// Get health server port from environment
	healthPort := defaultHealthPort
	if portStr := os.Getenv("HEALTH_PORT"); portStr != "" {
		if parsedPort, err := strconv.Atoi(portStr); err == nil && parsedPort > 0 {
			healthPort = parsedPort
		}
	}

	logger.Info(context.Background(), "Starting Schematic Datastream Replicator...")
	logger.Info(context.Background(), fmt.Sprintf("API URL: %s", apiBaseURL))
	if os.Getenv("SCHEMATIC_DATASTREAM_URL") != "" {
		logger.Info(context.Background(), fmt.Sprintf("Datastream URL: %s (explicit)", datastreamURL))
	} else {
		logger.Info(context.Background(), fmt.Sprintf("Datastream URL: %s (derived from API URL)", datastreamURL))
	}
	logger.Info(context.Background(), fmt.Sprintf("Health server port: %d", healthPort))

	// Create cache providers - only Redis, no local cache fallback
	var companiesCache CacheProvider[*rulesengine.Company]
	var usersCache CacheProvider[*rulesengine.User]
	var featuresCache CacheProvider[*rulesengine.Flag]

	// Create Redis client - if this fails, the application will exit
	redisClient := setupRedisClient()

	// Test Redis connection
	if err := testRedisConnection(redisClient, logger); err != nil {
		log.Fatalf("Redis connection failed: %v", err)
	}

	logger.Info(context.Background(), "Using Redis cache with batch support")
	companiesCache = NewRedisBatchCache[*rulesengine.Company](redisClient, cacheTTL)
	usersCache = NewRedisBatchCache[*rulesengine.User](redisClient, cacheTTL)
	featuresCache = NewRedisBatchCache[*rulesengine.Flag](redisClient, cacheTTL)

	// Initialize cache cleanup manager (only if cleanup is enabled)
	var cacheCleanupManager *CacheCleanupManager
	if cacheCleanupInterval > 0 {
		cacheCleanupManager = NewCacheCleanupManager(
			featuresCache,
			companiesCache,
			usersCache,
			logger,
			cacheCleanupInterval,
		)
		logger.Info(context.Background(), fmt.Sprintf("Cache cleanup enabled with interval: %v", cacheCleanupInterval))
	} else {
		logger.Info(context.Background(), "Cache cleanup disabled")
	}

	// Initialize Schematic Go client with proper configuration
	schematicClient := client.NewClient(
		option.WithAPIKey(apiKey),
		option.WithBaseURL(apiBaseURL),
	)

	// Initialize datastream WebSocket client (will be configured later)
	var datastreamClient *schematicdatastreamws.Client

	// Get async configuration from environment or use defaults
	asyncConfig := DefaultAsyncConfig()

	// Auto-detect or configure number of workers
	if numWorkersStr := os.Getenv("NUM_WORKERS"); numWorkersStr != "" {
		if numWorkers, err := strconv.Atoi(numWorkersStr); err == nil && numWorkers > 0 {
			asyncConfig.NumWorkers = numWorkers
		}
	}

	// If still 0 (default), auto-detect based on CPU cores with reasonable bounds
	if asyncConfig.NumWorkers == 0 {
		numCPU := runtime.NumCPU()
		workers := numCPU
		if workers < 2 {
			workers = 2 // Minimum for small systems
		}
		if workers > 16 {
			workers = 16 // Cap for large systems to avoid resource exhaustion
		}
		asyncConfig.NumWorkers = workers
	}

	if batchSizeStr := os.Getenv("BATCH_SIZE"); batchSizeStr != "" {
		if batchSize, err := strconv.Atoi(batchSizeStr); err == nil && batchSize > 0 {
			asyncConfig.BatchSize = batchSize
		}
	}

	if batchTimeoutStr := os.Getenv("BATCH_TIMEOUT"); batchTimeoutStr != "" {
		if batchTimeout, err := time.ParseDuration(batchTimeoutStr); err == nil {
			asyncConfig.BatchTimeout = batchTimeout
		}
	}

	// Channel size configuration for memory management
	if companyChanSizeStr := os.Getenv("COMPANY_CHANNEL_SIZE"); companyChanSizeStr != "" {
		if size, err := strconv.Atoi(companyChanSizeStr); err == nil && size > 0 {
			asyncConfig.CompanyChannelSize = size
		}
	}

	if userChanSizeStr := os.Getenv("USER_CHANNEL_SIZE"); userChanSizeStr != "" {
		if size, err := strconv.Atoi(userChanSizeStr); err == nil && size > 0 {
			asyncConfig.UserChannelSize = size
		}
	}

	if flagsChanSizeStr := os.Getenv("FLAGS_CHANNEL_SIZE"); flagsChanSizeStr != "" {
		if size, err := strconv.Atoi(flagsChanSizeStr); err == nil && size > 0 {
			asyncConfig.FlagsChannelSize = size
		}
	}

	// Circuit breaker configuration for customer environment resilience
	if cbThresholdStr := os.Getenv("CIRCUIT_BREAKER_THRESHOLD"); cbThresholdStr != "" {
		if threshold, err := strconv.Atoi(cbThresholdStr); err == nil && threshold > 0 {
			asyncConfig.CircuitBreakerThreshold = threshold
		}
	}

	if cbTimeoutStr := os.Getenv("CIRCUIT_BREAKER_TIMEOUT"); cbTimeoutStr != "" {
		if timeout, err := time.ParseDuration(cbTimeoutStr); err == nil {
			asyncConfig.CircuitBreakerTimeout = timeout
		}
	}

	logger.Info(context.Background(), fmt.Sprintf("Async processing config: workers=%d, batch_size=%d, batch_timeout=%v, channels=[company:%d, user:%d, flags:%d], circuit_breaker=[threshold:%d, timeout:%v]",
		asyncConfig.NumWorkers, asyncConfig.BatchSize, asyncConfig.BatchTimeout,
		asyncConfig.CompanyChannelSize, asyncConfig.UserChannelSize, asyncConfig.FlagsChannelSize,
		asyncConfig.CircuitBreakerThreshold, asyncConfig.CircuitBreakerTimeout))

	// Create async message handler with caching and batching
	messageHandler := NewAsyncReplicatorMessageHandler(companiesCache, usersCache, featuresCache, logger, cacheTTL, asyncConfig)

	// Create connection ready handler (wsClient will be set later)
	connectionReadyHandler := NewConnectionReadyHandler(schematicClient, nil, companiesCache, usersCache, featuresCache, logger, cacheTTL)

	// Configure the WebSocket client
	wsOptions := schematicdatastreamws.ClientOptions{
		URL:                    datastreamURL,
		ApiKey:                 apiKey,
		MessageHandler:         messageHandler.HandleMessage,
		ConnectionReadyHandler: connectionReadyHandler.OnConnectionReady,
		Logger:                 logger,
		MaxReconnectAttempts:   10,
		MinReconnectDelay:      1 * time.Second,
		MaxReconnectDelay:      30 * time.Second,
	}

	datastreamClient, err := schematicdatastreamws.NewClient(wsOptions)
	if err != nil {
		log.Fatalf("Failed to create WebSocket client: %v", err)
	}

	// Set the WebSocket client in the connection ready handler
	connectionReadyHandler.SetWebSocketClient(datastreamClient)

	// Create and start health server
	healthServer := NewHealthServer(healthPort, datastreamClient, redisClient, logger)
	healthServer.Start()

	// Start the WebSocket connection
	datastreamClient.Start()

	// Start cache cleanup manager if enabled
	if cacheCleanupManager != nil {
		cacheCleanupManager.Start(context.Background())
	}

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Monitor for errors
	go func() {
		errorChan := datastreamClient.GetErrorChannel()
		for err := range errorChan {
			logger.Error(context.Background(), fmt.Sprintf("WebSocket error: %v", err))
		}
	}()

	// Monitor async handler metrics
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				processed, dropped := messageHandler.GetMetrics()
				if processed > 0 || dropped > 0 {
					logger.Info(context.Background(), fmt.Sprintf("Message metrics: processed=%d, dropped=%d", processed, dropped))
				}
			case <-sigChan:
				return
			}
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	logger.Info(context.Background(), "Received shutdown signal, closing connection...")

	// Stop cache cleanup manager
	if cacheCleanupManager != nil {
		cacheCleanupManager.Stop()
		logger.Info(context.Background(), "Cache cleanup manager stopped")
	}

	// Stop async message handler
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := messageHandler.Shutdown(shutdownCtx); err != nil {
		logger.Error(context.Background(), fmt.Sprintf("Error shutting down async handler: %v", err))
	} else {
		logger.Info(context.Background(), "Async message handler stopped")
	}

	// Stop health server
	healthServer.Stop()

	// Close the WebSocket connection
	datastreamClient.Close()

	logger.Info(context.Background(), "Datastream replicator stopped")
}
