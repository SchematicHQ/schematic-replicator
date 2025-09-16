package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
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
	defaultBaseURL              = "ws://localhost:8080/datastream"
	apiKeyEnvVar                = "SCHEMATIC_API_KEY"
	defaultCacheTTL             = 0 * time.Second // Unlimited cache by default
	defaultCacheCleanupInterval = 1 * time.Hour   // Clean up stale cache entries every hour
	defaultHealthPort           = 8090
	cacheKeyPrefix              = "schematic"
	cacheKeyPrefixCompany       = "company"
	cacheKeyPrefixUser          = "user"
	cacheKeyPrefixFlags         = "flags"
)

// HealthServer provides health and readiness endpoints
type HealthServer struct {
	datastreamClient *schematicdatastreamws.Client
	redisClient      interface{}
	logger           *SchematicLogger
	server           *http.Server
	mu               sync.RWMutex
}

// HealthStatus represents the health status response
type HealthStatus struct {
	Status     string            `json:"status"`
	Ready      bool              `json:"ready"`
	Connected  bool              `json:"connected"`
	Components map[string]string `json:"components"`
	Timestamp  time.Time         `json:"timestamp"`
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
func (hs *HealthServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	status := HealthStatus{
		Status:    "ok",
		Ready:     false,
		Connected: false,
		Components: map[string]string{
			"redis":      "unknown",
			"datastream": "unknown",
		},
		Timestamp: time.Now(),
	}

	// Check datastream connection
	if hs.datastreamClient != nil {
		status.Connected = hs.datastreamClient.IsConnected()
		status.Ready = hs.datastreamClient.IsReady()
		if status.Connected {
			status.Components["datastream"] = "connected"
		} else {
			status.Components["datastream"] = "disconnected"
		}
	}

	// Redis is assumed healthy if we got this far (connection tested at startup)
	status.Components["redis"] = "connected"

	// Overall status
	if !status.Connected {
		status.Status = "unhealthy"
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(status); err != nil {
		hs.logger.Error(context.Background(), fmt.Sprintf("Failed to encode health status: %v", err))
	}
}

// readinessHandler handles the /ready endpoint (readiness probe)
func (hs *HealthServer) readinessHandler(w http.ResponseWriter, r *http.Request) {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	ready := false
	if hs.datastreamClient != nil {
		ready = hs.datastreamClient.IsReady()
	}

	status := HealthStatus{
		Status:    "not_ready",
		Ready:     ready,
		Connected: false,
		Components: map[string]string{
			"redis":      "connected",
			"datastream": "not_ready",
		},
		Timestamp: time.Now(),
	}

	if hs.datastreamClient != nil {
		status.Connected = hs.datastreamClient.IsConnected()
		if ready {
			status.Status = "ready"
			status.Components["datastream"] = "ready"
			w.WriteHeader(http.StatusOK)
		} else if status.Connected {
			status.Components["datastream"] = "connected"
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			status.Components["datastream"] = "disconnected"
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	} else {
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

	// Get datastream WebSocket URL from environment or use default
	datastreamURL := os.Getenv("SCHEMATIC_DATASTREAM_URL")
	if datastreamURL == "" {
		datastreamURL = defaultBaseURL
	}

	// Get API base URL from environment or use default
	apiBaseURL := os.Getenv("SCHEMATIC_API_URL")
	if apiBaseURL == "" {
		apiBaseURL = "https://api.schematichq.com"
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
	logger.Info(context.Background(), fmt.Sprintf("Datastream URL: %s", datastreamURL))
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

	logger.Info(context.Background(), "Using Redis cache")
	companiesCache = NewRedisCache[*rulesengine.Company](redisClient, cacheTTL)
	usersCache = NewRedisCache[*rulesengine.User](redisClient, cacheTTL)
	featuresCache = NewRedisCache[*rulesengine.Flag](redisClient, cacheTTL)

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

	// Create message handler with caching
	messageHandler := NewReplicatorMessageHandler(logger, companiesCache, usersCache, featuresCache, cacheTTL)

	// Create connection ready handler (wsClient will be set later)
	connectionReadyHandler := NewConnectionReadyHandler(schematicClient, nil, companiesCache, usersCache, featuresCache, logger, cacheTTL)

	// Set up headers with API key
	headers := http.Header{}
	headers.Set("X-Schematic-Api-Key", apiKey)

	// Configure the WebSocket client
	wsOptions := schematicdatastreamws.ClientOptions{
		URL:                    datastreamURL,
		Headers:                headers,
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

	// Wait for shutdown signal
	<-sigChan
	logger.Info(context.Background(), "Received shutdown signal, closing connection...")

	// Stop cache cleanup manager
	if cacheCleanupManager != nil {
		cacheCleanupManager.Stop()
		logger.Info(context.Background(), "Cache cleanup manager stopped")
	}

	// Stop health server
	healthServer.Stop()

	// Close the WebSocket connection
	datastreamClient.Close()

	logger.Info(context.Background(), "Datastream replicator stopped")
}
