package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"github.com/schematichq/rulesengine"
	schematicdatastreamws "github.com/schematichq/schematic-datastream-ws"
	"github.com/schematichq/schematic-go/client"
	"github.com/schematichq/schematic-go/option"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestReadiness(t *testing.T, version string) (*CacheReadiness, *miniredis.Miniredis, redis.Cmdable) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewCacheReadiness(client, NewSchematicLogger(), version, 0), mr, client
}

// A cold start is not ready until both halves of the initial load land, and
// completing persists a marker for the current cache version.
func TestCacheReadinessColdStartWaitsForFlagsAndEntities(t *testing.T) {
	ctx := context.Background()
	r, mr, _ := newTestReadiness(t, "v1")

	r.Load(ctx)
	assert.False(t, r.IsReady(), "no marker: cold start is not ready")

	r.MarkFlagsLoaded(ctx)
	assert.False(t, r.IsReady(), "flags alone are not a complete cache")
	assert.False(t, mr.Exists(defaultLoadCompleteKey))

	r.MarkEntitiesLoaded(ctx)
	assert.True(t, r.IsReady())
	got, err := mr.Get(defaultLoadCompleteKey)
	require.NoError(t, err)
	assert.Equal(t, "v1", got, "marker records the cache version it vouches for")
}

// A restarted replicator with a marker for its cache version is ready before
// anything loads, without a datastream connection.
func TestCacheReadinessRestartWithMarkerIsReady(t *testing.T) {
	ctx := context.Background()
	first, _, client := newTestReadiness(t, "v1")
	first.MarkFlagsLoaded(ctx)
	first.MarkEntitiesLoaded(ctx)

	restarted := NewCacheReadiness(client, NewSchematicLogger(), "v1", 0)
	assert.False(t, restarted.IsReady())
	restarted.Load(ctx)
	assert.True(t, restarted.IsReady())
}

// A marker written under another rules engine version must not make the new,
// empty key space look ready.
func TestCacheReadinessVersionChangeDoesNotInherit(t *testing.T) {
	ctx := context.Background()
	old, _, client := newTestReadiness(t, "v1")
	old.MarkFlagsLoaded(ctx)
	old.MarkEntitiesLoaded(ctx)
	require.True(t, old.IsReady())

	upgraded := NewCacheReadiness(client, NewSchematicLogger(), "v2", 0)
	upgraded.Load(ctx)
	assert.False(t, upgraded.IsReady(), "a v1 marker must not vouch for the v2 key space")

	upgraded.MarkFlagsLoaded(ctx)
	upgraded.MarkEntitiesLoaded(ctx)
	assert.True(t, upgraded.IsReady())

	// Rolling back reads a v2 marker, which doesn't vouch for v1 either.
	rolledBack := NewCacheReadiness(client, NewSchematicLogger(), "v1", 0)
	rolledBack.Load(ctx)
	assert.False(t, rolledBack.IsReady())
}

// With a cache TTL the marker expires with the entries it vouches for.
func TestCacheReadinessMarkerUsesCacheTTL(t *testing.T) {
	ctx := context.Background()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	r := NewCacheReadiness(client, NewSchematicLogger(), "v1", time.Hour)
	r.MarkFlagsLoaded(ctx)
	r.MarkEntitiesLoaded(ctx)
	assert.Equal(t, time.Hour, mr.TTL(defaultLoadCompleteKey))

	noTTL := NewCacheReadiness(client, NewSchematicLogger(), "v2", 0)
	noTTL.MarkFlagsLoaded(ctx)
	noTTL.MarkEntitiesLoaded(ctx)
	assert.Equal(t, time.Duration(0), mr.TTL(defaultLoadCompleteKey), "unlimited cache means no marker expiry")
}

// Handlers built without a tracker must not panic.
func TestCacheReadinessNilSafe(t *testing.T) {
	var r *CacheReadiness
	ctx := context.Background()
	r.Load(ctx)
	r.MarkFlagsLoaded(ctx)
	r.MarkEntitiesLoaded(ctx)
	assert.False(t, r.IsReady())
}

// A persisted cursor is only honored when the marker vouches for the cache: a
// cursor left by an initial load that never finished (or by another cache
// version) would otherwise skip the bulk load and leave the cache incomplete.
func TestDiscardUnvouchedReplayCursor(t *testing.T) {
	ctx := context.Background()

	t.Run("cursor without marker is discarded", func(t *testing.T) {
		r, mr, client := newTestReadiness(t, "v1")
		require.NoError(t, client.Set(ctx, defaultReplayCursorKey, "100-0", 0).Err())
		cursor := NewReplayCursor(client, NewSchematicLogger(), "")
		cursor.Load(ctx)
		r.Load(ctx)

		assert.True(t, discardUnvouchedReplayCursor(ctx, cursor, r, NewSchematicLogger()))
		assert.Equal(t, "", cursor.Get())
		assert.False(t, mr.Exists(defaultReplayCursorKey))
	})

	t.Run("cursor with a stale-version marker is discarded", func(t *testing.T) {
		r, _, client := newTestReadiness(t, "v2")
		require.NoError(t, client.Set(ctx, defaultReplayCursorKey, "100-0", 0).Err())
		require.NoError(t, client.Set(ctx, defaultLoadCompleteKey, "v1", 0).Err())
		cursor := NewReplayCursor(client, NewSchematicLogger(), "")
		cursor.Load(ctx)
		r.Load(ctx)

		assert.True(t, discardUnvouchedReplayCursor(ctx, cursor, r, NewSchematicLogger()))
		assert.Equal(t, "", cursor.Get())
	})

	t.Run("cursor with a current marker is kept", func(t *testing.T) {
		r, _, client := newTestReadiness(t, "v1")
		require.NoError(t, client.Set(ctx, defaultReplayCursorKey, "100-0", 0).Err())
		require.NoError(t, client.Set(ctx, defaultLoadCompleteKey, "v1", 0).Err())
		cursor := NewReplayCursor(client, NewSchematicLogger(), "")
		cursor.Load(ctx)
		r.Load(ctx)

		assert.False(t, discardUnvouchedReplayCursor(ctx, cursor, r, NewSchematicLogger()))
		assert.Equal(t, "100-0", cursor.Get())
	})
}

// The async loader reports the company/user half only when a run succeeds, and
// a failed reload leaves an already ready cache ready.
func TestAsyncLoaderReportsCompletionToReadiness(t *testing.T) {
	ctx := context.Background()

	t.Run("successful run", func(t *testing.T) {
		r, _, _ := newTestReadiness(t, "v1")
		r.MarkFlagsLoaded(ctx)
		al := newTestLoader(t)
		al.readiness = r
		release := make(chan struct{})
		al.loadCompaniesFn = func(context.Context) error { <-release; return nil }
		al.loadUsersFn = func(context.Context) error { return nil }

		al.StartAsyncLoading(ctx)
		assert.False(t, r.IsReady(), "not ready while companies are still loading")
		close(release)
		waitIdle(t, al)
		assert.True(t, r.IsReady())
	})

	t.Run("failed run stays not ready", func(t *testing.T) {
		r, _, _ := newTestReadiness(t, "v1")
		r.MarkFlagsLoaded(ctx)
		al := newTestLoader(t)
		al.readiness = r
		al.loadCompaniesFn = func(context.Context) error { return nil }
		al.loadUsersFn = func(context.Context) error { return errors.New("api unavailable") }

		al.StartAsyncLoading(ctx)
		waitIdle(t, al)
		assert.False(t, r.IsReady())
	})

	t.Run("failed reload keeps ready", func(t *testing.T) {
		r, _, _ := newTestReadiness(t, "v1")
		r.MarkFlagsLoaded(ctx)
		al := newTestLoader(t)
		al.readiness = r
		var fail atomic.Bool
		al.loadCompaniesFn = func(context.Context) error { return nil }
		al.loadUsersFn = func(context.Context) error {
			if fail.Load() {
				return errors.New("api unavailable")
			}
			return nil
		}

		al.StartAsyncLoading(ctx)
		waitIdle(t, al)
		require.True(t, r.IsReady())

		fail.Store(true)
		al.Reload(ctx)
		waitIdle(t, al)
		assert.True(t, r.IsReady(), "the cache stays populated through a failed reload")
	})
}

// A bulk flags snapshot applied through the real pipeline reports the flags
// half of the load.
func TestBulkFlagsSnapshotReportsToReadiness(t *testing.T) {
	h, _, ctx := newRealCacheHandler(t)
	r := NewCacheReadiness(nil, NewSchematicLogger(), "v1", 0)
	h.SetCacheReadiness(r)
	r.MarkEntitiesLoaded(ctx)

	data, err := json.Marshal([]*rulesengine.Flag{createAsyncTestFlag(t)})
	require.NoError(t, err)
	require.NoError(t, h.HandleMessage(ctx, &schematicdatastreamws.DataStreamResp{
		EntityType:  string(schematicdatastreamws.EntityTypeFlags),
		MessageType: schematicdatastreamws.MessageTypeFull,
		Data:        data,
	}))
	require.Eventually(t, r.IsReady, 2*time.Second, 10*time.Millisecond)
}

// healthResponse is the subset of the /health and /ready body the tests read.
type healthResponse struct {
	Ready        bool                           `json:"ready"`
	Connected    bool                           `json:"connected"`
	Components   map[string]ComponentStatusType `json:"components"`
	CacheVersion string                         `json:"cache_version"`
}

func getHealth(t *testing.T, hs *HealthServer, path string) (int, healthResponse) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if path == "/ready" {
		hs.readinessHandler(rec, req)
	} else {
		hs.healthHandler(rec, req)
	}
	var body healthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	return rec.Code, body
}

// fakeSchematic serves the Schematic list endpoints the sync loader calls and a
// datastream websocket that answers a flags subscription with a snapshot. It
// can hold the list endpoints to keep a load in progress, and go down to stand
// in for Schematic being unreachable.
type fakeSchematic struct {
	t          *testing.T
	srv        *httptest.Server
	releaseAPI chan struct{}
	flags      json.RawMessage

	mu    sync.Mutex
	down  bool
	conns []*websocket.Conn
}

func newFakeSchematic(t *testing.T) *fakeSchematic {
	t.Helper()
	flags, err := json.Marshal([]*rulesengine.Flag{createAsyncTestFlag(t)})
	require.NoError(t, err)
	f := &fakeSchematic{t: t, releaseAPI: make(chan struct{}), flags: flags}

	upgrader := websocket.Upgrader{}
	mux := http.NewServeMux()
	list := func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-f.releaseAPI:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[],"params":{}}`))
	}
	mux.HandleFunc("/companies", list)
	mux.HandleFunc("/users", list)
	mux.HandleFunc("/datastream", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		down := f.down
		f.mu.Unlock()
		if down {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		f.mu.Lock()
		f.conns = append(f.conns, conn)
		f.mu.Unlock()
		for {
			var req schematicdatastreamws.DataStreamBaseReq
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			if req.Data.EntityType == schematicdatastreamws.EntityTypeFlags {
				_ = conn.WriteJSON(schematicdatastreamws.DataStreamResp{
					EntityType:  string(schematicdatastreamws.EntityTypeFlags),
					MessageType: schematicdatastreamws.MessageTypeFull,
					Data:        f.flags,
				})
			}
		}
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// goDown drops every datastream connection and refuses new ones.
func (f *fakeSchematic) goDown() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = true
	for _, c := range f.conns {
		_ = c.Close()
	}
}

// replicatorUnderTest wires a message handler, readiness tracker, datastream
// client and health server the way main does, against miniredis and a
// fakeSchematic.
type replicatorUnderTest struct {
	health    *HealthServer
	ds        *schematicdatastreamws.Client
	readiness *CacheReadiness
	async     *AsyncConnectionReadyHandler
	sync      *ConnectionReadyHandler
	stop      func() // closes the datastream client; safe to call more than once
}

func startReplicatorUnderTest(t *testing.T, fake *fakeSchematic, rc redis.Cmdable, useAsync bool, configureAsync func(*AsyncConnectionReadyHandler)) *replicatorUnderTest {
	t.Helper()
	logger := NewSchematicLogger()
	logger.SetLevel(LogLevelError)
	ttl := time.Duration(0)

	companies := NewRedisBatchCache[*rulesengine.Company](rc, ttl)
	users := NewRedisBatchCache[*rulesengine.User](rc, ttl)
	flags := NewRedisBatchCache[*rulesengine.Flag](rc, ttl)
	companyLookup := NewRedisBatchCache[string](rc, ttl)
	userLookup := NewRedisBatchCache[string](rc, ttl)

	readiness := NewCacheReadiness(rc, logger, rulesengine.VersionKey, ttl)
	readiness.Load(context.Background())

	handler := NewAsyncReplicatorMessageHandler(companies, users, flags, companyLookup, userLookup, logger, ttl, createTestAsyncConfig())
	handler.SetCacheReadiness(readiness)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = handler.Shutdown(ctx)
	})

	api := client.NewClient(option.WithAPIKey("test"), option.WithBaseURL(fake.srv.URL), option.WithMaxAttempts(1))
	rut := &replicatorUnderTest{readiness: readiness}
	opts := schematicdatastreamws.ClientOptions{
		URL:                  fake.srv.URL,
		ApiKey:               "test",
		MessageHandler:       handler.HandleMessage,
		Logger:               logger,
		MaxReconnectAttempts: 3,
		MinReconnectDelay:    10 * time.Millisecond,
		MaxReconnectDelay:    20 * time.Millisecond,
	}
	if useAsync {
		rut.async = NewAsyncConnectionReadyHandler(api, nil, companies, users, flags, companyLookup, userLookup, logger, ttl, DefaultAsyncLoaderConfig())
		rut.async.SetCacheReadiness(readiness)
		if configureAsync != nil {
			configureAsync(rut.async)
		}
		opts.ConnectionReadyHandler = rut.async.OnConnectionReady
	} else {
		rut.sync = NewConnectionReadyHandler(api, nil, companies, users, flags, companyLookup, userLookup, logger, ttl)
		rut.sync.SetCacheReadiness(readiness)
		opts.ConnectionReadyHandler = rut.sync.OnConnectionReady
	}

	ds, err := schematicdatastreamws.NewClient(opts)
	require.NoError(t, err)
	if rut.async != nil {
		rut.async.SetWebSocketClient(ds)
	} else {
		rut.sync.SetWebSocketClient(ds)
	}
	go func() {
		for range ds.GetErrorChannel() {
		}
	}()
	var closeOnce sync.Once
	rut.stop = func() { closeOnce.Do(ds.Close) }
	t.Cleanup(rut.stop)
	rut.ds = ds

	rut.health = NewHealthServer(0, nil, rc, logger)
	rut.health.SetCacheReadiness(readiness)
	rut.health.SetDatastreamClient(ds)
	ds.Start()
	return rut
}

func newTestRedis(t *testing.T) redis.Cmdable {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

// assertEndpoints checks /health and /ready against the expected cache
// readiness and connectivity. /health is always 200; /ready is 200 only when
// the cache is ready. Both report real connectivity.
func assertEndpoints(t *testing.T, hs *HealthServer, wantReady, wantConnected bool, msg string) {
	t.Helper()
	wantDatastream := ComponentStatusDisconnected
	if wantConnected {
		wantDatastream = ComponentStatusConnected
	}

	code, body := getHealth(t, hs, "/health")
	assert.Equal(t, http.StatusOK, code, "/health is liveness: "+msg)
	assert.Equal(t, wantReady, body.Ready, "/health ready: "+msg)
	assert.Equal(t, wantConnected, body.Connected, "/health connected: "+msg)
	assert.Equal(t, wantDatastream, body.Components["datastream"], "/health datastream: "+msg)
	assert.Equal(t, rulesengine.GetVersionKey(), body.CacheVersion)

	wantCode := http.StatusServiceUnavailable
	if wantReady {
		wantCode = http.StatusOK
	}
	code, body = getHealth(t, hs, "/ready")
	assert.Equal(t, wantCode, code, "/ready status: "+msg)
	assert.Equal(t, wantReady, body.Ready, "/ready ready: "+msg)
	assert.Equal(t, wantConnected, body.Connected, "/ready connected: "+msg)
	assert.Equal(t, wantDatastream, body.Components["datastream"], "/ready datastream: "+msg)
}

// On the async path the datastream client reports ready as soon as
// OnConnectionReady returns, while companies and users are still loading in the
// background. /health and /ready must wait for the load; once complete they
// must stay ready through a disconnect that never recovers, and a restarted
// replicator that can't reach Schematic must report the complete cache ready.
func TestHealthReadyAsyncColdStartDisconnectAndRestart(t *testing.T) {
	fake := newFakeSchematic(t)
	rc := newTestRedis(t)
	release := make(chan struct{})
	rut := startReplicatorUnderTest(t, fake, rc, true, func(h *AsyncConnectionReadyHandler) {
		h.asyncLoader.loadCompaniesFn = func(context.Context) error { <-release; return nil }
		h.asyncLoader.loadUsersFn = func(context.Context) error { return nil }
	})

	require.Eventually(t, rut.ds.IsReady, 2*time.Second, 10*time.Millisecond, "datastream client should be ready once subscribed")
	// Give the flags snapshot time to land so only the company load is pending.
	time.Sleep(100 * time.Millisecond)
	assertEndpoints(t, rut.health, false, true, "companies are still loading, so the cache is not servable")

	close(release)
	require.Eventually(t, rut.readiness.IsReady, 2*time.Second, 10*time.Millisecond)
	assertEndpoints(t, rut.health, true, true, "load complete")

	// Schematic becomes unreachable for good.
	fake.goDown()
	require.Eventually(t, func() bool { return !rut.ds.IsConnected() }, 2*time.Second, 10*time.Millisecond)
	assertEndpoints(t, rut.health, true, false, "a disconnect leaves the cache populated and servable")

	// Restart against the same Redis with Schematic still down.
	rut.stop()
	restarted := startReplicatorUnderTest(t, fake, rc, true, nil)
	assertEndpoints(t, restarted.health, true, false, "the persisted marker vouches for the cache across a restart")
}

// On the sync path the endpoints become ready only after the company/user load
// and the flags snapshot, and stay ready after a disconnect.
func TestHealthReadySyncColdStartAndDisconnect(t *testing.T) {
	fake := newFakeSchematic(t)
	rut := startReplicatorUnderTest(t, fake, newTestRedis(t), false, nil)

	require.Eventually(t, rut.ds.IsConnected, 2*time.Second, 10*time.Millisecond)
	assertEndpoints(t, rut.health, false, true, "the sync load is still waiting on the API")

	close(fake.releaseAPI)
	require.Eventually(t, rut.readiness.IsReady, 2*time.Second, 10*time.Millisecond)
	assertEndpoints(t, rut.health, true, true, "load complete")

	fake.goDown()
	require.Eventually(t, func() bool { return !rut.ds.IsConnected() }, 2*time.Second, 10*time.Millisecond)
	assertEndpoints(t, rut.health, true, false, "disconnected with a complete cache")
}

// Before the datastream client exists (waiting on the writer lock during a
// rolling deploy), both endpoints report the persisted readiness, so the
// orchestrator can stop the old instance and the lease hands over.
func TestHealthReadyBeforeDatastreamClient(t *testing.T) {
	ctx := context.Background()

	for _, tt := range []struct {
		name   string
		marker string
		ready  bool
	}{
		{"warm cache", rulesengine.VersionKey, true},
		{"cold cache", "", false},
		{"marker from another cache version", "other-version", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r, _, client := newTestReadiness(t, rulesengine.VersionKey)
			if tt.marker != "" {
				require.NoError(t, client.Set(ctx, defaultLoadCompleteKey, tt.marker, 0).Err())
			}
			r.Load(ctx)

			hs := NewHealthServer(0, nil, client, NewSchematicLogger())
			hs.SetCacheReadiness(r)

			code, body := getHealth(t, hs, "/health")
			assert.Equal(t, http.StatusOK, code)
			assert.Equal(t, tt.ready, body.Ready)
			assert.False(t, body.Connected)
			assert.Equal(t, ComponentStatusUnknown, body.Components["datastream"])

			wantCode := http.StatusServiceUnavailable
			if tt.ready {
				wantCode = http.StatusOK
			}
			code, body = getHealth(t, hs, "/ready")
			assert.Equal(t, wantCode, code)
			assert.Equal(t, tt.ready, body.Ready)
			assert.False(t, body.Connected)
			assert.Equal(t, ComponentStatusUnknown, body.Components["datastream"])
		})
	}
}
