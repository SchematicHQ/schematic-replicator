package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplayStatsSnapshot(t *testing.T) {
	// Nil-safe: a nil ReplayStats snapshots to zeroes (replay disabled / unwired).
	var nilStats *ReplayStats
	assert.Equal(t, ReplayStatsSnapshot{}, nilStats.Snapshot())

	s := &ReplayStats{}
	assert.Equal(t, int64(0), s.Snapshot().Reconnects)

	// One connection is the initial connect → zero reconnects.
	s.IncConnections()
	assert.Equal(t, int64(0), s.Snapshot().Reconnects)

	s.IncConnections()
	s.IncConnections()
	s.IncReplaysEngaged()
	s.IncFullReloads()
	s.IncOverflowEscalations()

	snap := s.Snapshot()
	assert.Equal(t, int64(3), snap.Connections)
	assert.Equal(t, int64(2), snap.Reconnects)
	assert.Equal(t, int64(1), snap.ReplaysEngaged)
	assert.Equal(t, int64(1), snap.FullReloads)
	assert.Equal(t, int64(1), snap.OverflowEscalations)
}

func TestHealthServerDebugEndpoint(t *testing.T) {
	hs := NewHealthServer(0, nil, nil, NewSchematicLogger())

	cursor := NewReplayCursor(nil, NewSchematicLogger(), "")
	cursor.Track("500-0")
	cursor.Complete("500-0")
	stats := &ReplayStats{}
	stats.IncConnections()
	stats.IncConnections() // 1 reconnect
	stats.IncReplaysEngaged()
	hs.SetReplayDebug(cursor, stats, nil) // nil handler → message counters zero

	rec := httptest.NewRecorder()
	hs.debugHandler(rec, httptest.NewRequest(http.MethodGet, "/debug", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Replay struct {
			Enabled        bool   `json:"enabled"`
			Cursor         string `json:"cursor"`
			Reconnects     int64  `json:"reconnects"`
			ReplaysEngaged int64  `json:"replays_engaged"`
		} `json:"replay"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.True(t, resp.Replay.Enabled)
	assert.Equal(t, "500-0", resp.Replay.Cursor)
	assert.Equal(t, int64(1), resp.Replay.Reconnects)
	assert.Equal(t, int64(1), resp.Replay.ReplaysEngaged)
}

// With replay disabled, the cursor is nil; /debug must report it as disabled.
func TestHealthServerDebugReplayDisabled(t *testing.T) {
	hs := NewHealthServer(0, nil, nil, NewSchematicLogger())
	hs.SetReplayDebug(nil, &ReplayStats{}, nil)

	rec := httptest.NewRecorder()
	hs.debugHandler(rec, httptest.NewRequest(http.MethodGet, "/debug", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Replay struct {
			Enabled bool   `json:"enabled"`
			Cursor  string `json:"cursor"`
		} `json:"replay"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.False(t, resp.Replay.Enabled)
	assert.Equal(t, "", resp.Replay.Cursor)
}
