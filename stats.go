package main

import "sync/atomic"

// ReplayStats holds counters describing replay/reconnect behavior. They power
// the /debug endpoint and let integration tests assert that replay (not a full
// reload) actually engaged on reconnect. All methods are nil-safe and safe for
// concurrent use, so call sites need no guards when replay is disabled.
type ReplayStats struct {
	connections         int64 // total (re)connections established; reconnects = connections - 1
	replaysEngaged      int64 // subscribes that sent a ReplayFrom token
	fullReloads         int64 // full bulk reloads triggered (reload signal or cursor overflow)
	overflowEscalations int64 // cursor pending-set overflows
}

func (s *ReplayStats) IncConnections() {
	if s != nil {
		atomic.AddInt64(&s.connections, 1)
	}
}

func (s *ReplayStats) IncReplaysEngaged() {
	if s != nil {
		atomic.AddInt64(&s.replaysEngaged, 1)
	}
}

func (s *ReplayStats) IncFullReloads() {
	if s != nil {
		atomic.AddInt64(&s.fullReloads, 1)
	}
}

func (s *ReplayStats) IncOverflowEscalations() {
	if s != nil {
		atomic.AddInt64(&s.overflowEscalations, 1)
	}
}

// ReplayStatsSnapshot is a point-in-time, JSON-serializable view of ReplayStats.
type ReplayStatsSnapshot struct {
	Connections         int64 `json:"connections"`
	Reconnects          int64 `json:"reconnects"`
	ReplaysEngaged      int64 `json:"replays_engaged"`
	FullReloads         int64 `json:"full_reloads"`
	OverflowEscalations int64 `json:"overflow_escalations"`
}

func (s *ReplayStats) Snapshot() ReplayStatsSnapshot {
	if s == nil {
		return ReplayStatsSnapshot{}
	}
	connections := atomic.LoadInt64(&s.connections)
	reconnects := connections - 1
	if reconnects < 0 {
		reconnects = 0
	}
	return ReplayStatsSnapshot{
		Connections:         connections,
		Reconnects:          reconnects,
		ReplaysEngaged:      atomic.LoadInt64(&s.replaysEngaged),
		FullReloads:         atomic.LoadInt64(&s.fullReloads),
		OverflowEscalations: atomic.LoadInt64(&s.overflowEscalations),
	}
}
