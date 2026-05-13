package main

import (
	"net/http"
	"runtime/debug"
	"sync"
)

const (
	clientName       = "schematic-datastream-replicator"
	clientModulePath = "github.com/schematichq/schematic-datastream-replicator"
)

var (
	clientVersionOnce sync.Once
	clientVersion     string
)

// ClientVersion returns the replicator version reported on the DataStream
// handshake. It prefers the ldflag-injected `main.version` used by the
// Dockerfile build, then falls back to module build info (a tag for
// `go install …@v1.2.3` builds, "(devel)" for local `go build .`), and
// finally "unknown".
func ClientVersion() string {
	clientVersionOnce.Do(func() {
		clientVersion = resolveClientVersion()
	})
	return clientVersion
}

func resolveClientVersion() string {
	if version != "" {
		return version
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	if info.Main.Path == clientModulePath && info.Main.Version != "" {
		return info.Main.Version
	}
	for _, dep := range info.Deps {
		if dep == nil || dep.Path != clientModulePath {
			continue
		}
		if dep.Replace != nil && dep.Replace.Version != "" {
			return dep.Replace.Version
		}
		if dep.Version != "" {
			return dep.Version
		}
	}
	return "unknown"
}

// datastreamHandshakeHeaders returns the HTTP headers attached to the
// outbound DataStream WebSocket handshake so the backend can distinguish
// replicator connections from direct-SDK connections and correlate them to a
// release.
func datastreamHandshakeHeaders() http.Header {
	h := http.Header{}
	h.Set("X-Schematic-Datastream-Mode", "replicator")
	h.Set("X-Schematic-Client", clientName)
	h.Set("X-Schematic-Client-Version", ClientVersion())
	return h
}
