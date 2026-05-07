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

// ClientVersion returns the module version of the replicator recorded in the
// build. For tagged releases this returns the tag (e.g. v1.2.3). For untagged
// or replace-directive builds it returns "(devel)"; if build info is
// unavailable, "unknown".
func ClientVersion() string {
	clientVersionOnce.Do(func() {
		clientVersion = resolveClientVersion()
	})
	return clientVersion
}

func resolveClientVersion() string {
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
