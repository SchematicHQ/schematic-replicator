package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthPortFromEnv(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		expected int
	}{
		{"unset falls back to default", "", defaultHealthPort},
		{"valid port is used", "9090", 9090},
		{"unparseable falls back to default", "not-a-port", defaultHealthPort},
		{"zero falls back to default", "0", defaultHealthPort},
		{"negative falls back to default", "-1", defaultHealthPort},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue != "" {
				t.Setenv("HEALTH_PORT", tt.envValue)
			}
			assert.Equal(t, tt.expected, healthPortFromEnv())
		})
	}
}

// startProbeTarget stands up a server on 127.0.0.1 that answers with the given
// status, and points HEALTH_PORT at it so runHealthCheck finds it.
func startProbeTarget(t *testing.T, status int) {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)

	parsed, err := url.Parse(srv.URL)
	require.NoError(t, err)
	t.Setenv("HEALTH_PORT", parsed.Port())
}

func TestRunHealthCheck(t *testing.T) {
	t.Run("healthy endpoint exits zero", func(t *testing.T) {
		startProbeTarget(t, http.StatusOK)
		assert.Equal(t, 0, runHealthCheck("/health"))
	})

	t.Run("not-ready endpoint exits non-zero", func(t *testing.T) {
		// /ready returns 503 until the datastream has loaded initial data.
		startProbeTarget(t, http.StatusServiceUnavailable)
		assert.Equal(t, 1, runHealthCheck("/ready"))
	})

	t.Run("unreachable server exits non-zero", func(t *testing.T) {
		// Bind and immediately release a port so nothing is listening on it.
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		parsed, err := url.Parse(srv.URL)
		require.NoError(t, err)
		srv.Close()

		t.Setenv("HEALTH_PORT", parsed.Port())

		assert.Equal(t, 1, runHealthCheck("/health"))
	})
}
