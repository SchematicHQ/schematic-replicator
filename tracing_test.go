package main

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Tracing must stay off unless an endpoint is configured. This is the path
// every self-hosted replicator takes by default, and standing up an exporter
// there would mean a retry loop against a port nothing is bound to.
func TestInitTracingWithoutEndpointIsNoOp(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	shutdown, err := initTracing(context.Background(), NewSchematicLogger())
	if err != nil {
		t.Fatalf("initTracing: %v", err)
	}
	if shutdown == nil {
		t.Fatal("shutdown must be callable even when tracing is off")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestTracingEnabledFollowsEitherEndpoint(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	if tracingEnabled() {
		t.Error("no endpoint set, want disabled")
	}

	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://localhost:4318/v1/traces")
	if !tracingEnabled() {
		t.Error("traces-specific endpoint set, want enabled")
	}
}

// An unrecognised protocol has to fail loudly. Silently exporting over the
// wrong transport looks identical to a collector that is down.
func TestNewTraceExporterRejectsUnknownProtocol(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "carrier-pigeon")
	if _, err := newTraceExporter(context.Background()); err == nil {
		t.Fatal("want an error for an unsupported protocol")
	}
}

func TestBatchSpansCarryEntityAndSize(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	ctx, batch := startBatchSpan(context.Background(), "company", 12)
	_, cache := startCacheSpan(ctx, "cache", "company", 12)
	cache.End()
	batch.End()

	ended := recorder.Ended()
	if len(ended) != 2 {
		t.Fatalf("got %d spans, want 2", len(ended))
	}

	// The cache span ends first, so it is the one that must name the batch as
	// its parent — that nesting is the whole reason startBatchSpan returns a
	// context.
	if ended[0].Parent().SpanID() != ended[1].SpanContext().SpanID() {
		t.Error("cache span is not a child of the batch span")
	}

	for _, s := range ended {
		got := map[string]string{}
		for _, a := range s.Attributes() {
			got[string(a.Key)] = a.Value.Emit()
		}
		if got["replicator.entity"] != "company" {
			t.Errorf("%s: replicator.entity = %q", s.Name(), got["replicator.entity"])
		}
		if got["replicator.batch.size"] != "12" {
			t.Errorf("%s: replicator.batch.size = %q", s.Name(), got["replicator.batch.size"])
		}
	}
}
