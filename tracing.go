package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// tracerName identifies this service's own instrumentation scope. It appears on
// every span the replicator emits, which is how you tell them apart from spans
// produced by a library.
const tracerName = "github.com/schematichq/schematic-datastream-replicator"

// defaultServiceName is used when OTEL_SERVICE_NAME is not set. Anything
// running more than one replicator should override it.
const defaultServiceName = "schematic-replicator"

// tracingShutdownTimeout bounds the final flush at exit. Spans still queued
// when it expires are dropped rather than delaying shutdown further.
const tracingShutdownTimeout = 5 * time.Second

// Tracer returns the replicator's tracer. Before initTracing runs — or when
// tracing is switched off — this is the SDK's no-op tracer, so callers never
// need to check whether tracing is on.
func Tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

// initTracing configures OpenTelemetry tracing and returns a shutdown function.
//
// Tracing is off unless OTEL_EXPORTER_OTLP_ENDPOINT (or its traces-specific
// form) is set, so the default deployment exports nothing and pays nothing.
// Everything is driven by the standard OTEL_* environment variables rather than
// by flags of our own: this is a vendor-neutral OTLP exporter, and it will talk
// to any collector that speaks the protocol. See README.md for the variables
// that matter.
//
// The returned shutdown function is always safe to call.
func initTracing(ctx context.Context, logger *SchematicLogger) (func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }

	if !tracingEnabled() {
		logger.Debug(ctx, "OpenTelemetry tracing disabled (no OTLP endpoint configured)")
		return noop, nil
	}

	exporter, err := newTraceExporter(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating OTLP trace exporter: %w", err)
	}

	res, err := newTraceResource(ctx)
	if err != nil {
		// A schema-URL conflict still yields a usable resource. Losing the
		// schema URL is not worth losing tracing over, so report and continue.
		logger.Warn(ctx, fmt.Sprintf("OpenTelemetry resource: %v", err))
		if res == nil {
			return nil, err
		}
	}

	// No explicit sampler: the SDK default is ParentBased(AlwaysSample), which
	// keeps sampling policy in the collector where an operator can change it
	// without redeploying the replicator.
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	// The SDK writes handler errors to stderr through the standard library
	// logger by default, which lands outside this service's structured logs. A
	// failed export is the likeliest thing to go wrong, so route it in.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		logger.Warn(context.Background(), fmt.Sprintf("OpenTelemetry SDK: %v", err))
	}))

	logger.Info(ctx, "OpenTelemetry tracing enabled")

	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, tracingShutdownTimeout)
		defer cancel()
		return provider.Shutdown(ctx)
	}, nil
}

// newTraceExporter builds the OTLP exporter for the configured protocol.
//
// OTEL_EXPORTER_OTLP_PROTOCOL selects it, per the OTLP specification:
// "grpc", or "http/protobuf" (the default). Both read their endpoint, headers,
// and TLS settings from the standard environment variables, so nothing else
// here has to know which was chosen.
func newTraceExporter(ctx context.Context) (*otlptrace.Exporter, error) {
	protocol := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
	if protocol == "" {
		protocol = os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")
	}

	switch strings.TrimSpace(strings.ToLower(protocol)) {
	case "grpc":
		return otlptracegrpc.New(ctx)
	case "", "http/protobuf", "http/json":
		// http/json is not implemented by the Go SDK; treating it as protobuf
		// beats failing to start, and the collector accepts either.
		return otlptracehttp.New(ctx)
	default:
		return nil, fmt.Errorf("unsupported OTEL_EXPORTER_OTLP_PROTOCOL %q (want \"grpc\" or \"http/protobuf\")", protocol)
	}
}

// newTraceResource describes this process to the collector. resource.WithFromEnv
// picks up OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES, so an operator can
// add their own deployment tags without a code change; the service name below
// is only a fallback for when they have not set one.
func newTraceResource(ctx context.Context) (*resource.Resource, error) {
	return resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithProcessRuntimeDescription(),
		resource.WithAttributes(
			semconv.ServiceName(defaultServiceName),
			semconv.ServiceVersion(valueOrUnknown(version)),
		),
	)
}

// tracingEnabled reports whether an OTLP endpoint has been configured. Checking
// the endpoint rather than a switch of our own means there is one thing to set
// to turn tracing on, and it is the variable every OTLP tool already uses.
func tracingEnabled() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
}

// batchAttributes is the attribute set shared by the batch-processing spans.
func batchAttributes(entity string, count int) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("replicator.entity", entity),
		attribute.Int("replicator.batch.size", count),
	}
}

// startBatchSpan opens the span covering one batch of replicated messages. It
// is the root of the replicator's traces: the datastream delivers messages
// continuously, so there is no enclosing request to hang them off, and a span
// per message would be both enormous and useless — the batch is the unit that
// succeeds or fails.
func startBatchSpan(ctx context.Context, entity string, count int) (context.Context, trace.Span) {
	return Tracer().Start(ctx, "replicate "+entity,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(batchAttributes(entity, count)...),
	)
}

// startCacheSpan opens the span covering one Redis batch write. op is the
// logical operation ("cache" or "delete"), following the database semantic
// conventions closely enough to be recognisable without pretending to be a
// single statement — each of these is a pipeline of many commands.
func startCacheSpan(ctx context.Context, op, entity string, count int) (context.Context, trace.Span) {
	return Tracer().Start(ctx, op+" "+entity,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			append(batchAttributes(entity, count),
				semconv.DBSystemRedis,
				attribute.String("db.operation.name", op),
			)...,
		),
	)
}

// recordSpanError marks a span failed. Every call site here logs the same error
// as well; the span carries it so a trace explains its own red bar without a
// log pivot.
func recordSpanError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
