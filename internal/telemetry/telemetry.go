// Package telemetry wires OpenTelemetry tracing for the audit services.
// HTTP handlers instrumented with the OTel API fall back to the no-op
// tracer when Init is never called, so tracing stays optional at runtime
// (AUDIT_OTLP_ENDPOINT empty disables export).
package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Tracer owns the OTLP exporter lifecycle. It is nil when tracing is
// disabled.
type Tracer struct {
	shutdown func(context.Context) error
}

// Init starts an OTLP/HTTP trace exporter. endpoint must be an HTTP(S) URL
// such as http://jaeger:4318. An empty endpoint returns a nil Tracer (no
// export) without error. The returned Tracer must be shut down on process
// exit to flush buffered spans.
func Init(ctx context.Context, endpoint, serviceName string) (*Tracer, error) {
	if endpoint == "" {
		return nil, nil
	}
	if serviceName == "" {
		serviceName = "audit-governance"
	}
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(endpoint),
		// WithEndpointURL keeps only the URL host; the OTLP path must be
		// set explicitly or requests go to the root path.
		otlptracehttp.WithURLPath("v1/traces"),
	)
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter: %w", err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(5*time.Second)),
		sdktrace.WithResource(resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(serviceName))),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return &Tracer{shutdown: provider.Shutdown}, nil
}

// Shutdown flushes and stops the exporter. Safe on a nil receiver.
func (t *Tracer) Shutdown(ctx context.Context) error {
	if t == nil {
		return nil
	}
	return t.shutdown(ctx)
}
