// Package telemetry wires OpenTelemetry tracing for the audit services.
// HTTP handlers instrumented with the OTel API fall back to the no-op
// tracer when Init is never called, so tracing stays optional at runtime
// (AUDIT_OTLP_ENDPOINT empty disables export).
package telemetry

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

const otlpTracesPath = "/v1/traces"

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
	traceEndpoint, err := traceEndpointURL(endpoint)
	if err != nil {
		return nil, err
	}
	if serviceName == "" {
		serviceName = "audit-governance"
	}
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(traceEndpoint))
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

// traceEndpointURL validates the collector URL and appends the OTLP traces
// signal path to an optional gateway prefix. A caller may also provide the
// complete signal URL; in that case the operation is idempotent.
func traceEndpointURL(endpoint string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return "", fmt.Errorf("parse OTLP endpoint: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("OTLP endpoint scheme must be http or https")
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("OTLP endpoint host is required")
	}
	endpointPath := strings.TrimRight(parsed.Path, "/")
	if endpointPath == "" {
		endpointPath = otlpTracesPath
	} else if !strings.HasSuffix(endpointPath, otlpTracesPath) {
		endpointPath += otlpTracesPath
	}
	parsed.Path = endpointPath
	parsed.RawPath = ""
	return parsed.String(), nil
}

// Shutdown flushes and stops the exporter. Safe on a nil receiver.
func (t *Tracer) Shutdown(ctx context.Context) error {
	if t == nil {
		return nil
	}
	return t.shutdown(ctx)
}
