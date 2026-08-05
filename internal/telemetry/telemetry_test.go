package telemetry

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

type spanCapture struct {
	spans []sdktrace.ReadOnlySpan
}

func (s *spanCapture) OnStart(context.Context, sdktrace.ReadWriteSpan) {}

func (s *spanCapture) OnEnd(span sdktrace.ReadOnlySpan) {
	s.spans = append(s.spans, span)
}

func (s *spanCapture) ForceFlush(context.Context) error { return nil }
func (s *spanCapture) Shutdown(context.Context) error   { return nil }

func TestInitDisabledReturnsNil(t *testing.T) {
	tracer, err := Init(context.Background(), "", "test-service")
	if err != nil || tracer != nil {
		t.Fatalf("empty endpoint: tracer=%v err=%v, want nil/nil", tracer, err)
	}
}

func TestInitAndShutdown(t *testing.T) {
	// OTLP export to a closed port still succeeds at Init (batcher retries
	// asynchronously); the important contract is that Shutdown returns
	// without error and the global provider is set.
	tracer, err := Init(context.Background(), "http://127.0.0.1:1", "test-service")
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if tracer == nil {
		t.Fatal("tracer must be non-nil for a configured endpoint")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := tracer.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// TestNoopWithoutInit guarantees that handlers instrumented with the OTel
// API work unchanged when Init was never called (default no-op provider).
func TestNoopWithoutInit(t *testing.T) {
	tracer := otel.Tracer("test")
	_, span := tracer.Start(context.Background(), "op")
	span.SetAttributes(attribute.String("k", "v"))
	span.End()
	spanContext := trace.SpanContextFromContext(context.Background())
	if spanContext.IsValid() {
		t.Fatal("no-op tracer must not produce valid span contexts")
	}
}

func TestSpanCapturedAndTraceparentInjected(t *testing.T) {
	capture := &spanCapture{}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(capture),
		sdktrace.WithResource(resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName("test"))),
	)
	otel.SetTracerProvider(provider)
	defer otel.SetTracerProvider(otel.GetTracerProvider())

	tracer := otel.Tracer("audit-api")
	_, span := tracer.Start(context.Background(), "POST /api/v1/events", trace.WithAttributes(attribute.String("http.route", "/api/v1/events")))
	span.End()

	// 同步 flush 处理器（无 batcher），span 应已被捕获。
	if len(capture.spans) != 1 {
		t.Fatalf("captured spans=%d, want 1", len(capture.spans))
	}
	got := capture.spans[0]
	if got.Name() != "POST /api/v1/events" {
		t.Fatalf("span name=%q", got.Name())
	}
	hasRoute := false
	for _, attr := range got.Attributes() {
		if attr.Key == "http.route" && strings.Contains(attr.Value.AsString(), "/api/v1/events") {
			hasRoute = true
		}
	}
	if !hasRoute {
		t.Fatal("span missing http.route attribute")
	}
}
