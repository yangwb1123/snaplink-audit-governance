package httpapi

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/service"
)

type Server struct {
	Service      *service.Service
	Auth         auth.Authenticator
	Logger       *log.Logger
	requestCount atomic.Uint64
	errorCount   atomic.Uint64
	ingestCount  atomic.Uint64
	queryCount   atomic.Uint64

	// Domain-level observability counters required by the architecture plan:
	// idempotency hits, content conflicts, quota enforcement and integrity
	// verification outcomes.
	ingestDuplicateCount  atomic.Uint64
	ingestConflictCount   atomic.Uint64
	ingestQuotaCount      atomic.Uint64
	integrityValidCount   atomic.Uint64
	integrityInvalidCount atomic.Uint64

	// Request latency histogram (seconds). Bucket boundaries are fixed at
	// construction; counts are cumulative per bucket.
	latencyBuckets  []float64
	latencyCounts   []atomic.Uint64
	latencySumNanos atomic.Uint64
	latencyTotal    atomic.Uint64
}

type contextKey string

const (
	claimsKey    contextKey = "claims"
	requestIDKey contextKey = "request_id"
)

func NewServer(svc *service.Service, authenticator auth.Authenticator, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	server := &Server{Service: svc, Auth: authenticator, Logger: logger, latencyBuckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5}}
	server.latencyCounts = make([]atomic.Uint64, len(server.latencyBuckets))
	return server
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	// Every registration goes through spanWrap: it runs inside ServeMux after
	// pattern matching, so r.Pattern holds the matched route. A route
	// registered without spanWrap would be silently untraced (and fail the
	// route-coverage acceptance test), so keep the wrapper on every line.
	mux.HandleFunc("GET /healthz", s.spanWrap(s.healthz))
	mux.HandleFunc("GET /readyz", s.spanWrap(s.readyz))
	mux.HandleFunc("GET /metrics", s.spanWrap(s.metrics))
	mux.HandleFunc("POST /api/v1/events", s.spanWrap(s.postEvent))
	mux.HandleFunc("POST /api/v1/events:batch", s.spanWrap(s.postBatch))
	mux.HandleFunc("GET /api/v1/events/{eventID}", s.spanWrap(s.getEvent))
	mux.HandleFunc("GET /api/v1/events/{eventID}/receipt", s.spanWrap(s.getReceipt))
	mux.HandleFunc("GET /api/v1/events", s.spanWrap(s.queryEvents))
	mux.HandleFunc("GET /api/v1/compat/snaplink/audit/events", s.spanWrap(s.querySnaplinkAuditEvents))
	mux.HandleFunc("GET /api/v1/compat/snaplink/audit/events/{eventID}", s.spanWrap(s.getEvent))
	mux.HandleFunc("GET /api/v1/compat/snaplink/audit/facets", s.spanWrap(s.querySnaplinkAuditFacets))
	mux.HandleFunc("GET /api/v1/operations/{operationID}", s.spanWrap(s.getOperation))
	mux.HandleFunc("GET /api/v1/operations/{operationID}/timeline", s.spanWrap(s.getOperationTimeline))
	mux.HandleFunc("GET /api/v1/operations/{operationID}/replay", s.spanWrap(s.replayOperation))
	mux.HandleFunc("GET /api/v1/aggregates/{aggregateType}/{aggregateID}/timeline", s.spanWrap(s.getAggregateTimeline))
	mux.HandleFunc("POST /api/v1/exports", s.spanWrap(s.createExport))
	mux.HandleFunc("GET /api/v1/exports/{jobID}", s.spanWrap(s.getExport))
	mux.HandleFunc("GET /api/v1/exports/{jobID}/download", s.spanWrap(s.downloadExport))
	mux.HandleFunc("POST /api/v1/integrity/verify", s.spanWrap(s.verifyIntegrity))
	mux.HandleFunc("POST /api/v1/legal-holds", s.spanWrap(s.createLegalHold))
	mux.HandleFunc("GET /api/v1/legal-holds", s.spanWrap(s.listLegalHolds))
	mux.HandleFunc("POST /api/v1/legal-holds/{holdID}/release", s.spanWrap(s.releaseLegalHold))
	mux.HandleFunc("POST /api/v1/restores/preview", s.spanWrap(s.previewRestore))
	mux.HandleFunc("POST /api/v1/restores", s.spanWrap(s.createRestore))
	mux.HandleFunc("GET /api/v1/restores/{runID}", s.spanWrap(s.getRestore))
	mux.HandleFunc("POST /api/v1/restores/{runID}/approve", s.spanWrap(s.approveRestore))
	mux.HandleFunc("POST /api/v1/restores/{runID}/reject", s.spanWrap(s.rejectRestore))
	mux.HandleFunc("POST /api/v1/tenants", s.spanWrap(s.createTenant))
	mux.HandleFunc("GET /api/v1/tenants", s.spanWrap(s.listTenants))
	mux.HandleFunc("POST /api/v1/sources", s.spanWrap(s.createSource))
	mux.HandleFunc("GET /api/v1/sources", s.spanWrap(s.listSources))
	mux.HandleFunc("PUT /api/v1/sources/{sourceID}", s.spanWrap(s.updateSource))
	mux.HandleFunc("POST /api/v1/schemas", s.spanWrap(s.createSchema))
	mux.HandleFunc("GET /api/v1/schemas", s.spanWrap(s.listSchemas))
	mux.HandleFunc("PUT /api/v1/policies/retention", s.spanWrap(s.setRetention))
	mux.HandleFunc("GET /api/v1/policies/retention", s.spanWrap(s.getRetention))
	mux.HandleFunc("POST /api/v1/retention/evaluate", s.spanWrap(s.evaluateRetention))
	mux.HandleFunc("GET /api/v1/admin/actions", s.spanWrap(s.listAdminActions))
	return s.middleware(mux)
}

// middleware runs before ServeMux matching, so r.Pattern is not yet set and
// no span can be created here. It handles request identity and metrics only;
// the server span is created per-route in spanWrap, after pattern matching.
func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		requestID := truncateRequestID(r.Header.Get("X-Request-ID"))
		if requestID == "" {
			requestID = newRequestID()
		}
		ctx := context.WithValue(r.Context(), requestIDKey, requestID)
		// X-Trace-ID fallback for requests that match no route (no span is
		// created for them); spanWrap overwrites it for matched routes.
		w.Header().Set("X-Request-ID", requestID)
		w.Header().Set("X-Trace-ID", requestID)
		defer func() {
			s.requestCount.Add(1)
			s.recordLatency(time.Since(started))
		}()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// spanWrap is registered per-route inside ServeMux, so r.Pattern is populated
// with the matched pattern before it runs. It creates exactly one server span
// per matched request, injects W3C trace context into the response, and owns
// panic recovery so RecordError lands on a live span: recovery and span.End
// live in the same deferred function, preserving today's LIFO ordering (an
// outer-frame recovery would run after span.End during unwinding and would
// silently drop the panic event).
func (s *Server) spanWrap(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		propagator := otel.GetTextMapPropagator()
		ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		route := routeFromPattern(r.Pattern)
		spanName := r.Method + " " + route
		requestID, _ := r.Context().Value(requestIDKey).(string)
		ctx, span := otel.Tracer("audit-api").Start(ctx, spanName,
			trace.WithAttributes(
				attribute.String("http.method", r.Method),
				attribute.String("http.route", route),
				attribute.String("request_id", requestID),
			))
		propagator.Inject(ctx, propagation.HeaderCarrier(w.Header()))
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			w.Header().Set("X-Trace-ID", sc.TraceID().String())
		}
		r = r.WithContext(ctx)
		trackedWriter := &statusResponseWriter{ResponseWriter: w}
		defer func() {
			panicked := false
			if recovered := recover(); recovered != nil {
				panicked = true
				// writeError counts the 500: the panic is one error response.
				span.RecordError(fmt.Errorf("panic: %v", recovered))
				s.Logger.Printf("request_id=%s panic=%v", requestID, recovered)
				s.writeError(trackedWriter, r, http.StatusInternalServerError, fmt.Errorf("internal server error"))
			}
			statusCode := trackedWriter.StatusCode()
			span.SetAttributes(attribute.Int("http.status_code", statusCode))
			if panicked || (statusCode >= http.StatusInternalServerError && statusCode <= 599) {
				// Deliberately omit a description: error text can contain internal
				// diagnostics, while the panic exception above is a compatibility
				// event whose existing contents must be retained.
				span.SetStatus(codes.Error, "")
			}
			span.End()
		}()
		handler(trackedWriter, r)
	}
}

// routeFromPattern strips the leading method token from a ServeMux pattern
// ("GET /api/v1/events/{eventID}" -> "/api/v1/events/{eventID}"). The cut
// uses the pattern's own method token, not r.Method, because "GET" patterns
// also match HEAD requests and r.Pattern retains the registered "GET …" form.
// The fallback keeps naming bounded even for a non-mux caller.
func routeFromPattern(pattern string) string {
	if _, route, ok := strings.Cut(pattern, " "); ok {
		return route
	}
	return "/unmatched"
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.Service == nil || s.Service.Store == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	// Readiness reflects the control-plane store dependency first: a
	// PostgreSQL snapshot backend that cannot be reached must surface here
	// (503) instead of failing every read-modify-write later. File-backed
	// stores are always ready.
	if err := s.Service.Store.Ready(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "reason": "store_unavailable"})
		return
	}
	// Readiness reflects the configured archive dependency: an unavailable
	// WORM destination must surface here instead of silently degrading the
	// archived status of new events. An unconfigured store (nil or an
	// empty-dir local store) is skipped; configured stores probe their
	// destination.
	if archive.Configured(s.Service.Config.Archive) {
		if err := s.Service.Config.Archive.Ready(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "reason": "archive_unavailable"})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) metrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "audit_http_requests_total %d\n", s.requestCount.Load())
	fmt.Fprintf(w, "audit_http_errors_total %d\n", s.errorCount.Load())
	fmt.Fprintf(w, "audit_ingest_requests_total %d\n", s.ingestCount.Load())
	fmt.Fprintf(w, "audit_ingest_duplicates_total %d\n", s.ingestDuplicateCount.Load())
	fmt.Fprintf(w, "audit_ingest_conflicts_total %d\n", s.ingestConflictCount.Load())
	fmt.Fprintf(w, "audit_ingest_quota_exceeded_total %d\n", s.ingestQuotaCount.Load())
	// Unlabelled and monotonic: this counter is evidence that a tenant commit
	// died between its durable cold append and its control-plane write, and an
	// idempotent retry re-created the missing publication intent. A tenant,
	// event or route label would make the cardinality unbounded.
	if s.Service != nil {
		fmt.Fprintf(w, "audit_ledgered_intent_repaired_total %d\n", s.Service.LedgeredIntentRepairs())
		// Unlabelled and monotonic for the same cardinality reason. It counts
		// post-commit publications the ingest request path declined because
		// the background outbox flusher already owned that one pending event:
		// with a consistent durable outbox the request path never contends,
		// so a non-zero value means the one-publisher-per-pending-event
		// invariant was breached and at-least-once delivery absorbed it. An
		// atomic load, so a scrape never waits on the publication gate or the
		// store.
		fmt.Fprintf(w, "audit_ledgered_publish_deferred_total %d\n", s.Service.LedgeredPublishDeferrals())
	}
	fmt.Fprintf(w, "audit_query_requests_total %d\n", s.queryCount.Load())
	// Store pool + deadline observability. Unlabelled by construction: a
	// tenant, event, host, DSN or route label would make the cardinality
	// unbounded. max_open is reported verbatim, so 0 means "no operator
	// limit" (database/sql's default) rather than a hidden value — the
	// saturation decision is documented in docs/BENCHMARKS.md.
	//
	// The pool lines are omitted (not zero-filled) when the store has no
	// PostgreSQL pool: a file-backed deployment emitting
	// audit_store_pg_pool_in_use 0 would read as "pool exhausted at zero
	// connections", which is a different fact.
	if s.Service != nil && s.Service.Store != nil {
		if stats := s.Service.Store.PoolStats(); stats != nil {
			fmt.Fprintf(w, "audit_store_pg_pool_in_use %d\n", stats.InUse)
			fmt.Fprintf(w, "audit_store_pg_pool_idle %d\n", stats.Idle)
			fmt.Fprintf(w, "audit_store_pg_pool_max_open %d\n", stats.MaxOpenConnections)
			fmt.Fprintf(w, "audit_store_pg_pool_wait_count_total %d\n", stats.WaitCount)
			fmt.Fprintf(w, "audit_store_pg_pool_wait_duration_seconds_total %g\n", stats.WaitDuration.Seconds())
		}
		fmt.Fprintf(w, "audit_store_operation_deadline_exceeded_total %d\n", s.Service.Store.OperationDeadlineExhaustions())
	}
	fmt.Fprintf(w, "audit_integrity_checks_total{result=\"valid\"} %d\n", s.integrityValidCount.Load())
	fmt.Fprintf(w, "audit_integrity_checks_total{result=\"invalid\"} %d\n", s.integrityInvalidCount.Load())
	for i, upper := range s.latencyBuckets {
		label := fmt.Sprintf("%g", upper)
		fmt.Fprintf(w, "audit_http_request_duration_seconds_bucket{le=\"%s\"} %d\n", label, s.latencyCounts[i].Load())
	}
	fmt.Fprintf(w, "audit_http_request_duration_seconds_bucket{le=\"+Inf\"} %d\n", s.latencyTotal.Load())
	fmt.Fprintf(w, "audit_http_request_duration_seconds_sum %g\n", float64(s.latencySumNanos.Load())/1e9)
	fmt.Fprintf(w, "audit_http_request_duration_seconds_count %d\n", s.latencyTotal.Load())
}

// recordLatency updates the request duration histogram. Called from the
// middleware after each request (including panics).
func (s *Server) recordLatency(elapsed time.Duration) {
	seconds := float64(elapsed) / float64(time.Second)
	s.latencyTotal.Add(1)
	s.latencySumNanos.Add(uint64(elapsed))
	for i, upper := range s.latencyBuckets {
		if seconds <= upper {
			s.latencyCounts[i].Add(1)
		}
	}
}
