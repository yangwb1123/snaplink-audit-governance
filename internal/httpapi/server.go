package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
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
		defer func() {
			if recovered := recover(); recovered != nil {
				// writeError counts the 500: the panic is one error response.
				span.RecordError(fmt.Errorf("panic: %v", recovered))
				s.Logger.Printf("request_id=%s panic=%v", requestID, recovered)
				s.writeError(w, r, http.StatusInternalServerError, fmt.Errorf("internal server error"))
			}
			span.End()
		}()
		handler(w, r)
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
	fmt.Fprintf(w, "audit_query_requests_total %d\n", s.queryCount.Load())
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

func (s *Server) postEvent(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:event:write")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	var event domain.Event
	if err := decodeBody(w, r, &event); err != nil {
		s.writeError(w, r, http.StatusBadRequest, err)
		return
	}
	principal := domain.IngestPrincipal{ClientID: claims.ClientID}
	// F1: the ingest commit must survive client disconnect — the durable
	// write is detached from r.Context() cancellation (values are kept) so a
	// dropped connection can never roll back a ledgered event. The worker's
	// signal-driven context stays cancellable: server-initiated stop is a
	// legitimate abort.
	receipt, err := s.Service.Ingest(context.WithoutCancel(r.Context()), claims.TenantID, principal, event, r.URL.Query().Get("wait_for"))
	s.ingestCount.Add(1)
	if errors.Is(err, domain.ErrQuotaExceeded) {
		s.ingestQuotaCount.Add(1)
	}
	if receipt.Duplicate {
		s.ingestDuplicateCount.Add(1)
	}
	if receipt.Conflict {
		s.ingestConflictCount.Add(1)
	}
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"receipt": receipt, "receipt_url": "/api/v1/events/" + receipt.EventID + "/receipt"})
}

type batchRequest struct {
	Events  []domain.Event `json:"events"`
	WaitFor string         `json:"wait_for,omitempty"`
}

func (s *Server) postBatch(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:event:write")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	var request batchRequest
	if err := decodeBody(w, r, &request); err != nil {
		s.writeError(w, r, http.StatusBadRequest, err)
		return
	}
	if len(request.Events) == 0 {
		s.writeError(w, r, http.StatusBadRequest, fmt.Errorf("%w: events must not be empty", domain.ErrInvalid))
		return
	}
	if request.WaitFor == "" {
		request.WaitFor = r.URL.Query().Get("wait_for")
	}
	receipts := make([]domain.EventReceipt, 0, len(request.Events))
	principal := domain.IngestPrincipal{ClientID: claims.ClientID}
	commitCtx := context.WithoutCancel(r.Context())
	for _, event := range request.Events {
		receipt, ingestErr := s.Service.Ingest(commitCtx, claims.TenantID, principal, event, request.WaitFor)
		if receipt.Duplicate {
			s.ingestDuplicateCount.Add(1)
		}
		if receipt.Conflict {
			s.ingestConflictCount.Add(1)
		}
		receipts = append(receipts, receipt)
		if ingestErr != nil {
			if errors.Is(ingestErr, domain.ErrQuotaExceeded) {
				s.ingestQuotaCount.Add(1)
			}
			partialStatus := statusForError(ingestErr)
			if partialStatus >= 500 {
				s.errorCount.Add(1)
			}
			writeJSON(w, partialStatus, map[string]any{"receipts": receipts, "error": errorBody(partialStatus, ingestErr, r)})
			return
		}
		s.ingestCount.Add(1)
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"receipts": receipts})
}

func (s *Server) getEvent(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:event:read")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	event, err := s.Service.GetEvent(tenantID, claims.Subject, r.PathValue("eventID"))
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	// 响应剥离搜索摘要（深拷贝，绝不改动存储快照共享的 map）：摘要值曾
	// 可用于跨租户关联，且对事件读者无业务价值。剥离失败时 500 —— 宁可
	// 失败也不泄漏。
	stripped, err := security.StripSearchDigests(event.Payload)
	if err != nil {
		s.writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	event.Payload = stripped
	writeJSON(w, http.StatusOK, event)
}

func (s *Server) getReceipt(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:event:read")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	receipt, err := s.Service.GetReceipt(tenantID, claims.Subject, r.PathValue("eventID"))
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, receipt)
}

func (s *Server) queryEvents(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:event:read")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	query, err := parseQuery(r)
	if err != nil {
		s.writeError(w, r, http.StatusBadRequest, err)
		return
	}
	s.queryCount.Add(1)
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.QueryEvents(tenantID, claims.Subject, query)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	// 与 getEvent 相同：列表响应同样剥离搜索摘要，仅在响应副本上进行。
	for i := range result.Items {
		stripped, stripErr := security.StripSearchDigests(result.Items[i].Payload)
		if stripErr != nil {
			s.writeError(w, r, http.StatusInternalServerError, stripErr)
			return
		}
		result.Items[i].Payload = stripped
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) getOperation(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:operation:read")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.Operation(tenantID, claims.Subject, r.PathValue("operationID"))
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) getOperationTimeline(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:operation:read")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.OperationTimeline(tenantID, claims.Subject, r.PathValue("operationID"))
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	// 与 getEvent/queryEvents 相同：时间线响应剥离搜索摘要（深拷贝，绝不
	// 改动存储快照共享的 map）。剥离失败时 500 —— 宁可失败也不泄漏。
	for i := range result {
		stripped, stripErr := security.StripSearchDigests(result[i].Payload)
		if stripErr != nil {
			s.writeError(w, r, http.StatusInternalServerError, stripErr)
			return
		}
		result[i].Payload = stripped
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": result, "count": len(result)})
}

func (s *Server) replayOperation(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:operation:read")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.ReplayOperation(tenantID, claims.Subject, r.PathValue("operationID"))
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) getAggregateTimeline(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:operation:read")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	items, err := s.Service.AggregateTimeline(tenantID, claims.Subject, r.PathValue("aggregateType"), r.PathValue("aggregateID"))
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	// 与 getEvent/queryEvents 相同：聚合时间线响应剥离搜索摘要（深拷贝，
	// 绝不改动存储快照共享的 map）。剥离失败时 500 —— 宁可失败也不泄漏。
	for i := range items {
		stripped, stripErr := security.StripSearchDigests(items[i].Payload)
		if stripErr != nil {
			s.writeError(w, r, http.StatusInternalServerError, stripErr)
			return
		}
		items[i].Payload = stripped
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}

func (s *Server) createExport(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:export:create")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	query, err := parseBodyQuery(w, r)
	if err != nil {
		s.writeError(w, r, http.StatusBadRequest, err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	job, err := s.Service.CreateExport(tenantID, claims.Subject, query)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}

func (s *Server) getExport(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:export:create")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	job, err := s.Service.GetExport(tenantID, r.PathValue("jobID"))
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	// The API boundary never surfaces the raw export failure diagnostic
	// (filesystem paths, errno text); operators read the full detail from
	// state.json directly. Persist-time sanitization stays deferred: the
	// snapshot keeps the raw error, the response gets a fixed message.
	if job.Error != "" {
		job.Error = "export failed"
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) downloadExport(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:export:create")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	job, err := s.Service.GetExport(tenantID, r.PathValue("jobID"))
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	if job.Status != "completed" || job.ObjectPath == "" {
		s.writeError(w, r, http.StatusConflict, fmt.Errorf("%w: export is not completed", domain.ErrConflict))
		return
	}
	data, err := s.Service.Config.Archive.Get(r.Context(), job.ObjectPath)
	if err != nil {
		if os.IsNotExist(err) {
			s.writeError(w, r, http.StatusNotFound, domain.ErrNotFound)
			return
		}
		s.writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	// 下载完整性：绑定认证 + 摘要格式 + 摘要一致全部通过后才返回明文。
	// 任一失败均为 500（redacted），且不记录 audit.event.export —— 只有
	// 经过验证的下载才是治理事实（此前 404/500 归档失败也会先记录该事实）。
	decrypted, err := s.Service.VerifyExportDownload(job.TenantID, job.ID, data)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	// 导出下载是治理事实（audit.event.export）：服务层追加失败则中止下载
	// （fail-closed，与读自审计同一规则）。R4 法律保留门禁在服务方法内部
	// 重新执行，故 403 仍在任何字节流出前生效。
	if err := s.Service.RecordExportDownload(job.TenantID, claims.Subject, job.ID); err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Content-Disposition", `attachment; filename="`+safeDownloadName(job.ID)+`.jsonl"`)
	w.Header().Set("Content-Length", strconv.Itoa(len(decrypted)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(decrypted)
}

func (s *Server) verifyIntegrity(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:integrity:verify")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	var request struct {
		StreamID string `json:"stream_id"`
	}
	if err := decodeBody(w, r, &request); err != nil && !errors.Is(err, io.EOF) {
		s.writeError(w, r, http.StatusBadRequest, err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.VerifyIntegrity(r.Context(), tenantID, claims.Subject, request.StreamID)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	if result.Valid {
		s.integrityValidCount.Add(1)
	} else {
		s.integrityInvalidCount.Add(1)
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) createLegalHold(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:legal_hold:manage")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	var hold domain.LegalHold
	if err := decodeBody(w, r, &hold); err != nil {
		s.writeError(w, r, http.StatusBadRequest, err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	hold.TenantID = tenantID
	hold.CreatedBy = claims.Subject
	result, err := s.Service.CreateLegalHold(hold)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (s *Server) listLegalHolds(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:legal_hold:manage")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	items, err := s.Service.ListLegalHolds(tenantID)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}

func (s *Server) releaseLegalHold(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:legal_hold:manage")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	hold, err := s.Service.ReleaseLegalHold(tenantID, r.PathValue("holdID"), claims.Subject)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, hold)
}

func (s *Server) previewRestore(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:operation:read")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	var request domain.RestoreRequest
	if err := decodeBody(w, r, &request); err != nil {
		s.writeError(w, r, http.StatusBadRequest, err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.PreviewRestore(tenantID, request)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) createRestore(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:legal_hold:manage")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	var request domain.RestoreRequest
	if err := decodeBody(w, r, &request); err != nil {
		s.writeError(w, r, http.StatusBadRequest, err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.CreateRestore(tenantID, request, claims.Subject)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (s *Server) approveRestore(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:legal_hold:manage")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.ApproveRestore(tenantID, r.PathValue("runID"), claims.Subject)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) rejectRestore(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:legal_hold:manage")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.RejectRestore(tenantID, r.PathValue("runID"), claims.Subject)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) getRestore(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:operation:read")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.GetRestore(tenantID, r.PathValue("runID"))
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) createTenant(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:platform:cross_tenant")
	if err != nil || !claims.Platform {
		if err == nil {
			err = domain.ErrForbidden
		}
		s.writeError(w, r, statusForError(err), err)
		return
	}
	var tenant domain.Tenant
	if err := decodeBody(w, r, &tenant); err != nil {
		s.writeError(w, r, http.StatusBadRequest, err)
		return
	}
	if tenant.CreatedAt.IsZero() {
		tenant.CreatedAt = s.Service.Now()
	}
	if !tenant.Active {
		tenant.Active = true
	}
	if err := s.Service.CreateTenant(claims.Subject, tenant); err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusCreated, tenant)
}

func (s *Server) listTenants(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:platform:cross_tenant")
	if err != nil || !claims.Platform {
		if err == nil {
			err = domain.ErrForbidden
		}
		s.writeError(w, r, statusForError(err), err)
		return
	}
	items, err := s.Service.ListTenants()
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}

func (s *Server) createSource(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:policy:write")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	var source domain.SourceSystem
	if err := decodeBody(w, r, &source); err != nil {
		s.writeError(w, r, http.StatusBadRequest, err)
		return
	}
	if !claims.Platform {
		source.TenantID = claims.TenantID
	}
	if source.CreatedAt.IsZero() {
		source.CreatedAt = s.Service.Now()
	}
	if !source.Active {
		source.Active = true
	}
	if err := s.Service.AddSource(claims.Subject, source); err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusCreated, source)
}

func (s *Server) listSources(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:policy:read")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	items, err := s.Service.ListSources(tenantID)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}

func (s *Server) updateSource(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:policy:write")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	var source domain.SourceSystem
	if err := decodeBody(w, r, &source); err != nil {
		s.writeError(w, r, http.StatusBadRequest, err)
		return
	}
	source.ID = r.PathValue("sourceID")
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	source.TenantID = tenantID
	updated, err := s.Service.UpdateSource(claims.Subject, source)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (s *Server) createSchema(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:policy:write")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	var schema domain.EventSchema
	if err := decodeBody(w, r, &schema); err != nil {
		s.writeError(w, r, http.StatusBadRequest, err)
		return
	}
	if !claims.Platform {
		schema.TenantID = claims.TenantID
	}
	if schema.CreatedAt.IsZero() {
		schema.CreatedAt = s.Service.Now()
	}
	if !schema.Active {
		schema.Active = true
	}
	if err := s.Service.RegisterSchema(claims.Subject, schema); err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusCreated, schema)
}

func (s *Server) listSchemas(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:policy:read")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	items, err := s.Service.ListSchemas(tenantID)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}

func (s *Server) setRetention(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:policy:write")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	var policy domain.RetentionPolicy
	if err := decodeBody(w, r, &policy); err != nil {
		s.writeError(w, r, http.StatusBadRequest, err)
		return
	}
	if !claims.Platform {
		policy.TenantID = claims.TenantID
	}
	if err := s.Service.SetRetentionPolicy(claims.Subject, policy); err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, policy)
}

func (s *Server) getRetention(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:policy:read")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	policy, err := s.Service.GetRetentionPolicy(tenantID)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, policy)
}

func (s *Server) evaluateRetention(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:policy:read")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.EvaluateRetention(tenantID, time.Time{})
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) listAdminActions(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:policy:read")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	limit := 0
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil {
			s.writeError(w, r, http.StatusBadRequest, fmt.Errorf("%w: invalid limit", domain.ErrInvalid))
			return
		}
	}
	items, err := s.Service.ListAdminActions(tenantID, claims.Platform, limit)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}

func (s *Server) require(r *http.Request, permission string) (auth.Claims, error) {
	claims, err := s.Auth.Authenticate(r)
	if err != nil {
		return auth.Claims{}, fmt.Errorf("%w: %v", domain.ErrUnauthorized, err)
	}
	if !claims.Allows(permission) {
		return claims, domain.ErrForbidden
	}
	if permission != "audit:event:write" && claims.TenantID == "" && !claims.Platform {
		return claims, fmt.Errorf("%w: tenant context is required", domain.ErrUnauthorized)
	}
	return claims, nil
}

func (s *Server) tenantFor(r *http.Request, claims auth.Claims) (string, error) {
	if claims.Platform {
		if tenantID := r.URL.Query().Get("tenant_id"); tenantID != "" {
			// Key-framing charset rule: the escape-hatch tenant becomes a
			// composite-key component in every downstream read, so an embedded
			// KeySeparator (0x1F) would forge another tenant's keys (cross-tenant
			// read, forged self-audit record). Reject before any service or
			// store access. Empty stays legal (all-tenants read).
			if err := store.ValidTenantID(tenantID); err != nil {
				return "", err
			}
			return tenantID, nil
		}
		// A platform token has no tenant scope of its own; claims.TenantID is
		// meaningless here (dev tokens fill it with their own subject,
		// "platform"). Without a filter the empty sentinel is returned — the
		// all-tenants read — matching the production JWT path where platform
		// tokens carry an empty tenant_id claim.
		return "", nil
	}
	if claims.TenantID != "" {
		// Key-framing charset rule, non-platform branch: claims.TenantID is
		// canonical-safe by construction today (tenantClaim and parseDevToken
		// both validate), so this is defense-in-depth — the boundary must
		// re-check the same rule any future claim producer bypasses. Empty
		// stays legal (all-tenants read; client-id-resolved ingest).
		if err := store.ValidTenantID(claims.TenantID); err != nil {
			return "", err
		}
	}
	return claims.TenantID, nil
}

func parseQuery(r *http.Request) (domain.Query, error) {
	values := r.URL.Query()
	from, err := parseTime(values.Get("from"))
	if err != nil {
		return domain.Query{}, err
	}
	to, err := parseTime(values.Get("to"))
	if err != nil {
		return domain.Query{}, err
	}
	pageSize := 0
	if raw := values.Get("page_size"); raw != "" {
		pageSize, err = strconv.Atoi(raw)
		if err != nil {
			return domain.Query{}, fmt.Errorf("%w: invalid page_size", domain.ErrInvalid)
		}
	}
	return domain.Query{From: from, To: to, EventType: values.Get("event_type"), SourceSystem: values.Get("source_system"), ActorID: values.Get("actor_id"), TargetID: values.Get("target_id"), AggregateType: values.Get("aggregate_type"), AggregateID: values.Get("aggregate_id"), OperationID: values.Get("operation_id"), CausationID: values.Get("causation_id"), CorrelationID: values.Get("correlation_id"), TraceID: values.Get("trace_id"), Outcome: values.Get("outcome"), PayloadField: values.Get("payload_field"), PayloadDigest: values.Get("payload_digest"), StreamID: values.Get("stream_id"), Cursor: values.Get("cursor"), PageSize: pageSize}, nil
}

func parseBodyQuery(w http.ResponseWriter, r *http.Request) (domain.Query, error) {
	var query domain.Query
	if err := decodeBody(w, r, &query); err != nil {
		return domain.Query{}, err
	}
	return query, nil
}

func parseTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, fmt.Errorf("%w: from and to are required", domain.ErrInvalid)
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid time %q", domain.ErrInvalid, value)
	}
	return parsed.UTC(), nil
}

func decodeBody(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, domain.MaxEventBytes*2)
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("%w: request body must contain one JSON value", domain.ErrInvalid)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// writeError renders the error contract and counts the response against
// the error-rate instrumentation (H-3): audit_http_errors_total is the error
// budget source, so every >=500 response a handler emits must be counted
// here — the panic-recovery path and the batch partial-failure path both
// converge on this counter, keeping the SLO error rate measurable.
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, status int, err error) {
	if status >= 500 {
		s.errorCount.Add(1)
		w.Header().Set("Cache-Control", "no-store")
	}
	writeJSON(w, status, errorBody(status, err, r))
}

// errorBody renders the stable error contract. The status is authoritative
// for the message: responses with status >= 500 never carry internal error
// text (filesystem paths, errno strings, crypto details) and collapse to the
// fixed strings, while the code keeps the domain mapping below 500. The two
// stay consistent by construction because statusForError maps every domain
// error to < 500, so a >= 500 response always means internal_error.
func errorBody(status int, err error, r *http.Request) map[string]any {
	code := "internal_error"
	switch {
	case errors.Is(err, domain.ErrInvalid):
		code = "invalid_request"
	case errors.Is(err, domain.ErrUnauthorized):
		code = "unauthorized"
	case errors.Is(err, domain.ErrForbidden):
		code = "forbidden"
	case errors.Is(err, domain.ErrNotFound):
		code = "not_found"
	case errors.Is(err, domain.ErrConflict):
		code = "conflict"
	case errors.Is(err, domain.ErrQuotaExceeded):
		code = "quota_exceeded"
	case errors.Is(err, domain.ErrSchemaNotFound):
		code = "schema_not_found"
	case errors.Is(err, domain.ErrTenantMismatch):
		code = "tenant_mismatch"
	case errors.Is(err, store.ErrSnapshotConflict):
		code = "snapshot_conflict"
	}
	message := err.Error()
	if status >= 500 {
		message = "internal server error"
	}
	return map[string]any{"error": map[string]any{"code": code, "message": message, "request_id": r.Context().Value(requestIDKey)}}
}

func statusForError(err error) int {
	switch {
	case errors.Is(err, domain.ErrInvalid):
		return http.StatusBadRequest
	case errors.Is(err, domain.ErrUnauthorized):
		return http.StatusUnauthorized
	case errors.Is(err, domain.ErrForbidden):
		return http.StatusForbidden
	case errors.Is(err, domain.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, domain.ErrConflict):
		return http.StatusConflict
	case errors.Is(err, domain.ErrQuotaExceeded):
		return http.StatusTooManyRequests
	case errors.Is(err, domain.ErrSchemaNotFound):
		return http.StatusUnprocessableEntity
	case errors.Is(err, domain.ErrTenantMismatch):
		// 信封租户与令牌解析租户不一致：请求语义无效，拒绝入账（DS-08）。
		return http.StatusUnprocessableEntity
	case errors.Is(err, store.ErrSnapshotConflict):
		// 乐观锁冲突重试耗尽：请求结果未知，客户端应按幂等语义重试。
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func newRequestID() string { return fmt.Sprintf("req-%d", time.Now().UnixNano()) }

// maxRequestIDBytes caps the client-supplied X-Request-ID echoed into
// response headers, error bodies, span attributes and panic logs. The value
// is correlation-only (never used for authorization), so an unbounded copy
// would only amplify abuse (F-3): a flood of 1 MB IDs would balloon every
// error body and every OTLP span attribute.
const maxRequestIDBytes = 128

// truncateRequestID clips a client-supplied X-Request-ID to the cap.
func truncateRequestID(value string) string {
	if len(value) <= maxRequestIDBytes {
		return value
	}
	return value[:maxRequestIDBytes]
}

func safeDownloadName(value string) string {
	var b strings.Builder
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "export"
	}
	return b.String()
}
