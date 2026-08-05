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
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
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
	return &Server{Service: svc, Auth: authenticator, Logger: logger}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("POST /api/v1/events", s.postEvent)
	mux.HandleFunc("POST /api/v1/events:batch", s.postBatch)
	mux.HandleFunc("GET /api/v1/events/{eventID}", s.getEvent)
	mux.HandleFunc("GET /api/v1/events/{eventID}/receipt", s.getReceipt)
	mux.HandleFunc("GET /api/v1/events", s.queryEvents)
	mux.HandleFunc("GET /api/v1/operations/{operationID}", s.getOperation)
	mux.HandleFunc("GET /api/v1/operations/{operationID}/timeline", s.getOperationTimeline)
	mux.HandleFunc("GET /api/v1/operations/{operationID}/replay", s.replayOperation)
	mux.HandleFunc("GET /api/v1/aggregates/{aggregateType}/{aggregateID}/timeline", s.getAggregateTimeline)
	mux.HandleFunc("POST /api/v1/exports", s.createExport)
	mux.HandleFunc("GET /api/v1/exports/{jobID}", s.getExport)
	mux.HandleFunc("GET /api/v1/exports/{jobID}/download", s.downloadExport)
	mux.HandleFunc("POST /api/v1/integrity/verify", s.verifyIntegrity)
	mux.HandleFunc("POST /api/v1/legal-holds", s.createLegalHold)
	mux.HandleFunc("GET /api/v1/legal-holds", s.listLegalHolds)
	mux.HandleFunc("POST /api/v1/legal-holds/{holdID}/release", s.releaseLegalHold)
	mux.HandleFunc("POST /api/v1/restores/preview", s.previewRestore)
	mux.HandleFunc("POST /api/v1/restores", s.createRestore)
	mux.HandleFunc("GET /api/v1/restores/{runID}", s.getRestore)
	mux.HandleFunc("POST /api/v1/restores/{runID}/approve", s.approveRestore)
	mux.HandleFunc("POST /api/v1/restores/{runID}/reject", s.rejectRestore)
	mux.HandleFunc("POST /api/v1/tenants", s.createTenant)
	mux.HandleFunc("GET /api/v1/tenants", s.listTenants)
	mux.HandleFunc("POST /api/v1/sources", s.createSource)
	mux.HandleFunc("GET /api/v1/sources", s.listSources)
	mux.HandleFunc("PUT /api/v1/sources/{sourceID}", s.updateSource)
	mux.HandleFunc("POST /api/v1/schemas", s.createSchema)
	mux.HandleFunc("GET /api/v1/schemas", s.listSchemas)
	mux.HandleFunc("PUT /api/v1/policies/retention", s.setRetention)
	mux.HandleFunc("GET /api/v1/policies/retention", s.getRetention)
	mux.HandleFunc("POST /api/v1/retention/evaluate", s.evaluateRetention)
	mux.HandleFunc("GET /api/v1/admin/actions", s.listAdminActions)
	return s.middleware(mux)
}

func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = newRequestID()
		}
		// W3C trace context: extract an incoming traceparent, create a server
		// span for the route and inject the new context back into the
		// response headers. Falls back to the no-op tracer when tracing is
		// disabled (AUDIT_OTLP_ENDPOINT unset).
		propagator := otel.GetTextMapPropagator()
		ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		route := r.URL.Path
		spanName := r.Method + " " + route
		ctx, span := otel.Tracer("audit-api").Start(ctx, spanName,
			trace.WithAttributes(
				attribute.String("http.method", r.Method),
				attribute.String("http.route", route),
				attribute.String("request_id", requestID),
			))
		defer span.End()
		propagator.Inject(ctx, propagation.HeaderCarrier(w.Header()))
		traceID := requestID
		if spanContext := trace.SpanContextFromContext(ctx); spanContext.IsValid() {
			traceID = spanContext.TraceID().String()
		}

		ctx = context.WithValue(ctx, requestIDKey, requestID)
		r = r.WithContext(ctx)
		w.Header().Set("X-Request-ID", requestID)
		w.Header().Set("X-Trace-ID", traceID)
		defer func() {
			if recovered := recover(); recovered != nil {
				s.errorCount.Add(1)
				span.RecordError(fmt.Errorf("panic: %v", recovered))
				s.Logger.Printf("request_id=%s panic=%v", requestID, recovered)
				writeError(w, r, http.StatusInternalServerError, fmt.Errorf("internal server error"))
			}
			s.requestCount.Add(1)
			_ = started
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readyz(w http.ResponseWriter, _ *http.Request) {
	if s.Service == nil || s.Service.Store == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	// Readiness reflects the configured archive dependency: an unavailable
	// WORM destination must surface here instead of silently degrading the
	// archived status of new events.
	if dir := s.Service.Config.ArchiveDir; dir != "" {
		probe := filepath.Join(dir, ".readyz-probe")
		if err := os.MkdirAll(dir, 0o750); err != nil || os.WriteFile(probe, []byte("ok"), 0o640) != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "reason": "archive_unavailable"})
			return
		}
		_ = os.Remove(probe)
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
}

func (s *Server) postEvent(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:event:write")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	var event domain.Event
	if err := decodeBody(w, r, &event); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	principal := domain.IngestPrincipal{ClientID: claims.ClientID}
	receipt, err := s.Service.Ingest(claims.TenantID, principal, event, r.URL.Query().Get("wait_for"))
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
		writeError(w, r, statusForError(err), err)
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
		writeError(w, r, statusForError(err), err)
		return
	}
	var request batchRequest
	if err := decodeBody(w, r, &request); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	if len(request.Events) == 0 {
		writeError(w, r, http.StatusBadRequest, fmt.Errorf("%w: events must not be empty", domain.ErrInvalid))
		return
	}
	if request.WaitFor == "" {
		request.WaitFor = r.URL.Query().Get("wait_for")
	}
	receipts := make([]domain.EventReceipt, 0, len(request.Events))
	principal := domain.IngestPrincipal{ClientID: claims.ClientID}
	for _, event := range request.Events {
		receipt, ingestErr := s.Service.Ingest(claims.TenantID, principal, event, request.WaitFor)
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
			writeJSON(w, statusForError(ingestErr), map[string]any{"receipts": receipts, "error": errorBody(ingestErr, r)})
			return
		}
		s.ingestCount.Add(1)
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"receipts": receipts})
}

func (s *Server) getEvent(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:event:read")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	tenantID := s.tenantFor(r, claims)
	event, err := s.Service.GetEvent(tenantID, r.PathValue("eventID"))
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, event)
}

func (s *Server) getReceipt(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:event:read")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	receipt, err := s.Service.GetReceipt(s.tenantFor(r, claims), r.PathValue("eventID"))
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, receipt)
}

func (s *Server) queryEvents(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:event:read")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	query, err := parseQuery(r)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	s.queryCount.Add(1)
	result, err := s.Service.QueryEvents(s.tenantFor(r, claims), query)
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) getOperation(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:operation:read")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.Operation(s.tenantFor(r, claims), r.PathValue("operationID"))
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) getOperationTimeline(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:operation:read")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.OperationTimeline(s.tenantFor(r, claims), r.PathValue("operationID"))
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": result, "count": len(result)})
}

func (s *Server) replayOperation(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:operation:read")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.ReplayOperation(s.tenantFor(r, claims), r.PathValue("operationID"))
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) getAggregateTimeline(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:operation:read")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	items, err := s.Service.AggregateTimeline(s.tenantFor(r, claims), r.PathValue("aggregateType"), r.PathValue("aggregateID"))
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}

func (s *Server) createExport(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:export:create")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	query, err := parseBodyQuery(w, r)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	job, err := s.Service.CreateExport(s.tenantFor(r, claims), claims.Subject, query)
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}

func (s *Server) getExport(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:export:create")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	job, err := s.Service.GetExport(s.tenantFor(r, claims), r.PathValue("jobID"))
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) downloadExport(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:export:create")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	job, err := s.Service.GetExport(s.tenantFor(r, claims), r.PathValue("jobID"))
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	if job.Status != "completed" || job.ObjectPath == "" {
		writeError(w, r, http.StatusConflict, fmt.Errorf("%w: export is not completed", domain.ErrConflict))
		return
	}
	data, err := s.Service.Config.Archive.Get(r.Context(), job.ObjectPath)
	if err != nil {
		if os.IsNotExist(err) {
			writeError(w, r, http.StatusNotFound, domain.ErrNotFound)
			return
		}
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Content-Disposition", `attachment; filename="`+safeDownloadName(job.ID)+`.jsonl"`)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (s *Server) verifyIntegrity(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:integrity:verify")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	var request struct {
		StreamID string `json:"stream_id"`
	}
	if err := decodeBody(w, r, &request); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	result, err := s.Service.VerifyIntegrity(s.tenantFor(r, claims), request.StreamID)
	if err != nil {
		writeError(w, r, statusForError(err), err)
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
		writeError(w, r, statusForError(err), err)
		return
	}
	var hold domain.LegalHold
	if err := decodeBody(w, r, &hold); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	hold.TenantID = s.tenantFor(r, claims)
	hold.CreatedBy = claims.Subject
	result, err := s.Service.CreateLegalHold(hold)
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (s *Server) listLegalHolds(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:legal_hold:manage")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	items, err := s.Service.ListLegalHolds(s.tenantFor(r, claims))
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}

func (s *Server) releaseLegalHold(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:legal_hold:manage")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	hold, err := s.Service.ReleaseLegalHold(s.tenantFor(r, claims), r.PathValue("holdID"), claims.Subject)
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, hold)
}

func (s *Server) previewRestore(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:operation:read")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	var request domain.RestoreRequest
	if err := decodeBody(w, r, &request); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	result, err := s.Service.PreviewRestore(s.tenantFor(r, claims), request)
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) createRestore(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:legal_hold:manage")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	var request domain.RestoreRequest
	if err := decodeBody(w, r, &request); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	result, err := s.Service.CreateRestore(s.tenantFor(r, claims), request, claims.Subject)
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (s *Server) approveRestore(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:legal_hold:manage")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.ApproveRestore(s.tenantFor(r, claims), r.PathValue("runID"), claims.Subject)
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) rejectRestore(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:legal_hold:manage")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.RejectRestore(s.tenantFor(r, claims), r.PathValue("runID"), claims.Subject)
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) getRestore(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:operation:read")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.GetRestore(s.tenantFor(r, claims), r.PathValue("runID"))
	if err != nil {
		writeError(w, r, statusForError(err), err)
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
		writeError(w, r, statusForError(err), err)
		return
	}
	var tenant domain.Tenant
	if err := decodeBody(w, r, &tenant); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	if tenant.CreatedAt.IsZero() {
		tenant.CreatedAt = s.Service.Now()
	}
	if !tenant.Active {
		tenant.Active = true
	}
	if err := s.Service.CreateTenant(claims.Subject, tenant); err != nil {
		writeError(w, r, statusForError(err), err)
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
		writeError(w, r, statusForError(err), err)
		return
	}
	items, err := s.Service.ListTenants()
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}

func (s *Server) createSource(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:policy:write")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	var source domain.SourceSystem
	if err := decodeBody(w, r, &source); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
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
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusCreated, source)
}

func (s *Server) listSources(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:policy:read")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	items, err := s.Service.ListSources(s.tenantFor(r, claims))
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}

func (s *Server) updateSource(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:policy:write")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	var source domain.SourceSystem
	if err := decodeBody(w, r, &source); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	source.ID = r.PathValue("sourceID")
	source.TenantID = s.tenantFor(r, claims)
	updated, err := s.Service.UpdateSource(claims.Subject, source)
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (s *Server) createSchema(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:policy:write")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	var schema domain.EventSchema
	if err := decodeBody(w, r, &schema); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
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
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusCreated, schema)
}

func (s *Server) listSchemas(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:policy:read")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	items, err := s.Service.ListSchemas(s.tenantFor(r, claims))
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}

func (s *Server) setRetention(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:policy:write")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	var policy domain.RetentionPolicy
	if err := decodeBody(w, r, &policy); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	if !claims.Platform {
		policy.TenantID = claims.TenantID
	}
	if err := s.Service.SetRetentionPolicy(claims.Subject, policy); err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, policy)
}

func (s *Server) getRetention(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:policy:read")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	policy, err := s.Service.GetRetentionPolicy(s.tenantFor(r, claims))
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, policy)
}

func (s *Server) evaluateRetention(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:policy:read")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.EvaluateRetention(s.tenantFor(r, claims), time.Time{})
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) listAdminActions(w http.ResponseWriter, r *http.Request) {
	claims, err := s.require(r, "audit:policy:read")
	if err != nil {
		writeError(w, r, statusForError(err), err)
		return
	}
	tenantID := ""
	if !claims.Platform {
		tenantID = claims.TenantID
	} else {
		tenantID = r.URL.Query().Get("tenant_id")
	}
	limit := 0
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, fmt.Errorf("%w: invalid limit", domain.ErrInvalid))
			return
		}
	}
	items, err := s.Service.ListAdminActions(tenantID, claims.Platform, limit)
	if err != nil {
		writeError(w, r, statusForError(err), err)
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

func (s *Server) tenantFor(r *http.Request, claims auth.Claims) string {
	if claims.Platform {
		if tenantID := r.URL.Query().Get("tenant_id"); tenantID != "" {
			return tenantID
		}
	}
	return claims.TenantID
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

func writeError(w http.ResponseWriter, r *http.Request, status int, err error) {
	if status >= 500 {
		w.Header().Set("Cache-Control", "no-store")
	}
	if status >= 400 {
		_ = r
	}
	writeJSON(w, status, errorBody(err, r))
}

func errorBody(err error, r *http.Request) map[string]any {
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
	}
	message := err.Error()
	if strings.Contains(message, "internal server error") {
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
	default:
		return http.StatusInternalServerError
	}
}

func newRequestID() string { return fmt.Sprintf("req-%d", time.Now().UnixNano()) }

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
