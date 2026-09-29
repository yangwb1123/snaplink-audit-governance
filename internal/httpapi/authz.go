package httpapi

// Request authentication, tenant scope resolution and the error/status
// mapping every handler shares. The scope matrix (which permission may read
// which tenant, and how a platform token differs from a tenant token) is the
// single place that decides what a caller is allowed to see.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func (s *Server) require(r *http.Request, permission string) (auth.Claims, error) {
	return s.requirePermission(r, permission, false)
}

// requireAdminActionRead is the only policy-read exception for a tenantless
// Console capability. CrossTenantAuditRead is intentionally not accepted by
// the ordinary policy handlers: those handlers still require a real tenant
// credential or platform credential before scope resolution.
func (s *Server) requireAdminActionRead(r *http.Request) (auth.Claims, error) {
	return s.requirePermission(r, "audit:policy:read", true)
}

func (s *Server) requirePermission(r *http.Request, permission string, allowTenantlessConsole bool) (auth.Claims, error) {
	claims, err := s.Auth.Authenticate(r)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("authentication failed: %v", err)
		}
		return auth.Claims{}, fmt.Errorf("%w: %v", domain.ErrUnauthorized, err)
	}
	if !claims.Allows(permission) && !(allowTenantlessConsole && claims.CrossTenantAuditRead) {
		return claims, domain.ErrForbidden
	}
	if permission != "audit:event:write" && claims.TenantID == "" && !claims.Platform &&
		!(allowTenantlessConsole && claims.CrossTenantAuditRead) {
		return claims, fmt.Errorf("%w: tenant context is required", domain.ErrUnauthorized)
	}
	return claims, nil
}

func (s *Server) tenantFor(r *http.Request, claims auth.Claims) (string, error) {
	// Keep the tenant selector read in this boundary. The selector is read
	// once, including its cardinality, so repeated values cannot be reduced by
	// URL.Values.Get into an ambiguous first-value decision.
	queryTenant, _, err := queryTenantSelector(r)
	if err != nil {
		return "", err
	}
	if claims.Platform || claims.CrossTenantAuditRead {
		// A platform token has no tenant scope of its own; claims.TenantID is
		// never a fallback (development tokens may populate it with "platform").
		return queryTenant, nil
	}
	if claims.TenantID != "" {
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
	pageSize, cursor, err := parsePageOptions(values)
	if err != nil {
		return domain.Query{}, err
	}
	return domain.Query{From: from, To: to, EventType: values.Get("event_type"), SourceSystem: values.Get("source_system"), ActorID: values.Get("actor_id"), TargetID: values.Get("target_id"), AggregateType: values.Get("aggregate_type"), AggregateID: values.Get("aggregate_id"), OperationID: values.Get("operation_id"), CausationID: values.Get("causation_id"), CorrelationID: values.Get("correlation_id"), TraceID: values.Get("trace_id"), Outcome: values.Get("outcome"), PayloadField: values.Get("payload_field"), PayloadDigest: values.Get("payload_digest"), StreamID: values.Get("stream_id"), Cursor: cursor, PageSize: pageSize}, nil
}

func parseTimelinePage(r *http.Request) (int, string, error) {
	return parsePageOptions(r.URL.Query())
}

func parsePageOptions(values interface{ Get(string) string }) (int, string, error) {
	pageSize := 0
	if raw := values.Get("page_size"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return 0, "", fmt.Errorf("%w: invalid page_size", domain.ErrInvalid)
		}
		pageSize = parsed
	}
	return pageSize, values.Get("cursor"), nil
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
	case errors.Is(err, domain.ErrOccurredAtOutOfRange):
		// occurred_at 超出账本时间窗：语义级合同拒绝（与 tenant_mismatch /
		// schema_not_found 同级），必须先于 ErrInvalid 匹配——该哨兵包装了
		// ErrInvalid，顺序颠倒会塌缩回 400 invalid_request。
		code = "occurred_at_out_of_range"
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
	if status == http.StatusUnauthorized {
		// Authentication errors can contain JWKS endpoint, transport, or
		// parser details. Keep those details server-side and expose only the
		// stable protocol message.
		message = "unauthorized"
	} else if status >= 500 {
		message = "internal server error"
	}
	return map[string]any{"error": map[string]any{"code": code, "message": message, "request_id": r.Context().Value(requestIDKey)}}
}

func statusForError(err error) int {
	switch {
	case errors.Is(err, domain.ErrOccurredAtOutOfRange):
		// 同 errorBody：哨兵先于 ErrInvalid 匹配，保持 422 分类。
		return http.StatusUnprocessableEntity
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
