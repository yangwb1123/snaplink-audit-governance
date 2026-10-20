package httpapi

// Governance surface: compliance exports, integrity verification, Legal
// Holds, the restore approval flow, and the control-plane resources (tenants,
// sources, schemas, retention policy, admin trail). Every mutation here is
// recorded by the service layer as its own self-audit fact.

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func (s *Server) createExport(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
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
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	job, err := s.Service.CreateExport(r.Context(), scope.TenantID, claims.Subject, query)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}

func (s *Server) getExport(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	claims, err := s.require(r, "audit:export:create")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	job, err := s.Service.GetExport(r.Context(), scope.TenantID, r.PathValue("jobID"))
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
	job.FailureCode = domain.PublicExportFailureCode(job.FailureCode)
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) downloadExport(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	claims, err := s.require(r, "audit:export:create")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	job, err := s.Service.GetExport(r.Context(), scope.TenantID, r.PathValue("jobID"))
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	if job.Status != "completed" || job.ObjectPath == "" {
		s.writeError(w, r, http.StatusConflict, fmt.Errorf("%w: export is not completed", domain.ErrConflict))
		return
	}
	// The WORM object is read by the service, inside one store operation, so
	// the archive port is never reached from the transport: the read is
	// bounded by the store's own operation deadline and its failure is
	// classified where every other store operation is. A cancelled caller
	// still arrives bare as context.Canceled (Store.end propagates a
	// cancellation unchanged) and answers 499; an exhausted bound arrives as
	// store.ErrOperationDeadline and answers the retryable 503 store_timeout;
	// a missing object answers 404. Any genuine archive failure still falls
	// through statusForError's default arm to 500 internal_error with the
	// redacted message.
	data, err := s.Service.ReadExportObject(r.Context(), job.TenantID, job.ID)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	// 下载完整性：绑定认证 + 摘要格式 + 摘要一致全部通过后才返回明文。
	// 任一失败均为 500（redacted），且不记录 audit.event.export —— 只有
	// 经过验证的下载才是治理事实（此前 404/500 归档失败也会先记录该事实）。
	decrypted, err := s.Service.VerifyExportDownload(r.Context(), job.TenantID, job.ID, data)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	// 导出下载是治理事实（audit.event.export）：服务层追加失败则中止下载
	// （fail-closed，与读自审计同一规则）。R4 法律保留门禁在服务方法内部
	// 重新执行，故 403 仍在任何字节流出前生效。
	if err := s.Service.RecordExportDownload(r.Context(), job.TenantID, claims.Subject, job.ID); err != nil {
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
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.VerifyIntegrity(r.Context(), scope.TenantID, claims.Subject, request.StreamID)
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
	scope, err := s.resolveQueryBodyScope(r, claims, hold.TenantID)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	hold.TenantID = scope.TenantID
	hold.CreatedBy = claims.Subject
	result, err := s.Service.CreateLegalHold(r.Context(), hold)
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
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	items, err := s.Service.ListLegalHolds(r.Context(), scope.TenantID)
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
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	hold, err := s.Service.ReleaseLegalHold(r.Context(), scope.TenantID, r.PathValue("holdID"), claims.Subject)
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
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.PreviewRestore(r.Context(), scope.TenantID, request)
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
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.CreateRestore(r.Context(), scope.TenantID, request, claims.Subject)
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
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.ApproveRestore(r.Context(), scope.TenantID, r.PathValue("runID"), claims.Subject)
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
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.RejectRestore(r.Context(), scope.TenantID, r.PathValue("runID"), claims.Subject)
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
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.GetRestore(r.Context(), scope.TenantID, r.PathValue("runID"))
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
	if err := s.Service.CreateTenant(r.Context(), claims.Subject, tenant); err != nil {
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
	items, err := s.Service.ListTenants(r.Context())
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
	scope, err := s.resolveQueryBodyScope(r, claims, source.TenantID)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	source.TenantID = scope.TenantID
	if source.CreatedAt.IsZero() {
		source.CreatedAt = s.Service.Now()
	}
	if !source.Active {
		source.Active = true
	}
	if err := s.Service.AddSource(r.Context(), claims.Subject, source); err != nil {
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
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	items, err := s.Service.ListSources(r.Context(), scope.TenantID)
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
	scope, err := s.resolveQueryBodyScope(r, claims, source.TenantID)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	source.TenantID = scope.TenantID
	updated, err := s.Service.UpdateSource(r.Context(), claims.Subject, source)
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
	scope, err := s.resolveQueryBodyScope(r, claims, schema.TenantID)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	schema.TenantID = scope.TenantID
	if schema.CreatedAt.IsZero() {
		schema.CreatedAt = s.Service.Now()
	}
	if !schema.Active {
		schema.Active = true
	}
	if err := s.Service.RegisterSchema(r.Context(), claims.Subject, schema); err != nil {
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
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	items, err := s.Service.ListSchemas(r.Context(), scope.TenantID)
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
	scope, err := s.resolveQueryBodyScope(r, claims, policy.TenantID)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	policy.TenantID = scope.TenantID
	if err := s.Service.SetRetentionPolicy(r.Context(), claims.Subject, policy); err != nil {
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
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	policy, err := s.Service.GetRetentionPolicy(r.Context(), scope.TenantID)
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
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.EvaluateRetention(r.Context(), scope.TenantID, time.Time{})
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) listAdminActions(w http.ResponseWriter, r *http.Request) {
	claims, err := s.requireAdminActionRead(r)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	scope, err := s.resolveScope(r, claims, ScopeAllTenants)
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
	items, err := s.Service.ListAdminActions(r.Context(), scope.TenantID, claims.Platform || claims.CrossTenantAuditRead, limit)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}
