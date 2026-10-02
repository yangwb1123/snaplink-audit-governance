package httpapi

// Event surface: ingest (single and batch), event and receipt reads, the
// Snaplink compatibility queries, and the operation / aggregate timelines
// with their no-side-effect replay. The route table in server.go binds these
// handlers; the authorization and status mapping are shared.

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
)

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
	if err := domain.ValidateEventCaps(event); err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	scope, err := s.resolveBodyScope(r, claims, event.TenantID)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	principal := domain.IngestPrincipal{ClientID: claims.ClientID}
	// F1: the ingest commit must survive client disconnect — the durable
	// write is detached from r.Context() cancellation (values are kept) so a
	// dropped connection can never roll back a ledgered event. The worker's
	// signal-driven context stays cancellable: server-initiated stop is a
	// legitimate abort.
	receipt, err := s.Service.Ingest(context.WithoutCancel(r.Context()), scope.TenantID, principal, event, r.URL.Query().Get("wait_for"))
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
	if err := domain.CheckCount("events", len(request.Events), domain.MaxBatchEvents); err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	if request.WaitFor == "" {
		request.WaitFor = r.URL.Query().Get("wait_for")
	}
	scopes, err := s.resolveBatchBodyScopes(r, claims, request.Events)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	receipts := make([]domain.EventReceipt, 0, len(request.Events))
	principal := domain.IngestPrincipal{ClientID: claims.ClientID}
	commitCtx := context.WithoutCancel(r.Context())
	for index, event := range request.Events {
		if capErr := domain.ValidateEventCaps(event); capErr != nil {
			receipts = append(receipts, domain.EventReceipt{})
			partialStatus := statusForError(capErr)
			if partialStatus >= 500 {
				s.errorCount.Add(1)
			}
			writeJSON(w, partialStatus, map[string]any{"receipts": receipts, "error": errorBody(partialStatus, capErr, r)})
			return
		}
		receipt, ingestErr := s.Service.Ingest(commitCtx, scopes[index].TenantID, principal, event, request.WaitFor)
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
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	event, err := s.Service.GetEvent(r.Context(), scope.TenantID, claims.Subject, r.PathValue("eventID"))
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
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	receipt, err := s.Service.GetReceipt(r.Context(), scope.TenantID, claims.Subject, r.PathValue("eventID"))
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
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	result, err := s.Service.QueryEvents(r.Context(), scope.TenantID, claims.Subject, query)
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

func (s *Server) querySnaplinkAuditEvents(w http.ResponseWriter, r *http.Request) {
	claims, tenantID, query, ok := s.snaplinkConsoleQuery(w, r)
	if !ok {
		return
	}
	s.queryCount.Add(1)
	result, err := s.Service.QueryConsoleEvents(r.Context(), tenantID, claims.Subject, query)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	for index := range result.Items {
		stripped, stripErr := security.StripSearchDigests(result.Items[index].Payload)
		if stripErr != nil {
			s.writeError(w, r, http.StatusInternalServerError, stripErr)
			return
		}
		result.Items[index].Payload = stripped
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"events": result.Items, "count": result.Count, "next_cursor": result.NextCursor,
	})
}

func (s *Server) querySnaplinkAuditFacets(w http.ResponseWriter, r *http.Request) {
	claims, tenantID, query, ok := s.snaplinkConsoleQuery(w, r)
	if !ok {
		return
	}
	s.queryCount.Add(1)
	result, err := s.Service.QueryEventFacets(r.Context(), tenantID, claims.Subject, query)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"facets": result})
}

func (s *Server) getOperation(w http.ResponseWriter, r *http.Request) {
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
	result, err := s.Service.Operation(r.Context(), scope.TenantID, claims.Subject, r.PathValue("operationID"))
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
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	pageSize, cursor, err := parseTimelinePage(r)
	if err != nil {
		s.writeError(w, r, http.StatusBadRequest, err)
		return
	}
	result, err := s.Service.OperationTimelinePage(r.Context(), scope.TenantID, claims.Subject, r.PathValue("operationID"), pageSize, cursor)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	// 与 getEvent/queryEvents 相同：时间线响应剥离搜索摘要（深拷贝，绝不
	// 改动存储快照共享的 map）。剥离失败时 500 —— 宁可失败也不泄漏。
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

func (s *Server) replayOperation(w http.ResponseWriter, r *http.Request) {
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
	result, err := s.Service.ReplayOperation(r.Context(), scope.TenantID, claims.Subject, r.PathValue("operationID"))
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
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	pageSize, cursor, err := parseTimelinePage(r)
	if err != nil {
		s.writeError(w, r, http.StatusBadRequest, err)
		return
	}
	result, err := s.Service.AggregateTimelinePage(r.Context(), scope.TenantID, claims.Subject, r.PathValue("aggregateType"), r.PathValue("aggregateID"), pageSize, cursor)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	// 与 getEvent/queryEvents 相同：聚合时间线响应剥离搜索摘要（深拷贝，
	// 绝不改动存储快照共享的 map）。剥离失败时 500 —— 宁可失败也不泄漏。
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
