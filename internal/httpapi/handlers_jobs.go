package httpapi

import (
	"fmt"
	"net/http"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func (s *Server) listExports(w http.ResponseWriter, r *http.Request) {
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
	query, err := parseJobQuery(r)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	page, err := s.Service.ListExports(r.Context(), scope.TenantID, claims.Subject, query)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) listRestores(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
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
	query, err := parseJobQuery(r)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	if query.View == "actionable" && !claims.Allows("audit:legal_hold:manage") {
		s.writeError(w, r, http.StatusForbidden, domain.ErrForbidden)
		return
	}
	page, err := s.Service.ListRestores(r.Context(), scope.TenantID, claims.Subject, query)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func parseJobQuery(r *http.Request) (domain.JobQuery, error) {
	values := r.URL.Query()
	for _, key := range []string{"status", "requested_by", "operation_id", "view", "from", "to", "cursor", "page_size"} {
		if len(values[key]) > 1 {
			return domain.JobQuery{}, fmt.Errorf("%w: repeated job filter", domain.ErrInvalid)
		}
	}
	pageSize, cursor, err := parsePageOptions(values)
	if err != nil {
		return domain.JobQuery{}, err
	}
	var from, to time.Time
	if values.Has("from") {
		from, err = parseTime(values.Get("from"))
		if err != nil {
			return domain.JobQuery{}, err
		}
	}
	if values.Has("to") {
		to, err = parseTime(values.Get("to"))
		if err != nil {
			return domain.JobQuery{}, err
		}
	}
	return domain.JobQuery{Status: values.Get("status"), RequestedBy: values.Get("requested_by"), OperationID: values.Get("operation_id"), View: values.Get("view"), From: from, To: to, PageSize: pageSize, Cursor: cursor}, nil
}
