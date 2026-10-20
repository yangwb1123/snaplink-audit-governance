package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/snaplink/audit-governance/internal/domain"
)

// Actions do not accept replacement identity, filters, owner, status or lease
// metadata. Empty body, null and {} mean the same empty command.
func parseExportAction(w http.ResponseWriter, r *http.Request) error {
	var command map[string]json.RawMessage
	if err := decodeBody(w, r, &command); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: export action requires an empty JSON object", domain.ErrInvalid)
	}
	if len(command) != 0 {
		return fmt.Errorf("%w: export action does not accept fields", domain.ErrInvalid)
	}
	return nil
}

func (s *Server) cancelExport(w http.ResponseWriter, r *http.Request) {
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
	if err := parseExportAction(w, r); err != nil {
		s.writeError(w, r, http.StatusBadRequest, err)
		return
	}
	result, err := s.Service.CancelExport(r.Context(), scope.TenantID, claims.Subject, r.PathValue("jobID"))
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) retryExport(w http.ResponseWriter, r *http.Request) {
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
	if err := parseExportAction(w, r); err != nil {
		s.writeError(w, r, http.StatusBadRequest, err)
		return
	}
	result, err := s.Service.RetryExport(r.Context(), scope.TenantID, claims.Subject, r.PathValue("jobID"))
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}
