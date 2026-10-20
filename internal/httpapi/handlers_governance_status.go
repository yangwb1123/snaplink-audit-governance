package httpapi

import (
	"net/http"

	"github.com/snaplink/audit-governance/internal/service"
)

func (s *Server) getGovernanceStatus(w http.ResponseWriter, r *http.Request) {
	// Authorization errors also must not be cached across identity changes.
	w.Header().Set("Cache-Control", "no-store")
	claims, err := s.requirePermissions(r, []string{
		"audit:export:create", "audit:operation:read", "audit:legal_hold:manage", "audit:policy:read",
	}, false)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	status, err := s.Service.GovernanceStatus(r.Context(), scope.TenantID, claims.Subject, service.GovernanceStatusAccess{
		Exports:          claims.Allows("audit:export:create"),
		Restores:         claims.Allows("audit:operation:read"),
		RestoreDecisions: claims.Allows("audit:legal_hold:manage"),
		LegalHolds:       claims.Allows("audit:legal_hold:manage"),
		Retention:        claims.Allows("audit:policy:read"),
	})
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}
