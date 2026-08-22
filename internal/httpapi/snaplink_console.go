package httpapi

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

var (
	consoleAuditFrom = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	consoleAuditTo   = time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)
)

func (s *Server) snaplinkConsoleQuery(
	w http.ResponseWriter, r *http.Request,
) (claims authClaims, tenantID string, query domain.Query, ok bool) {
	verified, err := s.require(r, "audit:event:read")
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return authClaims{}, "", domain.Query{}, false
	}
	tenantID, err = s.tenantFor(r, verified)
	if err != nil {
		s.writeError(w, r, statusForError(err), err)
		return authClaims{}, "", domain.Query{}, false
	}
	query, err = parseSnaplinkConsoleQuery(r)
	if err != nil {
		s.writeError(w, r, http.StatusBadRequest, err)
		return authClaims{}, "", domain.Query{}, false
	}
	return authClaims{Subject: verified.Subject}, tenantID, query, true
}

// authClaims keeps the compatibility helper's return surface deliberately
// narrow: handlers need only the already-verified subject.
type authClaims struct{ Subject string }

func parseSnaplinkConsoleQuery(r *http.Request) (domain.Query, error) {
	values := r.URL.Query()
	from, err := optionalConsoleTime(values.Get("since"), consoleAuditFrom)
	if err != nil {
		return domain.Query{}, err
	}
	to, err := optionalConsoleTime(values.Get("until"), consoleAuditTo)
	if err != nil {
		return domain.Query{}, err
	}
	pageSize := domain.DefaultPageSize
	if raw := values.Get("limit"); raw != "" {
		pageSize, err = strconv.Atoi(raw)
		if err != nil || pageSize <= 0 || pageSize > domain.MaxPageSize {
			return domain.Query{}, fmt.Errorf("%w: invalid limit", domain.ErrInvalid)
		}
	}
	if offset := values.Get("offset"); offset != "" && offset != "0" {
		return domain.Query{}, fmt.Errorf("%w: offset pagination is unsupported; use cursor", domain.ErrInvalid)
	}
	eventType := values.Get("event_type")
	if eventType == "" {
		eventType = values.Get("type")
	}
	return domain.Query{
		From: from, To: to, EventType: eventType,
		SourceSystem: values.Get("client_id"), ActorID: values.Get("actor_id"),
		TraceID: values.Get("trace_id"), Outcome: values.Get("outcome"),
		Cursor: values.Get("cursor"), PageSize: pageSize,
	}, nil
}

func optionalConsoleTime(raw string, fallback time.Time) (time.Time, error) {
	if raw == "" {
		return fallback, nil
	}
	if unix, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return time.Unix(unix, 0).UTC(), nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid audit time", domain.ErrInvalid)
	}
	return parsed.UTC(), nil
}
