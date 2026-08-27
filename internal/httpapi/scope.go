package httpapi

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// ScopePolicy describes how an HTTP operation obtains its tenant boundary.
// The policy is transport metadata; service methods still enforce a non-empty
// tenant for every tenant-specific operation.
type ScopePolicy string

const (
	ScopeScopedQuery ScopePolicy = "scoped-query"
	ScopeScopedBody  ScopePolicy = "scoped-body"
	ScopeAllTenants  ScopePolicy = "all-tenants"
)

// TenantScope is the effective scope passed to an application service. An
// empty TenantID is legal only when AllTenants is true (the explicit Console
// all-tenant read path).
type TenantScope struct {
	TenantID   string
	AllTenants bool
}

// resolveScope resolves query-selected and approved all-tenant operations.
// tenantFor is the sole raw tenant_id query-selector boundary. Platform
// claims never contribute a fallback tenant: a platform caller must select a
// tenant on scoped operations, while an all-tenant operation may omit it.
func (s *Server) resolveScope(r *http.Request, claims auth.Claims, policy ScopePolicy) (TenantScope, error) {
	tenantID, err := s.tenantFor(r, claims)
	if err != nil {
		return TenantScope{}, err
	}
	if policy == ScopeAllTenants {
		return TenantScope{TenantID: tenantID, AllTenants: tenantID == ""}, nil
	}
	if policy != ScopeScopedQuery {
		return TenantScope{}, fmt.Errorf("%w: unsupported tenant scope policy", domain.ErrInvalid)
	}
	if tenantID == "" {
		return TenantScope{}, fmt.Errorf("%w: tenant_id is required for this operation", domain.ErrInvalid)
	}
	return TenantScope{TenantID: tenantID}, nil
}

// resolveBodyScope resolves an event-ingest body selector. Tenant tokens use
// their signed claim and no-tenant service credentials retain the existing
// source-registration resolution in Service.Ingest. A platform token must
// provide a tenant_id in every event body.
func (s *Server) resolveBodyScope(r *http.Request, claims auth.Claims, bodyTenant string) (TenantScope, error) {
	// Consume and validate a query selector even though body scope is
	// authoritative. This keeps repeated/malformed selectors fail-closed and
	// prevents an undocumented query from becoming a second precedence rule.
	queryTenant, present, err := queryTenantSelector(r)
	if err != nil {
		return TenantScope{}, err
	}
	if claims.Platform && present && queryTenant != bodyTenant {
		return TenantScope{}, fmt.Errorf("%w: body tenant_id must match query tenant_id", domain.ErrInvalid)
	}
	return bodyScopeFromTenant(claims, bodyTenant)
}

func bodyScopeFromTenant(claims auth.Claims, bodyTenant string) (TenantScope, error) {
	if bodyTenant != "" {
		if err := store.ValidTenantID(bodyTenant); err != nil {
			return TenantScope{}, err
		}
	}
	if claims.Platform {
		if bodyTenant == "" {
			return TenantScope{}, fmt.Errorf("%w: tenant_id is required in the request body", domain.ErrInvalid)
		}
		return TenantScope{TenantID: bodyTenant}, nil
	}
	if claims.TenantID != "" {
		if err := store.ValidTenantID(claims.TenantID); err != nil {
			return TenantScope{}, err
		}
		if bodyTenant != "" && bodyTenant != claims.TenantID {
			return TenantScope{}, fmt.Errorf("%w: envelope tenant_id %q does not match the signed tenant %q", domain.ErrTenantMismatch, bodyTenant, claims.TenantID)
		}
		return TenantScope{TenantID: claims.TenantID}, nil
	}
	// Credentials without a signed tenant retain the existing source
	// registration resolution. A body value is not trusted to choose a
	// tenant; Service.Ingest receives an empty hint and derives a unique
	// tenant from the authenticated client and source.
	return TenantScope{}, nil
}

func (s *Server) resolveBatchBodyScopes(r *http.Request, claims auth.Claims, events []domain.Event) ([]TenantScope, error) {
	// Resolve the request selector once, then preflight every event. Ingest is
	// not entered until all body selectors are valid, preventing a missing
	// platform selector in a later member from creating a valid prefix.
	queryTenant, present, err := queryTenantSelector(r)
	if err != nil {
		return nil, err
	}
	scopes := make([]TenantScope, len(events))
	for index, event := range events {
		if claims.Platform && present && queryTenant != event.TenantID {
			return nil, fmt.Errorf("%w: body tenant_id must match query tenant_id", domain.ErrInvalid)
		}
		scope, err := bodyScopeFromTenant(claims, event.TenantID)
		if err != nil {
			return nil, err
		}
		scopes[index] = scope
	}
	return scopes, nil
}

// resolveQueryBodyScope resolves a query-selected resource write and checks a
// supplied body tenant only as a consistency assertion. Platform callers use
// the query selector as authority; tenant-token callers retain compatibility
// by normalizing the body tenant to their signed claim.
func (s *Server) resolveQueryBodyScope(r *http.Request, claims auth.Claims, bodyTenant string) (TenantScope, error) {
	scope, err := s.resolveScope(r, claims, ScopeScopedQuery)
	if err != nil {
		return TenantScope{}, err
	}
	if bodyTenant == "" {
		return scope, nil
	}
	if err := store.ValidTenantID(bodyTenant); err != nil {
		return TenantScope{}, err
	}
	if claims.Platform && bodyTenant != scope.TenantID {
		return TenantScope{}, fmt.Errorf("%w: body tenant_id must match query tenant_id", domain.ErrInvalid)
	}
	return scope, nil
}

// queryTenantSelector parses the raw query instead of using URL.Values.Get.
// ParseQuery preserves whether a selector was supplied, rejects malformed
// escapes, and lets the boundary reject repeated selectors rather than
// silently choosing the first value.
func queryTenantSelector(r *http.Request) (string, bool, error) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return "", false, fmt.Errorf("%w: malformed query", domain.ErrInvalid)
	}
	selectors, present := values["tenant_id"]
	if len(selectors) > 1 {
		return "", false, fmt.Errorf("%w: tenant_id must occur once", domain.ErrInvalid)
	}
	if !present {
		return "", false, nil
	}
	if len(selectors) == 0 {
		return "", true, nil
	}
	if selectors[0] != "" {
		if err := store.ValidTenantID(selectors[0]); err != nil {
			return "", false, err
		}
	}
	return selectors[0], true, nil
}
