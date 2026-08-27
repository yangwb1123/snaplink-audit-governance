package service

import (
	"fmt"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// requireTenantScope is the application-layer guard for tenant-specific use
// cases. Empty scope is never a wildcard here; all-tenant behavior belongs to
// explicitly named Console/admin read methods only.
func requireTenantScope(tenantID string) error {
	if tenantID == "" {
		return fmt.Errorf("%w: tenant_id is required", domain.ErrInvalid)
	}
	return store.ValidTenantID(tenantID)
}

func requireOptionalTenantScope(tenantID string) error {
	if tenantID == "" {
		return nil
	}
	return store.ValidTenantID(tenantID)
}
