package main

import (
	"context"
	"sort"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

// governancePassTenants preserves the pass's existing withdrawal protocol:
// when a backend still returns its catalog, the first tenant attempts the
// original bounded recovery/checkpoint/archive/retention sequence before the
// between-tenant abort. Public ListTenants deliberately has a stricter gate.
// Keep the caller's canceled/deadlined context and every actual backend error;
// never detach, retry, cache, or fabricate a catalog to reach those steps.
func governancePassTenants(ctx context.Context, svc *service.Service) ([]domain.Tenant, error) {
	var tenants []domain.Tenant
	if err := svc.Store.ReadControl(ctx, func(data *store.Snapshot) error {
		for _, tenant := range data.Tenants {
			tenants = append(tenants, tenant)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(tenants, func(i, j int) bool { return tenants[i].ID < tenants[j].ID })
	return tenants, nil
}
