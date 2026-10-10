package main

import (
	"context"
	"sort"

	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

// Each timer pass admits one bounded page from one tenant. Volatile cursors are
// one per catalog tenant (not per event) and wrap after every sweep. Startup
// starts at zero; persistent publication confirmations prevent historical
// deliveries from being amplified. A failed tenant never blocks another one.
type ledgeredReconciler struct {
	cursors    map[string]store.LedgerPageCursor
	lastTenant string
}

func (r *ledgeredReconciler) next(ctx context.Context, svc *service.Service) (service.LedgeredReconcileResult, error) {
	if svc == nil || svc.Store == nil || svc.Config.LedgeredPublisher == nil {
		return service.LedgeredReconcileResult{Done: true}, nil
	}
	var tenants []string
	if err := svc.Store.ReadControl(ctx, func(data *store.Snapshot) error {
		for id := range data.Tenants {
			tenants = append(tenants, id)
		}
		return nil
	}); err != nil {
		return service.LedgeredReconcileResult{}, err
	}
	if r.cursors == nil {
		r.cursors = map[string]store.LedgerPageCursor{}
	}
	known := make(map[string]bool, len(tenants))
	for _, id := range tenants {
		known[id] = true
	}
	for id := range r.cursors {
		if !known[id] {
			delete(r.cursors, id)
		}
	}
	if len(tenants) == 0 {
		return service.LedgeredReconcileResult{Done: true}, nil
	}
	sort.Strings(tenants)
	index := sort.Search(len(tenants), func(i int) bool { return tenants[i] > r.lastTenant })
	if index == len(tenants) {
		index = 0
	}
	tenantID := tenants[index]
	r.lastTenant = tenantID // Advance even on error: avoid cross-tenant HOL.
	result, err := svc.ReconcileLedgeredPublications(ctx, tenantID, r.cursors[tenantID], store.MaxLedgerPageSize)
	if err == nil {
		r.cursors[tenantID] = result.Next
		if result.Done {
			r.cursors[tenantID] = store.LedgerPageCursor{}
		}
	}
	return result, err
}
