package service

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Instance-local faults at actual selected-stage Save and read-trail append.
// The unknown-result fault really commits before returning its error.
type planBackend struct {
	*checkpointBackend
	failPlan    atomic.Bool
	unknownPlan atomic.Bool
	failRead    atomic.Bool
	readEntered chan context.Context
	readRelease chan struct{}
}

func (b *planBackend) Save(ctx context.Context, data *store.Snapshot) error {
	b.mu.Lock()
	planning := false
	for id := range data.ExportPlans {
		if _, exists := b.data.ExportPlans[id]; !exists {
			planning = true
		}
	}
	b.mu.Unlock()
	if planning && b.failPlan.Load() {
		return errors.New("injected selected-stage Save failure")
	}
	if err := b.checkpointBackend.Save(ctx, data); err != nil {
		return err
	}
	if planning && b.unknownPlan.Load() {
		return errors.New("selected-stage Save result unknown")
	}
	return nil
}

func (b *planBackend) AppendAdminFact(ctx context.Context, fact domain.AdminAction, cap int) error {
	if fact.Detail == "stage=selected verification" {
		if b.failRead.Load() {
			return errors.New("injected selected-stage audit failure")
		}
		if b.readEntered != nil {
			b.readEntered <- ctx
			select {
			case <-b.readRelease:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return b.checkpointBackend.AppendAdminFact(ctx, fact, cap)
}

func newPlanService(t *testing.T) (*Service, *planBackend, *checkpointArchive) {
	t.Helper()
	svc, base, objects := newCheckpointService(t)
	backend := &planBackend{checkpointBackend: base}
	svc.Store = store.NewWithBackend(backend)
	return svc, backend, objects
}

func leaveSelectedPlan(t *testing.T, svc *Service) exportClaim {
	t.Helper()
	claim, won, err := svc.claimPendingExport(testCtx, "checkpoint-job")
	if err != nil || !won {
		t.Fatal("initial executor did not claim selected-stage fixture")
	}
	claim, events, ready := svc.exportExecutionEvidence(exportClaimContext(testCtx, claim), claim)
	if !ready || len(events) != 1 || len(claim.Plan.Members) != 1 {
		t.Fatal("actual executor did not persist one selected member before generation")
	}
	data := leaseSnapshot(t, svc)
	if !svc.validExportPlan(data.Exports[claim.ID], data.ExportPlans[claim.ID], svc.Now()) || len(data.ExportCheckpoints) != 0 {
		t.Fatal("selected-stage fixture was not a durable authenticated plan")
	}
	return claim
}

// A genuine source Get boundary, not a sleep during fixture preparation.
// The forbidden key proves a restart never reselects later members when there
// is no current Hold requiring an independent historical evidence scan.
type planSourceArchive struct {
	archive.Store
	forbidden  string
	entered    chan context.Context
	release    chan struct{}
	ignoreCtx  bool
	sourceGets atomic.Int64
}

func (a *planSourceArchive) Get(ctx context.Context, key string) ([]byte, error) {
	if !strings.HasPrefix(key, "events/") {
		return a.Store.Get(ctx, key)
	}
	a.sourceGets.Add(1)
	if key == a.forbidden {
		return nil, errors.New("unexpected selection of a later member")
	}
	if a.entered != nil {
		a.entered <- ctx
		if a.ignoreCtx {
			<-a.release
		} else {
			select {
			case <-a.release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return a.Store.Get(ctx, key)
}
