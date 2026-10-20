package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Faults are at the actual progress Save and independent read-trail append.
// Unknown really commits the new prefix before reporting an error.
type generationBackend struct {
	*planBackend
	failChunk, unknownChunk, failGenerationRead atomic.Bool
	generationReadEntered                       chan context.Context
	generationReadRelease                       chan struct{}
}

func (b *generationBackend) Save(ctx context.Context, data *store.Snapshot) error {
	b.mu.Lock()
	progress := false
	for id, plan := range data.ExportPlans {
		if len(plan.Chunks) > len(b.data.ExportPlans[id].Chunks) {
			progress = true
		}
	}
	b.mu.Unlock()
	if progress && b.failChunk.Load() {
		return errors.New("injected generation progress Save failure")
	}
	if err := b.planBackend.Save(ctx, data); err != nil {
		return err
	}
	if progress && b.unknownChunk.Load() {
		return errors.New("generation progress Save result unknown")
	}
	return nil
}

func (b *generationBackend) AppendAdminFact(ctx context.Context, fact domain.AdminAction, cap int) error {
	if fact.Detail == "stage=generating verification" {
		if b.failGenerationRead.Load() {
			return errors.New("injected generation read audit failure")
		}
		if b.generationReadEntered != nil {
			b.generationReadEntered <- ctx
			select {
			case <-b.generationReadRelease:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return b.planBackend.AppendAdminFact(ctx, fact, cap)
}

func seedGenerationEvents(t *testing.T, svc *Service, first, through int) {
	t.Helper()
	for i := first; i < through; i++ {
		event := testEvent(fmt.Sprintf("generation-%04d", i), "generation-operation", pinnedNow.Add(10*time.Second+time.Duration(i)*time.Millisecond))
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived); err != nil {
			t.Fatal("could not commit generation source fixture")
		}
	}
}

func newGenerationService(t *testing.T) (*Service, *generationBackend, *checkpointArchive) {
	t.Helper()
	svc, base, objects := newPlanService(t)
	backend := &generationBackend{planBackend: base}
	svc.Store = store.NewWithBackend(backend)
	seedGenerationEvents(t, svc, 1, store.ExportGenerationPageEvents+1)
	return svc, backend, objects
}

func selectedGenerationPlan(t *testing.T, svc *Service) exportClaim {
	t.Helper()
	claim, won, err := svc.claimPendingExport(testCtx, "checkpoint-job")
	if err != nil || !won {
		t.Fatal("generation fixture claim failed")
	}
	claim, events, ready := svc.exportExecutionEvidence(exportClaimContext(testCtx, claim), claim)
	if !ready || events != nil || claim.Plan.Version != 2 || len(claim.Plan.Members) != store.ExportGenerationPageEvents+1 {
		t.Fatal("multipage selection did not persist a v2 plan before generating bytes")
	}
	return claim
}

func leaveGeneratedPage(t *testing.T, svc *Service) exportClaim {
	t.Helper()
	claim := selectedGenerationPlan(t, svc)
	claim, plain, err := svc.generateExportPage(exportClaimContext(testCtx, claim), claim)
	if err != nil || len(plain) == 0 || len(claim.Plan.Chunks) != 1 || exportGeneratedMembers(claim.Plan) != store.ExportGenerationPageEvents {
		t.Fatal("actual page Put and progress Save did not commit the first prefix")
	}
	if !svc.validExportPlan(claim.ExportJob, leaseSnapshot(t, svc).ExportPlans[claim.ID], svc.Now()) {
		t.Fatal("generated page is not durable authenticated progress")
	}
	return claim
}

type generationArchive struct {
	archive.Store
	forbiddenSources                            map[string]bool
	sourceGets, chunkGets, chunkPuts, finalPuts atomic.Int64
	putEntered                                  chan leasePutObservation
	getEntered                                  chan context.Context
	release                                     chan struct{}
	ignoreCancel                                bool
}

func (a *generationArchive) Put(ctx context.Context, key string, data []byte) error {
	if strings.HasPrefix(key, "export-chunks/") {
		a.chunkPuts.Add(1)
		if a.putEntered != nil {
			a.putEntered <- leasePutObservation{ctx: ctx, key: key}
			if a.ignoreCancel {
				<-a.release
				return a.Store.Put(context.Background(), key, data)
			}
			select {
			case <-a.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	} else if strings.HasPrefix(key, "exports/") {
		a.finalPuts.Add(1)
	}
	return a.Store.Put(ctx, key, data)
}

func (a *generationArchive) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasPrefix(key, "events/") {
		a.sourceGets.Add(1)
		if a.forbiddenSources[key] {
			return nil, errors.New("attempt to regenerate an already committed page or reselect a late member")
		}
	}
	if strings.HasPrefix(key, "export-chunks/") {
		a.chunkGets.Add(1)
		if a.getEntered != nil {
			a.getEntered <- ctx
			select {
			case <-a.release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return a.Store.Get(ctx, key)
}

func generatedSourceKeys(plan store.ExportPlan) map[string]bool {
	keys := map[string]bool{}
	for _, member := range plan.Members[:exportGeneratedMembers(plan)] {
		keys[eventArchiveKey(plan.TenantID, member.StreamID, member.Sequence, member.EventID)] = true
	}
	return keys
}
