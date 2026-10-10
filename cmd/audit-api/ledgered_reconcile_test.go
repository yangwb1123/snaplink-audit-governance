package main

import (
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

type loopPublicationRecorder struct {
	mu     sync.Mutex
	events []domain.Event
}

func (p *loopPublicationRecorder) Publish(_ context.Context, event domain.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
	return nil
}

func reconcileLoopService(t *testing.T) (*service.Service, *loopPublicationRecorder) {
	t.Helper()
	st, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	publisher := &loopPublicationRecorder{}
	svc, err := service.New(st, service.Config{AllowDevSecrets: true, LedgeredPublisher: publisher})
	if err != nil {
		t.Fatal(err)
	}
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		if err := svc.CreateTenant(context.Background(), "test", domain.Tenant{ID: tenantID, Name: tenantID}); err != nil {
			t.Fatal(err)
		}
		if err := st.UpdateTenant(context.Background(), tenantID, store.HotFirst, func(view *store.TenantView) error {
			return view.Ledger.SetReceipt(domain.EventReceipt{TenantID: tenantID, EventID: "unavailable-original", StreamID: tenantID + ":source:crm", Sequence: 1, Hash: "hash", Status: domain.StatusLedgered})
		}); err != nil {
			t.Fatal(err)
		}
	}
	return svc, publisher
}

func TestLedgeredReconcilerRotatesTenantsAndWraps(t *testing.T) {
	svc, _ := reconcileLoopService(t)
	r := &ledgeredReconciler{}
	for _, want := range []string{"tenant-a", "tenant-b", "tenant-a"} {
		result, err := r.next(context.Background(), svc)
		if err != nil || r.lastTenant != want || result.Scanned != 1 || result.Failed != 1 {
			t.Fatalf("tenant=%s want=%s result=%+v err=%v", r.lastTenant, want, result, err)
		}
		if r.cursors[want] != (store.LedgerPageCursor{}) {
			t.Fatal("completed scan must wrap to zero and revisit failed evidence")
		}
	}
	// A failed pass also rotates rather than holding the entire service on
	// one unavailable tenant. The tenant retains its cursor for later retry.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := r.next(ctx, svc)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled pass err=%v", err)
	}
}

func TestLedgeredOutboxLoopRunsActiveReconciliationAtStartup(t *testing.T) {
	svc, _ := reconcileLoopService(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := make(chan struct{}, 1)
	svc.Config.Logf = func(string, ...any) {
		select {
		case observed <- struct{}{}:
		default:
		}
	}
	done := make(chan struct{})
	go func() {
		runLedgeredOutboxLoop(ctx, svc, log.New(io.Discard, "", 0))
		close(done)
	}()
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("startup did not scan ledger evidence without a client retry")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("maintenance loop ignored cancellation")
	}
}
