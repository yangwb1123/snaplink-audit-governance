package main

import (
	"context"
	"errors"
	"io"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestHotCommitLoopRecoversWithNoPublisherOrClientRetry(t *testing.T) {
	var armed atomic.Bool
	appended := make(chan struct{})
	var once sync.Once
	st, err := store.OpenWithLedgerWriteFault(filepath.Join(t.TempDir(), "state.json"), func() error {
		if armed.Load() {
			return errors.New("injected append error")
		}
		once.Do(func() { close(appended) })
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	svc, err := service.New(st, service.Config{SigningSecret: "test-secret", AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant(context.Background(), "test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true, EventsPerSecond: 1000, Burst: 1000}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource(context.Background(), "test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema(context.Background(), "test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource"}}); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	event := domain.Event{EventID: "interrupted", TenantID: "tenant-a", SourceSystem: "crm", SchemaID: "audit.event", SchemaVersion: 1, EventType: "audit.event", OccurredAt: time.Now().UTC(), Actor: domain.Actor{ID: "operator"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "idem-one", Payload: map[string]any{"resource": "one"}}
	principal := domain.IngestPrincipal{ClientID: "crm"}
	if _, err := svc.Ingest(context.Background(), "tenant-a", principal, event, domain.StatusLedgered); err == nil {
		t.Fatal("fault not reached")
	}
	armed.Store(false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); runHotCommitRecoveryLoop(ctx, svc, log.New(io.Discard, "", 0)) }()
	// Synchronize at the real cold append boundary, not disk preparation time.
	select {
	case <-appended:
	case <-time.After(10 * time.Second):
		t.Fatal("background append not reached")
	}
	if err := st.ReadTenant(context.Background(), "tenant-a", func(view *store.TenantView) error {
		receipt, ok := view.Ledger.Receipt(store.EventKey("tenant-a", event.EventID))
		if !ok || receipt.Sequence != 1 || receipt.Status != domain.StatusLedgered {
			t.Fatal("missing original committed receipt")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop ignored cancellation")
	}
	if svc.Config.LedgeredPublisher != nil || svc.HotCommitRecovered() != 1 {
		t.Fatal("recovery relied on publication")
	}
}
