package store

// Regression coverage for the control-plane delta a tenant closure writes.
//
// The ingest publication intent is created on view.Global inside the
// UpdateTenant closure, so the intent is only as durable as that commit. These
// tests pin the three things that make that safe: every backend that can serve
// UpdateTenant persists the closure's control delta with the same commit, a
// control-write failure after the hot document and the cold ledger are durable
// leaves exactly that window behind (and nothing worse), and re-running the
// closure against a fresh view — the PostgreSQL ErrSnapshotConflict retry —
// still lands the delta.

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

func controlDeltaEvent(tenantID, eventID string) domain.Event {
	return domain.Event{
		EventID: eventID, TenantID: tenantID, SourceSystem: "crm", EventType: "audit.event",
		StreamID: "tenant-a:source:crm", Sequence: 1, Hash: "hash-1",
		Payload: map[string]any{"value": 1},
	}
}

// memoryControlDeltaBackend is a minimal in-memory Backend for the v1
// single-document path (NewWithBackend), which serves UpdateTenant through
// updateLegacyTenant instead of the split store.
type memoryControlDeltaBackend struct{ data *Snapshot }

func (b *memoryControlDeltaBackend) Load(ctx context.Context) (*Snapshot, error) {
	return cloneSnapshot(b.data)
}

func (b *memoryControlDeltaBackend) LoadForUpdate(ctx context.Context) (*Snapshot, error) {
	return cloneSnapshot(b.data)
}

func (b *memoryControlDeltaBackend) Save(ctx context.Context, data *Snapshot) error {
	clone, err := cloneSnapshot(data)
	if err != nil {
		return err
	}
	b.data = clone
	return nil
}

func TestUpdateTenantPersistsControlDeltaFromClosure(t *testing.T) {
	queue := func(view *TenantView, tenantID, eventID string) {
		view.Global.LedgeredOutbox[EventKey(tenantID, eventID)] = controlDeltaEvent(tenantID, eventID)
	}
	requireIntent := func(t *testing.T, read func(context.Context, func(*Snapshot) error) error, tenantID, eventID, backend string) {
		t.Helper()
		key := EventKey(tenantID, eventID)
		if err := read(testCtx, func(data *Snapshot) error {
			if _, ok := data.LedgeredOutbox[key]; !ok {
				t.Errorf("%s: control delta %q missing after a committed UpdateTenant", backend, key)
			}
			return nil
		}); err != nil {
			t.Errorf("%s: %v", backend, err)
		}
	}

	t.Run("file-split", func(t *testing.T) {
		st, err := Open(filepath.Join(t.TempDir(), "state.json"))
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		if err := st.UpdateTenant(testCtx, "tenant-a", HotFirst, func(view *TenantView) error {
			queue(view, "tenant-a", "delta-file")
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		requireIntent(t, st.Read, "tenant-a", "delta-file", "file-split Read")
		requireIntent(t, st.ReadControl, "tenant-a", "delta-file", "file-split ReadControl")
	})

	t.Run("legacy-tenant-api", func(t *testing.T) {
		// Defensive: ingestTenant is gated on HotCold(), but the v1 tenant API
		// must not silently drop a control delta written by a closure.
		st := NewWithBackend(&memoryControlDeltaBackend{data: NewSnapshot()})
		if err := st.UpdateTenant(testCtx, "tenant-a", HotFirst, func(view *TenantView) error {
			queue(view, "tenant-a", "delta-legacy")
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		requireIntent(t, st.Read, "tenant-a", "delta-legacy", "legacy-tenant-api")
	})

	t.Run("postgres-split", func(t *testing.T) {
		db := newHotColdPostgresTestDB(t)
		st, err := OpenPostgres(db)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.UpdateTenant(testCtx, "tenant-a", HotFirst, func(view *TenantView) error {
			queue(view, "tenant-a", "delta-pg")
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		requireIntent(t, st.Read, "tenant-a", "delta-pg", "postgres-split Read")
		requireIntent(t, st.ReadControl, "tenant-a", "delta-pg", "postgres-split ReadControl")
	})
}

func TestOpenWithControlWriteFaultRejectsNilHook(t *testing.T) {
	if _, err := OpenWithControlWriteFault(filepath.Join(t.TempDir(), "state.json"), nil); err == nil {
		t.Fatal("a nil control write fault must be rejected, not silently installed as inert")
	}
}

// TestUpdateTenantControlWriteFaultLosesOnlyControlDelta pins the residual
// window the service repair compensates for: a control write that fails after
// the hot document and the cold ledger are durable destroys the intent and
// nothing else. Before the single-commit change this state was unreachable —
// the intent was written by a later, separate store operation — which is why
// this test is the real guard for "one store operation carries both".
func TestUpdateTenantControlWriteFaultLosesOnlyControlDelta(t *testing.T) {
	var armed atomic.Bool
	armed.Store(true)
	st, err := OpenWithControlWriteFault(filepath.Join(t.TempDir(), "state.json"), func() error {
		if armed.Load() {
			return errors.New("injected control write failure")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Update(testCtx, func(data *Snapshot) error {
		data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	key := EventKey("tenant-a", "fault-delta")
	receipt := domain.EventReceipt{EventID: "fault-delta", TenantID: "tenant-a", Status: domain.StatusLedgered, StreamID: "tenant-a:source:crm", Sequence: 1, Hash: "hash-1"}
	err = st.UpdateTenant(testCtx, "tenant-a", HotFirst, func(view *TenantView) error {
		view.Hot.Events[key] = controlDeltaEvent("tenant-a", receipt.EventID)
		view.Global.LedgeredOutbox[key] = controlDeltaEvent("tenant-a", receipt.EventID)
		return view.Ledger.SetReceipt(receipt)
	})
	if err == nil || !strings.Contains(err.Error(), "injected control write failure") {
		t.Fatalf("faulted commit err=%v, want the injected control write failure", err)
	}
	if err := st.ReadTenant(testCtx, "tenant-a", func(view *TenantView) error {
		if _, hot := view.Hot.Events[key]; !hot {
			t.Error("hot event must already be durable when the control write fails")
		}
		got, ok := view.Ledger.Receipt(key)
		if !ok {
			t.Fatal("cold receipt must already be durable when the control write fails")
		}
		if got.Status != domain.StatusLedgered || !got.IndexedAt.IsZero() || !got.ArchivedAt.IsZero() {
			t.Errorf("receipt=%+v, want an interrupted ledgered state", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.ReadControl(testCtx, func(data *Snapshot) error {
		if _, ok := data.LedgeredOutbox[key]; ok {
			t.Error("control delta must not be durable after a failed control write")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var records int
	if err := st.LedgerScan(testCtx, "tenant-a", func(LedgerRecord) error { records++; return nil }); err != nil {
		t.Fatal(err)
	}
	if records == 0 {
		t.Fatal("cold ledger must already be durable when the control write fails")
	}

	// A re-run of the same closure — the shape PostgreSQL's ErrSnapshotConflict
	// retry takes, against a freshly cloned view.Global — must still land the
	// delta. A guard that had claimed the key inside the first attempt would
	// skip this write and lose the intent for good.
	armed.Store(false)
	if err := st.UpdateTenant(testCtx, "tenant-a", HotFirst, func(view *TenantView) error {
		view.Global.LedgeredOutbox[key] = controlDeltaEvent("tenant-a", receipt.EventID)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.ReadControl(testCtx, func(data *Snapshot) error {
		if _, ok := data.LedgeredOutbox[key]; !ok {
			t.Error("control delta lost when the closure is re-run against a fresh view")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
