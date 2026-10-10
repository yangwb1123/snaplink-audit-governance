package store

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestPendingTenantCommitSurvivesRestartAndCannotBeOverwritten(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	var armed atomic.Bool
	st, err := OpenWithLedgerWriteFault(path, func() error {
		if armed.Load() {
			return errors.New("injected append failure")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Update(testCtx, func(data *Snapshot) error {
		data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a"}
		data.Tenants["tenant-b"] = domain.Tenant{ID: "tenant-b"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	receipt := domain.EventReceipt{TenantID: "tenant-a", EventID: "recover-one", Status: domain.StatusLedgered, StreamID: "stream", Sequence: 1, Hash: "hash"}
	armed.Store(true)
	if err := st.UpdateTenant(testCtx, "tenant-a", HotFirst, func(view *TenantView) error {
		view.Hot.Events[EventKey("tenant-a", receipt.EventID)] = domain.Event{TenantID: "tenant-a", EventID: receipt.EventID, Hash: receipt.Hash}
		return view.Ledger.SetReceipt(receipt)
	}); err == nil {
		t.Fatal("append fault was not reached")
	}
	if err := st.UpdateTenant(testCtx, "tenant-a", HotFirst, func(view *TenantView) error {
		delete(view.Hot.Events, EventKey("tenant-a", receipt.EventID))
		return nil
	}); !errors.Is(err, ErrTenantCommitPending) {
		t.Fatal(err)
	}
	// An unrelated tenant and management-only snapshot mutation still progress.
	if err := st.UpdateTenant(testCtx, "tenant-b", HotFirst, func(view *TenantView) error {
		view.Global.Policies["tenant-b"] = domain.RetentionPolicy{TenantID: "tenant-b"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Update(testCtx, func(data *Snapshot) error {
		data.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	candidate, err := st.ReadPendingTenantCommit(testCtx, "tenant-a")
	if err != nil || len(candidate.Records) != 1 {
		t.Fatalf("records=%d err=%v", len(candidate.Records), err)
	}
	if err := st.ReadTenant(testCtx, "tenant-a", func(view *TenantView) error {
		if _, found := view.Ledger.Receipt(EventKey("tenant-a", receipt.EventID)); found {
			t.Fatal("pending marker became a receipt")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	bad := candidate
	bad.Version++
	if _, err := st.CommitPendingTenantCommit(testCtx, bad); !errors.Is(err, ErrSnapshotConflict) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := st.CommitPendingTenantCommit(ctx, candidate); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if n, err := st.CommitPendingTenantCommit(testCtx, candidate); err != nil || n != 1 {
		t.Fatalf("recovered=%d err=%v", n, err)
	}
	if n, err := st.CommitPendingTenantCommit(testCtx, candidate); err != nil || n != 0 {
		t.Fatalf("repeat=%d err=%v", n, err)
	}
	if err := st.ReadTenant(testCtx, "tenant-a", func(view *TenantView) error {
		got, ok := view.Ledger.Receipt(EventKey("tenant-a", receipt.EventID))
		if !ok || got != receipt {
			t.Fatal("lost original receipt")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPendingTenantCommitCaptureOwnsItsMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	armed := false
	st, err := OpenWithLedgerWriteFault(path, func() error {
		if armed {
			return errors.New("append failure")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Update(testCtx, func(data *Snapshot) error { data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a"}; return nil }); err != nil {
		t.Fatal(err)
	}
	armed = true
	_ = st.UpdateTenant(testCtx, "tenant-a", HotFirst, func(view *TenantView) error {
		view.Hot.Events[EventKey("tenant-a", "one")] = domain.Event{TenantID: "tenant-a", EventID: "one", Payload: map[string]any{"nested": map[string]any{"value": "original"}}}
		return view.Ledger.SetReceipt(domain.EventReceipt{TenantID: "tenant-a", EventID: "one"})
	})
	candidate, err := st.ReadPendingTenantCommit(testCtx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	candidate.Records[0].Receipt.Hash = "changed"
	candidate.Hot.Events[EventKey("tenant-a", "one")].Payload["nested"].(map[string]any)["value"] = "changed"
	again, err := st.ReadPendingTenantCommit(testCtx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if again.Records[0].Receipt.Hash != "" || again.Hot.Events[EventKey("tenant-a", "one")].Payload["nested"].(map[string]any)["value"] != "original" {
		t.Fatal("capture aliases durable metadata")
	}
}
