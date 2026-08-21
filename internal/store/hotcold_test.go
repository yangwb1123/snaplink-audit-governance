package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

func seedHotColdTenant(t *testing.T, path string) (*Store, domain.EventReceipt) {
	t.Helper()
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Update(func(data *Snapshot) error {
		data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	receipt := domain.EventReceipt{EventID: "fault-event", TenantID: "tenant-a", Status: domain.StatusLedgered, StreamID: "tenant-a:source:crm", Sequence: 1, Hash: "hash-1"}
	if err := st.UpdateTenant("tenant-a", HotFirst, func(view *TenantView) error {
		view.Hot.Events[EventKey("tenant-a", receipt.EventID)] = domain.Event{EventID: receipt.EventID, TenantID: receipt.TenantID, StreamID: receipt.StreamID, Sequence: receipt.Sequence, Hash: receipt.Hash, Payload: map[string]any{"value": 1}}
		view.Ledger.SetReceipt(receipt)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return st, receipt
}

func TestHotFirstColdFailureRetainsDurableHotState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.json")
	st, receipt := seedHotColdTenant(t, path)
	originalAppend := appendLedgerFile
	appendLedgerFile = func(string, string, []LedgerRecord) error { return errors.New("injected cold append failure") }
	t.Cleanup(func() { appendLedgerFile = originalAppend })
	updated := receipt
	updated.Status = domain.StatusIndexed
	err := st.UpdateTenant("tenant-a", HotFirst, func(view *TenantView) error {
		event := view.Hot.Events[EventKey("tenant-a", receipt.EventID)]
		event.Payload = map[string]any{"value": 2}
		view.Hot.Events[EventKey("tenant-a", receipt.EventID)] = event
		view.Ledger.SetReceipt(updated)
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "injected cold append failure") {
		t.Fatalf("HotFirst cold failure=%v, want injected error", err)
	}
	if err := st.ReadTenant("tenant-a", func(view *TenantView) error {
		if got := view.Hot.Events[EventKey("tenant-a", receipt.EventID)].Payload["value"]; fmt.Sprint(got) != "2" {
			t.Fatalf("durable hot value=%v, want 2", got)
		}
		got, ok := view.Ledger.Receipt(EventKey("tenant-a", receipt.EventID))
		if !ok || got.Status != domain.StatusLedgered {
			return errors.New("failed cold append changed the committed receipt")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.ReadTenant("tenant-a", func(view *TenantView) error {
		if got := view.Hot.Events[EventKey("tenant-a", receipt.EventID)].Payload["value"]; fmt.Sprint(got) != "2" {
			t.Fatalf("reloaded hot value=%v, want 2", got)
		}
		got, ok := view.Ledger.Receipt(EventKey("tenant-a", receipt.EventID))
		if !ok || got.Status != domain.StatusLedgered {
			return errors.New("reloaded cold ledger changed after failed append")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestColdFirstHotFailureLeavesRecoverableArchivedState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.json")
	st, receipt := seedHotColdTenant(t, path)
	originalWrite := writeDurableFile
	writeDurableFile = func(target string, data []byte, mode os.FileMode) error {
		if strings.Contains(filepath.ToSlash(target), "/tenants/") {
			return errors.New("injected hot save failure")
		}
		return originalWrite(target, data, mode)
	}
	t.Cleanup(func() { writeDurableFile = originalWrite })
	err := st.UpdateTenant("tenant-a", ColdFirst, func(view *TenantView) error {
		archived := receipt
		archived.Status = domain.StatusArchived
		view.Ledger.SetReceipt(archived)
		delete(view.Hot.Events, EventKey("tenant-a", receipt.EventID))
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "injected hot save failure") {
		t.Fatalf("ColdFirst hot failure=%v, want injected error", err)
	}
	if err := st.ReadTenant("tenant-a", func(view *TenantView) error {
		if _, ok := view.Hot.Events[EventKey("tenant-a", receipt.EventID)]; !ok {
			return errors.New("hot event was evicted before hot save committed")
		}
		got, ok := view.Ledger.Receipt(EventKey("tenant-a", receipt.EventID))
		if !ok || got.Status != domain.StatusArchived {
			return errors.New("archived receipt was not durably committed before hot failure")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Clear the fault and run the recovery half of the worker pass. The
	// already-archived receipt makes this a pure safe eviction.
	writeDurableFile = originalWrite
	if err := st.UpdateTenant("tenant-a", ColdFirst, func(view *TenantView) error {
		if receipt, ok := view.Ledger.Receipt(EventKey("tenant-a", "fault-event")); ok && receipt.Status == domain.StatusArchived {
			delete(view.Hot.Events, EventKey("tenant-a", "fault-event"))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.ReadTenant("tenant-a", func(view *TenantView) error {
		if _, ok := view.Hot.Events[EventKey("tenant-a", "fault-event")]; ok {
			return errors.New("recovery did not evict the already-archived hot event")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTenantLedgerVersionsAndImmutableFamilies(t *testing.T) {
	ledger := NewTenantLedger("tenant-a")
	receipt := domain.EventReceipt{EventID: "evt-1", TenantID: "tenant-a", IdempotencyKey: "idem-1", Status: domain.StatusLedgered}
	ledger.SetReceipt(receipt)
	first := ledger.pendingRecords()
	if len(first) != 1 || first[0].Version != 1 {
		t.Fatalf("first receipt records=%+v, want version 1", first)
	}
	ledger.commitPending(first)
	ledger.SetReceipt(receipt)
	if got := len(ledger.pendingRecords()); got != 0 {
		t.Fatalf("identical receipt retry buffered %d records", got)
	}
	receipt.Status = domain.StatusIndexed
	ledger.SetReceipt(receipt)
	second := ledger.pendingRecords()
	if len(second) != 1 || second[0].Version != 2 {
		t.Fatalf("status transition records=%+v, want version 2", second)
	}
	ledger.commitPending(second)
	segment := domain.Segment{TenantID: "tenant-a", StreamID: "stream-1", FirstSequence: 1, LastSequence: 1, EventCount: 1}
	checkpoint := domain.Checkpoint{ID: "checkpoint-1", TenantID: "tenant-a", StreamID: "stream-1", Sequence: 1}
	ledger.AppendSegment(segment)
	ledger.AppendCheckpoint(checkpoint)
	ledger.commitPending(ledger.pendingRecords())
	if got, ok := ledger.Receipt(EventKey("tenant-a", "evt-1")); !ok || got.Status != domain.StatusIndexed {
		t.Fatalf("latest receipt=%+v present=%v", got, ok)
	}
	if len(ledger.Segments(StreamKey("tenant-a", "stream-1"))) != 1 || len(ledger.Checkpoints(StreamKey("tenant-a", "stream-1"))) != 1 {
		t.Fatal("immutable segment/checkpoint family was not indexed")
	}
	if got, ok := ledger.FindReceiptByIdempotencyKey("idem-1"); !ok || got.EventID != "evt-1" {
		t.Fatalf("idempotency index lookup=%+v present=%v", got, ok)
	}
}

func TestTenantLedgerOverlayInheritsReceiptVersion(t *testing.T) {
	parent := NewTenantLedger("tenant-a")
	first := domain.EventReceipt{EventID: "evt-overlay", TenantID: "tenant-a", Status: domain.StatusLedgered}
	parent.SetReceipt(first)
	committed := parent.pendingRecords()
	parent.commitPending(committed)

	overlay := newTenantLedgerView(parent)
	first.Status = domain.StatusIndexed
	overlay.SetReceipt(first)
	pending := overlay.pendingRecords()
	if len(pending) != 1 || pending[0].Version != 2 {
		t.Fatalf("overlay receipt records=%+v, want one v2 record", pending)
	}
}

func TestOpenMigratesV1SnapshotIntoHotColdLayout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.json")
	streamID := "tenant-a:aggregate:invoice:inv-1"
	hot := domain.Event{EventID: "evt-hot", TenantID: "tenant-a", StreamID: streamID, Sequence: 2, Hash: "hash-hot"}
	archived := domain.Event{EventID: "evt-archived", TenantID: "tenant-a", StreamID: streamID, Sequence: 1, Hash: "hash-archived"}
	data := NewSnapshot()
	data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}
	data.Events[EventKey("tenant-a", hot.EventID)] = hot
	data.Events[EventKey("tenant-a", archived.EventID)] = archived
	data.Receipts[EventKey("tenant-a", hot.EventID)] = domain.EventReceipt{EventID: hot.EventID, TenantID: hot.TenantID, IdempotencyKey: "idem-hot", Status: domain.StatusIndexed, StreamID: streamID, Sequence: 2, Hash: hot.Hash}
	data.Receipts[EventKey("tenant-a", archived.EventID)] = domain.EventReceipt{EventID: archived.EventID, TenantID: archived.TenantID, IdempotencyKey: "idem-archived", Status: domain.StatusArchived, StreamID: streamID, Sequence: 1, Hash: archived.Hash}
	data.Streams[StreamKey("tenant-a", streamID)] = StreamState{TenantID: "tenant-a", StreamID: streamID, NextSequence: 3, HeadHash: hot.Hash}
	data.Segments[StreamKey("tenant-a", streamID)] = []domain.Segment{{TenantID: "tenant-a", StreamID: streamID, FirstSequence: 1, LastSequence: 1, EventCount: 1}}
	data.Checkpoints[StreamKey("tenant-a", streamID)] = []domain.Checkpoint{{ID: "checkpoint-1", TenantID: "tenant-a", StreamID: streamID, Sequence: 1}}
	encoded, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o640); err != nil {
		t.Fatal(err)
	}

	st, err := Open(path)
	if err != nil {
		t.Fatalf("Open migration: %v", err)
	}
	snapshot, err := st.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.LayoutVersion != hotColdLayoutVersion || len(snapshot.Events) != 1 || len(snapshot.Receipts) != 2 {
		t.Fatalf("migrated snapshot layout=%d events=%d receipts=%d", snapshot.LayoutVersion, len(snapshot.Events), len(snapshot.Receipts))
	}
	if _, ok := snapshot.Events[EventKey("tenant-a", archived.EventID)]; ok {
		t.Fatal("archived event was copied into tenant hot layout")
	}
	if _, err := os.Stat(path + ".v1"); err != nil {
		t.Fatalf("v1 backup missing: %v", err)
	}
	var control Snapshot
	controlBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(controlBytes, &control); err != nil {
		t.Fatal(err)
	}
	if control.LayoutVersion != hotColdLayoutVersion || len(control.Events) != 0 || len(control.Receipts) != 0 {
		t.Fatalf("control file still contains ledger data: layout=%d events=%d receipts=%d", control.LayoutVersion, len(control.Events), len(control.Receipts))
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen migrated layout: %v", err)
	}
	defer reopened.Close()
	reloaded, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(reloaded.Events) != 1 || reloaded.Receipts[EventKey("tenant-a", archived.EventID)].Status != domain.StatusArchived {
		t.Fatalf("restart changed migrated state: events=%d archived=%+v", len(reloaded.Events), reloaded.Receipts[EventKey("tenant-a", archived.EventID)])
	}
}

func TestOpenRejectsIncompleteV2LedgerLayout(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "control.json")
	control := NewSnapshot()
	control.LayoutVersion = hotColdLayoutVersion
	control.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}
	encoded, err := json.Marshal(control)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := writeTenantFile(root, "tenant-a", tenantHotSnapshot{TenantID: "tenant-a", Version: 1, Events: map[string]domain.Event{}, Streams: map[string]StreamState{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("Open accepted v2 layout without the tenant ledger file")
	} else if !strings.Contains(err.Error(), "ledger file is missing") {
		t.Fatalf("incomplete v2 error=%v, want missing ledger evidence", err)
	}
}
