package store

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestTenantLedgerMutatorsRejectOwnershipAndIdentity(t *testing.T) {
	ledger := NewTenantLedger("tenant-a")
	cases := []struct {
		name string
		call func() error
	}{
		{name: "receipt tenant", call: func() error {
			return ledger.SetReceipt(domain.EventReceipt{EventID: "evt-1", TenantID: "tenant-b"})
		}},
		{name: "segment tenant", call: func() error {
			return ledger.AppendSegment(domain.Segment{TenantID: "tenant-b", StreamID: "stream-1", FirstSequence: 1})
		}},
		{name: "checkpoint tenant", call: func() error {
			return ledger.AppendCheckpoint(domain.Checkpoint{ID: "checkpoint-1", TenantID: "tenant-b"})
		}},
		{name: "receipt identity", call: func() error {
			return ledger.SetReceipt(domain.EventReceipt{TenantID: "tenant-a"})
		}},
		{name: "segment identity", call: func() error {
			return ledger.AppendSegment(domain.Segment{TenantID: "tenant-a"})
		}},
		{name: "checkpoint identity", call: func() error {
			return ledger.AppendCheckpoint(domain.Checkpoint{TenantID: "tenant-a"})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); err == nil {
				t.Fatal("invalid ledger mutation succeeded")
			}
			if got := len(ledger.pendingRecords()); got != 0 {
				t.Fatalf("pending records=%d, want zero after rejection", got)
			}
		})
	}
}

func TestTenantLedgerImmutableDuplicatesAndConflicts(t *testing.T) {
	ledger := NewTenantLedger("tenant-a")
	segment := domain.Segment{TenantID: "tenant-a", StreamID: "stream-1", FirstSequence: 1, LastSequence: 1, EventCount: 1}
	if err := ledger.AppendSegment(segment); err != nil {
		t.Fatal(err)
	}
	ledger.commitPending(ledger.pendingRecords())
	if err := ledger.AppendSegment(segment); err != nil {
		t.Fatalf("exact segment duplicate: %v", err)
	}
	changedSegment := segment
	changedSegment.LastHash = "different"
	if err := ledger.AppendSegment(changedSegment); !errors.Is(err, ErrLedgerRecordConflict) {
		t.Fatalf("segment conflict=%v, want ErrLedgerRecordConflict", err)
	}
	if got := len(ledger.pendingRecords()); got != 0 {
		t.Fatalf("conflicting segment buffered %d records", got)
	}

	checkpoint := domain.Checkpoint{ID: "checkpoint-1", TenantID: "tenant-a", StreamID: "stream-1", Sequence: 1}
	if err := ledger.AppendCheckpoint(checkpoint); err != nil {
		t.Fatal(err)
	}
	ledger.commitPending(ledger.pendingRecords())
	changedCheckpoint := checkpoint
	changedCheckpoint.MerkleRoot = "different"
	if err := ledger.AppendCheckpoint(changedCheckpoint); !errors.Is(err, ErrLedgerRecordConflict) {
		t.Fatalf("checkpoint conflict=%v, want ErrLedgerRecordConflict", err)
	}

	receipt := domain.EventReceipt{EventID: "evt-1", TenantID: "tenant-a", Status: domain.StatusLedgered}
	if err := ledger.SetReceipt(receipt); err != nil {
		t.Fatal(err)
	}
	ledger.commitPending(ledger.pendingRecords())
	if err := ledger.SetReceipt(receipt); err != nil {
		t.Fatalf("exact receipt duplicate: %v", err)
	}
	receipt.Status = domain.StatusIndexed
	if err := ledger.SetReceipt(receipt); err != nil {
		t.Fatal(err)
	}
	if pending := ledger.pendingRecords(); len(pending) != 1 || pending[0].Version != 2 {
		t.Fatalf("receipt transition=%+v, want one version-2 record", pending)
	}
}

func TestUpdateTenantRejectsLedgerMutationWithoutPersistingHotState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
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
	mutationErr := st.UpdateTenant("tenant-a", HotFirst, func(view *TenantView) error {
		view.Hot.Events[EventKey("tenant-a", "should-not-persist")] = domain.Event{EventID: "should-not-persist", TenantID: "tenant-a"}
		return view.Ledger.SetReceipt(domain.EventReceipt{EventID: "bad", TenantID: "tenant-b"})
	})
	if mutationErr == nil {
		t.Fatal("invalid tenant mutation succeeded")
	}
	assertTenantMutationRolledBack(t, st)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	assertTenantMutationRolledBack(t, reopened)
}

func assertTenantMutationRolledBack(t *testing.T, st *Store) {
	t.Helper()
	if err := st.ReadTenant("tenant-a", func(view *TenantView) error {
		if len(view.Hot.Events) != 0 || len(view.Ledger.Records()) != 0 {
			return errors.New("rejected tenant mutation changed durable state")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLedgerFileAndPostgresDecodeRejectCanonicalMismatches(t *testing.T) {
	receipt := domain.EventReceipt{EventID: "evt-1", TenantID: "tenant-b"}
	record := LedgerRecord{TenantID: "tenant-a", RecordType: LedgerReceipt, Key: EventKey("tenant-a", "evt-1"), Version: 1, Receipt: &receipt, WrittenAt: time.Unix(1, 0).UTC()}
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeLedgerRecord(encoded, "tenant-a"); err == nil {
		t.Fatal("postgres decoder accepted mismatched receipt tenant")
	}

	path := filepath.Join(t.TempDir(), "state.json")
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
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	ledgerPath := filepath.Join(filepath.Dir(path), "ledger", "tenant-a.jsonl")
	file, err := os.OpenFile(ledgerPath, os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.Write(append(encoded, '\n'))
	_ = file.Close()
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "validate ledger") {
		t.Fatalf("file loader err=%v, want fail-closed validation error", err)
	}
}

func TestValidatePostgresLedgerIdentityRejectsSQLPayloadMismatches(t *testing.T) {
	record := LedgerRecord{
		TenantID:   "tenant-a",
		RecordType: LedgerReceipt,
		Key:        EventKey("tenant-a", "evt-1"),
		Version:    2,
		Receipt:    &domain.EventReceipt{EventID: "evt-1", TenantID: "tenant-a"},
	}
	cases := []struct {
		name      string
		sqlTenant string
		sqlType   string
		sqlKey    string
		sqlVer    int
	}{
		{name: "tenant", sqlTenant: "tenant-b", sqlType: string(LedgerReceipt), sqlKey: record.Key, sqlVer: record.Version},
		{name: "record type", sqlTenant: record.TenantID, sqlType: string(LedgerSegment), sqlKey: record.Key, sqlVer: record.Version},
		{name: "key", sqlTenant: record.TenantID, sqlType: string(LedgerReceipt), sqlKey: EventKey("tenant-a", "other"), sqlVer: record.Version},
		{name: "version", sqlTenant: record.TenantID, sqlType: string(LedgerReceipt), sqlKey: record.Key, sqlVer: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validatePostgresLedgerIdentity(record, tc.sqlTenant, LedgerRecordType(tc.sqlType), tc.sqlKey, tc.sqlVer); err == nil {
				t.Fatal("SQL/payload identity mismatch was accepted")
			}
		})
	}
	if err := validatePostgresLedgerIdentity(record, record.TenantID, LedgerReceipt, record.Key, record.Version); err != nil {
		t.Fatalf("matching SQL identity rejected: %v", err)
	}
}

func TestZPostgresLedgerImmutableConflictParity(t *testing.T) {
	db := newHotColdPostgresTestDB(t)
	st, err := OpenPostgres(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateControl(func(data *Snapshot) error {
		data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	segment := domain.Segment{TenantID: "tenant-a", StreamID: "stream-1", FirstSequence: 1, LastSequence: 1, EventCount: 1}
	if err := st.UpdateTenant("tenant-a", HotFirst, func(view *TenantView) error {
		return view.Ledger.AppendSegment(segment)
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateTenant("tenant-a", HotFirst, func(view *TenantView) error {
		return view.Ledger.AppendSegment(segment)
	}); err != nil {
		t.Fatalf("exact PostgreSQL duplicate: %v", err)
	}
	segment.LastHash = "different"
	err = st.UpdateTenant("tenant-a", HotFirst, func(view *TenantView) error {
		return view.Ledger.AppendSegment(segment)
	})
	if !errors.Is(err, ErrLedgerRecordConflict) {
		t.Fatalf("PostgreSQL conflict=%v, want ErrLedgerRecordConflict", err)
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM audit_ledger WHERE tenant_id = 'tenant-a' AND record_type = 'segment'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("PostgreSQL segment count=%d, want one", count)
	}
}
