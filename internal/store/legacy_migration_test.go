package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

func legacyOwnershipFixture() *Snapshot {
	data := NewSnapshot()
	data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a"}
	data.Tenants["tenant-b"] = domain.Tenant{ID: "tenant-b"}
	return data
}

func TestSplitLegacySnapshotRejectsOwnershipMismatchAcrossFamilies(t *testing.T) {
	const key = "tenant-a" + KeySeparator + "record-1"
	tests := []struct {
		name   string
		family string
		setup  func(*Snapshot)
		index  string
	}{
		{"event", "events", func(data *Snapshot) {
			data.Events[key] = domain.Event{EventID: "record-1", TenantID: "tenant-b"}
		}, ""},
		{"archived event", "events", func(data *Snapshot) {
			data.Events[key] = domain.Event{EventID: "record-1", TenantID: "tenant-b"}
			data.Receipts[key] = domain.EventReceipt{EventID: "record-1", TenantID: "tenant-a", Status: domain.StatusArchived}
		}, ""},
		{"receipt", "receipts", func(data *Snapshot) {
			data.Receipts[key] = domain.EventReceipt{EventID: "record-1", TenantID: "tenant-b"}
		}, ""},
		{"stream", "streams", func(data *Snapshot) {
			data.Streams[key] = StreamState{TenantID: "tenant-b"}
		}, ""},
		{"segment", "segments", func(data *Snapshot) {
			data.Segments[key] = []domain.Segment{{TenantID: "tenant-b"}}
		}, "index=0"},
		{"checkpoint", "checkpoints", func(data *Snapshot) {
			data.Checkpoints[key] = []domain.Checkpoint{{ID: "checkpoint-1", TenantID: "tenant-b"}}
		}, "index=0"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := legacyOwnershipFixture()
			test.setup(data)
			_, err := splitLegacySnapshot(data)
			if err == nil || !errors.Is(err, ErrLegacySnapshotOwnership) {
				t.Fatalf("split error=%v, want ownership error", err)
			}
			message := err.Error()
			for _, want := range []string{"family=" + test.family, "key=" + key, "reason=tenant mismatch", test.index} {
				if want != "" && !strings.Contains(message, want) {
					t.Errorf("error %q does not contain %q", message, want)
				}
			}
		})
	}
}

func TestSplitLegacySnapshotRejectsUnknownKeyAndPayloadTenants(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		setup func(*Snapshot, string)
		want  string
	}{
		{"event key", EventKey("tenant-unknown", "event-1"), func(data *Snapshot, key string) {
			data.Events[key] = domain.Event{EventID: "event-1", TenantID: "tenant-a"}
		}, "unregistered key tenant"},
		{"receipt payload", EventKey("tenant-a", "receipt-1"), func(data *Snapshot, key string) {
			data.Receipts[key] = domain.EventReceipt{EventID: "receipt-1", TenantID: "tenant-unknown"}
		}, "unregistered payload tenant"},
		{"stream key", StreamKey("tenant-unknown", "stream-1"), func(data *Snapshot, key string) {
			data.Streams[key] = StreamState{TenantID: "tenant-a"}
		}, "unregistered key tenant"},
		{"segment payload", StreamKey("tenant-a", "stream-1"), func(data *Snapshot, key string) {
			data.Segments[key] = []domain.Segment{{TenantID: "tenant-unknown"}}
		}, "unregistered payload tenant"},
		{"checkpoint payload", StreamKey("tenant-a", "stream-1"), func(data *Snapshot, key string) {
			data.Checkpoints[key] = []domain.Checkpoint{{ID: "checkpoint-1", TenantID: "tenant-unknown"}}
		}, "unregistered payload tenant"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := legacyOwnershipFixture()
			test.setup(data, test.key)
			_, err := splitLegacySnapshot(data)
			if err == nil || !strings.Contains(err.Error(), test.want) || !strings.Contains(err.Error(), "key="+test.key) {
				t.Fatalf("split error=%v, want key and %q", err, test.want)
			}
		})
	}
}

func TestSplitLegacySnapshotUsesRegisteredKeyOwnership(t *testing.T) {
	data := legacyOwnershipFixture()
	data.Events[EventKey("tenant-a", "hot")] = domain.Event{EventID: "hot", TenantID: "tenant-a"}
	data.Events[EventKey("tenant-a", "archived")] = domain.Event{EventID: "archived", TenantID: "tenant-a"}
	data.Receipts[EventKey("tenant-a", "archived")] = domain.EventReceipt{EventID: "archived", TenantID: "tenant-a", Status: domain.StatusArchived}
	data.Streams[StreamKey("tenant-a", "stream-1")] = StreamState{TenantID: "tenant-a", StreamID: "stream-1"}
	data.Segments[StreamKey("tenant-a", "stream-1")] = []domain.Segment{{TenantID: "tenant-a", StreamID: "stream-1", FirstSequence: 1}}
	data.Checkpoints[StreamKey("tenant-a", "stream-1")] = []domain.Checkpoint{{ID: "checkpoint-1", TenantID: "tenant-a", StreamID: "stream-1"}}

	prepared, err := splitLegacySnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.tenant) != 2 {
		t.Fatalf("destinations=%d, want registered tenants only", len(prepared.tenant))
	}
	state := prepared.tenant["tenant-a"]
	if _, ok := state.hot.Events[EventKey("tenant-a", "hot")]; !ok {
		t.Fatal("hot event was not assigned to its key tenant")
	}
	if _, ok := state.hot.Events[EventKey("tenant-a", "archived")]; ok {
		t.Fatal("archived event was copied into hot storage")
	}
	if got := len(state.ledger.committedRecords()); got != 3 {
		t.Fatalf("ledger records=%d, want receipt, segment, checkpoint", got)
	}
	for _, record := range state.ledger.committedRecords() {
		if record.TenantID != "tenant-a" {
			t.Fatalf("ledger outer tenant=%q", record.TenantID)
		}
	}
}

func TestValidateLegacyLedgerRecordChecksGeneratedOwnership(t *testing.T) {
	segment := domain.Segment{TenantID: "tenant-a", StreamID: "stream-1", FirstSequence: 1}
	record := LedgerRecord{TenantID: "tenant-a", RecordType: LedgerSegment, Key: "tenant-b" + KeySeparator + "stream-1" + KeySeparator + "1", Version: 1, Segment: &segment}
	if err := validateLegacyLedgerRecord(StreamKey("tenant-a", "stream-1"), record); err == nil || !strings.Contains(err.Error(), "reason=tenant mismatch") {
		t.Fatalf("segment ownership error=%v, want tenant mismatch", err)
	}

	checkpoint := domain.Checkpoint{ID: "checkpoint-1", TenantID: "tenant-a"}
	record = LedgerRecord{TenantID: "tenant-a", RecordType: LedgerCheckpoint, Key: "wrong-key", Version: 1, Checkpoint: &checkpoint}
	if err := validateLegacyLedgerRecord(StreamKey("tenant-a", "stream-1"), record); err == nil || !strings.Contains(err.Error(), "reason=tenant mismatch") {
		t.Fatalf("checkpoint ownership error=%v, want tenant mismatch", err)
	}
}
