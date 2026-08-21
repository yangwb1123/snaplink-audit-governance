package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
)

type fileHotColdSize struct {
	state   int64
	archive int64
}

func measureFileHotColdSize(t *testing.T, count, payloadSize int) fileHotColdSize {
	t.Helper()
	root := t.TempDir()
	statePath := filepath.Join(root, "control.json")
	st, err := Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Update(func(data *Snapshot) error {
		data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateTenant("tenant-a", HotFirst, func(view *TenantView) error {
		for i := 0; i < count; i++ {
			eventID := fmt.Sprintf("capacity-%05d", i)
			receipt := domain.EventReceipt{EventID: eventID, TenantID: "tenant-a", IdempotencyKey: "idem-" + eventID, Status: domain.StatusArchived, StreamID: "tenant-a:source:crm", Sequence: int64(i + 1), Hash: "hash-" + eventID}
			view.Ledger.SetReceipt(receipt)
			if i%2 == 1 {
				segment := domain.Segment{TenantID: "tenant-a", StreamID: receipt.StreamID, FirstSequence: int64(i), LastSequence: int64(i + 1), EventCount: 2}
				view.Ledger.AppendSegment(segment)
				view.Ledger.AppendCheckpoint(domain.Checkpoint{ID: "checkpoint-" + eventID, TenantID: "tenant-a", StreamID: receipt.StreamID, Sequence: int64(i + 1)})
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	archiveRoot := filepath.Join(root, "archive")
	archiveStore := &archive.FileStore{Dir: archiveRoot}
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("events/tenant-a/source/%08d.json", i)
		if err := archiveStore.Put(context.Background(), key, []byte(strings.Repeat("x", payloadSize))); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	archiveBytes := directoryBytes(t, archiveRoot)
	return fileHotColdSize{state: directoryBytes(t, filepath.Dir(statePath)) - archiveBytes, archive: archiveBytes}
}

func directoryBytes(t *testing.T, root string) int64 {
	t.Helper()
	var total int64
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return total
}

// TestFileHotColdCapacitySlope pins the A1 file envelope: retained receipt,
// segment and checkpoint metadata grows linearly with event count, while
// changing an already-evicted payload does not enlarge the control/ledger
// state. Archive bytes are measured separately because WORM payload retention
// is intentionally proportional to payload size.
func TestFileHotColdCapacitySlope(t *testing.T) {
	small := measureFileHotColdSize(t, 100, 4096)
	medium := measureFileHotColdSize(t, 1000, 4096)
	large := measureFileHotColdSize(t, 5000, 4096)
	smallDelta := medium.state - small.state
	largeDelta := large.state - medium.state
	if smallDelta <= 0 || largeDelta <= 0 {
		t.Fatalf("state size did not grow with retained metadata: small=%d medium=%d large=%d", small.state, medium.state, large.state)
	}
	if largeDelta > smallDelta*6 {
		t.Fatalf("state growth is super-linear: 100->1000=%d, 1000->5000=%d", smallDelta, largeDelta)
	}
	doubled := measureFileHotColdSize(t, 100, 8192)
	if doubled.state > small.state+4096 || small.state > doubled.state+4096 {
		t.Fatalf("evicted payload changed retained state size: 4K=%d 8K=%d", small.state, doubled.state)
	}
	if doubled.archive <= small.archive {
		t.Fatalf("archive payload size did not increase: 4K=%d 8K=%d", small.archive, doubled.archive)
	}
}
