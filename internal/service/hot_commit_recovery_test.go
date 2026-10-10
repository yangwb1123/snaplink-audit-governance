package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func hotRecoveryService(t *testing.T, segmentSize int) (*Service, string, func(bool)) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.json")
	var armed atomic.Bool
	st, err := store.OpenWithLedgerWriteFault(path, func() error {
		if armed.Load() {
			return errors.New("injected ledger append failure")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc, err := New(st, Config{SegmentSize: segmentSize, SigningSecret: "test-secret", AllowDevSecrets: true, Now: func() time.Time { return time.Unix(1700000000, 0).UTC() }})
	if err != nil {
		t.Fatal(err)
	}
	seedTestDomain(t, svc)
	return svc, path, func(on bool) { armed.Store(on) }
}

func TestHotCommitRecoveryAfterRestartWithoutClientOrPublisher(t *testing.T) {
	for _, size := range []int{1, 2, 100, 102} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			segmentSize, prefix := size, 0
			if size == 2 {
				prefix = 1
			}
			if size == 100 {
				prefix = 2
			} // authenticate prior hashes in an unsealed prefix
			if size == 102 {
				segmentSize, prefix = 2, 3
			} // recover a later sealed segment
			svc, path, arm := hotRecoveryService(t, segmentSize)
			at := time.Unix(1700000010, 0).UTC()
			for i := 0; i < prefix; i++ {
				if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("before-"+strconv.Itoa(i), "op-before", at), domain.StatusIndexed); err != nil {
					t.Fatal(err)
				}
			}
			event := testEvent("interrupted", "op-recovery", at.Add(time.Second))
			arm(true)
			receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
			if err == nil || receipt.Status != "" || !receipt.LedgeredAt.IsZero() {
				t.Fatalf("noncommitted receipt advertised: status=%s err=%v", receipt.Status, err)
			}
			if _, err := svc.GetReceipt(testCtx, "tenant-a", "", event.EventID); !errors.Is(err, domain.ErrNotFound) {
				t.Fatal(err)
			}
			if page, err := svc.QueryEvents(testCtx, "tenant-a", "", domain.Query{From: at, To: at.Add(time.Hour)}); err != nil || page.Count != prefix {
				t.Fatalf("hot-only query count=%d err=%v", page.Count, err)
			}
			candidate, err := svc.Store.ReadPendingTenantCommit(testCtx, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			original := *candidate.Records[0].Receipt
			if err := svc.Store.Close(); err != nil {
				t.Fatal(err)
			}
			st, err := store.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			restarted, err := New(st, svc.Config)
			if err != nil {
				t.Fatal(err)
			}
			n, err := restarted.RecoverHotTenantCommit(testCtx, "tenant-a")
			if err != nil || n != len(candidate.Records) {
				t.Fatalf("recovered=%d err=%v", n, err)
			}
			got := durableReceipt(t, restarted, "tenant-a", event.EventID)
			if got != original || got.Status != domain.StatusLedgered {
				t.Fatal("recovery changed original coordinates or status")
			}
			if n, err := restarted.RecoverHotTenantCommit(testCtx, "tenant-a"); err != nil || n != 0 {
				t.Fatalf("repeat=%d err=%v", n, err)
			}
			if err := st.ReadTenant(testCtx, "tenant-a", func(view *store.TenantView) error {
				sealedCount := (prefix + 1) / segmentSize
				if len(view.Ledger.Segments(store.StreamKey("tenant-a", got.StreamID))) != sealedCount {
					t.Fatal("lost sealed segment")
				}
				if len(view.Ledger.Checkpoints(store.StreamKey("tenant-a", got.StreamID))) != sealedCount {
					t.Fatal("lost exact checkpoint")
				}
				if view.Hot.Streams[store.StreamKey("tenant-a", got.StreamID)].NextSequence != got.Sequence+1 {
					t.Fatal("stream resequenced")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			duplicate, err := restarted.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
			if err != nil || !duplicate.Duplicate || duplicate.Hash != got.Hash || duplicate.Sequence != got.Sequence {
				t.Fatalf("retry duplicate=%v err=%v", duplicate.Duplicate, err)
			}
		})
	}
}

func TestHotCommitRecoveryRejectsBadEvidenceAndPreservesMarker(t *testing.T) {
	svc, _, arm := hotRecoveryService(t, 1)
	arm(true)
	_, _ = svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("bad-evidence", "op-bad", time.Unix(1700000010, 0).UTC()), domain.StatusLedgered)
	for _, kind := range []string{"payload", "hash", "signature", "checkpoint", "stream"} {
		t.Run(kind, func(t *testing.T) {
			candidate, err := svc.Store.ReadPendingTenantCommit(testCtx, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			key := store.EventKey("tenant-a", "bad-evidence")
			switch kind {
			case "payload":
				candidate.Hot.Events[key].Payload["value"] = "tampered"
			case "hash":
				candidate.Records[0].Receipt.Hash = "tampered"
			case "signature":
				candidate.Records[1].Segment.Signature = "tampered"
				candidate.Records[2].Checkpoint.Signature = "tampered"
			case "checkpoint":
				candidate.Records[2].Checkpoint.Sequence++
			case "stream":
				value := candidate.Hot.Streams[store.StreamKey("tenant-a", candidate.Records[0].Receipt.StreamID)]
				value.HeadHash = "tampered"
				candidate.Hot.Streams[store.StreamKey("tenant-a", value.StreamID)] = value
			}
			if err := svc.validateHotTenantCommit(testCtx, candidate); err == nil {
				t.Fatal("bad evidence accepted")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := svc.RecoverHotTenantCommit(ctx, "tenant-a"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := svc.GetReceipt(testCtx, "tenant-a", "", "bad-evidence"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("failed recovery fabricated a receipt", err)
	}
	arm(false)
	if n, err := svc.RecoverHotTenantCommit(testCtx, "tenant-a"); err != nil || n != 3 {
		t.Fatalf("healthy retry n=%d err=%v", n, err)
	}
}

func TestHotCommitRecoveryRejectsPersistedPoison(t *testing.T) {
	svc, path, arm := hotRecoveryService(t, 100)
	arm(true)
	_, _ = svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("poisoned", "op-poison", time.Unix(1700000010, 0).UTC()), domain.StatusLedgered)
	if err := svc.Store.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(filepath.Dir(path), "tenants", "*.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("hot files=%v err=%v", files, err)
	}
	raw, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	var disk map[string]any
	if err := json.Unmarshal(raw, &disk); err != nil {
		t.Fatal(err)
	}
	disk["events"].(map[string]any)[store.EventKey("tenant-a", "poisoned")].(map[string]any)["payload"].(map[string]any)["value"] = "poison marker"
	raw, err = json.Marshal(disk)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(files[0], raw, 0600); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	restarted, err := New(st, svc.Config)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := restarted.RecoverHotTenantCommit(testCtx, "tenant-a"); err == nil || n != 0 {
		t.Fatalf("poison recovery n=%d err=%v", n, err)
	}
	candidate, err := st.ReadPendingTenantCommit(testCtx, "tenant-a")
	if err != nil || len(candidate.Records) != 1 {
		t.Fatal("failed validation erased journal", err)
	}
	if _, err := restarted.GetReceipt(testCtx, "tenant-a", "", "poisoned"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("poison became committed", err)
	}
	if restarted.HotCommitRecoveryFailures() != 1 {
		t.Fatal("missing recovery failure signal")
	}
}

func TestPreJournalHotOnlyIsNotACommittedDuplicateOrHead(t *testing.T) {
	svc, path, arm := hotRecoveryService(t, 100)
	event := testEvent("legacy-orphan", "op-legacy", time.Unix(1700000010, 0).UTC())
	arm(true)
	_, _ = svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err := svc.Store.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(filepath.Dir(path), "tenants", "*.json"))
	if err != nil || len(files) != 1 {
		t.Fatal("missing hot fixture", err)
	}
	raw, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	var disk map[string]any
	if err := json.Unmarshal(raw, &disk); err != nil {
		t.Fatal(err)
	}
	delete(disk, "pending_ledger") // reproduce an old-version interrupted write
	raw, err = json.Marshal(disk)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(files[0], raw, 0600); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	restarted, err := New(st, svc.Config)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := restarted.RecoverHotTenantCommit(testCtx, "tenant-a"); err != nil || n != 0 {
		t.Fatal("legacy plan was inferred", err)
	}
	for _, value := range []domain.Event{event, testEvent("new-after-orphan", "op-new", event.OccurredAt.Add(time.Second))} {
		receipt, err := restarted.Ingest(testCtx, "tenant-a", crmPrincipal, value, domain.StatusLedgered)
		if !errors.Is(err, domain.ErrConflict) || receipt.Status != "" || receipt.Duplicate {
			t.Fatalf("legacy evidence fabricated commitment: status=%s err=%v", receipt.Status, err)
		}
	}
	if _, err := restarted.GetReceipt(testCtx, "tenant-a", "", event.EventID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("legacy receipt fabricated", err)
	}
}

func TestIngestRecoversJournalBeforeAllocatingAnotherSequence(t *testing.T) {
	svc, _, arm := hotRecoveryService(t, 2)
	at := time.Unix(1700000010, 0).UTC()
	arm(true)
	_, _ = svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("first", "op-first", at), domain.StatusLedgered)
	arm(false)
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("second", "op-second", at.Add(time.Second)), domain.StatusIndexed)
	if err != nil || receipt.Sequence != 2 {
		t.Fatalf("next sequence=%d err=%v", receipt.Sequence, err)
	}
	if got := durableReceipt(t, svc, "tenant-a", "first"); got.Sequence != 1 || got.Status != domain.StatusLedgered {
		t.Fatal("first receipt lost")
	}
}
