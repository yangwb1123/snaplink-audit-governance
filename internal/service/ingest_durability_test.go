package service

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// This file pins the receipt status-transition durability contract of Ingest
// (requirements R-1..R-5 / direction internal-store-c7524028 finding #2): the
// three status-transition Store.Update calls after the ledger CAS must fail
// closed — a non-durable Indexed/Archived transition must never be reported,
// waitFor must never pass on an in-memory copy, and an idempotent retry must
// surface the durably persisted StatusLedgered receipt.

// failSaveBackend is a stateful in-memory store.Backend whose Save fails on a
// 1-indexed inclusive call window with either ErrSnapshotConflict (bounded
// retry then exhaustion) or a sentinel generic error (immediate return).
// Ingest performs exactly two Store.Update calls per non-duplicate event —
// the ledger CAS (one Save), then the status transition — so arming the
// window at saves+2..saves+2+retries faults every Save attempt of the status
// write while the ledger CAS commits, keeping the receipt durably
// StatusLedgered. Clone-on-load gives the private-copy semantics of
// fileBackend.LoadForUpdate without exporting store.cloneSnapshot; the JSON
// round-trip with UseNumber mirrors cloneTestSnapshot/scriptedConflictBackend
// so payload numbers survive without collapsing through float64.
type failSaveBackend struct {
	mu           sync.Mutex
	data         *store.Snapshot
	saves        int
	conflictFrom int // 0 = disabled
	conflictTo   int
	errorFrom    int // 0 = disabled
	errorTo      int
	sentinel     error
}

func newFailSaveBackend() *failSaveBackend {
	return &failSaveBackend{data: store.NewSnapshot()}
}

func (b *failSaveBackend) Load() (*store.Snapshot, error) { return cloneTestSnapshot(b.data) }

func (b *failSaveBackend) LoadForUpdate() (*store.Snapshot, error) { return cloneTestSnapshot(b.data) }

func (b *failSaveBackend) Save(data *store.Snapshot) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.saves++
	if b.conflictFrom > 0 && b.saves >= b.conflictFrom && b.saves <= b.conflictTo {
		return store.ErrSnapshotConflict
	}
	if b.errorFrom > 0 && b.saves >= b.errorFrom && b.saves <= b.errorTo {
		return b.sentinel
	}
	committed, err := cloneTestSnapshot(data)
	if err != nil {
		return err
	}
	b.data = committed
	return nil
}

// faultStatusWriteWithConflicts arms the backend so the NEXT Ingest's
// status-transition Store.Update exhausts the conflict budget while the
// ledger CAS commits. The window is derived from the live save counter so
// domain seeding (CreateTenant/AddSource/RegisterSchema) can never shift it.
// snapshotConflictRetries (store.go:200) is unexported, so the retry budget
// is hardcoded at its current value 3 with a loud-drift guard: if the
// constant grows, the window under-faults and Ingest succeeds, failing the
// test with a non-nil-error assertion; if it shrinks, the test still passes
// (over-covered) but the store suite pins the value.
func (b *failSaveBackend) faultStatusWriteWithConflicts() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.conflictFrom = b.saves + 2 // ledger CAS Save, must commit
	b.conflictTo = b.saves + 2 + 3
}

// faultStatusWriteWithError arms the backend so the NEXT Ingest's
// status-transition Store.Update fails immediately with the sentinel
// (non-conflict errors are not retried by Update, store.go:243-244).
func (b *failSaveBackend) faultStatusWriteWithError(sentinel error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.errorFrom = b.saves + 2
	b.errorTo = b.saves + 2
	b.sentinel = sentinel
}

// durabilityTestService wires a Service over the given store with the
// standard test domain (same as newServiceOn + seedTestDomain) and, when
// archive is true, a recording archive store so the StatusArchived write
// branch is reached without touching the filesystem.
func durabilityTestService(t *testing.T, archive bool, st *store.Store) *Service {
	t.Helper()
	svc := newServiceOn(t, st)
	seedTestDomain(t, svc)
	if archive {
		svc.Config.Archive = &recordingArchive{}
	}
	return svc
}

// persistedReceipt reads the receipt for eventID from the committed snapshot.
func persistedReceipt(t *testing.T, svc *Service, tenantID, eventID string) domain.EventReceipt {
	t.Helper()
	var got domain.EventReceipt
	err := svc.Store.Read(func(data *store.Snapshot) error {
		got = data.Receipts[store.EventKey(tenantID, eventID)]
		return nil
	})
	if err != nil {
		t.Fatalf("Store.Read: %v", err)
	}
	return got
}

// assertLedgeredTruth asserts the returned/persisted receipt carries exactly
// the ledger-CAS-committed state: StatusLedgered with zero IndexedAt and
// ArchivedAt — never a fabricated Indexed/Archived transition.
func assertLedgeredTruth(t *testing.T, label string, receipt domain.EventReceipt) {
	t.Helper()
	if receipt.Status != domain.StatusLedgered {
		t.Fatalf("%s status = %q, want %q", label, receipt.Status, domain.StatusLedgered)
	}
	if !receipt.IndexedAt.IsZero() {
		t.Fatalf("%s IndexedAt = %v, want zero (transition never committed)", label, receipt.IndexedAt)
	}
	if !receipt.ArchivedAt.IsZero() {
		t.Fatalf("%s ArchivedAt = %v, want zero (transition never committed)", label, receipt.ArchivedAt)
	}
}

// TestIngestStatusWriteConflictExhaustionFailsClosed is AC-1: when every Save
// attempt of the status-transition Update returns ErrSnapshotConflict and the
// bounded retry budget is exhausted, Ingest must return the conflict (mapped
// to 503 by the HTTP layer) and must not report Indexed/Archived — the
// returned and persisted receipt stay at the durable StatusLedgered state.
func TestIngestStatusWriteConflictExhaustionFailsClosed(t *testing.T) {
	cases := []struct {
		name    string
		archive bool
		waitFor string
	}{
		{name: "archive-enabled-wait-archived", archive: true, waitFor: domain.StatusArchived},
		{name: "archive-disabled-wait-indexed", archive: false, waitFor: domain.StatusIndexed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backend := newFailSaveBackend()
			svc := durabilityTestService(t, tc.archive, store.NewWithBackend(backend))
			backend.faultStatusWriteWithConflicts()
			event := testEvent("durability-"+tc.name, "op-durability", time.Unix(1_700_000_010, 0).UTC())

			receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, tc.waitFor)

			if !errors.Is(err, store.ErrSnapshotConflict) {
				t.Fatalf("Ingest err = %v, want ErrSnapshotConflict (not swallowed)", err)
			}
			assertLedgeredTruth(t, "returned receipt", receipt)
			assertLedgeredTruth(t, "persisted receipt", persistedReceipt(t, svc, "tenant-a", event.EventID))
		})
	}
}

// TestIngestStatusWriteBackendErrorFailsClosed is AC-2: a generic (non-
// conflict) backend error on the status-transition Update is returned
// immediately and must not be swallowed; the response and the persisted
// receipt stay at StatusLedgered.
func TestIngestStatusWriteBackendErrorFailsClosed(t *testing.T) {
	backend := newFailSaveBackend()
	svc := durabilityTestService(t, false, store.NewWithBackend(backend))
	errBackendDown := errors.New("backend unavailable")
	backend.faultStatusWriteWithError(errBackendDown)
	event := testEvent("durability-backend-down", "op-durability", time.Unix(1_700_000_010, 0).UTC())

	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)

	if !errors.Is(err, errBackendDown) {
		t.Fatalf("Ingest err = %v, want the backend sentinel (not swallowed)", err)
	}
	assertLedgeredTruth(t, "returned receipt", receipt)
	assertLedgeredTruth(t, "persisted receipt", persistedReceipt(t, svc, "tenant-a", event.EventID))
}

// TestGetReceiptAfterFailedStatusWriteReportsLedgeredAndRetryIsTruthful is
// AC-3: after a conflict-exhausted status write, GetReceipt reports the
// durable StatusLedgered state (never Indexed/Archived), and an idempotent
// re-ingest of the same event succeeds with Duplicate=true while still
// reporting the persisted StatusLedgered state — no fabricated transition.
func TestGetReceiptAfterFailedStatusWriteReportsLedgeredAndRetryIsTruthful(t *testing.T) {
	backend := newFailSaveBackend()
	svc := durabilityTestService(t, true, store.NewWithBackend(backend))
	backend.faultStatusWriteWithConflicts()
	event := testEvent("durability-retry", "op-durability", time.Unix(1_700_000_010, 0).UTC())

	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived); !errors.Is(err, store.ErrSnapshotConflict) {
		t.Fatalf("first Ingest err = %v, want ErrSnapshotConflict", err)
	}
	// actor "" keeps recordReadAction a no-op so save accounting stays
	// deterministic (the read itself must not fault anything).
	receipt, err := svc.GetReceipt("tenant-a", "", event.EventID)
	if err != nil {
		t.Fatalf("GetReceipt: %v", err)
	}
	assertLedgeredTruth(t, "GetReceipt", receipt)

	// Re-ingest the same event: the ledger CAS is now outside the fault
	// window, hits the duplicate path and returns the persisted receipt.
	receipt, err = svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err != nil {
		t.Fatalf("idempotent re-ingest err = %v, want nil", err)
	}
	if !receipt.Duplicate {
		t.Fatalf("re-ingest Duplicate = false, want true (idempotent retry)")
	}
	assertLedgeredTruth(t, "re-ingest receipt", receipt)
}

// TestIngestWaitForFailsWithoutDurableTransition is AC-4 (failure path):
// waitFor=indexed|archived must never succeed on a transition that was not
// durably written — with a conflict-exhausting status write the ingest fails
// for every (archive config, waitFor) combination instead of passing on the
// in-memory copy.
func TestIngestWaitForFailsWithoutDurableTransition(t *testing.T) {
	cases := []struct {
		name    string
		archive bool
		waitFor string
	}{
		{name: "archive-enabled-wait-indexed", archive: true, waitFor: domain.StatusIndexed},
		{name: "archive-enabled-wait-archived", archive: true, waitFor: domain.StatusArchived},
		{name: "archive-disabled-wait-indexed", archive: false, waitFor: domain.StatusIndexed},
		{name: "archive-disabled-wait-archived", archive: false, waitFor: domain.StatusArchived},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backend := newFailSaveBackend()
			svc := durabilityTestService(t, tc.archive, store.NewWithBackend(backend))
			backend.faultStatusWriteWithConflicts()
			event := testEvent("durability-wait-"+tc.name, "op-durability", time.Unix(1_700_000_010, 0).UTC())

			if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, tc.waitFor); err == nil {
				t.Fatalf("Ingest(waitFor=%s) succeeded without a durable transition", tc.waitFor)
			}
		})
	}
}

// TestIngestWaitForSuccessRequiresPersistedStatus is AC-4 (success path): with
// a healthy backend the waitFor gates are derived from committed state — a
// fresh Store.Read immediately after a successful ingest shows the transition
// was durably written (pins R-3/R-4 success-path parity).
func TestIngestWaitForSuccessRequiresPersistedStatus(t *testing.T) {
	t.Run("archive-disabled-wait-indexed", func(t *testing.T) {
		svc := testService(t, false)
		event := testEvent("durability-ok-indexed", "op-durability", time.Unix(1_700_000_010, 0).UTC())
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusIndexed); err != nil {
			t.Fatalf("Ingest(waitFor=indexed) = %v, want nil", err)
		}
		got := persistedReceipt(t, svc, "tenant-a", event.EventID)
		if got.Status != domain.StatusIndexed && got.Status != domain.StatusArchived {
			t.Fatalf("persisted status = %q, want indexed (or archived)", got.Status)
		}
	})
	t.Run("archive-enabled-wait-archived", func(t *testing.T) {
		svc := testService(t, true)
		event := testEvent("durability-ok-archived", "op-durability", time.Unix(1_700_000_010, 0).UTC())
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived); err != nil {
			t.Fatalf("Ingest(waitFor=archived) = %v, want nil", err)
		}
		if got := persistedReceipt(t, svc, "tenant-a", event.EventID); got.Status != domain.StatusArchived {
			t.Fatalf("persisted status = %q, want archived", got.Status)
		}
	})
}
