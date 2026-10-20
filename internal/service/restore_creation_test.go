package service

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Reuse the actual Save-boundary seam, not an artificial pre-Load callback.
// The replay still reads authenticated FileStore source; only control commit
// outcomes are injected. It keeps its existing separate operation budget.
func TestRestoreCreationSaveOutcomesKeepAtomicFactsAndCommitBudget(t *testing.T) {
	for _, mode := range []string{"success", "conflict", "exhausted", "failure", "unknown", "deadline", "cancelled", "withdrawn-save", "committed-withdrawal"} {
		t.Run(mode, func(t *testing.T) {
			backend := &restoreDecisionBackend{checkpointBackend: &checkpointBackend{data: store.NewSnapshot()}}
			svc := newServiceOn(t, store.NewWithBackend(backend))
			t.Cleanup(func() { _ = svc.Store.Close() })
			seedTestDomain(t, svc)
			objects := &checkpointArchive{Store: &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")}}
			svc.Config.Archive = objects
			if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("creation-source", "creation-operation", pinnedNow.Add(time.Second)), domain.StatusArchived); err != nil {
				t.Fatal("could not commit real original creation source")
			}
			before := restoreCreationSnapshot(t, svc)
			gets, puts := objects.gets.Load(), objects.puts.Load()
			backend.calls, backend.loads, backend.writes = 0, nil, nil
			backend.mode = mode
			if mode == "conflict" {
				backend.conflicts = 2
			} else if mode == "exhausted" {
				backend.conflicts = 4
			}
			if mode == "deadline" {
				svc.Store = store.NewWithBackendOptions(backend, store.Options{OperationTimeout: 200 * time.Millisecond})
			}
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			backend.withdraw = cancel
			if mode == "cancelled" {
				cancel()
			}
			got, err := svc.CreateRestore(ctx, "tenant-a", domain.RestoreRequest{OperationID: "creation-operation", Reason: "case"}, "new-requester")
			known := mode == "success" || mode == "conflict"
			if known {
				if err != nil || got.ID == "" {
					t.Error("known creation commit did not return its request")
				}
			} else if err == nil || got != (domain.RestoreRun{}) {
				t.Error("failed or unknown creation exposed an unconfirmed request")
			}
			if (mode == "failure" || mode == "unknown") && !errors.Is(err, errRestoreDecisionSave) {
				t.Error("creation hid the original Save outcome")
			}
			if mode == "exhausted" && !errors.Is(err, store.ErrSnapshotConflict) {
				t.Error("creation lost bounded conflict classification")
			}
			if mode == "deadline" && (!errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || svc.Store.OperationDeadlineExhaustions() != 1) {
				t.Error("creation commit restarted or misclassified the original deadline")
			}
			if (mode == "cancelled" || mode == "withdrawn-save" || mode == "committed-withdrawal") && !errors.Is(err, context.Canceled) {
				t.Error("creation lost caller withdrawal")
			}
			wantSaves, wantGets := 1, int64(1)
			switch mode {
			case "conflict":
				wantSaves = 3
			case "exhausted":
				wantSaves = 4
			case "cancelled":
				wantSaves, wantGets = 0, 0
			}
			if backend.calls != wantSaves || objects.gets.Load()-gets != wantGets || objects.puts.Load() != puts || len(readFacts(t, svc)) != 0 {
				t.Error("commit retry changed finite Saves, repeated replay, wrote source or added a silent read fact")
			}
			if len(backend.writes) > 0 {
				for _, deadline := range append(append([]time.Time{}, backend.loads...), backend.writes...) {
					if deadline.IsZero() || !deadline.Equal(backend.writes[0]) {
						t.Error("creation commit retry restarted the operation budget")
					}
				}
			}
			current := restoreCreationSnapshot(t, svc)
			committed := known || mode == "unknown" || mode == "committed-withdrawal"
			if committed {
				if len(current.RestoreRuns) != 1 {
					t.Fatal("actual creation commit duplicated or lost its request")
				}
				for _, durable := range current.RestoreRuns {
					assertRestoreCreationDelta(t, before, current, durable)
					if known && !reflect.DeepEqual(got, durable) {
						t.Error("known response did not match its durable creation")
					}
				}
			} else if !reflect.DeepEqual(current, before) {
				t.Error("uncommitted creation leaked a request, fact or evidence mutation")
			}
		})
	}
}
