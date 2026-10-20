package service

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Reuse the real Backend.Save seam: unknown outcomes really persist the
// configuration and fact, then report failure. This is not file/PG durability.
func TestRetentionControlSaveOutcomesKeepAtomicFactAndOriginalBudget(t *testing.T) {
	for _, mode := range []string{"success", "conflict", "exhausted", "failure", "unknown", "deadline", "cancelled", "withdrawn-save", "committed-withdrawal"} {
		t.Run(mode, func(t *testing.T) {
			backend := &restoreDecisionBackend{checkpointBackend: &checkpointBackend{data: store.NewSnapshot()}}
			svc := newServiceOn(t, store.NewWithBackend(backend))
			t.Cleanup(func() { _ = svc.Store.Close() })
			seedTestDomain(t, svc)
			before := restoreCreationSnapshot(t, svc)
			objects := &checkpointArchive{}
			svc.Config.Archive = objects // any attempted Get/Put fails this zero-I/O oracle
			backend.calls, backend.loads, backend.writes, backend.mode = 0, nil, nil, mode
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
			policy := retentionControlPolicy()
			err := svc.SetRetentionPolicy(ctx, "policy-manager", policy)
			known := mode == "success" || mode == "conflict"
			if known && err != nil || !known && err == nil {
				t.Error("policy Save incorrectly reported its known/failed outcome")
			}
			if (mode == "failure" || mode == "unknown") && !errors.Is(err, errRestoreDecisionSave) {
				t.Error("policy hid the original Save failure")
			}
			if mode == "exhausted" && !errors.Is(err, store.ErrSnapshotConflict) {
				t.Error("policy lost finite conflict classification")
			}
			if mode == "deadline" && (!errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || svc.Store.OperationDeadlineExhaustions() != 1) {
				t.Error("policy restarted or misclassified the original operation deadline")
			}
			if (mode == "cancelled" || mode == "withdrawn-save" || mode == "committed-withdrawal") && !errors.Is(err, context.Canceled) {
				t.Error("policy lost caller withdrawal")
			}
			wantSaves := 1
			switch mode {
			case "conflict":
				wantSaves = 3
			case "exhausted":
				wantSaves = 4
			case "cancelled":
				wantSaves = 0
			}
			if backend.calls != wantSaves || objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(readFacts(t, svc)) != 0 {
				t.Error("policy changed finite Saves, performed source I/O or invented a read fact")
			}
			if len(backend.writes) > 0 {
				for _, deadline := range append(append([]time.Time{}, backend.loads...), backend.writes...) {
					if deadline.IsZero() || !deadline.Equal(backend.writes[0]) {
						t.Error("policy conflict retry restarted its operation budget")
					}
				}
			}
			current := restoreCreationSnapshot(t, svc)
			if known || mode == "unknown" || mode == "committed-withdrawal" {
				assertRetentionControlDelta(t, before, current, policy)
			} else if !reflect.DeepEqual(current, before) {
				t.Error("uncommitted policy leaked configuration or a fact")
			}
		})
	}
}

func TestRetentionControlValidationAndRepeatedSetKeepExistingSemantics(t *testing.T) {
	backend := &restoreDecisionBackend{checkpointBackend: &checkpointBackend{data: store.NewSnapshot()}}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	t.Cleanup(func() { _ = svc.Store.Close() })
	seedTestDomain(t, svc)
	policy := retentionControlPolicy()
	for _, field := range []string{"empty-tenant", "missing-tenant", "empty-class", "negative-hot", "negative-warm", "negative-archive"} {
		bad, want := policy, domain.ErrInvalid
		switch field {
		case "empty-tenant":
			bad.TenantID = ""
		case "missing-tenant":
			bad.TenantID, want = "missing-tenant", domain.ErrNotFound
		case "empty-class":
			bad.RetentionClass = " \t"
		case "negative-hot":
			bad.HotDays = -1
		case "negative-warm":
			bad.WarmDays = -1
		case "negative-archive":
			bad.ArchiveDays = -1
		}
		before, saves := restoreCreationSnapshot(t, svc), backend.calls
		if err := svc.SetRetentionPolicy(testCtx, "policy-manager", bad); !errors.Is(err, want) || backend.calls != saves || !reflect.DeepEqual(restoreCreationSnapshot(t, svc), before) {
			t.Errorf("refused policy mutated control or changed classification field=%s", field)
		}
	}
	// Same-value sets still each record the original mutation attempt. This
	// repair does not invent versioning, request idempotency or no-op semantics.
	for range 2 {
		before, saves := restoreCreationSnapshot(t, svc), backend.calls
		if err := svc.SetRetentionPolicy(testCtx, "policy-manager", policy); err != nil || backend.calls != saves+1 {
			t.Fatal("repeated policy set changed original one-commit semantics")
		}
		assertRetentionControlDelta(t, before, restoreCreationSnapshot(t, svc), policy)
	}
}
