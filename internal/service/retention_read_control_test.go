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

// Reuse the actual backend Load seam, not a simulated success or durability
// boundary. Policy observation must not expose the snapshot after withdrawal.
func TestRetentionReadControlLoadOutcomesKeepOneBudgetAndServeNoWithdrawnValue(t *testing.T) {
	for _, mode := range []string{"success", "failure", "cancel-before", "cancel-with-snapshot", "deadline-with-snapshot", "caller-deadline-with-snapshot"} {
		t.Run(mode, func(t *testing.T) {
			data := store.NewSnapshot()
			want := retentionControlPolicy()
			data.Policies[want.TenantID] = want
			original, err := cloneTestSnapshot(data)
			if err != nil {
				t.Fatal("could not capture original policy metadata")
			}
			backend := &restoreReadControlBackend{checkpointBackend: &checkpointBackend{data: data}, mode: mode}
			svc := newServiceOn(t, store.NewWithBackendOptions(backend, store.Options{OperationTimeout: 200 * time.Millisecond}))
			t.Cleanup(func() { _ = svc.Store.Close() })
			objects := &checkpointArchive{}
			svc.Config.Archive = objects
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			var callerDeadline time.Time
			if mode == "caller-deadline-with-snapshot" {
				var earlierCancel context.CancelFunc
				ctx, earlierCancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer earlierCancel()
				callerDeadline, _ = ctx.Deadline()
			}
			backend.cancel = cancel
			if mode == "cancel-before" {
				cancel()
			}
			got, err := svc.GetRetentionPolicy(ctx, "tenant-a")
			if mode == "success" {
				if err != nil || got != want {
					t.Error("policy observation changed the stored configuration")
				}
			} else if err == nil || got != (domain.RetentionPolicy{}) {
				t.Error("failed or withdrawn policy read exposed a successful configuration")
			}
			if mode == "failure" && !errors.Is(err, errRestoreReadControlLoad) {
				t.Error("policy read hid its original Load failure")
			}
			deadlineMode := mode == "deadline-with-snapshot" || mode == "caller-deadline-with-snapshot"
			if deadlineMode {
				if !errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || svc.Store.OperationDeadlineExhaustions() != 1 {
					t.Error("policy read lost the original single deadline classification")
				}
			} else if svc.Store.OperationDeadlineExhaustions() != 0 {
				t.Error("policy observation invented a deadline exhaustion")
			}
			if (mode == "cancel-before" || mode == "cancel-with-snapshot") && !errors.Is(err, context.Canceled) {
				t.Error("policy read lost caller withdrawal")
			}
			wantLoads := 1
			if mode == "cancel-before" {
				wantLoads = 0
			}
			if backend.loads != wantLoads || backend.saves != 0 || backend.appends != 0 || objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(backend.trail) != 0 {
				t.Error("policy observation retried Load, wrote control/facts or performed WORM I/O")
			}
			if wantLoads == 1 && (backend.deadline.IsZero() || backend.deadline.After(backend.loadedAt.Add(200*time.Millisecond))) {
				t.Error("policy observation restarted its original operation budget")
			}
			if !callerDeadline.IsZero() && !backend.deadline.Equal(callerDeadline) {
				t.Error("policy observation replaced the earlier caller deadline")
			}
			if !reflect.DeepEqual(backend.data, original) {
				t.Error("policy observation changed original stored metadata")
			}
		})
	}
}

func TestRetentionReadControlPreservesScopeExactConfigurationAndOwnedOutput(t *testing.T) {
	for _, policy := range []domain.RetentionPolicy{retentionControlPolicy(), {TenantID: "tenant-a", RetentionClass: "historical-class", ArchiveDays: 0}} {
		t.Run(policy.RetentionClass, func(t *testing.T) {
			st, err := store.Open("")
			if err != nil {
				t.Fatal("could not open owned policy control fixture")
			}
			t.Cleanup(func() { _ = st.Close() })
			svc := newServiceOn(t, st)
			neighbor := domain.RetentionPolicy{TenantID: "tenant-b", HotDays: 5, WarmDays: 60, ArchiveDays: 730, RetentionClass: "neighbor"}
			if err := st.UpdateControl(testCtx, func(data *store.Snapshot) error {
				data.Policies[policy.TenantID], data.Policies[neighbor.TenantID] = policy, neighbor
				return nil
			}); err != nil {
				t.Fatal("could not prepare historical configuration without a current tenant registration")
			}
			before := restoreCreationSnapshot(t, svc)
			objects := &checkpointArchive{}
			svc.Config.Archive = objects
			got, err := svc.GetRetentionPolicy(testCtx, "tenant-a")
			if err != nil || got != policy {
				t.Fatal("policy read reinterpreted stored configuration or required current tenant registration")
			}
			got.HotDays, got.ArchiveDays, got.RetentionClass = 999, 999, "caller-copy"
			if next, err := svc.GetRetentionPolicy(testCtx, "tenant-a"); err != nil || next != policy {
				t.Error("returned policy value was not independently owned")
			}
			if next, err := svc.GetRetentionPolicy(testCtx, "tenant-b"); err != nil || next != neighbor {
				t.Error("policy observation mixed independent tenant configurations")
			}
			for _, scope := range []struct {
				tenant string
				err    error
			}{{"", domain.ErrInvalid}, {"absent", domain.ErrNotFound}} {
				if value, err := svc.GetRetentionPolicy(testCtx, scope.tenant); !errors.Is(err, scope.err) || value != (domain.RetentionPolicy{}) {
					t.Error("policy read changed scope refusal or invented a default configuration")
				}
			}
			if !reflect.DeepEqual(restoreCreationSnapshot(t, svc), before) || objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(readFacts(t, svc)) != 0 {
				t.Error("policy observation changed control/evidence or its original silent-read policy")
			}
		})
	}
}

// retentionWithdrawnLoad mirrors the worker fixture's load contract: it can
// still serve an already-withdrawn caller, so the service-layer evaluation
// path is observable without a real backend's own cancellation gate.
type retentionWithdrawnLoad struct{ *checkpointBackend }

func (b retentionWithdrawnLoad) Load(context.Context) (*store.Snapshot, error) {
	return b.checkpointBackend.Load(context.Background())
}

// The worker's between-tenant abort contract attempts the in-flight tenant's
// full recovery→seal→aggregate→archive→retention sequence, so EvaluateRetention
// keeps its original withdrawn-caller evaluation path while the public policy
// read stays fail-closed. Pinned end to end by
// cmd/audit-governance-worker TestRunEvaluatePassAbortsBetweenTenants.
func TestEvaluateRetentionKeepsWithdrawnContextEvaluationPath(t *testing.T) {
	data := store.NewSnapshot()
	policy := retentionControlPolicy()
	data.Policies[policy.TenantID] = policy
	backend := retentionWithdrawnLoad{checkpointBackend: &checkpointBackend{data: data}}
	svc := newServiceOn(t, store.NewWithBackendOptions(backend, store.Options{}))
	t.Cleanup(func() { _ = svc.Store.Close() })
	ctx, cancel := context.WithCancel(testCtx)
	cancel()
	if got, err := svc.GetRetentionPolicy(ctx, policy.TenantID); !errors.Is(err, context.Canceled) || got != (domain.RetentionPolicy{}) {
		t.Fatal("public policy read must stay fail-closed on a withdrawn caller")
	}
	report, err := svc.EvaluateRetention(ctx, policy.TenantID, time.Unix(1_700_000_000, 0).UTC())
	if err != nil {
		t.Fatalf("withdrawn evaluation path must survive the control-only policy read: %v", err)
	}
	if report.Policy != policy || report.Action != "archive_only_immutable_ledger" || report.EligibleEvents != 0 {
		t.Fatalf("unexpected retention report: %+v", report)
	}
}
