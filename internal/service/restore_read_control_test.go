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

// This seam observes the actual Load boundary, including a backend returning a
// valid snapshot after withdrawal. It never writes or simulates durability.
type restoreReadControlBackend struct {
	*checkpointBackend
	mode     string
	cancel   context.CancelFunc
	loads    int
	appends  int
	deadline time.Time
	loadedAt time.Time
}

var errRestoreReadControlLoad = errors.New("injected restore control Load failure")

func (b *restoreReadControlBackend) Load(ctx context.Context) (*store.Snapshot, error) {
	b.loads++
	b.loadedAt = time.Now()
	b.deadline, _ = ctx.Deadline()
	data, err := b.checkpointBackend.Load(ctx)
	if err != nil {
		return nil, err
	}
	switch b.mode {
	case "failure":
		return nil, errRestoreReadControlLoad
	case "cancel-with-snapshot":
		b.cancel()
	case "deadline-with-snapshot", "caller-deadline-with-snapshot":
		<-ctx.Done()
	}
	return data, nil
}

func (b *restoreReadControlBackend) AppendAdminFact(ctx context.Context, fact domain.AdminAction, cap int) error {
	b.appends++
	return b.checkpointBackend.AppendAdminFact(ctx, fact, cap)
}

func restoreReadControlRecord(status string) domain.RestoreRun {
	approved, rejected := pinnedNow.Add(time.Second), pinnedNow.Add(2*time.Second)
	return domain.RestoreRun{ID: "control-read-run", TenantID: "tenant-a", OperationID: "operation",
		Status: status, Reason: "case", CreatedBy: "requester", CreatedAt: pinnedNow,
		ApprovedBy: "original-approver", ApprovedAt: &approved, RejectedBy: "original-rejecter", RejectedAt: &rejected}
}

func TestRestoreReadControlLoadOutcomesKeepOneBudgetAndServeNoWithdrawnValue(t *testing.T) {
	for _, mode := range []string{"success", "failure", "cancel-before", "cancel-with-snapshot", "deadline-with-snapshot", "caller-deadline-with-snapshot"} {
		t.Run(mode, func(t *testing.T) {
			data := store.NewSnapshot()
			want := restoreReadControlRecord(domain.RestoreStatusApproved)
			data.RestoreRuns[want.ID] = want
			original, err := cloneTestSnapshot(data)
			if err != nil {
				t.Fatal("could not capture original control metadata")
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
			got, err := svc.GetRestore(ctx, "tenant-a", want.ID)
			if mode == "success" {
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Error("restore control read changed the stored decision metadata")
				}
			} else if err == nil || got != (domain.RestoreRun{}) {
				t.Error("failed or withdrawn restore control read exposed a successful value")
			}
			if mode == "failure" && !errors.Is(err, errRestoreReadControlLoad) {
				t.Error("restore control read hid its original Load failure")
			}
			deadlineMode := mode == "deadline-with-snapshot" || mode == "caller-deadline-with-snapshot"
			if deadlineMode {
				if !errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || svc.Store.OperationDeadlineExhaustions() != 1 {
					t.Error("late restore metadata escaped or lost the original single deadline classification")
				}
			} else if svc.Store.OperationDeadlineExhaustions() != 0 {
				t.Error("restore control read invented a deadline exhaustion")
			}
			if (mode == "cancel-before" || mode == "cancel-with-snapshot") && !errors.Is(err, context.Canceled) {
				t.Error("restore control read lost caller withdrawal")
			}
			wantLoads := 1
			if mode == "cancel-before" {
				wantLoads = 0
			}
			if backend.loads != wantLoads || backend.saves != 0 || backend.appends != 0 || objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(backend.trail) != 0 {
				t.Error("restore control read retried Load, wrote state/facts or performed WORM I/O")
			}
			if wantLoads == 1 && (backend.deadline.IsZero() || backend.deadline.After(backend.loadedAt.Add(200*time.Millisecond))) {
				t.Error("restore control read restarted its original operation budget")
			}
			if !callerDeadline.IsZero() && !backend.deadline.Equal(callerDeadline) {
				t.Error("restore control read replaced the earlier caller deadline")
			}
			if !reflect.DeepEqual(backend.data, original) {
				t.Error("restore control read changed original stored metadata")
			}
		})
	}
}

func TestRestoreReadControlPreservesScopeHistoricalMetadataAndOwnedOutput(t *testing.T) {
	for _, status := range []string{domain.RestoreStatusPendingApproval, domain.RestoreStatusApproved, domain.RestoreStatusRejected, "historical-diagnostic"} {
		t.Run(status, func(t *testing.T) {
			st, err := store.Open("")
			if err != nil {
				t.Fatal("could not open owned control fixture")
			}
			t.Cleanup(func() { _ = st.Close() })
			svc := newServiceOn(t, st)
			want := restoreReadControlRecord(status)
			if err := st.UpdateControl(testCtx, func(data *store.Snapshot) error {
				data.RestoreRuns[want.ID] = want
				return nil
			}); err != nil {
				t.Fatal("could not prepare historical control metadata")
			}
			before := restoreCreationSnapshot(t, svc)
			objects := &checkpointArchive{}
			svc.Config.Archive = objects
			got, err := svc.GetRestore(testCtx, "tenant-a", want.ID)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatal("restore control read reinterpreted historical fields or required a current tenant registration")
			}
			*got.ApprovedAt, *got.RejectedAt = pinnedNow, pinnedNow
			for _, scope := range []struct {
				tenant, id string
				err        error
			}{{"", want.ID, domain.ErrInvalid}, {"tenant-b", want.ID, domain.ErrNotFound}, {"tenant-a", "absent", domain.ErrNotFound}} {
				if value, err := svc.GetRestore(testCtx, scope.tenant, scope.id); !errors.Is(err, scope.err) || value != (domain.RestoreRun{}) {
					t.Error("restore control read changed scope refusal or exposed another tenant record")
				}
			}
			if !reflect.DeepEqual(restoreCreationSnapshot(t, svc), before) || objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(readFacts(t, svc)) != 0 {
				t.Error("restore metadata escaped ownership or changed evidence/control/silent-read policy")
			}
		})
	}
}
