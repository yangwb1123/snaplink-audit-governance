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

func prepareRestoreDecisionRequest(t *testing.T, svc *Service) domain.RestoreRun {
	t.Helper()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("decision-source", "decision-operation", pinnedNow.Add(time.Second)), domain.StatusLedgered); err != nil {
		t.Fatal("could not commit original restore evidence")
	}
	run, err := svc.CreateRestore(testCtx, "tenant-a", domain.RestoreRequest{OperationID: "decision-operation", Reason: "case"}, "requester")
	if err != nil {
		t.Fatal("could not prepare the original pending request from committed evidence")
	}
	return run
}

func callRestoreDecision(ctx context.Context, svc *Service, tenant, id, actor, target string) (domain.RestoreRun, error) {
	if target == domain.RestoreStatusApproved {
		return svc.ApproveRestore(ctx, tenant, id, actor)
	}
	return svc.RejectRestore(ctx, tenant, id, actor)
}

func restoreDecisionFacts(t *testing.T, data *store.Snapshot, id string) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, fact := range data.AdminActions {
		if fact.TargetID != id || (fact.Action != domain.AdminActionRestoreCreated && fact.Action != domain.AdminActionRestoreApproved && fact.Action != domain.AdminActionRestoreRejected) {
			continue
		}
		if fact.TenantID != "tenant-a" || fact.TargetType != "restore" || fact.ID == "" {
			t.Fatal("restore fact lost its original identity or tenant scope")
		}
		counts[fact.Action]++
	}
	return counts
}

// Script only the real Backend Save boundary; an unknown outcome really commits
// the decision and its fact before returning an error. No timer guesses a commit.
var errRestoreDecisionSave = errors.New("injected restore decision Save outcome")

type restoreDecisionBackend struct {
	*checkpointBackend
	mode      string
	calls     int
	conflicts int
	loads     []time.Time
	writes    []time.Time
	withdraw  context.CancelFunc
}

func (b *restoreDecisionBackend) LoadForUpdate(ctx context.Context) (*store.Snapshot, error) {
	deadline, _ := ctx.Deadline()
	b.loads = append(b.loads, deadline)
	return b.checkpointBackend.LoadForUpdate(ctx)
}

func (b *restoreDecisionBackend) Save(ctx context.Context, data *store.Snapshot) error {
	b.calls++
	deadline, _ := ctx.Deadline()
	b.writes = append(b.writes, deadline)
	if b.conflicts > 0 {
		b.conflicts--
		return store.ErrSnapshotConflict
	}
	switch b.mode {
	case "failure":
		return errRestoreDecisionSave
	case "deadline":
		<-ctx.Done()
		return ctx.Err()
	case "withdrawn-save":
		b.withdraw()
		return ctx.Err()
	}
	if err := b.checkpointBackend.Save(ctx, data); err != nil {
		return err
	}
	if b.mode == "unknown" {
		return errRestoreDecisionSave
	}
	if b.mode == "committed-withdrawal" {
		b.withdraw()
		return ctx.Err()
	}
	return nil
}

func TestRestoreDecisionSaveOutcomesAreAtomicAndKeepOriginalBudget(t *testing.T) {
	for _, target := range []string{domain.RestoreStatusApproved, domain.RestoreStatusRejected} {
		for _, mode := range []string{"success", "conflict", "exhausted", "failure", "unknown", "deadline", "cancelled", "withdrawn-save", "committed-withdrawal"} {
			t.Run(target+"/"+mode, func(t *testing.T) {
				backend := &restoreDecisionBackend{checkpointBackend: &checkpointBackend{data: store.NewSnapshot()}}
				svc := newServiceOn(t, store.NewWithBackend(backend))
				seedTestDomain(t, svc)
				run := prepareRestoreDecisionRequest(t, svc)
				before := leaseSnapshot(t, svc)
				backend.calls, backend.loads, backend.writes = 0, nil, nil
				backend.mode = mode
				if mode == "conflict" {
					backend.conflicts = 2
				} else if mode == "exhausted" {
					backend.conflicts = 4 // existing initial Save plus three bounded retries
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
				got, err := callRestoreDecision(ctx, svc, "tenant-a", run.ID, "decider", target)
				knownSuccess := mode == "success" || mode == "conflict"
				if knownSuccess {
					if err != nil || got.ID != run.ID || got.Status != target {
						t.Error("successful decision lost its committed result")
					}
				} else if err == nil || got != (domain.RestoreRun{}) {
					t.Error("failed or unknown Save exposed an unconfirmed decision")
				}
				if (mode == "failure" || mode == "unknown") && !errors.Is(err, errRestoreDecisionSave) {
					t.Error("decision Save hid the original failure")
				}
				if mode == "exhausted" && !errors.Is(err, store.ErrSnapshotConflict) {
					t.Error("decision lost the bounded conflict classification")
				}
				if mode == "deadline" && (!errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || svc.Store.OperationDeadlineExhaustions() != 1) {
					t.Error("decision deadline classification or counter changed")
				}
				if (mode == "cancelled" || mode == "withdrawn-save" || mode == "committed-withdrawal") && !errors.Is(err, context.Canceled) {
					t.Error("decision lost caller withdrawal")
				}
				wantCalls := 1
				switch mode {
				case "cancelled":
					wantCalls = 0
				case "conflict":
					wantCalls = 3
				case "exhausted":
					wantCalls = 4
				}
				if backend.calls != wantCalls {
					t.Error("decision changed the original finite Save retry count")
				}
				if len(backend.writes) != 0 {
					deadline := backend.writes[0]
					for _, at := range append(append([]time.Time{}, backend.loads...), backend.writes...) {
						if at.IsZero() || !at.Equal(deadline) {
							t.Error("decision retry restarted its original operation budget")
						}
					}
				}
				current := leaseSnapshot(t, svc)
				committed := knownSuccess || mode == "unknown" || mode == "committed-withdrawal"
				facts := restoreDecisionFacts(t, current, run.ID)
				if facts[domain.AdminActionRestoreCreated] != 1 {
					t.Error("decision lost the original request fact")
				}
				decisions := facts[domain.AdminActionRestoreApproved] + facts[domain.AdminActionRestoreRejected]
				if committed {
					stored := current.RestoreRuns[run.ID]
					if stored.Status != target || decisions != 1 || (target == domain.RestoreStatusApproved && (stored.ApprovedBy != "decider" || stored.ApprovedAt == nil || stored.RejectedAt != nil)) || (target == domain.RestoreStatusRejected && (stored.RejectedBy != "decider" || stored.RejectedAt == nil || stored.ApprovedAt != nil)) {
						t.Error("actual commit lost or split the decision and its one atomic fact")
					}
				} else if !reflect.DeepEqual(current, before) || decisions != 0 {
					t.Error("uncommitted decision leaked state or a success fact")
				}
			})
		}
	}
}

func TestRestoreDecisionRefusalsAreZeroWriteAndKeepPrecedence(t *testing.T) {
	backend := &restoreDecisionBackend{checkpointBackend: &checkpointBackend{data: store.NewSnapshot()}}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	seedTestDomain(t, svc)
	run := prepareRestoreDecisionRequest(t, svc)
	for _, target := range []string{domain.RestoreStatusApproved, domain.RestoreStatusRejected} {
		for _, tc := range []struct {
			tenant, id, actor string
			want              error
		}{{"", run.ID, "requester", domain.ErrInvalid}, {"tenant-b", run.ID, "requester", domain.ErrNotFound}, {"tenant-a", "missing", "decider", domain.ErrNotFound}, {"tenant-a", run.ID, "requester", domain.ErrForbidden}} {
			before, saves := leaseSnapshot(t, svc), backend.calls
			got, err := callRestoreDecision(testCtx, svc, tc.tenant, tc.id, tc.actor, target)
			if !errors.Is(err, tc.want) || got != (domain.RestoreRun{}) || backend.calls != saves || !reflect.DeepEqual(leaseSnapshot(t, svc), before) {
				t.Fatal("refused decision changed scope/precedence, saved or exposed a result")
			}
		}
	}
	if _, err := svc.ApproveRestore(testCtx, "tenant-a", run.ID, "first-decider"); err != nil {
		t.Fatal("could not prepare the first durable decision")
	}
	before, saves := leaseSnapshot(t, svc), backend.calls
	for _, target := range []string{domain.RestoreStatusApproved, domain.RestoreStatusRejected} {
		for _, actor := range []string{"requester", "first-decider", "other-decider"} {
			got, err := callRestoreDecision(testCtx, svc, "tenant-a", run.ID, actor, target)
			if !errors.Is(err, domain.ErrConflict) || got != (domain.RestoreRun{}) || backend.calls != saves || !reflect.DeepEqual(leaseSnapshot(t, svc), before) {
				t.Fatal("terminal decision was replayed or conflict lost precedence over duty refusal")
			}
		}
	}
}
