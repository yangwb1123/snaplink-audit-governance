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

// Faults are armed after fixture preparation and only at the actual new-job
// Save. An unknown Save can already have committed; neither result creates an
// execution receipt, rolls back the parent or permits an automatic new child.
type preflightBudgetSaveBackend struct {
	*timelineBudgetBackend
	saveDeadline time.Time
	saveError    error
	unknownSave  bool
	waitAtSave   bool
	cancelAtSave context.CancelFunc
}

func (b *preflightBudgetSaveBackend) Save(ctx context.Context, data *store.Snapshot) error {
	newJob := false
	b.mu.Lock()
	for id, job := range data.Exports {
		if _, exists := b.data.Exports[id]; !exists && job.Status == "pending" {
			newJob = true
		}
	}
	b.mu.Unlock()
	if newJob {
		b.saveDeadline, _ = ctx.Deadline()
		if b.cancelAtSave != nil {
			b.cancelAtSave()
			return ctx.Err()
		}
		if b.waitAtSave {
			<-ctx.Done()
			return ctx.Err()
		}
		if b.saveError != nil && !b.unknownSave {
			return b.saveError
		}
	}
	if err := b.checkpointBackend.Save(ctx, data); err != nil {
		return err
	}
	if newJob {
		return b.saveError
	}
	return nil
}

func preflightBudgetFixture(t *testing.T) (*Service, *preflightBudgetSaveBackend, *timelineBudgetArchive, domain.Query, domain.ExportJob) {
	t.Helper()
	svc, base, objects, _, query := preflightEvidenceFixture(t)
	parent := seedPreflightParent(t, svc, query)
	accessNonmatchingHold(t, svc)
	backend := &preflightBudgetSaveBackend{timelineBudgetBackend: &timelineBudgetBackend{checkpointBackend: base}}
	observed := &timelineBudgetArchive{Store: objects}
	svc.Store, svc.Config.Archive = store.NewWithBackend(backend), observed
	return svc, backend, observed, query, parent
}

func TestExportPreflightQueryHoldAuditAndSaveShareOneDeadline(t *testing.T) {
	for _, surface := range []string{"create", "retry"} {
		for _, mode := range []string{"success", "caller-deadline", "audit-failure", "Hold-Get-failure", "cancel-in-Hold-Get", "cancel-at-Save", "failed-Save", "unknown-Save"} {
			t.Run(surface+"/"+mode, func(t *testing.T) {
				svc, backend, objects, query, parent := preflightBudgetFixture(t)
				ctx, cancel := context.WithCancel(testCtx)
				defer cancel()
				primary := errors.New("injected preflight dependency failure")
				switch mode {
				case "caller-deadline":
					var stop context.CancelFunc
					ctx, stop = context.WithTimeout(ctx, 5*time.Second)
					defer stop()
				case "audit-failure":
					backend.appendError = primary
				case "cancel-at-Save":
					backend.cancelAtSave = cancel
				case "failed-Save", "unknown-Save":
					backend.saveError, backend.unknownSave = primary, mode == "unknown-Save"
				case "Hold-Get-failure", "cancel-in-Hold-Get":
					objects.get = func(getCtx context.Context, key string, index int) ([]byte, error) {
						if index == 2 {
							if mode == "cancel-in-Hold-Get" {
								cancel()
								return nil, getCtx.Err()
							}
							return nil, primary
						}
						return objects.Store.Get(getCtx, key)
					}
				}
				id, err := callExportPreflight(t, ctx, svc, surface, query)
				assertTimelineDeadlines(t, backend.timelineBudgetBackend, objects, true)
				if !backend.saveDeadline.IsZero() && !backend.saveDeadline.Equal(backend.readDeadline) {
					t.Fatal("new-job Save restarted the preflight deadline")
				}
				if mode == "caller-deadline" {
					deadline, _ := ctx.Deadline()
					if !deadline.Equal(backend.readDeadline) {
						t.Fatal("preflight extended an earlier caller deadline")
					}
				}
				if mode == "success" || mode == "caller-deadline" {
					data := leaseSnapshot(t, svc)
					if err != nil || id == "" || len(objects.deadlines) != 2 || len(backend.trail) != 1 || backend.saveDeadline.IsZero() || len(data.Exports) != 2 || !reflect.DeepEqual(data.Exports[parent.ID], parent) {
						t.Fatal("successful preflight lost original audit, current Hold or new-job commit")
					}
					return
				}
				if err == nil || id != "" {
					t.Fatal("failed, unknown or withdrawn preflight returned success metadata")
				}
				if mode == "cancel-in-Hold-Get" || mode == "cancel-at-Save" {
					if !errors.Is(err, context.Canceled) || errors.Is(err, store.ErrOperationDeadline) || svc.Store.OperationDeadlineExhaustions() != 0 {
						t.Fatal("caller withdrawal became a new budget or store-timeout failure")
					}
				} else if !errors.Is(err, primary) {
					t.Fatal("preflight lost the real dependency failure")
				}
				wantReads := 1 // a read attempt is not a committed export or bytes delivered
				if mode == "audit-failure" {
					wantReads = 0
					if len(objects.deadlines) != 1 || !backend.saveDeadline.IsZero() {
						t.Fatal("failed query self-audit continued to Hold or new-job Save")
					}
				}
				if len(backend.trail) != wantReads {
					t.Fatal("preflight lost or duplicated the actual read attempt fact")
				}
				if mode != "unknown-Save" {
					assertPreflightNoCreation(t, svc, parent)
					return
				}
				data := leaseSnapshot(t, svc)
				if len(data.Exports) != 2 || !reflect.DeepEqual(data.Exports[parent.ID], parent) {
					t.Fatal("unknown Save was inferred to be a rollback or reset the terminal parent")
				}
				if surface == "retry" {
					// Explicit reconciliation of the linked attempt is metadata
					// only, even though its original Save returned an error.
					backend.saveError = nil
					child, err := svc.RetryExport(testCtx, "tenant-a", "operator", parent.ID)
					if err != nil || child.RetryOf != parent.ID || len(leaseSnapshot(t, svc).Exports) != 2 || len(backend.trail) != wantReads {
						t.Fatal("unknown retry Save duplicated the child or re-read evidence on reconciliation")
					}
				}
			})
		}
	}
}

func TestExportPreflightActualDeadlineAtHoldGetOrNewJobSave(t *testing.T) {
	for _, surface := range []string{"create", "retry"} {
		for _, boundary := range []string{"Hold-Get", "Save"} {
			t.Run(surface+"/"+boundary, func(t *testing.T) {
				svc, backend, objects, query, parent := preflightBudgetFixture(t)
				// Short test-local budget is armed after all preparation; only
				// the actual Get/Save waits. Production limits are untouched.
				svc.Store = store.NewWithBackendOptions(backend, store.Options{OperationTimeout: 300 * time.Millisecond})
				if boundary == "Hold-Get" {
					objects.get = func(ctx context.Context, key string, index int) ([]byte, error) {
						if index == 2 {
							<-ctx.Done()
							return nil, ctx.Err()
						}
						return objects.Store.Get(ctx, key)
					}
				} else {
					backend.waitAtSave = true
				}
				id, err := callExportPreflight(t, testCtx, svc, surface, query)
				if id != "" || !errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || svc.Store.OperationDeadlineExhaustions() != 1 || len(backend.trail) != 1 || len(objects.deadlines) != 2 {
					t.Fatal("expired real preflight boundary returned success or lost/doubled store-timeout classification")
				}
				assertTimelineDeadlines(t, backend.timelineBudgetBackend, objects, true)
				if boundary == "Save" && !backend.saveDeadline.Equal(backend.readDeadline) {
					t.Fatal("Save did not inherit the original exhausted preflight deadline")
				}
				assertPreflightNoCreation(t, svc, parent)
			})
		}
	}
}
