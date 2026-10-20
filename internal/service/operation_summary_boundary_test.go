package service

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestOperationSummarySharesOriginalBudgetThroughReadAppend(t *testing.T) {
	for _, mode := range []string{"success", "caller-deadline", "internal", "append-failure", "cancel-at-get", "cancel-at-append"} {
		t.Run(mode, func(t *testing.T) {
			svc, backend, objects := summaryEvidenceFixture(t, true)
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			actor := "reader"
			if mode == "internal" {
				actor = ""
			}
			if mode == "caller-deadline" {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 5*time.Second)
				defer stop()
			}
			appendError := errors.New("injected summary read append failure")
			switch mode {
			case "append-failure":
				backend.appendError = appendError
			case "cancel-at-get":
				objects.get = func(getCtx context.Context, _ string, _ int) ([]byte, error) { cancel(); return nil, getCtx.Err() }
			case "cancel-at-append":
				backend.cancelAtAppend = cancel
			}
			saves := backend.saves
			result, err := svc.Operation(ctx, "tenant-a", actor, "summary-operation")
			assertTimelineDeadlines(t, backend, objects, actor != "" && mode != "cancel-at-get")
			if backend.saves != saves {
				t.Fatal("summary rewrote captured control instead of appending its independent read fact")
			}
			if mode == "success" || mode == "caller-deadline" || mode == "internal" {
				facts := 1
				if actor == "" {
					facts = 0
				}
				if err != nil || result.EventCount != 2 || len(objects.deadlines) != 2 || backend.appendCalls != facts || len(backend.trail) != facts {
					t.Fatal("summary did not authenticate both sources and append-before-serve exactly once")
				}
				if actor != "" {
					fact := backend.trail[0]
					if fact.Action != domain.AdminActionEventRead || fact.Actor != actor || fact.TenantID != "tenant-a" || fact.TargetType != "operation" || fact.TargetID != "summary-operation" || fact.Detail != "summary" {
						t.Fatal("summary read fact changed its original scope, actor or target")
					}
				}
				if mode == "caller-deadline" {
					deadline, _ := ctx.Deadline()
					if !deadline.Equal(backend.readDeadline) {
						t.Fatal("summary extended the earlier caller deadline")
					}
				}
				return
			}
			if err == nil || !reflect.DeepEqual(result, domain.OperationSummary{}) || len(backend.trail) != 0 {
				t.Fatal("failed/cancelled summary Get or append served partial count/time/outcomes")
			}
			if mode == "append-failure" && !errors.Is(err, appendError) {
				t.Fatal("summary lost the original append error")
			}
			if mode != "append-failure" && (!errors.Is(err, context.Canceled) || errors.Is(err, store.ErrOperationDeadline) || svc.Store.OperationDeadlineExhaustions() != 0) {
				t.Fatal("summary caller cancellation became a store timeout")
			}
			if mode == "cancel-at-get" && (len(objects.deadlines) != 1 || backend.appendCalls != 0) {
				t.Fatal("cancelled summary Get continued reading or appended a fact")
			}
		})
	}
}

func TestOperationSummaryActualDeadlineAtGetAndAppend(t *testing.T) {
	for _, boundary := range []string{"get", "append"} {
		t.Run(boundary, func(t *testing.T) {
			svc, backend, objects := summaryEvidenceFixture(t, true)
			// Exact in-memory bytes are already prepared; only the actual Get
			// or append boundary waits. Production budgets are unchanged.
			svc.Store = store.NewWithBackendOptions(backend, store.Options{OperationTimeout: 300 * time.Millisecond})
			if boundary == "get" {
				objects.get = func(ctx context.Context, _ string, _ int) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() }
			} else {
				backend.waitAtAppend = true
			}
			result, err := svc.Operation(testCtx, "tenant-a", "reader", "summary-operation")
			if !errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(result, domain.OperationSummary{}) || svc.Store.OperationDeadlineExhaustions() != 1 || len(backend.trail) != 0 {
				t.Fatal("summary deadline served data or lost/doubled store-timeout classification")
			}
			if boundary == "get" && (len(objects.deadlines) != 1 || backend.appendCalls != 0) {
				t.Fatal("summary Get timeout restarted a read or appended a fact")
			}
			if boundary == "append" && (len(objects.deadlines) != 2 || backend.appendCalls != 1) {
				t.Fatal("summary timeout did not reach append after both source reads")
			}
			assertTimelineDeadlines(t, backend, objects, boundary == "append")
		})
	}
}

type summaryCommittedAppendBackend struct {
	*checkpointBackend
	cancel context.CancelFunc
}

func (b *summaryCommittedAppendBackend) AppendAdminFact(ctx context.Context, fact domain.AdminAction, cap int) error {
	if err := b.checkpointBackend.AppendAdminFact(ctx, fact, cap); err != nil {
		return err
	}
	if b.cancel != nil {
		b.cancel()
		return nil // a successful append does not authorize late response data
	}
	return errors.New("injected summary append result unknown after commit")
}

func TestOperationSummaryUnknownOrWithdrawnAppendDoesNotServeOrUndoFact(t *testing.T) {
	for _, mode := range []string{"unknown-result", "cancel-after-commit"} {
		t.Run(mode, func(t *testing.T) {
			svc, backend, _ := summaryEvidenceFixture(t, false)
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			committed := &summaryCommittedAppendBackend{checkpointBackend: backend.checkpointBackend}
			if mode == "cancel-after-commit" {
				committed.cancel = cancel
			}
			svc.Store = store.NewWithBackend(committed)
			saves := backend.saves
			result, err := svc.Operation(ctx, "tenant-a", "reader", "summary-operation")
			if err == nil || !reflect.DeepEqual(result, domain.OperationSummary{}) || len(backend.trail) != 1 || backend.saves != saves {
				t.Fatal("late/unknown append served summary data, rewrote control or pretended the attempt was undone")
			}
			if mode == "cancel-after-commit" && !errors.Is(err, context.Canceled) {
				t.Fatal("summary lost cancellation after the actual append")
			}
		})
	}
}

func TestOperationSummaryCapturedCatalogAndUncommittedMembership(t *testing.T) {
	t.Run("captured-exact-catalog", func(t *testing.T) {
		svc, backend, _ := summaryEvidenceFixture(t, true)
		paused := &queryPausedArchive{Store: svc.Config.Archive, entered: make(chan struct{}, 1), release: make(chan struct{})}
		svc.Config.Archive = paused
		ctx, cancel := context.WithCancel(testCtx)
		defer cancel()
		release := sync.OnceFunc(func() { close(paused.release) })
		done, exited := make(chan error, 1), make(chan struct{})
		defer func() { release(); cancel(); <-exited }()
		go func() {
			defer close(exited)
			result, err := svc.Operation(ctx, "tenant-a", "reader", "summary-operation")
			if err == nil && result.EventCount != 2 {
				err = errors.New("captured summary changed membership")
			}
			done <- err
		}()
		select {
		case <-paused.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("summary catalog test missed the real Get boundary")
		}
		if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
			key := store.SchemaKey("tenant-a", "audit.event", 2)
			schema := data.Schemas[key]
			schema.Version = 3
			data.Schemas[store.SchemaKey("tenant-a", "audit.event", 3)] = schema
			delete(data.Schemas, key)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		release()
		if err := <-done; err != nil || backend.appendCalls != 1 {
			t.Fatal("concurrent catalog edit mutated captured exact authentication")
		}
		result, err := svc.Operation(testCtx, "tenant-a", "reader", "summary-operation")
		if err == nil || !reflect.DeepEqual(result, domain.OperationSummary{}) || backend.appendCalls != 1 {
			t.Fatal("new summary substituted a newer Schema for a missing exact version")
		}
	})
	for _, layout := range []int{0, 2} {
		t.Run(fmt.Sprintf("layout-%d", layout), func(t *testing.T) {
			svc, backend, _ := summaryEvidenceFixture(t, false)
			if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
				data.LayoutVersion = layout
				event, err := domain.CloneEvent(data.Events[store.EventKey("tenant-a", "summary-a")])
				if err != nil {
					return err
				}
				event.EventID = "summary-hot-only"
				data.Events[store.EventKey("tenant-a", event.EventID)] = event
				event.TenantID, event.EventID = "tenant-b", "foreign-invalid"
				data.Events[store.EventKey("tenant-b", event.EventID)] = event
				data.Receipts[store.EventKey("tenant-b", event.EventID)] = domain.EventReceipt{TenantID: "tenant-b", EventID: event.EventID}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			result, err := svc.Operation(testCtx, "tenant-a", "reader", "summary-operation")
			if err != nil || result.EventCount != 2 || backend.appendCalls != 1 {
				t.Fatal("summary included hot-only/foreign evidence or authenticated another tenant")
			}
			if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
				delete(data.Receipts, store.EventKey("tenant-a", "summary-a"))
				delete(data.Receipts, store.EventKey("tenant-a", "summary-b"))
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			result, err = svc.Operation(testCtx, "tenant-a", "reader", "summary-operation")
			if !errors.Is(err, domain.ErrNotFound) || !reflect.DeepEqual(result, domain.OperationSummary{}) || backend.appendCalls != 1 {
				t.Fatal("uncommitted residue became summary truth")
			}
		})
	}
}

func TestOperationSummaryMissingPrefixAndInvalidScopeServeNothing(t *testing.T) {
	for _, mode := range []string{"missing-after-prefix", "missing-indexed-body", "cancel-before", "empty-tenant", "unknown-tenant", "empty-operation", "unknown-operation"} {
		t.Run(mode, func(t *testing.T) {
			svc, backend, objects := summaryEvidenceFixture(t, true)
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			tenant, operation := "tenant-a", "summary-operation"
			switch mode {
			case "missing-after-prefix":
				objects.get = func(ctx context.Context, key string, index int) ([]byte, error) {
					if index == 2 {
						return nil, errors.New("injected missing summary source")
					}
					return objects.Store.Get(ctx, key)
				}
			case "missing-indexed-body":
				if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
					key := store.EventKey("tenant-a", "summary-b")
					r := data.Receipts[key]
					r.Status = domain.StatusIndexed
					data.Receipts[key] = r
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			case "cancel-before":
				cancel()
			case "empty-tenant":
				tenant = ""
			case "unknown-tenant":
				tenant = "tenant-missing"
			case "empty-operation":
				operation = ""
			case "unknown-operation":
				operation = "operation-missing"
			}
			result, err := svc.Operation(ctx, tenant, "reader", operation)
			if err == nil || !reflect.DeepEqual(result, domain.OperationSummary{}) || backend.appendCalls != 0 || len(backend.trail) != 0 {
				t.Fatal("missing evidence or invalid/withdrawn scope served a partial summary/read fact")
			}
			if mode == "missing-after-prefix" && len(objects.deadlines) != 2 {
				t.Fatal("summary missing-evidence test never authenticated the prefix")
			}
			if (mode == "cancel-before" || mode == "empty-tenant" || mode == "unknown-tenant" || mode == "empty-operation") && len(objects.deadlines) != 0 {
				t.Fatal("invalid/withdrawn summary scope performed source IO")
			}
		})
	}
}
