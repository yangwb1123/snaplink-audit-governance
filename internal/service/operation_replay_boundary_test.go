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

func TestOperationReplaySharesOriginalBudgetThroughReadAppend(t *testing.T) {
	for _, mode := range []string{"success", "caller-deadline", "internal", "append-failure", "cancel-at-get", "cancel-at-append"} {
		t.Run(mode, func(t *testing.T) {
			svc, backend, objects := operationReplayFixture(t, true)
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
			appendError := errors.New("injected replay read append failure")
			switch mode {
			case "append-failure":
				backend.appendError = appendError
			case "cancel-at-get":
				objects.get = func(getCtx context.Context, _ string, _ int) ([]byte, error) { cancel(); return nil, getCtx.Err() }
			case "cancel-at-append":
				backend.cancelAtAppend = cancel
			}
			saves := backend.saves
			result, err := svc.ReplayOperation(ctx, "tenant-a", actor, "replay-operation")
			assertTimelineDeadlines(t, backend, objects, actor != "" && mode != "cancel-at-get")
			if backend.saves != saves {
				t.Fatal("replay saved a captured control snapshot instead of appending its independent read fact")
			}
			if mode == "success" || mode == "caller-deadline" || mode == "internal" {
				facts := 1
				if actor == "" {
					facts = 0
				}
				if err != nil || result.EventCount != 2 || len(objects.deadlines) != 2 || backend.appendCalls != facts || len(backend.trail) != facts {
					t.Fatal("replay did not authenticate both sources and append-before-serve once")
				}
				if actor != "" {
					fact := backend.trail[0]
					if fact.Action != domain.AdminActionEventRead || fact.Actor != actor || fact.TenantID != "tenant-a" || fact.TargetType != "operation" || fact.TargetID != "replay-operation" || fact.Detail != "replay" {
						t.Fatal("replay changed its original read fact scope or target")
					}
				}
				if mode == "caller-deadline" {
					deadline, _ := ctx.Deadline()
					if !deadline.Equal(backend.readDeadline) {
						t.Fatal("replay extended the earlier caller deadline")
					}
				}
				return
			}
			if err == nil || !reflect.DeepEqual(result, domain.ReplayResult{}) || len(backend.trail) != 0 {
				t.Fatal("failed or cancelled replay Get/append served a partial folded state")
			}
			if mode == "append-failure" && !errors.Is(err, appendError) {
				t.Fatal("replay lost the original read append failure")
			}
			if mode != "append-failure" && (!errors.Is(err, context.Canceled) || errors.Is(err, store.ErrOperationDeadline) || svc.Store.OperationDeadlineExhaustions() != 0) {
				t.Fatal("replay caller cancellation became a store timeout")
			}
			if mode == "cancel-at-get" && (len(objects.deadlines) != 1 || backend.appendCalls != 0) {
				t.Fatal("cancelled replay Get continued reading or appended a read fact")
			}
		})
	}
}

type operationReplayWaitingLoad struct{ *timelineBudgetBackend }

func (b *operationReplayWaitingLoad) Load(ctx context.Context) (*store.Snapshot, error) {
	b.readDeadline, _ = ctx.Deadline()
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestOperationReplayActualDeadlineAtCaptureGetAndAppend(t *testing.T) {
	for _, boundary := range []string{"capture", "get", "append"} {
		t.Run(boundary, func(t *testing.T) {
			svc, backend, objects := operationReplayFixture(t, true)
			// Arm only after exact fixture bytes are prepared; the actual
			// Load/Get/append boundary waits, not unrelated file preparation.
			svc.Store = store.NewWithBackendOptions(backend, store.Options{OperationTimeout: 300 * time.Millisecond})
			switch boundary {
			case "capture":
				svc.Store = store.NewWithBackendOptions(&operationReplayWaitingLoad{backend}, store.Options{OperationTimeout: 300 * time.Millisecond})
			case "get":
				objects.get = func(ctx context.Context, _ string, _ int) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() }
			case "append":
				backend.waitAtAppend = true
			}
			result, err := svc.ReplayOperation(testCtx, "tenant-a", "reader", "replay-operation")
			if !errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(result, domain.ReplayResult{}) || svc.Store.OperationDeadlineExhaustions() != 1 || len(backend.trail) != 0 {
				t.Fatal("replay deadline served state or lost/doubled store timeout classification")
			}
			wantGets, wantAppends := 0, 0
			if boundary == "get" {
				wantGets = 1
			} else if boundary == "append" {
				wantGets, wantAppends = 2, 1
			}
			if len(objects.deadlines) != wantGets || backend.appendCalls != wantAppends {
				t.Fatal("replay deadline test missed its actual boundary or restarted work")
			}
			assertTimelineDeadlines(t, backend, objects, boundary == "append")
		})
	}
}

type operationReplayCommittedAppend struct {
	*checkpointBackend
	cancel context.CancelFunc
}

func (b *operationReplayCommittedAppend) AppendAdminFact(ctx context.Context, fact domain.AdminAction, cap int) error {
	if err := b.checkpointBackend.AppendAdminFact(ctx, fact, cap); err != nil {
		return err
	}
	if b.cancel != nil {
		b.cancel()
		return nil
	}
	return errors.New("injected replay append result unknown after commit")
}

func TestOperationReplayUnknownOrWithdrawnAppendDoesNotServeOrUndoFact(t *testing.T) {
	for _, mode := range []string{"unknown-result", "cancel-after-commit"} {
		t.Run(mode, func(t *testing.T) {
			svc, backend, _ := operationReplayFixture(t, false)
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			committed := &operationReplayCommittedAppend{checkpointBackend: backend.checkpointBackend}
			if mode == "cancel-after-commit" {
				committed.cancel = cancel
			}
			svc.Store = store.NewWithBackend(committed)
			saves := backend.saves
			result, err := svc.ReplayOperation(ctx, "tenant-a", "reader", "replay-operation")
			if err == nil || !reflect.DeepEqual(result, domain.ReplayResult{}) || len(backend.trail) != 1 || backend.saves != saves {
				t.Fatal("late or unknown append served replay state, rewrote control or pretended the attempt was undone")
			}
			if mode == "cancel-after-commit" && !errors.Is(err, context.Canceled) {
				t.Fatal("replay lost cancellation after the actual append")
			}
		})
	}
}

func TestOperationReplayCapturedCatalogAndCommittedMembership(t *testing.T) {
	t.Run("captured-exact-catalog", func(t *testing.T) {
		svc, backend, _ := operationReplayFixture(t, true)
		paused := &queryPausedArchive{Store: svc.Config.Archive, entered: make(chan struct{}, 1), release: make(chan struct{})}
		svc.Config.Archive = paused
		ctx, cancel := context.WithCancel(testCtx)
		defer cancel()
		release := sync.OnceFunc(func() { close(paused.release) })
		done, exited := make(chan error, 1), make(chan struct{})
		defer func() { release(); cancel(); <-exited }()
		go func() {
			defer close(exited)
			result, err := svc.ReplayOperation(ctx, "tenant-a", "reader", "replay-operation")
			if err == nil && (result.EventCount != 2 || result.State["status"] != "latest") {
				err = errors.New("captured replay changed its original fold")
			}
			done <- err
		}()
		select {
		case <-paused.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("replay catalog test missed the actual Get")
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
			t.Fatal("concurrent catalog edit changed captured exact replay authentication")
		}
		result, err := svc.ReplayOperation(testCtx, "tenant-a", "reader", "replay-operation")
		if err == nil || !reflect.DeepEqual(result, domain.ReplayResult{}) || backend.appendCalls != 1 {
			t.Fatal("new replay substituted a newer Schema for a missing exact version")
		}
	})
	for _, layout := range []int{0, 2} {
		t.Run(fmt.Sprintf("layout-%d", layout), func(t *testing.T) {
			svc, backend, _ := operationReplayFixture(t, false)
			if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
				data.LayoutVersion = layout
				event, err := domain.CloneEvent(data.Events[store.EventKey("tenant-a", "replay-a")])
				if err != nil {
					return err
				}
				event.EventID = "replay-hot-only"
				data.Events[store.EventKey("tenant-a", event.EventID)] = event
				event.TenantID, event.EventID = "tenant-b", "foreign-invalid"
				data.Events[store.EventKey("tenant-b", event.EventID)] = event
				data.Receipts[store.EventKey("tenant-b", event.EventID)] = domain.EventReceipt{TenantID: "tenant-b", EventID: event.EventID}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			result, err := svc.ReplayOperation(testCtx, "tenant-a", "reader", "replay-operation")
			if err != nil || result.EventCount != 2 || backend.appendCalls != 1 {
				t.Fatal("replay included hot-only or foreign evidence or authenticated another tenant")
			}
			if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
				delete(data.Receipts, store.EventKey("tenant-a", "replay-a"))
				delete(data.Receipts, store.EventKey("tenant-a", "replay-b"))
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			result, err = svc.ReplayOperation(testCtx, "tenant-a", "reader", "replay-operation")
			if !errors.Is(err, domain.ErrNotFound) || !reflect.DeepEqual(result, domain.ReplayResult{}) || backend.appendCalls != 1 {
				t.Fatal("uncommitted residue became replay truth")
			}
		})
	}
}

func TestOperationReplayMissingPrefixAndInvalidScopeServeNothing(t *testing.T) {
	for _, mode := range []string{"missing-after-prefix", "missing-indexed-body", "cancel-before", "empty-tenant", "unknown-tenant", "unknown-operation"} {
		t.Run(mode, func(t *testing.T) {
			svc, backend, objects := operationReplayFixture(t, true)
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			tenant, operation := "tenant-a", "replay-operation"
			switch mode {
			case "missing-after-prefix":
				objects.get = func(ctx context.Context, key string, index int) ([]byte, error) {
					if index == 2 {
						return nil, errors.New("injected missing replay source")
					}
					return objects.Store.Get(ctx, key)
				}
			case "missing-indexed-body":
				if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
					key := store.EventKey("tenant-a", "replay-b")
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
			case "unknown-operation":
				operation = "operation-missing"
			}
			result, err := svc.ReplayOperation(ctx, tenant, "reader", operation)
			if err == nil || !reflect.DeepEqual(result, domain.ReplayResult{}) || backend.appendCalls != 0 || len(backend.trail) != 0 {
				t.Fatal("missing evidence or invalid/withdrawn scope served partial replay state/read fact")
			}
			if mode == "missing-after-prefix" && len(objects.deadlines) != 2 {
				t.Fatal("replay missing-evidence test never authenticated the original prefix")
			}
			if (mode == "cancel-before" || mode == "empty-tenant" || mode == "unknown-tenant") && len(objects.deadlines) != 0 {
				t.Fatal("invalid or withdrawn replay scope performed source IO")
			}
		})
	}
}
