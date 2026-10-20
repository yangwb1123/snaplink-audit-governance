package service

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

type timelineBudgetBackend struct {
	*checkpointBackend
	readDeadline, auditDeadline time.Time
	appendCalls                 int
	appendError                 error
	cancelAtAppend              context.CancelFunc
	waitAtAppend                bool
}

func (b *timelineBudgetBackend) Load(ctx context.Context) (*store.Snapshot, error) {
	b.readDeadline, _ = ctx.Deadline()
	return b.checkpointBackend.Load(ctx)
}

func (b *timelineBudgetBackend) AppendAdminFact(ctx context.Context, fact domain.AdminAction, cap int) error {
	b.appendCalls++
	b.auditDeadline, _ = ctx.Deadline()
	if b.cancelAtAppend != nil {
		b.cancelAtAppend()
	}
	if b.waitAtAppend {
		<-ctx.Done()
		return ctx.Err()
	}
	if b.appendError != nil {
		return b.appendError
	}
	return b.checkpointBackend.AppendAdminFact(ctx, fact, cap)
}

type timelineBudgetArchive struct {
	archive.Store
	deadlines []time.Time
	get       func(context.Context, string, int) ([]byte, error)
}

func (a *timelineBudgetArchive) Get(ctx context.Context, key string) ([]byte, error) {
	deadline, _ := ctx.Deadline()
	a.deadlines = append(a.deadlines, deadline)
	if a.get != nil {
		return a.get(ctx, key, len(a.deadlines))
	}
	return a.Store.Get(ctx, key)
}

func budgetTimelineFixture(t *testing.T) (*Service, *timelineBudgetBackend, *timelineBudgetArchive) {
	t.Helper()
	backend := &timelineBudgetBackend{checkpointBackend: &checkpointBackend{data: store.NewSnapshot()}}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	t.Cleanup(func() { _ = svc.Store.Close() })
	seedTestDomain(t, svc)
	// Only the actual Get/append boundary consumes the tested short budget.
	// FileStore source/control exclusion and durable PG paths are separate.
	objects := &timelineBudgetArchive{Store: newConsoleMemoryArchive()}
	svc.Config.Archive = objects
	for i, id := range []string{"timeline-a", "timeline-b"} {
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent(id, "timeline-operation", pinnedNow.Add(time.Duration(i)*time.Second)), domain.StatusArchived); err != nil {
			t.Fatal(err)
		}
	}
	backend.readDeadline, backend.auditDeadline = time.Time{}, time.Time{}
	return svc, backend, objects
}

func assertTimelineDeadlines(t *testing.T, backend *timelineBudgetBackend, objects *timelineBudgetArchive, audit bool) {
	t.Helper()
	if backend.readDeadline.IsZero() {
		t.Fatal("snapshot capture has no operation deadline")
	}
	for _, deadline := range objects.deadlines {
		if !deadline.Equal(backend.readDeadline) {
			t.Fatal("archive read restarted the timeline operation deadline")
		}
	}
	if audit && !backend.readDeadline.Equal(backend.auditDeadline) {
		t.Fatal("self-audit restarted the timeline operation deadline")
	}
}

func TestTimelinePageCapturedCatalogUsesExactVersion(t *testing.T) {
	for _, kind := range []string{"operation", "aggregate"} {
		t.Run(kind, func(t *testing.T) {
			svc, backend, objects := budgetTimelineFixture(t)
			paused := &queryPausedArchive{Store: objects, entered: make(chan struct{}, 1), release: make(chan struct{})}
			svc.Config.Archive = paused
			release := sync.OnceFunc(func() { close(paused.release) })
			done, exited := make(chan error, 1), make(chan struct{})
			var page domain.QueryResult
			t.Cleanup(func() { release(); <-exited })
			go func() {
				defer close(exited)
				var err error
				page, err = readTimelinePage(testCtx, svc, kind, "tenant-a", "reader", 1, "")
				done <- err
			}()
			select {
			case <-paused.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("catalog test never reached the real Get boundary")
			}
			if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
				key := store.SchemaKey("tenant-a", "audit.event", 1)
				schema := data.Schemas[key]
				schema.Version = 2
				data.Schemas[store.SchemaKey("tenant-a", "audit.event", 2)] = schema
				delete(data.Schemas, key)
				return nil
			}); err != nil {
				t.Fatal("concurrent catalog edit failed")
			}
			release()
			if err := <-done; err != nil || page.Count != 2 || len(page.Items) != 1 || backend.appendCalls != 1 {
				t.Fatal("captured catalog graph changed during lock-free authentication")
			}
			page, err := readTimelinePage(testCtx, svc, kind, "tenant-a", "reader", 1, "")
			if err == nil || !reflect.DeepEqual(page, domain.QueryResult{}) || backend.appendCalls != 1 {
				t.Fatal("new read silently substituted the newest Schema for missing exact version")
			}
		})
	}
}

func TestTimelinePageSharedDeadlineAndFailClosedAppend(t *testing.T) {
	for _, kind := range []string{"operation", "aggregate"} {
		for _, mode := range []string{"success", "caller-deadline", "append-failure", "cancel-at-append", "cancel-at-get"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				svc, backend, objects := budgetTimelineFixture(t)
				ctx, cancel := context.WithCancel(testCtx)
				defer cancel()
				if mode == "caller-deadline" {
					var cancelDeadline context.CancelFunc
					ctx, cancelDeadline = context.WithTimeout(ctx, 5*time.Second)
					defer cancelDeadline()
				}
				appendFailure := errors.New("injected timeline read fact append failure")
				switch mode {
				case "append-failure":
					backend.appendError = appendFailure
				case "cancel-at-append":
					backend.cancelAtAppend = cancel
				case "cancel-at-get":
					objects.get = func(getCtx context.Context, key string, _ int) ([]byte, error) {
						cancel()
						return nil, getCtx.Err()
					}
				}
				saves := backend.saves
				page, err := readTimelinePage(ctx, svc, kind, "tenant-a", "reader", 1, "")
				audited := mode != "cancel-at-get"
				assertTimelineDeadlines(t, backend, objects, audited)
				if backend.saves != saves {
					t.Fatal("timeline page rewrote the control snapshot instead of appending a read fact")
				}
				if mode == "success" || mode == "caller-deadline" {
					if err != nil || page.Count != 2 || len(page.Items) != 1 || page.NextCursor == "" || backend.appendCalls != 1 || len(backend.trail) != 1 {
						t.Fatal("authenticated timeline did not append-before-serve exactly once")
					}
					if mode == "caller-deadline" {
						deadline, _ := ctx.Deadline()
						if !deadline.Equal(backend.readDeadline) {
							t.Fatal("earlier caller deadline was extended")
						}
					}
					return
				}
				if err == nil || !reflect.DeepEqual(page, domain.QueryResult{}) || len(backend.trail) != 0 {
					t.Fatal("failed/cancelled append or Get served partial events/cursor/count")
				}
				if mode == "append-failure" && !errors.Is(err, appendFailure) {
					t.Fatal("timeline lost the read fact append error")
				}
				if mode != "append-failure" && (!errors.Is(err, context.Canceled) || errors.Is(err, store.ErrOperationDeadline) || svc.Store.OperationDeadlineExhaustions() != 0) {
					t.Fatal("caller cancellation was misclassified as a store timeout")
				}
				if mode == "cancel-at-get" && (len(objects.deadlines) != 1 || backend.appendCalls != 0) {
					t.Fatal("cancelled Get continued source reads or self-audit")
				}
			})
		}
	}
}

func TestTimelinePageActualDeadlineStopsAtGetOrAppend(t *testing.T) {
	for _, kind := range []string{"operation", "aggregate"} {
		for _, boundary := range []string{"get", "append"} {
			t.Run(kind+"/"+boundary, func(t *testing.T) {
				svc, backend, objects := budgetTimelineFixture(t)
				// Arm the test-local operation budget only AFTER real fixture
				// preparation. Neither seeding nor production gets a new SLA.
				svc.Store = store.NewWithBackendOptions(backend, store.Options{OperationTimeout: 300 * time.Millisecond})
				if boundary == "get" {
					objects.get = func(ctx context.Context, _ string, _ int) ([]byte, error) {
						<-ctx.Done() // the actual remote Get boundary
						return nil, ctx.Err()
					}
				} else {
					backend.waitAtAppend = true // the actual trail append boundary
				}
				page, err := readTimelinePage(testCtx, svc, kind, "tenant-a", "reader", 1, "")
				if !errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(page, domain.QueryResult{}) || svc.Store.OperationDeadlineExhaustions() != 1 || len(backend.trail) != 0 {
					t.Fatal("exhausted timeline budget served a result or lost/doubled store-timeout classification")
				}
				if boundary == "get" && (len(objects.deadlines) != 1 || backend.appendCalls != 0) {
					t.Fatal("exhausted Get restarted a source read or appended a read fact")
				}
				if boundary == "append" && (len(objects.deadlines) != 2 || backend.appendCalls != 1) {
					t.Fatal("deadline test did not reach the actual trail append after both source reads")
				}
				assertTimelineDeadlines(t, backend, objects, boundary == "append")
			})
		}
	}
}

func TestTimelinePageMissingEvidenceAndCancelledScopeServeNothing(t *testing.T) {
	for _, kind := range []string{"operation", "aggregate"} {
		for _, mode := range []string{"missing-after-prefix", "missing-indexed-body", "cancelled-before", "empty-tenant", "unknown-tenant"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				svc, backend, objects := budgetTimelineFixture(t)
				ctx, cancel := context.WithCancel(testCtx)
				defer cancel()
				tenant := "tenant-a"
				switch mode {
				case "missing-after-prefix":
					objects.get = func(ctx context.Context, key string, index int) ([]byte, error) {
						if index == 2 {
							return nil, errors.New("injected missing archived evidence")
						}
						return objects.Store.Get(ctx, key)
					}
				case "missing-indexed-body":
					if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
						key := store.EventKey("tenant-a", "timeline-b")
						receipt := data.Receipts[key]
						receipt.Status = domain.StatusIndexed
						data.Receipts[key] = receipt
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				case "cancelled-before":
					cancel()
				case "empty-tenant":
					tenant = ""
				case "unknown-tenant":
					tenant = "tenant-missing"
				}
				page, err := readTimelinePage(ctx, svc, kind, tenant, "reader", 1, "")
				if err == nil || !reflect.DeepEqual(page, domain.QueryResult{}) || backend.appendCalls != 0 || len(backend.trail) != 0 {
					t.Fatal("missing evidence or withdrawn/invalid scope served partial output/audit")
				}
				if mode == "missing-after-prefix" && len(objects.deadlines) != 2 {
					t.Fatal("missing-evidence test did not authenticate the actual prefix first")
				}
				if (mode == "cancelled-before" || mode == "empty-tenant" || mode == "unknown-tenant") && len(objects.deadlines) != 0 {
					t.Fatal("withdrawn/invalid/foreign scope caused source IO")
				}
			})
		}
	}
}
