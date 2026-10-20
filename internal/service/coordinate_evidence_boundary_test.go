package service

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestCoordinateEvidenceSharedDeadlineAndAuditOutcomes(t *testing.T) {
	for _, surface := range []string{"event", "continuation", "projection"} {
		for _, mode := range []string{"success", "internal", "caller-deadline", "append-failure", "append-unknown", "cancel-get", "cancel-get-with-bytes", "cancel-append", "cancel-after-append"} {
			t.Run(surface+"/"+mode, func(t *testing.T) {
				svc, backend, objects := initialQueryFixture(t, true, true)
				actor := "reader"
				if mode == "internal" {
					actor = ""
				}
				outcome := &initialQueryAuditOutcome{timelineBudgetBackend: backend}
				svc.Store = store.NewWithBackend(outcome)
				read := coordinateReader(t, svc, surface, actor)
				ctx, cancel := context.WithCancel(testCtx)
				defer cancel()
				if mode == "caller-deadline" {
					var stop context.CancelFunc
					ctx, stop = context.WithTimeout(ctx, 2*time.Second)
					defer stop()
				}
				appendFailure := errors.New("injected coordinate read audit failure")
				switch mode {
				case "append-failure":
					backend.appendError = appendFailure
				case "append-unknown":
					outcome.unknown = true
				case "cancel-append":
					backend.cancelAtAppend = cancel
				case "cancel-after-append":
					outcome.afterAppend = cancel
				case "cancel-get", "cancel-get-with-bytes":
					objects.get = func(getCtx context.Context, key string, _ int) ([]byte, error) {
						if mode == "cancel-get-with-bytes" {
							raw, err := objects.Store.Get(getCtx, key)
							cancel()
							return raw, err
						}
						cancel()
						return nil, getCtx.Err()
					}
				}
				gets, appends, facts, saves := len(objects.deadlines), backend.appendCalls, len(backend.trail), backend.saves
				page, err := read(ctx)
				if backend.saves != saves || backend.readDeadline.IsZero() || len(objects.deadlines) != gets+1 {
					t.Fatal("selected-coordinate read rewrote control, scanned unrelated source, or lost its operation deadline")
				}
				if !objects.deadlines[gets].Equal(backend.readDeadline) || (backend.appendCalls > appends && !backend.auditDeadline.Equal(backend.readDeadline)) {
					t.Fatal("coordinate Get/audit restarted its original deadline")
				}
				if mode == "success" || mode == "internal" || mode == "caller-deadline" {
					want := 1
					if actor == "" {
						want = 0
					}
					if err != nil || len(page.Items) != 1 || page.Items[0].EventID != "initial-b" || backend.appendCalls != appends+want || len(backend.trail) != facts+want {
						t.Fatal("valid coordinate read lost its one selected member or original read-fact policy")
					}
					if surface == "event" && want != 0 {
						fact := backend.trail[len(backend.trail)-1]
						if fact.TenantID != "tenant-a" || fact.Actor != actor || fact.Action != domain.AdminActionEventRead || fact.TargetType != "event" || fact.TargetID != "initial-b" || fact.Detail != "" {
							t.Fatal("single-event read changed the original safe fact encoding")
						}
					}
					if mode == "caller-deadline" {
						deadline, _ := ctx.Deadline()
						if !deadline.Equal(backend.readDeadline) {
							t.Fatal("coordinate read extended the earlier caller deadline")
						}
					}
					return
				}
				if err == nil || !reflect.DeepEqual(page, domain.QueryResult{}) {
					t.Fatal("failed/withdrawn coordinate read served partial evidence or membership")
				}
				wantFacts := facts
				if mode == "append-unknown" || mode == "cancel-after-append" {
					wantFacts++
				}
				if len(backend.trail) != wantFacts {
					t.Fatal("coordinate read invented success or retracted a durable attempt fact")
				}
				if mode == "append-failure" && !errors.Is(err, appendFailure) {
					t.Fatal("coordinate read lost the audit append failure")
				}
				if mode != "append-failure" && mode != "append-unknown" {
					if !errors.Is(err, context.Canceled) || errors.Is(err, store.ErrOperationDeadline) || svc.Store.OperationDeadlineExhaustions() != 0 {
						t.Fatal("coordinate caller withdrawal was misclassified as store timeout")
					}
				}
				if (mode == "cancel-get" || mode == "cancel-get-with-bytes") && backend.appendCalls != appends {
					t.Fatal("coordinate read appended success after source cancellation")
				}
			})
		}
	}
}

func TestCoordinateEvidenceActualGetAndAppendDeadline(t *testing.T) {
	for _, surface := range []string{"event", "continuation", "projection"} {
		for _, boundary := range []string{"get", "append"} {
			t.Run(surface+"/"+boundary, func(t *testing.T) {
				svc, backend, objects := initialQueryFixture(t, true, true)
				svc.Store = store.NewWithBackendOptions(backend, store.Options{OperationTimeout: 200 * time.Millisecond})
				read := coordinateReader(t, svc, surface, "reader")
				// All exact fixture bytes and cursor metadata exist before the
				// tested operation begins; wait only at the real Get/append boundary.
				if boundary == "get" {
					objects.get = func(ctx context.Context, _ string, _ int) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() }
				} else {
					backend.waitAtAppend = true
				}
				gets, appends, facts := len(objects.deadlines), backend.appendCalls, len(backend.trail)
				page, err := read(testCtx)
				if !errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(page, domain.QueryResult{}) || svc.Store.OperationDeadlineExhaustions() != 1 || len(backend.trail) != facts {
					t.Fatal("coordinate read exceeded its original budget, served evidence, or lost/doubled timeout classification")
				}
				wantAppends := appends
				if boundary == "append" {
					wantAppends++
				}
				if len(objects.deadlines) != gets+1 || backend.appendCalls != wantAppends || !objects.deadlines[gets].Equal(backend.readDeadline) || (boundary == "append" && !backend.auditDeadline.Equal(backend.readDeadline)) {
					t.Fatal("coordinate timeout missed its actual boundary or rearmed the shared deadline")
				}
			})
		}
	}
}

func TestCoordinateEvidenceCapturedCatalogIsOwned(t *testing.T) {
	for _, surface := range []string{"event", "continuation", "projection"} {
		t.Run(surface, func(t *testing.T) {
			svc, backend, objects := initialQueryFixture(t, true, true)
			read := coordinateReader(t, svc, surface, "reader")
			facts := backend.appendCalls
			paused := &queryPausedArchive{Store: objects, entered: make(chan struct{}, 1), release: make(chan struct{})}
			svc.Config.Archive = paused
			release := sync.OnceFunc(func() { close(paused.release) })
			done, exited := make(chan error, 1), make(chan struct{})
			t.Cleanup(func() { release(); <-exited })
			var page domain.QueryResult
			go func() {
				defer close(exited)
				var err error
				page, err = read(testCtx)
				done <- err
			}()
			select {
			case <-paused.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("coordinate read missed its actual source Get")
			}
			if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
				key := store.SchemaKey("tenant-a", "audit.event", 2)
				schema := data.Schemas[key]
				schema.SchemaID = "drifted-after-capture"
				data.Schemas[key] = schema
				return nil
			}); err != nil {
				t.Fatal("coordinate source Get blocked the concurrent catalog change")
			}
			release()
			if err := <-done; err != nil || len(page.Items) != 1 || page.Items[0].EventID != "initial-b" || backend.appendCalls != facts+1 {
				t.Fatal("captured exact catalog was aliased or replaced with a later definition")
			}
			again, err := read(testCtx)
			if err == nil || !reflect.DeepEqual(again, domain.QueryResult{}) || backend.appendCalls != facts+1 {
				t.Fatal("subsequent coordinate read failed to observe current misbound catalog")
			}
		})
	}
}
