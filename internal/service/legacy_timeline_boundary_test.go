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

func TestLegacyTimelineReceiptAndExactSchemaBindings(t *testing.T) {
	key, schemaKey := store.EventKey("tenant-a", "replay-a"), store.SchemaKey("tenant-a", "audit.event", 2)
	mutations := []struct {
		name   string
		mutate func(*store.Snapshot)
	}{
		{"receipt-key", func(d *store.Snapshot) {
			d.Receipts[store.EventKey("tenant-a", "wrong-event")] = d.Receipts[key]
			delete(d.Receipts, key)
		}},
		{"receipt-tenant", func(d *store.Snapshot) { r := d.Receipts[key]; r.TenantID = "tenant-b"; d.Receipts[key] = r }},
		{"receipt-event", func(d *store.Snapshot) { r := d.Receipts[key]; r.EventID = "wrong-event"; d.Receipts[key] = r }},
		{"receipt-hash", func(d *store.Snapshot) { r := d.Receipts[key]; r.Hash = "wrong-hash"; d.Receipts[key] = r }},
		{"schema-tenant", func(d *store.Snapshot) { s := d.Schemas[schemaKey]; s.TenantID = "tenant-b"; d.Schemas[schemaKey] = s }},
		{"schema-id", func(d *store.Snapshot) {
			s := d.Schemas[schemaKey]
			s.SchemaID = "wrong-schema"
			d.Schemas[schemaKey] = s
		}},
		{"schema-version", func(d *store.Snapshot) { s := d.Schemas[schemaKey]; s.Version = 3; d.Schemas[schemaKey] = s }},
		{"missing-exact-schema", func(d *store.Snapshot) {
			s := d.Schemas[schemaKey]
			s.Version = 3
			d.Schemas[store.SchemaKey("tenant-a", "audit.event", 3)] = s
			delete(d.Schemas, schemaKey)
		}},
	}
	for _, kind := range legacyTimelineKinds {
		for _, archived := range []bool{false, true} {
			for _, mutation := range mutations {
				t.Run(fmt.Sprintf("%s/archived-%t/%s", kind, archived, mutation.name), func(t *testing.T) {
					svc, backend, _ := operationReplayFixture(t, archived)
					if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error { mutation.mutate(data); return nil }); err != nil {
						t.Fatal(err)
					}
					result, err := readLegacyTimeline(testCtx, svc, kind, "tenant-a", "reader")
					if err == nil || !reflect.DeepEqual(result, legacyTimelineResult{}) || backend.appendCalls != 0 {
						t.Fatal("misbound receipt or exact Schema produced legacy output or a read fact")
					}
				})
			}
		}
	}
}

func TestLegacyTimelineSharesOriginalBudgetThroughReadAppend(t *testing.T) {
	for _, kind := range legacyTimelineKinds {
		for _, mode := range []string{"success", "caller-deadline", "internal", "append-failure", "cancel-at-get", "cancel-at-append"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
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
				appendError := errors.New("injected legacy read append failure")
				switch mode {
				case "append-failure":
					backend.appendError = appendError
				case "cancel-at-get":
					objects.get = func(getCtx context.Context, _ string, _ int) ([]byte, error) { cancel(); return nil, getCtx.Err() }
				case "cancel-at-append":
					backend.cancelAtAppend = cancel
				}
				saves := backend.saves
				result, err := readLegacyTimeline(ctx, svc, kind, "tenant-a", actor)
				assertTimelineDeadlines(t, backend, objects, actor != "" && mode != "cancel-at-get")
				if backend.saves != saves {
					t.Fatal("legacy read saved a captured control snapshot instead of appending its independent read fact")
				}
				if mode == "success" || mode == "caller-deadline" || mode == "internal" {
					facts := 1
					if actor == "" {
						facts = 0
					}
					if err != nil || legacyTimelineCount(result, kind) != 2 || len(objects.deadlines) != 2 || backend.appendCalls != facts || len(backend.trail) != facts {
						t.Fatal("legacy read did not authenticate both sources and append-before-serve once")
					}
					if actor != "" {
						assertLegacyTimelineFact(t, backend.trail[0], kind, actor)
					}
					if mode == "caller-deadline" {
						deadline, _ := ctx.Deadline()
						if !deadline.Equal(backend.readDeadline) {
							t.Fatal("legacy read extended the earlier caller deadline")
						}
					}
					return
				}
				if err == nil || !reflect.DeepEqual(result, legacyTimelineResult{}) || len(backend.trail) != 0 {
					t.Fatal("failed or cancelled legacy Get/append served partial output")
				}
				if mode == "append-failure" && !errors.Is(err, appendError) {
					t.Fatal("legacy read lost the original read append failure")
				}
				if mode != "append-failure" && (!errors.Is(err, context.Canceled) || errors.Is(err, store.ErrOperationDeadline) || svc.Store.OperationDeadlineExhaustions() != 0) {
					t.Fatal("legacy caller cancellation became a store timeout")
				}
				if mode == "cancel-at-get" && (len(objects.deadlines) != 1 || backend.appendCalls != 0) {
					t.Fatal("cancelled legacy Get continued work or appended a read fact")
				}
			})
		}
	}
}

func TestLegacyTimelineActualDeadlineAtCaptureGetAndAppend(t *testing.T) {
	for _, kind := range legacyTimelineKinds {
		for _, boundary := range []string{"capture", "get", "append"} {
			t.Run(kind+"/"+boundary, func(t *testing.T) {
				svc, backend, objects := operationReplayFixture(t, true)
				// Arm after fixture preparation, then wait at the actual
				// Load/Get/append boundary. No new production performance SLA.
				svc.Store = store.NewWithBackendOptions(backend, store.Options{OperationTimeout: 300 * time.Millisecond})
				switch boundary {
				case "capture":
					svc.Store = store.NewWithBackendOptions(&operationReplayWaitingLoad{backend}, store.Options{OperationTimeout: 300 * time.Millisecond})
				case "get":
					objects.get = func(ctx context.Context, _ string, _ int) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() }
				case "append":
					backend.waitAtAppend = true
				}
				result, err := readLegacyTimeline(testCtx, svc, kind, "tenant-a", "reader")
				if !errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(result, legacyTimelineResult{}) || svc.Store.OperationDeadlineExhaustions() != 1 || len(backend.trail) != 0 {
					t.Fatal("legacy deadline served output or lost/doubled store timeout classification")
				}
				wantGets, wantAppends := 0, 0
				if boundary == "get" {
					wantGets = 1
				} else if boundary == "append" {
					wantGets, wantAppends = 2, 1
				}
				if len(objects.deadlines) != wantGets || backend.appendCalls != wantAppends {
					t.Fatal("legacy deadline test missed its actual boundary or restarted work")
				}
				assertTimelineDeadlines(t, backend, objects, boundary == "append")
			})
		}
	}
}

func TestLegacyTimelineUnknownOrWithdrawnAppendDoesNotServeOrUndoFact(t *testing.T) {
	for _, kind := range legacyTimelineKinds {
		for _, mode := range []string{"unknown-result", "cancel-after-commit"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				svc, backend, _ := operationReplayFixture(t, false)
				ctx, cancel := context.WithCancel(testCtx)
				defer cancel()
				committed := &operationReplayCommittedAppend{checkpointBackend: backend.checkpointBackend}
				if mode == "cancel-after-commit" {
					committed.cancel = cancel
				}
				svc.Store = store.NewWithBackend(committed)
				saves := backend.saves
				result, err := readLegacyTimeline(ctx, svc, kind, "tenant-a", "reader")
				if err == nil || !reflect.DeepEqual(result, legacyTimelineResult{}) || len(backend.trail) != 1 || backend.saves != saves {
					t.Fatal("late/unknown append served legacy output, rewrote control or pretended the attempt was undone")
				}
				assertLegacyTimelineFact(t, backend.trail[0], kind, "reader")
				if mode == "cancel-after-commit" && !errors.Is(err, context.Canceled) {
					t.Fatal("legacy read lost cancellation after the actual append")
				}
			})
		}
	}
}

func TestLegacyTimelineCapturedCatalogUsesExactVersion(t *testing.T) {
	for _, kind := range legacyTimelineKinds {
		t.Run(kind, func(t *testing.T) {
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
				result, err := readLegacyTimeline(ctx, svc, kind, "tenant-a", "reader")
				if err == nil && legacyTimelineCount(result, kind) != 2 {
					err = errors.New("legacy read changed captured membership")
				}
				done <- err
			}()
			select {
			case <-paused.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("legacy catalog test missed the actual Get")
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
				t.Fatal("concurrent catalog edit changed captured exact legacy authentication")
			}
			result, err := readLegacyTimeline(testCtx, svc, kind, "tenant-a", "reader")
			if err == nil || !reflect.DeepEqual(result, legacyTimelineResult{}) || backend.appendCalls != 1 {
				t.Fatal("new legacy read substituted a newer Schema for a missing exact version")
			}
		})
	}
}

func TestLegacyTimelineMissingEvidenceAndInvalidScopeServeNothing(t *testing.T) {
	for _, kind := range legacyTimelineKinds {
		for _, mode := range []string{"missing-after-prefix", "missing-indexed-body", "cancel-before", "empty-tenant", "unknown-tenant", "unmatched-committed-corruption"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				svc, backend, objects := operationReplayFixture(t, true)
				ctx, cancel := context.WithCancel(testCtx)
				defer cancel()
				tenant := "tenant-a"
				switch mode {
				case "missing-after-prefix":
					objects.get = func(ctx context.Context, key string, index int) ([]byte, error) {
						if index == 2 {
							return nil, errors.New("injected missing legacy source")
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
				case "unmatched-committed-corruption":
					other := testEvent("replay-z", "unselected-operation", pinnedNow)
					other.AggregateID = "unselected-aggregate"
					if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, other, domain.StatusArchived); err != nil {
						t.Fatal("could not prepare unmatched committed evidence")
					}
					svc.Config.Archive = &timelineMutatedArchive{Store: objects, mutate: func(event *domain.Event) {
						if event.EventID == other.EventID {
							event.ChangedFields["status"] = domain.FieldChange{After: "forged"}
						}
					}}
				}
				result, err := readLegacyTimeline(ctx, svc, kind, tenant, "reader")
				if err == nil || !reflect.DeepEqual(result, legacyTimelineResult{}) || backend.appendCalls != 0 || len(backend.trail) != 0 {
					t.Fatal("missing/corrupt evidence or withdrawn/invalid scope served partial legacy output/read fact")
				}
				if mode == "missing-after-prefix" && len(objects.deadlines) != 2 {
					t.Fatal("legacy missing-evidence test never authenticated the original prefix")
				}
				if (mode == "cancel-before" || mode == "empty-tenant" || mode == "unknown-tenant") && len(objects.deadlines) != 0 {
					t.Fatal("invalid/withdrawn legacy scope performed source IO")
				}
			})
		}
	}
}
