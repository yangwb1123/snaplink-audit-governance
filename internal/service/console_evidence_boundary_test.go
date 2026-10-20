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

func assertConsoleDeadlines(t *testing.T, backend *timelineBudgetBackend, objects *timelineBudgetArchive, audit bool) {
	t.Helper()
	if backend.readDeadline.IsZero() {
		t.Fatal("Console capture has no operation deadline")
	}
	for _, deadline := range objects.deadlines {
		if !deadline.Equal(backend.readDeadline) {
			t.Fatal("Console source Get restarted the operation deadline")
		}
	}
	if audit && !backend.readDeadline.Equal(backend.auditDeadline) {
		t.Fatal("Console read self-audit restarted the operation deadline")
	}
}

func TestConsoleEvidenceSharedDeadlineAndFailClosedAppend(t *testing.T) {
	for _, kind := range []string{"page", "facets"} {
		for _, tenant := range []string{"tenant-a", ""} {
			for _, mode := range []string{"success", "caller-deadline", "append-failure", "cancel-at-append", "cancel-at-get", "internal"} {
				t.Run(kind+"/"+tenant+"/"+mode, func(t *testing.T) {
					// Reuse generic Load/Get/append instrumentation with exact
					// archived bytes, without unrelated file fixture fsyncs.
					svc, backend, objects := consoleBudgetFixture(t)
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
					appendFailure := errors.New("injected Console read fact append failure")
					switch mode {
					case "append-failure":
						backend.appendError = appendFailure
					case "cancel-at-append":
						backend.cancelAtAppend = cancel
					case "cancel-at-get":
						objects.get = func(getCtx context.Context, _ string, _ int) ([]byte, error) {
							cancel()
							return nil, getCtx.Err()
						}
					}
					saves := backend.saves
					result, err := readConsoleEvidence(ctx, svc, kind, tenant, actor, consoleEvidenceQuery())
					assertConsoleDeadlines(t, backend, objects, mode != "cancel-at-get" && actor != "")
					if backend.saves != saves {
						t.Fatal("Console read rewrote control instead of appending one independent fact")
					}
					if mode == "success" || mode == "caller-deadline" || mode == "internal" {
						facts := 1
						if actor == "" {
							facts = 0
						}
						if err != nil || result.count(kind) != 2 || len(objects.deadlines) != 2 || backend.appendCalls != facts || len(backend.trail) != facts {
							t.Fatal("Console did not authenticate both sources and append-before-serve once")
						}
						if actor != "" {
							fact := backend.trail[0]
							detail := "snaplink-console"
							if kind == "facets" {
								detail = "facets"
							}
							if fact.Action != domain.AdminActionEventRead || fact.Actor != actor || fact.TenantID != tenant || fact.TargetType != "query" || fact.TargetID != "" || fact.Detail != detail {
								t.Fatal("Console read fact scope/actor/target changed")
							}
						}
						if mode == "caller-deadline" {
							deadline, _ := ctx.Deadline()
							if !deadline.Equal(backend.readDeadline) {
								t.Fatal("Console extended the caller deadline")
							}
						}
						return
					}
					if err == nil || !reflect.DeepEqual(result, consoleReadResult{}) || len(backend.trail) != 0 {
						t.Fatal("Console returned partial page/facet counts after failed Get/append")
					}
					if mode == "append-failure" && !errors.Is(err, appendFailure) {
						t.Fatal("Console lost the audit append error")
					}
					if mode != "append-failure" && (!errors.Is(err, context.Canceled) || errors.Is(err, store.ErrOperationDeadline) || svc.Store.OperationDeadlineExhaustions() != 0) {
						t.Fatal("Console caller cancellation became a store timeout")
					}
					if mode == "cancel-at-get" && (len(objects.deadlines) != 1 || backend.appendCalls != 0) {
						t.Fatal("cancelled Console Get continued source reads or self-audit")
					}
				})
			}
		}
	}
}

func TestConsoleEvidenceActualDeadlineAtGetAndAppend(t *testing.T) {
	for _, kind := range []string{"page", "facets"} {
		for _, tenant := range []string{"tenant-a", ""} {
			for _, boundary := range []string{"get", "append"} {
				t.Run(kind+"/"+tenant+"/"+boundary, func(t *testing.T) {
					svc, backend, objects := consoleBudgetFixture(t)
					// Resolve exact fixture bytes before arming the short budget:
					// reaching append must not require filesystem performance.
					data, err := svc.Store.Snapshot(testCtx)
					if err != nil {
						t.Fatal(err)
					}
					cached := map[string][]byte{}
					for _, receipt := range data.Receipts {
						key := eventArchiveKey(receipt.TenantID, receipt.StreamID, receipt.Sequence, receipt.EventID)
						raw, err := objects.Store.Get(testCtx, key)
						if err != nil {
							t.Fatal("could not prepare exact Console source bytes")
						}
						cached[key] = raw
					}
					objects.get = func(_ context.Context, key string, _ int) ([]byte, error) {
						raw, exists := cached[key]
						if !exists {
							return nil, errors.New("unexpected Console source identity")
						}
						return append([]byte(nil), raw...), nil
					}
					// Only this test operation is short; seeding and production
					// configuration keep their original budgets and requirements.
					svc.Store = store.NewWithBackendOptions(backend, store.Options{OperationTimeout: 300 * time.Millisecond})
					if boundary == "get" {
						objects.get = func(ctx context.Context, _ string, _ int) ([]byte, error) {
							<-ctx.Done()
							return nil, ctx.Err()
						}
					} else {
						backend.waitAtAppend = true
					}
					result, err := readConsoleEvidence(testCtx, svc, kind, tenant, "reader", consoleEvidenceQuery())
					if !errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(result, consoleReadResult{}) || svc.Store.OperationDeadlineExhaustions() != 1 || len(backend.trail) != 0 {
						t.Fatal("Console budget exhaustion served data or lost/doubled timeout classification")
					}
					if boundary == "get" && (len(objects.deadlines) != 1 || backend.appendCalls != 0) {
						t.Fatal("Console deadline restarted a Get or appended a fact")
					}
					if boundary == "append" && (len(objects.deadlines) != 2 || backend.appendCalls != 1) {
						t.Fatal("Console deadline test missed the real append after both source reads")
					}
					assertConsoleDeadlines(t, backend, objects, boundary == "append")
				})
			}
		}
	}
}

func TestConsoleEvidenceUnavailablePrefixAndInvalidRequestsServeNothing(t *testing.T) {
	for _, kind := range []string{"page", "facets"} {
		for _, mode := range []string{"missing-after-prefix", "missing-indexed-body", "cancelled-before", "invalid-tenant", "invalid-window", "missing-window", "invalid-size", "unpaired-digest", "invalid-cursor"} {
			if kind == "facets" && mode == "invalid-cursor" {
				continue // facets have always ignored pagination input
			}
			t.Run(kind+"/"+mode, func(t *testing.T) {
				svc, backend, objects := consoleBudgetFixture(t)
				ctx, cancel := context.WithCancel(testCtx)
				defer cancel()
				tenant, query := "tenant-a", consoleEvidenceQuery()
				switch mode {
				case "missing-after-prefix":
					objects.get = func(ctx context.Context, key string, index int) ([]byte, error) {
						if index == 2 {
							return nil, errors.New("injected missing Console evidence")
						}
						return objects.Store.Get(ctx, key)
					}
				case "missing-indexed-body":
					if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
						key := store.EventKey("tenant-a", "console-budget-b")
						receipt := data.Receipts[key]
						receipt.Status = domain.StatusIndexed
						data.Receipts[key] = receipt
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				case "cancelled-before":
					cancel()
				case "invalid-tenant":
					tenant = "tenant/invalid"
				case "invalid-window":
					query.To = query.From
				case "missing-window":
					query.From = time.Time{}
				case "invalid-size":
					query.PageSize = domain.MaxPageSize + 1
				case "unpaired-digest":
					query.PayloadField = "value"
				case "invalid-cursor":
					query.Cursor = "not-a-cursor"
				}
				result, err := readConsoleEvidence(ctx, svc, kind, tenant, "reader", query)
				if err == nil || !reflect.DeepEqual(result, consoleReadResult{}) || backend.appendCalls != 0 || len(backend.trail) != 0 {
					t.Fatal("Console returned a partial result/fact after missing evidence or invalid request")
				}
				if mode == "missing-after-prefix" && len(objects.deadlines) != 2 {
					t.Fatal("Console unavailable-evidence regression missed the real authenticated prefix")
				}
				if mode != "missing-after-prefix" && mode != "missing-indexed-body" && mode != "invalid-cursor" && len(objects.deadlines) != 0 {
					t.Fatal("invalid or withdrawn Console request performed source IO")
				}
			})
		}
	}
}

func TestConsoleEvidenceCapturedCatalogIsOwned(t *testing.T) {
	for _, kind := range []string{"page", "facets"} {
		t.Run(kind, func(t *testing.T) {
			svc, _ := consoleEvidenceFixture(t, true)
			paused := &queryPausedArchive{Store: svc.Config.Archive, entered: make(chan struct{}, 1), release: make(chan struct{})}
			svc.Config.Archive = paused
			done, exited := make(chan error, 1), make(chan struct{})
			var result consoleReadResult
			release := sync.OnceFunc(func() { close(paused.release) })
			t.Cleanup(func() { release(); <-exited })
			go func() {
				defer close(exited)
				var err error
				result, err = readConsoleEvidence(testCtx, svc, kind, "tenant-a", "reader", consoleEvidenceQuery())
				done <- err
			}()
			select {
			case <-paused.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("Console catalog regression never reached source Get")
			}
			if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
				key := store.SchemaKey("tenant-a", "audit.event", 1)
				schema := data.Schemas[key]
				schema.Version = 2
				data.Schemas[store.SchemaKey("tenant-a", "audit.event", 2)] = schema
				delete(data.Schemas, key)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			release()
			if err := <-done; err != nil || result.count(kind) != 1 {
				t.Fatal("concurrent catalog mutation changed already captured Console evidence")
			}
			result, err := readConsoleEvidence(testCtx, svc, kind, "tenant-a", "reader", consoleEvidenceQuery())
			if err == nil || !reflect.DeepEqual(result, consoleReadResult{}) || len(consoleReadFacts(t, svc)) != 1 {
				t.Fatal("new Console call substituted latest Schema for missing exact version")
			}
		})
	}
}

type consoleUnknownAuditBackend struct{ *checkpointBackend }

func (b *consoleUnknownAuditBackend) AppendAdminFact(ctx context.Context, fact domain.AdminAction, cap int) error {
	if err := b.checkpointBackend.AppendAdminFact(ctx, fact, cap); err != nil {
		return err
	}
	return errors.New("injected Console read fact result unknown after append")
}

func TestConsoleEvidenceUnknownAuditOutcomeDoesNotServeOrUndoFact(t *testing.T) {
	for _, kind := range []string{"page", "facets"} {
		t.Run(kind, func(t *testing.T) {
			svc, backend := consoleEvidenceFixture(t, false)
			svc.Store = store.NewWithBackend(&consoleUnknownAuditBackend{checkpointBackend: backend})
			saves := backend.saves
			result, err := readConsoleEvidence(testCtx, svc, kind, "tenant-a", "reader", consoleEvidenceQuery())
			if err == nil || !reflect.DeepEqual(result, consoleReadResult{}) || len(backend.trail) != 1 || backend.saves != saves {
				t.Fatal("unknown append outcome served data, rewrote control or pretended a durable attempt was rolled back")
			}
		})
	}
}
