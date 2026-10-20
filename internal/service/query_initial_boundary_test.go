package service

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

type initialQueryAuditOutcome struct {
	*timelineBudgetBackend
	unknown     bool
	afterAppend context.CancelFunc
}

func (b *initialQueryAuditOutcome) AppendAdminFact(ctx context.Context, fact domain.AdminAction, cap int) error {
	if err := b.timelineBudgetBackend.AppendAdminFact(ctx, fact, cap); err != nil {
		return err
	}
	if b.afterAppend != nil {
		b.afterAppend()
	}
	if b.unknown {
		return errors.New("injected initial-query audit result unknown after commit")
	}
	return nil
}

func TestInitialQuerySharedDeadlineAndAuditOutcomes(t *testing.T) {
	for _, mode := range []string{"success", "caller-deadline", "internal", "append-failure", "append-unknown", "cancel-get", "cancel-append", "cancel-after-append", "missing-after-prefix"} {
		t.Run(mode, func(t *testing.T) {
			svc, backend, objects := initialQueryFixture(t, true, true)
			outcome := &initialQueryAuditOutcome{timelineBudgetBackend: backend}
			svc.Store = store.NewWithBackend(outcome)
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
			appendFailure := errors.New("injected initial-query audit append failure")
			switch mode {
			case "append-failure":
				backend.appendError = appendFailure
			case "append-unknown":
				outcome.unknown = true
			case "cancel-append":
				backend.cancelAtAppend = cancel
			case "cancel-after-append":
				outcome.afterAppend = cancel
			case "cancel-get":
				objects.get = func(getCtx context.Context, _ string, _ int) ([]byte, error) { cancel(); return nil, getCtx.Err() }
			case "missing-after-prefix":
				objects.get = func(getCtx context.Context, key string, index int) ([]byte, error) {
					if index == 2 {
						return nil, errors.New("injected missing initial-query source")
					}
					return objects.Store.Get(getCtx, key)
				}
			}
			saves := backend.saves
			page, err := svc.QueryEvents(ctx, "tenant-a", actor, initialQuery())
			if backend.saves != saves || backend.readDeadline.IsZero() {
				t.Fatal("initial query rewrote captured control or lost its original operation deadline")
			}
			for _, deadline := range objects.deadlines {
				if !deadline.Equal(backend.readDeadline) {
					t.Fatal("initial-query source Get restarted the operation budget")
				}
			}
			if backend.appendCalls != 0 && !backend.auditDeadline.Equal(backend.readDeadline) {
				t.Fatal("initial-query audit append restarted the operation budget")
			}
			if mode == "success" || mode == "caller-deadline" || mode == "internal" {
				wantFacts := 1
				if actor == "" {
					wantFacts = 0
				}
				if err != nil || page.Count != 2 || len(page.Items) != 1 || page.NextCursor == "" || len(objects.deadlines) != 2 || backend.appendCalls != wantFacts || len(backend.trail) != wantFacts {
					t.Fatal("initial query lost full authenticated membership or append-before-serve semantics")
				}
				if wantFacts != 0 {
					fact := backend.trail[0]
					if fact.Action != domain.AdminActionEventRead || fact.Actor != actor || fact.TenantID != "tenant-a" || fact.TargetType != "query" || fact.TargetID != "" || fact.Detail != "from=2023-11-14T22:12:20Z to=2023-11-14T22:14:20Z" {
						t.Fatal("initial-query read fact changed its original safe encoding")
					}
				}
				if mode == "caller-deadline" {
					deadline, _ := ctx.Deadline()
					if !deadline.Equal(backend.readDeadline) {
						t.Fatal("initial query extended the caller deadline")
					}
				}
				return
			}
			if err == nil || !reflect.DeepEqual(page, domain.QueryResult{}) {
				t.Fatal("failed or withdrawn initial query returned partial items/count/cursor/observation")
			}
			wantFacts := 0
			if mode == "append-unknown" || mode == "cancel-after-append" {
				wantFacts = 1
			}
			if len(backend.trail) != wantFacts {
				t.Fatal("initial query invented an audit success or pretended a committed attempt was rolled back")
			}
			if mode == "append-failure" && !errors.Is(err, appendFailure) {
				t.Fatal("initial query hid its audit append failure")
			}
			if mode == "cancel-get" || mode == "cancel-append" || mode == "cancel-after-append" {
				if !errors.Is(err, context.Canceled) || errors.Is(err, store.ErrOperationDeadline) || svc.Store.OperationDeadlineExhaustions() != 0 {
					t.Fatal("withdrawn initial query became a store deadline failure")
				}
			}
			if mode == "cancel-get" && (len(objects.deadlines) != 1 || backend.appendCalls != 0) {
				t.Fatal("cancelled initial query continued source work or appended a read fact")
			}
			if mode == "missing-after-prefix" && (len(objects.deadlines) != 2 || backend.appendCalls != 0) {
				t.Fatal("missing-source regression did not authenticate a real prefix before rejecting the whole selection")
			}
		})
	}
}

func TestInitialQueryActualDeadlineAtGetAndAppend(t *testing.T) {
	for _, boundary := range []string{"get", "append"} {
		t.Run(boundary, func(t *testing.T) {
			svc, backend, objects := initialQueryFixture(t, true, true)
			// The operation starts only after all fixture Put/Save preparation.
			svc.Store = store.NewWithBackendOptions(backend, store.Options{OperationTimeout: 300 * time.Millisecond})
			if boundary == "get" {
				objects.get = func(ctx context.Context, _ string, _ int) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() }
			} else {
				backend.waitAtAppend = true
			}
			page, err := svc.QueryEvents(testCtx, "tenant-a", "reader", initialQuery())
			if !errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(page, domain.QueryResult{}) || svc.Store.OperationDeadlineExhaustions() != 1 || len(backend.trail) != 0 {
				t.Fatal("initial query served data after actual deadline or lost/doubled timeout classification")
			}
			wantGets, wantAppends := 1, 0
			if boundary == "append" {
				wantGets, wantAppends = 2, 1
				if !backend.auditDeadline.Equal(backend.readDeadline) {
					t.Fatal("actual query append restarted the original deadline")
				}
			}
			if len(objects.deadlines) != wantGets || backend.appendCalls != wantAppends {
				t.Fatal("initial-query timeout regression missed the intended real boundary")
			}
			for _, deadline := range objects.deadlines {
				if !deadline.Equal(backend.readDeadline) {
					t.Fatal("actual query Get restarted the original deadline")
				}
			}
		})
	}
}

func TestInitialQueryFileGetPreservesConcurrentTerminalControl(t *testing.T) {
	svc := queryMemoryService(t, true) // real FileStore source objects, not a throughput oracle
	var clock atomic.Int64
	clock.Store(pinnedNow.UnixNano())
	svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	for i, id := range []string{"initial-a", "initial-b"} {
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent(id, "initial-operation", pinnedNow.Add(time.Duration(i)*time.Second)), domain.StatusArchived); err != nil {
			t.Fatal("could not commit real initial-query file source")
		}
	}
	seedLeaseExport(t, svc, "initial-neighbor", "tenant-a")
	claim, won, err := svc.claimPendingExport(testCtx, "initial-neighbor")
	if err != nil || !won {
		t.Fatal("could not prepare original neighboring leased job")
	}
	paused := &queryPausedArchive{Store: svc.Config.Archive, entered: make(chan struct{}, 1), release: make(chan struct{})}
	svc.Config.Archive = paused
	ctx, cancel := context.WithCancel(testCtx)
	release := sync.OnceFunc(func() { close(paused.release) })
	done, exited := make(chan error, 1), make(chan struct{})
	var controlExited chan struct{}
	t.Cleanup(func() {
		release()
		cancel()
		<-exited
		if controlExited != nil {
			<-controlExited
		}
	})
	var page domain.QueryResult
	go func() {
		defer close(exited)
		var err error
		page, err = svc.QueryEvents(ctx, "tenant-a", "reader", initialQuery())
		done <- err
	}()
	select {
	case <-paused.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("initial query never reached its actual file Get")
	}
	control, finished := make(chan error, 1), make(chan struct{})
	controlExited = finished
	go func() {
		defer close(finished)
		clock.Store(pinnedNow.Add(ExportHeartbeatInterval).UnixNano())
		if err := svc.renewExportLease(ctx, claim); err != nil {
			control <- err
			return
		}
		if _, err := svc.CancelExport(ctx, "tenant-a", "operator", claim.ID); err != nil {
			control <- err
			return
		}
		control <- svc.Store.UpdateControl(ctx, func(data *store.Snapshot) error {
			data.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a", ArchiveDays: 365, RetentionClass: "standard"}
			data.LegalHolds["initial-hold"] = domain.LegalHold{ID: "initial-hold", TenantID: "tenant-a", Name: "Concurrent Hold", CreatedAt: svc.Now()}
			return nil
		})
	}()
	select {
	case err := <-control:
		if err != nil {
			t.Fatal("initial-query Get blocked lease renewal/cancellation/control mutation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("initial-query Get retained Store.mu")
	}
	release()
	if err := <-done; err != nil || page.Count != 2 || len(page.Items) != 1 || page.Items[0].EventID != "initial-a" || page.NextCursor == "" {
		t.Fatal("initial query failed to authenticate its original owned membership")
	}
	svc.finishExport(exportClaimContext(testCtx, claim), claim.ID, "completed", "late-object", "late-digest", 1, "")
	current := leaseSnapshot(t, svc)
	if current.Exports[claim.ID].Status != domain.ExportStatusCancelled || current.Exports[claim.ID].ObjectPath != "" || len(current.ExportLeases) != 0 || current.Policies["tenant-a"].ArchiveDays != 365 || current.LegalHolds["initial-hold"].ID == "" || len(readFacts(t, svc)) != 1 {
		t.Fatal("initial query overwrote current terminal/lease/Hold/policy or relaxed late-owner fencing")
	}
}

func TestInitialQueryCapturedExactCatalogAndResultAreOwned(t *testing.T) {
	svc, backend, objects := initialQueryFixture(t, true, true)
	paused := &queryPausedArchive{Store: objects, entered: make(chan struct{}, 1), release: make(chan struct{})}
	svc.Config.Archive = paused
	release := sync.OnceFunc(func() { close(paused.release) })
	done, exited := make(chan error, 1), make(chan struct{})
	t.Cleanup(func() { release(); <-exited })
	var page domain.QueryResult
	go func() {
		defer close(exited)
		var err error
		page, err = svc.QueryEvents(testCtx, "tenant-a", "reader", initialQuery())
		done <- err
	}()
	select {
	case <-paused.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("initial-query catalog regression missed the real Get")
	}
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		key := store.SchemaKey("tenant-a", "audit.event", 2)
		schema := data.Schemas[key]
		schema.Version++
		data.Schemas[store.SchemaKey("tenant-a", "audit.event", schema.Version)] = schema
		delete(data.Schemas, key)
		return nil
	}); err != nil {
		t.Fatal("could not change current catalog during initial-query capture")
	}
	release()
	if err := <-done; err != nil || page.Count != 2 || backend.appendCalls != 1 {
		t.Fatal("concurrent catalog change altered already captured exact query evidence")
	}
	page.Items[0].Actor.Roles[0] = "caller-mutation"
	page.Items[0].Payload["value"] = "caller-mutation"
	page, err := svc.QueryEvents(testCtx, "tenant-a", "reader", initialQuery())
	if err == nil || !reflect.DeepEqual(page, domain.QueryResult{}) || backend.appendCalls != 1 {
		t.Fatal("fresh query substituted latest Schema for missing exact version")
	}
	data := leaseSnapshot(t, svc)
	if data.Schemas[store.SchemaKey("tenant-a", "audit.event", 3)].Version != 3 {
		t.Fatal("query wrote its captured catalog back over the current directory")
	}
}
