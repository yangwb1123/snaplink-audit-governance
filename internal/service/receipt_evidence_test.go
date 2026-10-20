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

func TestReceiptReadRejectsLookupBodyIdentity(t *testing.T) {
	for _, field := range []string{"tenant", "event"} {
		t.Run(field, func(t *testing.T) {
			svc, backend, objects := initialQueryFixture(t, true, true)
			key := store.EventKey("tenant-a", "initial-a")
			if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
				receipt := data.Receipts[key]
				if field == "tenant" {
					receipt.TenantID = "tenant-b"
				} else {
					receipt.EventID = "another-event"
				}
				data.Receipts[key] = receipt
				return nil
			}); err != nil {
				t.Fatal("could not install owned receipt identity fault")
			}
			saves, gets := backend.saves, len(objects.deadlines)
			receipt, err := svc.GetReceipt(testCtx, "tenant-a", "reader", "initial-a")
			if !errors.Is(err, domain.ErrConflict) || !reflect.DeepEqual(receipt, domain.EventReceipt{}) || backend.appendCalls != 0 || backend.saves != saves || len(objects.deadlines) != gets {
				t.Fatal("receipt lookup served another identity, appended success or touched source/control")
			}
		})
	}
}

func TestReceiptReadPreservesMetadataWithoutSourceReads(t *testing.T) {
	for _, status := range []string{domain.StatusAccepted, domain.StatusLedgered, domain.StatusIndexed, domain.StatusArchived, "historical-unknown"} {
		t.Run(status, func(t *testing.T) {
			svc, backend, objects := initialQueryFixture(t, true, true)
			key := store.EventKey("tenant-a", "initial-a")
			if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
				receipt := data.Receipts[key]
				receipt.Status = status
				data.Receipts[key] = receipt
				// Receipt metadata is independent of source availability and catalog.
				data.Schemas = map[string]domain.EventSchema{}
				return nil
			}); err != nil {
				t.Fatal("could not prepare owned receipt metadata matrix")
			}
			want := backend.data.Receipts[key]
			saves, gets := backend.saves, len(objects.deadlines)
			got, err := svc.GetReceipt(testCtx, "tenant-a", "reader", "initial-a")
			if err != nil || !reflect.DeepEqual(got, want) || backend.saves != saves || len(objects.deadlines) != gets || backend.appendCalls != 1 || len(backend.trail) != 1 {
				t.Fatal("receipt read reclassified metadata, needed source/Schema, rewrote control or lost its one fact")
			}
			fact := backend.trail[0]
			if fact.TenantID != "tenant-a" || fact.Actor != "reader" || fact.Action != domain.AdminActionEventRead || fact.TargetType != "event" || fact.TargetID != "initial-a" || fact.Detail != "receipt" {
				t.Fatal("receipt read changed its original safe fact encoding")
			}
			got.Status = "caller-owned-change"
			if backend.data.Receipts[key].Status != want.Status {
				t.Fatal("returned receipt aliases retained metadata")
			}
		})
	}
}

func TestReceiptReadSharedDeadlineAndAuditOutcomes(t *testing.T) {
	for _, mode := range []string{"success", "internal", "caller-deadline", "append-failure", "append-unknown", "cancel-append", "cancel-after-append", "cancel-before-read", "not-found", "foreign-tenant"} {
		t.Run(mode, func(t *testing.T) {
			svc, backend, objects := initialQueryFixture(t, true, true)
			outcome := &initialQueryAuditOutcome{timelineBudgetBackend: backend}
			svc.Store = store.NewWithBackend(outcome)
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			actor, tenant, id := "reader", "tenant-a", "initial-a"
			if mode == "internal" {
				actor = ""
			}
			if mode == "caller-deadline" {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, time.Second)
				defer stop()
			}
			appendFailure := errors.New("injected receipt audit failure")
			switch mode {
			case "append-failure":
				backend.appendError = appendFailure
			case "append-unknown":
				outcome.unknown = true
			case "cancel-append":
				backend.cancelAtAppend = cancel
			case "cancel-after-append":
				outcome.afterAppend = cancel
			case "cancel-before-read":
				cancel()
			case "not-found":
				id = "absent-event"
			case "foreign-tenant":
				tenant = "tenant-b"
			}
			saves, gets := backend.saves, len(objects.deadlines)
			receipt, err := svc.GetReceipt(ctx, tenant, actor, id)
			if backend.saves != saves || len(objects.deadlines) != gets {
				t.Fatal("receipt read accessed source or rewrote captured control")
			}
			if backend.appendCalls != 0 && (backend.readDeadline.IsZero() || !backend.readDeadline.Equal(backend.auditDeadline)) {
				t.Fatal("receipt read restarted its original deadline at audit append")
			}
			if mode == "success" || mode == "internal" || mode == "caller-deadline" {
				wantFacts := 1
				if actor == "" {
					wantFacts = 0
				}
				if err != nil || receipt.EventID != id || backend.appendCalls != wantFacts || len(backend.trail) != wantFacts || backend.readDeadline.IsZero() {
					t.Fatal("valid receipt read lost metadata, budget or its original fact policy")
				}
				if mode == "caller-deadline" {
					deadline, _ := ctx.Deadline()
					if !deadline.Equal(backend.readDeadline) {
						t.Fatal("receipt read extended the caller deadline")
					}
				}
				return
			}
			if err == nil || !reflect.DeepEqual(receipt, domain.EventReceipt{}) {
				t.Fatal("failed/withdrawn receipt read served metadata")
			}
			wantFacts := 0
			if mode == "append-unknown" || mode == "cancel-after-append" {
				wantFacts = 1
			}
			if len(backend.trail) != wantFacts {
				t.Fatal("receipt read invented success or retracted a durable attempt fact")
			}
			if mode == "not-found" || mode == "foreign-tenant" {
				if !errors.Is(err, domain.ErrNotFound) || backend.appendCalls != 0 {
					t.Fatal("missing scoped receipt lost not-found or appended success")
				}
			} else if mode == "append-failure" {
				if !errors.Is(err, appendFailure) {
					t.Fatal("receipt read lost audit append failure")
				}
			} else if mode != "append-unknown" && (!errors.Is(err, context.Canceled) || svc.Store.OperationDeadlineExhaustions() != 0) {
				t.Fatal("receipt caller withdrawal was reclassified as store timeout")
			}
		})
	}
}

func TestReceiptReadActualAppendDeadline(t *testing.T) {
	svc, backend, objects := initialQueryFixture(t, true, true)
	svc.Store = store.NewWithBackendOptions(backend, store.Options{OperationTimeout: 200 * time.Millisecond})
	backend.waitAtAppend = true
	gets, saves := len(objects.deadlines), backend.saves
	receipt, err := svc.GetReceipt(testCtx, "tenant-a", "reader", "initial-a")
	if !errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(receipt, domain.EventReceipt{}) || svc.Store.OperationDeadlineExhaustions() != 1 || len(backend.trail) != 0 || backend.appendCalls != 1 {
		t.Fatal("receipt append timeout served metadata or lost/doubled timeout classification")
	}
	if !backend.readDeadline.Equal(backend.auditDeadline) || backend.saves != saves || len(objects.deadlines) != gets {
		t.Fatal("receipt timeout rearmed the read budget or touched source/control")
	}
}
