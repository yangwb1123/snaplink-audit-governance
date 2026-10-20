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

type receiptLoadBoundary struct {
	*timelineBudgetBackend
	wait   bool
	cancel context.CancelFunc
}

func (b *receiptLoadBoundary) Load(ctx context.Context) (*store.Snapshot, error) {
	data, err := b.timelineBudgetBackend.Load(ctx)
	if err != nil {
		return nil, err
	}
	if b.wait {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if b.cancel != nil {
		b.cancel()
	}
	return data, nil
}

func TestReceiptReadActualLoadDeadlineAndLateCancellation(t *testing.T) {
	for _, mode := range []string{"deadline", "cancel-with-snapshot"} {
		t.Run(mode, func(t *testing.T) {
			svc, backend, _ := initialQueryFixture(t, true, true)
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			boundary := &receiptLoadBoundary{timelineBudgetBackend: backend}
			if mode == "deadline" {
				boundary.wait = true
			} else {
				boundary.cancel = cancel
			}
			svc.Store = store.NewWithBackendOptions(boundary, store.Options{OperationTimeout: 200 * time.Millisecond})
			receipt, err := svc.GetReceipt(ctx, "tenant-a", "reader", "initial-a")
			if err == nil || !reflect.DeepEqual(receipt, domain.EventReceipt{}) || backend.appendCalls != 0 || len(backend.trail) != 0 {
				t.Fatal("late/withdrawn lookup served metadata or appended a successful read")
			}
			if mode == "deadline" {
				if !errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || svc.Store.OperationDeadlineExhaustions() != 1 {
					t.Fatal("actual receipt Load timeout lost/doubled its original classification")
				}
			} else if !errors.Is(err, context.Canceled) || svc.Store.OperationDeadlineExhaustions() != 0 {
				t.Fatal("receipt Load withdrawal was treated as timeout")
			}
		})
	}
}

type receiptPausedAuditBackend struct {
	*checkpointBackend
	entered chan context.Context
	release chan struct{}
}

func (b *receiptPausedAuditBackend) AppendAdminFact(ctx context.Context, fact domain.AdminAction, cap int) error {
	if fact.Action == domain.AdminActionEventRead && fact.Detail == "receipt" {
		b.entered <- ctx
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.checkpointBackend.AppendAdminFact(ctx, fact, cap)
}

func TestReceiptReadAuditPauseAllowsCurrentControlAndLeaseFencing(t *testing.T) {
	svc, instrumented, _ := initialQueryFixture(t, true, true)
	backend := instrumented.checkpointBackend
	svc.Store = store.NewWithBackend(backend)
	seedLeaseExport(t, svc, "receipt-neighbor", "tenant-a")
	claim, won, err := svc.claimPendingExport(testCtx, "receipt-neighbor")
	if err != nil || !won {
		t.Fatal("could not prepare neighboring leased receipt work")
	}
	paused := &receiptPausedAuditBackend{checkpointBackend: backend, entered: make(chan context.Context, 1), release: make(chan struct{})}
	svc.Store = store.NewWithBackend(paused)
	release := sync.OnceFunc(func() { close(paused.release) })
	done, exited := make(chan error, 1), make(chan struct{})
	t.Cleanup(func() { release(); <-exited })
	go func() {
		defer close(exited)
		receipt, err := svc.GetReceipt(testCtx, "tenant-a", "reader", "initial-a")
		if err == nil && receipt.EventID != "initial-a" {
			err = errors.New("receipt changed selected identity")
		}
		done <- err
	}()
	select {
	case <-paused.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("receipt read did not reach its actual independent audit append")
	}
	if err := svc.renewExportLease(testCtx, claim); err != nil {
		t.Fatal("receipt audit held Store.mu across append")
	}
	if _, err := svc.CancelExport(testCtx, "tenant-a", "operator", claim.ID); err != nil {
		t.Fatal("receipt audit blocked owner cancellation")
	}
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		data.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a", ArchiveDays: 365, RetentionClass: "standard"}
		data.LegalHolds["receipt-hold"] = domain.LegalHold{ID: "receipt-hold", TenantID: "tenant-a", CreatedAt: svc.Now()}
		return nil
	}); err != nil {
		t.Fatal("receipt audit blocked current Hold/policy")
	}
	release()
	if err := <-done; err != nil {
		t.Fatal("receipt read failed after its independent audit resumed")
	}
	svc.finishExport(exportClaimContext(testCtx, claim), claim.ID, "completed", "late-object", "late-digest", 1, "")
	current := leaseSnapshot(t, svc)
	if current.Exports[claim.ID].Status != domain.ExportStatusCancelled || current.Exports[claim.ID].ObjectPath != "" || len(current.ExportLeases) != 0 || current.Policies["tenant-a"].ArchiveDays != 365 || current.LegalHolds["receipt-hold"].ID == "" || len(backend.trail) != 1 {
		t.Fatal("receipt read overwrote current terminal/lease/Hold/policy or allowed late publication")
	}
}
