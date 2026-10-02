package service

// The store-operation deadline at the service boundary.
//
// A store operation whose outcome is unknown must never become a success at
// the service layer: Service.Ingest is the only writer of a ledgered event, and
// a caller that receives a receipt must be able to trust that the receipt is
// durable. The store returns store.ErrOperationDeadline in that case and the
// service must propagate it unchanged, so the transport can map it to the
// documented retryable class.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// serviceDeadlineBackend spends the whole operation budget inside
// LoadForUpdate, so the legacy (non-split) store path fails the same way the
// split path fails on a hung PostgreSQL statement.
type serviceDeadlineBackend struct{}

func (b *serviceDeadlineBackend) Load(ctx context.Context) (*store.Snapshot, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *serviceDeadlineBackend) LoadForUpdate(ctx context.Context) (*store.Snapshot, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *serviceDeadlineBackend) Save(context.Context, *store.Snapshot) error { return nil }

// TestIngestFailsClosedOnStoreOperationDeadline pins A2+A3 at the service
// boundary: an ingest whose store operation exhausts the deadline returns an
// empty receipt plus ErrOperationDeadline, and the ledger stays empty. A
// caller can therefore never read success out of an unknown write outcome.
func TestIngestFailsClosedOnStoreOperationDeadline(t *testing.T) {
	st := store.NewWithBackendOptions(&serviceDeadlineBackend{}, store.Options{OperationTimeout: 40 * time.Millisecond})
	svc, err := New(st, Config{SegmentSize: 2, SigningSecret: "test-secret", Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1_700_000_010, 0).UTC()
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("deadline-1", "op-deadline", at), domain.StatusLedgered)
	if err == nil {
		t.Fatal("Ingest reported success for a store operation that exhausted its deadline")
	}
	if !errors.Is(err, store.ErrOperationDeadline) {
		t.Fatalf("err=%v, want errors.Is(err, store.ErrOperationDeadline)", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want errors.Is(err, context.DeadlineExceeded)", err)
	}
	if receipt.EventID != "" || receipt.Sequence != 0 {
		t.Fatalf("receipt=%+v, want the zero receipt (no durable outcome was reported)", receipt)
	}
	if got := st.OperationDeadlineExhaustions(); got != 1 {
		t.Fatalf("OperationDeadlineExhaustions()=%d, want exactly 1", got)
	}
}
