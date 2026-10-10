package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func snapshotFixture(id int, now time.Time) QuerySnapshot {
	return QuerySnapshot{ID: fmt.Sprintf("%032x", id), TenantID: "tenant-a", Binding: "keyed", Source: "ledger", CreatedAt: now, ExpiresAt: now.Add(QuerySnapshotLifetime), Signature: "authenticated-by-service", Events: []EventReference{{EventID: "event", StreamID: "stream", Sequence: 1, Hash: "hash"}}}
}

func TestQuerySnapshotFileRestartIsolationAndExpiry(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Update(ctx, func(data *Snapshot) error {
		data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	q := snapshotFixture(1, now)
	if err := st.SaveQuerySnapshot(ctx, q, now); err != nil {
		t.Fatal(err)
	}
	q.Events[0].Hash = "caller-mutated"
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	got, err := st.ReadQuerySnapshot(ctx, "tenant-a", q.ID, now)
	if err != nil || got.Events[0].Hash != "hash" {
		t.Fatalf("q=%+v err=%v", got, err)
	}
	got.Events[0].Hash = "read-mutated"
	got, err = st.ReadQuerySnapshot(ctx, "tenant-a", q.ID, now)
	if err != nil || got.Events[0].Hash != "hash" {
		t.Fatalf("q=%+v err=%v", got, err)
	}
	for _, tenant := range []string{"tenant-b", "", "../tenant-a"} {
		if _, err := st.ReadQuerySnapshot(ctx, tenant, q.ID, now); !errors.Is(err, ErrQuerySnapshotUnavailable) {
			t.Fatal(err)
		}
	}
	if _, err := st.ReadQuerySnapshot(ctx, "tenant-a", q.ID, q.ExpiresAt); !errors.Is(err, ErrQuerySnapshotUnavailable) {
		t.Fatal(err)
	}
}

func TestQuerySnapshotAdmissionAndValidation(t *testing.T) {
	ctx := context.Background()
	st, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC()
	for i := 1; i <= MaxActiveQuerySnapshots; i++ {
		if err := st.SaveQuerySnapshot(ctx, snapshotFixture(i, now), now); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.SaveQuerySnapshot(ctx, snapshotFixture(99, now), now); !errors.Is(err, domain.ErrQuotaExceeded) {
		t.Fatal(err)
	}
	// Cleanup only disposable membership, not receipts or publications.
	later := now.Add(QuerySnapshotLifetime)
	if err := st.SaveQuerySnapshot(ctx, snapshotFixture(99, later), later); err != nil {
		t.Fatal(err)
	}
	for _, alter := range []func(*QuerySnapshot){
		func(q *QuerySnapshot) { q.ID = "../../escape" },
		func(q *QuerySnapshot) { q.ExpiresAt = q.CreatedAt.Add(QuerySnapshotLifetime + time.Second) },
		func(q *QuerySnapshot) { q.Events[0].Sequence = 0 },
		func(q *QuerySnapshot) { q.Events = append(q.Events, q.Events[0]) },
	} {
		q := snapshotFixture(100, later)
		alter(&q)
		if err := st.SaveQuerySnapshot(ctx, q, later); !errors.Is(err, ErrQuerySnapshotUnavailable) {
			t.Fatal(err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := st.SaveQuerySnapshot(canceled, snapshotFixture(101, later), later); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestWithOperationSharesDeadlineWithoutLock(t *testing.T) {
	st, err := OpenWithOptions("", Options{OperationTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	before := st.OperationDeadlineExhaustions()
	err = st.WithOperation(context.Background(), func(ctx context.Context) error {
		if err := st.ReadControl(ctx, func(*Snapshot) error { return nil }); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, ErrOperationDeadline) || st.OperationDeadlineExhaustions() != before+1 {
		t.Fatalf("err=%v count=%d", err, st.OperationDeadlineExhaustions())
	}
}
