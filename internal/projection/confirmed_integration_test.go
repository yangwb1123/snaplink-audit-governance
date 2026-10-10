package projection

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestClickHouseConfirmedProjectionIntegration(t *testing.T) {
	dsn := os.Getenv("AUDIT_TEST_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("requires disposable ClickHouse")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	st, err := Open(dsn + "?async_insert=1&wait_for_async_insert=0")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	var async bool
	if err := st.db.QueryRowContext(ctx, "SELECT getSetting('async_insert')").Scan(&async); err != nil || async {
		t.Fatalf("async=%t err=%v", async, err)
	}
	one := ledgeredFixture("confirmed-one")
	one.TenantID, one.StreamID, one.Sequence, one.Hash, one.PrevHash = "confirm-it", "stream", 1, "h1", ""
	three := one
	three.EventID, three.Sequence, three.Hash, three.PrevHash = "confirmed-three", 3, "h3", "h2"
	for _, event := range []domain.Event{one, one, three} {
		if err := st.Insert(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	if count, err := st.CountTenant(ctx, one.TenantID); err != nil || count != 2 {
		t.Fatalf("logical count=%d err=%v", count, err)
	}
	receipt := domain.EventReceipt{TenantID: one.TenantID, EventID: one.EventID, StreamID: one.StreamID, Sequence: 1, Hash: "h1"}
	if confirmed, err := st.Confirmed(ctx, receipt); err != nil || !confirmed {
		t.Fatalf("confirmed=%t err=%v", confirmed, err)
	}
	receipt.TenantID = "other-tenant"
	if confirmed, err := st.Confirmed(ctx, receipt); err != nil || confirmed {
		t.Fatalf("cross tenant confirmed=%t err=%v", confirmed, err)
	}
	if watermark, err := st.Watermark(ctx, one.TenantID, one.StreamID); err != nil || watermark.Sequence != 1 || !watermark.Gap {
		t.Fatalf("gap watermark=%+v err=%v", watermark, err)
	}
	two := one
	two.EventID, two.Sequence, two.Hash, two.PrevHash = "confirmed-two", 2, "h2", "h1"
	// A late business occurrence must repair the sequence prefix, not be
	// excluded by an occurred_at high-water mark.
	two.OccurredAt = one.OccurredAt.Add(-time.Hour)
	if err := st.Insert(ctx, two); err != nil {
		t.Fatal(err)
	}
	if watermark, err := st.Watermark(ctx, one.TenantID, one.StreamID); err != nil || watermark.Sequence != 3 || watermark.Gap {
		t.Fatalf("repaired watermark=%+v err=%v", watermark, err)
	}
	// Same identity under another tenant remains independent.
	foreign := one
	foreign.TenantID = "other-tenant"
	if err := st.Insert(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	if count, err := st.CountTenant(ctx, foreign.TenantID); err != nil || count != 1 {
		t.Fatalf("foreign count=%d err=%v", count, err)
	}
	// Physical merging must not erase conflicts at a fact identity.
	conflict := one
	conflict.Hash = "conflicting"
	if err := st.Insert(ctx, conflict); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, "OPTIMIZE TABLE audit_projection_events FINAL"); err != nil {
		t.Fatal(err)
	}
	receipt.TenantID = one.TenantID
	if _, err := st.Confirmed(ctx, receipt); !errors.Is(err, ErrProjectionConflict) {
		t.Fatal(err)
	}
	if _, err := st.CountTenant(ctx, one.TenantID); !errors.Is(err, ErrProjectionConflict) {
		t.Fatal(err)
	}
	if _, err := st.Watermark(ctx, one.TenantID, one.StreamID); !errors.Is(err, ErrProjectionConflict) {
		t.Fatal(err)
	}
}
