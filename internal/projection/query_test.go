package projection

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestProjectionQueryFiltersAreParametersAndSensitiveSearchStaysOut(t *testing.T) {
	q := domain.Query{From: time.Unix(1700000000, 0).UTC(), To: time.Unix(1700000100, 0).UTC(), ActorID: "actor' OR 1=1", TargetID: "object", SourceSystem: "source", CorrelationID: "flow", StreamID: "stream"}
	statement, args, err := projectionQuery("tenant-a", q)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(statement, q.ActorID) || !strings.Contains(statement, "tenant_id = ?") || !strings.Contains(statement, "variants != 1 OR") || !strings.Contains(statement, "LIMIT 10001") || len(args) != 8 {
		t.Fatalf("query=%s args=%v", statement, args)
	}
	q.PayloadField, q.PayloadDigest = "email", "never-send-this-digest"
	if _, _, err := projectionQuery("tenant-a", q); !errors.Is(err, domain.ErrInvalid) || strings.Contains(err.Error(), q.PayloadDigest) {
		t.Fatal(err)
	}
	if _, _, err := projectionQuery("", q); !errors.Is(err, ErrTenantUnscoped) {
		t.Fatal(err)
	}
}

func TestClickHouseQueryIntegration(t *testing.T) {
	dsn := os.Getenv("AUDIT_TEST_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("requires disposable ClickHouse")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	st, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1700000000, 500).UTC()
	one := ledgeredFixture("query-one")
	one.TenantID, one.StreamID, one.Hash, one.Sequence, one.OccurredAt = "query-it", "stream", "h1", 1, at
	one.Actor.ID, one.CorrelationID, one.Targets = "reader", "flow", []domain.Target{{ID: "object", Type: "doc"}}
	two := one
	two.EventID, two.Sequence, two.Hash, two.PrevHash, two.OccurredAt = "query-two", 2, "h2", "h1", at.Add(time.Second)
	foreign := one
	foreign.TenantID = "other-query-it"
	for _, event := range []domain.Event{one, one, two, foreign} {
		if err := st.Insert(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	q := domain.Query{From: at.Add(-time.Nanosecond), To: two.OccurredAt.Add(time.Nanosecond), ActorID: "reader", TargetID: "object", CorrelationID: "flow"}
	references, err := st.SelectQueryReferences(ctx, one.TenantID, q)
	if err != nil || len(references) != 2 || references[0].EventID != one.EventID || !references[0].OccurredAt.Equal(at) {
		t.Fatalf("refs=%+v err=%v", references, err)
	}
	q.From = at.Add(time.Nanosecond)
	if references, err := st.SelectQueryReferences(ctx, one.TenantID, q); err != nil || len(references) != 1 || references[0].EventID != two.EventID {
		t.Fatalf("refs=%+v err=%v", references, err)
	}
	q.ActorID = "unmatched"
	if references, err := st.SelectQueryReferences(ctx, one.TenantID, q); err != nil || len(references) != 0 {
		t.Fatalf("refs=%+v err=%v", references, err)
	}
	// Whole supported ledger horizon: do not use DateTime64(9), whose 2262
	// ceiling would overflow events valid under the 2299 projection contract.
	horizon := one
	horizon.EventID, horizon.Hash, horizon.Sequence, horizon.OccurredAt = "query-horizon", "h9", 9, domain.MaxOccurredAt
	if err := st.Insert(ctx, horizon); err != nil {
		t.Fatal(err)
	}
	if refs, err := st.SelectQueryReferences(ctx, one.TenantID, domain.Query{From: domain.MaxOccurredAt.Add(-time.Second), To: domain.MaxOccurredAt.Add(time.Millisecond)}); err != nil || len(refs) != 1 {
		t.Fatalf("refs=%+v err=%v", refs, err)
	}
	conflict := one
	conflict.Hash = "other-hash"
	if err := st.Insert(ctx, conflict); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, "OPTIMIZE TABLE audit_projection_events FINAL"); err != nil {
		t.Fatal(err)
	}
	// Conflicts must not disappear under a nonmatching predicate or a merge.
	if _, err := st.SelectQueryReferences(ctx, one.TenantID, q); !errors.Is(err, ErrProjectionConflict) {
		t.Fatal(err)
	}
}
