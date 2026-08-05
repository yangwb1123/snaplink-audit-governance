package outbox

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

type fakeExecer struct {
	query string
	args  []any
}

func (f *fakeExecer) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	f.query, f.args = query, args
	return fakeResult{}, nil
}

type fakeResult struct{}

func (fakeResult) LastInsertId() (int64, error) { return 1, nil }
func (fakeResult) RowsAffected() (int64, error) { return 1, nil }

func TestInsertUsesCallerTransaction(t *testing.T) {
	fake := &fakeExecer{}
	event := domain.Event{EventID: "outbox-1", TenantID: "tenant-a", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "outbox-idem-1", Payload: map[string]any{"value": 1}}
	if err := Insert(context.Background(), fake, event); err != nil {
		t.Fatal(err)
	}
	if fake.query == "" || len(fake.args) != 5 || fake.args[0] != "outbox-1" || fake.args[1] != "tenant-a" {
		t.Fatalf("unexpected SQL call: %q %#v", fake.query, fake.args)
	}
}
