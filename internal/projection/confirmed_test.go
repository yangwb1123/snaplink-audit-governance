package projection

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

type projectionScript struct {
	columns []string
	rows    [][]driver.Value
	query   func(string, []driver.NamedValue) error
	exec    func(string, []driver.NamedValue) error
}

type projectionDriver struct{ script *projectionScript }
type projectionConn struct{ script *projectionScript }
type projectionRows struct {
	columns []string
	rows    [][]driver.Value
}

func (d projectionDriver) Open(string) (driver.Conn, error) { return &projectionConn{d.script}, nil }
func (c *projectionConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not supported")
}
func (c *projectionConn) Close() error              { return nil }
func (c *projectionConn) Begin() (driver.Tx, error) { return nil, errors.New("not supported") }
func (c *projectionConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.script.query != nil {
		if err := c.script.query(query, args); err != nil {
			return nil, err
		}
	}
	return &projectionRows{c.script.columns, append([][]driver.Value(nil), c.script.rows...)}, nil
}
func (c *projectionConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.script.exec != nil {
		if err := c.script.exec(query, args); err != nil {
			return nil, err
		}
	}
	return driver.RowsAffected(1), nil
}
func (r *projectionRows) Columns() []string { return r.columns }
func (r *projectionRows) Close() error      { return nil }
func (r *projectionRows) Next(dest []driver.Value) error {
	if len(r.rows) == 0 {
		return io.EOF
	}
	copy(dest, r.rows[0])
	r.rows = r.rows[1:]
	return nil
}

var projectionScriptID atomic.Uint64

func scriptedProjection(t *testing.T, script *projectionScript) *Store {
	t.Helper()
	name := fmt.Sprintf("projection-script-%d", projectionScriptID.Add(1))
	sql.Register(name, projectionDriver{script})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return &Store{db: db}
}

func TestProjectionEnvelopePreservesProtectedEvidenceWithoutDigests(t *testing.T) {
	event := ledgeredFixture("envelope")
	event.Payload = map[string]any{"secret": "enc:v1:opaque", "secret__search_digest": "hidden", "exact": json.Number("9007199254740993")}
	event.ChangedFields = map[string]domain.FieldChange{"field": {Before: map[string]any{"x__search_digest": "hidden", "value": 1}, After: "enc:v1:opaque"}}
	encoded, digest, err := projectionEnvelope(event)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "__search_digest") || !strings.Contains(string(encoded), "enc:v1:opaque") || !strings.Contains(string(encoded), "9007199254740993") || len(digest) != 64 {
		t.Fatal("unsafe or lossy projected envelope")
	}
	if _, ok := event.Payload["secret__search_digest"]; !ok {
		t.Fatal("source payload was mutated")
	}
	if _, ok := event.ChangedFields["field"].Before.(map[string]any)["x__search_digest"]; !ok {
		t.Fatal("source changes were mutated")
	}
}

func TestProjectionConfirmationScopeAndConflict(t *testing.T) {
	receipt := domain.EventReceipt{TenantID: "demo", EventID: "event", StreamID: "stream", Sequence: 1, Hash: "hash"}
	for _, tc := range []struct {
		name               string
		variants, matching int64
		confirmed          bool
		err                error
	}{
		{"unknown", 0, 0, false, nil}, {"duplicates", 1, 10, true, nil}, {"coordinate mismatch", 1, 0, false, ErrProjectionConflict}, {"multiple variants", 2, 1, false, ErrProjectionConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := scriptedProjection(t, &projectionScript{columns: []string{"variants", "matching"}, rows: [][]driver.Value{{tc.variants, tc.matching}}, query: func(query string, args []driver.NamedValue) error {
				if !strings.Contains(query, "tenant_id = ? AND event_id = ?") || args[3].Value != "demo" || args[4].Value != "event" {
					t.Fatal("confirmation is not identity scoped")
				}
				return nil
			}})
			confirmed, err := st.Confirmed(context.Background(), receipt)
			if confirmed != tc.confirmed || !errors.Is(err, tc.err) {
				t.Fatalf("confirmed=%t err=%v", confirmed, err)
			}
		})
	}
	var st *Store
	receipt.TenantID = ""
	if _, err := st.Confirmed(context.Background(), receipt); !errors.Is(err, ErrTenantUnscoped) {
		t.Fatal(err)
	}
	if _, err := st.CountTenant(context.Background(), "bad:tenant"); !errors.Is(err, ErrTenantUnscoped) {
		t.Fatal(err)
	}
	if _, err := st.Watermark(context.Background(), "", "stream"); !errors.Is(err, ErrTenantUnscoped) {
		t.Fatal(err)
	}
}

func TestProjectionWatermarkNeverUsesMaximumSequenceAsPrefix(t *testing.T) {
	for _, tc := range []struct {
		name   string
		rows   [][]driver.Value
		prefix uint64
		gap    bool
		err    error
	}{
		{"empty", nil, 0, false, nil},
		{"linked", [][]driver.Value{{int64(1), "h1", "", int64(1)}, {int64(2), "h2", "h1", int64(1)}}, 2, false, nil},
		{"late first", [][]driver.Value{{int64(4), "h4", "h3", int64(1)}}, 0, true, nil},
		{"gap", [][]driver.Value{{int64(1), "h1", "", int64(1)}, {int64(3), "h3", "h2", int64(1)}}, 1, true, nil},
		{"broken link", [][]driver.Value{{int64(1), "h1", "", int64(1)}, {int64(2), "h2", "wrong", int64(1)}}, 0, false, ErrProjectionConflict},
		{"conflict", [][]driver.Value{{int64(1), "h1", "", int64(2)}}, 0, false, ErrProjectionConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := scriptedProjection(t, &projectionScript{columns: []string{"sequence", "hash", "prev", "variants"}, rows: tc.rows, query: func(query string, args []driver.NamedValue) error {
				if !strings.Contains(query, "tenant_id = ? AND stream_id = ?") || args[0].Value != "demo" || args[1].Value != "stream" {
					t.Fatal("watermark is not scoped")
				}
				return nil
			}})
			result, err := st.Watermark(context.Background(), "demo", "stream")
			if result.Sequence != tc.prefix || result.Gap != tc.gap || !errors.Is(err, tc.err) {
				t.Fatalf("watermark=%+v err=%v", result, err)
			}
		})
	}
}

func TestProjectionSchemaMustPreserveConflictingVariants(t *testing.T) {
	for _, key := range []string{"tenant_id, stream_id, sequence, event_id, event_hash, envelope_digest", "tenant_id, event_id"} {
		alter := false
		st := scriptedProjection(t, &projectionScript{columns: []string{"engine", "sorting_key"}, rows: [][]driver.Value{{"ReplacingMergeTree", key}}, exec: func(query string, _ []driver.NamedValue) error {
			alter = strings.Contains(query, "fsync_after_insert = 1")
			return nil
		}})
		err := st.checkConfirmedSchema(context.Background())
		if key == "tenant_id, event_id" {
			if err == nil || alter {
				t.Fatal("incompatible schema silently accepted")
			}
		} else if err != nil || !alter {
			t.Fatalf("durability was not enforced: err=%v", err)
		}
	}
}

func TestProjectionSecondWriteFailureIsNotSuccessful(t *testing.T) {
	calls := 0
	st := scriptedProjection(t, &projectionScript{exec: func(query string, args []driver.NamedValue) error {
		calls++
		if strings.Contains(query, "audit_projection_events") {
			return errors.New("failed confirmed write")
		}
		return nil
	}})
	if err := st.Insert(context.Background(), ledgeredFixture("failed-second")); err == nil || calls != 2 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestProjectionCountsDeduplicateAndRejectConflict(t *testing.T) {
	for _, conflicts := range []int64{0, 1} {
		st := scriptedProjection(t, &projectionScript{columns: []string{"count", "conflicts"}, rows: [][]driver.Value{{int64(2), conflicts}}, query: func(query string, args []driver.NamedValue) error {
			if !strings.Contains(query, "GROUP BY event_id") || args[0].Value != "demo" {
				t.Fatal("count is not logical and tenant scoped")
			}
			return nil
		}})
		count, err := st.CountTenant(context.Background(), "demo")
		if conflicts == 0 && (err != nil || count != 2) {
			t.Fatalf("count=%d err=%v", count, err)
		}
		if conflicts != 0 && !errors.Is(err, ErrProjectionConflict) {
			t.Fatal(err)
		}
	}
}
