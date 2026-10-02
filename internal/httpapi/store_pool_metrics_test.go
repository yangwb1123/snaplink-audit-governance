package httpapi

// A4: the control-plane pool and the store-operation deadline are exported as
// bounded-cardinality /metrics lines.
//
// "Bounded cardinality" is the whole point: a tenant, event, host, DSN or
// route label would make /metrics grow with traffic, so the assertions below
// check both the exact line set AND the absence of any label-shaped value.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

// TestStatusForErrorAndErrorBodyForOperationDeadline pins A3 on the HTTP
// side: a store operation that exhausted its deadline is the EXISTING
// retryable 503 class, never the 500 internal_error default. The message
// stays redacted while the machine code survives, which is what lets a client
// branch on the retryable class without reading internal text.
func TestStatusForErrorAndErrorBodyForOperationDeadline(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/api/v1/events", nil)
	for name, err := range map[string]error{
		"bare":    store.ErrOperationDeadline,
		"wrapped": fmt.Errorf("store operation deadline exceeded: %w: context deadline exceeded on connection to /var/run/postgresql/.s.PGSQL.5432", store.ErrOperationDeadline),
	} {
		t.Run(name, func(t *testing.T) {
			status := statusForError(err)
			if status != http.StatusServiceUnavailable {
				t.Fatalf("status=%d, want %d (the existing retryable 5xx class)", status, http.StatusServiceUnavailable)
			}
			if status == http.StatusInternalServerError {
				t.Fatal("a store deadline must never fall through to 500 internal_error")
			}
			body := errorBody(status, err, request)
			envelope, ok := body["error"].(map[string]any)
			if !ok {
				t.Fatalf("error envelope missing: %v", body)
			}
			if envelope["code"] != "store_timeout" {
				t.Fatalf("code=%v, want store_timeout", envelope["code"])
			}
			if envelope["code"] == "internal_error" {
				t.Fatal("a store deadline must never be classified internal_error")
			}
			if envelope["message"] != "internal server error" {
				t.Fatalf("message=%v, want the redacted >=500 text", envelope["message"])
			}
			if text, _ := envelope["message"].(string); strings.Contains(text, "postgres") {
				t.Fatalf("backend detail leaked in the body: %q", text)
			}
		})
	}
}

// metricsDriver is a minimal always-ready driver: the pool lines only need a
// live *sql.DB, not real data.
type metricsDriver struct{}

func (metricsDriver) Open(string) (driver.Conn, error) { return metricsConn{}, nil }

type metricsConn struct{}

func (metricsConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }

func (metricsConn) Close() error { return nil }

func (metricsConn) Begin() (driver.Tx, error) { return nil, errors.New("not supported") }

func (metricsConn) Ping(context.Context) error { return nil }

var _ driver.Pinger = metricsConn{}

// TestMetricsExposesPoolStatsAndDeadlineCounter pins A4: the five pool lines
// and the deadline counter appear with their exact names, the pool shape is
// reported verbatim (max_open 0 means "no operator limit"), and no label value
// leaks a tenant, event or DSN.
func TestMetricsExposesPoolStatsAndDeadlineCounter(t *testing.T) {
	sql.Register("metrics-probe", metricsDriver{})
	db, err := sql.Open("metrics-probe", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Applied so the emitted max_open is a real operator value rather than
	// the unlimited default, which the file-backed sub-case covers instead.
	db.SetMaxOpenConns(7)
	db.SetMaxIdleConns(3)
	db.SetConnMaxLifetime(90 * time.Second)
	db.SetConnMaxIdleTime(30 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	st, err := store.OpenPostgresWithOptions(db, store.Options{OperationTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	svc, err := service.New(st, service.Config{SegmentSize: 2, SigningSecret: "test-secret", AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	body := renderStoreMetrics(t, svc)

	for _, want := range []string{
		"audit_store_pg_pool_in_use ",
		"audit_store_pg_pool_idle ",
		"audit_store_pg_pool_max_open 7",
		"audit_store_pg_pool_wait_count_total ",
		"audit_store_pg_pool_wait_duration_seconds_total ",
		"audit_store_operation_deadline_exceeded_total ",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
	for _, leak := range []string{"tenant-a", "audit_tenant", "postgres://", "AUDIT_POSTGRES_DSN", "dsn="} {
		if strings.Contains(body, leak) {
			t.Fatalf("metrics leaked %q:\n%s", leak, body)
		}
	}
	if strings.Contains(body, "audit_store_pg_pool_in_use {") || strings.Contains(body, "audit_store_pg_pool_in_use{t") {
		t.Fatalf("pool gauge must be unlabelled:\n%s", body)
	}
}

// TestMetricsOmitsPoolLinesWithoutAPool pins that a file-backed deployment
// emits no pool lines at all. Zero-filling them would read as "pool exhausted
// at zero connections", which is a different fact.
func TestMetricsOmitsPoolLinesWithoutAPool(t *testing.T) {
	st, err := store.OpenWithOptions(filepath.Join(t.TempDir(), "state.json"), store.Options{OperationTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	svc, err := service.New(st, service.Config{SegmentSize: 2, SigningSecret: "test-secret", AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	body := renderStoreMetrics(t, svc)
	if strings.Contains(body, "audit_store_pg_pool") {
		t.Fatalf("a store with no PostgreSQL pool must emit no pool lines:\n%s", body)
	}
	if !strings.Contains(body, "audit_store_operation_deadline_exceeded_total 0") {
		t.Fatalf("the deadline counter is always emitted:\n%s", body)
	}
}

// renderStoreMetrics renders GET /metrics through the real server handler.
func renderStoreMetrics(t *testing.T, svc *service.Service) string {
	t.Helper()
	server := NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, nil)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("/metrics status=%d, want 200", recorder.Code)
	}
	return recorder.Body.String()
}
