package httpapi

// The WORM fallback bound, observed through the HTTP surface it unblocks:
// `store_timeout → 503` and audit_store_operation_deadline_exceeded_total are
// already in the contract, but only reachable if the archive fallback is
// bounded by the store's own operation deadline. Neither this file nor the
// production handler adds a status, a code or a counter — it proves the
// existing ones now fire.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

// fallbackOperationTimeout is short enough that a regression which waits the
// 30s archiveReadTimeout ceiling (or N × bound) is a visible failure instead of
// a slow pass, and long enough that a healthy local store never trips it.
const fallbackOperationTimeout = 150 * time.Millisecond

// memoryArchive is a working in-memory archive.Store used for the ingest phase:
// the event must be really archived and really hot-evicted before the read
// phase swaps in the black hole. It is not an *archive.FileStore or
// *archive.S3Store, so archive.Configured reports it as configured.
type memoryArchive struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMemoryArchive() *memoryArchive { return &memoryArchive{objects: map[string][]byte{}} }

func (a *memoryArchive) Put(_ context.Context, key string, data []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.objects[key] = append([]byte(nil), data...)
	return nil
}

func (a *memoryArchive) Get(_ context.Context, key string) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	data, ok := a.objects[key]
	if !ok {
		return nil, fmt.Errorf("archived object %q not found", key)
	}
	return append([]byte(nil), data...), nil
}

func (a *memoryArchive) Ready(context.Context) error { return nil }

// blackHoleArchive accepts every write and black-holes every read until its
// context ends, which is what a lost WORM endpoint looks like to the service.
// It counts reads so the bound's early stop is observable.
//
// Get pauses briefly after the context ends (see blackHoleSettle): the archive
// fallback bound is min(archiveReadTimeout, OperationTimeout()), so the fallback
// deadline and the store operation deadline carry the SAME duration here and
// differ only by the microseconds between their derivation. Without the pause,
// whether Store.end observes op.ctx as already expired — i.e. whether the
// documented retryable store_timeout classification is applied — would be a
// coin flip between two timer callbacks due at the same instant. A real remote
// read returns slightly after its context ends; the pause reproduces that
// shape instead of racing it.
type blackHoleArchive struct {
	mu   sync.Mutex
	gets int
}

// blackHoleSettle is the post-deadline pause described on blackHoleArchive. It
// is far below the 150ms operation bound, so every "bounded, not hung"
// assertion in this file keeps its margin.
const blackHoleSettle = 10 * time.Millisecond

func (a *blackHoleArchive) Put(context.Context, string, []byte) error { return nil }

func (a *blackHoleArchive) Get(ctx context.Context, _ string) ([]byte, error) {
	a.mu.Lock()
	a.gets++
	a.mu.Unlock()
	<-ctx.Done()
	time.Sleep(blackHoleSettle)
	return nil, ctx.Err()
}

func (a *blackHoleArchive) Ready(context.Context) error { return nil }

func (a *blackHoleArchive) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.gets
}

// archiveFallbackFixture keeps the real file store and HTTP ingest. Arm the
// short read budget only after preparation is durable and reopened; ingest
// fsync is not the archive Get deadline being measured.
type archiveFallbackFixture struct {
	svc       *service.Service
	server    *httptest.Server
	statePath string
}

func newArchiveFallbackFixture(t *testing.T) *archiveFallbackFixture {
	t.Helper()
	statePath := filepath.Join(t.TempDir(), "state.json")
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	svc, err := service.New(st, service.Config{
		Archive:         newMemoryArchive(),
		SegmentSize:     100,
		SigningSecret:   "test-secret",
		Now:             func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
		AllowDevSecrets: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant(testCtx, "test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource(testCtx, "test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{
		TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true,
		RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"},
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if svc.Store != st {
			_ = svc.Store.Close()
		}
	})
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, log.New(io.Discard, "", 0)).Handler())
	t.Cleanup(server.Close)
	return &archiveFallbackFixture{svc: svc, server: server, statePath: statePath}
}

func (f *archiveFallbackFixture) armReadBudget(t *testing.T) {
	t.Helper()
	f.server.Close() // join every preparation handler before replacing Store
	if err := f.svc.Store.Read(testCtx, func(data *store.Snapshot) error {
		if len(data.Events) != 0 || len(data.Receipts) != 1 {
			return errors.New("fallback preparation did not durably archive and hot-evict the original event")
		}
		for _, receipt := range data.Receipts {
			if receipt.Status != domain.StatusArchived || receipt.Hash == "" || receipt.ArchivedAt.IsZero() {
				return errors.New("fallback preparation has no original archived receipt")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.svc.Store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.OpenWithOptions(f.statePath, store.Options{OperationTimeout: fallbackOperationTimeout})
	if err != nil {
		t.Fatal(err)
	}
	f.svc.Store = reopened
	f.server = httptest.NewServer(NewServer(f.svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, log.New(io.Discard, "", 0)).Handler())
	t.Cleanup(f.server.Close)
}

// ingestArchivedEvent POSTs one event with wait_for=archived. A successful
// archive write deletes the hot payload (transitionReceipt evicts), so the
// receipt ends up StatusArchived with no hot event and every later read must go
// through the WORM fallback.
func (f *archiveFallbackFixture) ingestArchivedEvent(t *testing.T, eventID string) {
	t.Helper()
	body := fmt.Sprintf(`{"event_id":%q,"source_system":"crm","event_type":"audit.event","schema_id":"audit.event","schema_version":1,"occurred_at":"2023-11-14T22:13:30Z","operation_id":"fallback-op","actor":{"id":"user-1"},"action":"update","outcome":"success","data_classification":"internal","retention_class":"standard","idempotency_key":%q,"payload":{"resource":"invoice","value":10}}`, eventID, "idem-"+eventID)
	request, err := http.NewRequest(http.MethodPost, f.server.URL+"/api/v1/events?wait_for=archived", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest status=%d body=%s, want 202", response.StatusCode, data)
	}
	var envelope struct {
		Receipt domain.EventReceipt `json:"receipt"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Receipt.Status != domain.StatusArchived {
		t.Fatalf("fixture receipt=%+v, want a hot-evicted archived receipt", envelope.Receipt)
	}
}

// queryEvents issues GET /api/v1/events over the window that selects the
// ingested event and returns the status and the raw body.
func (f *archiveFallbackFixture) queryEvents(t *testing.T) (int, []byte, time.Duration) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, f.server.URL+"/api/v1/events?from=2023-11-14T22:13:00Z&to=2023-11-14T22:14:00Z", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
	started := time.Now()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, data, time.Since(started)
}

// deadlineCounter scrapes the unlabelled deadline line from GET /metrics.
func (f *archiveFallbackFixture) deadlineCounter(t *testing.T) uint64 {
	t.Helper()
	response, err := http.Get(f.server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	matches := regexp.MustCompile(`(?m)^audit_store_operation_deadline_exceeded_total (\d+)$`).FindStringSubmatch(string(data))
	if matches == nil {
		t.Fatalf("deadline counter missing from /metrics:\n%s", data)
	}
	value, err := strconv.ParseUint(matches[1], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

// TestQueryWithBlackHoledArchiveReturnsStoreTimeout pins AC-3. The route, the
// 503, the store_timeout code and the redacted message are all pre-existing
// contract; what this proves is that a bounded archive fallback makes them
// REACHABLE in bounded time instead of hanging until the HTTP write timeout.
func TestQueryWithBlackHoledArchiveReturnsStoreTimeout(t *testing.T) {
	f := newArchiveFallbackFixture(t)
	f.ingestArchivedEvent(t, "fallback-query-1")
	f.armReadBudget(t)
	hole := &blackHoleArchive{}
	f.svc.Config.Archive = hole

	status, body, elapsed := f.queryEvents(t)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s, want the retryable 503 store_timeout", status, body)
	}
	if status == http.StatusOK {
		t.Fatal("a black-holed archive must fail the query closed, never answer 200")
	}
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("body=%s: %v", body, err)
	}
	if envelope.Error.Code != "store_timeout" {
		t.Fatalf("code=%q, want store_timeout", envelope.Error.Code)
	}
	if envelope.Error.Message != "internal server error" {
		t.Fatalf("message=%q, want the redacted >=500 text", envelope.Error.Message)
	}
	for _, leak := range []string{"archive", "postgres", "state.json", "fallback-query-1", "tenant-a", "deadline exceeded"} {
		if strings.Contains(string(body), leak) {
			t.Fatalf("response leaked backend detail %q: %s", leak, body)
		}
	}
	// Bounded, not hung: the bound is the store's 150ms operation deadline.
	if budget := 5*fallbackOperationTimeout + 2*time.Second; elapsed > budget {
		t.Fatalf("query took %s, want under %s (a hang, not a bounded failure)", elapsed, budget)
	}
	if got := hole.count(); got == 0 {
		t.Fatal("the black-holed archive was never read, so this test did not exercise the fallback")
	}
}

// TestStoreDeadlineMetricMovesAfterArchiveFallbackTimeout pins AC-4: the
// already-declared counter moves by the bounded fallback failure, and the
// rendered /metrics keeps its bounded cardinality (no tenant, event, route or
// DSN value).
func TestStoreDeadlineMetricMovesAfterArchiveFallbackTimeout(t *testing.T) {
	f := newArchiveFallbackFixture(t)
	f.ingestArchivedEvent(t, "fallback-metric-1")
	f.armReadBudget(t)
	before := f.deadlineCounter(t)
	f.svc.Config.Archive = &blackHoleArchive{}

	status, body, _ := f.queryEvents(t)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s, want 503", status, body)
	}
	after := f.deadlineCounter(t)
	if after < before+1 {
		t.Fatalf("audit_store_operation_deadline_exceeded_total went %d -> %d, want at least +1", before, after)
	}

	response, err := http.Get(f.server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	metrics, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(metrics), "audit_store_operation_deadline_exceeded_total "+strconv.FormatUint(after, 10)) {
		t.Fatalf("metrics missing the moved counter:\n%s", metrics)
	}
	for _, leak := range []string{"tenant-a", "fallback-metric-1", "audit_tenant", "postgres://", "dsn=", "/api/v1/"} {
		if strings.Contains(string(metrics), leak) {
			t.Fatalf("metrics leaked %q:\n%s", leak, metrics)
		}
	}
}
