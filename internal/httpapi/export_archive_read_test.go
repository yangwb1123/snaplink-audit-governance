package httpapi

// The export download's archive read, observed at the transport boundary.
//
// GET /api/v1/exports/{jobID}/download used to be the only route in this
// package that reached the WORM archive port itself, with the raw request
// context: unbounded, unclassified, and a layering hole under every other
// bounded read in the tree. Service.ReadExportObject moved that read behind
// the store operation. These tests pin the three consequences the route now
// owes: it no longer touches the port, a stalled archive is answered with the
// retryable store_timeout instead of hanging until the HTTP write timeout, and
// the missing-object and successful-download contracts are untouched.

import (
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

// exportReadOperationTimeout is the per-operation bound of the fixture below.
// It is short enough that a regression which waits the 30s archiveReadTimeout
// ceiling (or the HTTP write timeout) is a visible failure rather than a slow
// pass, and long enough that the healthy local store and the export pipeline
// that seeds the fixture never trip it. It is ALSO the read bound, because
// archiveReadBound() is min(archiveReadTimeout, Store.OperationTimeout()) and
// the shipped ceiling is far above it — the relation that makes an exhausted
// read arrive classified as the retryable 503.
const exportReadOperationTimeout = 500 * time.Millisecond

// exportReadSettle is the post-deadline pause of exportReadBlackHole. The read
// bound and the store operation bound carry the SAME duration in every
// shipping configuration, so without a pause after the context ends, whether
// Store.end observes op.ctx as already expired — i.e. whether the retryable
// classification is applied at all — would be a coin flip between two timers
// due together. A real remote read returns slightly after its context ends.
const exportReadSettle = 20 * time.Millisecond

// exportReadPort is the archive.Store surface the fake below delegates to. It
// is declared locally so this fixture does not depend on which concrete
// implementation service.New wired for ArchiveDir.
type exportReadPort interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Ready(ctx context.Context) error
}

// exportReadBlackHole black-holes the sealed-object read until its context
// ends, which is what a lost WORM endpoint looks like to the download route.
// Event objects are answered from the real local archive, so a stalled sealed
// read can never be confused with a stalled export query.
type exportReadBlackHole struct {
	inner  exportReadPort
	settle time.Duration
	mu     sync.Mutex
	gets   int
}

func (a *exportReadBlackHole) Put(ctx context.Context, key string, data []byte) error {
	return a.inner.Put(ctx, key, data)
}

func (a *exportReadBlackHole) Get(ctx context.Context, key string) ([]byte, error) {
	a.mu.Lock()
	a.gets++
	settle := a.settle
	a.mu.Unlock()
	// Event objects are answered from the real local archive, so a stalled
	// sealed read can never be confused with a stalled export query. The
	// sealed-object read is the black hole.
	if strings.HasPrefix(key, gateEventPrefix) {
		return a.inner.Get(ctx, key)
	}
	<-ctx.Done()
	if settle > 0 {
		time.Sleep(settle)
	}
	return nil, fmt.Errorf("get %s: %w", key, ctx.Err())
}

func (a *exportReadBlackHole) Ready(ctx context.Context) error { return a.inner.Ready(ctx) }

func (a *exportReadBlackHole) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.gets
}

// exportReadFixture serves a real download handler over a real store whose
// per-operation bound is exportReadOperationTimeout.
type exportReadFixture struct {
	svc    *service.Service
	st     *store.Store
	server *httptest.Server
}

func newExportReadFixture(t *testing.T) *exportReadFixture {
	t.Helper()
	st, err := store.OpenWithOptions(filepath.Join(t.TempDir(), "state.json"), store.Options{OperationTimeout: exportReadOperationTimeout})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	svc, err := service.New(st, service.Config{
		ArchiveDir:      filepath.Join(t.TempDir(), "archive"),
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
	api := NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, log.New(io.Discard, "", 0))
	server := httptest.NewServer(api.Handler())
	t.Cleanup(server.Close)
	return &exportReadFixture{svc: svc, st: st, server: server}
}

// seed plants a genuinely completed export: the sealed object, its job binding
// and its digest are all produced by the real service, so the download's
// verification path is exercised rather than stubbed.
func (f *exportReadFixture) seed(t *testing.T) domain.ExportJob {
	t.Helper()
	body := `{"event_id":"export-read-event","source_system":"crm","event_type":"audit.event","schema_id":"audit.event","schema_version":1,"occurred_at":"2023-11-14T22:13:30Z","operation_id":"export-read-op","actor":{"id":"user-1"},"action":"update","outcome":"success","data_classification":"internal","retention_class":"standard","idempotency_key":"export-read-idem","payload":{"resource":"invoice","value":10}}`
	request, err := http.NewRequest(http.MethodPost, f.server.URL+"/api/v1/events?wait_for=archived", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest status=%d body=%s, want 202", response.StatusCode, data)
	}
	return postExport(t, f.server.URL, gateComplianceToken, domain.Query{
		From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_020, 0).UTC(),
	})
}

func (f *exportReadFixture) download(t *testing.T, jobID string) (*http.Response, []byte, time.Duration) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, f.server.URL+"/api/v1/exports/"+jobID+"/download", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+gateComplianceToken)
	started := time.Now()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	return response, data, time.Since(started)
}

// counter scrapes one labelled or unlabelled series from GET /metrics.
func (f *exportReadFixture) counter(t *testing.T, name string) float64 {
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
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, name+" ") && !strings.HasPrefix(line, name+"{") {
			continue
		}
		fields := strings.Fields(line)
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			t.Fatalf("cannot parse metric line %q: %v", line, err)
		}
		return value
	}
	t.Fatalf("metric %q missing from /metrics:\n%s", name, data)
	return 0
}

// exportFacts counts the export governance facts currently committed, across
// both stores: the audit.event.export fact a served download appends to the
// read self-audit trail, and the export.blocked fact a held one appends to the
// snapshot's admin actions.
func exportFacts(t *testing.T, f *exportReadFixture) map[string]int {
	t.Helper()
	snapshot, err := f.st.Snapshot(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	trail, err := f.st.ReadAdminTrail(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, action := range append(append([]domain.AdminAction{}, snapshot.AdminActions...), trail...) {
		switch action.Action {
		case domain.AdminActionEventExport, domain.AdminActionExportBlocked:
			counts[action.Action]++
		}
	}
	return counts
}

// TestDownloadExportArchiveReadIsBoundedAndClassifiedStoreTimeout is the
// headline: a black-holed WORM endpoint no longer pins the download handler
// until the HTTP write timeout. The read ends at the store's operation
// deadline and is classified by Store.end, which is exactly the retryable 503
// store_timeout this route already declares — no new status, no new code, no
// new counter.
func TestDownloadExportArchiveReadIsBoundedAndClassifiedStoreTimeout(t *testing.T) {
	f := newExportReadFixture(t)
	job := f.seed(t)
	hole := &exportReadBlackHole{inner: f.svc.Config.Archive, settle: exportReadSettle}
	f.svc.Config.Archive = hole

	errorsBefore := f.counter(t, "audit_http_errors_total")
	storeBefore := f.counter(t, "audit_store_operation_deadline_exceeded_total")
	cancelledBefore := f.counter(t, `audit_http_request_cancelled_total{reason="cancelled"}`)
	deadlineBefore := f.counter(t, `audit_http_request_cancelled_total{reason="deadline_exceeded"}`)

	response, body, elapsed := f.download(t, job.ID)

	if elapsed > 5*exportReadOperationTimeout+2*time.Second {
		t.Fatalf("download took %s: a hang, not a bounded failure", elapsed)
	}
	if hole.count() != 1 {
		t.Fatalf("sealed object reads=%d, want exactly 1 (this test did not exercise the bounded read)", hole.count())
	}
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s, want the retryable 503 store_timeout", response.StatusCode, body)
	}
	var envelope errorEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("body=%s: %v", body, err)
	}
	if envelope.Error.Code != "store_timeout" || envelope.Error.Message != "internal server error" {
		t.Fatalf("envelope=%+v, want store_timeout / internal server error", envelope)
	}
	for _, leak := range []string{job.ID, "exports/", "tenant-a", "export-read-event", "deadline exceeded", "archive"} {
		if strings.Contains(string(body), leak) {
			t.Fatalf("503 body leaked %q: %s", leak, body)
		}
	}
	if contentType := response.Header.Get("Content-Type"); strings.Contains(contentType, "x-ndjson") {
		t.Fatalf("a failed download must not stream ndjson, content-type=%q", contentType)
	}
	if disposition := response.Header.Get("Content-Disposition"); disposition != "" {
		t.Fatalf("a failed download must not offer an attachment, Content-Disposition=%q", disposition)
	}

	if got := f.counter(t, "audit_store_operation_deadline_exceeded_total"); got != storeBefore+1 {
		t.Fatalf("audit_store_operation_deadline_exceeded_total %v -> %v, want +1", storeBefore, got)
	}
	if got := f.counter(t, "audit_http_errors_total"); got != errorsBefore+1 {
		t.Fatalf("audit_http_errors_total %v -> %v, want +1 (a retryable 503 IS an availability error)", errorsBefore, got)
	}
	if got := f.counter(t, `audit_http_request_cancelled_total{reason="cancelled"}`); got != cancelledBefore {
		t.Fatalf("withdrawal cancelled counter moved %v -> %v", cancelledBefore, got)
	}
	if got := f.counter(t, `audit_http_request_cancelled_total{reason="deadline_exceeded"}`); got != deadlineBefore {
		t.Fatalf("withdrawal deadline counter moved %v -> %v", deadlineBefore, got)
	}
	// Fail-closed: a download that served nothing records nothing.
	if facts := exportFacts(t, f); len(facts) != 0 {
		t.Fatalf("a bounded-failure download appended governance facts: %v", facts)
	}
}

// TestDownloadExportStillServesItsOwnSealedObjectOnce is the regression side of
// the move: the successful path is what it was, it reads exactly one sealed
// object and no event object, and it records exactly one audit.event.export
// fact.
func TestDownloadExportStillServesItsOwnSealedObjectOnce(t *testing.T) {
	f := newExportReadFixture(t)
	job := f.seed(t)
	fake := newGateHTTPArchive(archiveDirOf(t, f.svc), false)
	f.svc.Config.Archive = fake

	response, body, _ := f.download(t, job.ID)

	if response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s, want 200", response.StatusCode, body)
	}
	if contentType := response.Header.Get("Content-Type"); !strings.Contains(contentType, "application/x-ndjson") {
		t.Fatalf("content-type=%q, want application/x-ndjson", contentType)
	}
	if got := response.Header.Get("Content-Length"); got != strconv.Itoa(len(body)) {
		t.Fatalf("Content-Length=%q body=%d bytes, want them to agree", got, len(body))
	}
	if !strings.Contains(string(body), `"event_id":"export-read-event"`) {
		t.Fatalf("download body must carry the sealed JSONL, got %s", body)
	}
	counts := fake.count()
	if counts["exports/"] != 1 {
		t.Fatalf("sealed object reads=%d, want exactly 1 (counts=%v)", counts["exports/"], counts)
	}
	if counts[gateEventPrefix] != 0 {
		t.Fatalf("event object reads=%d, want 0: the download route is not a query (counts=%v)", counts[gateEventPrefix], counts)
	}
	if facts := exportFacts(t, f); facts[domain.AdminActionEventExport] != 1 {
		t.Fatalf("export facts=%v, want exactly one audit.event.export", facts)
	}
}

// TestDownloadExportMissingObjectStaysNotFound pins the 404 class across the
// move. The transport no longer matches the archive adapter's error taxonomy
// (os.IsNotExist cannot unwrap the service's %w wrapper), so this is the test
// that would fail if the missing-object class were lost: a missing sealed
// object must answer 404 not_found — never the opaque 500 a non-sentinel
// archive error produces — and it must never serve bytes or record a fact.
func TestDownloadExportMissingObjectStaysNotFound(t *testing.T) {
	f := newExportReadFixture(t)
	job := f.seed(t)
	// An archive with no object under the job's key, but otherwise healthy.
	f.svc.Config.Archive = newGateHTTPArchive(filepath.Join(t.TempDir(), "empty-archive"), false)

	response, body, _ := f.download(t, job.ID)

	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d body=%s, want 404", response.StatusCode, body)
	}
	var envelope errorEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("body=%s: %v", body, err)
	}
	if envelope.Error.Code != "not_found" {
		t.Fatalf("envelope=%+v, want the not_found code", envelope)
	}
	if strings.Contains(response.Header.Get("Content-Type"), "x-ndjson") {
		t.Fatalf("a 404 must not stream ndjson, content-type=%q", response.Header.Get("Content-Type"))
	}
	if facts := exportFacts(t, f); len(facts) != 0 {
		t.Fatalf("a 404 download appended governance facts: %v", facts)
	}
}

// TestGovernanceHandlersHoldNoArchivePort is the layering pin. The governance
// handlers used to be the only place in internal/httpapi that reached the WORM
// port through Service.Config; they must now reach it only through a service
// method, while the route's other stages stay exactly where they were.
func TestGovernanceHandlersHoldNoArchivePort(t *testing.T) {
	source, err := os.ReadFile("handlers_governance.go")
	if err != nil {
		t.Fatalf("read handlers_governance.go (go test runs with CWD=package dir): %v", err)
	}
	text := string(source)
	for _, needle := range []string{"Config.Archive", ".Archive.Get(", ".Archive.Put(", "os.IsNotExist"} {
		if strings.Contains(text, needle) {
			t.Fatalf("handlers_governance.go still reaches the archive port directly (%q); the transport must go through a service method", needle)
		}
	}
	// Positive control: the read moved, it did not disappear.
	if !strings.Contains(text, "s.Service.ReadExportObject(") {
		t.Fatal("downloadExport must read its sealed object through Service.ReadExportObject")
	}
	for _, stage := range []string{"s.Service.VerifyExportDownload(", "s.Service.RecordExportDownload("} {
		if !strings.Contains(text, stage) {
			t.Fatalf("download stages changed: %q is gone from the handler", stage)
		}
	}
}

// TestArchivePortIsReachedOnlyByTheReadinessProbe pins the package-wide
// invariant the change establishes: in internal/httpapi the archive port is
// reachable from the readiness probe and nowhere else. Readiness is the
// deliberate exception — a dependency probe must touch infrastructure without
// a domain round-trip — so the assertion is "only readyz", not "none".
func TestArchivePortIsReachedOnlyByTheReadinessProbe(t *testing.T) {
	fset, functions, _ := packageFuncs(t)
	probe := functions["readyz"]
	if probe == nil || probe.Body == nil {
		t.Fatal("func (s *Server) readyz not found in package httpapi: the readiness probe is the one permitted archive-port caller")
	}
	offenders := map[string][]string{}
	for name, decl := range functions {
		if decl.Body == nil || name == "readyz" {
			continue
		}
		ast.Inspect(decl.Body, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Archive" {
				return true
			}
			offenders[name] = append(offenders[name], fset.Position(sel.Pos()).String())
			return true
		})
	}
	if len(offenders) != 0 {
		t.Fatalf("httpapi reaches Service.Config.Archive outside readyz: %v", offenders)
	}
	// Positive control: the probe really does use the port, so the scan above
	// cannot pass by the port disappearing from the package.
	probes := 0
	ast.Inspect(probe.Body, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "Archive" {
			probes++
		}
		return true
	})
	if probes == 0 {
		t.Fatal("readyz no longer probes the archive port: the scan above would pass vacuously")
	}
}
