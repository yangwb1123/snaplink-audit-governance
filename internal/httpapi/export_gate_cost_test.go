package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

// Export-gate cost regression at the HTTP boundary.
//
// The legal-hold gate collects its candidate holds before it materializes any
// event, so a tenant with no active hold (or only holds whose window provably
// cannot intersect the export query) is answered from the in-memory control
// plane alone. These tests pin the transport-observable consequences and the
// two fail-closed paths that must NOT be relaxed by that short circuit.

// gateEventPrefix is the archive-key namespace that holds WORM event objects.
const gateEventPrefix = "events/"

// gateHTTPArchive is a per-key archive.Store for these tests. Every key
// outside the events/ namespace is delegated to a real *archive.FileStore, so
// a sealed export object still authenticates through the transport's own read
// and through service.VerifyExportDownload. Keys under events/ are counted
// and, when failEvents is set, answered as an unreadable WORM object.
//
// The counts are the cost oracle: eventsFromSnapshot performs Archive.Get
// BEFORE applying the selection predicate, so a non-zero eventGets proves the
// gate materialized the tenant's event set and a zero proves it did not.
type gateHTTPArchive struct {
	inner      *archive.FileStore
	mu         sync.Mutex
	gets       map[string]int
	failEvents bool
}

func newGateHTTPArchive(dir string, failEvents bool) *gateHTTPArchive {
	return &gateHTTPArchive{inner: &archive.FileStore{Dir: dir}, gets: map[string]int{}, failEvents: failEvents}
}

func (a *gateHTTPArchive) Put(ctx context.Context, key string, data []byte) error {
	return a.inner.Put(ctx, key, data)
}

func (a *gateHTTPArchive) Get(ctx context.Context, key string) ([]byte, error) {
	a.mu.Lock()
	a.gets[namespaceOf(key)]++
	fail := a.failEvents && strings.HasPrefix(key, gateEventPrefix)
	a.mu.Unlock()
	if fail {
		return nil, errors.New("worm event object unavailable")
	}
	return a.inner.Get(ctx, key)
}

func (a *gateHTTPArchive) Ready(ctx context.Context) error { return a.inner.Ready(ctx) }

// count returns the number of Get calls per key namespace since the last reset.
func (a *gateHTTPArchive) count() map[string]int {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := map[string]int{}
	for key, value := range a.gets {
		out[key] = value
	}
	return out
}

func (a *gateHTTPArchive) reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.gets = map[string]int{}
}

func namespaceOf(key string) string {
	if index := strings.Index(key, "/"); index >= 0 {
		return key[:index+1]
	}
	return key
}

// gateCostServer mirrors testHTTPServerWithStore but also returns the
// *service.Service handle, which these tests need in order to swap
// svc.Config.Archive after the fixture data is seeded. testHTTPServerWithStore
// itself is deliberately left untouched.
func gateCostServer(t *testing.T) (*httptest.Server, *service.Service, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	archiveDir := filepath.Join(t.TempDir(), "archive")
	svc, err := service.New(st, service.Config{ArchiveDir: archiveDir, Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, log.New(io.Discard, "", 0)).Handler())
	t.Cleanup(server.Close)
	return server, svc, st
}

const gateComplianceToken = "dev:tenant-a:compliance"

func gateGet(t *testing.T, serverURL, token, path string) (int, []byte) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, serverURL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, body
}

// gateSeedEvictedTenant ingests one event (the configured archive commits it
// to WORM and evicts the hot body in the same transaction) and creates one
// genuinely completed export whose sealed object, job binding and digest were
// produced by the real service.
func gateSeedEvictedTenant(t *testing.T, serverURL string) domain.ExportJob {
	t.Helper()
	event := domain.Event{EventID: "gate-event", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "read", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "gate-idem", Payload: map[string]any{"resource": "invoice", "value": 10}}
	body, _ := json.Marshal(event)
	request, _ := http.NewRequest(http.MethodPost, serverURL+"/api/v1/events", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err := http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest failed: status=%d err=%v", response.StatusCode, err)
	}
	response.Body.Close()
	return postExport(t, serverURL, gateComplianceToken, domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_020, 0).UTC()})
}

func archiveDirOf(t *testing.T, svc *service.Service) string {
	t.Helper()
	inner, ok := svc.Config.Archive.(*archive.FileStore)
	if !ok {
		t.Fatalf("fixture archive is %T, want *archive.FileStore", svc.Config.Archive)
	}
	return inner.Dir
}

func createHTTPHold(t *testing.T, serverURL string, from, to time.Time) domain.LegalHold {
	t.Helper()
	holdBody, _ := json.Marshal(domain.LegalHold{Name: "gate-case", Reason: "gate cost hold", Filter: domain.Query{From: from, To: to}})
	request, _ := http.NewRequest(http.MethodPost, serverURL+"/api/v1/legal-holds", bytes.NewReader(holdBody))
	request.Header.Set("Authorization", "Bearer "+gateComplianceToken)
	response, err := http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusCreated {
		t.Fatalf("hold creation failed: status=%d err=%v", response.StatusCode, err)
	}
	defer response.Body.Close()
	var hold domain.LegalHold
	if err := json.NewDecoder(response.Body).Decode(&hold); err != nil {
		t.Fatal(err)
	}
	if hold.ID == "" {
		t.Fatal("hold creation must return an ID")
	}
	return hold
}

// R-0 baseline / documented behaviour change: with zero active holds the
// status and download routes no longer read the tenant's WORM event objects,
// so an unreadable archived object no longer turns a completed job's status
// read into a 500. Both routes returned 500 before the gate reordering; the
// create and run paths still fail closed on the very same object (pinned by
// TestHTTPExportCreateStillFailsClosedOnUnreadableArchivedObject).
func TestHTTPExportStatusIgnoresUnreadableArchivedObjectWhenNoHold(t *testing.T) {
	server, svc, _ := gateCostServer(t)
	job := gateSeedEvictedTenant(t, server.URL)
	svc.Config.Archive = newGateHTTPArchive(archiveDirOf(t, svc), true)

	status, body := gateGet(t, server.URL, gateComplianceToken, "/api/v1/exports/"+job.ID)
	if status != http.StatusOK {
		t.Fatalf("GET export status=%d body=%s, want 200 (no active hold can block the gate)", status, body)
	}
	var served domain.ExportJob
	if err := json.Unmarshal(body, &served); err != nil {
		t.Fatal(err)
	}
	if served.ID != job.ID || served.Status != "completed" {
		t.Fatalf("served job=%+v, want id=%s status=completed", served, job.ID)
	}

	status, body = gateGet(t, server.URL, gateComplianceToken, "/api/v1/exports/"+job.ID+"/download")
	if status != http.StatusOK {
		t.Fatalf("download status=%d body=%s, want 200 (the gate is not the download choke point)", status, body)
	}
	if !strings.Contains(string(body), `"event_id":"gate-event"`) {
		t.Fatalf("download body must carry the sealed JSONL, got %s", body)
	}
}

// The short circuit is a cost win, not a skip of the authorization work: with
// an intersecting hold active both routes stay 403 and the gate really does
// read the event objects (eventGets > 0) before deciding.
func TestHTTPExportStatusStillBlocksOnIntersectingHold(t *testing.T) {
	server, svc, st := gateCostServer(t)
	job := gateSeedEvictedTenant(t, server.URL)
	fake := newGateHTTPArchive(archiveDirOf(t, svc), false)
	svc.Config.Archive = fake
	hold := createHTTPHold(t, server.URL, time.Unix(1_700_000_000, 0).UTC(), time.Unix(1_700_000_020, 0).UTC())

	for _, path := range []string{"/api/v1/exports/" + job.ID, "/api/v1/exports/" + job.ID + "/download"} {
		fake.reset()
		status, body := gateGet(t, server.URL, gateComplianceToken, path)
		if status != http.StatusForbidden {
			t.Fatalf("%s under hold: status=%d body=%s, want 403", path, status, body)
		}
		var envelope errorEnvelope
		if err := json.Unmarshal(body, &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.Error.Code != "forbidden" || envelope.Error.Message != "export blocked by active legal hold "+hold.ID {
			t.Fatalf("%s error envelope=%+v, want the exact hold denial", path, envelope)
		}
		if fake.count()[gateEventPrefix] == 0 {
			t.Fatalf("%s: hold denial must be produced by a real match, not a skipped gate", path)
		}
	}

	// Audit facts unchanged (B-7): at the HTTP boundary the denial happens in
	// the read-only GetExport, so neither route appends an export.blocked fact
	// and no sealed bytes are ever streamed.
	snap, err := st.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range snap.AdminActions {
		if action.TenantID == "tenant-a" && (action.Action == domain.AdminActionExportBlocked || action.Action == domain.AdminActionEventExport) {
			t.Fatalf("denied export routes must append no fact, got %+v", action)
		}
	}
}

// Fail-closed control: CreateExport runs the audited read (QueryEvents) before
// the gate, so an unreadable WORM event object still refuses the create with a
// non-2xx (never 202) and no job is persisted.
func TestHTTPExportCreateStillFailsClosedOnUnreadableArchivedObject(t *testing.T) {
	server, svc, st := gateCostServer(t)
	gateSeedEvictedTenant(t, server.URL)
	svc.Config.Archive = newGateHTTPArchive(archiveDirOf(t, svc), true)

	before, err := st.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	jobsBefore := len(before.Exports)
	body, _ := json.Marshal(domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_020, 0).UTC()})
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/exports", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+gateComplianceToken)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusAccepted {
		t.Fatalf("create must fail closed on an unreadable archived object, got 202")
	}
	if response.StatusCode < 400 {
		t.Fatalf("create status=%d, want a non-2xx failure", response.StatusCode)
	}
	after, err := st.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Exports) != jobsBefore {
		t.Fatalf("create persisted %d new jobs, want 0 (fail closed before any job exists)", len(after.Exports)-jobsBefore)
	}
}
