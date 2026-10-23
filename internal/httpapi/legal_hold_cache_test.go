package httpapi

// Legal Hold HTTP cache-hardening regression. The three existing Hold
// operations must set Cache-Control: no-store at handler entry, before
// authentication, tenant selection, body/filter parsing or service access, so
// success, authentication/authorization refusal, invalid input and reachable
// service failures all share one non-storable response policy. This is
// transport hardening only: Hold scope, first-release decision, self-audit,
// control-CAS, WORM isolation and the original error semantics are unchanged.

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
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

const (
	holdNoStoreSecretName   = "private-hold-name-never-echoed"
	holdNoStoreSecretReason = "private-hold-reason-never-echoed"
	holdNoStoreFaultText    = "private hold control write failure"
)

var (
	holdNoStoreNow      = time.Unix(1_700_000_000, 0).UTC()
	errHoldNoStoreFault = errors.New(holdNoStoreFaultText)
)

// holdNoStoreArchive counts every WORM port call. A Legal Hold journey is
// control-only: create, list and release must never read or write an object.
type holdNoStoreArchive struct {
	inner *archive.FileStore
	mu    sync.Mutex
	gets  int
	puts  int
	ready int
}

func (a *holdNoStoreArchive) Put(ctx context.Context, key string, data []byte) error {
	a.mu.Lock()
	a.puts++
	a.mu.Unlock()
	return a.inner.Put(ctx, key, data)
}

func (a *holdNoStoreArchive) Get(ctx context.Context, key string) ([]byte, error) {
	a.mu.Lock()
	a.gets++
	a.mu.Unlock()
	return a.inner.Get(ctx, key)
}

func (a *holdNoStoreArchive) Ready(ctx context.Context) error {
	a.mu.Lock()
	a.ready++
	a.mu.Unlock()
	return a.inner.Ready(ctx)
}

func (a *holdNoStoreArchive) counts() (gets, puts, ready int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.gets, a.puts, a.ready
}

type holdNoStoreFixture struct {
	server      *httptest.Server
	handler     http.Handler
	service     *service.Service
	store       *store.Store
	path        string
	archiveDir  string
	archive     *holdNoStoreArchive
	faultArmed  *atomic.Bool
	serverClose bool
	storeClose  bool
}

type holdNoStoreResponse struct {
	status int
	header http.Header
	body   []byte
}

func newHoldNoStoreFixture(t *testing.T) *holdNoStoreFixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.json")
	armed := &atomic.Bool{}
	st, err := store.OpenWithControlWriteFault(path, func() error {
		if armed.Load() {
			return errHoldNoStoreFault
		}
		return nil
	})
	if err != nil {
		t.Fatal("could not open owned Legal Hold file store")
	}
	fixture := &holdNoStoreFixture{path: path, store: st, faultArmed: armed}
	// Register the Store cleanup immediately after a successful open. The HTTP
	// cleanup below is registered later, so LIFO stops HTTP before closing the
	// Store. A close error is reported, never swallowed, on normal and Fatal
	// paths.
	t.Cleanup(func() { fixture.closeStore(t) })

	archiveDir := filepath.Join(filepath.Dir(path), "archive")
	files := &holdNoStoreArchive{inner: &archive.FileStore{Dir: archiveDir}}
	svc, err := service.New(st, service.Config{
		Archive:         files,
		ArchiveDir:      archiveDir,
		Now:             func() time.Time { return holdNoStoreNow },
		AllowDevSecrets: true,
	})
	if err != nil {
		t.Fatal("could not build Legal Hold HTTP service")
	}
	for _, tenant := range []domain.Tenant{
		{ID: "tenant-a", Name: "Tenant A", Active: true},
		{ID: "tenant-b", Name: "Tenant B", Active: true},
	} {
		if err := svc.CreateTenant(testCtx, "fixture", tenant); err != nil {
			t.Fatal("could not create Legal Hold fixture tenant")
		}
	}
	api := NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, log.New(io.Discard, "", 0))
	fixture.service = svc
	fixture.archiveDir = archiveDir
	fixture.archive = files
	fixture.handler = api.Handler()
	fixture.server = httptest.NewServer(fixture.handler)
	t.Cleanup(fixture.stopHTTP)
	return fixture
}

func (f *holdNoStoreFixture) stopHTTP() {
	if f.server != nil && !f.serverClose {
		f.server.Close()
		f.serverClose = true
	}
}

func (f *holdNoStoreFixture) closeStore(t *testing.T) {
	t.Helper()
	if f.storeClose {
		return
	}
	f.storeClose = true
	if err := f.store.Close(); err != nil {
		t.Errorf("closing owned Legal Hold HTTP store: %v", err)
	}
}

func (f *holdNoStoreFixture) do(t *testing.T, method, path, token string, body []byte) holdNoStoreResponse {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequest(method, f.server.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := f.server.Client().Do(request)
	if err != nil {
		t.Fatalf("Legal Hold HTTP request failed: %v", err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return holdNoStoreResponse{status: response.StatusCode, header: response.Header.Clone(), body: raw}
}

// doHandler drives the real handler in-process for requests whose context must
// already be withdrawn before ServeHTTP. A live socket cannot deterministically
// observe the response of a request cancelled at entry.
func (f *holdNoStoreFixture) doHandler(t *testing.T, ctx context.Context, method, path, token string, body []byte) holdNoStoreResponse {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request := httptest.NewRequest(method, path, reader).WithContext(ctx)
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	f.handler.ServeHTTP(recorder, request)
	return holdNoStoreResponse{status: recorder.Code, header: recorder.Header().Clone(), body: recorder.Body.Bytes()}
}

func assertHoldNoStore(t *testing.T, label string, response holdNoStoreResponse) {
	t.Helper()
	if got := response.header.Get("Cache-Control"); got != "no-store" {
		t.Errorf("%s: Cache-Control=%q, want no-store (status=%d)", label, got, response.status)
	}
}

func assertHoldError(t *testing.T, label string, response holdNoStoreResponse, wantStatus int, wantCode string, forbidden ...string) {
	t.Helper()
	assertHoldNoStore(t, label, response)
	if response.status != wantStatus {
		t.Fatalf("%s: status=%d, want %d (body=%s)", label, response.status, wantStatus, response.body)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(response.body, &top); err != nil {
		t.Fatalf("%s: error body is not JSON: %v (body=%s)", label, err, response.body)
	}
	if len(top) != 1 || top["error"] == nil {
		t.Fatalf("%s: refusal served something beyond the fixed error envelope: %s", label, response.body)
	}
	var envelope errorEnvelope
	if err := json.Unmarshal(response.body, &envelope); err != nil {
		t.Fatalf("%s: error envelope decode: %v (body=%s)", label, err, response.body)
	}
	if envelope.Error.Code != wantCode {
		t.Fatalf("%s: error code=%q, want %q (body=%s)", label, envelope.Error.Code, wantCode, response.body)
	}
	for _, token := range forbidden {
		if token != "" && strings.Contains(string(response.body), token) {
			t.Fatalf("%s: refusal leaked %q: %s", label, token, response.body)
		}
	}
}

func decodeHoldList(t *testing.T, label string, response holdNoStoreResponse) []domain.LegalHold {
	t.Helper()
	assertHoldNoStore(t, label, response)
	if response.status != http.StatusOK {
		t.Fatalf("%s: status=%d, want 200 (body=%s)", label, response.status, response.body)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(response.body, &envelope); err != nil {
		t.Fatalf("%s: list envelope decode: %v (body=%s)", label, err, response.body)
	}
	itemsRaw, itemsOK := envelope["items"]
	countRaw, countOK := envelope["count"]
	if !itemsOK || !countOK {
		t.Fatalf("%s: list lost items/count fields: %s", label, response.body)
	}
	itemsRaw = bytes.TrimSpace(itemsRaw)
	if len(itemsRaw) == 0 || itemsRaw[0] != '[' {
		t.Fatalf("%s: items must be a JSON array, got %s", label, itemsRaw)
	}
	var items []domain.LegalHold
	if err := json.Unmarshal(itemsRaw, &items); err != nil {
		t.Fatalf("%s: items decode: %v (%s)", label, err, itemsRaw)
	}
	var count int
	if err := json.Unmarshal(countRaw, &count); err != nil || count != len(items) {
		t.Fatalf("%s: count=%s does not match %d items", label, countRaw, len(items))
	}
	return items
}

func holdNoStoreBody(t *testing.T, hold domain.LegalHold) []byte {
	t.Helper()
	body, err := json.Marshal(hold)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func holdNoStoreSnapshot(t *testing.T, st *store.Store) *store.Snapshot {
	t.Helper()
	snapshot, err := st.Snapshot(testCtx)
	if err != nil {
		t.Fatalf("could not snapshot Legal Hold fixture store: %v", err)
	}
	return snapshot
}

func assertHoldFactCounts(t *testing.T, st *store.Store, holdID string, wantCreated, wantReleased int) {
	t.Helper()
	snapshot := holdNoStoreSnapshot(t, st)
	created, released := 0, 0
	for _, action := range snapshot.AdminActions {
		if action.TargetID != holdID {
			continue
		}
		if action.TenantID != "tenant-a" || action.TargetType != "legal_hold" || action.ID == "" {
			t.Fatalf("Hold fact lost its original identity/scope encoding: %+v", action)
		}
		switch action.Action {
		case domain.AdminActionLegalHoldCreated:
			created++
		case domain.AdminActionLegalHoldReleased:
			released++
		}
	}
	if created != wantCreated || released != wantReleased {
		t.Fatalf("Hold fact counts created=%d released=%d, want %d/%d", created, released, wantCreated, wantReleased)
	}
}

func assertHoldNoWORM(t *testing.T, files *holdNoStoreArchive, label string) {
	t.Helper()
	gets, puts, ready := files.counts()
	if gets != 0 || puts != 0 || ready != 0 {
		t.Fatalf("%s touched WORM: gets=%d puts=%d ready=%d", label, gets, puts, ready)
	}
}

func assertHoldNoFactsChanged(t *testing.T, before, after *store.Snapshot, holdID string) {
	t.Helper()
	if !reflect.DeepEqual(before.AdminActions, after.AdminActions) {
		t.Fatalf("Legal Hold list changed the self-audit trail: before=%+v after=%+v", before.AdminActions, after.AdminActions)
	}
	if !reflect.DeepEqual(before.LegalHolds[holdID], after.LegalHolds[holdID]) {
		t.Fatalf("Legal Hold list changed control state: before=%+v after=%+v", before.LegalHolds[holdID], after.LegalHolds[holdID])
	}
}

func TestHTTPHoldNoStoreJourneyReopen(t *testing.T) {
	fixture := newHoldNoStoreFixture(t)

	caseWorker := mintRestoreJWT(t, "hold-case-worker", "tenant-a", []string{"compliance"})
	releaseWorker := mintRestoreJWT(t, "hold-release-worker", "tenant-a", []string{"compliance"})
	secondReleaser := mintRestoreJWT(t, "hold-second-release-worker", "tenant-a", []string{"compliance"})
	reader := mintRestoreJWT(t, "hold-reader", "tenant-a", []string{"auditor"})
	otherTenant := mintRestoreJWT(t, "hold-other-tenant", "tenant-b", []string{"compliance"})
	platform := mintRestoreJWT(t, "hold-platform", "", []string{"platform-admin"})

	valid := holdNoStoreBody(t, domain.LegalHold{
		Name:   holdNoStoreSecretName,
		Reason: holdNoStoreSecretReason,
		Filter: domain.Query{From: holdNoStoreNow.Add(-time.Hour), To: holdNoStoreNow.Add(time.Hour)},
	})
	releasePath := "/api/v1/legal-holds/hold-absent/release"

	// The owned journey begins with an empty tenant list: the HTTP contract
	// still renders items:[] (never null) and performs no write or WORM read.
	if empty := decodeHoldList(t, "initial empty list", fixture.do(t, http.MethodGet, "/api/v1/legal-holds", caseWorker, nil)); len(empty) != 0 {
		t.Fatalf("initial tenant-a Hold list=%+v, want empty", empty)
	}

	// 401: authentication fails before Hold scope or service access, but the
	// response must already carry the non-storable policy.
	for _, testCase := range []struct {
		label, method, path string
		body                []byte
	}{
		{"create unauthenticated", http.MethodPost, "/api/v1/legal-holds", valid},
		{"list unauthenticated", http.MethodGet, "/api/v1/legal-holds", nil},
		{"release unauthenticated", http.MethodPost, "/api/v1/legal-holds/hold-absent/release", nil},
	} {
		assertHoldError(t, testCase.label, fixture.do(t, testCase.method, testCase.path, "", testCase.body), http.StatusUnauthorized, "unauthorized", holdNoStoreSecretName, holdNoStoreSecretReason)
	}

	// A presented but invalid JWT is the same fail-closed 401 class and must
	// not be cacheable either.
	for _, testCase := range []struct {
		label, method, path string
		body                []byte
	}{
		{"create invalid JWT", http.MethodPost, "/api/v1/legal-holds", valid},
		{"list invalid JWT", http.MethodGet, "/api/v1/legal-holds", nil},
		{"release invalid JWT", http.MethodPost, releasePath, nil},
	} {
		assertHoldError(t, testCase.label, fixture.do(t, testCase.method, testCase.path, "not-a-valid-jwt", testCase.body), http.StatusUnauthorized, "unauthorized", holdNoStoreSecretName, holdNoStoreSecretReason)
	}

	// 403: a real signed tenant without audit:legal_hold:manage is refused on
	// every operation; no other tenant's fields may appear.
	for _, testCase := range []struct {
		label, method, path string
		body                []byte
	}{
		{"create missing permission", http.MethodPost, "/api/v1/legal-holds", valid},
		{"list missing permission", http.MethodGet, "/api/v1/legal-holds", nil},
		{"release missing permission", http.MethodPost, releasePath, nil},
	} {
		assertHoldError(t, testCase.label, fixture.do(t, testCase.method, testCase.path, reader, testCase.body), http.StatusForbidden, "forbidden", holdNoStoreSecretName, holdNoStoreSecretReason)
	}

	// A platform credential never contributes a fallback tenant: every scoped
	// Hold operation must reject an omitted selector before service access.
	for _, testCase := range []struct {
		label, method, path string
		body                []byte
	}{
		{"create platform without tenant", http.MethodPost, "/api/v1/legal-holds", holdNoStoreBody(t, domain.LegalHold{TenantID: "tenant-a", Name: holdNoStoreSecretName, Reason: holdNoStoreSecretReason})},
		{"list platform without tenant", http.MethodGet, "/api/v1/legal-holds", nil},
		{"release platform without tenant", http.MethodPost, releasePath, nil},
	} {
		assertHoldError(t, testCase.label, fixture.do(t, testCase.method, testCase.path, platform, testCase.body), http.StatusBadRequest, "invalid_request", holdNoStoreSecretReason)
	}

	// Invalid input and a missing target stay fixed 400/404 responses, never a
	// success object or a diagnostic echo.
	missingFields := holdNoStoreBody(t, domain.LegalHold{Name: "", Reason: ""})
	assertHoldError(t, "create missing fields", fixture.do(t, http.MethodPost, "/api/v1/legal-holds", caseWorker, missingFields), http.StatusBadRequest, "invalid_request", holdNoStoreSecretReason)
	malformed := []byte(`{"name":"` + holdNoStoreSecretName + `","reason":"` + holdNoStoreSecretReason + `"}{}`)
	assertHoldError(t, "create malformed body", fixture.do(t, http.MethodPost, "/api/v1/legal-holds", caseWorker, malformed), http.StatusBadRequest, "invalid_request", holdNoStoreSecretReason)
	assertHoldError(t, "list repeated tenant selector", fixture.do(t, http.MethodGet, "/api/v1/legal-holds?tenant_id=tenant-a&tenant_id=tenant-b", caseWorker, nil), http.StatusBadRequest, "invalid_request", holdNoStoreSecretName, holdNoStoreSecretReason)
	assertHoldError(t, "release absent hold", fixture.do(t, http.MethodPost, releasePath, caseWorker, nil), http.StatusNotFound, "not_found", holdNoStoreSecretName, holdNoStoreSecretReason)

	// Platform query selector and body tenant must agree; a mismatch is a
	// request error before any Hold is created.
	platformMismatch := holdNoStoreBody(t, domain.LegalHold{TenantID: "tenant-b", Name: holdNoStoreSecretName, Reason: holdNoStoreSecretReason})
	assertHoldError(t, "create platform body tenant mismatch", fixture.do(t, http.MethodPost, "/api/v1/legal-holds?tenant_id=tenant-a", platform, platformMismatch), http.StatusBadRequest, "invalid_request", holdNoStoreSecretName, holdNoStoreSecretReason)

	// Signed tenant priority: neither a query selector nor a body tenant can
	// widen a tenant token. The signed tenant remains tenant-a.
	foreignBody := holdNoStoreBody(t, domain.LegalHold{TenantID: "tenant-b", Name: "signed-priority-hold", Reason: "signed-priority-reason"})
	created := fixture.do(t, http.MethodPost, "/api/v1/legal-holds?tenant_id=tenant-b", caseWorker, foreignBody)
	assertHoldNoStore(t, "create signed tenant priority", created)
	if created.status != http.StatusCreated {
		t.Fatalf("create signed tenant priority status=%d body=%s", created.status, created.body)
	}
	var hold domain.LegalHold
	if err := json.Unmarshal(created.body, &hold); err != nil {
		t.Fatalf("created Hold decode: %v (body=%s)", err, created.body)
	}
	if hold.ID == "" || hold.TenantID != "tenant-a" || hold.CreatedBy != "hold-case-worker" ||
		hold.Name != "signed-priority-hold" || hold.Reason != "signed-priority-reason" ||
		!hold.CreatedAt.Equal(holdNoStoreNow) || hold.ReleasedAt != nil || hold.ReleasedBy != "" {
		t.Fatalf("signed tenant priority changed the created Hold: %+v", hold)
	}

	// The real create/list/release journey. The list below also supplies a
	// foreign query selector: the signed tenant-a claim stays authoritative.
	beforeList := holdNoStoreSnapshot(t, fixture.store)
	list := fixture.do(t, http.MethodGet, "/api/v1/legal-holds?tenant_id=tenant-b", caseWorker, nil)
	items := decodeHoldList(t, "list success", list)
	if !reflect.DeepEqual(items, []domain.LegalHold{hold}) {
		t.Fatalf("tenant-a Hold list=%+v, want exactly the created Hold %+v", items, hold)
	}
	if otherItems := decodeHoldList(t, "neighbor list success", fixture.do(t, http.MethodGet, "/api/v1/legal-holds", otherTenant, nil)); len(otherItems) != 0 {
		t.Fatalf("tenant-b list observed tenant-a Holds: %+v", otherItems)
	}
	assertHoldNoFactsChanged(t, beforeList, holdNoStoreSnapshot(t, fixture.store), hold.ID)
	assertHoldFactCounts(t, fixture.store, hold.ID, 1, 0)
	assertHoldNoWORM(t, fixture.archive, "create/list journey")

	releasedResponse := fixture.do(t, http.MethodPost, "/api/v1/legal-holds/"+hold.ID+"/release", releaseWorker, nil)
	assertHoldNoStore(t, "release success", releasedResponse)
	if releasedResponse.status != http.StatusOK {
		t.Fatalf("release success status=%d body=%s", releasedResponse.status, releasedResponse.body)
	}
	var released domain.LegalHold
	if err := json.Unmarshal(releasedResponse.body, &released); err != nil {
		t.Fatalf("released Hold decode: %v (body=%s)", err, releasedResponse.body)
	}
	if released.ID != hold.ID || released.TenantID != hold.TenantID || released.Name != hold.Name ||
		released.Reason != hold.Reason || !reflect.DeepEqual(released.Filter, hold.Filter) ||
		released.CreatedBy != hold.CreatedBy || !released.CreatedAt.Equal(hold.CreatedAt) ||
		released.ReleasedBy != "hold-release-worker" || released.ReleasedAt == nil ||
		!released.ReleasedAt.Equal(holdNoStoreNow) {
		t.Fatalf("first release changed original fields or decision: created=%+v released=%+v", hold, released)
	}
	assertHoldFactCounts(t, fixture.store, hold.ID, 1, 1)

	relisted := decodeHoldList(t, "list after release", fixture.do(t, http.MethodGet, "/api/v1/legal-holds", caseWorker, nil))
	if !reflect.DeepEqual(relisted, []domain.LegalHold{released}) {
		t.Fatalf("list after release=%+v, want %+v", relisted, released)
	}
	beforeRepeat := holdNoStoreSnapshot(t, fixture.store)
	repeat := fixture.do(t, http.MethodPost, "/api/v1/legal-holds/"+hold.ID+"/release", secondReleaser, nil)
	assertHoldNoStore(t, "repeat release", repeat)
	if repeat.status != http.StatusOK {
		t.Fatalf("repeat release status=%d body=%s", repeat.status, repeat.body)
	}
	var repeated domain.LegalHold
	if err := json.Unmarshal(repeat.body, &repeated); err != nil {
		t.Fatalf("repeat release decode: %v (body=%s)", err, repeat.body)
	}
	if !reflect.DeepEqual(repeated, released) {
		t.Fatalf("repeat release replaced the first decision: first=%+v repeat=%+v", released, repeated)
	}
	assertHoldNoFactsChanged(t, beforeRepeat, holdNoStoreSnapshot(t, fixture.store), hold.ID)
	assertHoldFactCounts(t, fixture.store, hold.ID, 1, 1)
	finalList := decodeHoldList(t, "list after repeat release", fixture.do(t, http.MethodGet, "/api/v1/legal-holds", caseWorker, nil))
	if !reflect.DeepEqual(finalList, []domain.LegalHold{released}) {
		t.Fatalf("final Hold list=%+v, want %+v", finalList, released)
	}
	assertHoldNoWORM(t, fixture.archive, "full create/list/release/list/repeat journey")

	// Same-path durability: stop HTTP first, close the owned file Store with a
	// visible error, then reopen/re-observe the first release decision.
	fixture.stopHTTP()
	fixture.closeStore(t)
	reopened, err := store.Open(fixture.path)
	if err != nil {
		t.Fatalf("could not reopen Legal Hold store on the same path: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("closing reopened Legal Hold store: %v", err)
		}
	})
	reopenedArchive := &holdNoStoreArchive{inner: &archive.FileStore{Dir: fixture.archiveDir}}
	reopenedService, err := service.New(reopened, service.Config{
		Archive:         reopenedArchive,
		ArchiveDir:      fixture.archiveDir,
		Now:             func() time.Time { return holdNoStoreNow },
		AllowDevSecrets: true,
	})
	if err != nil {
		t.Fatalf("could not build reopened Legal Hold service: %v", err)
	}
	reopenedItems, err := reopenedService.ListLegalHolds(testCtx, "tenant-a")
	if err != nil || !reflect.DeepEqual(reopenedItems, []domain.LegalHold{released}) {
		t.Fatalf("reopened Hold list=%+v err=%v, want %+v", reopenedItems, err, released)
	}
	assertHoldFactCounts(t, reopened, hold.ID, 1, 1)
	if empty, err := reopenedService.ListLegalHolds(testCtx, "tenant-b"); err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("reopened neighbor list lost its empty non-nil array: items=%+v err=%v", empty, err)
	}
	assertHoldNoWORM(t, reopenedArchive, "reopened list")
}

func TestHTTPHoldNoStoreRejectionsFailures(t *testing.T) {
	fixture := newHoldNoStoreFixture(t)

	caseWorker := mintRestoreJWT(t, "hold-case-worker", "tenant-a", []string{"compliance"})
	otherTenant := mintRestoreJWT(t, "hold-other-tenant", "tenant-b", []string{"compliance"})
	platform := mintRestoreJWT(t, "hold-platform", "", []string{"platform-admin"})

	holdBody := holdNoStoreBody(t, domain.LegalHold{Name: holdNoStoreSecretName, Reason: holdNoStoreSecretReason})
	created := fixture.do(t, http.MethodPost, "/api/v1/legal-holds", caseWorker, holdBody)
	assertHoldNoStore(t, "active Hold creation", created)
	if created.status != http.StatusCreated {
		t.Fatalf("active Hold creation status=%d body=%s", created.status, created.body)
	}
	var hold domain.LegalHold
	if err := json.Unmarshal(created.body, &hold); err != nil {
		t.Fatalf("active Hold decode: %v (body=%s)", err, created.body)
	}

	// Cross-tenant release is a masked 404 with no tenant-a name, reason,
	// filter, creator or release field in the body.
	assertHoldError(t, "cross-tenant release", fixture.do(t, http.MethodPost, "/api/v1/legal-holds/"+hold.ID+"/release", otherTenant, nil), http.StatusNotFound, "not_found", holdNoStoreSecretName, holdNoStoreSecretReason, hold.CreatedBy)
	if current := holdNoStoreSnapshot(t, fixture.store).LegalHolds[hold.ID]; !reflect.DeepEqual(current, hold) {
		t.Fatalf("cross-tenant release changed the Hold: before=%+v after=%+v", hold, current)
	}
	assertHoldFactCounts(t, fixture.store, hold.ID, 1, 0)

	// A platform caller's body tenant cannot contradict its explicit query
	// selector, and a signed tenant's body/query can never widen its scope.
	mismatch := holdNoStoreBody(t, domain.LegalHold{TenantID: "tenant-b", Name: "platform-mismatch", Reason: "platform-mismatch-reason"})
	assertHoldError(t, "platform body tenant mismatch", fixture.do(t, http.MethodPost, "/api/v1/legal-holds?tenant_id=tenant-a", platform, mismatch), http.StatusBadRequest, "invalid_request", holdNoStoreSecretName, holdNoStoreSecretReason)
	widened := fixture.do(t, http.MethodPost, "/api/v1/legal-holds?tenant_id=tenant-b", caseWorker,
		holdNoStoreBody(t, domain.LegalHold{TenantID: "tenant-b", Name: "signed-scope-hold", Reason: "signed-scope-reason"}))
	assertHoldNoStore(t, "signed tenant scope", widened)
	if widened.status != http.StatusCreated {
		t.Fatalf("signed tenant scope create status=%d body=%s", widened.status, widened.body)
	}
	var widenedHold domain.LegalHold
	if err := json.Unmarshal(widened.body, &widenedHold); err != nil {
		t.Fatalf("signed tenant scope decode: %v (body=%s)", err, widened.body)
	}
	if widenedHold.TenantID != "tenant-a" || widenedHold.Name != "signed-scope-hold" {
		t.Fatalf("signed tenant was widened by query/body: %+v", widenedHold)
	}
	if neighbor := decodeHoldList(t, "neighbor list after attempted widening", fixture.do(t, http.MethodGet, "/api/v1/legal-holds", otherTenant, nil)); len(neighbor) != 0 {
		t.Fatalf("tenant-b observed a widened Hold: %+v", neighbor)
	}

	// Deterministic pre-Save control failure: create and release return the
	// fixed redacted 500, no success object, no fault diagnostic and no new
	// durable fact. This fixture seam fails before the write; the test does not
	// reinterpret any unknown write outcome as a rollback.
	beforeFault := holdNoStoreSnapshot(t, fixture.store)
	fixture.faultArmed.Store(true)
	faultBody := holdNoStoreBody(t, domain.LegalHold{Name: holdNoStoreSecretName + "-fault", Reason: holdNoStoreSecretReason + "-fault"})
	assertHoldError(t, "create control write failure", fixture.do(t, http.MethodPost, "/api/v1/legal-holds", caseWorker, faultBody), http.StatusInternalServerError, "internal_error", holdNoStoreSecretName, holdNoStoreSecretReason, holdNoStoreFaultText)
	assertHoldError(t, "release control write failure", fixture.do(t, http.MethodPost, "/api/v1/legal-holds/"+hold.ID+"/release", caseWorker, nil), http.StatusInternalServerError, "internal_error", holdNoStoreSecretName, holdNoStoreSecretReason, holdNoStoreFaultText)
	fixture.faultArmed.Store(false)
	afterFault := holdNoStoreSnapshot(t, fixture.store)
	if !reflect.DeepEqual(beforeFault.LegalHolds, afterFault.LegalHolds) || !reflect.DeepEqual(beforeFault.AdminActions, afterFault.AdminActions) {
		t.Fatalf("failed control writes served/leaked a successful mutation: before=%+v after=%+v", beforeFault.LegalHolds, afterFault.LegalHolds)
	}
	assertHoldFactCounts(t, fixture.store, hold.ID, 1, 0)

	// Caller withdrawal keeps the existing 499 classification and fixed
	// cancellation envelope. The list and checked release both consult the
	// caller context before service work; neither serves a success object.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	assertHoldError(t, "list caller withdrawal", fixture.doHandler(t, cancelled, http.MethodGet, "/api/v1/legal-holds", "dev:tenant-a:compliance", nil), statusClientClosedRequest, "client_cancelled", holdNoStoreSecretName, holdNoStoreSecretReason)
	assertHoldError(t, "release caller withdrawal", fixture.doHandler(t, cancelled, http.MethodPost, "/api/v1/legal-holds/"+hold.ID+"/release", "dev:tenant-a:compliance", nil), statusClientClosedRequest, "client_cancelled", holdNoStoreSecretName, holdNoStoreSecretReason)
	if current := holdNoStoreSnapshot(t, fixture.store).LegalHolds[hold.ID]; !reflect.DeepEqual(current, hold) {
		t.Fatalf("caller withdrawal changed the Hold: before=%+v after=%+v", hold, current)
	}
	assertHoldFactCounts(t, fixture.store, hold.ID, 1, 0)
	assertHoldNoWORM(t, fixture.archive, "refusal/fault/withdrawal coverage")
}
