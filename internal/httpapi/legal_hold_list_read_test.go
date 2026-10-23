package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

type holdListHTTPFixture struct {
	server *httptest.Server
	svc    *service.Service
	st     *store.Store
	path   string
}

var holdListNow = time.Unix(1_700_000_000, 0).UTC()

// The real file Store owns its cleanup immediately after a successful open; the
// HTTP server cleanup is registered afterwards so LIFO stops HTTP first. A
// close error is reported, never swallowed, on both normal and Fatal paths.
// Only the list route is exercised here: no export queue or background worker
// is started by this fixture.
func newHoldListHTTPFixture(t *testing.T) *holdListHTTPFixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal("could not open owned Hold list file store")
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("closing Hold list HTTP store: %v", err)
		}
	})
	svc, err := service.New(st, service.Config{ArchiveDir: filepath.Join(filepath.Dir(path), "archive"), Now: func() time.Time { return holdListNow }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal("could not build Hold list HTTP service")
	}
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		if err := svc.CreateTenant(testCtx, "test", domain.Tenant{ID: tenant, Name: tenant, Active: true}); err != nil {
			t.Fatal("could not create independent Hold list tenant")
		}
	}
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, log.New(io.Discard, "", 0)).Handler())
	t.Cleanup(server.Close)
	return &holdListHTTPFixture{server: server, svc: svc, st: st, path: path}
}

func holdListGET(t *testing.T, serverURL, token, query string) (int, []byte) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, serverURL+"/api/v1/legal-holds"+query, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("Hold list request failed: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, body
}

// holdListRawEnvelope checks the raw JSON contract, not just a decoded length:
// a nil Go slice marshals to items:null, which a []domain.LegalHold decode
// silently turns into an empty list. Both fields must be present and items must
// be a JSON array.
func holdListRawEnvelope(t *testing.T, body []byte) (json.RawMessage, json.RawMessage) {
	t.Helper()
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("could not decode Hold list envelope: %v body=%s", err, body)
	}
	items, ok := envelope["items"]
	if !ok {
		t.Fatalf("Hold list response has no items field: %s", body)
	}
	count, ok := envelope["count"]
	if !ok {
		t.Fatalf("Hold list response has no count field: %s", body)
	}
	trimmed := bytes.TrimSpace(items)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		t.Fatalf("Hold list items must be a JSON array, got %s", items)
	}
	var decoded []json.RawMessage
	if err := json.Unmarshal(trimmed, &decoded); err != nil {
		t.Fatalf("Hold list items is not a JSON array: %v (%s)", err, items)
	}
	return trimmed, bytes.TrimSpace(count)
}

func assertHoldListRawEmpty(t *testing.T, body []byte) {
	t.Helper()
	items, count := holdListRawEnvelope(t, body)
	if !bytes.Equal(items, []byte("[]")) || !bytes.Equal(count, []byte("0")) {
		t.Fatalf("Hold list no-match must be items:[] and count:0, got items=%s count=%s body=%s", items, count, body)
	}
}

func assertHoldListRawPayload(t *testing.T, body []byte, want []domain.LegalHold) {
	t.Helper()
	items, count := holdListRawEnvelope(t, body)
	var decodedCount int
	if err := json.Unmarshal(count, &decodedCount); err != nil {
		t.Fatalf("count is not a JSON number: %v (%s)", err, count)
	}
	var got []domain.LegalHold
	if err := json.Unmarshal(items, &got); err != nil {
		t.Fatalf("could not decode Hold list items: %v (%s)", err, items)
	}
	if decodedCount != len(want) || len(got) != len(want) {
		t.Fatalf("Hold list count=%d items=%d want=%d body=%s", decodedCount, len(got), len(want), body)
	}
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("Hold list fields/order changed: got=%s want=%s", gotJSON, wantJSON)
	}
}

func TestHTTPHoldListControlScopeIsolationFieldsAndSignedTenantPriority(t *testing.T) {
	fixture := newHoldListHTTPFixture(t)

	if status, body := holdListGET(t, fixture.server.URL, "", ""); status != http.StatusUnauthorized {
		t.Fatalf("unauthenticated Hold list status=%d body=%s, want 401", status, body)
	}
	if status, body := holdListGET(t, fixture.server.URL, "dev:tenant-a:auditor", ""); status != http.StatusForbidden {
		t.Fatalf("permissionless Hold list status=%d body=%s, want 403", status, body)
	}
	if status, body := holdListGET(t, fixture.server.URL, "dev:platform:platform-admin", ""); status != http.StatusBadRequest {
		t.Fatalf("platform Hold list without tenant status=%d body=%s, want 400", status, body)
	}

	// This tenant has no Hold at all (and no other tenant has one yet). The
	// raw success payload is still an array.
	if status, body := holdListGET(t, fixture.server.URL, "dev:tenant-a:compliance", ""); status != http.StatusOK {
		t.Fatalf("empty Hold list status=%d body=%s", status, body)
	} else {
		assertHoldListRawEmpty(t, body)
	}

	foreign, err := fixture.svc.CreateLegalHold(testCtx, domain.LegalHold{ID: "hold-list-neighbor", TenantID: "tenant-b", Name: "neighbor case", Reason: "neighbor reason",
		CreatedBy: "neighbor-worker", CreatedAt: holdListNow.Add(-3 * time.Minute)})
	if err != nil {
		t.Fatal("could not create independent neighboring Hold")
	}

	// Only the neighboring tenant has a Hold: this tenant's list stays an
	// empty array rather than null, and the neighboring Hold is not mixed in.
	if status, body := holdListGET(t, fixture.server.URL, "dev:tenant-a:compliance", ""); status != http.StatusOK {
		t.Fatalf("tenant-a Hold list status=%d body=%s", status, body)
	} else {
		assertHoldListRawEmpty(t, body)
	}
	wantB := []domain.LegalHold{foreign}
	if status, body := holdListGET(t, fixture.server.URL, "dev:tenant-b:compliance", ""); status != http.StatusOK {
		t.Fatalf("tenant-b Hold list status=%d body=%s", status, body)
	} else {
		assertHoldListRawPayload(t, body, wantB)
	}

	active, err := fixture.svc.CreateLegalHold(testCtx, domain.LegalHold{ID: "hold-list-active", TenantID: "tenant-a", Name: "active case", Reason: "active reason",
		Filter: domain.Query{From: holdListNow.Add(-time.Hour), To: holdListNow.Add(time.Hour)}, CreatedBy: "case-worker", CreatedAt: holdListNow.Add(-2 * time.Minute)})
	if err != nil {
		t.Fatal("could not create original active Hold")
	}
	released, err := fixture.svc.CreateLegalHold(testCtx, domain.LegalHold{ID: "hold-list-released", TenantID: "tenant-a", Name: "released case", Reason: "released reason",
		CreatedBy: "case-worker", CreatedAt: holdListNow.Add(-time.Minute)})
	if err != nil {
		t.Fatal("could not create original released Hold")
	}
	if released, err = fixture.svc.ReleaseLegalHold(testCtx, "tenant-a", released.ID, "release-worker"); err != nil {
		t.Fatal("could not release original Hold")
	}
	wantA := []domain.LegalHold{active, released}

	if status, body := holdListGET(t, fixture.server.URL, "dev:tenant-a:compliance", ""); status != http.StatusOK {
		t.Fatalf("tenant-a Hold list status=%d body=%s", status, body)
	} else {
		assertHoldListRawPayload(t, body, wantA)
	}
	if status, body := holdListGET(t, fixture.server.URL, "dev:tenant-b:compliance", ""); status != http.StatusOK {
		t.Fatalf("tenant-b Hold list status=%d body=%s", status, body)
	} else {
		assertHoldListRawPayload(t, body, wantB)
	}
	if status, body := holdListGET(t, fixture.server.URL, "dev:platform:platform-admin", "?tenant_id=tenant-b"); status != http.StatusOK {
		t.Fatalf("platform-selected Hold list status=%d body=%s", status, body)
	} else {
		assertHoldListRawPayload(t, body, wantB)
	}
	// A signed tenant is authoritative; the query selector cannot widen or
	// redirect a tenant token to the neighboring tenant.
	signedA := mintRestoreJWT(t, "case-reader", "tenant-a", []string{"compliance"})
	if status, body := holdListGET(t, fixture.server.URL, signedA, "?tenant_id=tenant-b"); status != http.StatusOK {
		t.Fatalf("signed-tenant Hold list status=%d body=%s", status, body)
	} else {
		assertHoldListRawPayload(t, body, wantA)
	}
	// A successful no-match stays an empty array with count 0, never null.
	if status, body := holdListGET(t, fixture.server.URL, "dev:platform:platform-admin", "?tenant_id=tenant-missing"); status != http.StatusOK {
		t.Fatalf("no-match Hold list status=%d body=%s", status, body)
	} else {
		assertHoldListRawEmpty(t, body)
	}

	// The HTTP fixture only observes lists. Stop HTTP before closing the file
	// store, then verify the same path reopens with the original records.
	fixture.server.Close()
	if err := fixture.st.Close(); err != nil {
		t.Fatalf("could not close Hold list file store after HTTP exit: %v", err)
	}
	reopened, err := store.Open(fixture.path)
	if err != nil {
		t.Fatalf("could not reopen Hold list store on the same path: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("closing reopened Hold list store: %v", err)
		}
	})
	reopenedSvc, err := service.New(reopened, service.Config{ArchiveDir: filepath.Join(filepath.Dir(fixture.path), "archive"), Now: func() time.Time { return holdListNow }, AllowDevSecrets: true})
	if err != nil {
		t.Fatalf("could not build reopened Hold list service: %v", err)
	}
	items, err := reopenedSvc.ListLegalHolds(testCtx, "tenant-a")
	if err != nil || items == nil || len(items) != len(wantA) {
		t.Fatalf("reopened Hold list failed: items=%+v err=%v", items, err)
	}
	gotJSON, err := json.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	wantJSON, err := json.Marshal(wantA)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("reopened Hold list changed fields/order: got=%s want=%s", gotJSON, wantJSON)
	}
	if foreignItems, err := reopenedSvc.ListLegalHolds(testCtx, "tenant-b"); err != nil || !reflect.DeepEqual(foreignItems, wantB) {
		t.Fatalf("reopened neighboring Hold list failed: items=%+v err=%v", foreignItems, err)
	}
	if empty, err := reopenedSvc.ListLegalHolds(testCtx, "tenant-missing"); err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("reopened no-match Hold list lost its empty non-nil list: items=%+v err=%v", empty, err)
	}
}
