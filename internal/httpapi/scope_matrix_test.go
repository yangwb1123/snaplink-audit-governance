package httpapi

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

type scopedRequestCase struct {
	name   string
	method string
	path   string
	body   string
}

func platformScopedRequests() []scopedRequestCase {
	query := "?from=2023-11-14T22:13:20Z&to=2023-11-14T22:13:40Z"
	return []scopedRequestCase{
		{"console detail", http.MethodGet, "/api/v1/compat/snaplink/audit/events/event", ""},
		{"event query", http.MethodGet, "/api/v1/events" + query, ""},
		{"event detail", http.MethodGet, "/api/v1/events/event", ""},
		{"receipt", http.MethodGet, "/api/v1/events/event/receipt", ""},
		{"operation", http.MethodGet, "/api/v1/operations/op", ""},
		{"operation timeline", http.MethodGet, "/api/v1/operations/op/timeline", ""},
		{"operation replay", http.MethodGet, "/api/v1/operations/op/replay", ""},
		{"aggregate timeline", http.MethodGet, "/api/v1/aggregates/invoice/inv/timeline", ""},
		{"create export", http.MethodPost, "/api/v1/exports", `{"from":"2023-11-14T22:13:20Z","to":"2023-11-14T22:13:40Z"}`},
		{"get export", http.MethodGet, "/api/v1/exports/job", ""},
		{"download export", http.MethodGet, "/api/v1/exports/job/download", ""},
		{"verify integrity", http.MethodPost, "/api/v1/integrity/verify", `{}`},
		{"create legal hold", http.MethodPost, "/api/v1/legal-holds", `{"name":"case","reason":"review"}`},
		{"list legal holds", http.MethodGet, "/api/v1/legal-holds", ""},
		{"release legal hold", http.MethodPost, "/api/v1/legal-holds/hold/release", ""},
		{"preview restore", http.MethodPost, "/api/v1/restores/preview", `{"operation_id":"op","reason":"review"}`},
		{"create restore", http.MethodPost, "/api/v1/restores", `{"operation_id":"op","reason":"review"}`},
		{"get restore", http.MethodGet, "/api/v1/restores/run", ""},
		{"approve restore", http.MethodPost, "/api/v1/restores/run/approve", ""},
		{"reject restore", http.MethodPost, "/api/v1/restores/run/reject", ""},
		{"create source", http.MethodPost, "/api/v1/sources", `{"id":"new-source","name":"New source"}`},
		{"list sources", http.MethodGet, "/api/v1/sources", ""},
		{"update source", http.MethodPut, "/api/v1/sources/crm", `{"name":"CRM","active":true}`},
		{"create schema", http.MethodPost, "/api/v1/schemas", `{"schema_id":"new-schema","version":1,"event_type":"audit.event"}`},
		{"list schemas", http.MethodGet, "/api/v1/schemas", ""},
		{"set retention", http.MethodPut, "/api/v1/policies/retention", `{"tenant_id":"tenant-a","hot_days":1,"warm_days":1,"archive_days":1,"retention_class":"standard"}`},
		{"get retention", http.MethodGet, "/api/v1/policies/retention", ""},
		{"evaluate retention", http.MethodPost, "/api/v1/retention/evaluate", ""},
	}
}

func TestPlatformExplicitSelectionMatrix(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()
	before := snapshotBytes(t, st)
	beforeTrail := trailBytes(t, st)
	cases := platformScopedRequests()
	if len(cases) != 28 {
		t.Fatalf("scoped query operation matrix has %d cases, want 28", len(cases))
	}
	for _, selector := range []struct {
		name   string
		suffix string
	}{
		{name: "omitted"},
		{name: "empty", suffix: "tenant_id="},
		{name: "repeated", suffix: "tenant_id=tenant-a&tenant_id=tenant-b"},
		{name: "malformed", suffix: "tenant_id=%zz"},
		{name: "invalid", suffix: "tenant_id=a:b"},
	} {
		for _, tc := range cases {
			tc := tc
			t.Run(selector.name+"/"+tc.name, func(t *testing.T) {
				var body io.Reader
				if tc.body != "" {
					body = bytes.NewBufferString(tc.body)
				}
				path := tc.path
				if selector.suffix != "" {
					separator := "?"
					if strings.Contains(path, "?") {
						separator = "&"
					}
					path += separator + selector.suffix
				}
				request, err := http.NewRequest(tc.method, server.URL+path, body)
				if err != nil {
					t.Fatal(err)
				}
				request.Header.Set("Authorization", "Bearer dev:platform:platform-admin")
				response, err := http.DefaultClient.Do(request)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				var result struct {
					Error struct {
						Code string `json:"code"`
					} `json:"error"`
				}
				if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
					t.Fatal(err)
				}
				if response.StatusCode != http.StatusBadRequest || result.Error.Code != "invalid_request" {
					t.Fatalf("status=%d code=%q, want 400 invalid_request", response.StatusCode, result.Error.Code)
				}
				if after := snapshotBytes(t, st); !bytes.Equal(before, after) {
					t.Fatalf("rejected %s changed the store", tc.name)
				}
				if after := trailBytes(t, st); !bytes.Equal(beforeTrail, after) {
					t.Fatalf("rejected %s appended a self-audit fact", tc.name)
				}
			})
		}
	}
}

func TestPlatformBodySelectionAndBatchPreflight(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()
	if err := st.Update(func(data *store.Snapshot) error {
		source := data.Sources[store.SourceKey("tenant-a", "crm")]
		source.AllowedClientIDs = []string{"crm", "platform"}
		data.Sources[store.SourceKey("tenant-a", "crm")] = source
		data.Sources[store.SourceKey("tenant-b", "crm")] = domain.SourceSystem{TenantID: "tenant-b", ID: "crm", Name: "CRM B", Active: true, AllowedClientIDs: []string{"platform"}}
		data.Schemas[store.SchemaKey("tenant-b", "audit.event", 1)] = domain.EventSchema{TenantID: "tenant-b", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before := snapshotBytes(t, st)
	selected := httpReadSeedEvent()
	selected.EventID = "platform-body-event"
	selected.IdempotencyKey = "platform-body-idem"
	selected.TenantID = "tenant-a"
	if result := postTestEvent(t, server.URL, "dev:platform:platform-admin", selected); result.Status != http.StatusAccepted {
		t.Fatalf("platform body-selected event status=%d code=%q", result.Status, result.Code)
	}
	batchA := httpReadSeedEvent()
	batchA.EventID = "platform-batch-a"
	batchA.IdempotencyKey = "platform-batch-a-idem"
	batchA.TenantID = "tenant-a"
	batchB := httpReadSeedEvent()
	batchB.EventID = "platform-batch-b"
	batchB.IdempotencyKey = "platform-batch-b-idem"
	batchB.TenantID = "tenant-b"
	batchBody, err := json.Marshal(map[string]any{"events": []domain.Event{batchA, batchB}})
	if err != nil {
		t.Fatal(err)
	}
	batchRequest, err := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events:batch", bytes.NewReader(batchBody))
	if err != nil {
		t.Fatal(err)
	}
	batchRequest.Header.Set("Authorization", "Bearer dev:platform:platform-admin")
	batchResponse, err := http.DefaultClient.Do(batchRequest)
	if err != nil {
		t.Fatal(err)
	}
	var batchResult struct {
		Receipts []domain.EventReceipt `json:"receipts"`
	}
	if err := json.NewDecoder(batchResponse.Body).Decode(&batchResult); err != nil {
		batchResponse.Body.Close()
		t.Fatal(err)
	}
	batchResponse.Body.Close()
	if batchResponse.StatusCode != http.StatusAccepted || len(batchResult.Receipts) != 2 || batchResult.Receipts[0].TenantID != "tenant-a" || batchResult.Receipts[1].TenantID != "tenant-b" {
		t.Fatalf("platform batch status=%d receipts=%+v", batchResponse.StatusCode, batchResult.Receipts)
	}
	before = snapshotBytes(t, st)
	base := httpReadSeedEvent()
	base.TenantID = ""
	body, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := json.Marshal(map[string]any{"events": []domain.Event{base}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   []byte
	}{
		{"single event missing body tenant", http.MethodPost, "/api/v1/events", body},
		{"batch member missing body tenant", http.MethodPost, "/api/v1/events:batch", batch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request, err := http.NewRequest(tc.method, server.URL+tc.path, bytes.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Authorization", "Bearer dev:platform:platform-admin")
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			var result struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if decodeErr := json.NewDecoder(response.Body).Decode(&result); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			response.Body.Close()
			if response.StatusCode != http.StatusBadRequest || result.Error.Code != "invalid_request" {
				t.Fatalf("status=%d code=%q, want 400 invalid_request", response.StatusCode, result.Error.Code)
			}
		})
	}
	if after := snapshotBytes(t, st); !bytes.Equal(before, after) {
		t.Fatal("body-scope failures changed the store")
	}
	before = snapshotBytes(t, st)
	mismatchA := httpReadSeedEvent()
	mismatchA.EventID = "tenant-token-batch-a"
	mismatchA.IdempotencyKey = "tenant-token-batch-a-idem"
	mismatchA.TenantID = "tenant-a"
	mismatchB := mismatchA
	mismatchB.EventID = "tenant-token-batch-b"
	mismatchB.IdempotencyKey = "tenant-token-batch-b-idem"
	mismatchB.TenantID = "tenant-b"
	mismatchBody, err := json.Marshal(map[string]any{"events": []domain.Event{mismatchA, mismatchB}})
	if err != nil {
		t.Fatal(err)
	}
	mismatchRequest, err := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events:batch", bytes.NewReader(mismatchBody))
	if err != nil {
		t.Fatal(err)
	}
	mismatchRequest.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	mismatchResponse, err := http.DefaultClient.Do(mismatchRequest)
	if err != nil {
		t.Fatal(err)
	}
	mismatchResponse.Body.Close()
	if mismatchResponse.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("tenant-token mismatch batch status=%d, want 422", mismatchResponse.StatusCode)
	}
	if after := snapshotBytes(t, st); !bytes.Equal(before, after) {
		t.Fatal("tenant-token mismatch batch committed a prefix")
	}
}

func TestPlatformBodyQueryConflictIsRejected(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()
	before := snapshotBytes(t, st)
	event := httpReadSeedEvent()
	event.EventID = "scope-conflict"
	event.IdempotencyKey = "scope-conflict-idem"
	event.TenantID = "tenant-b"
	body, _ := json.Marshal(event)
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events?tenant_id=tenant-a", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:platform:platform-admin")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400 for body/query tenant conflict", response.StatusCode)
	}
	if after := snapshotBytes(t, st); !bytes.Equal(before, after) {
		t.Fatal("body/query conflict changed the store")
	}
}

func TestTenantTokenIsolationMatrix(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	if result := postTestEvent(t, server.URL, "dev:tenant-a:service:crm", httpReadSeedEvent()); result.Status != http.StatusAccepted {
		t.Fatalf("seed status=%d", result.Status)
	}
	token := "dev:tenant-a:tenant-admin"
	for _, tc := range []struct {
		name string
		path string
	}{
		{"events", "/api/v1/events?from=2023-11-14T22:13:20Z&to=2023-11-14T22:13:40Z&tenant_id=tenant-b"},
		{"sources", "/api/v1/sources?tenant_id=tenant-b"},
		{"schemas", "/api/v1/schemas?tenant_id=tenant-b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request, _ := http.NewRequest(http.MethodGet, server.URL+tc.path, nil)
			request.Header.Set("Authorization", "Bearer "+token)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("status=%d, want 200 with signed tenant scope", response.StatusCode)
			}
			data, _ := io.ReadAll(response.Body)
			if bytes.Contains(data, []byte("tenant-b")) {
				t.Fatalf("signed tenant token widened through query: %s", data)
			}
		})
	}
}

func TestApprovedAllTenantAllowlist(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()
	if err := st.Update(func(data *store.Snapshot) error {
		source := data.Sources[store.SourceKey("tenant-a", "crm")]
		source.AllowedClientIDs = []string{"crm", "platform"}
		data.Sources[store.SourceKey("tenant-a", "crm")] = source
		data.Sources[store.SourceKey("tenant-b", "crm")] = domain.SourceSystem{TenantID: "tenant-b", ID: "crm", Name: "CRM B", Active: true, AllowedClientIDs: []string{"platform"}}
		data.Schemas[store.SchemaKey("tenant-b", "audit.event", 1)] = domain.EventSchema{TenantID: "tenant-b", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}
		data.AdminActions = append(data.AdminActions, domain.AdminAction{ID: "foreign-admin", TenantID: "tenant-b", Actor: "operator", Action: "tenant.created", CreatedAt: time.Unix(1_700_000_000, 0).UTC()})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	local := httpReadSeedEvent()
	local.EventID = "local-console-event"
	local.IdempotencyKey = "local-console-idem"
	local.TenantID = "tenant-a"
	if result := postTestEvent(t, server.URL, "dev:platform:platform-admin", local); result.Status != http.StatusAccepted {
		t.Fatalf("tenant-a platform seed status=%d code=%q", result.Status, result.Code)
	}
	foreign := httpReadSeedEvent()
	foreign.EventID = "foreign-console-event"
	foreign.IdempotencyKey = "foreign-console-idem"
	foreign.TenantID = "tenant-b"
	if result := postTestEvent(t, server.URL, "dev:platform:platform-admin", foreign); result.Status != http.StatusAccepted {
		t.Fatalf("tenant-b platform seed status=%d code=%q", result.Status, result.Code)
	}
	if err := st.Update(func(data *store.Snapshot) error {
		// Keep a second tenant-b payload hot as well as the HTTP-seeded
		// archived event. This makes the all-tenant assertion independent of
		// archive fallback mechanics.
		hot := foreign
		hot.EventID = "foreign-console-hot"
		data.Events[store.EventKey("tenant-b", hot.EventID)] = hot
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	get := func(path string) []byte {
		t.Helper()
		request, _ := http.NewRequest(http.MethodGet, server.URL+path, nil)
		request.Header.Set("Authorization", "Bearer dev:platform:platform-admin")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			data, _ := io.ReadAll(response.Body)
			t.Fatalf("GET %s status=%d body=%s", path, response.StatusCode, data)
		}
		data, _ := io.ReadAll(response.Body)
		return data
	}
	if data := get("/api/v1/compat/snaplink/audit/events"); !bytes.Contains(data, []byte("tenant-b")) {
		t.Fatalf("approved all-tenant event route omitted tenant-b: %s", data)
	}
	if data := get("/api/v1/admin/actions"); !bytes.Contains(data, []byte("tenant-b")) {
		t.Fatalf("approved all-tenant admin route omitted tenant-b: %s", data)
	}
	consoleToken := mintTenantlessConsoleToken(t)
	consoleRequest, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/compat/snaplink/audit/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	consoleRequest.Header.Set("Authorization", "Bearer "+consoleToken)
	consoleResponse, err := http.DefaultClient.Do(consoleRequest)
	if err != nil {
		t.Fatal(err)
	}
	consoleResponse.Body.Close()
	if consoleResponse.StatusCode != http.StatusOK {
		t.Fatalf("tenantless Console event read status=%d, want 200", consoleResponse.StatusCode)
	}
	consoleDetail, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/events/local-console-event", nil)
	if err != nil {
		t.Fatal(err)
	}
	consoleDetail.Header.Set("Authorization", "Bearer "+consoleToken)
	detailResponse, err := http.DefaultClient.Do(consoleDetail)
	if err != nil {
		t.Fatal(err)
	}
	detailResponse.Body.Close()
	if detailResponse.StatusCode != http.StatusUnauthorized {
		t.Fatalf("tenantless Console detail status=%d, want 401", detailResponse.StatusCode)
	}
	filteredEvents := get("/api/v1/compat/snaplink/audit/events?tenant_id=tenant-a")
	if bytes.Contains(filteredEvents, []byte("tenant-b")) || !bytes.Contains(filteredEvents, []byte("local-console-event")) {
		t.Fatalf("filtered Console events crossed tenant scope: %s", filteredEvents)
	}
	var facets struct {
		Facets struct {
			Total int `json:"total"`
		} `json:"facets"`
	}
	data := get("/api/v1/compat/snaplink/audit/facets")
	if err := json.Unmarshal(data, &facets); err != nil {
		t.Fatal(err)
	}
	if facets.Facets.Total < 2 {
		t.Fatalf("approved all-tenant facets total=%d, want both tenants: %s", facets.Facets.Total, data)
	}
	var filteredFacets struct {
		Facets struct {
			Total int `json:"total"`
		} `json:"facets"`
	}
	filteredFacetData := get("/api/v1/compat/snaplink/audit/facets?tenant_id=tenant-a")
	if err := json.Unmarshal(filteredFacetData, &filteredFacets); err != nil {
		t.Fatal(err)
	}
	if filteredFacets.Facets.Total != 1 {
		t.Fatalf("filtered Console facets total=%d, want tenant-a only: %s", filteredFacets.Facets.Total, filteredFacetData)
	}
	filteredAdmin := get("/api/v1/admin/actions?tenant_id=tenant-a")
	if bytes.Contains(filteredAdmin, []byte("tenant-b")) {
		t.Fatalf("filtered admin actions crossed tenant scope: %s", filteredAdmin)
	}
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/events/foreign-console-event", nil)
	request.Header.Set("Authorization", "Bearer dev:platform:platform-admin")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("scoped detail without selector status=%d, want 400", response.StatusCode)
	}
}

func TestSourceUpdateTenantScopeRegression(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()
	if err := st.Update(func(data *store.Snapshot) error {
		data.Sources[store.SourceKey("tenant-b", "crm")] = domain.SourceSystem{TenantID: "tenant-b", ID: "crm", Name: "CRM B", Active: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	update := func(token, path, body string) (int, domain.SourceSystem) {
		t.Helper()
		request, err := http.NewRequest(http.MethodPut, server.URL+path, bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var source domain.SourceSystem
		if response.StatusCode == http.StatusOK {
			if err := json.NewDecoder(response.Body).Decode(&source); err != nil {
				t.Fatal(err)
			}
		}
		return response.StatusCode, source
	}
	body := `{"name":"CRM A updated","active":true}`
	if status, source := update("dev:platform:platform-admin", "/api/v1/sources/crm?tenant_id=tenant-a", body); status != http.StatusOK || source.TenantID != "tenant-a" {
		t.Fatalf("selected tenant-a update status=%d source=%+v", status, source)
	}
	if status, _ := update("dev:platform:platform-admin", "/api/v1/sources/crm", body); status != http.StatusBadRequest {
		t.Fatalf("unscoped platform update status=%d, want 400", status)
	}
	if status, _ := update("dev:platform:platform-admin", "/api/v1/sources/crm?tenant_id=tenant-a", `{"tenant_id":"tenant-b","name":"redirect","active":true}`); status != http.StatusBadRequest {
		t.Fatalf("body tenant redirect status=%d, want 400", status)
	}
	if status, source := update("dev:tenant-a:tenant-admin", "/api/v1/sources/crm?tenant_id=tenant-b", `{"name":"CRM A tenant token","active":true}`); status != http.StatusOK || source.TenantID != "tenant-a" {
		t.Fatalf("tenant-token override status=%d source=%+v", status, source)
	}
	if err := st.Read(func(data *store.Snapshot) error {
		if got := data.Sources[store.SourceKey("tenant-a", "crm")].Name; got != "CRM A tenant token" {
			t.Fatalf("tenant-a source name=%q, want tenant-token update", got)
		}
		if got := data.Sources[store.SourceKey("tenant-b", "crm")].Name; got != "CRM B" {
			t.Fatalf("tenant-b source was modified: %q", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTenantSelectorRejectsMalformedAndRepeatedValues(t *testing.T) {
	for _, raw := range []string{"/api/v1/events/x?tenant_id=tenant-a&tenant_id=tenant-b", "/api/v1/events/x?tenant_id=%zz"} {
		request := httptest.NewRequest(http.MethodGet, raw, nil)
		if _, err := (&Server{}).tenantFor(request, auth.Claims{Platform: true}); err == nil {
			t.Fatalf("tenant selector %q was accepted", raw)
		}
	}
}

func TestScopeResolverPrecedenceAndBodyRules(t *testing.T) {
	server := &Server{}
	platform := auth.Claims{Platform: true, TenantID: "platform"}
	for _, raw := range []string{"/api/v1/events/x", "/api/v1/events/x?tenant_id=", "/api/v1/events/x?tenant_id=a:b"} {
		request := httptest.NewRequest(http.MethodGet, raw, nil)
		if _, err := server.resolveScope(request, platform, ScopeScopedQuery); err == nil {
			t.Fatalf("scoped platform request %q was accepted", raw)
		}
	}
	tenant := auth.Claims{TenantID: "tenant-a"}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/events/x?tenant_id=tenant-b", nil)
	scope, err := server.resolveScope(request, tenant, ScopeScopedQuery)
	if err != nil || scope.TenantID != "tenant-a" {
		t.Fatalf("tenant query override resolved to (%q, %v), want tenant-a", scope.TenantID, err)
	}
	request = httptest.NewRequest(http.MethodPost, "/api/v1/events?tenant_id=tenant-a", nil)
	if _, err := server.resolveBodyScope(request, platform, "tenant-b"); err == nil {
		t.Fatal("platform body/query tenant conflict was accepted")
	}
	if _, err := server.resolveBodyScope(httptest.NewRequest(http.MethodPost, "/api/v1/events", nil), tenant, "tenant-b"); err == nil || !errors.Is(err, domain.ErrTenantMismatch) {
		t.Fatalf("tenant body mismatch error=%v, want ErrTenantMismatch", err)
	}
	if scope, err := server.resolveBodyScope(httptest.NewRequest(http.MethodPost, "/api/v1/events", nil), auth.Claims{}, "tenant-a"); err != nil || scope.TenantID != "" {
		t.Fatalf("tenantless body selector resolved to (%q, %v), want empty compatibility hint", scope.TenantID, err)
	}
}

func mintTenantlessConsoleToken(t *testing.T) string {
	t.Helper()
	header, err := json.Marshal(map[string]any{"alg": "HS256", "typ": "at+jwt"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]any{"sub": "console-reader", "scope": "admin:read", "exp": 4_100_000_000})
	if err != nil {
		t.Fatal(err)
	}
	encodedHeader := base64.RawURLEncoding.EncodeToString(header)
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	unsigned := encodedHeader + "." + encodedPayload
	mac := hmac.New(sha256.New, []byte(testJWTSecret))
	_, _ = mac.Write([]byte(unsigned))
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func snapshotBytes(t *testing.T, st *store.Store) []byte {
	t.Helper()
	var data []byte
	if err := st.Read(func(snapshot *store.Snapshot) error {
		var err error
		data, err = json.Marshal(snapshot)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return data
}

func trailBytes(t *testing.T, st *store.Store) []byte {
	t.Helper()
	trail, err := st.ReadAdminTrail()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(trail)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
