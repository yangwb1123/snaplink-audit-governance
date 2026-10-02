package httpapi

// Control-plane resource caps at the HTTP boundary. The three write routes
// (POST /api/v1/sources, PUT /api/v1/sources/{sourceID}, POST /api/v1/schemas)
// enforce them in the service write boundary (normalizeSource /
// RegisterSchema), so this file proves the transport contract rather than
// re-implementing the rule: an over-cap list is a 400 invalid_request that
// names the cap, and nothing — no source row, no schema key, no admin action,
// no read-trail row — is persisted before the rejection.
//
// Every fixture asserts its serialized size is below the transport body cap
// (MaxEventBytes*2) before it is sent. Without that, a MaxBytesReader
// rejection would also be a 400 and the test could pass for the wrong reason:
// errorBody renders a raw size rejection as code "internal_error", so
// code == "invalid_request" is the discriminator.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// TestHTTPControlPlaneResourceCapPreflight pins both cap dimensions on all
// three control-plane write routes.
func TestHTTPControlPlaneResourceCapPreflight(t *testing.T) {
	for _, route := range []struct {
		name   string
		method string
		path   string
		cases  []struct {
			payload map[string]any
			want    string
		}
		// absent is the row key the request must not create; unchanged is the
		// pre-existing row that must come back byte-identical (PUT).
		absent    string
		unchanged string
	}{
		{
			name:   "create source",
			method: http.MethodPost,
			path:   "/api/v1/sources",
			cases: []struct {
				payload map[string]any
				want    string
			}{
				{map[string]any{"id": "capped-source", "name": "Capped", "allowed_client_ids": httpCapClientIDs(domain.MaxSourceClientIDs + 1)},
					fmt.Sprintf("allowed_client_ids count %d, max %d", domain.MaxSourceClientIDs+1, domain.MaxSourceClientIDs)},
				{map[string]any{"id": "capped-source", "name": "Capped", "allowed_client_ids": []string{strings.Repeat("c", domain.MaxEnvelopeFieldBytes+1)}},
					fmt.Sprintf("allowed_client_ids[] is %d bytes, max %d", domain.MaxEnvelopeFieldBytes+1, domain.MaxEnvelopeFieldBytes)},
			},
			absent: store.SourceKey("tenant-a", "capped-source"),
		},
		{
			name:   "update source",
			method: http.MethodPut,
			path:   "/api/v1/sources/crm",
			cases: []struct {
				payload map[string]any
				want    string
			}{
				{map[string]any{"name": "CRM renamed", "active": true, "allowed_client_ids": httpCapClientIDs(domain.MaxSourceClientIDs + 1)},
					fmt.Sprintf("allowed_client_ids count %d, max %d", domain.MaxSourceClientIDs+1, domain.MaxSourceClientIDs)},
				{map[string]any{"name": "CRM renamed", "active": true, "allowed_client_ids": []string{strings.Repeat("c", domain.MaxEnvelopeFieldBytes+1)}},
					fmt.Sprintf("allowed_client_ids[] is %d bytes, max %d", domain.MaxEnvelopeFieldBytes+1, domain.MaxEnvelopeFieldBytes)},
			},
			unchanged: store.SourceKey("tenant-a", "crm"),
		},
		{
			name:   "create schema",
			method: http.MethodPost,
			path:   "/api/v1/schemas",
			cases: []struct {
				payload map[string]any
				want    string
			}{
				{map[string]any{"schema_id": "capped.schema", "version": 1, "event_type": "audit.event",
					"required_fields": httpCapClientIDs(domain.MaxSchemaFields + 1)},
					fmt.Sprintf("required_fields count %d, max %d", domain.MaxSchemaFields+1, domain.MaxSchemaFields)},
				{map[string]any{"schema_id": "capped.schema", "version": 1, "event_type": "audit.event",
					"required_fields": []string{strings.Repeat("f", domain.MaxEnvelopeFieldBytes+1)}},
					fmt.Sprintf("required_fields[] is %d bytes, max %d", domain.MaxEnvelopeFieldBytes+1, domain.MaxEnvelopeFieldBytes)},
			},
			absent: store.SchemaKey("tenant-a", "capped.schema", 1),
		},
	} {
		for _, testCase := range route.cases {
			t.Run(route.name+" "+testCase.want, func(t *testing.T) {
				server, st := testHTTPServerWithStore(t)
				defer server.Close()

				before := httpControlPlaneState(t, st)
				beforeActions := httpAdminActionCount(t, server, "dev:tenant-a:tenant-admin")

				status, code, message := postHTTPControlPlane(t, server.URL+route.path, route.method, testCase.payload)
				if status != http.StatusBadRequest || code != "invalid_request" {
					t.Fatalf("over-cap %s response=%d %q, want 400 invalid_request", route.name, status, code)
				}
				if !strings.Contains(message, testCase.want) {
					t.Fatalf("over-cap %s message=%q does not contain %q", route.name, message, testCase.want)
				}
				if !strings.Contains(message, "envelope size cap exceeded") {
					t.Fatalf("over-cap %s message=%q must come from the shared domain helper", route.name, message)
				}

				assertHTTPControlPlaneStateUnchanged(t, st, before, route.absent)
				if afterActions := httpAdminActionCount(t, server, "dev:tenant-a:tenant-admin"); afterActions != beforeActions {
					t.Fatalf("over-cap %s appended admin actions: before=%d after=%d", route.name, beforeActions, afterActions)
				}
				if route.unchanged != "" {
					after := httpControlPlaneState(t, st).sources[route.unchanged]
					if len(after.AllowedClientIDs) != len(before.sources[route.unchanged].AllowedClientIDs) ||
						after.Name != before.sources[route.unchanged].Name ||
						!after.CreatedAt.Equal(before.sources[route.unchanged].CreatedAt) {
						t.Fatalf("rejected update mutated the stored source: before=%+v after=%+v", before.sources[route.unchanged], after)
					}
				}
			})
		}
	}
}

// TestHTTPControlPlaneCapsExactBoundaryAccepted is the MaxBatchEvents-boundary
// analogue for the control plane: the exact cap is served, and the full list is
// persisted — the caps reject above the boundary, not at it.
func TestHTTPControlPlaneCapsExactBoundaryAccepted(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()

	atCapIDs := httpCapClientIDs(domain.MaxSourceClientIDs)
	status, code, message := postHTTPControlPlane(t, server.URL+"/api/v1/sources", http.MethodPost,
		map[string]any{"id": "at-cap-source", "name": "At cap", "allowed_client_ids": atCapIDs})
	if status != http.StatusCreated || code != "" {
		t.Fatalf("at-cap source status=%d code=%q message=%q, want 201", status, code, message)
	}
	if err := st.Read(testCtx, func(data *store.Snapshot) error {
		stored, ok := data.Sources[store.SourceKey("tenant-a", "at-cap-source")]
		if !ok || len(stored.AllowedClientIDs) != domain.MaxSourceClientIDs {
			t.Fatalf("at-cap source stored %d entries", len(stored.AllowedClientIDs))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	status, code, message = postHTTPControlPlane(t, server.URL+"/api/v1/sources/crm", http.MethodPut,
		map[string]any{"name": "CRM", "active": true, "allowed_client_ids": atCapIDs})
	if status != http.StatusOK || code != "" {
		t.Fatalf("at-cap source update status=%d code=%q message=%q, want 200", status, code, message)
	}

	status, code, message = postHTTPControlPlane(t, server.URL+"/api/v1/schemas", http.MethodPost,
		map[string]any{"schema_id": "at-cap.schema", "version": 1, "event_type": "audit.event",
			"required_fields":   httpCapClientIDs(domain.MaxSchemaFields),
			"allowed_fields":    httpCapClientIDs(domain.MaxSchemaFields),
			"encrypted_fields":  []string{strings.Repeat("e", domain.MaxEnvelopeFieldBytes)},
			"searchable_fields": []string{strings.Repeat("s", domain.MaxEnvelopeFieldBytes)},
		})
	if status != http.StatusCreated || code != "" {
		t.Fatalf("at-cap schema status=%d code=%q message=%q, want 201", status, code, message)
	}
	if err := st.Read(testCtx, func(data *store.Snapshot) error {
		stored, ok := data.Schemas[store.SchemaKey("tenant-a", "at-cap.schema", 1)]
		if !ok {
			t.Fatal("at-cap schema was not persisted")
		}
		for name, entries := range map[string][]string{
			"required_fields": stored.RequiredFields, "allowed_fields": stored.AllowedFields,
			"encrypted_fields": stored.EncryptedFields, "searchable_fields": stored.SearchableFields,
		} {
			want := domain.MaxSchemaFields
			if name == "encrypted_fields" || name == "searchable_fields" {
				want = 1
			}
			if len(entries) != want {
				t.Fatalf("stored %s=%d entries, want %d", name, len(entries), want)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestHTTPControlPlaneCapsDoNotGateReads pins the scope guard at the transport
// level: a source row that predates the cap is still served verbatim by
// GET /api/v1/sources, so a deployment is never locked out of its own data.
func TestHTTPControlPlaneCapsDoNotGateReads(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()

	legacy := domain.SourceSystem{TenantID: "tenant-a", ID: "legacy", Name: "Legacy", Active: true,
		AllowedClientIDs: httpCapClientIDs(domain.MaxSourceClientIDs + 32)}
	if err := st.Update(testCtx, func(data *store.Snapshot) error {
		data.Sources[store.SourceKey("tenant-a", "legacy")] = legacy
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	request, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/sources", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer dev:tenant-a:tenant-admin")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("list sources status=%d, want 200", response.StatusCode)
	}
	var payload struct {
		Items []domain.SourceSystem `json:"items"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	for _, source := range payload.Items {
		if source.ID != "legacy" {
			continue
		}
		if len(source.AllowedClientIDs) != len(legacy.AllowedClientIDs) {
			t.Fatalf("GET /sources returned %d entries for the legacy row, want %d", len(source.AllowedClientIDs), len(legacy.AllowedClientIDs))
		}
		return
	}
	t.Fatal("legacy source row missing from GET /api/v1/sources")
}

type httpControlPlaneSnapshot struct {
	sources      map[string]domain.SourceSystem
	schemas      map[string]domain.EventSchema
	adminActions int
	trail        []domain.AdminAction
}

func httpControlPlaneState(t *testing.T, st *store.Store) httpControlPlaneSnapshot {
	t.Helper()
	state := httpControlPlaneSnapshot{sources: map[string]domain.SourceSystem{}, schemas: map[string]domain.EventSchema{}}
	if err := st.Read(testCtx, func(data *store.Snapshot) error {
		for key, source := range data.Sources {
			state.sources[key] = source
		}
		for key, schema := range data.Schemas {
			state.schemas[key] = schema
		}
		state.adminActions = len(data.AdminActions)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	trail, err := st.ReadAdminTrail(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	state.trail = trail
	return state
}

// assertHTTPControlPlaneStateUnchanged pins the pre-store rejection: absent
// must not exist, no row set changed, and no admin action or read-trail row
// was appended.
func assertHTTPControlPlaneStateUnchanged(t *testing.T, st *store.Store, before httpControlPlaneSnapshot, absent string) {
	t.Helper()
	after := httpControlPlaneState(t, st)
	if _, created := after.sources[absent]; created {
		t.Fatalf("rejected request created source %q", absent)
	}
	if _, created := after.schemas[absent]; created {
		t.Fatalf("rejected request created schema %q", absent)
	}
	if len(after.sources) != len(before.sources) || len(after.schemas) != len(before.schemas) {
		t.Fatalf("rejected request changed the row set: sources %d->%d schemas %d->%d",
			len(before.sources), len(after.sources), len(before.schemas), len(after.schemas))
	}
	if after.adminActions != before.adminActions {
		t.Fatalf("rejected request appended an admin action: before=%d after=%d", before.adminActions, after.adminActions)
	}
	if len(after.trail) != len(before.trail) {
		t.Fatalf("rejected request appended a read-trail fact: before=%d after=%d", len(before.trail), len(after.trail))
	}
}

// postHTTPControlPlane sends one control-plane write and returns the status
// plus the documented error envelope fields (code/message are empty on success).
func postHTTPControlPlane(t *testing.T, url, method string, payload map[string]any) (int, string, string) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture must stay compact: a cap violation has to be observable, not
	// masked by the transport body cap.
	if len(body) >= domain.MaxEventBytes*2 {
		t.Fatalf("fixture is %d bytes, must stay below the aggregate body cap %d", len(body), domain.MaxEventBytes*2)
	}
	request, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer dev:tenant-a:tenant-admin")
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("decode response %s: %v", raw, err)
	}
	return response.StatusCode, envelope.Error.Code, envelope.Error.Message
}

func httpCapClientIDs(n int) []string {
	entries := make([]string, n)
	for i := range entries {
		entries[i] = fmt.Sprintf("client-%06d", i)
	}
	sort.Strings(entries)
	return entries
}
