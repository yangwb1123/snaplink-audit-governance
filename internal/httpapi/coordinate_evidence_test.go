package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestHTTPCoordinateEventScopeExactSchemaAndNoStore(t *testing.T) {
	server, svc := timelineStripFixture(t) // actual file state and archived source
	defer svc.Store.Close()
	defer server.Close()
	objects := &initialQueryHTTPArchive{Store: svc.Config.Archive}
	svc.Config.Archive = objects
	reads := 0
	for _, phase := range []string{"valid", "misbound-schema"} {
		if phase == "misbound-schema" {
			if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
				key := store.SchemaKey("tenant-a", "audit.event", 2)
				schema := data.Schemas[key]
				schema.Version++
				data.Schemas[key] = schema
				return nil
			}); err != nil {
				t.Fatal("could not persist owned HTTP exact-Schema identity fault")
			}
		}
		for _, path := range []string{"/api/v1/events/strip-evt-1", "/api/v1/compat/snaplink/audit/events/strip-evt-1"} {
			for _, access := range []struct {
				name, token, selector string
				status                int
				gets                  int64
			}{
				{"tenant", "dev:tenant-a:auditor", "", http.StatusOK, 1},
				{"signed-scope-wins", "dev:tenant-a:auditor", "tenant-b", http.StatusOK, 1},
				{"platform-selected", "dev:platform:platform-admin", "tenant-a", http.StatusOK, 1},
				{"missing-credential", "", "", http.StatusUnauthorized, 0},
				{"missing-permission", "dev:tenant-a:viewer", "", http.StatusForbidden, 0},
				{"foreign-tenant", "dev:tenant-b:auditor", "tenant-a", http.StatusNotFound, 0},
				{"platform-unselected", "dev:platform:platform-admin", "", http.StatusBadRequest, 0},
			} {
				t.Run(phase+path+"/"+access.name, func(t *testing.T) {
					values := url.Values{}
					if access.selector != "" {
						values.Set("tenant_id", access.selector)
					}
					request, err := http.NewRequest(http.MethodGet, server.URL+path+"?"+values.Encode(), nil)
					if err != nil {
						t.Fatal("could not prepare scoped coordinate event request")
					}
					if access.token != "" {
						request.Header.Set("Authorization", "Bearer "+access.token)
					}
					objects.gets.Store(0)
					response, err := http.DefaultClient.Do(request)
					if err != nil {
						t.Fatal("coordinate event request did not return a response")
					}
					raw, readErr := io.ReadAll(response.Body)
					response.Body.Close()
					status := access.status
					if phase == "misbound-schema" && status == http.StatusOK {
						status = http.StatusConflict
					}
					if readErr != nil || response.StatusCode != status || response.Header.Get("Cache-Control") != "no-store" || objects.gets.Load() != access.gets {
						t.Fatal("coordinate event changed scope/no-store or Get happened before authorization/identity validation")
					}
					if status == http.StatusOK {
						var event domain.Event
						if json.Unmarshal(raw, &event) != nil || event.EventID != "strip-evt-1" || event.TenantID != "tenant-a" || hasDigestKey(event.Payload) {
							t.Fatal("coordinate event changed its selected identity or exposed a search sidecar")
						}
						reads++
						return
					}
					var denied errorEnvelope
					if json.Unmarshal(raw, &denied) != nil || denied.Error.Code == "" || bytes.Contains(raw, []byte(`"payload"`)) || bytes.Contains(raw, []byte(`"source_digest"`)) || bytes.Contains(raw, []byte(`"event_id"`)) {
						t.Fatal("denied coordinate read served partial evidence or backend content")
					}
					if status == http.StatusConflict && (denied.Error.Code != "conflict" || denied.Error.Message != "conflict: query evidence exact schema identity mismatch") {
						t.Fatal("coordinate Schema denial lost its fixed safe classification")
					}
				})
			}
		}
	}
	facts, err := svc.ListAdminActions(testCtx, "tenant-a", false, 100)
	if err != nil {
		t.Fatal("could not inspect independent HTTP coordinate read facts")
	}
	actual := 0
	for _, fact := range facts {
		if fact.Action == domain.AdminActionEventRead {
			actual++
			if fact.TargetType != "event" || fact.TargetID != "strip-evt-1" || fact.TenantID != "tenant-a" || fact.Actor == "" {
				t.Fatal("HTTP coordinate read changed original fact identity/target")
			}
		}
	}
	if reads != 6 || actual != reads {
		t.Fatal("HTTP coordinate denial appended success or a permitted read was not independently audited")
	}
}

func TestHTTPCoordinateContinuationRejectsCurrentExactSchemaDrift(t *testing.T) {
	server, svc := timelineStripFixture(t)
	defer svc.Store.Close()
	defer server.Close()
	event := domain.Event{EventID: "strip-evt-2", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 2, OccurredAt: svc.Now().Add(11 * time.Second), Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "strip-idem-2", Payload: map[string]any{"resource": "invoice", "email": "synthetic@example.test"}}
	if _, err := svc.Ingest(testCtx, "tenant-a", domain.IngestPrincipal{ClientID: "crm"}, event, domain.StatusArchived); err != nil {
		t.Fatal("could not prepare second real HTTP coordinate source")
	}
	query := domain.Query{From: svc.Now(), To: svc.Now().Add(time.Minute), PageSize: 1}
	read := func() (int, []byte, string) {
		t.Helper()
		raw, err := json.Marshal(query)
		if err != nil {
			t.Fatal("could not prepare selected membership request body")
		}
		request, err := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events:query", bytes.NewReader(raw))
		if err != nil {
			t.Fatal("could not prepare coordinate continuation request")
		}
		request.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal("coordinate continuation did not return a response")
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			t.Fatal("could not read coordinate continuation response")
		}
		return response.StatusCode, body, response.Header.Get("Cache-Control")
	}
	status, raw, cache := read()
	var first domain.QueryResult
	if status != http.StatusOK || cache != "no-store" || json.Unmarshal(raw, &first) != nil || first.Count != 2 || len(first.Items) != 1 || first.NextCursor == "" {
		t.Fatal("HTTP coordinate continuation did not start from authenticated fixed membership")
	}
	query.Cursor = first.NextCursor
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		key := store.SchemaKey("tenant-a", "audit.event", 2)
		schema := data.Schemas[key]
		schema.SchemaID = "misbound-http-schema"
		data.Schemas[key] = schema
		return nil
	}); err != nil {
		t.Fatal("could not persist current HTTP coordinate Schema identity drift")
	}
	status, raw, cache = read()
	var denied errorEnvelope
	if status != http.StatusConflict || cache != "no-store" || json.Unmarshal(raw, &denied) != nil || denied.Error.Code != "conflict" || bytes.Contains(raw, []byte(`"items"`)) || bytes.Contains(raw, []byte(`"count"`)) || bytes.Contains(raw, []byte(`"snapshot"`)) {
		t.Fatal("HTTP coordinate continuation reselected, exposed partial membership or accepted exact-Schema body drift")
	}
	facts, err := svc.ListAdminActions(testCtx, "tenant-a", false, 100)
	reads := 0
	for _, fact := range facts {
		if fact.Action == domain.AdminActionEventRead {
			reads++
		}
	}
	if err != nil || reads != 1 {
		t.Fatal("denied HTTP coordinate continuation appended success")
	}
}
