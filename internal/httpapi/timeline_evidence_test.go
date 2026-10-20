package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestHTTPTimelinePageEvidenceAndAuthorizationBoundaries(t *testing.T) {
	server, svc := timelineStripFixture(t)
	defer svc.Store.Close()
	defer server.Close()
	objects := &exportAccessHTTPArchive{Store: svc.Config.Archive, drift: "source-actor"}
	for _, path := range []string{"/api/v1/operations/strip-op-1/timeline", "/api/v1/aggregates/invoice/inv-1/timeline"} {
		// The healthy HTTP path still strips derived search keys without
		// changing stored evidence or exposing decrypted protected fields.
		request, err := http.NewRequest(http.MethodGet, server.URL+path, nil)
		if err != nil {
			t.Fatal("could not prepare the timeline HTTP request")
		}
		request.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal("healthy timeline HTTP request failed")
		}
		var page domain.QueryResult
		decodeErr := json.NewDecoder(response.Body).Decode(&page)
		response.Body.Close()
		if decodeErr != nil || response.StatusCode != http.StatusOK || page.Count != 1 || len(page.Items) != 1 || hasDigestKey(page.Items[0].Payload) {
			t.Fatal("authenticated timeline response changed its payload-safety contract")
		}
	}
	svc.Config.Archive = objects
	for _, path := range []string{"/api/v1/operations/strip-op-1/timeline", "/api/v1/aggregates/invoice/inv-1/timeline"} {
		for _, access := range []struct {
			token, selector string
			status          int
			evidenceRead    bool
		}{
			{"", "", http.StatusUnauthorized, false},
			{"dev:tenant-a:viewer", "", http.StatusForbidden, false},
			{"dev:tenant-b:auditor", "?tenant_id=tenant-a", http.StatusNotFound, false},
			{"dev:platform:platform-admin", "", http.StatusBadRequest, false},
			{"dev:platform:platform-admin", "?tenant_id=tenant-a", http.StatusInternalServerError, true},
			{"dev:tenant-a:auditor", "?tenant_id=tenant-b", http.StatusInternalServerError, true},
		} {
			objects.sourceGet.Store(0)
			request, err := http.NewRequest(http.MethodGet, server.URL+path+access.selector, nil)
			if err != nil {
				t.Fatal("could not prepare the timeline boundary request")
			}
			if access.token != "" {
				request.Header.Set("Authorization", "Bearer "+access.token)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal("timeline boundary request did not return a response")
			}
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			var denied errorEnvelope
			if readErr != nil || response.StatusCode != access.status || json.Unmarshal(body, &denied) != nil || bytes.Contains(body, []byte(`"items"`)) || bytes.Contains(body, []byte(`"payload"`)) || bytes.Contains(body, []byte(`"next_cursor"`)) {
				t.Fatal("timeline boundary served partial evidence or changed authorization")
			}
			if access.evidenceRead != (objects.sourceGet.Load() > 0) {
				t.Fatal("timeline request did not respect the authorization-before-Get boundary")
			}
			if access.status == http.StatusInternalServerError && (denied.Error.Code != "internal_error" || denied.Error.Message != "internal server error") {
				t.Fatal("timeline evidence authentication failure leaked diagnostics")
			}
		}
	}
	facts, err := svc.ListAdminActions(testCtx, "tenant-a", false, 100)
	if err != nil {
		t.Fatal("could not inspect timeline read facts")
	}
	reads := 0
	for _, fact := range facts {
		if fact.Action == domain.AdminActionEventRead {
			reads++
		}
	}
	if reads != 2 {
		t.Fatal("denied/corrupt HTTP timeline requests appended successful read facts")
	}
}
