package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestHTTPOperationSummaryEvidenceScopeAndNoStore(t *testing.T) {
	server, svc := timelineStripFixture(t)
	defer svc.Store.Close()
	defer server.Close()
	objects := &exportAccessHTTPArchive{Store: svc.Config.Archive, drift: "sidecar"}
	svc.Config.Archive = objects
	reads := 0
	for _, access := range []struct {
		name, token, selector, operation string
		status                           int
		evidenceRead                     bool
	}{
		{"tenant", "dev:tenant-a:auditor", "", "strip-op-1", http.StatusOK, true},
		{"signed-scope-wins", "dev:tenant-a:auditor", "?tenant_id=tenant-b", "strip-op-1", http.StatusOK, true},
		{"platform-selected", "dev:platform:platform-admin", "?tenant_id=tenant-a", "strip-op-1", http.StatusOK, true},
		{"missing-credential", "", "", "strip-op-1", http.StatusUnauthorized, false},
		{"missing-permission", "dev:tenant-a:viewer", "", "strip-op-1", http.StatusForbidden, false},
		{"other-tenant", "dev:tenant-b:auditor", "?tenant_id=tenant-a", "strip-op-1", http.StatusNotFound, false},
		{"platform-unselected", "dev:platform:platform-admin", "", "strip-op-1", http.StatusBadRequest, false},
		{"platform-empty", "dev:platform:platform-admin", "?tenant_id=", "strip-op-1", http.StatusBadRequest, false},
		{"unknown-operation", "dev:tenant-a:auditor", "", "unknown-operation", http.StatusNotFound, true},
	} {
		t.Run(access.name, func(t *testing.T) {
			objects.sourceGet.Store(0)
			request, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/operations/"+access.operation+access.selector, nil)
			if err != nil {
				t.Fatal("could not prepare the summary HTTP request")
			}
			if access.token != "" {
				request.Header.Set("Authorization", "Bearer "+access.token)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal("summary request did not return a response")
			}
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr != nil || response.StatusCode != access.status || response.Header.Get("Cache-Control") != "no-store" || access.evidenceRead != (objects.sourceGet.Load() > 0) {
				t.Fatal("summary changed no-store, scope or authorization-before-Get")
			}
			if access.status == http.StatusOK {
				var summary domain.OperationSummary
				if json.Unmarshal(body, &summary) != nil || summary.OperationID != "strip-op-1" || summary.TenantID != "tenant-a" || summary.EventCount != 1 || len(summary.Outcomes) != 1 || summary.Outcomes[0] != "success" || bytes.Contains(body, []byte(`"payload"`)) || bytes.Contains(body, []byte(`"event_id"`)) {
					t.Fatal("summary exposed source fields or changed its safe summary shape")
				}
				reads++
				return
			}
			var denied errorEnvelope
			if json.Unmarshal(body, &denied) != nil || denied.Error.Code == "" || bytes.Contains(body, []byte(`"event_count"`)) || bytes.Contains(body, []byte(`"outcomes"`)) {
				t.Fatal("denied summary served partial data or changed its error envelope")
			}
		})
	}
	// A valid chain hash that references stored SourceDigest is not enough:
	// the protected source must be authenticated before returning any summary.
	objects.drift = "source-actor"
	for _, token := range []string{"dev:tenant-a:auditor", "dev:platform:platform-admin"} {
		objects.sourceGet.Store(0)
		request, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/operations/strip-op-1?tenant_id=tenant-a", nil)
		if err != nil {
			t.Fatal("could not prepare summary authentication failure request")
		}
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal("summary authentication failure did not return a response")
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		var denied errorEnvelope
		if readErr != nil || response.StatusCode != http.StatusInternalServerError || response.Header.Get("Cache-Control") != "no-store" || json.Unmarshal(body, &denied) != nil || denied.Error.Code != "internal_error" || denied.Error.Message != "internal server error" || objects.sourceGet.Load() == 0 || bytes.Contains(body, []byte(`"event_count"`)) || bytes.Contains(body, []byte(`"outcomes"`)) {
			t.Fatal("unauthenticated summary served data or leaked internal diagnostics")
		}
	}
	facts, err := svc.ListAdminActions(testCtx, "tenant-a", false, 100)
	if err != nil {
		t.Fatal("could not inspect summary read facts")
	}
	actual := 0
	for _, fact := range facts {
		if fact.Action == domain.AdminActionEventRead {
			actual++
			if fact.TargetType != "operation" || fact.TargetID != "strip-op-1" || fact.Detail != "summary" {
				t.Fatal("summary changed the original read target encoding")
			}
		}
	}
	if actual != reads || reads != 3 {
		t.Fatal("denied/corrupt summary appended a successful read fact or success was not audited once")
	}
}
