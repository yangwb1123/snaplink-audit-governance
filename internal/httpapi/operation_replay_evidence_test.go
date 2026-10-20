package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestHTTPOperationReplayEvidenceScopeAndNoStore(t *testing.T) {
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
			request, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/operations/"+access.operation+"/replay"+access.selector, nil)
			if err != nil {
				t.Fatal("could not prepare the replay HTTP request")
			}
			if access.token != "" {
				request.Header.Set("Authorization", "Bearer "+access.token)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal("replay request did not return a response")
			}
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr != nil || response.StatusCode != access.status || response.Header.Get("Cache-Control") != "no-store" || access.evidenceRead != (objects.sourceGet.Load() > 0) {
				t.Fatal("replay changed no-store, scope or authorization-before-Get")
			}
			if access.status == http.StatusOK {
				var result domain.ReplayResult
				if json.Unmarshal(body, &result) != nil || result.OperationID != "strip-op-1" || result.TenantID != "tenant-a" || result.EventCount != 1 || result.LastSequence != 1 || result.State == nil || len(result.State) != 0 || bytes.Contains(body, []byte(`"payload"`)) || bytes.Contains(body, []byte(`"event_id"`)) || bytes.Contains(body, []byte(`"proposed_state"`)) {
					t.Fatal("replay exposed source fields, became a restore plan or changed the fold response")
				}
				reads++
				return
			}
			var denied errorEnvelope
			if json.Unmarshal(body, &denied) != nil || denied.Error.Code == "" || bytes.Contains(body, []byte(`"state"`)) || bytes.Contains(body, []byte(`"event_count"`)) {
				t.Fatal("denied replay served partial state or changed its error envelope")
			}
		})
	}
	// A saved SourceDigest and valid chain coordinates do not authenticate
	// altered source content. Failure must not serve any local fold result.
	objects.drift = "source-actor"
	for _, token := range []string{"dev:tenant-a:auditor", "dev:platform:platform-admin"} {
		objects.sourceGet.Store(0)
		request, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/operations/strip-op-1/replay?tenant_id=tenant-a", nil)
		if err != nil {
			t.Fatal("could not prepare replay authentication failure request")
		}
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal("replay authentication failure did not return a response")
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		var denied errorEnvelope
		if readErr != nil || response.StatusCode != http.StatusInternalServerError || response.Header.Get("Cache-Control") != "no-store" || json.Unmarshal(body, &denied) != nil || denied.Error.Code != "internal_error" || denied.Error.Message != "internal server error" || objects.sourceGet.Load() == 0 || bytes.Contains(body, []byte(`"state"`)) || bytes.Contains(body, []byte(`"event_count"`)) {
			t.Fatal("unauthenticated replay served state or leaked internal diagnostics")
		}
	}
	facts, err := svc.ListAdminActions(testCtx, "tenant-a", false, 100)
	if err != nil {
		t.Fatal("could not inspect replay read facts")
	}
	actual := 0
	for _, fact := range facts {
		if fact.Action == domain.AdminActionEventRead {
			actual++
			if fact.TargetType != "operation" || fact.TargetID != "strip-op-1" || fact.Detail != "replay" {
				t.Fatal("replay changed or duplicated its original read target")
			}
		}
	}
	if actual != reads || reads != 3 {
		t.Fatal("denied/corrupt replay appended a success fact or success was not audited once")
	}
}
