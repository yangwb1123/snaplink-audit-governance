package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestHTTPGovernanceListsIsolationAndCapabilities(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()
	at := time.Unix(1_700_000_000, 0).UTC()
	if err := st.UpdateControl(testCtx, func(data *store.Snapshot) error {
		data.Exports["own"] = domain.ExportJob{ID: "own", TenantID: "tenant-a", RequestedBy: "tenant-a", Status: "completed", CreatedAt: at, ObjectPath: "/private/archive", Digest: "sensitive-digest", EventCount: 42, Error: "/private/backend", Query: domain.Query{ActorID: "sensitive-actor"}}
		data.Exports["other"] = domain.ExportJob{ID: "other", TenantID: "tenant-b", Status: "completed", CreatedAt: at}
		data.RestoreRuns["mine"] = domain.RestoreRun{ID: "mine", TenantID: "tenant-a", CreatedBy: "tenant-a", Status: domain.RestoreStatusPendingApproval, CreatedAt: at}
		data.RestoreRuns["actionable"] = domain.RestoreRun{ID: "actionable", TenantID: "tenant-a", CreatedBy: "different-actor", Status: domain.RestoreStatusPendingApproval, CreatedAt: at}
		data.RestoreRuns["foreign"] = domain.RestoreRun{ID: "foreign", TenantID: "tenant-b", CreatedBy: "different-actor", Status: domain.RestoreStatusPendingApproval, CreatedAt: at}
		data.LegalHolds["hold"] = domain.LegalHold{ID: "hold", TenantID: "tenant-a", Name: "review", Reason: "private reason"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path   string
		token  string
		status int
		count  int
	}{
		{"/api/v1/exports?tenant_id=tenant-b", "dev:tenant-a:compliance", 200, 1},
		{"/api/v1/exports", "dev:tenant-b:compliance", 200, 1},
		{"/api/v1/exports", "dev:tenant-a:auditor", 403, 0},
		{"/api/v1/exports?view=actionable", "dev:tenant-a:compliance", 400, 0},
		{"/api/v1/restores?view=mine", "dev:tenant-a:auditor", 200, 1},
		{"/api/v1/restores?view=actionable", "dev:tenant-a:auditor", 403, 0},
		{"/api/v1/restores?view=actionable", "dev:tenant-a:compliance", 200, 1},
		{"/api/v1/restores", "dev:tenant-a:service", 403, 0},
		{"/api/v1/exports?page_size=1001", "dev:tenant-a:compliance", 400, 0},
		{"/api/v1/exports?view=mine&view=all", "dev:tenant-a:compliance", 400, 0},
		{"/api/v1/exports?from=not-a-date", "dev:tenant-a:compliance", 400, 0},
		{"/api/v1/restores?tenant_id=tenant-a&tenant_id=tenant-b", "dev:tenant-a:compliance", 400, 0},
		{"/api/v1/exports", "dev:platform:platform-admin", 400, 0},
		{"/api/v1/restores", "dev:platform:platform-admin", 400, 0},
		{"/api/v1/exports?tenant_id=tenant-b", "dev:platform:platform-admin", 200, 1},
	} {
		t.Run(tc.path+"/"+tc.token, func(t *testing.T) {
			request, _ := http.NewRequest(http.MethodGet, server.URL+tc.path, nil)
			request.Header.Set("Authorization", "Bearer "+tc.token)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != tc.status {
				t.Fatalf("status=%d body=%s", response.StatusCode, body)
			}
			if tc.status != 200 {
				return
			}
			if response.Header.Get("Cache-Control") != "no-store" {
				t.Fatal("work list must not be cached")
			}
			var page struct {
				Count int               `json:"count"`
				Items []json.RawMessage `json:"items"`
			}
			if err := json.Unmarshal(body, &page); err != nil || page.Count != tc.count || len(page.Items) != tc.count {
				t.Fatalf("page=%+v err=%v", page, err)
			}
			if strings.HasPrefix(tc.path, "/api/v1/exports") {
				for _, forbidden := range []string{"object_path", "digest", "query", "event_count", "error", "private", "sensitive"} {
					if strings.Contains(string(body), forbidden) {
						t.Fatalf("export summary leaked %s: %s", forbidden, body)
					}
				}
			}
		})
	}
}
