package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func mintGovernanceStatusJWT(t *testing.T, tenant string, permissions []string, console bool) string {
	t.Helper()
	payload := map[string]any{"sub": "alice", "tenant_id": tenant, "permissions": permissions, "exp": 4_100_000_000}
	if console {
		payload["scope"] = "admin:read"
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	unsigned := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"at+jwt"}`)) + "." + base64.RawURLEncoding.EncodeToString(body)
	mac := hmac.New(sha256.New, []byte(testJWTSecret))
	_, _ = mac.Write([]byte(unsigned))
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func TestHTTPGovernanceStatusPermissionSectionsAndScope(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()
	at := time.Unix(1_700_000_000, 0).UTC()
	if err := st.UpdateControl(testCtx, func(data *store.Snapshot) error {
		data.Exports["own"] = domain.ExportJob{ID: "own", TenantID: "tenant-a", Status: "completed", Query: domain.Query{PayloadDigest: "sensitive-digest"}, ObjectPath: "/private/archive", Error: "private diagnostic", EventCount: 99}
		data.Exports["foreign"] = domain.ExportJob{ID: "foreign", TenantID: "tenant-b", Status: "pending"}
		data.LegalHolds["hold"] = domain.LegalHold{ID: "hold", TenantID: "tenant-a", Name: "private case", Reason: "private reason"}
		data.RestoreRuns["mine"] = domain.RestoreRun{ID: "mine", TenantID: "tenant-a", CreatedBy: "alice", Status: domain.RestoreStatusPendingApproval}
		data.RestoreRuns["other"] = domain.RestoreRun{ID: "other", TenantID: "tenant-a", CreatedBy: "bob", Status: domain.RestoreStatusPendingApproval}
		data.RestoreRuns["legacy"] = domain.RestoreRun{ID: "legacy", TenantID: "tenant-a", Status: domain.RestoreStatusPendingApproval}
		data.RestoreRuns["decided"] = domain.RestoreRun{ID: "decided", TenantID: "tenant-a", CreatedBy: "bob", Status: domain.RestoreStatusApproved, ApprovedAt: &at}
		data.RestoreRuns["foreign"] = domain.RestoreRun{ID: "foreign", TenantID: "tenant-b", CreatedBy: "bob", Status: domain.RestoreStatusPendingApproval}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	all := []string{"audit:export:create", "audit:operation:read", "audit:legal_hold:manage", "audit:policy:read"}
	for _, tc := range []struct {
		name        string
		permissions []string
		sections    string
		actionable  bool
	}{
		{"export only", []string{"audit:export:create"}, "exports", false},
		{"restore read", []string{"audit:operation:read"}, "restores", false},
		{"hold only", []string{"audit:legal_hold:manage"}, "legal_holds", false},
		{"policy only", []string{"audit:policy:read"}, "retention", false},
		{"restore decision", []string{"audit:operation:read", "audit:legal_hold:manage"}, "restores,legal_holds", true},
		{"all", all, "exports,restores,legal_holds,retention", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token := mintGovernanceStatusJWT(t, "tenant-a", tc.permissions, false)
			request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/governance/status?tenant_id=tenant-b", nil)
			request.Header.Set("Authorization", "Bearer "+token)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode != 200 || response.Header.Get("Cache-Control") != "no-store" {
				t.Fatalf("status=%d body=%s err=%v", response.StatusCode, body, err)
			}
			var fields map[string]json.RawMessage
			var status domain.GovernanceStatus
			if err := json.Unmarshal(body, &fields); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(body, &status); err != nil {
				t.Fatal(err)
			}
			if status.TenantID != "tenant-a" || status.Source != "control" || status.Completeness != "not_asserted" {
				t.Fatalf("invalid observation: %+v", status)
			}
			for _, section := range []string{"exports", "restores", "legal_holds", "retention"} {
				_, present := fields[section]
				if present != strings.Contains(tc.sections, section) {
					t.Fatalf("section %s present=%v permissions=%v body=%s", section, present, tc.permissions, body)
				}
			}
			if status.Exports != nil && (status.Exports.Total != 1 || status.Exports.Completed != 1 || status.Exports.Pending != 0) {
				t.Fatalf("foreign export count: %+v", status.Exports)
			}
			if status.Restores != nil {
				if status.Restores.Total != 4 || status.Restores.PendingApproval != 3 || status.Restores.Approved != 1 || status.Restores.MinePendingApproval != 1 || status.Restores.Execution != "not_tracked" || (status.Restores.Actionable != nil) != tc.actionable {
					t.Fatalf("invalid decision counts: %+v", status.Restores)
				}
				if tc.actionable && *status.Restores.Actionable != 1 {
					t.Fatalf("self/legacy requester included: %+v", status.Restores)
				}
			}
			for _, forbidden := range []string{"private", "sensitive", "query", "digest", "object_path", "event_count", "reason", "created_by", "requested_by", "alice", "bob", "foreign"} {
				if strings.Contains(string(body), forbidden) {
					t.Fatalf("status leaks %s: %s", forbidden, body)
				}
			}
		})
	}
	facts, err := st.ReadAdminTrail(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	if len(facts) != 6 {
		t.Fatalf("read facts=%d want exactly one per response", len(facts))
	}
	for _, fact := range facts {
		if fact.Action != domain.AdminActionGovernanceStatusRead || fact.TenantID != "tenant-a" || fact.Actor != "alice" {
			t.Fatalf("wrong read fact: %+v", fact)
		}
	}
	for _, tc := range []struct {
		token, selector string
		status          int
	}{
		{"", "", 401},
		{"dev:tenant-a:service", "", 403},
		{mintGovernanceStatusJWT(t, "tenant-a", []string{"audit:event:read"}, false), "", 403},
		{mintTenantlessConsoleToken(t), "?tenant_id=tenant-a", 403},
		{mintGovernanceStatusJWT(t, "", []string{"audit:policy:read"}, true), "?tenant_id=tenant-a", 401},
		{"dev:platform:platform-admin", "", 400},
		{"dev:platform:platform-admin", "?tenant_id=", 400},
		{"dev:platform:platform-admin", "?tenant_id=missing", 404},
		{"dev:platform:platform-admin", "?tenant_id=tenant-b", 200},
	} {
		request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/governance/status"+tc.selector, nil)
		if tc.token != "" {
			request.Header.Set("Authorization", "Bearer "+tc.token)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != tc.status || response.Header.Get("Cache-Control") != "no-store" {
			t.Fatalf("selector=%s status=%d want=%d body=%s err=%v", tc.selector, response.StatusCode, tc.status, body, err)
		}
		if tc.status == 200 {
			var status domain.GovernanceStatus
			if err := json.Unmarshal(body, &status); err != nil || status.TenantID != "tenant-b" || status.Exports.Total != 1 || status.Exports.Pending != 1 || status.Restores.Total != 1 {
				t.Fatalf("platform selection status=%+v err=%v", status, err)
			}
		}
	}
}

func TestHTTPGovernanceStatusSelfAuditFailClosed(t *testing.T) {
	backend := &scriptedStoreBackend{data: store.NewSnapshot()}
	server, _ := testHTTPServerWithBackend(t, backend)
	defer server.Close()
	backend.saveErr = errors.New("private diagnostic")
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/governance/status", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:tenant-admin")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != 500 || !strings.Contains(string(body), "internal server error") || strings.Contains(string(body), "private") || strings.Contains(string(body), "exports") || strings.Contains(string(body), "observed_at") || response.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("uncertified summary served: status=%d body=%s err=%v", response.StatusCode, body, err)
	}
}
