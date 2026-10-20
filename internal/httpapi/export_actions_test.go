package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

func exportActionRequest(t *testing.T, base, path, token, body string) (int, []byte) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, base+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil || response.Header.Get("Cache-Control") != "no-store" {
		t.Fatal("action response was cached or unreadable", err)
	}
	return response.StatusCode, raw
}

func TestHTTPExportActionsPermissionsScopeAndSafeSummary(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()
	t.Cleanup(func() {
		server.Close() // no more commands may wake the finite dispatcher
		defer func() {
			if err := st.Close(); err != nil {
				t.Error("export action fixture store did not close")
			}
		}()
		deadline := time.After(10 * time.Second)
		for {
			finished := true
			if err := st.ReadControl(testCtx, func(data *store.Snapshot) error {
				for _, job := range data.Exports {
					if job.Status == "pending" || job.Status == "running" {
						finished = false
					}
				}
				return nil
			}); err != nil {
				t.Fatal("export action fixture could not observe final control Save")
			}
			if finished {
				return // terminal observation follows actual Save/fsync under Store.mu
			}
			select {
			case <-deadline:
				t.Fatal("export action fixture still has detached writers before cleanup")
			case <-time.After(time.Millisecond):
			}
		}
	})
	at := time.Unix(1700000000, 0).UTC()
	if err := st.UpdateControl(testCtx, func(data *store.Snapshot) error {
		for id, tenant := range map[string]string{"own": "tenant-a", "foreign": "tenant-b", "platform-owned": "tenant-a", "different": "tenant-a", "legacy": "tenant-a"} {
			owner := "tenant-a"
			if id == "platform-owned" {
				owner = "platform"
			}
			if id == "different" {
				owner = "other"
			}
			if id == "legacy" {
				owner = ""
			}
			data.Exports[id] = domain.ExportJob{ID: id, TenantID: tenant, RequestedBy: owner, Status: "pending", CreatedAt: at,
				Query: domain.Query{From: at, To: at.Add(time.Minute), ActorID: "private-selector", PayloadDigest: "private-digest"}, ObjectPath: "private-object", Error: "private-error", Digest: "private-hash", EventCount: 99}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path, token, body string
		status            int
	}{
		{"/api/v1/exports/own/cancel", "", "", 401},
		{"/api/v1/exports/own/cancel", "dev:tenant-a:auditor", "", 403},
		{"/api/v1/exports/foreign/cancel", "dev:tenant-a:compliance", "", 404},
		{"/api/v1/exports/different/cancel", "dev:tenant-a:compliance", "", 403},
		{"/api/v1/exports/legacy/cancel", "dev:tenant-a:compliance", "", 403},
		{"/api/v1/exports/platform-owned/cancel", "dev:platform:platform-admin", "", 400},
		{"/api/v1/exports/own/cancel", "dev:tenant-a:compliance", `{"requested_by":"other"}`, 400},
		{"/api/v1/exports/own/cancel", "dev:tenant-a:compliance", `{} {}`, 400},
		{"/api/v1/exports/own/cancel", "dev:tenant-a:compliance", `[]`, 400},
		{"/api/v1/exports/own/retry", "dev:tenant-a:auditor", "", 403},
		{"/api/v1/exports/foreign/retry", "dev:tenant-a:compliance", "", 404},
		{"/api/v1/exports/different/retry", "dev:tenant-a:compliance", "", 403},
		{"/api/v1/exports/own/retry", "dev:tenant-a:compliance", "", 409},
		{"/api/v1/exports/platform-owned/retry", "dev:platform:platform-admin", "", 400},
		{"/api/v1/exports/own/retry", "dev:tenant-a:compliance", `{"query":{}}`, 400},
		{"/api/v1/exports/platform-owned/cancel?tenant_id=tenant-a", "dev:platform:platform-admin", "", 200},
		{"/api/v1/exports/own/cancel?tenant_id=tenant-b", "dev:tenant-a:compliance", "{}", 200},
		{"/api/v1/exports/own/cancel", "dev:tenant-a:compliance", "null", 200},
	} {
		status, raw := exportActionRequest(t, server.URL, tc.path, tc.token, tc.body)
		if status != tc.status {
			t.Fatalf("%s status=%d want=%d body=%s", tc.path, status, tc.status, raw)
		}
		if status == 200 {
			var summary domain.ExportSummary
			if err := json.Unmarshal(raw, &summary); err != nil || summary.TenantID != "tenant-a" || summary.Status != domain.ExportStatusCancelled {
				t.Fatal("unsafe cancellation result", err)
			}
			for _, forbidden := range []string{"private", "query", "object_path", "digest", "event_count", "error", "lease", "owner"} {
				if strings.Contains(string(raw), forbidden) {
					t.Fatal("summary leaked", forbidden)
				}
			}
		}
	}
}

func TestHTTPExportRetryReturnsSameChildAndRevalidatesDetails(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()
	at := time.Unix(1700000000, 0).UTC()
	if err := st.UpdateControl(testCtx, func(data *store.Snapshot) error {
		data.Exports["retry-parent"] = domain.ExportJob{ID: "retry-parent", TenantID: "tenant-a", RequestedBy: "tenant-a", Status: "failed", CreatedAt: at, FinishedAt: &at,
			Query: domain.Query{From: at, To: at.Add(time.Minute), Cursor: "expired-private-cursor"}, Error: "private-storage-error"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var childID string
	for i := 0; i < 3; i++ {
		status, raw := exportActionRequest(t, server.URL, "/api/v1/exports/retry-parent/retry", "dev:tenant-a:compliance", "")
		if status != 202 {
			t.Fatalf("retry status=%d body=%s", status, raw)
		}
		var result domain.ExportSummary
		if err := json.Unmarshal(raw, &result); err != nil || result.RetryOf != "retry-parent" || result.ID == "retry-parent" {
			t.Fatal("bad child linkage", err)
		}
		if childID == "" {
			childID = result.ID
		}
		if childID != result.ID {
			t.Fatal("repeated request duplicated child")
		}
		for _, forbidden := range []string{"private", "query", "error", "digest", "event_count", "owner"} {
			if strings.Contains(string(raw), forbidden) {
				t.Fatal("retry leaked", forbidden)
			}
		}
	}
	// Wait for the actual detached execution before temporary store cleanup.
	deadline := time.Now().Add(10 * time.Second)
	for {
		terminal := false
		if err := st.ReadControl(testCtx, func(data *store.Snapshot) error {
			terminal = data.Exports[childID].FinishedAt != nil
			if data.Exports["retry-parent"].Status != "failed" {
				t.Fatal("parent changed")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if terminal {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retry execution did not converge")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHTTPExportCancelSelfAuditFailureAndWithdrawalServeNoSuccess(t *testing.T) {
	primary := errors.New("private control write unavailable")
	armed := false
	st, err := store.OpenWithControlWriteFault(filepath.Join(t.TempDir(), "state.json"), func() error {
		if armed {
			return primary
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	svc, err := service.New(st, service.Config{AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateControl(testCtx, func(data *store.Snapshot) error {
		data.Exports["fail-cancel"] = domain.ExportJob{ID: "fail-cancel", TenantID: "tenant-a", RequestedBy: "tenant-a", Status: "pending"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	handler := NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler()
	for _, cancelled := range []bool{false, true} {
		armed = !cancelled
		ctx, cancel := context.WithCancel(testCtx)
		if cancelled {
			cancel()
		}
		request := httptest.NewRequest(http.MethodPost, "/api/v1/exports/fail-cancel/cancel", nil).WithContext(ctx)
		request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		cancel()
		want := 500
		if cancelled {
			want = statusClientClosedRequest
		}
		if response.Code != want || response.Header().Get("Cache-Control") != "no-store" || strings.Contains(response.Body.String(), "private") || strings.Contains(response.Body.String(), `"status":"cancelled"`) {
			t.Fatalf("unknown outcome served unsafe success: %d %s", response.Code, response.Body.String())
		}
	}
	armed = false
	if err := st.ReadControl(testCtx, func(data *store.Snapshot) error {
		if data.Exports["fail-cancel"].Status != "pending" {
			t.Fatal("failed cancellation changed state")
		}
		for _, fact := range data.AdminActions {
			if fact.Action == domain.AdminActionExportCancelled {
				t.Fatal("failed action wrote success fact")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
