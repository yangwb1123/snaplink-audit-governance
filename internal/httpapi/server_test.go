package httpapi

import (
	"bytes"
	"encoding/json"
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

func testHTTPServer(t *testing.T) *httptest.Server {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc := service.New(st, service.Config{Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }})
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-b", Name: "Tenant B", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler())
}

func TestHTTPIngestQueryAndTenantIsolation(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	event := domain.Event{EventID: "http-evt-1", TenantID: "tenant-b", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), OperationID: "http-op-1", Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "http-idem-1", Payload: map[string]any{"value": 1}}
	body, _ := json.Marshal(event)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events?wait_for=ledgered", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusAccepted {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("ingest status=%d body=%s", response.StatusCode, data)
	}
	response.Body.Close()

	queryURL := server.URL + "/api/v1/events?from=2023-11-14T22:13:00Z&to=2023-11-14T22:14:00Z"
	req, _ = http.NewRequest(http.MethodGet, queryURL, nil)
	req.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
	response, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("query status=%d", response.StatusCode)
	}
	var result domain.QueryResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if result.Count != 1 || len(result.Items) != 1 || result.Items[0].TenantID != "tenant-a" {
		t.Fatalf("unexpected query result: %+v", result)
	}

	req, _ = http.NewRequest(http.MethodGet, server.URL+"/api/v1/events/http-evt-1?tenant_id=tenant-a", nil)
	req.Header.Set("Authorization", "Bearer dev:tenant-b:auditor")
	response, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant access status=%d", response.StatusCode)
	}
	response.Body.Close()
}

func TestHTTPIngestRejectsCrossSourceWithoutEnumeration(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	event := domain.Event{EventID: "spoof-event", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "service"}, Action: "write", Outcome: "success", Payload: map[string]any{"value": 1}, DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "spoof-idem"}
	wrong := postTestEvent(t, server.URL, "dev:tenant-a:service:other-client", event)
	if wrong.Status != http.StatusForbidden || wrong.Code != "forbidden" {
		t.Fatalf("cross-source response: %+v", wrong)
	}
	event.EventID = "unknown-event"
	event.IdempotencyKey = "unknown-idem"
	event.SourceSystem = "unknown"
	unknown := postTestEvent(t, server.URL, "dev:tenant-a:service:other-client", event)
	if unknown.Status != wrong.Status || unknown.Code != wrong.Code || unknown.Message != wrong.Message {
		t.Fatalf("source enumeration leak: wrong=%+v unknown=%+v", wrong, unknown)
	}
}

func TestHTTPUpdateSourceBinding(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	body := []byte(`{"name":"CRM","active":true,"allowed_client_ids":["snaplink-relay"]}`)
	request, _ := http.NewRequest(http.MethodPut, server.URL+"/api/v1/sources/crm", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:tenant-admin")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("update source status=%d body=%s", response.StatusCode, data)
	}
	response.Body.Close()
	event := domain.Event{EventID: "relay-event", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "service"}, Action: "write", Outcome: "success", Payload: map[string]any{"value": 1}, DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "relay-idem"}
	result := postTestEvent(t, server.URL, "dev:tenant-a:service:snaplink-relay", event)
	if result.Status != http.StatusAccepted {
		t.Fatalf("updated source binding rejected: %+v", result)
	}
}

type testEventResponse struct {
	Status  int
	Code    string
	Message string
}

func postTestEvent(t *testing.T, baseURL, token string, event domain.Event) testEventResponse {
	t.Helper()
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodPost, baseURL+"/api/v1/events", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if response.StatusCode != http.StatusAccepted {
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
	}
	return testEventResponse{Status: response.StatusCode, Code: result.Error.Code, Message: result.Error.Message}
}

func TestHTTPQueryRequiresTimeBounds(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/events", nil)
	req.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d", response.StatusCode)
	}
	response.Body.Close()
}

func TestHTTPExportDownloadIsTenantScoped(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	event := domain.Event{EventID: "download-event", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "read", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "download-idem", Payload: map[string]any{"resource": "invoice"}}
	body, _ := json.Marshal(event)
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err := http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest failed: status=%d err=%v", response.StatusCode, err)
	}
	response.Body.Close()
	query := domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_020, 0).UTC()}
	body, _ = json.Marshal(query)
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/exports", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusAccepted {
		t.Fatalf("export creation failed: status=%d err=%v", response.StatusCode, err)
	}
	var job domain.ExportJob
	if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		request, _ = http.NewRequest(http.MethodGet, server.URL+"/api/v1/exports/"+job.ID, nil)
		request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
		response, err = http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
			response.Body.Close()
			t.Fatal(err)
		}
		response.Body.Close()
		if job.Status == "completed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if job.Status != "completed" {
		t.Fatalf("export did not complete: %+v", job)
	}
	request, _ = http.NewRequest(http.MethodGet, server.URL+"/api/v1/exports/"+job.ID+"/download", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("download failed: status=%d err=%v", response.StatusCode, err)
	}
	data, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if len(data) == 0 || response.Header.Get("Content-Type") != "application/x-ndjson" {
		t.Fatalf("unexpected download: content_type=%q bytes=%d", response.Header.Get("Content-Type"), len(data))
	}
	request, _ = http.NewRequest(http.MethodGet, server.URL+"/api/v1/exports/"+job.ID+"/download", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-b:compliance")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant download status=%d", response.StatusCode)
	}
	response.Body.Close()
}

func TestHTTPRestoreApprovalWorkflow(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()

	event := domain.Event{EventID: "restore-evt-1", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), OperationID: "restore-op-1", Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "restore-idem-1", Payload: map[string]any{"value": 1}}
	body, _ := json.Marshal(event)
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err := http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest status=%d err=%v", response.StatusCode, err)
	}
	response.Body.Close()

	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/restores", bytes.NewReader([]byte(`{"operation_id":"restore-op-1","reason":"rollback user error"}`)))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusAccepted {
		t.Fatalf("create restore status=%d err=%v", response.StatusCode, err)
	}
	var run domain.RestoreRun
	if err := json.NewDecoder(response.Body).Decode(&run); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if run.Status != domain.RestoreStatusPendingApproval {
		t.Fatalf("created run status=%s", run.Status)
	}

	// 无审批权限的角色被拒绝。
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/restores/"+run.ID+"/approve", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("auditor approve status=%d err=%v", response.StatusCode, err)
	}
	response.Body.Close()

	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/restores/"+run.ID+"/approve", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("approve status=%d err=%v", response.StatusCode, err)
	}
	var approved domain.RestoreRun
	if err := json.NewDecoder(response.Body).Decode(&approved); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if approved.Status != domain.RestoreStatusApproved || approved.ApprovedBy != "tenant-a" {
		t.Fatalf("unexpected approved run: %+v", approved)
	}

	// 重复审批 → 409。
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/restores/"+run.ID+"/approve", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusConflict {
		t.Fatalf("double approve status=%d err=%v", response.StatusCode, err)
	}
	response.Body.Close()

	// 已批准后拒绝 → 409；reject 路径对未决 run 返回 200。
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/restores/"+run.ID+"/reject", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusConflict {
		t.Fatalf("reject after approve status=%d err=%v", response.StatusCode, err)
	}
	response.Body.Close()

	// 跨租户审批 → 404。
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/restores/"+run.ID+"/approve", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-b:compliance")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusNotFound {
		t.Fatalf("cross tenant approve status=%d err=%v", response.StatusCode, err)
	}
	response.Body.Close()
}

func TestHTTPAdminActionsSelfAudit(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()

	request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/admin/actions", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:tenant-admin")
	response, err := http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("list actions status=%d err=%v", response.StatusCode, err)
	}
	var result struct {
		Items []domain.AdminAction `json:"items"`
		Count int                  `json:"count"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if result.Count != 3 {
		t.Fatalf("actions count=%d, want 3 (tenant/source/schema bootstrap)", result.Count)
	}
	seen := map[string]bool{}
	for _, action := range result.Items {
		seen[action.Action] = true
	}
	for _, action := range []string{domain.AdminActionTenantCreated, domain.AdminActionSourceCreated, domain.AdminActionSchemaCreated} {
		if !seen[action] {
			t.Fatalf("missing %s in %v", action, result.Items)
		}
	}

	// 无 policy:read 权限的角色（auditor）→ 403。
	request, _ = http.NewRequest(http.MethodGet, server.URL+"/api/v1/admin/actions", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("auditor list actions status=%d err=%v", response.StatusCode, err)
	}
	response.Body.Close()

	// 跨租户 token 看不到 tenant-a 的动作。
	request, _ = http.NewRequest(http.MethodGet, server.URL+"/api/v1/admin/actions", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-b:tenant-admin")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("tenant-b list actions status=%d err=%v", response.StatusCode, err)
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if result.Count != 1 {
		t.Fatalf("tenant-b actions count=%d, want 1 (tenant.created)", result.Count)
	}
}

func TestHTTPMetricsCounters(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()

	// 写入一个事件，然后重复投递触发幂等命中。
	event := domain.Event{EventID: "metrics-evt-1", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "metrics-idem-1", Payload: map[string]any{"value": 1}}
	body, _ := json.Marshal(event)
	for i := 0; i < 2; i++ {
		request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events", bytes.NewReader(body))
		request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
		response, err := http.DefaultClient.Do(request)
		if err != nil || response.StatusCode != http.StatusAccepted {
			t.Fatalf("ingest %d status=%d err=%v", i, response.StatusCode, err)
		}
		response.Body.Close()
	}
	// 完整性校验（有效）。
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/integrity/verify", bytes.NewReader([]byte(`{}`)))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
	response, err := http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("verify status=%d err=%v", response.StatusCode, err)
	}
	response.Body.Close()

	request, _ = http.NewRequest(http.MethodGet, server.URL+"/metrics", nil)
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("metrics status=%d err=%v", response.StatusCode, err)
	}
	data, _ := io.ReadAll(response.Body)
	response.Body.Close()
	text := string(data)
	for _, want := range []string{
		"audit_ingest_requests_total 2",
		"audit_ingest_duplicates_total 1",
		"audit_ingest_conflicts_total 0",
		"audit_ingest_quota_exceeded_total 0",
		`audit_integrity_checks_total{result="valid"} 1`,
		`audit_integrity_checks_total{result="invalid"} 0`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q in:\n%s", want, text)
		}
	}
}
