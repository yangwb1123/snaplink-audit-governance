package httpapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

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
	// The archive is configured by default so export jobs can complete
	// (unconfigured FileStore.Put fails loudly instead of writing into the
	// working directory).
	svc, err := service.New(st, service.Config{ArchiveDir: filepath.Join(t.TempDir(), "archive"), Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
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
	return httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: "test-secret", AllowLocalHS256: true}, log.New(io.Discard, "", 0)).Handler())
}

// mintRestoreJWT mints a locally-signable HS256 JWT so HTTP tests can
// express actors distinct from the dev-token principal (dev tokens fix
// Subject == tenant ID, so they can never be a second decision actor). The
// exp is a fixed far-future timestamp because the harness clock is pinned
// at time.Unix(1_700_000_000, 0).
func mintRestoreJWT(t *testing.T, subject, tenantID string, roles []string) string {
	t.Helper()
	header, err := json.Marshal(map[string]any{"alg": "HS256", "typ": "at+jwt"})
	if err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{"sub": subject, "tenant_id": tenantID, "roles": roles, "exp": 4_100_000_000}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	signed := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(body)
	mac := hmac.New(sha256.New, []byte("test-secret"))
	_, _ = mac.Write([]byte(signed))
	return signed + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// httpAdminActionCount lists the tenant-a admin action trail through the
// public API. The dev tenant-admin token can read it (audit:policy:read).
func httpAdminActionCount(t *testing.T, server *httptest.Server, token string) int {
	t.Helper()
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/admin/actions", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("list actions status=%d", response.StatusCode)
	}
	var result struct {
		Items []domain.AdminAction `json:"items"`
		Count int                  `json:"count"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result.Count
}

func TestHTTPIngestQueryAndTenantIsolation(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	event := domain.Event{EventID: "http-evt-1", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), OperationID: "http-op-1", Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "http-idem-1", Payload: map[string]any{"value": 1}}
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

	// DS-08: envelope tenant (tenant-b) inconsistent with the signed tenant
	// claim (tenant-a) is rejected 422 and nothing is ingested.
	tampered := domain.Event{EventID: "http-evt-2", TenantID: "tenant-b", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_011, 0).UTC(), OperationID: "http-op-2", Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "http-idem-2", Payload: map[string]any{"value": 2}}
	tamperedBody, _ := json.Marshal(tampered)
	req, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/events?wait_for=ledgered", bytes.NewReader(tamperedBody))
	req.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusUnprocessableEntity {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("tampered tenant status=%d body=%s, want 422", response.StatusCode, data)
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

	// 创建者审批自己的请求 → 403（dev token 的 sub 就是租户 ID，与
	// CreatedBy 相同，SoD 拒绝）。
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/restores/"+run.ID+"/approve", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("same actor approve status=%d err=%v", response.StatusCode, err)
	}
	response.Body.Close()

	// 被拒绝的尝试不写管理动作：count 保持 4（3 bootstrap + restore.created）。
	if count := httpAdminActionCount(t, server, "dev:tenant-a:tenant-admin"); count != 4 {
		t.Fatalf("admin actions after refused approve=%d, want 4", count)
	}

	// 第二主体（minted HS256 JWT，sub=approver-1）审批 → 200。
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/restores/"+run.ID+"/approve", nil)
	request.Header.Set("Authorization", "Bearer "+mintRestoreJWT(t, "approver-1", "tenant-a", []string{"compliance"}))
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("approve status=%d err=%v", response.StatusCode, err)
	}
	var approved domain.RestoreRun
	if err := json.NewDecoder(response.Body).Decode(&approved); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if approved.Status != domain.RestoreStatusApproved || approved.ApprovedBy != "approver-1" {
		t.Fatalf("unexpected approved run: %+v", approved)
	}
	if count := httpAdminActionCount(t, server, "dev:tenant-a:tenant-admin"); count != 5 {
		t.Fatalf("admin actions after approve=%d, want 5", count)
	}

	// 重复审批 → 409。
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/restores/"+run.ID+"/approve", nil)
	request.Header.Set("Authorization", "Bearer "+mintRestoreJWT(t, "approver-1", "tenant-a", []string{"compliance"}))
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusConflict {
		t.Fatalf("double approve status=%d err=%v", response.StatusCode, err)
	}
	response.Body.Close()

	// 已批准后拒绝 → 409；reject 路径对未决 run 返回 200。
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/restores/"+run.ID+"/reject", nil)
	request.Header.Set("Authorization", "Bearer "+mintRestoreJWT(t, "approver-1", "tenant-a", []string{"compliance"}))
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

	// 拒绝路径：创建者拒绝 → 403，第二主体拒绝 → 200。
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/restores", bytes.NewReader([]byte(`{"operation_id":"restore-op-1","reason":"reject path"}`)))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusAccepted {
		t.Fatalf("create second restore status=%d err=%v", response.StatusCode, err)
	}
	var run2 domain.RestoreRun
	if err := json.NewDecoder(response.Body).Decode(&run2); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/restores/"+run2.ID+"/reject", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("same actor reject status=%d err=%v", response.StatusCode, err)
	}
	response.Body.Close()
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/restores/"+run2.ID+"/reject", nil)
	request.Header.Set("Authorization", "Bearer "+mintRestoreJWT(t, "approver-1", "tenant-a", []string{"compliance"}))
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("distinct actor reject status=%d err=%v", response.StatusCode, err)
	}
	var rejected domain.RestoreRun
	if err := json.NewDecoder(response.Body).Decode(&rejected); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if rejected.Status != domain.RestoreStatusRejected || rejected.RejectedBy != "approver-1" {
		t.Fatalf("unexpected rejected run: %+v", rejected)
	}
}

func TestHTTPRestorePlatformEscapeHatch(t *testing.T) {
	// S15: with dev auth every run's creator is the tenant principal, so a
	// distinct decision actor requires a real JWT. The documented escape
	// hatch is a platform token (audit:platform:cross_tenant) naming the
	// tenant via the ?tenant_id query — and even the platform cannot
	// self-approve its own run.
	server := testHTTPServer(t)
	defer server.Close()

	event := domain.Event{EventID: "restore-evt-plat", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), OperationID: "restore-op-plat", Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "restore-idem-plat", Payload: map[string]any{"value": 1}}
	body, _ := json.Marshal(event)
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err := http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest status=%d err=%v", response.StatusCode, err)
	}
	response.Body.Close()

	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/restores", bytes.NewReader([]byte(`{"operation_id":"restore-op-plat","reason":"rollback user error"}`)))
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

	// 平台 token（无 tenant_id，platform-admin 角色）通过查询参数审批 → 200。
	platform := mintRestoreJWT(t, "platform-ops-1", "", []string{"platform-admin"})
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/restores/"+run.ID+"/approve?tenant_id=tenant-a", nil)
	request.Header.Set("Authorization", "Bearer "+platform)
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("platform approve status=%d err=%v", response.StatusCode, err)
	}
	var approved domain.RestoreRun
	if err := json.NewDecoder(response.Body).Decode(&approved); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if approved.Status != domain.RestoreStatusApproved || approved.ApprovedBy != "platform-ops-1" {
		t.Fatalf("unexpected platform approval: %+v", approved)
	}

	// 平台也不能自我审批：平台创建的 run，同一平台主体审批 → 403。
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/restores?tenant_id=tenant-a", bytes.NewReader([]byte(`{"operation_id":"restore-op-plat","reason":"platform created"}`)))
	request.Header.Set("Authorization", "Bearer "+platform)
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusAccepted {
		t.Fatalf("platform create status=%d err=%v", response.StatusCode, err)
	}
	var selfRun domain.RestoreRun
	if err := json.NewDecoder(response.Body).Decode(&selfRun); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/restores/"+selfRun.ID+"/approve?tenant_id=tenant-a", nil)
	request.Header.Set("Authorization", "Bearer "+platform)
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("platform self approve status=%d err=%v", response.StatusCode, err)
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

// TestErrorBodyRedactsServerErrors is T0: a status × error matrix pinning
// the redaction contract at the single errorBody choke point. Every domain
// mapping below 500 keeps its exact message; every >= 500 response collapses
// to the fixed strings no matter how much internal detail the error carries,
// and the status wins even when a domain error is forced to 500.
func TestErrorBodyRedactsServerErrors(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
	request = request.WithContext(context.WithValue(request.Context(), requestIDKey, "req-matrix"))
	cases := []struct {
		name     string
		status   int
		err      error
		wantCode string
		wantMsg  string
	}{
		{"invalid-400", http.StatusBadRequest, fmt.Errorf("%w: events must not be empty", domain.ErrInvalid), "invalid_request", "invalid request: events must not be empty"},
		{"unauthorized-401", http.StatusUnauthorized, fmt.Errorf("%w: bad token", domain.ErrUnauthorized), "unauthorized", "unauthorized: bad token"},
		{"forbidden-403", http.StatusForbidden, domain.ErrForbidden, "forbidden", "forbidden"},
		{"not-found-404", http.StatusNotFound, domain.ErrNotFound, "not_found", "not found"},
		{"conflict-409", http.StatusConflict, fmt.Errorf("%w: event_id content differs", domain.ErrConflict), "conflict", "conflict: event_id content differs"},
		{"quota-429", http.StatusTooManyRequests, domain.ErrQuotaExceeded, "quota_exceeded", "quota exceeded"},
		{"schema-422", http.StatusUnprocessableEntity, domain.ErrSchemaNotFound, "schema_not_found", "schema not found"},
		{"tenant-mismatch-422", http.StatusUnprocessableEntity, fmt.Errorf("%w: envelope tenant_id %q does not match", domain.ErrTenantMismatch, "tenant-b"), "tenant_mismatch", "tenant mismatch: envelope tenant_id \"tenant-b\" does not match"},
		{"snapshot-conflict-503", http.StatusServiceUnavailable, store.ErrSnapshotConflict, "snapshot_conflict", "internal server error"},
		{"internal-raw-path", http.StatusInternalServerError, fmt.Errorf("open /var/lib/audit/state.json.tmp: is a directory"), "internal_error", "internal server error"},
		{"internal-raw-crypto", http.StatusInternalServerError, fmt.Errorf("cipher: message authentication failed (key id 0x7f)"), "internal_error", "internal server error"},
		{"internal-domain-error-wins-status", http.StatusInternalServerError, fmt.Errorf("%w: leaked detail", domain.ErrInvalid), "invalid_request", "internal server error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := errorBody(tc.status, tc.err, request)
			envelope, ok := body["error"].(map[string]any)
			if !ok {
				t.Fatalf("error envelope missing: %v", body)
			}
			if envelope["code"] != tc.wantCode || envelope["message"] != tc.wantMsg {
				t.Fatalf("code=%v message=%v, want code=%s message=%s", envelope["code"], envelope["message"], tc.wantCode, tc.wantMsg)
			}
			if envelope["request_id"] != "req-matrix" {
				t.Fatalf("request_id=%v, want req-matrix", envelope["request_id"])
			}
		})
	}
}

// TestHTTPDownloadExportRedacts500 is T1 (AC-1): a completed export whose
// archive destination is broken (a regular file planted at the directory
// path) makes Archive.Get fail with ENOTDIR. The 500 body must be exactly
// the fixed strings — no path, no errno text — while the missing-object
// 404 mapping stays untouched.
func TestHTTPDownloadExportRedacts500(t *testing.T) {
	archiveDir := filepath.Join(t.TempDir(), "archive")
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{ArchiveDir: archiveDir, Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Update(func(data *store.Snapshot) error {
		data.Exports["export-t1"] = domain.ExportJob{ID: "export-t1", TenantID: "tenant-a", Status: "completed", ObjectPath: "exports/dummy.jsonl"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler())
	defer server.Close()

	download := func() (int, []byte) {
		t.Helper()
		request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/exports/export-t1/download", nil)
		request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		return response.StatusCode, data
	}

	// Missing object under a healthy archive stays 404 (D-2: the
	// os.IsNotExist mapping is untouched).
	status, _ := download()
	if status != http.StatusNotFound {
		t.Fatalf("download with missing object status=%d, want 404", status)
	}

	// Archive directory replaced by a regular file → ENOTDIR → 500.
	if err := os.RemoveAll(archiveDir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(archiveDir, []byte("occupied"), 0o640); err != nil {
		t.Fatal(err)
	}
	status, data := download()
	if status != http.StatusInternalServerError {
		t.Fatalf("download with broken archive status=%d body=%s, want 500", status, data)
	}
	var result struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(bytes.NewReader(data)).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Error.Code != "internal_error" || result.Error.Message != "internal server error" {
		t.Fatalf("unexpected 500 body: %s", data)
	}
	if strings.Contains(result.Error.Message, "/") || strings.Contains(result.Error.Message, "not a directory") {
		t.Fatalf("500 message leaks path/errno: %q", result.Error.Message)
	}
	if strings.Contains(string(data), "/") {
		t.Fatalf("500 body leaks a path: %s", data)
	}
}

// TestHTTPIngestRedactsStorePersistError is T2 (AC-2): a directory planted
// at state.json.tmp makes the atomic persist fail with EISDIR on the next
// write (no privilege required). The 500 body must not contain the store
// path or the errno text.
func TestHTTPIngestRedactsStorePersistError(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{ArchiveDir: filepath.Join(t.TempDir(), "archive"), Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler())
	defer server.Close()

	// Break the persist path after all bootstrap writes succeeded: the
	// tmp-path directory makes the next os.WriteFile fail with EISDIR.
	if err := os.Mkdir(statePath+".tmp", 0o750); err != nil {
		t.Fatal(err)
	}

	event := domain.Event{EventID: "persist-fail-event", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "persist-fail-idem", Payload: map[string]any{"value": 1}}
	body, _ := json.Marshal(event)
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events?wait_for=ledgered", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusInternalServerError {
		t.Fatalf("ingest with broken persist status=%d body=%s, want 500", response.StatusCode, data)
	}
	var result struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(bytes.NewReader(data)).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Error.Code != "internal_error" || result.Error.Message != "internal server error" {
		t.Fatalf("unexpected 500 body: %s", data)
	}
	for _, leaked := range []string{"state.json", "is a directory", "/"} {
		if strings.Contains(string(data), leaked) {
			t.Fatalf("5xx body leaks %q: %s", leaked, data)
		}
	}
}

// TestHTTPExportFailureIsMasked is T3 (AC-3): an export whose archive Put
// fails persists the raw diagnostic in the snapshot (operator-only
// state.json), but the API must never surface it — polled GET
// /exports/{jobID} reports the fixed "export failed" message.
func TestHTTPExportFailureIsMasked(t *testing.T) {
	archiveDir := filepath.Join(t.TempDir(), "archive")
	if err := os.WriteFile(archiveDir, []byte("occupied"), 0o640); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{ArchiveDir: archiveDir, Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler())
	defer server.Close()

	query := domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_020, 0).UTC()}
	body, _ := json.Marshal(query)
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/exports", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
	response, err := http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusAccepted {
		t.Fatalf("export creation failed: status=%d err=%v", response.StatusCode, err)
	}
	var job domain.ExportJob
	if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()

	deadline := time.Now().Add(2 * time.Second)
	var lastBody []byte
	for time.Now().Before(deadline) {
		request, _ = http.NewRequest(http.MethodGet, server.URL+"/api/v1/exports/"+job.ID, nil)
		request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
		response, err = http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		lastBody, _ = io.ReadAll(response.Body)
		response.Body.Close()
		if err := json.Unmarshal(lastBody, &job); err != nil {
			t.Fatal(err)
		}
		if job.Status == "failed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if job.Status != "failed" {
		t.Fatalf("export did not fail: %+v", job)
	}
	if job.Error != "export failed" {
		t.Fatalf("export error not masked: %q", job.Error)
	}
	for _, leaked := range []string{"archive", "not a directory", "/"} {
		if strings.Contains(string(lastBody), leaked) {
			t.Fatalf("export response leaks %q: %s", leaked, lastBody)
		}
	}
}

func TestHTTPReadyzArchiveProbe(t *testing.T) {
	archiveDir := filepath.Join(t.TempDir(), "archive")
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{ArchiveDir: archiveDir, Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler())
	defer server.Close()

	// 归档目录可写 → ready。
	response, err := http.Get(server.URL + "/readyz")
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("readyz status=%d err=%v", response.StatusCode, err)
	}
	response.Body.Close()

	// 归档路径被普通文件占用 → MkdirAll 失败 → not ready。
	if err := os.RemoveAll(archiveDir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(archiveDir, []byte("occupied"), 0o640); err != nil {
		t.Fatal(err)
	}
	response, err = http.Get(server.URL + "/readyz")
	if err != nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz with broken archive status=%d err=%v", response.StatusCode, err)
	}
	response.Body.Close()
}

func TestHTTPReadyzSkipsUnconfiguredArchive(t *testing.T) {
	// Ready consolidation: an unconfigured archive (nil Store, which New
	// cannot produce but a caller could inject) must skip the probe and
	// report ready instead of panicking on the nil interface.
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	svc.Config.Archive = nil
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler())
	defer server.Close()

	response, err := http.Get(server.URL + "/readyz")
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("readyz with nil archive status=%d err=%v", response.StatusCode, err)
	}
	response.Body.Close()
}

func TestHTTPTraceparentInjected(t *testing.T) {
	// 启用内存 tracer：响应头必须携带 W3C traceparent，且传入的
	// traceparent 会被延续（同 trace id）。
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(&noopProcessor{}))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}))
	defer otel.SetTracerProvider(otel.GetTracerProvider())

	server := testHTTPServer(t)
	defer server.Close()
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/healthz", nil)
	request.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	response, err := http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("healthz status=%d err=%v", response.StatusCode, err)
	}
	defer response.Body.Close()
	traceparent := response.Header.Get("traceparent")
	if !strings.HasPrefix(traceparent, "00-4bf92f3577b34da6a3ce929d0e0e4736-") {
		t.Fatalf("traceparent=%q, want same incoming trace id", traceparent)
	}
}

type noopProcessor struct{}

func (n *noopProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (n *noopProcessor) OnEnd(sdktrace.ReadOnlySpan)                     {}
func (n *noopProcessor) ForceFlush(context.Context) error                { return nil }
func (n *noopProcessor) Shutdown(context.Context) error                  { return nil }

func TestHTTPLatencyHistogramExposed(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	// 两次普通请求：metrics 请求自身的延迟在其 body 写出后才记录，
	// 因此断言值基于此前已完成的请求。
	for i := 0; i < 2; i++ {
		response, err := http.Get(server.URL + "/healthz")
		if err != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("healthz %d status=%d err=%v", i, response.StatusCode, err)
		}
		response.Body.Close()
	}
	response, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(response.Body)
	response.Body.Close()
	text := string(data)
	for _, want := range []string{
		`audit_http_request_duration_seconds_bucket{le="0.001"}`,
		`audit_http_request_duration_seconds_bucket{le="5"}`,
		`audit_http_request_duration_seconds_bucket{le="+Inf"}`,
		"audit_http_request_duration_seconds_sum",
		"audit_http_request_duration_seconds_count 2",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q in:\n%s", want, text)
		}
	}
}

// TestHTTPIngestLargeAggregateVersionConflictNotDuplicate is AC-4: an event
// with AggregateVersion 2^53 is ingested; re-ingesting the same event_id
// with 2^53+1 must return 409 ErrConflict (not silently Duplicate), and
// re-ingesting the identical content must still return Duplicate.
func TestHTTPIngestLargeAggregateVersionConflictNotDuplicate(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	at := time.Unix(1_700_000_010, 0).UTC()
	base := func(version int64) domain.Event {
		return domain.Event{EventID: "big-agg", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: at, OperationID: "big-op", Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "big-idem", AggregateVersion: version, Payload: map[string]any{"resource": "invoice"}}
	}
	body, _ := json.Marshal(base(9007199254740992))
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events?wait_for=ledgered", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusAccepted {
		data, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("first ingest status=%d body=%s", response.StatusCode, data)
	}
	response.Body.Close()

	// 2^53+1 is a distinct fact: it must conflict, not dedupe.
	body, _ = json.Marshal(base(9007199254740993))
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/events?wait_for=ledgered", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("2^53+1 re-ingest status=%d body=%s, want 409", response.StatusCode, data)
	}
	var conflict struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&conflict); err != nil {
		t.Fatal(err)
	}
	if conflict.Error.Code != "conflict" || !strings.Contains(conflict.Error.Message, "content differs") {
		t.Fatalf("unexpected conflict body: %+v", conflict)
	}
	response.Body.Close()

	// Re-ingesting the identical content must still dedupe (202, Duplicate).
	body, _ = json.Marshal(base(9007199254740992))
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/events?wait_for=ledgered", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("identical re-ingest status=%d body=%s, want 202", response.StatusCode, data)
	}
	var wrapped struct {
		Receipt domain.EventReceipt `json:"receipt"`
	}
	if err := json.NewDecoder(response.Body).Decode(&wrapped); err != nil {
		t.Fatal(err)
	}
	if !wrapped.Receipt.Duplicate {
		t.Fatalf("identical re-ingest must be Duplicate=true: %+v", wrapped.Receipt)
	}
}

func TestHTTPNegativeCases(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	fromTo := "?from=2026-08-01T00:00:00Z&to=2026-09-01T00:00:00Z"

	// 401：缺少 Authorization 头。
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/events"+fromTo, nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing token status=%d err=%v, want 401", response.StatusCode, err)
	}
	response.Body.Close()

	// 400：非法 wait_for。
	body := `{"event_id":"neg-1","source_system":"crm","event_type":"audit.event","schema_id":"audit.event","schema_version":1,"occurred_at":"2026-08-05T10:00:00Z","actor":{"id":"u1"},"action":"update","outcome":"success","data_classification":"internal","retention_class":"standard","idempotency_key":"neg-1","payload":{"value":1}}`
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/events?wait_for=bogus", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	request.Header.Set("Content-Type", "application/json")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad wait_for status=%d err=%v, want 400", response.StatusCode, err)
	}
	response.Body.Close()

	// 400：payload 超限（> 256KB 规范编码）。
	huge := `{"event_id":"neg-2","source_system":"crm","event_type":"audit.event","schema_id":"audit.event","schema_version":1,"occurred_at":"2026-08-05T10:00:00Z","actor":{"id":"u1"},"action":"update","outcome":"success","data_classification":"internal","retention_class":"standard","idempotency_key":"neg-2","payload":{"data":"` + strings.Repeat("a", 300*1024) + `"}}`
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/events", strings.NewReader(huge))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	request.Header.Set("Content-Type", "application/json")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusBadRequest {
		t.Fatalf("oversized payload status=%d err=%v, want 400", response.StatusCode, err)
	}
	response.Body.Close()

	// 400：无效 cursor。
	request, _ = http.NewRequest(http.MethodGet, server.URL+"/api/v1/events"+fromTo+"&cursor=not-a-cursor", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad cursor status=%d err=%v, want 400", response.StatusCode, err)
	}
	response.Body.Close()
}

// TestHTTPCreateTenantRejectsKeyFramingIDs is AC-1 over HTTP (REQ-4): the
// existing statusForError mapping turns ErrInvalid into 400; no handler-level
// validation is added. Rejected bodies must leave the snapshot untouched.
func TestHTTPCreateTenantRejectsKeyFramingIDs(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler())
	defer server.Close()

	postTenant := func(id string) int {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"id": id, "name": "X", "active": true})
		request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/tenants", bytes.NewReader(body))
		request.Header.Set("Authorization", "Bearer dev:platform:platform-admin")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		return response.StatusCode
	}

	// The control-separator body is exactly the cross-tenant leak vector.
	for _, id := range []string{"a\u001fb", "a/b", "a b", "\u0000-nul"} {
		if status := postTenant(id); status != http.StatusBadRequest {
			t.Fatalf("POST tenant id=%q status=%d, want 400", id, status)
		}
	}
	// A valid creation still works after the rejections, and the rejected
	// IDs were never persisted.
	if status := postTenant("tenant-http-c"); status != http.StatusCreated {
		t.Fatalf("valid POST tenant status=%d, want 201", status)
	}
	if err := st.Read(func(data *store.Snapshot) error {
		for _, id := range []string{"a\u001fb", "a/b", "a b", "\u0000-nul"} {
			if _, exists := data.Tenants[id]; exists {
				t.Fatalf("rejected tenant %q was persisted", id)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// hasDigestKey reports whether any key ending in "__search_digest" exists
// at any nesting depth of a decoded JSON value (recursive scan).
func hasDigestKey(value any) bool {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if strings.HasSuffix(key, "__search_digest") || hasDigestKey(child) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if hasDigestKey(child) {
				return true
			}
		}
	}
	return false
}

// TestHTTPResponsesStripSearchDigestsRecursively is AC-2: GET /events/{id}
// and GET /events responses contain no key matching *__search_digest
// anywhere in the payload (recursive scan), while the store keeps the
// digest — the strip happens on response copies only.
func TestHTTPResponsesStripSearchDigestsRecursively(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{ArchiveDir: filepath.Join(t.TempDir(), "archive"), Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 2, EventType: "audit.event", Active: true, AllowedFields: []string{"resource", "email", "nested"}, SearchableFields: []string{"email"}}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: "test-secret", AllowLocalHS256: true}, log.New(io.Discard, "", 0)).Handler())
	defer server.Close()

	event := domain.Event{EventID: "strip-evt-1", TenantID: "tenant-a", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 2, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), OperationID: "strip-op-1", Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "strip-idem-1", Payload: map[string]any{"resource": "invoice", "email": "alice@example.test", "nested": map[string]any{"note__search_digest": "sd2:nested", "keep": "yes"}}}
	postTestEvent(t, server.URL, "dev:tenant-a:service:crm", event)

	get := func(path string) []byte {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, server.URL+path, nil)
		req.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status=%d", path, response.StatusCode)
		}
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}

	// GET /events/{id}: single event, stripped at every depth.
	single := get("/api/v1/events/strip-evt-1")
	var fetched domain.Event
	if err := json.Unmarshal(single, &fetched); err != nil {
		t.Fatal(err)
	}
	if hasDigestKey(fetched.Payload) {
		t.Fatalf("GET /events/{id} payload still contains a digest key: %s", single)
	}
	if fetched.Payload["email"] != "alice@example.test" {
		t.Fatalf("plaintext field must survive the strip: %+v", fetched.Payload)
	}
	if fetched.Payload["nested"].(map[string]any)["keep"] != "yes" {
		t.Fatalf("non-digest nested content must survive: %+v", fetched.Payload)
	}

	// GET /events: list response, stripped at every depth.
	list := get("/api/v1/events?from=2023-11-14T22:13:00Z&to=2023-11-14T22:14:00Z")
	var result domain.QueryResult
	if err := json.Unmarshal(list, &result); err != nil {
		t.Fatal(err)
	}
	if result.Count != 1 {
		t.Fatalf("unexpected query result: %+v", result)
	}
	if hasDigestKey(result.Items[0].Payload) {
		t.Fatalf("GET /events payload still contains a digest key: %s", list)
	}

	// Positive control: the store keeps the digest — stripping must never
	// mutate the stored payload shared with the snapshot.
	stored, err := svc.GetEvent("tenant-a", "test", "strip-evt-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stored.Payload["email__search_digest"]; !ok {
		t.Fatal("store must retain the search digest; strip must be copy-only")
	}
	if _, ok := stored.Payload["nested"].(map[string]any)["note__search_digest"]; !ok {
		t.Fatal("store must retain the nested digest-like key")
	}
}

// TestHTTPReadSelfAuditVisible pins T-12 at the HTTP boundary: after a
// single-event read and a query, the caller's audit.event.read rows are
// visible through GET /api/v1/admin/actions — the self-audit is not only a
// service-layer side effect.
func TestHTTPReadSelfAuditVisible(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	ingestEvent := domain.Event{EventID: "t12-evt", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "t12-idem", Payload: map[string]any{"value": 1}}
	body, _ := json.Marshal(ingestEvent)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	if response, err := http.DefaultClient.Do(req); err != nil || response.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest: %v status=%v", err, response)
	}
	readReq, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/events/t12-evt", nil)
	readReq.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
	if response, err := http.DefaultClient.Do(readReq); err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("read: %v status=%v", err, response)
	}
	queryReq, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/events?from=2023-11-14T22:13:00Z&to=2023-11-14T22:14:00Z", nil)
	queryReq.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
	if response, err := http.DefaultClient.Do(queryReq); err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("query: %v status=%v", err, response)
	}
	actionsReq, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/admin/actions", nil)
	actionsReq.Header.Set("Authorization", "Bearer dev:tenant-a:tenant-admin")
	response, err := http.DefaultClient.Do(actionsReq)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result struct {
		Items []domain.AdminAction `json:"items"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	var reads int
	for _, action := range result.Items {
		if action.Action == domain.AdminActionEventRead && action.Actor == "tenant-a" {
			reads++
		}
	}
	if reads != 2 {
		t.Fatalf("audit.event.read rows visible at HTTP boundary=%d, want 2 (get + query)", reads)
	}
}

// TestHTTPTenantMismatchBodyCode pins T-13's response contract: the 422 body
// carries the machine-readable tenant_mismatch code.
func TestHTTPTenantMismatchBodyCode(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	event := domain.Event{EventID: "t13-evt", TenantID: "tenant-b", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_011, 0).UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "t13-idem", Payload: map[string]any{"value": 1}}
	body, _ := json.Marshal(event)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d, want 422", response.StatusCode)
	}
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Code != "tenant_mismatch" {
		t.Fatalf("error code=%q, want tenant_mismatch", envelope.Error.Code)
	}
}
