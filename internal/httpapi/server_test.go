package httpapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

// testJWTSecret is the shared >=32-byte local HS256 test key for the HTTP
// harness: every Authenticator fixture and mintRestoreJWT must use the same
// constant so minted tokens verify against the configured trust source. It
// must stay >=32 bytes or every HS256 fixture fails the ValidateConfiguration
// gate.
const testJWTSecret = "test-secret-0123456789abcdefghijklmnopqrs"

func testHTTPServer(t *testing.T) *httptest.Server {
	t.Helper()
	server, _ := testHTTPServerWithStore(t)
	return server
}

// testHTTPServerWithStore is testHTTPServer plus the backing store, so
// boundary tests can walk the snapshot and prove "nothing persisted" for
// rejected requests (AC-1/AC-2 acceptance asserts the ledger side).
func testHTTPServerWithStore(t *testing.T) (*httptest.Server, *store.Store) {
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
	return httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, log.New(io.Discard, "", 0)).Handler()), st
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
	mac := hmac.New(sha256.New, []byte(testJWTSecret))
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
	// AC-2: the bytes served hash-equal the job's stored digest.
	if domain.HashBytes(data) != job.Digest {
		t.Fatal("downloaded bytes must hash-equal job.Digest")
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

// TestHTTPAdminActionsPlatformTenantFilter pins AC-1/AC-2: a platform token
// filtering with tenant_id receives only that tenant's actions (pre-fix this
// leaked all tenants), while the unfiltered platform read keeps the
// all-tenants view.
func TestHTTPAdminActionsPlatformTenantFilter(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()

	get := func(query string) (int, []domain.AdminAction) {
		t.Helper()
		url := server.URL + "/api/v1/admin/actions"
		if query != "" {
			url += "?" + query
		}
		request, _ := http.NewRequest(http.MethodGet, url, nil)
		request.Header.Set("Authorization", "Bearer dev:platform:platform-admin")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("list actions status=%d err=%v", response.StatusCode, err)
		}
		var result struct {
			Items []domain.AdminAction `json:"items"`
			Count int                  `json:"count"`
		}
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		return result.Count, result.Items
	}

	// AC-1: platform + tenant_id=tenant-a → only tenant-a's 3 bootstrap
	// actions; tenant-b's tenant.created must be excluded.
	count, items := get("tenant_id=tenant-a")
	if count != 3 {
		t.Fatalf("filtered actions count=%d, want 3 (tenant-a bootstrap); tenant-b action leaked", count)
	}
	if count != len(items) {
		t.Fatalf("count=%d != len(items)=%d", count, len(items))
	}
	for _, action := range items {
		if action.TenantID != "tenant-a" {
			t.Fatalf("filtered action tenant=%q, want tenant-a", action.TenantID)
		}
	}

	// AC-2: no query → all tenants (tenant-a 3 + tenant-b 1).
	count, items = get("")
	if count != 4 {
		t.Fatalf("unfiltered actions count=%d, want 4 (both tenants)", count)
	}
	if count != len(items) {
		t.Fatalf("count=%d != len(items)=%d", count, len(items))
	}

	// AC-2b: empty tenant_id is legal per the OpenAPI pattern and keeps the
	// all-tenants read.
	count, _ = get("tenant_id=")
	if count != 4 {
		t.Fatalf("empty-filter actions count=%d, want 4 (all tenants)", count)
	}

	// FM-8: a nonexistent tenant is a filter, not an existence check — 200
	// with an empty page, never 404.
	count, items = get("tenant_id=no-such-tenant")
	if count != 0 || len(items) != 0 {
		t.Fatalf("nonexistent-tenant filter count=%d len=%d, want 0/0", count, len(items))
	}

	// F5: limit parsing — invalid limit is a 400, 0 falls back to the
	// default cap of 100 (ListAdminActions normalizes <=0), and the cap
	// applies after the tenant filter at the HTTP boundary too.
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/admin/actions?limit=abc", nil)
	request.Header.Set("Authorization", "Bearer dev:platform:platform-admin")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("limit=abc status=%d, want 400", response.StatusCode)
	}

	count, items = get("limit=0")
	if count != 4 {
		t.Fatalf("limit=0 actions count=%d, want 4 (default cap)", count)
	}

	count, items = get("tenant_id=tenant-a&limit=1")
	if count != 1 || len(items) != 1 {
		t.Fatalf("tenant-a limit=1 = count=%d len=%d, want 1/1", count, len(items))
	}
	if items[0].TenantID != "tenant-a" {
		t.Fatalf("tenant-a limit=1 first=%+v, want tenant-a item (filter before cap)", items[0])
	}
}

// TestHTTPAdminActionsTenantTokenIgnoresOverride pins F3: a tenant-scoped
// token cannot widen its scope via ?tenant_id= — tenantFor's non-platform
// branch derives the tenant from the signed claim and ignores the query.
func TestHTTPAdminActionsTenantTokenIgnoresOverride(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()

	request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/admin/actions?tenant_id=tenant-a", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-b:tenant-admin")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("tenant-b list actions status=%d, want 200", response.StatusCode)
	}
	var result struct {
		Items []domain.AdminAction `json:"items"`
		Count int                  `json:"count"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Count != 1 {
		t.Fatalf("tenant-b + override count=%d, want 1 (tenant-b tenant.created only; override ignored)", result.Count)
	}
	for _, action := range result.Items {
		if action.TenantID != "tenant-b" {
			t.Fatalf("tenant-b + override returned tenant %q, want tenant-b", action.TenantID)
		}
	}
}

// TestHTTPCreateExportPlatformRequiresTenantID pins the ADR-0009 item-4
// fail-closed decision (S-2.1): a platform token without ?tenant_id= reaches
// CreateExport with the empty sentinel and must be rejected with 400 before
// any job, self-audit record, or worker goroutine exists. With a tenant_id
// the platform export path is unchanged (202).
func TestHTTPCreateExportPlatformRequiresTenantID(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()

	body, _ := json.Marshal(domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_020, 0).UTC()})
	post := func(rawurl string) int {
		t.Helper()
		request, _ := http.NewRequest(http.MethodPost, rawurl, bytes.NewReader(body))
		request.Header.Set("Authorization", "Bearer dev:platform:platform-admin")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		return response.StatusCode
	}

	if status := post(server.URL + "/api/v1/exports"); status != http.StatusBadRequest {
		t.Fatalf("platform export without tenant_id status=%d, want 400", status)
	}
	if err := st.Read(func(data *store.Snapshot) error {
		if len(data.Exports) != 0 {
			t.Fatalf("rejected export persisted %d job(s), want 0", len(data.Exports))
		}
		for _, action := range data.AdminActions {
			if action.TenantID == "" {
				t.Fatalf("rejected export appended empty-tenant self-audit record %s", action.Action)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Platform export with tenant_id is unchanged (202), and the job is
	// tenant-scoped. Keep the worker goroutine alive until it finishes
	// writing the archive; otherwise t.TempDir cleanup races the async job.
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/exports?tenant_id=tenant-a", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:platform:platform-admin")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusAccepted {
		response.Body.Close()
		t.Fatalf("platform export with tenant_id status=%d, want 202", response.StatusCode)
	}
	var job domain.ExportJob
	if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if job.TenantID != "tenant-a" || job.Status != "pending" {
		t.Fatalf("platform export job=%+v, want tenant-a pending", job)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		poll, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/exports/"+job.ID+"?tenant_id=tenant-a", nil)
		poll.Header.Set("Authorization", "Bearer dev:platform:platform-admin")
		response, err := http.DefaultClient.Do(poll)
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
		if time.Now().After(deadline) {
			t.Fatalf("export job did not complete: status=%s", job.Status)
		}
		time.Sleep(10 * time.Millisecond)
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

// writeArchiveObject plants bytes at an archive key directly on disk,
// removing any pre-existing object first (FileStore objects are created
// read-only, so an in-place overwrite is not possible). Tests use it to
// simulate host-level tampering of a sealed export object.
func writeArchiveObject(t *testing.T, archiveDir, key string, data []byte) {
	t.Helper()
	path := filepath.Join(archiveDir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(path)
	if err := os.WriteFile(path, data, 0o440); err != nil {
		t.Fatal(err)
	}
}

// downloadExportTestServer builds a server whose store holds one completed
// export record. The caller plants the sealed archive object at
// job.ObjectPath via writeArchiveObject using the returned service's
// encryption key, so tests control the exact bytes served (no asynchronous
// export pipeline, no timing).
func downloadExportTestServer(t *testing.T, job domain.ExportJob) (*httptest.Server, *service.Service, string) {
	t.Helper()
	if job.ID == "" {
		job.ID = "export-seeded"
	}
	if job.TenantID == "" {
		job.TenantID = "tenant-a"
	}
	if job.Status == "" {
		job.Status = "completed"
	}
	if job.ObjectPath == "" {
		job.ObjectPath = "exports/" + job.ID + ".jsonl"
	}
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
		data.Exports[job.ID] = job
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler())
	return server, svc, archiveDir
}

// downloadExport performs the download request as tenant-a compliance.
func downloadExport(t *testing.T, server *httptest.Server, jobID string) (int, string, []byte) {
	t.Helper()
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/exports/"+jobID+"/download", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	return response.StatusCode, response.Header.Get("Content-Type"), data
}

// assertRejectedDownload asserts the stable redacted 500 contract for a
// rejected download: internal_error envelope, fixed message, no ndjson
// content type and no plaintext fragment in the body.
func assertRejectedDownload(t *testing.T, status int, contentType string, body []byte) {
	t.Helper()
	if status != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s, want 500", status, body)
	}
	if strings.Contains(contentType, "x-ndjson") {
		t.Fatalf("rejected download must not stream ndjson, content-type=%q", contentType)
	}
	var result struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("500 body is not the error envelope: %v (%s)", err, body)
	}
	if result.Error.Code != "internal_error" || result.Error.Message != "internal server error" {
		t.Fatalf("unexpected 500 envelope: %s", body)
	}
	if bytes.Contains(body, []byte("legit")) || bytes.Contains(body, []byte("attacker")) || bytes.Contains(body, []byte("event line")) {
		t.Fatalf("500 body leaks download content: %s", body)
	}
}

// TestHTTPDownloadExportRejectsSwappedBlob pins the legacy (export:v1:)
// swap story (design F10): a v1 blob sealed under the platform key but
// containing content that does not match the job's stored digest, planted
// at the job's object path. Reproduce-first: before the digest-verification
// fix the download decrypts the swapped blob and serves it 200; after the
// fix the digest comparison rejects it with a redacted 500 and no ndjson
// body. The v2 (job-bound AAD) swap variant is added once EncryptBytesBound
// exists.
func TestHTTPDownloadExportRejectsSwappedBlob(t *testing.T) {
	legit := []byte("legit event line\n")
	job := domain.ExportJob{ID: "export-swap-v1", TenantID: "tenant-a", Status: "completed", ObjectPath: "exports/export-swap-v1.jsonl", Digest: domain.HashBytes(legit)}
	server, svc, archiveDir := downloadExportTestServer(t, job)
	defer server.Close()
	key := svc.Config.EncryptionKey

	sealed, err := security.EncryptBytes(legit, key)
	if err != nil {
		t.Fatal(err)
	}
	writeArchiveObject(t, archiveDir, job.ObjectPath, sealed)

	// Positive control: the job's own blob downloads 200.
	if status, _, _ := downloadExport(t, server, job.ID); status != http.StatusOK {
		t.Fatalf("legit download status=%d, want 200", status)
	}

	// v1 swap: valid GCM under the shared key, wrong content for this job.
	attacker, err := security.EncryptBytes([]byte("attacker content not matching the digest\n"), key)
	if err != nil {
		t.Fatal(err)
	}
	writeArchiveObject(t, archiveDir, job.ObjectPath, attacker)
	status, contentType, body := downloadExport(t, server, job.ID)
	assertRejectedDownload(t, status, contentType, body)
}

// TestHTTPDownloadExportRejectsSwappedV2Blob pins the headline fix (F8): a
// v2 blob sealed under another job's binding, copied onto this job's object
// path, fails GCM authentication before a single byte is served —
// cryptographic rejection independent of the digest check.
func TestHTTPDownloadExportRejectsSwappedV2Blob(t *testing.T) {
	legitA := []byte("job A legit line\n")
	jobA := domain.ExportJob{ID: "export-swap-v2a", TenantID: "tenant-a", Status: "completed", ObjectPath: "exports/export-swap-v2a.jsonl", Digest: domain.HashBytes(legitA)}
	server, svc, archiveDir := downloadExportTestServer(t, jobA)
	defer server.Close()
	key := svc.Config.EncryptionKey

	sealedA, err := security.EncryptBytesBound(legitA, key, security.ExportBinding(jobA.TenantID, jobA.ID))
	if err != nil {
		t.Fatal(err)
	}
	writeArchiveObject(t, archiveDir, jobA.ObjectPath, sealedA)
	if status, _, _ := downloadExport(t, server, jobA.ID); status != http.StatusOK {
		t.Fatalf("legit download status=%d, want 200", status)
	}

	// Job B: a second completed job in the same store, sealed under B's own
	// binding.
	jobB := domain.ExportJob{ID: "export-swap-v2b", TenantID: "tenant-a", Status: "completed", ObjectPath: "exports/export-swap-v2b.jsonl", Digest: domain.HashBytes([]byte("job B content\n"))}
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Exports[jobB.ID] = jobB
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sealedB, err := security.EncryptBytesBound([]byte("job B content\n"), key, security.ExportBinding(jobB.TenantID, jobB.ID))
	if err != nil {
		t.Fatal(err)
	}
	writeArchiveObject(t, archiveDir, jobB.ObjectPath, sealedB)

	// Copy B's sealed object onto A's path: AAD binding mismatch must reject.
	writeArchiveObject(t, archiveDir, jobA.ObjectPath, sealedB)
	status, contentType, body := downloadExport(t, server, jobA.ID)
	assertRejectedDownload(t, status, contentType, body)
}

// TestHTTPDownloadExportRejectsTamperedBlob pins AC-1 (F9): a bit-flip in
// the sealed archive object is rejected with the redacted 500 envelope, no
// ndjson content type and no plaintext fragment. Regression guard — GCM
// already rejects tampering today; the test proves the new choke point
// preserves that.
func TestHTTPDownloadExportRejectsTamperedBlob(t *testing.T) {
	legit := []byte("legit event line\n")
	job := domain.ExportJob{ID: "export-tamper", TenantID: "tenant-a", Status: "completed", ObjectPath: "exports/export-tamper.jsonl", Digest: domain.HashBytes(legit)}
	server, svc, archiveDir := downloadExportTestServer(t, job)
	defer server.Close()

	sealed, err := security.EncryptBytesBound(legit, svc.Config.EncryptionKey, security.ExportBinding(job.TenantID, job.ID))
	if err != nil {
		t.Fatal(err)
	}
	writeArchiveObject(t, archiveDir, job.ObjectPath, sealed)

	if status, _, _ := downloadExport(t, server, job.ID); status != http.StatusOK {
		t.Fatalf("legit download status=%d, want 200", status)
	}

	// Flip a byte in the ciphertext body (never the plaintext prefix — the
	// object holds sealed bytes only).
	tampered := append([]byte(nil), sealed...)
	tampered[len(tampered)/2] ^= 0x01
	writeArchiveObject(t, archiveDir, job.ObjectPath, tampered)
	status, contentType, body := downloadExport(t, server, job.ID)
	assertRejectedDownload(t, status, contentType, body)
}

// TestHTTPDownloadExportRejectsDigestMismatch pins the negative control of
// AC-2 (F12): a blob re-sealed under the job's own binding with different
// content passes GCM authentication but fails the digest comparison, so
// WriteHeader(200) is unreachable until content equality holds.
func TestHTTPDownloadExportRejectsDigestMismatch(t *testing.T) {
	legit := []byte("legit event line\n")
	job := domain.ExportJob{ID: "export-mismatch", TenantID: "tenant-a", Status: "completed", ObjectPath: "exports/export-mismatch.jsonl", Digest: domain.HashBytes(legit)}
	server, svc, archiveDir := downloadExportTestServer(t, job)
	defer server.Close()

	sealed, err := security.EncryptBytesBound(legit, svc.Config.EncryptionKey, security.ExportBinding(job.TenantID, job.ID))
	if err != nil {
		t.Fatal(err)
	}
	writeArchiveObject(t, archiveDir, job.ObjectPath, sealed)
	if status, _, _ := downloadExport(t, server, job.ID); status != http.StatusOK {
		t.Fatalf("legit download status=%d, want 200", status)
	}

	wrong, err := security.EncryptBytesBound([]byte("different event line\n"), svc.Config.EncryptionKey, security.ExportBinding(job.TenantID, job.ID))
	if err != nil {
		t.Fatal(err)
	}
	writeArchiveObject(t, archiveDir, job.ObjectPath, wrong)
	status, contentType, body := downloadExport(t, server, job.ID)
	assertRejectedDownload(t, status, contentType, body)
}

// TestHTTPDownloadExportNotCompleted409 pins F3: a job that is not
// completed (or has no object path) is rejected with 409 conflict before any
// archive access, and the body is the conflict envelope, never ndjson.
func TestHTTPDownloadExportNotCompleted409(t *testing.T) {
	job := domain.ExportJob{ID: "export-running", TenantID: "tenant-a", Status: "running"}
	server, _, _ := downloadExportTestServer(t, job)
	defer server.Close()

	status, contentType, body := downloadExport(t, server, job.ID)
	if status != http.StatusConflict {
		t.Fatalf("status=%d body=%s, want 409", status, body)
	}
	if strings.Contains(contentType, "x-ndjson") {
		t.Fatalf("409 must not stream ndjson, content-type=%q", contentType)
	}
	var result struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("409 body is not the error envelope: %v (%s)", err, body)
	}
	if result.Error.Code != "conflict" {
		t.Fatalf("409 envelope code=%q, want conflict", result.Error.Code)
	}
}

// TestHTTPDownloadVerificationFacts pins the F-2 governance decision at the
// HTTP boundary: a validated download appends exactly one audit.event.export
// fact and no rejection fact; a tampered download appends exactly one
// export.download_rejected fact and no audit.event.export fact (only
// validated downloads are governance facts).
func TestHTTPDownloadVerificationFacts(t *testing.T) {
	legit := []byte("legit event line\n")
	job := domain.ExportJob{ID: "export-facts", TenantID: "tenant-a", Status: "completed", ObjectPath: "exports/export-facts.jsonl", Digest: domain.HashBytes(legit)}
	server, svc, archiveDir := downloadExportTestServer(t, job)
	defer server.Close()

	sealed, err := security.EncryptBytesBound(legit, svc.Config.EncryptionKey, security.ExportBinding(job.TenantID, job.ID))
	if err != nil {
		t.Fatal(err)
	}
	writeArchiveObject(t, archiveDir, job.ObjectPath, sealed)

	// Validated download: audit.event.export, no rejection fact.
	if status, _, _ := downloadExport(t, server, job.ID); status != http.StatusOK {
		t.Fatalf("legit download status=%d, want 200", status)
	}
	// Tampered download: rejection fact, no audit.event.export.
	tampered := append([]byte(nil), sealed...)
	tampered[len(tampered)/2] ^= 0x01
	writeArchiveObject(t, archiveDir, job.ObjectPath, tampered)
	status, _, _ := downloadExport(t, server, job.ID)
	if status != http.StatusInternalServerError {
		t.Fatalf("tampered download status=%d, want 500", status)
	}

	exportFacts, rejectedFacts := 0, 0
	// Read-path facts are persisted in the append-only Admin Trail rather than
	// Snapshot.AdminActions. Verify the public merged view so this boundary
	// test stays correct for both the file and PostgreSQL backends.
	actions, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range actions {
		if action.Action == domain.AdminActionEventExport && action.TargetID == job.ID {
			exportFacts++
		}
		if action.Action == domain.AdminActionExportRejected && action.TargetID == job.ID {
			rejectedFacts++
		}
	}
	if exportFacts != 1 {
		t.Fatalf("validated download must append exactly one audit.event.export fact, found %d", exportFacts)
	}
	if rejectedFacts != 1 {
		t.Fatalf("tampered download must append exactly one export.download_rejected fact, found %d", rejectedFacts)
	}
}

// TestHTTPDownloadExportRejectsMalformedStoredDigest pins the fail-closed
// digest-format rule (design F11): a completed job whose stored Digest is
// empty must not serve a download even when the sealed object is valid.
// Reproduce-first: before the fix the empty digest is never consulted and
// the download serves 200; after the fix the format check rejects it.
func TestHTTPDownloadExportRejectsMalformedStoredDigest(t *testing.T) {
	legit := []byte("event line\n")
	job := domain.ExportJob{ID: "export-bad-digest", TenantID: "tenant-a", Status: "completed", ObjectPath: "exports/export-bad-digest.jsonl", Digest: ""}
	server, svc, archiveDir := downloadExportTestServer(t, job)
	defer server.Close()

	sealed, err := security.EncryptBytes(legit, svc.Config.EncryptionKey)
	if err != nil {
		t.Fatal(err)
	}
	writeArchiveObject(t, archiveDir, job.ObjectPath, sealed)

	status, contentType, body := downloadExport(t, server, job.ID)
	assertRejectedDownload(t, status, contentType, body)
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

// spanCapture is an in-memory sdktrace.SpanProcessor that records ended
// spans (the same pattern as internal/telemetry/telemetry_test.go, whose
// type is unexported, so a copy lives here).
type spanCapture struct {
	spans []sdktrace.ReadOnlySpan
}

func (c *spanCapture) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (c *spanCapture) OnEnd(span sdktrace.ReadOnlySpan) {
	c.spans = append(c.spans, span)
}
func (c *spanCapture) ForceFlush(context.Context) error { return nil }
func (c *spanCapture) Shutdown(context.Context) error   { return nil }

// captureSpans installs a global tracer provider backed by a capturing
// processor plus a W3C TraceContext propagator, and restores the previous
// globals when the test finishes.
func captureSpans(t *testing.T) *spanCapture {
	t.Helper()
	capture := &spanCapture{}
	prevProvider := otel.GetTracerProvider()
	prevPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(capture)))
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}))
	t.Cleanup(func() {
		otel.SetTracerProvider(prevProvider)
		otel.SetTextMapPropagator(prevPropagator)
	})
	return capture
}

// spanAttribute returns the value of the named attribute on a recorded span.
func spanAttribute(span sdktrace.ReadOnlySpan, key string) (attribute.Value, bool) {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

// patternToPath substitutes every {segment} of a ServeMux pattern with a
// marker value, producing a concrete request path.
func patternToPath(pattern string) string {
	var b strings.Builder
	for i := 0; i < len(pattern); {
		if pattern[i] == '{' {
			end := strings.IndexByte(pattern[i:], '}')
			b.WriteString("MKR-val")
			i += end + 1
			continue
		}
		b.WriteByte(pattern[i])
		i++
	}
	return b.String()
}

// AC-1: the server span is named from the matched pattern and http.route
// carries the pattern route; X-Trace-ID matches the span trace ID.
func TestHTTPSpanUsesMatchedPattern(t *testing.T) {
	capture := captureSpans(t)
	server := testHTTPServer(t)
	defer server.Close()

	response, err := http.Get(server.URL + "/api/v1/events/evt-123")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()

	if len(capture.spans) != 1 {
		t.Fatalf("captured %d spans, want exactly 1", len(capture.spans))
	}
	span := capture.spans[0]
	if name := span.Name(); name != "GET /api/v1/events/{eventID}" {
		t.Errorf("span name=%q, want %q", name, "GET /api/v1/events/{eventID}")
	}
	if value, ok := spanAttribute(span, "http.route"); !ok || value.AsString() != "/api/v1/events/{eventID}" {
		t.Errorf("http.route=%q (present=%v), want %q", value.AsString(), ok, "/api/v1/events/{eventID}")
	}
	if value, ok := spanAttribute(span, "http.method"); !ok || value.AsString() != http.MethodGet {
		t.Errorf("http.method=%q (present=%v), want GET", value.AsString(), ok)
	}
	if got, want := response.Header.Get("X-Trace-ID"), span.SpanContext().TraceID().String(); got != want {
		t.Errorf("X-Trace-ID=%q, want span trace ID %q", got, want)
	}
}

// AC-2: no raw path-segment or query value ever appears in a span name or
// any span attribute, across all parameterized route families.
func TestHTTPSpanNeverContainsRawIDs(t *testing.T) {
	capture := captureSpans(t)
	server := testHTTPServer(t)
	defer server.Close()

	requests := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/v1/events/MKR-evt/receipt"},
		{http.MethodGet, "/api/v1/exports/MKR-job"},
		{http.MethodGet, "/api/v1/exports/MKR-job/download"},
		{http.MethodGet, "/api/v1/operations/MKR-op"},
		{http.MethodGet, "/api/v1/aggregates/MKR-aggregate-type/MKR-aggregate-id/timeline"},
		{http.MethodPost, "/api/v1/legal-holds/MKR-hold/release"},
		{http.MethodGet, "/api/v1/restores/MKR-run"},
		{http.MethodPut, "/api/v1/sources/MKR-src"},
		{http.MethodGet, "/api/v1/events/MKR-query-evt?tenant_id=MKR-tenant"},
	}
	for _, tc := range requests {
		before := len(capture.spans)
		request, err := http.NewRequest(tc.method, server.URL+tc.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()

		if len(capture.spans) != before+1 {
			t.Fatalf("%s %s: captured %d spans total (want exactly 1 more than %d)", tc.method, tc.path, len(capture.spans), before)
		}
		span := capture.spans[before]
		if strings.Contains(span.Name(), "MKR-") {
			t.Errorf("%s %s: marker leaked into span name %q", tc.method, tc.path, span.Name())
		}
		for _, kv := range span.Attributes() {
			if strings.Contains(kv.Value.AsString(), "MKR-") {
				t.Errorf("%s %s: marker leaked into attribute %s=%q", tc.method, tc.path, kv.Key, kv.Value.AsString())
			}
		}
	}
}

// AC-3: span-name cardinality is bounded — 1000 distinct raw IDs on one
// route yield exactly one span name.
func TestHTTPSpanNameCardinalityBounded(t *testing.T) {
	capture := captureSpans(t)
	server, _ := testHTTPServerWithStore(t)
	defer server.Close()
	handler := server.Config.Handler

	for i := 0; i < 1000; i++ {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/events/evt-%d", i), nil))
	}

	if len(capture.spans) != 1000 {
		t.Fatalf("captured %d spans, want 1000", len(capture.spans))
	}
	names := make(map[string]bool)
	for _, span := range capture.spans {
		names[span.Name()] = true
	}
	if len(names) != 1 || !names["GET /api/v1/events/{eventID}"] {
		t.Errorf("span-name set over 1000 requests = %v, want exactly {GET /api/v1/events/{eventID}}", names)
	}
}

// AC-5 (REQ-4): unmatched requests create no span, emit no traceparent, and
// X-Trace-ID falls back to the request ID.
func TestHTTPUnmatchedNoSpan(t *testing.T) {
	capture := captureSpans(t)
	server := testHTTPServer(t)
	defer server.Close()

	response, err := http.Get(server.URL + "/api/v1/definitely-not-a-route")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()

	if len(capture.spans) != 0 {
		t.Fatalf("captured %d spans for unmatched request, want 0", len(capture.spans))
	}
	if got := response.Header.Get("traceparent"); got != "" {
		t.Errorf("traceparent=%q on unmatched request, want none", got)
	}
	requestID := response.Header.Get("X-Request-ID")
	if got := response.Header.Get("X-Trace-ID"); got != requestID {
		t.Errorf("X-Trace-ID=%q, want request ID %q", got, requestID)
	}
}

// AC-6 (REQ-6): a handler panic is recovered, returned as the standard 500
// error body, counted in audit_http_errors_total, and recorded on a live
// span (recovery and span.End share one deferred function).
func TestHTTPSpanWrapPanicRecovered(t *testing.T) {
	capture := captureSpans(t)
	s := &Server{Logger: log.New(io.Discard, "", 0)}
	h := s.spanWrap(func(w http.ResponseWriter, _ *http.Request) {
		panic("boom")
	})
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/api/v1/events/evt-1", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d, want 500", rec.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	errorObj, ok := body["error"].(map[string]any)
	if !ok || errorObj["code"] != "internal_error" || errorObj["message"] != "internal server error" {
		t.Errorf("error body=%v, want code=internal_error message=internal server error", body)
	}
	if got := s.errorCount.Load(); got != 1 {
		t.Errorf("errorCount=%d, want 1", got)
	}
	if len(capture.spans) != 1 {
		t.Fatalf("captured %d spans, want exactly 1", len(capture.spans))
	}
	span := capture.spans[0]
	if name := span.Name(); name != "GET /unmatched" {
		t.Errorf("span name=%q, want bounded fallback %q", name, "GET /unmatched")
	}
	found := false
	for _, event := range span.Events() {
		if event.Name != "exception" {
			continue
		}
		for _, kv := range event.Attributes {
			if string(kv.Key) == "exception.message" && strings.Contains(kv.Value.AsString(), "boom") {
				found = true
			}
		}
	}
	if !found {
		t.Error("span has no exception event carrying the panic message (RecordError did not land on a live span)")
	}
}

// AC-7 (F4): HEAD requests match "GET" patterns; the span is named from the
// pattern's own method token, never "HEAD GET …".
func TestHTTPSpanHeadUsesPatternMethod(t *testing.T) {
	capture := captureSpans(t)
	server := testHTTPServer(t)
	defer server.Close()

	request, _ := http.NewRequest(http.MethodHead, server.URL+"/api/v1/events/evt-1", nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()

	if len(capture.spans) != 1 {
		t.Fatalf("captured %d spans, want exactly 1", len(capture.spans))
	}
	span := capture.spans[0]
	if name := span.Name(); name != "HEAD /api/v1/events/{eventID}" {
		t.Errorf("span name=%q, want %q", name, "HEAD /api/v1/events/{eventID}")
	}
	if value, ok := spanAttribute(span, "http.route"); !ok || value.AsString() != "/api/v1/events/{eventID}" {
		t.Errorf("http.route=%q (present=%v), want %q", value.AsString(), ok, "/api/v1/events/{eventID}")
	}
}

// AC-8 (F10): every registered pattern produces a span named from its own
// pattern; an unwrapped future registration fails this table.
func TestHTTPSpanRouteCoverageAllPatterns(t *testing.T) {
	capture := captureSpans(t)
	server := testHTTPServer(t)
	defer server.Close()

	routes := []struct {
		method  string
		pattern string
	}{
		{http.MethodGet, "/healthz"},
		{http.MethodGet, "/readyz"},
		{http.MethodGet, "/metrics"},
		{http.MethodPost, "/api/v1/events"},
		{http.MethodPost, "/api/v1/events:batch"},
		{http.MethodGet, "/api/v1/events/{eventID}"},
		{http.MethodGet, "/api/v1/events/{eventID}/receipt"},
		{http.MethodGet, "/api/v1/events"},
		{http.MethodGet, "/api/v1/compat/snaplink/audit/events"},
		{http.MethodGet, "/api/v1/compat/snaplink/audit/events/{eventID}"},
		{http.MethodGet, "/api/v1/compat/snaplink/audit/facets"},
		{http.MethodGet, "/api/v1/operations/{operationID}"},
		{http.MethodGet, "/api/v1/operations/{operationID}/timeline"},
		{http.MethodGet, "/api/v1/operations/{operationID}/replay"},
		{http.MethodGet, "/api/v1/aggregates/{aggregateType}/{aggregateID}/timeline"},
		{http.MethodPost, "/api/v1/exports"},
		{http.MethodGet, "/api/v1/exports/{jobID}"},
		{http.MethodGet, "/api/v1/exports/{jobID}/download"},
		{http.MethodPost, "/api/v1/integrity/verify"},
		{http.MethodPost, "/api/v1/legal-holds"},
		{http.MethodGet, "/api/v1/legal-holds"},
		{http.MethodPost, "/api/v1/legal-holds/{holdID}/release"},
		{http.MethodPost, "/api/v1/restores/preview"},
		{http.MethodPost, "/api/v1/restores"},
		{http.MethodGet, "/api/v1/restores/{runID}"},
		{http.MethodPost, "/api/v1/restores/{runID}/approve"},
		{http.MethodPost, "/api/v1/restores/{runID}/reject"},
		{http.MethodPost, "/api/v1/tenants"},
		{http.MethodGet, "/api/v1/tenants"},
		{http.MethodPost, "/api/v1/sources"},
		{http.MethodGet, "/api/v1/sources"},
		{http.MethodPut, "/api/v1/sources/{sourceID}"},
		{http.MethodPost, "/api/v1/schemas"},
		{http.MethodGet, "/api/v1/schemas"},
		{http.MethodPut, "/api/v1/policies/retention"},
		{http.MethodGet, "/api/v1/policies/retention"},
		{http.MethodPost, "/api/v1/retention/evaluate"},
		{http.MethodGet, "/api/v1/admin/actions"},
	}
	for _, route := range routes {
		before := len(capture.spans)
		request, err := http.NewRequest(route.method, server.URL+patternToPath(route.pattern), nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()

		if len(capture.spans) != before+1 {
			t.Fatalf("%s %s: captured %d spans (want exactly 1 more than %d)", route.method, route.pattern, len(capture.spans), before)
		}
		span := capture.spans[before]
		want := route.method + " " + route.pattern
		if name := span.Name(); name != want {
			t.Errorf("%s %s: span name=%q, want %q", route.method, route.pattern, name, want)
		}
	}
}

// AC-9 (F1/F2): routeFromPattern strips the method token and falls back to
// a bounded route for method-less or empty patterns.
func TestRouteFromPatternFallback(t *testing.T) {
	cases := []struct{ pattern, want string }{
		{"GET /api/v1/events/{eventID}", "/api/v1/events/{eventID}"},
		{"/no-method", "/unmatched"},
		{"", "/unmatched"},
	}
	for _, tc := range cases {
		if got := routeFromPattern(tc.pattern); got != tc.want {
			t.Errorf("routeFromPattern(%q)=%q, want %q", tc.pattern, got, tc.want)
		}
	}
}

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

// TestHTTPIngestStripsClientStreamID is L6 (FR-5 at the wire surface): a
// body-supplied stream_id is accepted (202, not 400) and stripped; the
// receipt carries the server-derived stream and the crafted value never
// appears in the response.
func TestHTTPIngestStripsClientStreamID(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	event := domain.Event{EventID: "http-strip-1", StreamID: "crafted-stream", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), AggregateType: "invoice", AggregateID: "inv-1", Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "http-strip-idem-1", Payload: map[string]any{"value": 1}}
	body, _ := json.Marshal(event)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events?wait_for=ledgered", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest status=%d body=%s, want 202", response.StatusCode, data)
	}
	var wrapped struct {
		Receipt domain.EventReceipt `json:"receipt"`
	}
	if err := json.Unmarshal(data, &wrapped); err != nil {
		t.Fatal(err)
	}
	if wrapped.Receipt.StreamID != "tenant-a:aggregate:invoice:inv-1" {
		t.Fatalf("receipt stream_id=%q want tenant-a:aggregate:invoice:inv-1", wrapped.Receipt.StreamID)
	}
	if bytes.Contains(data, []byte("crafted-stream")) {
		t.Fatalf("crafted stream value leaked into response: %s", data)
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

// TestHTTPIngestRejectsTrailingJSONBody closes the M1 gap: decodeBody's
// second Decode + io.EOF exhaustion check ("request body must contain one
// JSON value") is the HTTP side of the strict single-value contract that
// grpcapi's decodeJSONNumber now enforces. A body whose first JSON value is
// followed by non-whitespace must be rejected with 400 and nothing may
// reach the ledger; trailing whitespace stays legal (design AC-1 Case D
// analogue and negative control for the harness).
func TestHTTPIngestRejectsTrailingJSONBody(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()

	bodyFor := func(id string) string {
		return `{"event_id":"` + id + `","source_system":"crm","event_type":"audit.event","schema_id":"audit.event","schema_version":1,"occurred_at":"2026-08-05T10:00:00Z","actor":{"id":"u1"},"action":"update","outcome":"success","data_classification":"internal","retention_class":"standard","idempotency_key":"` + id + `","payload":{"value":1}}`
	}
	post := func(raw string) (int, string) {
		t.Helper()
		request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events?wait_for=ledgered", strings.NewReader(raw))
		request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(data)
	}

	// Trailing content after the first JSON value: 400 with decodeBody's
	// message, event not ledgered (checked below).
	status, body := post(bodyFor("trail-reject-1") + " extra")
	if status != http.StatusBadRequest {
		t.Fatalf("trailing body status=%d body=%s, want 400", status, body)
	}
	if !strings.Contains(body, "request body must contain one JSON value") {
		t.Fatalf("trailing body error=%s, want request body must contain one JSON value", body)
	}
	// Adjacent JSON values are the same class (first value followed by
	// content), not a multi-document request.
	if status, body := post(bodyFor("trail-reject-2") + `{"second":true}`); status != http.StatusBadRequest {
		t.Fatalf("adjacent values status=%d body=%s, want 400", status, body)
	}

	// Negative control: trailing whitespace stays legal and the event is
	// ledgered with a receipt (proves harness + envelope are valid).
	if status, body := post(bodyFor("trail-ok") + "  \n\t"); status != http.StatusAccepted {
		t.Fatalf("whitespace trailer status=%d body=%s, want 202", status, body)
	}

	if err := st.Read(func(data *store.Snapshot) error {
		key := store.EventKey("tenant-a", "trail-ok")
		if receipt, exists := data.Receipts[key]; !exists || receipt.Status == "" {
			t.Error("whitespace-trailer event was not receipted")
		}
		if _, exists := data.Receipts[key]; !exists {
			t.Error("whitespace-trailer event has no receipt")
		}
		for _, id := range []string{"trail-reject-1", "trail-reject-2"} {
			key := store.EventKey("tenant-a", id)
			if _, exists := data.Events[key]; exists {
				t.Errorf("rejected event %q was persisted", id)
			}
			if _, exists := data.Receipts[key]; exists {
				t.Errorf("rejected event %q left a receipt", id)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
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

// TestDevAuditorCannotCreateTenant is AC-1.3 with the M2 (listTenants) and L1
// (4-part service form) folds: a dev token whose subject is "platform" but
// whose roles are non-admin must never create or list tenants. Pre-fix,
// dev:platform:auditor short-circuited Platform and POST /api/v1/tenants
// returned 201 with the tenant persisted; post-fix it is 403 with nothing
// persisted, while the pinned platform-admin form keeps 201 (AC-2).
func TestDevAuditorCannotCreateTenant(t *testing.T) {
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

	postTenant := func(token, id string) int {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"id": id, "name": "X", "active": true})
		request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/tenants", bytes.NewReader(body))
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		return response.StatusCode
	}
	// AC-1.3: subject "platform" + non-admin auditor role is 403, not 201.
	if status := postTenant("dev:platform:auditor", "auditor-created"); status != http.StatusForbidden {
		t.Fatalf("POST tenants with dev:platform:auditor status=%d, want 403", status)
	}
	// L1 fold: the 4-part non-admin service form must not create tenants either.
	if status := postTenant("dev:platform:service:crm", "service-created"); status != http.StatusForbidden {
		t.Fatalf("POST tenants with dev:platform:service:crm status=%d, want 403", status)
	}
	// M2 fold: GET /api/v1/tenants is 403 for the same non-admin principal.
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/tenants", nil)
	request.Header.Set("Authorization", "Bearer dev:platform:auditor")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("GET tenants with dev:platform:auditor status=%d, want 403", response.StatusCode)
	}
	// AC-2 control: the pinned platform-admin dev form still creates tenants.
	if status := postTenant("dev:platform:platform-admin", "admin-created"); status != http.StatusCreated {
		t.Fatalf("POST tenants with dev:platform:platform-admin status=%d, want 201", status)
	}
	// The 403 bodies were never persisted; the 201 control was.
	if err := st.Read(func(data *store.Snapshot) error {
		for _, id := range []string{"auditor-created", "service-created"} {
			if _, exists := data.Tenants[id]; exists {
				t.Fatalf("rejected tenant %q was persisted", id)
			}
		}
		if _, exists := data.Tenants["admin-created"]; !exists {
			t.Fatalf("control tenant admin-created was not persisted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestDevAuditorTenantOverrideIgnored is AC-1.4: for a non-Platform claim the
// ?tenant_id= escape hatch in tenantFor (server.go) is unreachable, so a dev
// token with subject "platform" and role auditor is scoped to its own tenant
// context ("platform") on every read route. The design pins the events route:
// an event living in tenant-b is 404 under dev:platform:auditor with a
// tenant_id=tenant-b override (override ignored -> scoped to "platform"), and
// the same request returns 200 for a real platform principal (escape hatch
// preserved).
func TestDevAuditorTenantOverrideIgnored(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{ArchiveDir: filepath.Join(t.TempDir(), "archive"), Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, tenantID := range []string{"platform", "tenant-a", "tenant-b"} {
		if err := svc.CreateTenant("test", domain.Tenant{ID: tenantID, Name: tenantID, Active: true}); err != nil {
			t.Fatal(err)
		}
	}
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		if err := svc.AddSource("test", domain.SourceSystem{TenantID: tenantID, ID: "crm", Name: "CRM", Active: true}); err != nil {
			t.Fatal(err)
		}
	}
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: tenantID, SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
			t.Fatal(err)
		}
	}
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler())
	defer server.Close()

	// The victim event lives in tenant-b.
	victim := domain.Event{EventID: "cross-tenant-event", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "read", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "cross-tenant-idem", Payload: map[string]any{"resource": "invoice"}}
	if got := postTestEvent(t, server.URL, "dev:tenant-b:service:crm", victim); got.Status != http.StatusAccepted {
		t.Fatalf("victim ingest status=%d, want 202", got.Status)
	}

	getEvent := func(token, query string) int {
		t.Helper()
		request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/events/cross-tenant-event"+query, nil)
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		return response.StatusCode
	}
	// 404: override ignored — the auditor claim is scoped to its own tenant
	// "platform", where the victim event does not exist.
	if status := getEvent("dev:platform:auditor", "?tenant_id=tenant-b"); status != http.StatusNotFound {
		t.Fatalf("GET event with override status=%d, want 404 (override ignored)", status)
	}
	// 404: without an override the same claim still cannot see tenant-b's event.
	if status := getEvent("dev:platform:auditor", ""); status != http.StatusNotFound {
		t.Fatalf("GET event without override status=%d, want 404", status)
	}
	// 200: the escape hatch is preserved for a real platform principal.
	if status := getEvent("dev:platform:platform-admin", "?tenant_id=tenant-b"); status != http.StatusOK {
		t.Fatalf("GET event with platform-admin override status=%d, want 200", status)
	}
}

// TestDevPlatformComplianceExportScopedToOwnTenant is review fold M1: the
// highest-impact exfil vector — dev:platform:compliance + POST /api/v1/exports
// with a ?tenant_id=<victim> override — is closed by the fix. The export job
// must be scoped to the claim's own tenant ("platform") and nothing may be
// persisted under the victim tenant; the platform-admin control still honors
// the override.
func TestDevPlatformComplianceExportScopedToOwnTenant(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{ArchiveDir: filepath.Join(t.TempDir(), "archive"), Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, tenantID := range []string{"platform", "tenant-a", "tenant-b"} {
		if err := svc.CreateTenant("test", domain.Tenant{ID: tenantID, Name: tenantID, Active: true}); err != nil {
			t.Fatal(err)
		}
	}
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		if err := svc.AddSource("test", domain.SourceSystem{TenantID: tenantID, ID: "crm", Name: "CRM", Active: true}); err != nil {
			t.Fatal(err)
		}
	}
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: tenantID, SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
			t.Fatal(err)
		}
	}
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler())
	defer server.Close()

	victim := domain.Event{EventID: "export-victim", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "read", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "export-victim-idem", Payload: map[string]any{"resource": "invoice"}}
	if got := postTestEvent(t, server.URL, "dev:tenant-b:service:crm", victim); got.Status != http.StatusAccepted {
		t.Fatalf("victim ingest status=%d, want 202", got.Status)
	}

	createExport := func(token string) domain.ExportJob {
		t.Helper()
		body, _ := json.Marshal(domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_020, 0).UTC()})
		request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/exports?tenant_id=tenant-b", bytes.NewReader(body))
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusAccepted {
			t.Fatalf("POST exports status=%d, want 202", response.StatusCode)
		}
		var job domain.ExportJob
		if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
			t.Fatal(err)
		}
		return job
	}
	// The compliance dev token is scoped to its own tenant: the override is
	// ignored and the job belongs to "platform", not the victim.
	job := createExport("dev:platform:compliance")
	if job.TenantID != "platform" {
		t.Fatalf("export job.TenantID=%q, want platform (override ignored)", job.TenantID)
	}
	// The platform-admin control still honors the escape hatch.
	adminJob := createExport("dev:platform:platform-admin")
	if adminJob.TenantID != "tenant-b" {
		t.Fatalf("platform-admin export job.TenantID=%q, want tenant-b (escape hatch preserved)", adminJob.TenantID)
	}
	// The compliance token's job is persisted under its own tenant; the
	// platform-admin control's job is persisted under the victim (escape
	// hatch). Neither may be mis-scoped.
	if err := st.Read(func(data *store.Snapshot) error {
		if got := data.Exports[job.ID].TenantID; got != "platform" {
			t.Fatalf("compliance export %q persisted under tenant %q, want platform", job.ID, got)
		}
		if got := data.Exports[adminJob.ID].TenantID; got != "tenant-b" {
			t.Fatalf("platform-admin export %q persisted under tenant %q, want tenant-b", adminJob.ID, got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Let the async runExport goroutines finish so the archive writes land
	// before TempDir cleanup (same pattern as the existing export tests);
	// poll the snapshot directly to avoid re-entering route scoping.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		if err := st.Read(func(data *store.Snapshot) error {
			for _, id := range []string{job.ID, adminJob.ID} {
				current := data.Exports[id]
				if current.Status != "completed" && current.Status != "failed" {
					done = false
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestDevPlatformTenantAdminSourceStampedToOwnTenant is review fold M2: the
// cross-tenant policy-write stamping. A dev:platform:tenant-admin token that
// sends a body tenant_id for a victim tenant must have the source stamped to
// its own tenant ("platform") — post-fix the !claims.Platform branch in
// createSource stamps claims.TenantID; nothing may be created under the
// victim tenant's key.
func TestDevPlatformTenantAdminSourceStampedToOwnTenant(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, tenantID := range []string{"platform", "tenant-a", "tenant-b"} {
		if err := svc.CreateTenant("test", domain.Tenant{ID: tenantID, Name: tenantID, Active: true}); err != nil {
			t.Fatal(err)
		}
	}
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler())
	defer server.Close()

	body, _ := json.Marshal(map[string]any{"id": "src-x", "tenant_id": "tenant-b", "name": "X", "active": true})
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/sources", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:platform:tenant-admin")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("POST sources status=%d, want 201", response.StatusCode)
	}
	var source domain.SourceSystem
	if err := json.NewDecoder(response.Body).Decode(&source); err != nil {
		t.Fatal(err)
	}
	// The body tenant override must be stamped to the claim's own tenant.
	if source.TenantID != "platform" {
		t.Fatalf("source.TenantID=%q, want platform (body tenant_id override ignored)", source.TenantID)
	}
	if err := st.Read(func(data *store.Snapshot) error {
		if _, exists := data.Sources[store.SourceKey("platform", "src-x")]; !exists {
			t.Fatalf("source src-x was not persisted under the claim's own tenant platform")
		}
		if _, exists := data.Sources[store.SourceKey("tenant-b", "src-x")]; exists {
			t.Fatalf("source src-x must not be persisted under victim tenant tenant-b")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestDevPlatformAuditorSourceTamperRejected is the security reviewer's extra
// pin: the cross-tenant tamper chain (create a source in a victim tenant by
// sending tenant_id in the body) is blocked for a non-admin dev token — 403
// before any store access, victim source list byte-identical, nothing
// persisted.
func TestDevPlatformAuditorSourceTamperRejected(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "victim-src", Name: "Victim", Active: true}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler())
	defer server.Close()

	body, _ := json.Marshal(map[string]any{"id": "tamper-src", "tenant_id": "tenant-a", "name": "Tamper", "active": true})
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/sources", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:platform:auditor")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("POST sources with dev:platform:auditor status=%d, want 403", response.StatusCode)
	}
	if err := st.Read(func(data *store.Snapshot) error {
		if len(data.Sources) != 1 {
			t.Fatalf("source count=%d, want 1 (no tamper persisted)", len(data.Sources))
		}
		victim, exists := data.Sources[store.SourceKey("tenant-a", "victim-src")]
		if !exists || victim.ID != "victim-src" || victim.Name != "Victim" {
			t.Fatalf("victim source was altered: %+v", data.Sources)
		}
		if _, exists := data.Sources[store.SourceKey("tenant-a", "tamper-src")]; exists {
			t.Fatalf("tamper source was persisted")
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
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, log.New(io.Discard, "", 0)).Handler())
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

// timelineStripFixture seeds one event carrying BOTH an OperationID and an
// AggregateType/AggregateID, with a top-level digest key (email__search_digest,
// server-derived because email is searchable) and a nested digest key
// (nested.note__search_digest, client-planted), plus non-digest content. It
// returns the HTTP server and the service (for store-level positive controls).
// Mirrors the fixture style of TestHTTPResponsesStripSearchDigestsRecursively.
func timelineStripFixture(t *testing.T) (*httptest.Server, *service.Service) {
	t.Helper()
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
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, log.New(io.Discard, "", 0)).Handler())
	event := domain.Event{EventID: "strip-evt-1", TenantID: "tenant-a", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 2, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), OperationID: "strip-op-1", AggregateType: "invoice", AggregateID: "inv-1", Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "strip-idem-1", Payload: map[string]any{"resource": "invoice", "email": "alice@example.test", "nested": map[string]any{"note__search_digest": "sd2:nested", "keep": "yes"}}}
	postTestEvent(t, server.URL, "dev:tenant-a:service:crm", event)
	return server, svc
}

// timelineStripAssertions runs the shared AC-1 assertions over one decoded
// timeline response: 200 already checked by the caller; count/len coherence,
// no *__search_digest key at any nesting depth in any item payload, plaintext
// survival, and the store-level positive control (digests retained).
func timelineStripAssertions(t *testing.T, svc *service.Service, body []byte) {
	t.Helper()
	var result struct {
		Items []domain.Event `json:"items"`
		Count int            `json:"count"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode timeline response: %v (body=%s)", err, body)
	}
	if result.Count != len(result.Items) || result.Count != 1 {
		t.Fatalf("count=%d len(items)=%d, want 1/1: %s", result.Count, len(result.Items), body)
	}
	for i := range result.Items {
		if hasDigestKey(result.Items[i].Payload) {
			t.Fatalf("timeline item %d payload still contains a digest key: %s", i, body)
		}
	}
	payload := result.Items[0].Payload
	if payload["email"] != "alice@example.test" {
		t.Fatalf("plaintext field must survive the strip: %+v", payload)
	}
	if payload["nested"].(map[string]any)["keep"] != "yes" {
		t.Fatalf("non-digest nested content must survive: %+v", payload)
	}
	// Positive control: the store keeps the digests — stripping must never
	// mutate the stored payload shared with the snapshot (REQ-3).
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

// TestHTTPOperationTimelineStripsSearchDigests is AC-1 (REQ-1): the operation
// timeline response contains no key matching *__search_digest at any nesting
// depth, count stays coherent, plaintext survives, and the store keeps the
// digests. Fails on the pre-fix tree (digests are live in the response).
func TestHTTPOperationTimelineStripsSearchDigests(t *testing.T) {
	server, svc := timelineStripFixture(t)
	defer server.Close()
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/operations/strip-op-1/timeline", nil)
	req.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("operation timeline status=%d, want 200", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	timelineStripAssertions(t, svc, body)
}

// TestHTTPAggregateTimelineStripsSearchDigests is AC-1 (REQ-2): the aggregate
// timeline response gets the identical treatment as the operation timeline.
// Fails on the pre-fix tree (digests are live in the response).
func TestHTTPAggregateTimelineStripsSearchDigests(t *testing.T) {
	server, svc := timelineStripFixture(t)
	defer server.Close()
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/aggregates/invoice/inv-1/timeline", nil)
	req.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("aggregate timeline status=%d, want 200", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	timelineStripAssertions(t, svc, body)
}

// registeredRoutes is the exhaustive route table of (s *Server).Handler() —
// every mux.HandleFunc registration in server.go. Adding a route without
// declaring it here fails the guard test, forcing a conscious review.
var registeredRoutes = []struct {
	pattern string
	handler string
}{
	{"GET /healthz", "healthz"},
	{"GET /readyz", "readyz"},
	{"GET /metrics", "metrics"},
	{"POST /api/v1/events", "postEvent"},
	{"POST /api/v1/events:batch", "postBatch"},
	{"GET /api/v1/events/{eventID}", "getEvent"},
	{"GET /api/v1/events/{eventID}/receipt", "getReceipt"},
	{"GET /api/v1/events", "queryEvents"},
	{"GET /api/v1/compat/snaplink/audit/events", "querySnaplinkAuditEvents"},
	{"GET /api/v1/compat/snaplink/audit/events/{eventID}", "getEvent"},
	{"GET /api/v1/compat/snaplink/audit/facets", "querySnaplinkAuditFacets"},
	{"GET /api/v1/operations/{operationID}", "getOperation"},
	{"GET /api/v1/operations/{operationID}/timeline", "getOperationTimeline"},
	{"GET /api/v1/operations/{operationID}/replay", "replayOperation"},
	{"GET /api/v1/aggregates/{aggregateType}/{aggregateID}/timeline", "getAggregateTimeline"},
	{"POST /api/v1/exports", "createExport"},
	{"GET /api/v1/exports/{jobID}", "getExport"},
	{"GET /api/v1/exports/{jobID}/download", "downloadExport"},
	{"POST /api/v1/integrity/verify", "verifyIntegrity"},
	{"POST /api/v1/legal-holds", "createLegalHold"},
	{"GET /api/v1/legal-holds", "listLegalHolds"},
	{"POST /api/v1/legal-holds/{holdID}/release", "releaseLegalHold"},
	{"POST /api/v1/restores/preview", "previewRestore"},
	{"POST /api/v1/restores", "createRestore"},
	{"GET /api/v1/restores/{runID}", "getRestore"},
	{"POST /api/v1/restores/{runID}/approve", "approveRestore"},
	{"POST /api/v1/restores/{runID}/reject", "rejectRestore"},
	{"POST /api/v1/tenants", "createTenant"},
	{"GET /api/v1/tenants", "listTenants"},
	{"POST /api/v1/sources", "createSource"},
	{"GET /api/v1/sources", "listSources"},
	{"PUT /api/v1/sources/{sourceID}", "updateSource"},
	{"POST /api/v1/schemas", "createSchema"},
	{"GET /api/v1/schemas", "listSchemas"},
	{"PUT /api/v1/policies/retention", "setRetention"},
	{"GET /api/v1/policies/retention", "getRetention"},
	{"POST /api/v1/retention/evaluate", "evaluateRetention"},
	{"GET /api/v1/admin/actions", "listAdminActions"},
}

// eventReturningServiceMethods is the provably complete set of service methods
// returning domain.Event payloads (verified by auditing every s.Service.<Method>
// call site in server.go; replay/restore/integrity return derived state, never
// Payload). A future event-returning service method must be added here AND to
// the guard's handler check at the same commit — documented, accepted surface.
var eventReturningServiceMethods = map[string]bool{
	"GetEvent":           true,
	"QueryEvents":        true,
	"QueryConsoleEvents": true,
	"OperationTimeline":  true,
	"AggregateTimeline":  true,
}

// TestEventReturningHandlersStripSearchDigests is AC-2 (REQ-7): a static
// repository-level guard proving every handler that serializes domain.Event
// payloads (derived from its actual s.Service.<Method> calls) invokes
// security.StripSearchDigests BEFORE writeJSON. Route inventory walks
// (s *Server).Handler() — where all mux.HandleFunc registrations live — and
// must equal the declared route table, so any new route fails until declared.
// Every registration must use the s.spanWrap(s.<handler>) form: an unwrapped
// handler would be silently untraced (spanWrap creates the per-route span).
//
// Mutation checks (verified during development): commenting out either
// timeline strip loop fails this test; adding an unstripped event-returning
// route to Handler() + table fails it too. It also failed on the pre-fix tree
// (both timeline handlers called event-returning methods without stripping).
func TestEventReturningHandlersStripSearchDigests(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatalf("read server.go (go test runs with CWD=package dir): %v", err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "server.go", src, parser.AllErrors)
	if err != nil {
		t.Fatalf("parse server.go: %v", err)
	}

	// Locate func (s *Server) Handler() — the sole route registrar.
	var handlerDecl *ast.FuncDecl
	handlers := map[string]*ast.FuncDecl{}
	for _, decl := range file.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Recv == nil {
			continue
		}
		handlers[fd.Name.Name] = fd
		if fd.Name.Name == "Handler" {
			handlerDecl = fd
		}
	}
	if handlerDecl == nil {
		t.Fatal("server.go: func (s *Server) Handler() not found")
	}

	// Route inventory: every mux.HandleFunc("METHOD /path", s.handler) call
	// inside Handler()'s body.
	type route struct{ pattern, handler string }
	var found []route
	ast.Inspect(handlerDecl.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "HandleFunc" || len(call.Args) != 2 {
			return true
		}
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			t.Fatalf("HandleFunc pattern arg is not a string literal: %s", fset.Position(call.Pos()))
		}
		pattern, err := strconv.Unquote(lit.Value)
		if err != nil || !strings.Contains(pattern, " ") {
			t.Fatalf("HandleFunc pattern %q must be \"METHOD /path\": %s", lit.Value, fset.Position(call.Pos()))
		}
		// Every registration must be wrapped per-route:
		// mux.HandleFunc(pattern, s.spanWrap(s.handler)). Unwrap spanWrap so
		// the handler identity below stays the direct s.<handler> selector; an
		// unwrapped registration is silently untraced and fails here.
		wrap, ok := call.Args[1].(*ast.CallExpr)
		if !ok || len(wrap.Args) != 1 {
			t.Fatalf("HandleFunc %q second arg must be s.spanWrap(s.<handler>): %s", pattern, fset.Position(call.Pos()))
		}
		wrapSel, ok := wrap.Fun.(*ast.SelectorExpr)
		if !ok || wrapSel.Sel.Name != "spanWrap" {
			t.Fatalf("HandleFunc %q second arg must be s.spanWrap(s.<handler>): %s", pattern, fset.Position(call.Pos()))
		}
		handlerArg, ok := wrap.Args[0].(*ast.SelectorExpr)
		if !ok {
			t.Fatalf("HandleFunc %q second arg must be s.spanWrap(s.<handler>): %s", pattern, fset.Position(call.Pos()))
		}
		found = append(found, route{pattern: pattern, handler: handlerArg.Sel.Name})
		return true
	})

	// Exhaustive route-table assertion: any new route fails until declared.
	table := map[string]string{}
	for _, r := range registeredRoutes {
		table[r.pattern] = r.handler
	}
	collected := map[string]string{}
	for _, r := range found {
		collected[r.pattern] = r.handler
	}
	if len(collected) != len(table) {
		t.Fatalf("route count mismatch: collected=%d declared=%d\ncollected=%v\ndeclared=%v", len(collected), len(table), collected, table)
	}
	for pattern, handler := range table {
		if got, ok := collected[pattern]; !ok || got != handler {
			t.Fatalf("route %q: collected handler=%q, declared=%q (update registeredRoutes when adding routes)", pattern, collected[pattern], handler)
		}
	}

	// Every event route must map to its reviewed event-returning handler
	// (explicit mapping assertion, REQ-7 step 4).
	eventRoutes := map[string]string{
		"GET /api/v1/events/{eventID}":                                  "getEvent",
		"GET /api/v1/events":                                            "queryEvents",
		"GET /api/v1/compat/snaplink/audit/events":                      "querySnaplinkAuditEvents",
		"GET /api/v1/compat/snaplink/audit/events/{eventID}":            "getEvent",
		"GET /api/v1/operations/{operationID}/timeline":                 "getOperationTimeline",
		"GET /api/v1/aggregates/{aggregateType}/{aggregateID}/timeline": "getAggregateTimeline",
	}
	for pattern, want := range eventRoutes {
		if table[pattern] != want {
			t.Fatalf("event route %q maps to %q, want %q", pattern, table[pattern], want)
		}
	}

	// For every handler, derive event-returning service calls from its actual
	// AST body; require StripSearchDigests before writeJSON in each.
	for _, r := range found {
		decl, ok := handlers[r.handler]
		if !ok {
			t.Fatalf("route %q: handler s.%s has no method declaration in server.go", r.pattern, r.handler)
		}
		var serviceMethods []string
		var stripAt, writeJSONAt = -1, -1
		ast.Inspect(decl.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fun := call.Fun.(type) {
			case *ast.SelectorExpr:
				// s.Service.<Method>(...) — direct service calls only.
				if inner, ok := fun.X.(*ast.SelectorExpr); ok {
					if ident, ok := inner.X.(*ast.Ident); ok && ident.Name == "s" && inner.Sel.Name == "Service" {
						serviceMethods = append(serviceMethods, fun.Sel.Name)
					}
				}
				// security.StripSearchDigests or any selector form.
				if fun.Sel.Name == "StripSearchDigests" && stripAt == -1 {
					stripAt = fset.Position(fun.Pos()).Offset
				}
			case *ast.Ident:
				if fun.Name == "StripSearchDigests" && stripAt == -1 {
					stripAt = fset.Position(fun.Pos()).Offset
				}
				if fun.Name == "writeJSON" && writeJSONAt == -1 {
					writeJSONAt = fset.Position(fun.Pos()).Offset
				}
			}
			return true
		})
		for _, method := range serviceMethods {
			if !eventReturningServiceMethods[method] {
				continue
			}
			if stripAt == -1 {
				t.Errorf("handler s.%s (route %s) calls event-returning s.Service.%s but never calls StripSearchDigests — digest keys leak into the response", r.handler, r.pattern, method)
				continue
			}
			if writeJSONAt == -1 || stripAt >= writeJSONAt {
				t.Errorf("handler s.%s (route %s) calls StripSearchDigests at offset %d but writes the response (writeJSON at %d) before/without it — strip must dominate serialization", r.handler, r.pattern, stripAt, writeJSONAt)
			}
		}
	}
}

// --- Key-framing charset invariant: HTTP boundary tests (AC-1..AC-4, G4) ---

// assertNoFramedCompositeKeys walks the snapshot asserting no composite-key
// map holds a multi-separator key (the fail-closed SplitTenantKey case) and
// that none of the rejected identifiers was persisted.
func assertNoFramedCompositeKeys(t *testing.T, st *store.Store, rejectedIDs ...string) {
	t.Helper()
	if err := st.Read(func(data *store.Snapshot) error {
		composite := map[string]func() map[string]string{
			"Events":      func() map[string]string { return stringKeys(data.Events) },
			"Receipts":    func() map[string]string { return stringKeys(data.Receipts) },
			"Streams":     func() map[string]string { return stringKeys(data.Streams) },
			"Segments":    func() map[string]string { return stringKeys(data.Segments) },
			"Checkpoints": func() map[string]string { return stringKeys(data.Checkpoints) },
			"Sources":     func() map[string]string { return stringKeys(data.Sources) },
			// Schemas are intentionally excluded: SchemaKey has three
			// components (tenant + schema_id + version), two separators are
			// its well-formed shape, and it is never SplitTenantKey-parsed.
		}
		for name, keys := range composite {
			for key := range keys() {
				if strings.Count(key, "\x1f") > 1 {
					t.Errorf("%s holds multi-separator key %q", name, key)
				}
			}
		}
		for _, id := range rejectedIDs {
			if _, exists := data.Events[store.EventKey("tenant-a", id)]; exists {
				t.Errorf("rejected event %q was persisted", id)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func stringKeys[V any](m map[string]V) map[string]string {
	keys := make(map[string]string, len(m))
	for key := range m {
		keys[key] = key
	}
	return keys
}

// TestHTTPIngestRejectsKeyFramingEventIDs is AC-1: POST /events rejects
// control characters (incl. 0x1F), whitespace and path separators in
// event_id and source_system with 400, persists nothing, and never appends
// a self-audit record (F-6). Positive control: valid events still ingest.
func TestHTTPIngestRejectsKeyFramingEventIDs(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()

	postEvent := func(event domain.Event) (int, map[string]any) {
		t.Helper()
		body, _ := json.Marshal(event)
		request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events", bytes.NewReader(body))
		request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var envelope map[string]any
		if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, envelope
	}

	base := domain.Event{EventID: "http-kf-valid-1", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "http-kf-idem-1", Payload: map[string]any{"value": 1}}

	before := httpAdminActionCount(t, server, "dev:tenant-a:tenant-admin")
	var rejectedIDs []string
	for i, tc := range []struct {
		name   string
		mutate func(*domain.Event)
	}{
		{"event_id 0x1F", func(e *domain.Event) { e.EventID = "a\x1fb" }},
		{"event_id NUL", func(e *domain.Event) { e.EventID = "\x00-nul" }},
		{"event_id TAB", func(e *domain.Event) { e.EventID = "a\tb" }},
		{"event_id space", func(e *domain.Event) { e.EventID = "a b" }},
		{"event_id slash", func(e *domain.Event) { e.EventID = "a/b" }},
		{"event_id backslash", func(e *domain.Event) { e.EventID = `a\b` }},
		{"source_system 0x1F", func(e *domain.Event) { e.SourceSystem = "x\x1fy" }},
		{"source_system space", func(e *domain.Event) { e.SourceSystem = "x y" }},
		{"source_system slash", func(e *domain.Event) { e.SourceSystem = "x/y" }},
	} {
		event := base
		event.EventID = fmt.Sprintf("http-kf-rej-%02d", i)
		rejectedIDs = append(rejectedIDs, event.EventID)
		tc.mutate(&event)
		status, envelope := postEvent(event)
		if status != http.StatusBadRequest {
			t.Errorf("%s: status=%d, want 400", tc.name, status)
		}
		if code, _ := envelope["error"].(map[string]any)["code"].(string); code != "invalid_request" {
			t.Errorf("%s: error code=%v, want invalid_request", tc.name, code)
		}
	}
	if after := httpAdminActionCount(t, server, "dev:tenant-a:tenant-admin"); after != before {
		t.Fatalf("rejected ingests appended admin actions: %d -> %d (F-6)", before, after)
	}
	// Positive control: valid event still ingests 202.
	status, _ := postEvent(base)
	if status != http.StatusAccepted {
		t.Fatalf("valid ingest status=%d, want 202", status)
	}
	// AC-1 ledger assertion: no rejected event id was persisted and no
	// composite-key map holds a multi-separator key (the fail-closed
	// SplitTenantKey case) after any of the rejected or accepted posts.
	assertNoFramedCompositeKeys(t, st, rejectedIDs...)
	if err := st.Read(func(data *store.Snapshot) error {
		if receipt, exists := data.Receipts[store.EventKey("tenant-a", base.EventID)]; !exists || receipt.Status == "" {
			t.Fatal("positive-control event was not receipted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestHTTPIngestBatchAbortsOnKeyFramingEvent is AC-1 batch: an invalid event
// aborts the batch inside the existing partial-status semantics — earlier
// valid events get receipts, the invalid tail is rejected, nothing invalid
// is persisted.
func TestHTTPIngestBatchAbortsOnKeyFramingEvent(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()
	valid := domain.Event{EventID: "http-kfb-ok-1", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "http-kfb-idem-1", Payload: map[string]any{"value": 1}}
	invalid := valid
	invalid.EventID = "http-kfb-bad-1"
	invalid.SourceSystem = "x\x1fy"
	invalid.IdempotencyKey = "http-kfb-idem-2"

	postBatch := func(events ...domain.Event) (int, map[string]any) {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"events": events})
		request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events:batch", bytes.NewReader(body))
		request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var envelope map[string]any
		if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, envelope
	}

	// [valid, invalid]: the valid prefix is receipted and the failing event
	// leaves a zero-value receipt (not_attempted evidence, M0-5 shape); the
	// batch aborts 400.
	status, envelope := postBatch(valid, invalid)
	if status != http.StatusBadRequest {
		t.Fatalf("[valid, invalid] status=%d, want 400", status)
	}
	receipts, _ := envelope["receipts"].([]any)
	if len(receipts) != 2 {
		t.Fatalf("[valid, invalid] receipts=%d, want 2 (accepted + zero-value failed)", len(receipts))
	}
	first, _ := receipts[0].(map[string]any)
	if first["event_id"] != "http-kfb-ok-1" {
		t.Fatalf("[valid, invalid] first receipt = %v, want the accepted event", first)
	}
	second, _ := receipts[1].(map[string]any)
	if eventID, _ := second["event_id"].(string); eventID != "" {
		t.Fatalf("[valid, invalid] second receipt = %v, want zero-value (not_attempted evidence)", second)
	}
	// [invalid, valid]: zero-value receipt for the failed head, nothing
	// ingested at all.
	status, envelope = postBatch(invalid, valid)
	if status != http.StatusBadRequest {
		t.Fatalf("[invalid, valid] status=%d, want 400", status)
	}
	if receipts, _ := envelope["receipts"].([]any); len(receipts) != 1 {
		t.Fatalf("[invalid, valid] receipts=%d, want 1 (zero-value failed head)", len(receipts))
	}
	// AC-1 batch ledger assertion: the invalid event was never persisted,
	// the valid prefix of the first batch WAS (partial acceptance), and no
	// composite-key map holds a multi-separator key.
	assertNoFramedCompositeKeys(t, st, "http-kfb-bad-1")
	if err := st.Read(func(data *store.Snapshot) error {
		if receipt, exists := data.Receipts[store.EventKey("tenant-a", "http-kfb-ok-1")]; !exists || receipt.Status == "" {
			t.Fatal("valid batch prefix was not receipted before the abort")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestHTTPSourceSchemaRejectKeyFramingIDs is AC-2: createSource, updateSource
// (path id, %1F-decoded before the handler) and createSchema reject
// key-framing IDs with 400 and persist nothing; valid IDs keep working.
func TestHTTPSourceSchemaRejectKeyFramingIDs(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()

	postSource := func(id string) int {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"id": id, "name": "X", "active": true})
		request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/sources", bytes.NewReader(body))
		request.Header.Set("Authorization", "Bearer dev:tenant-a:tenant-admin")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		return response.StatusCode
	}
	postSchema := func(schemaID string) int {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"schema_id": schemaID, "version": 1, "event_type": "x", "active": true})
		request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/schemas", bytes.NewReader(body))
		request.Header.Set("Authorization", "Bearer dev:tenant-a:tenant-admin")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		return response.StatusCode
	}

	for _, id := range []string{"a\x1fb", "\x00-nul", "a/b", "a b"} {
		if status := postSource(id); status != http.StatusBadRequest {
			t.Errorf("POST sources id=%q status=%d, want 400", id, status)
		}
	}
	if status := postSource("src-ok"); status != http.StatusCreated {
		t.Fatalf("valid POST sources status=%d, want 201", status)
	}
	for _, id := range []string{"a\x1fb", "a/b"} {
		if status := postSchema(id); status != http.StatusBadRequest {
			t.Errorf("POST schemas schema_id=%q status=%d, want 400", id, status)
		}
	}
	if status := postSchema("ok.schema"); status != http.StatusCreated {
		t.Fatalf("valid POST schemas status=%d, want 201", status)
	}

	// updateSource: the path value %1F decodes to 0x1F before the handler
	// (Go ServeMux unescapes PathValue), lands in SourceKey via
	// normalizeSource, and is rejected before any store access.
	body, _ := json.Marshal(map[string]any{"name": "X", "active": true})
	request, _ := http.NewRequest(http.MethodPut, server.URL+"/api/v1/sources/a%1fb", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:tenant-admin")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("PUT /sources/a%%1fb status=%d, want 400", response.StatusCode)
	}
	// A valid update still works through the same handler.
	request, _ = http.NewRequest(http.MethodPut, server.URL+"/api/v1/sources/crm", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:tenant-admin")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("valid PUT /sources/crm status=%d, want 200", response.StatusCode)
	}
	// AC-2 ledger assertion: no rejected source/schema id was persisted, the
	// valid ones were, and no composite-key map holds a multi-separator key
	// (the %1F-decoded path id created nothing).
	assertNoFramedCompositeKeys(t, st)
	if err := st.Read(func(data *store.Snapshot) error {
		for _, id := range []string{"a\x1fb", "\x00-nul", "a/b", "a b"} {
			if _, exists := data.Sources[store.SourceKey("tenant-a", id)]; exists {
				t.Errorf("rejected source id %q was persisted", id)
			}
		}
		if _, exists := data.Sources[store.SourceKey("tenant-a", "src-ok")]; !exists {
			t.Error("valid source src-ok missing after 201")
		}
		for _, id := range []string{"a\x1fb", "a/b"} {
			if _, exists := data.Schemas[store.SchemaKey("tenant-a", id, 1)]; exists {
				t.Errorf("rejected schema id %q was persisted", id)
			}
		}
		if _, exists := data.Schemas[store.SchemaKey("tenant-a", "ok.schema", 1)]; !exists {
			t.Error("valid schema ok.schema missing after 201")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestHTTPPlatformEscapeHatchRejectsKeyFramingTenant is AC-3 + F-4: a
// platform token's ?tenant_id=a%1fb is rejected 400 before any service or
// store access on five endpoints — no cross-tenant read, no forged approve,
// and no self-audit record is appended under the forged tenant.
func TestHTTPPlatformEscapeHatchRejectsKeyFramingTenant(t *testing.T) {
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
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, log.New(io.Discard, "", 0)).Handler())
	defer server.Close()

	event := domain.Event{EventID: "http-kft-1", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), OperationID: "http-kft-op", Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "http-kft-idem", Payload: map[string]any{"value": 1}}
	body, _ := json.Marshal(event)
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err := http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest status=%d err=%v", response.StatusCode, err)
	}
	response.Body.Close()
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/restores", bytes.NewReader([]byte(`{"operation_id":"http-kft-op","reason":"rollback"}`)))
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

	platform := mintRestoreJWT(t, "platform-ops-1", "", []string{"platform-admin"})
	do := func(method, path string) int {
		t.Helper()
		request, _ := http.NewRequest(method, server.URL+path, nil)
		request.Header.Set("Authorization", "Bearer "+platform)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		return response.StatusCode
	}

	// Forged requests: all must 400 before touching the store.
	for _, path := range []string{
		"/api/v1/events/http-kft-1?tenant_id=a%1fb",
		"/api/v1/sources?tenant_id=a%1fb",
		"/api/v1/schemas?tenant_id=a%1fb",
		"/api/v1/admin/actions?tenant_id=a%1fb",
	} {
		if status := do(http.MethodGet, path); status != http.StatusBadRequest {
			t.Errorf("forged GET %s status=%d, want 400", path, status)
		}
	}
	if status := do(http.MethodPost, "/api/v1/restores/"+run.ID+"/approve?tenant_id=a%1fb"); status != http.StatusBadRequest {
		t.Errorf("forged approve status=%d, want 400", status)
	}

	// F-4: no self-audit record under the forged tenant, and the forged
	// calls appended nothing at all (count unchanged from the seed calls).
	if err := st.Read(func(data *store.Snapshot) error {
		for _, action := range data.AdminActions {
			if action.TenantID == "a\x1fb" {
				t.Errorf("forged self-audit record found: %+v", action)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Positive controls: the same endpoints with a valid tenant still work.
	if status := do(http.MethodGet, "/api/v1/events/http-kft-1?tenant_id=tenant-a"); status != http.StatusOK {
		t.Errorf("valid tenant read status=%d, want 200", status)
	}
	if status := do(http.MethodGet, "/api/v1/sources?tenant_id=tenant-a"); status != http.StatusOK {
		t.Errorf("valid tenant sources status=%d, want 200", status)
	}
	if status := do(http.MethodGet, "/api/v1/schemas?tenant_id=tenant-a"); status != http.StatusOK {
		t.Errorf("valid tenant schemas status=%d, want 200", status)
	}
	if status := do(http.MethodGet, "/api/v1/admin/actions?tenant_id=tenant-a"); status != http.StatusOK {
		t.Errorf("valid tenant actions status=%d, want 200", status)
	}
	// Empty ?tenant_id= keeps today's all-tenants semantics (200).
	if status := do(http.MethodGet, "/api/v1/admin/actions?tenant_id="); status != http.StatusOK {
		t.Errorf("empty tenant_id status=%d, want 200", status)
	}
	// The forged approve never flipped the run: it is still pending and the
	// platform can still approve it via the documented escape hatch.
	if status := do(http.MethodPost, "/api/v1/restores/"+run.ID+"/approve?tenant_id=tenant-a"); status != http.StatusOK {
		t.Errorf("valid approve status=%d, want 200", status)
	}
}

// TestHTTPDevTokenRejectsKeyFramingTenant is AC-4 HTTP: dev tokens whose
// subject violates the key-framing charset fail authentication (401) on
// protected routes; valid dev tokens behave exactly as before.
func TestHTTPDevTokenRejectsKeyFramingTenant(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0))
	// Direct handler invocation: net/http's client refuses to serialize a
	// NUL byte in a header, so the \x00 token must be exercised in-process
	// (it reaches the authenticator exactly as a raw request would).
	for _, token := range []string{"dev:a\x1fb:auditor", "dev:a b:auditor", "dev:a/b:auditor", "dev:a\\b:auditor", "dev:\x00:auditor"} {
		request := httptest.NewRequest(http.MethodGet, "/api/v1/events/nonexistent", nil)
		request.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusUnauthorized {
			t.Errorf("token %q status=%d, want 401", token, recorder.Code)
		}
	}
	// Positive control: the valid dev auditor authenticates and reaches the
	// handler (404 for a missing event, never 401).
	request := httptest.NewRequest(http.MethodGet, "/api/v1/events/nonexistent", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("valid dev auditor status=%d, want 404 (authenticated, event missing)", recorder.Code)
	}
}

// TestHTTPJWTRejectsKeyFramingTenantClaim is REQ-6's HTTP surface: a signed
// JWT whose tenant_id claim violates the canonical key-framing rule fails
// authentication with 401 and error.code == "unauthorized" before any
// handler logic — no admin action is appended. The positive control uses a
// read-capable role (auditor) so the exact-string claim provably reaches
// the handler (404 for a missing event, never 401).
func TestHTTPJWTRejectsKeyFramingTenantClaim(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}); err != nil {
		t.Fatal(err)
	}
	server := NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, log.New(io.Discard, "", 0))
	// Auth rejection happens before any handler logic: the rejected claims
	// appended nothing to the self-audit trail (count unchanged from the
	// CreateTenant seed action).
	var before int
	if err := st.Read(func(data *store.Snapshot) error {
		before = len(data.AdminActions)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, tenantID := range []string{"a/b", `a\b`, "a\x1fb"} {
		token := mintRestoreJWT(t, "auditor-1", tenantID, []string{"auditor"})
		request := httptest.NewRequest(http.MethodGet, "/api/v1/events/nonexistent", nil)
		request.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusUnauthorized {
			t.Errorf("tenant claim %q status=%d, want 401", tenantID, recorder.Code)
			continue
		}
		var body struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.Error.Code != "unauthorized" {
			t.Errorf("tenant claim %q error.code=%q, want unauthorized", tenantID, body.Error.Code)
		}
	}
	if err := st.Read(func(data *store.Snapshot) error {
		if len(data.AdminActions) != before {
			t.Errorf("rejected claims appended %d admin actions, want %d", len(data.AdminActions), before)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Positive control: an exact-string tenant claim authenticates and
	// reaches the handler (404 for a missing event, never 401).
	token := mintRestoreJWT(t, "auditor-1", "tenant-a", []string{"auditor"})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/events/nonexistent", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("valid tenant claim status=%d, want 404 (authenticated, event missing)", recorder.Code)
	}
}

// TestHTTPTenantForRechecksClaimTenantID is REQ-6b / FM-4: the non-platform
// tenantFor branch re-checks claims.TenantID against the canonical
// key-framing rule (store.ValidTenantID) before returning it, surfacing the
// raw domain.ErrInvalid-wrapped error — identical status semantics (400
// invalid_request) to the platform escape-hatch branch. Empty tenant
// context stays legal (all-tenants reads; client-id-resolved ingest).
func TestHTTPTenantForRechecksClaimTenantID(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/events/x", nil)
	for _, bad := range []string{"a/b", `a\b`, "a\x1fb", "a b", "a:b"} {
		if _, err := (&Server{}).tenantFor(request, auth.Claims{TenantID: bad}); !errors.Is(err, domain.ErrInvalid) {
			t.Errorf("tenantFor with claim %q err=%v, want domain.ErrInvalid", bad, err)
		}
	}
	for _, good := range []string{"", "tenant-a"} {
		tenantID, err := (&Server{}).tenantFor(request, auth.Claims{TenantID: good})
		if err != nil || tenantID != good {
			t.Errorf("tenantFor with claim %q = (%q,%v), want (%q,nil)", good, tenantID, err, good)
		}
	}
	// Platform branch unaffected: the escape-hatch query wins over a claim.
	query := httptest.NewRequest(http.MethodGet, "/api/v1/events/x?tenant_id=tenant-a", nil)
	if tenantID, err := (&Server{}).tenantFor(query, auth.Claims{Platform: true, TenantID: "a/b"}); err != nil || tenantID != "tenant-a" {
		t.Errorf("platform tenantFor = (%q,%v), want (tenant-a,nil)", tenantID, err)
	}
	// REQ-2: the platform escape-hatch query is a tenant input too — a colon
	// tenant filter must be rejected with the same ErrInvalid (400) so a
	// platform read can never select a tenant whose ID would be ambiguous
	// in stream framing.
	colonQuery := httptest.NewRequest(http.MethodGet, "/api/v1/events/x?tenant_id=a:b", nil)
	if _, err := (&Server{}).tenantFor(colonQuery, auth.Claims{Platform: true, TenantID: ""}); !errors.Is(err, domain.ErrInvalid) {
		t.Errorf("platform tenantFor(?tenant_id=a:b) err=%v, want domain.ErrInvalid", err)
	}
	// S-2.3: the platform branch returns the empty all-tenants sentinel when
	// no (or an empty) tenant_id query is present — it must never fall
	// through to claims.TenantID (dev tokens fill it with their subject,
	// e.g. "platform"; production JWT platform tokens carry an empty claim).
	for _, raw := range []string{"/api/v1/events/x", "/api/v1/events/x?tenant_id="} {
		unfiltered := httptest.NewRequest(http.MethodGet, raw, nil)
		tenantID, err := (&Server{}).tenantFor(unfiltered, auth.Claims{Platform: true, TenantID: "platform"})
		if err != nil || tenantID != "" {
			t.Errorf("platform tenantFor(%q) = (%q,%v), want (\"\",nil) all-tenants sentinel", raw, tenantID, err)
		}
	}
}

// TestHTTPIngestRejectsColonStreamComponents is REQ-4 at the HTTP boundary:
// the single-event ingest endpoint rejects the colon-aggregate collision
// pair with 400 invalid_request (statusForError(ErrInvalid) — never 422,
// which is reserved for ErrSchemaNotFound/ErrTenantMismatch). The identical
// event with colon-free components is accepted.
func TestHTTPIngestRejectsColonStreamComponents(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	base := domain.Event{EventID: "http-colon-1", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "http-colon-idem", Payload: map[string]any{"value": 1}}
	for _, tc := range []struct {
		name   string
		mutate func(*domain.Event)
	}{
		{"aggregate_type colon (collision pair a)", func(e *domain.Event) { e.AggregateType = "a:b"; e.AggregateID = "c" }},
		{"aggregate_id colon (collision pair b)", func(e *domain.Event) { e.AggregateType = "a"; e.AggregateID = "b:c" }},
	} {
		event := base
		tc.mutate(&event)
		got := postTestEvent(t, server.URL, "dev:tenant-a:service:crm", event)
		if got.Status != http.StatusBadRequest || got.Code != "invalid_request" {
			t.Errorf("%s: status=%d code=%q, want 400 invalid_request", tc.name, got.Status, got.Code)
		}
	}
	// Positive control: the same event with colon-free components ingests.
	ok := base
	ok.EventID = "http-colon-ok"
	ok.IdempotencyKey = "http-colon-idem-ok"
	ok.AggregateType = "invoice"
	ok.AggregateID = "inv-1"
	if got := postTestEvent(t, server.URL, "dev:tenant-a:service:crm", ok); got.Status != http.StatusAccepted {
		t.Fatalf("colon-free aggregate ingest status=%d, want 202", got.Status)
	}
}

// TestTenantForBoundaryGuard is G4: the raw query read
// r.URL.Query().Get("tenant_id") exists in server.go exactly once — inside
// tenantFor — and tenantFor returns two values (compile-forced error
// handling at every call site). Any duplicate raw read re-opens the forged
// escape hatch.
// scriptedStoreBackend is an in-memory store.Backend whose Save can be armed
// to fail (non-conflict error or permanent ErrSnapshotConflict). It exists so
// fail-closed HTTP tests can seed through a clean backend and arm the append
// failure afterwards — the file-backed testHTTPServerWithStore has no
// injection seam (async review finding 1). LoadForUpdate hands Update a deep
// copy so closure mutations never leak into shared state when Save fails.
type scriptedStoreBackend struct {
	data           *store.Snapshot
	saveErr        error // armed after seeding: Save returns it immediately
	alwaysConflict bool  // armed after seeding: Save conflicts forever
	saves          int
	loads          int
}

func (b *scriptedStoreBackend) Load() (*store.Snapshot, error) { return b.data, nil }

func (b *scriptedStoreBackend) LoadForUpdate() (*store.Snapshot, error) {
	b.loads++
	encoded, err := json.Marshal(b.data)
	if err != nil {
		return nil, err
	}
	copyData := store.NewSnapshot()
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(copyData); err != nil {
		return nil, err
	}
	return copyData, nil
}

func (b *scriptedStoreBackend) Save(data *store.Snapshot) error {
	b.saves++
	if b.saveErr != nil {
		return b.saveErr
	}
	if b.alwaysConflict {
		return store.ErrSnapshotConflict
	}
	b.data = data
	return nil
}

// testHTTPServerWithBackend builds the standard test server (same tenants,
// source and schema as testHTTPServerWithStore) over an in-memory scripted
// backend instead of a file-backed store, so fail-closed tests can arm Save
// failures after seeding. Returns the server and the service for backend
// access.
func testHTTPServerWithBackend(t *testing.T, backend *scriptedStoreBackend) (*httptest.Server, *service.Service) {
	t.Helper()
	svc, err := service.New(store.NewWithBackend(backend), service.Config{ArchiveDir: filepath.Join(t.TempDir(), "archive"), Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
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
	return httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, log.New(io.Discard, "", 0)).Handler()), svc
}

// httpReadSeedEvent is the standard event for HTTP read-endpoint tests: it
// carries an aggregate so the aggregate timeline route resolves, and an
// operation so timeline/replay resolve.
func httpReadSeedEvent() domain.Event {
	return domain.Event{EventID: "http-read-evt", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), OperationID: "http-read-op", AggregateType: "invoice", AggregateID: "inv-1", AggregateVersion: 1, Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "http-read-idem", Payload: map[string]any{"value": 1}}
}

// performReadEndpoints executes the six audited read endpoints with the
// given bearer token and returns the response status codes in fixed order
// (receipt, operation summary, timeline, replay, aggregate timeline,
// integrity verify).
func performReadEndpoints(t *testing.T, server *httptest.Server, token string) []int {
	t.Helper()
	requests := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodGet, "/api/v1/events/http-read-evt/receipt", ""},
		{http.MethodGet, "/api/v1/operations/http-read-op", ""},
		{http.MethodGet, "/api/v1/operations/http-read-op/timeline", ""},
		{http.MethodGet, "/api/v1/operations/http-read-op/replay", ""},
		{http.MethodGet, "/api/v1/aggregates/invoice/inv-1/timeline", ""},
		{http.MethodPost, "/api/v1/integrity/verify", `{"stream_id":""}`},
	}
	statuses := make([]int, 0, len(requests))
	for _, req := range requests {
		var reader io.Reader
		if req.body != "" {
			reader = bytes.NewReader([]byte(req.body))
		}
		request, _ := http.NewRequest(req.method, server.URL+req.path, reader)
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		statuses = append(statuses, response.StatusCode)
	}
	return statuses
}

// httpReadFacts lists tenant-a's admin trail through the public API and
// returns the audit.event.read rows.
func httpReadFacts(t *testing.T, server *httptest.Server) []domain.AdminAction {
	t.Helper()
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/admin/actions", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:tenant-admin")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("list actions status=%d body=%s", response.StatusCode, data)
	}
	var result struct {
		Items []domain.AdminAction `json:"items"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	var reads []domain.AdminAction
	for _, action := range result.Items {
		if action.Action == domain.AdminActionEventRead {
			reads = append(reads, action)
		}
	}
	return reads
}

// TestHTTPReadEndpointsAppendSelfAuditFacts is AC-1 (HTTP): the six read
// routes append exactly six audit.event.read rows carrying the acting
// subject (claims.Subject) and the FR-4 target encoding.
func TestHTTPReadEndpointsAppendSelfAuditFacts(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	if result := postTestEvent(t, server.URL, "dev:tenant-a:service:crm", httpReadSeedEvent()); result.Status != http.StatusAccepted {
		t.Fatalf("seed ingest status=%d: %+v", result.Status, result)
	}
	// The acting subject must be distinct from the tenant for the actor
	// assertion to be meaningful: dev tokens fix Subject == tenant id, so a
	// locally signed JWT expresses the auditor (mintRestoreJWT pattern).
	token := mintRestoreJWT(t, "auditor", "tenant-a", []string{"compliance"})
	for i, status := range performReadEndpoints(t, server, token) {
		if status != http.StatusOK {
			t.Fatalf("read endpoint %d status=%d, want 200", i, status)
		}
	}
	reads := httpReadFacts(t, server)
	if len(reads) != 6 {
		t.Fatalf("audit.event.read rows=%d, want 6; %+v", len(reads), reads)
	}
	for _, read := range reads {
		if read.Actor != "auditor" {
			t.Fatalf("read actor=%q, want auditor (claims.Subject); %+v", read.Actor, read)
		}
	}
	got := map[string]bool{}
	for _, read := range reads {
		got[read.TargetType+"|"+read.TargetID+"|"+read.Detail] = true
	}
	want := map[string]bool{
		"event|http-read-evt|receipt":     true,
		"operation|http-read-op|summary":  true,
		"operation|http-read-op|timeline": true,
		"operation|http-read-op|replay":   true,
		"aggregate|inv-1|invoice":         true,
		"integrity||verify":               true,
	}
	if len(got) != len(want) {
		t.Fatalf("read facts=%v, want exact set %v", got, want)
	}
	for key := range want {
		if !got[key] {
			t.Fatalf("read facts=%v, missing %q", got, key)
		}
	}
}

// TestHTTPOperationSummaryAppendsExactlyOneFact is AC-1 focused: a single
// GET of the operation summary appends exactly one audit.event.read row
// (delta from the pre-request baseline) with the FR-4 encoding
// (operation, id, summary) and the acting subject (claims.Subject).
func TestHTTPOperationSummaryAppendsExactlyOneFact(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	if result := postTestEvent(t, server.URL, "dev:tenant-a:service:crm", httpReadSeedEvent()); result.Status != http.StatusAccepted {
		t.Fatalf("seed ingest status=%d: %+v", result.Status, result)
	}
	token := mintRestoreJWT(t, "auditor", "tenant-a", []string{"compliance"})
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/operations/http-read-op", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("summary status=%d, want 200", response.StatusCode)
	}
	reads := httpReadFacts(t, server)
	if len(reads) != 1 {
		t.Fatalf("audit.event.read rows=%d, want exactly 1; %+v", len(reads), reads)
	}
	read := reads[0]
	if read.Action != domain.AdminActionEventRead || read.TargetType != "operation" || read.TargetID != "http-read-op" || read.Detail != "summary" || read.Actor != "auditor" {
		t.Fatalf("summary fact=%+v, want (operation, http-read-op, summary) by auditor", read)
	}
}

// TestHTTPOperationSummaryNotFoundAppendsNothing is AC-2 (HTTP): an unknown
// operation id 404s before the append point and appends no fact. The empty
// segment /api/v1/operations/ is answered 404 by the ServeMux itself (Go 1.22
// wildcard routing never invokes the handler), so both legs leave the trail
// untouched; the ErrInvalid→400 mapping is covered at the service layer
// (TestReadSelfAuditOperationInvalidAppendsNothing).
func TestHTTPOperationSummaryNotFoundAppendsNothing(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	token := mintRestoreJWT(t, "auditor", "tenant-a", []string{"compliance"})
	for _, path := range []string{"/api/v1/operations/does-not-exist", "/api/v1/operations/"} {
		request, _ := http.NewRequest(http.MethodGet, server.URL+path, nil)
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusNotFound {
			t.Fatalf("GET %s status=%d, want 404", path, response.StatusCode)
		}
	}
	if reads := httpReadFacts(t, server); len(reads) != 0 {
		t.Fatalf("audit.event.read rows=%d, want 0 (rejected reads append nothing); %+v", len(reads), reads)
	}
}

// TestHTTPReadEndpointsFailClosedOnAppendFailure is AC-2 (HTTP) plus the F3
// status mapping: when the admin-action append fails, all six read routes
// return non-200 and append nothing. The backend is seeded and the event
// ingested through a clean backend FIRST, then the failure is armed (async
// review finding 1).
func TestHTTPReadEndpointsFailClosedOnAppendFailure(t *testing.T) {
	seed := func(t *testing.T, backend *scriptedStoreBackend) (*httptest.Server, string) {
		t.Helper()
		server, _ := testHTTPServerWithBackend(t, backend)
		t.Cleanup(server.Close)
		if result := postTestEvent(t, server.URL, "dev:tenant-a:service:crm", httpReadSeedEvent()); result.Status != http.StatusAccepted {
			t.Fatalf("seed ingest status=%d: %+v", result.Status, result)
		}
		return server, mintRestoreJWT(t, "auditor", "tenant-a", []string{"compliance"})
	}
	t.Run("non-conflict append failure fails reads closed with 500", func(t *testing.T) {
		backend := &scriptedStoreBackend{data: store.NewSnapshot()}
		server, token := seed(t, backend)
		backend.saveErr = errors.New("append failed") // arm AFTER seeding + ingest
		for i, status := range performReadEndpoints(t, server, token) {
			if status != http.StatusInternalServerError {
				t.Fatalf("endpoint %d status=%d, want 500 (statusForError non-conflict save error)", i, status)
			}
		}
		if reads := httpReadFacts(t, server); len(reads) != 0 {
			t.Fatalf("audit.event.read rows=%d, want 0; %+v", len(reads), reads)
		}
	})
	t.Run("conflict exhaustion fails reads closed with 503", func(t *testing.T) {
		backend := &scriptedStoreBackend{data: store.NewSnapshot()}
		server, token := seed(t, backend)
		backend.alwaysConflict = true // arm AFTER seeding + ingest
		for i, status := range performReadEndpoints(t, server, token) {
			if status != http.StatusServiceUnavailable {
				t.Fatalf("endpoint %d status=%d, want 503 (statusForError ErrSnapshotConflict)", i, status)
			}
		}
		if reads := httpReadFacts(t, server); len(reads) != 0 {
			t.Fatalf("audit.event.read rows=%d, want 0; %+v", len(reads), reads)
		}
	})
}

// TestHTTPVerifyIntegrityRejectsOversizedStreamID is the F2 regression test
// at the HTTP boundary: verify with an over-cap, whitespace or control-char
// stream_id is rejected 400 and appends no row; a healthy verify still 200s.
func TestHTTPVerifyIntegrityRejectsOversizedStreamID(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	token := mintRestoreJWT(t, "auditor", "tenant-a", []string{"compliance"})
	verify := func(body string) int {
		t.Helper()
		request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/integrity/verify", strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		return response.StatusCode
	}
	if status := verify(`{"stream_id":"` + strings.Repeat("s", 257) + `"}`); status != http.StatusBadRequest {
		t.Fatalf("oversized stream_id status=%d, want 400", status)
	}
	if status := verify(`{"stream_id":"stream with space"}`); status != http.StatusBadRequest {
		t.Fatalf("whitespace stream_id status=%d, want 400", status)
	}
	if status := verify(`{"stream_id":"stream\u001fid"}`); status != http.StatusBadRequest {
		t.Fatalf("control-char stream_id status=%d, want 400", status)
	}
	if reads := httpReadFacts(t, server); len(reads) != 0 {
		t.Fatalf("audit.event.read rows=%d, want 0 (rejected verifies append nothing); %+v", len(reads), reads)
	}
	if status := verify(`{"stream_id":""}`); status != http.StatusOK {
		t.Fatalf("healthy verify status=%d, want 200", status)
	}
}

func TestTenantForBoundaryGuard(t *testing.T) {
	src, err := os.ReadFile("scope.go")
	if err != nil {
		t.Fatalf("read scope.go (go test runs with CWD=package dir): %v", err)
	}
	text := string(src)
	rawRead := `url.ParseQuery(r.URL.RawQuery)`
	if count := strings.Count(text, rawRead); count != 1 {
		t.Fatalf("scope.go contains %d raw tenant query parses, want exactly 1 (inside queryTenantSelector)", count)
	}
	serverSrc, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatalf("read server.go (go test runs with CWD=package dir): %v", err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "server.go", serverSrc, parser.AllErrors)
	if err != nil {
		t.Fatalf("parse server.go: %v", err)
	}
	var tenantFor *ast.FuncDecl
	for _, decl := range file.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "tenantFor" {
			continue
		}
		tenantFor = fd
	}
	if tenantFor == nil {
		t.Fatal("server.go: func tenantFor not found")
	}
	if tenantFor.Type.Results == nil || len(tenantFor.Type.Results.List) != 2 {
		t.Fatalf("tenantFor must return (string, error), got %d result groups", len(tenantFor.Type.Results.List))
	}
}
