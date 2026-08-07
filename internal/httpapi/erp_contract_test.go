package httpapi

// ERP 契约对拍（M0）——sverpweb-stable 审计接入的双侧契约测试。
//
// 发送端契约（sverpweb-stable apps/api/src/audit/）：
//   - POST {AUDIT_APP_URL}/api/v1/events:batch
//   - 载荷 {schemaVersion, source, sentAt, events[]}，事件字段映射后满足
//     ValidateBasic 9 必填字段；idempotency_key = event.id（跨重试稳定）。
//
// 本文件钉死接收端语义（对应修订版路线图 §2.2/§2.3 三分支）：
//   1. 202 + receipt.Duplicate=true  —— 同 event.id + 同内容重投 = 去重成功；
//   2. 409 + event_id_content_conflict —— 同 event.id + 内容不同 = 发端缺陷；
//   3. 409 + idempotency_key_conflict —— 同 key + 异 event.id = 发端缺陷；
//   4. 400 必填字段清单（B1 字段映射的 golden 断言列表）；
//   5. 批端点形状与首错截断语义（not_attempted 四态依据）。

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

func erpTestHTTPServer(t *testing.T) *httptest.Server {
	t.Helper()
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
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "sverp-web", Name: "SVERP Web", Active: true}); err != nil {
		t.Fatal(err)
	}
	// ERP schema 注册（sverpweb 侧"每 event_type 一条"模式的契约样例）。
	for _, eventType := range []string{"purchase_request.created.v1", "purchase_request.approved.v1"} {
		if err := svc.RegisterSchema("test", domain.EventSchema{
			TenantID: "tenant-a", SchemaID: "sverp." + eventType, Version: 1,
			EventType: eventType, Active: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	return httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler())
}

func erpEvent(eventID string, idempotencyKey string, payload map[string]any) domain.Event {
	return domain.Event{
		EventID: eventID, SourceSystem: "sverp-web", EventType: "purchase_request.created.v1",
		SchemaID: "sverp.purchase_request.created.v1", SchemaVersion: 1,
		OccurredAt: time.Unix(1_700_000_010, 0).UTC(), OperationID: "op-" + eventID,
		Actor: domain.Actor{ID: "user-1"}, Action: "create", Outcome: "success",
		DataClassification: "internal", RetentionClass: "standard",
		IdempotencyKey: idempotencyKey, Payload: payload,
	}
}

func postEvents(t *testing.T, server *httptest.Server, events []domain.Event) (int, map[string]any) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"events": events})
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events:batch", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer dev:tenant-a:service:sverp-web")
	req.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var decoded map[string]any
	raw, _ := io.ReadAll(response.Body)
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &decoded)
	}
	return response.StatusCode, decoded
}

// M0-1：202 + Duplicate —— 同 event.id + 同内容重投 = 去重成功（幂等安全网）。
func TestERPContractIdempotentRepostIs202Duplicate(t *testing.T) {
	server := erpTestHTTPServer(t)
	defer server.Close()
	event := erpEvent("erp-evt-1", "erp-evt-1", map[string]any{"order": "PO-1001", "status": "draft"})

	status, body := postEvents(t, server, []domain.Event{event})
	if status != http.StatusAccepted {
		t.Fatalf("first ingest status=%d body=%v", status, body)
	}
	status, body = postEvents(t, server, []domain.Event{event})
	if status != http.StatusAccepted {
		t.Fatalf("duplicate re-post status=%d body=%v (must be 202, not 409)", status, body)
	}
	receipts, _ := body["receipts"].([]any)
	if len(receipts) != 1 {
		t.Fatalf("expected 1 receipt, got %v", body)
	}
	receipt := receipts[0].(map[string]any)
	if receipt["duplicate"] != true {
		t.Fatalf("expected receipt.Duplicate=true, got %v", receipt)
	}
	if receipt["conflict"] == true {
		t.Fatalf("duplicate re-post must not conflict: %v", receipt)
	}
}

// M0-2：409 + event_id_content_conflict —— 同 event.id + 内容不同 = 发端缺陷（不得静默改写）。
func TestERPContractSameEventIDDifferentContentIs409(t *testing.T) {
	server := erpTestHTTPServer(t)
	defer server.Close()
	event := erpEvent("erp-evt-2", "erp-evt-2", map[string]any{"order": "PO-1002", "status": "draft"})
	status, _ := postEvents(t, server, []domain.Event{event})
	if status != http.StatusAccepted {
		t.Fatalf("first ingest status=%d", status)
	}

	tampered := erpEvent("erp-evt-2", "erp-evt-2", map[string]any{"order": "PO-1002", "status": "APPROVED"})
	status, body := postEvents(t, server, []domain.Event{tampered})
	if status != http.StatusConflict {
		t.Fatalf("content-conflict status=%d body=%v (must be 409)", status, body)
	}
	if code := receiptErrorCode(body); code != "event_id_content_conflict" {
		t.Fatalf("expected event_id_content_conflict receipt error_code, got %q (body=%v)", code, body)
	}
}

// M0-3：409 + idempotency_key_conflict —— 同 key + 异 event.id = 发端缺陷（key 不得复用）。
func TestERPContractIdempotencyKeyReuseIs409(t *testing.T) {
	server := erpTestHTTPServer(t)
	defer server.Close()
	first := erpEvent("erp-evt-3a", "shared-key-1", map[string]any{"order": "PO-1003"})
	status, _ := postEvents(t, server, []domain.Event{first})
	if status != http.StatusAccepted {
		t.Fatalf("first ingest status=%d", status)
	}

	second := erpEvent("erp-evt-3b", "shared-key-1", map[string]any{"order": "PO-1003"})
	status, body := postEvents(t, server, []domain.Event{second})
	if status != http.StatusConflict {
		t.Fatalf("key-reuse status=%d body=%v (must be 409)", status, body)
	}
	if code := receiptErrorCode(body); code != "idempotency_key_conflict" {
		t.Fatalf("expected idempotency_key_conflict receipt error_code, got %q (body=%v)", code, body)
	}
}

// M0-4：400 必填字段清单（B1 字段映射的 golden 断言列表）——
// ERP 适配器映射后的每个事件必须满足这 9 个必填字段，缺一即整批拒绝。
func TestERPContractMissingRequiredFieldIs400(t *testing.T) {
	server := erpTestHTTPServer(t)
	defer server.Close()
	cases := []struct {
		name   string
		mutate func(*domain.Event)
	}{
		{"event_id", func(e *domain.Event) { e.EventID = "" }},
		{"source_system", func(e *domain.Event) { e.SourceSystem = "" }},
		{"event_type", func(e *domain.Event) { e.EventType = "" }},
		{"schema_id", func(e *domain.Event) { e.SchemaID = "" }},
		{"action", func(e *domain.Event) { e.Action = "" }},
		{"outcome", func(e *domain.Event) { e.Outcome = "" }},
		{"data_classification", func(e *domain.Event) { e.DataClassification = "" }},
		{"retention_class", func(e *domain.Event) { e.RetentionClass = "" }},
		{"idempotency_key", func(e *domain.Event) { e.IdempotencyKey = "" }},
		{"actor.id", func(e *domain.Event) { e.Actor.ID = "" }},
		{"payload", func(e *domain.Event) { e.Payload = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event := erpEvent("erp-evt-4-"+tc.name, "idem-4-"+tc.name, map[string]any{"order": "PO-1004"})
			tc.mutate(&event)
			status, body := postEvents(t, server, []domain.Event{event})
			if status != http.StatusBadRequest {
				t.Fatalf("%s: status=%d body=%v (must be 400)", tc.name, status, body)
			}
			if !stringsContains(body["error"], tc.name) {
				t.Fatalf("%s: error %v must mention field %q", tc.name, body["error"], tc.name)
			}
		})
	}
}

// M0-5：批端点形状与首错截断——批内第 2 条坏事件时，返回 400 + 已处理 receipts
// 前缀（第 1 条已入账），后续事件未被尝试（not_attempted 语义依据）。
func TestERPContractBatchStopsAtFirstError(t *testing.T) {
	server := erpTestHTTPServer(t)
	defer server.Close()
	good := erpEvent("erp-evt-5a", "idem-5a", map[string]any{"order": "PO-1005"})
	bad := erpEvent("erp-evt-5b", "idem-5b", map[string]any{"order": "PO-1005"})
	bad.SchemaID = "sverp.unknown.v1" // 未注册 schema

	status, body := postEvents(t, server, []domain.Event{good, bad})
	// 未注册 schema = 422（修订版 §2.2 permanent 分类含 422）——首错截断，
	// 批内后续事件未被尝试（not_attempted）。
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("batch status=%d body=%v (must be 422)", status, body)
	}
	receipts, _ := body["receipts"].([]any)
	// 首错截断：receipts 前缀 = [已入账的 good, 零值 bad（not_attempted 证据）]
	if len(receipts) != 2 {
		t.Fatalf("expected 2 receipts (accepted + zero-value), got %v", body)
	}
	firstReceipt, _ := receipts[0].(map[string]any)
	if firstReceipt["event_id"] != "erp-evt-5a" || firstReceipt["status"] != "accepted" {
		t.Fatalf("first receipt must be the accepted event, got %v", firstReceipt)
	}
	secondReceipt, _ := receipts[1].(map[string]any)
	if eventID, _ := secondReceipt["event_id"].(string); eventID != "" {
		t.Fatalf("second receipt must be the zero-value (not_attempted), got %v", secondReceipt)
	}
	// 第 1 条已入账（receipt 可查）——发送端据此逐事件裁决，不得整批重投。
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/events/erp-evt-5a?tenant_id=tenant-a", nil)
	req.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("accepted event must be queryable, status=%d", response.StatusCode)
	}
}

func receiptErrorCode(body map[string]any) string {
	receipts, _ := body["receipts"].([]any)
	if len(receipts) == 0 {
		return ""
	}
	receipt, ok := receipts[len(receipts)-1].(map[string]any)
	if !ok {
		return ""
	}
	code, _ := receipt["error_code"].(string)
	return code
}

func stringsContains(v any, needle string) bool {
	// 错误体双层嵌套：{error: {code, message, request_id}}
	outer, ok := v.(map[string]any)
	if !ok {
		s, ok := v.(string)
		return ok && bytes.Contains([]byte(s), []byte(needle))
	}
	inner, ok := outer["error"].(map[string]any)
	if !ok {
		return false
	}
	msg, _ := inner["message"].(string)
	return bytes.Contains([]byte(msg), []byte(needle))
}
