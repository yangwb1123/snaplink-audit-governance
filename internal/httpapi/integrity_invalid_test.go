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

// TestHTTPVerifyIntegrityInvalidResult is the E2E invalid-path contract: a
// tampered ledger is reported through the documented IntegrityResult JSON
// (HTTP 200, "valid":false, free-form errors) and the
// audit_integrity_checks_total{result="invalid"} counter increments.
func TestHTTPVerifyIntegrityInvalidResult(t *testing.T) {
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
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler())
	defer server.Close()

	event := domain.Event{EventID: "invalid-evt-1", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "invalid-idem-1", Payload: map[string]any{"value": 1}}
	body, _ := json.Marshal(event)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events?wait_for=ledgered", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest status=%d err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	// Tamper the stored event through the store handle.
	if err := st.Update(func(data *store.Snapshot) error {
		key := store.EventKey("tenant-a", "invalid-evt-1")
		value := data.Events[key]
		value.Payload = map[string]any{"value": 424242}
		value.SourceDigest = "attacker-chosen-value"
		value.Hash = strings.Repeat("0", len(value.Hash))
		data.Events[key] = value
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	req, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/integrity/verify", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("verify status=%d err=%v", resp.StatusCode, err)
	}
	responseBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var result service.IntegrityResult
	if err := json.Unmarshal(responseBody, &result); err != nil {
		t.Fatalf("invalid IntegrityResult JSON: %v\n%s", err, responseBody)
	}
	if result.Valid {
		t.Fatalf("tampered ledger must be reported invalid: %s", responseBody)
	}
	if !strings.Contains(strings.Join(result.Errors, "\n"), "content digest mismatch") {
		t.Fatalf("expected content digest mismatch, got %+v", result.Errors)
	}

	req, _ = http.NewRequest(http.MethodGet, server.URL+"/metrics", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status=%d err=%v", resp.StatusCode, err)
	}
	metricsBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(metricsBody), `audit_integrity_checks_total{result="invalid"} 1`) {
		t.Fatalf("expected invalid metric, got:\n%s", metricsBody)
	}
}
