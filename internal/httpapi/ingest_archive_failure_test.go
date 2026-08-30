package httpapi

import (
	"context"
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

type httpFailingArchive struct {
	err  error
	puts int
}

func (a *httpFailingArchive) Put(context.Context, string, []byte) error {
	a.puts++
	return a.err
}

func (a *httpFailingArchive) Get(context.Context, string) ([]byte, error) { return nil, nil }

func (a *httpFailingArchive) Ready(context.Context) error { return nil }

func TestHTTPArchivedIngestMasksArchiveFailure(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	archiveErr := errors.New("archive backend path leaked")
	archiveStore := &httpFailingArchive{err: archiveErr}
	svc, err := service.New(st, service.Config{
		Archive:         archiveStore,
		SegmentSize:     100,
		SigningSecret:   "test-signing-secret",
		Now:             func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
		AllowDevSecrets: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{
		TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true,
		RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"},
	}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler())
	defer server.Close()

	event := domain.Event{
		EventID: "http-archive-failure", SourceSystem: "crm", EventType: "audit.event",
		SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(),
		OperationID: "http-archive-operation", Actor: domain.Actor{ID: "user-1"}, Action: "update",
		Outcome: "success", DataClassification: "internal", RetentionClass: "standard",
		IdempotencyKey: "http-archive-failure-key", Payload: map[string]any{"resource": "invoice", "value": 10},
	}
	body := `{"event_id":"http-archive-failure","source_system":"crm","event_type":"audit.event","schema_id":"audit.event","schema_version":1,"occurred_at":"2023-11-14T22:13:30Z","operation_id":"http-archive-operation","actor":{"id":"user-1"},"action":"update","outcome":"success","data_classification":"internal","retention_class":"standard","idempotency_key":"http-archive-failure-key","payload":{"resource":"invoice","value":10}}`
	request, err := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events?wait_for=archived", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s, want 500", response.StatusCode, data)
	}
	if strings.Contains(string(data), archiveErr.Error()) || strings.Contains(string(data), "receipt") {
		t.Fatalf("response leaked archive detail or success receipt: %s", data)
	}
	if !strings.Contains(string(data), `"code":"internal_error"`) || !strings.Contains(string(data), `"message":"internal server error"`) {
		t.Fatalf("response=%s, want redacted internal_error envelope", data)
	}
	if archiveStore.puts != 1 {
		t.Fatalf("archive puts=%d, want one attempted write", archiveStore.puts)
	}
	receipt, err := svc.GetReceipt("tenant-a", "", event.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Status != domain.StatusIndexed || !receipt.ArchivedAt.IsZero() {
		t.Fatalf("durable receipt=%+v, want indexed and not archived", receipt)
	}
}
