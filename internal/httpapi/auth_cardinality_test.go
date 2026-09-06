package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestHTTPRejectsAmbiguousAuthorizationBeforeIngest(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()

	cases := []struct {
		name   string
		values []string
	}{
		{name: "none", values: nil},
		{name: "identical-valid", values: []string{"Bearer dev:tenant-a:service:crm", "Bearer dev:tenant-a:service:crm"}},
		{name: "valid-and-invalid", values: []string{"Bearer dev:tenant-a:service:crm", "Bearer invalid"}},
		{name: "different-tenants", values: []string{"Bearer dev:tenant-a:service:crm", "Bearer dev:tenant-b:service:crm"}},
	}
	for index, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := newHTTPIngestRequest(t, server.URL, "http-ambiguous-"+tc.name+"-"+string(rune('a'+index)))
			for _, value := range tc.values {
				request.Header.Add("Authorization", value)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr != nil {
				t.Fatal(readErr)
			}
			if response.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status=%d body=%s, want 401", response.StatusCode, body)
			}
			for _, value := range tc.values {
				if len(value) > 0 && bytes.Contains(body, []byte(value)) {
					t.Fatalf("authorization value leaked in response: %s", body)
				}
			}
			assertHTTPStoreEmpty(t, st)
		})
	}

	request := newHTTPIngestRequest(t, server.URL, "http-single-valid")
	request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("single credential status=%d, want 202", response.StatusCode)
	}
	if err := st.Read(func(data *store.Snapshot) error {
		if _, ok := data.Receipts[store.EventKey("tenant-a", "http-single-valid")]; !ok {
			t.Fatal("single valid credential did not persist receipt")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func newHTTPIngestRequest(t *testing.T, baseURL, eventID string) *http.Request {
	t.Helper()
	event, err := json.Marshal(domain.Event{
		EventID: eventID, SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1,
		OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "service"}, Action: "write",
		Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: eventID + "-idem",
		Payload: map[string]any{"value": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, baseURL+"/api/v1/events?wait_for=ledgered", bytes.NewReader(event))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	return request
}

func assertHTTPStoreEmpty(t *testing.T, st *store.Store) {
	t.Helper()
	if err := st.Read(func(data *store.Snapshot) error {
		if len(data.Events) != 0 || len(data.Receipts) != 0 || len(data.LedgeredOutbox) != 0 {
			t.Fatalf("ambiguous credentials changed persistence: events=%d receipts=%d outbox=%d", len(data.Events), len(data.Receipts), len(data.LedgeredOutbox))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
