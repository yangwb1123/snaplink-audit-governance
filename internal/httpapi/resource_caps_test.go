package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// TestHTTPBatchResourceCapPreflight proves the count cap is checked before
// the first ingest. The boundary fixture is deliberately compact and asserts
// its serialized size is below the aggregate body cap, so a failure cannot be
// misclassified as a transport-size rejection.
func TestHTTPBatchResourceCapPreflight(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()

	overEvents := make([]domain.Event, domain.MaxBatchEvents+1)
	for i := range overEvents {
		overEvents[i] = httpCapEvent("http-over-batch", i)
	}
	status, response := postHTTPBatch(t, server.URL, overEvents)
	if status != http.StatusBadRequest || response.Error.Code != "invalid_request" {
		t.Fatalf("over-cap batch response=%+v, want 400 invalid_request", response)
	}
	if len(response.Receipts) != 0 {
		t.Fatalf("over-cap batch response receipts=%d, want 0", len(response.Receipts))
	}
	if !strings.Contains(response.Error.Message, fmt.Sprintf("max %d", domain.MaxBatchEvents)) {
		t.Fatalf("over-cap batch message=%q does not name max %d", response.Error.Message, domain.MaxBatchEvents)
	}
	assertHTTPBatchEventsAbsent(t, st, overEvents)

	boundary := make([]domain.Event, domain.MaxBatchEvents)
	for i := range boundary {
		boundary[i] = httpCapEvent("http-boundary-batch", i)
	}
	body, err := json.Marshal(map[string]any{"events": boundary})
	if err != nil {
		t.Fatal(err)
	}
	if len(body) >= domain.MaxEventBytes*2 {
		t.Fatalf("boundary fixture is %d bytes, must stay below aggregate cap %d", len(body), domain.MaxEventBytes*2)
	}
	status, response = postHTTPBatch(t, server.URL, boundary)
	if status != http.StatusAccepted {
		t.Fatalf("exact-boundary batch status=%d code=%q message=%q, want 202", status, response.Error.Code, response.Error.Message)
	}
	if len(response.Receipts) != domain.MaxBatchEvents {
		t.Fatalf("exact-boundary response receipts=%d, want %d", len(response.Receipts), domain.MaxBatchEvents)
	}
	assertHTTPBatchReceiptsPresent(t, st, boundary)
}

// TestHTTPBatchMemberCapsPreservePartialStatus keeps ordinary member failure
// semantics unchanged: a valid prefix commits, the cap-violating member is
// not ingested, and later members are not attempted. Only the count preflight
// above is atomic for the whole batch.
func TestHTTPBatchMemberCapsPreservePartialStatus(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()

	valid := httpCapEvent("http-member-valid", 0)
	over := httpCapEvent("http-member-over", 1)
	over.Reason = strings.Repeat("r", domain.MaxEnvelopeFieldBytes+1)
	tail := httpCapEvent("http-member-tail", 2)
	status, response := postHTTPBatch(t, server.URL, []domain.Event{valid, over, tail})
	if status != http.StatusBadRequest || response.Error.Code != "invalid_request" {
		t.Fatalf("member-cap response=%+v, want 400 invalid_request", response)
	}
	if len(response.Receipts) != 2 {
		t.Fatalf("member-cap receipts=%d, want valid prefix plus zero failed receipt", len(response.Receipts))
	}
	if response.Receipts[0].EventID != valid.EventID || response.Receipts[1].EventID != "" {
		t.Fatalf("member-cap receipts=%+v, want accepted prefix and zero failed receipt", response.Receipts)
	}
	assertHTTPBatchReceiptsPresent(t, st, []domain.Event{valid})
	assertHTTPBatchEventsAbsent(t, st, []domain.Event{over, tail})
}

func httpCapEvent(prefix string, index int) domain.Event {
	return domain.Event{
		EventID: fmt.Sprintf("%s-%03d", prefix, index), SourceSystem: "crm",
		EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1,
		OccurredAt: time.Unix(1_700_000_010+int64(index), 0).UTC(),
		Actor:      domain.Actor{ID: "service"}, Action: "write", Outcome: "success",
		DataClassification: "internal", RetentionClass: "standard",
		IdempotencyKey: fmt.Sprintf("%s-idem-%03d", prefix, index),
		Payload:        map[string]any{"value": index},
	}
}

type httpBatchResponse struct {
	Receipts []domain.EventReceipt `json:"receipts"`
	Error    struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Error   struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	} `json:"error"`
}

func postHTTPBatch(t *testing.T, baseURL string, events []domain.Event) (int, httpBatchResponse) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"events": events})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, baseURL+"/api/v1/events:batch", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var envelope httpBatchResponse
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Code == "" {
		envelope.Error.Code = envelope.Error.Error.Code
		envelope.Error.Message = envelope.Error.Error.Message
	}
	return response.StatusCode, envelope
}

func assertHTTPBatchEventsAbsent(t *testing.T, st *store.Store, events []domain.Event) {
	t.Helper()
	if err := st.Read(func(data *store.Snapshot) error {
		for _, event := range events {
			key := store.EventKey("tenant-a", event.EventID)
			if _, exists := data.Events[key]; exists {
				t.Fatalf("rejected event %q was persisted", event.EventID)
			}
			if _, exists := data.Receipts[key]; exists {
				t.Fatalf("rejected receipt %q was persisted", event.EventID)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func assertHTTPBatchReceiptsPresent(t *testing.T, st *store.Store, events []domain.Event) {
	t.Helper()
	if err := st.Read(func(data *store.Snapshot) error {
		for _, event := range events {
			if _, exists := data.Receipts[store.EventKey("tenant-a", event.EventID)]; !exists {
				t.Fatalf("accepted receipt %q was not persisted", event.EventID)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
