package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestSnaplinkConsoleAuditReadCompatibility(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	base := time.Unix(1_700_000_010, 0).UTC()
	for index, outcome := range []string{"success", "failure", "success"} {
		event := domain.Event{
			EventID: "console-event-" + string(rune('1'+index)), SourceSystem: "crm",
			EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1,
			OccurredAt: base.Add(time.Duration(index) * time.Second), Actor: domain.Actor{ID: "admin"},
			Action: "read", Outcome: outcome, DataClassification: "internal",
			RetentionClass: "standard", IdempotencyKey: "console-idem-" + string(rune('1'+index)),
			Payload: map[string]any{"index": index},
		}
		if got := postTestEvent(t, server.URL, "dev:tenant-a:service:crm", event); got.Status != http.StatusAccepted {
			t.Fatalf("ingest %d: %+v", index, got)
		}
	}

	var page struct {
		Events     []domain.Event `json:"events"`
		Count      int            `json:"count"`
		NextCursor string         `json:"next_cursor"`
	}
	consoleGET(t, server.URL+"/api/v1/compat/snaplink/audit/events?limit=2", &page)
	if page.Count != 3 || len(page.Events) != 2 || page.Events[0].EventID != "console-event-3" ||
		page.Events[1].EventID != "console-event-2" || page.NextCursor == "" {
		t.Fatalf("newest-first page = %+v", page)
	}

	var facets struct {
		Facets domain.EventFacets `json:"facets"`
	}
	consoleGET(t, server.URL+"/api/v1/compat/snaplink/audit/facets", &facets)
	if facets.Facets.Total != 3 || facets.Facets.Outcomes["success"] != 2 ||
		facets.Facets.Outcomes["failure"] != 1 || facets.Facets.Clients["crm"] != 3 {
		t.Fatalf("facets = %+v", facets.Facets)
	}

	var detail domain.Event
	consoleGET(t, server.URL+"/api/v1/compat/snaplink/audit/events/console-event-2", &detail)
	if detail.EventID != "console-event-2" || detail.TenantID != "tenant-a" {
		t.Fatalf("detail = %+v", detail)
	}
}

func consoleGET(t *testing.T, url string, target any) {
	t.Helper()
	request, _ := http.NewRequest(http.MethodGet, url, nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:auditor")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("GET %s: status=%d body=%s", url, response.StatusCode, body)
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatal(err)
	}
}
