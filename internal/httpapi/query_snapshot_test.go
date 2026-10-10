package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestHTTPBodyQueryFixedMembershipAndAuthorization(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()
	defer st.Close()
	at := time.Unix(1700000010, 0).UTC()
	for i := 0; i < 3; i++ {
		event := domain.Event{EventID: fmt.Sprintf("snapshot-%d", i), SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: at.Add(time.Duration(i) * time.Second), Actor: domain.Actor{ID: "writer"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: fmt.Sprintf("idem-snapshot-%d", i), Payload: map[string]any{"value": i}}
		raw, _ := json.Marshal(event)
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events?wait_for=ledgered", bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 202 {
			t.Fatal(resp.StatusCode)
		}
	}
	q := domain.Query{From: at.Add(-time.Second), To: at.Add(4 * time.Second), PageSize: 1}
	send := func(token string, body any) (int, domain.QueryResult) {
		t.Helper()
		raw, _ := json.Marshal(body)
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events:query", bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.Header.Get("Cache-Control") != "no-store" {
			t.Fatal("query response can be cached")
		}
		var page domain.QueryResult
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, page
	}
	token := "dev:tenant-a:auditor"
	status, first := send(token, q)
	if status != 200 || first.Count != 3 || first.NextCursor == "" || first.Snapshot == nil {
		t.Fatalf("status=%d page=%+v", status, first)
	}
	q.Cursor = first.NextCursor
	if status, page := send("dev:tenant-b:auditor", q); status != 400 || len(page.Items) != 0 {
		t.Fatalf("status=%d page=%+v", status, page)
	}
	if status, _ := send("dev:tenant-a:service", q); status != 403 {
		t.Fatal(status)
	}
	if status, page := send(token, map[string]any{"from": q.From, "to": q.To, "cursor": q.Cursor, "page_size": 1, "tenant_id": "tenant-b"}); status != 200 || page.Items[0].TenantID != "tenant-a" {
		t.Fatalf("status=%d page=%+v", status, page)
	}
	// The GET compatibility path can resume the same fixed membership. Method
	// is not authority and a body cannot alter the signed tenant selector.
	params := url.Values{"from": {q.From.Format(time.RFC3339Nano)}, "to": {q.To.Format(time.RFC3339Nano)}, "page_size": {"1"}, "cursor": {q.Cursor}}
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/events?"+params.Encode(), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var page domain.QueryResult
	if json.Unmarshal(raw, &page) != nil || resp.StatusCode != 200 || page.Count != 3 || len(page.Items) != 1 || page.Items[0].EventID != "snapshot-1" {
		t.Fatalf("status=%d body=%s", resp.StatusCode, raw)
	}
	if st.OperationDeadlineExhaustions() != 0 {
		t.Fatal("ordinary query exhausted operation deadline")
	}
}

func TestQuerySnapshotArtifactUnavailableDoesNotResumeLive(t *testing.T) {
	st, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.ReadQuerySnapshot(testCtx, "tenant-a", "00000000000000000000000000000000", time.Now()); err == nil {
		t.Fatal("missing membership accepted")
	}
}
