package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

// errorEnvelope is the stable transport error contract: the status is
// authoritative and messages below 500 carry the domain error text.
type errorEnvelope struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// postExport creates an export over [from,to] and polls it to completion.
func postExport(t *testing.T, serverURL, token string, query domain.Query) domain.ExportJob {
	t.Helper()
	body, err := json.Marshal(query)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, serverURL+"/api/v1/exports", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusAccepted {
		t.Fatalf("export creation failed: status=%d err=%v", response.StatusCode, err)
	}
	var job domain.ExportJob
	if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		request, err := http.NewRequest(http.MethodGet, serverURL+"/api/v1/exports/"+job.ID, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
			response.Body.Close()
			t.Fatal(err)
		}
		response.Body.Close()
		if job.Status == "completed" || job.Status == "failed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if job.Status != "completed" {
		t.Fatalf("export did not complete: %+v", job)
	}
	return job
}

// TestHTTPExportHold403BodyLeaksNothingCrossTenant pins the transport
// contract: a held export returns 403 with the stable error envelope whose
// message is exactly "export blocked by active legal hold <own hold ID>",
// nothing is streamed, and a cross-tenant probe gets 404 (existence masked,
// never a hold hint).
func TestHTTPExportHold403BodyLeaksNothingCrossTenant(t *testing.T) {
	server, st := testHTTPServerWithStore(t)
	defer server.Close()

	event := domain.Event{EventID: "hold-event", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), Actor: domain.Actor{ID: "user-1"}, Action: "read", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "hold-idem", Payload: map[string]any{"resource": "invoice"}}
	body, _ := json.Marshal(event)
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err := http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest failed: status=%d err=%v", response.StatusCode, err)
	}
	response.Body.Close()

	job := postExport(t, server.URL, "dev:tenant-a:compliance", domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_020, 0).UTC()})

	holdBody, _ := json.Marshal(domain.LegalHold{Name: "case-http", Reason: "http litigation hold", Filter: domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_020, 0).UTC()}})
	request, _ = http.NewRequest(http.MethodPost, server.URL+"/api/v1/legal-holds", bytes.NewReader(holdBody))
	request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusCreated {
		t.Fatalf("hold creation failed: status=%d err=%v", response.StatusCode, err)
	}
	var hold domain.LegalHold
	if err := json.NewDecoder(response.Body).Decode(&hold); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if hold.ID == "" {
		t.Fatal("hold creation must return an ID")
	}

	// GET job: 403, exact message with the caller's own hold ID.
	request, _ = http.NewRequest(http.MethodGet, server.URL+"/api/v1/exports/"+job.ID, nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("GET export under hold: status=%d err=%v", response.StatusCode, err)
	}
	var envelope errorEnvelope
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if envelope.Error.Code != "forbidden" {
		t.Fatalf("error code=%q, want forbidden", envelope.Error.Code)
	}
	if want := "export blocked by active legal hold " + hold.ID; envelope.Error.Message != want {
		t.Fatalf("403 message=%q, want exactly %q", envelope.Error.Message, want)
	}

	// Download: 403 before any streaming (no ndjson content type; the body is
	// the stable error envelope, never sealed content).
	request, _ = http.NewRequest(http.MethodGet, server.URL+"/api/v1/exports/"+job.ID+"/download", nil)
	request.Header.Set("Authorization", "Bearer dev:tenant-a:compliance")
	response, err = http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("download under hold: status=%d err=%v", response.StatusCode, err)
	}
	if ct := response.Header.Get("Content-Type"); strings.Contains(ct, "x-ndjson") {
		t.Fatalf("blocked download must not stream sealed content, content-type=%q", ct)
	}
	var denied errorEnvelope
	if err := json.NewDecoder(response.Body).Decode(&denied); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if denied.Error.Code != "forbidden" || !strings.Contains(denied.Error.Message, hold.ID) {
		t.Fatalf("blocked download error envelope=%+v", denied.Error)
	}

	// Cross-tenant probes: 404 (existence masked), never 403.
	for _, path := range []string{"/api/v1/exports/" + job.ID, "/api/v1/exports/" + job.ID + "/download"} {
		request, _ := http.NewRequest(http.MethodGet, server.URL+path, nil)
		request.Header.Set("Authorization", "Bearer dev:tenant-b:compliance")
		response, err := http.DefaultClient.Do(request)
		if err != nil || response.StatusCode != http.StatusNotFound {
			t.Fatalf("cross-tenant %s: status=%d err=%v, want 404", path, response.StatusCode, err)
		}
		response.Body.Close()
	}

	// At the HTTP boundary the denial happens in GetExport, which is
	// read-only: no export.blocked fact, and the sealed object is never
	// streamed, so no audit.event.export fact for the denied attempt.
	// (The download-denial fact path is pinned at the service layer by
	// TestExportAccessBlockedByHoldCreatedAfterCompletion, which calls
	// RecordExportDownload directly.)
	snap, err := st.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	blocked, streamed := 0, 0
	for _, a := range snap.AdminActions {
		if a.TenantID != "tenant-a" {
			continue
		}
		if a.Action == domain.AdminActionExportBlocked && a.TargetID == hold.ID {
			blocked++
		}
		if a.Action == domain.AdminActionEventExport && a.TargetID == job.ID {
			streamed++
		}
	}
	if blocked != 0 {
		t.Fatalf("HTTP GetExport denials are read-only and must record no export.blocked fact, found %d", blocked)
	}
	if streamed != 0 {
		t.Fatalf("a denied download must never stream nor append audit.event.export, found %d", streamed)
	}
}
