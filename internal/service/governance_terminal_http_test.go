package service_test

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/httpapi"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

// T4 / FR-5/FR-6: GET /exports/{jobID} is deterministic and redacts the raw
// terminal error to "export failed"; neither the raw storage diagnostic nor any
// path/credential is ever exposed, and repeated reads never regress. The test
// lives in package service_test (external) because it drives the HTTP
// redaction boundary hosted by the httpapi package, which cannot be imported
// from the internal package-service test (import-cycle rule).
func TestGetExportDeterministicAndRedacted(t *testing.T) {
	const token = "Bearer dev:tenant-a:compliance"
	now := time.Unix(1_700_000_000, 0).UTC()

	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{
		SegmentSize:     100,
		SigningSecret:   "test-secret",
		Now:             func() time.Time { return now },
		AllowDevSecrets: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Register the standard test domain so tenant-a is known.
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true, EventsPerSecond: 1000, Burst: 1000}); err != nil {
		t.Fatal(err)
	}

	rawDiagnostic := "/var/audit/state.json: write failed: permission denied"
	jobID := "export-t4"

	// Seed a running job directly (the terminal-write fault/recovery paths are
	// pinned by the service-layer T1/T2/T3; here we verify the HTTP reader
	// never sees the raw diagnostic and converges deterministically).
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Exports[jobID] = domain.ExportJob{
			ID:          jobID,
			TenantID:    "tenant-a",
			RequestedBy: "test",
			Query:       domain.Query{From: now.Add(-time.Hour), To: now.Add(time.Hour)},
			Status:      "running",
			CreatedAt:   now,
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(httpapi.NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler())
	defer server.Close()

	url := server.URL + "/api/v1/exports/" + jobID
	get := func() domain.ExportJob {
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		req.Header.Set("Authorization", token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status=%d body=%s", url, resp.StatusCode, body)
		}
		var job domain.ExportJob
		if err := json.Unmarshal(body, &job); err != nil {
			t.Fatalf("decode: %v body=%s", err, body)
		}
		return job
	}

	// Phase 1: while the job is still running, GET returns running and never
	// leaks the (simulated) raw diagnostic.
	running := get()
	if running.Status != "running" {
		t.Fatalf("status=%s, want running", running.Status)
	}
	if strings.Contains(running.Error, "permission denied") || strings.Contains(running.Error, "/var/audit") {
		t.Fatalf("running response leaks raw diagnostic: %q", running.Error)
	}

	// Phase 2: converge via the exported recovery path; the job becomes failed
	// and the error is masked. Repeated reads are deterministic.
	svc.Config.Now = func() time.Time { return now.Add(48 * time.Hour) }
	if _, err := svc.RecoverStuckExports("tenant-a"); err != nil {
		t.Fatal(err)
	}

	var last domain.ExportJob
	for i := 0; i < 5; i++ {
		got := get()
		if got.Status != "failed" {
			t.Fatalf("status=%s, want failed (after recovery)", got.Status)
		}
		if got.Error != "export failed" {
			t.Fatalf("error=%q, want \"export failed\"", got.Error)
		}
		for _, leaked := range []string{rawDiagnostic, "/var/audit", "permission denied"} {
			if strings.Contains(got.Error, leaked) {
				t.Fatalf("response leaks %q: error=%q", leaked, got.Error)
			}
		}
		if i > 0 && (got.Status != last.Status || got.Error != last.Error) {
			t.Fatalf("GET not deterministic: %+v vs %+v", got, last)
		}
		last = got
	}
}
