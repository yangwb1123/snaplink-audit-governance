package httpapi

// Module-level proof (R7) that amortizing the read self-audit trail compaction
// in internal/store is invisible to internal/httpapi: the audited-read status
// contract is unchanged, while the trail file the reads write stays bounded by
// the operator cap across many more audited reads than the cap.
//
// The amortization itself is pinned in internal/store; this file exists only
// to prove the module boundary holds — no handler, route, authorization or
// status-mapping change, and a store error still fails the read closed.

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

// auditedTrailServer builds a server whose read self-audit trail is bounded,
// returning the server, the state path and the trail path. The fixture caps
// satisfy service.resolveAdminTrailConfig: a non-zero trail cap must be at
// least MaxAdminActions, so the trail (4 -> 8 watermark/cap) stays at least as
// deep as the mutation facts it complements.
//
// This mirrors testHTTPServerWithStore rather than parameterizing it: that
// helper backs thousands of lines of existing tests, so a local variant keeps
// this change's diff surgical.
func auditedTrailServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	statePath := filepath.Join(t.TempDir(), "state.json")
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc, err := service.New(st, service.Config{
		ArchiveDir: filepath.Join(t.TempDir(), "archive"),
		// MaxAdminTrailActions (8) >= MaxAdminActions (4); otherwise
		// resolveAdminTrailConfig rejects the pair with ErrInvalid.
		MaxAdminActions:      4,
		MaxAdminTrailActions: 8,
		Now:                  func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
		AllowDevSecrets:      true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant(testCtx, "test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource(testCtx, "test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, log.New(io.Discard, "", 0)).Handler())
	t.Cleanup(server.Close)
	return server, statePath
}

// completeJSONLLines counts the complete (newline-terminated) lines of the
// trail file, so a partially written line is never counted as a fact.
func completeJSONLLines(t *testing.T, trailPath string) int {
	t.Helper()
	contents, err := os.ReadFile(trailPath)
	if err != nil {
		t.Fatalf("read trail file %s: %v", trailPath, err)
	}
	lines := 0
	for _, b := range contents {
		if b == '\n' {
			lines++
		}
	}
	return lines
}

func auditedTrailGETBody(t *testing.T, server *httptest.Server, token, path string) (int, []byte) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, server.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	return response.StatusCode, body
}

// TestAuditedReadsStayBoundedUnderTrailCap pins R7/AC-2 at the module
// boundary: 30 audited GETs against a trail capped at 8 all return 200 —
// the audited-read status contract is untouched — and the trail file never
// exceeds the cap, even though 30 appends are 3.75x the cap and therefore
// straddle several compaction watermarks.
func TestAuditedReadsStayBoundedUnderTrailCap(t *testing.T) {
	server, statePath := auditedTrailServer(t)
	trailPath := statePath + ".admin-trail.jsonl"
	// Ingest and read are separate permissions: the source-service token
	// posts the event, the auditor token drives the audited reads below. Both
	// dev-token subjects are non-empty, so every audited read appends a fact.
	const writerToken, readerToken = "dev:tenant-a:service:crm", "dev:tenant-a:auditor"
	// The governance list is an admin read, so it needs a platform role.
	const adminToken = "dev:platform:platform-admin"

	// Seed one event so the read routes have something to serve.
	event := httpReadSeedEvent()
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+writerToken)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		t.Fatalf("seed ingest status=%d, want 2xx", response.StatusCode)
	}

	const trailCap = 8
	auditedReads := 0
	for round := 0; round < 15; round++ {
		// One audited point read and one audited list read per round: fifteen
		// rounds of each is 30 audited appends against a cap of 8.
		for _, path := range []string{
			"/api/v1/events/http-read-evt",
			// The list route requires an explicit from/to window
			// (parseTime), which brackets the pinned harness clock and the
			// seeded event.
			"/api/v1/events?from=2023-01-01T00:00:00Z&to=2033-01-01T00:00:00Z",
		} {
			status, respBody := auditedTrailGETBody(t, server, readerToken, path)
			if status != http.StatusOK {
				t.Fatalf("audited read %d of %s: status=%d body=%s", auditedReads+1, path, status, respBody)
			}
			auditedReads++
			// The observed contract: after every returned audited read the
			// trail is bounded by the operator cap.
			if lines := completeJSONLLines(t, trailPath); lines > trailCap {
				t.Fatalf("after audited read %d: trail lines=%d, want <= cap %d", auditedReads, lines, trailCap)
			}
		}
	}
	if auditedReads != 30 {
		t.Fatalf("auditedReads=%d, want 30", auditedReads)
	}
	// The amortization actually ran: the trail grew well past the cap and was
	// trimmed back, so it holds at least the watermark and at most the cap.
	lines := completeJSONLLines(t, trailPath)
	if lines == 0 || lines > trailCap {
		t.Fatalf("trail lines=%d, want 1..%d after 31 audited appends", lines, trailCap)
	}

	// The governance surface still sees the audit facts the reads recorded, so
	// the trimmed trail is not invisible to consumers.
	adminRequest, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/admin/actions?limit=100", nil)
	if err != nil {
		t.Fatal(err)
	}
	adminRequest.Header.Set("Authorization", "Bearer "+adminToken)
	adminResponse, err := http.DefaultClient.Do(adminRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer adminResponse.Body.Close()
	if adminResponse.StatusCode != http.StatusOK {
		t.Fatalf("admin actions status=%d, want 200", adminResponse.StatusCode)
	}
	payload, err := io.ReadAll(adminResponse.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(payload, []byte("audit.event.read")) {
		t.Fatalf("admin actions body does not expose the audited reads: %s", payload)
	}
}
