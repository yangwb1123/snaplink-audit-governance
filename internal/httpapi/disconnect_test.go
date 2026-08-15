package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
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

// blockingHTTPsigner is the httpapi-boundary stand-in for an in-flight Vault
// call. It reports when the ingest's seal actually reached Sign (entered),
// then blocks until either the caller releases it or its context is
// cancelled — mirroring the real signer, whose http request is bound to the
// context. sawCancel fires if (and only if) the context was cancelled while
// the call was in flight: the F1 fix (context.WithoutCancel at the ingest
// boundary) must keep it silent even though the client disconnected
// mid-request.
type blockingHTTPsigner struct {
	service.Signer
	entered        chan struct{}
	release        chan struct{}
	releasedCtxErr chan error
	sawCancel      chan struct{}
}

func (b *blockingHTTPsigner) Sign(ctx context.Context, data []byte) (string, error) {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		close(b.sawCancel)
		return "", ctx.Err()
	case <-b.release:
		b.releasedCtxErr <- ctx.Err()
		return b.Signer.Sign(ctx, data)
	}
}

// TestHTTPIngestClientDisconnectKeepsEvent pins async-review F1 at the HTTP
// boundary: a client that disconnects mid-ingest must NOT roll back the
// durable write. The ingest commit is detached from r.Context() cancellation
// (values kept), so the seal's signer context stays live — the event is
// ledgered even though the response never reaches the client. Before the F1
// fix (r.Context() threaded straight through), the disconnect cancelled the
// signer, the atomic Update aborted, and the event was silently dropped.
func TestHTTPIngestClientDisconnectKeepsEvent(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{
		ArchiveDir:      filepath.Join(t.TempDir(), "archive"),
		SegmentSize:     2, // the second event seals, i.e. signs, inside ingest
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
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}}); err != nil {
		t.Fatal(err)
	}
	signer := &blockingHTTPsigner{
		Signer:         svc.Config.Signer,
		entered:        make(chan struct{}, 1),
		release:        make(chan struct{}),
		releasedCtxErr: make(chan error, 1),
		sawCancel:      make(chan struct{}),
	}
	svc.Config.Signer = signer
	server := httptest.NewServer(NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, log.New(io.Discard, "", 0)).Handler())
	defer server.Close()

	// Event 1 establishes the stream with one pending hash (no seal).
	event1 := domain.Event{EventID: "http-disc-1", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_010, 0).UTC(), OperationID: "op-disc", Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "disc-1", Payload: map[string]any{"resource": "r1"}}
	body1, _ := json.Marshal(event1)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/events?wait_for=ledgered", strings.NewReader(string(body1)))
	req.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("event 1 status=%d", resp.StatusCode)
	}

	// Event 2 crosses SegmentSize=2: the seal calls Sign, which blocks. We
	// write the request over a raw TCP connection, wait until the signer is
	// entered, then close the connection — the client disappears mid-ingest.
	event2 := domain.Event{EventID: "http-disc-2", SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_011, 0).UTC(), OperationID: "op-disc", Actor: domain.Actor{ID: "user-1"}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "disc-2", Payload: map[string]any{"resource": "r2"}}
	body2, _ := json.Marshal(event2)
	addr := strings.TrimPrefix(server.URL, "http://")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	request := fmt.Sprintf("POST /api/v1/events?wait_for=ledgered HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer dev:tenant-a:service:crm\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", addr, len(body2), body2)
	if _, err := conn.Write([]byte(request)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-signer.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("seal signer was never invoked")
	}
	// Client disconnects mid-request. The server observes the EOF
	// asynchronously; with the F1 fix the commit context is detached, so the
	// signer stays blocked on release and sawCancel stays silent. Without the
	// fix (r.Context() threaded through), the disconnect cancels the commit
	// context and the signer aborts the atomic write — the event would be
	// silently dropped.
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-signer.sawCancel:
		t.Fatal("client disconnect cancelled the ingest commit: the event would be silently dropped")
	case <-time.After(500 * time.Millisecond):
		// The commit context survived the disconnect.
	}
	close(signer.release)
	// The commit context must still be live at release time: ctx.Err() is nil
	// only if the ingest used context.WithoutCancel.
	if err := <-signer.releasedCtxErr; err != nil {
		t.Fatalf("ingest commit context was cancelled by client disconnect: %v", err)
	}
	// The event must be durably ledgered despite the lost response.
	deadline := time.Now().Add(2 * time.Second)
	for {
		snap, snapErr := st.Snapshot()
		if snapErr != nil {
			t.Fatal(snapErr)
		}
		if _, ok := snap.Events[store.EventKey("tenant-a", "http-disc-2")]; ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("event was silently dropped by the client disconnect")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestHTTPRequestIDTruncated pins security-review F-3: the client-supplied
// X-Request-ID is correlation-only, so an unbounded value must never be
// echoed wholesale into response headers or error bodies. A 10 KB ID on an
// unauthenticated request yields a 401 whose echoed request_id is capped at
// maxRequestIDBytes.
func TestHTTPRequestIDTruncated(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()
	longID := strings.Repeat("x", 10*1024)
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/events", nil)
	req.Header.Set("X-Request-ID", longID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d, want 401", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Request-ID"); len(got) > maxRequestIDBytes {
		t.Fatalf("response header X-Request-ID length=%d, want <= %d", len(got), maxRequestIDBytes)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Error struct {
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("error body decode: %v (body=%s)", err, body)
	}
	if len(decoded.Error.RequestID) > maxRequestIDBytes {
		t.Fatalf("error body request_id length=%d, want <= %d", len(decoded.Error.RequestID), maxRequestIDBytes)
	}
}
