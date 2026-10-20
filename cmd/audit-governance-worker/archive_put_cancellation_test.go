package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// TestRunEvaluatePassAbortsOnArchivePut is the worker-level AC-2 regression:
// cancellation while the first archive Put is in flight must reach the Put,
// produce one archive_error line, and let the pass return promptly.
func TestRunEvaluatePassAbortsOnArchivePut(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := workerService(t, backend)
	seedPendingReceipts(t, backend, 1)
	blocked := &blockingArchive{started: make(chan struct{}, 1)}
	svc.Config.Archive = blocked

	ctx, cancel := context.WithCancel(context.Background())
	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		runEvaluatePass(ctx, log.New(&buf, "", 0), svc)
		close(done)
	}()
	select {
	case <-blocked.started:
	case <-time.After(2 * time.Second):
		t.Fatal("archive Put never started")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runEvaluatePass did not abort promptly on archive cancellation")
	}
	if blocked.key == "" {
		t.Fatal("blocking archive did not record the event key")
	}
	if !errors.Is(blocked.observedErr, context.Canceled) {
		t.Fatalf("archive Put observed %v, want context.Canceled", blocked.observedErr)
	}
	out := buf.String()
	if n := strings.Count(out, "tenant=tenant-a archive_error="); n != 1 {
		t.Fatalf("archive_error lines=%d, want exactly one; log=%q", n, out)
	}
	if !strings.Contains(out, "context canceled") {
		t.Fatalf("archive error must identify cancellation; log=%q", out)
	}
}

type hungS3Endpoint struct {
	bucket     string
	seen       chan struct{}
	cancelled  chan struct{}
	once       sync.Once
	cancelOnce sync.Once
}

func (s *hungS3Endpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bucketPath := "/" + s.bucket
	if r.Method == http.MethodHead && r.URL.Path == bucketPath {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method == http.MethodHead && strings.HasPrefix(r.URL.Path, bucketPath+"/") && len(r.URL.Path) > len(bucketPath)+1 {
		s.once.Do(func() { close(s.seen) })
		<-r.Context().Done()
		if s.cancelled != nil {
			s.cancelOnce.Do(func() { close(s.cancelled) })
		}
		return
	}
	if _, ok := r.URL.Query()["location"]; ok {
		writeS3XML(w, `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`)
		return
	}
	if _, ok := r.URL.Query()["object-lock"]; ok {
		writeS3XML(w, `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Days>365</Days></DefaultRetention></Rule></ObjectLockConfiguration>`)
		return
	}
	if _, ok := r.URL.Query()["versioning"]; ok {
		writeS3XML(w, `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func writeS3XML(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(body))
}

func seedHungArchiveState(t *testing.T, path string) {
	t.Helper()
	snapshot := store.NewSnapshot()
	snapshot.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}
	key := store.EventKey("tenant-a", "evt-hung-archive")
	snapshot.Events[key] = domain.Event{
		EventID: "evt-hung-archive", TenantID: "tenant-a", StreamID: "stream-a", Sequence: 1,
		EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1,
		OccurredAt: time.Unix(1_700_000_000, 0).UTC(), ReceivedAt: time.Unix(1_700_000_000, 0).UTC(),
		Action: "update", Outcome: "success", RetentionClass: "other",
		Payload: map[string]any{"resource": "invoice"},
	}
	snapshot.Receipts[key] = domain.EventReceipt{
		EventID: "evt-hung-archive", TenantID: "tenant-a", Status: domain.StatusIndexed,
		StreamID: "stream-a", Sequence: 1,
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func cleanAuditEnvironment() []string {
	env := make([]string, 0, len(os.Environ())+3)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "AUDIT_") {
			env = append(env, value)
		}
	}
	return env
}

// TestSigtermDuringHungArchivePutExitsBoundedWindow exercises the real worker,
// minio-backed S3Store, signal.NotifyContext, and an in-flight StatObject.
// The endpoint answers every readiness probe but never answers the object HEAD;
// SIGTERM must cancel that request and let -once exit with the cancellation log.
func TestSigtermDuringHungArchivePutExitsBoundedWindow(t *testing.T) {
	endpoint := &hungS3Endpoint{bucket: "worm-audit", seen: make(chan struct{}), cancelled: make(chan struct{})}
	server := httptest.NewServer(endpoint)
	t.Cleanup(server.Close)
	child, statePath := startArchiveWorker(t, server.URL, endpoint.bucket)
	// Start measuring shutdown only after the actual archive socket is in
	// flight. Startup/migration are not a 10s performance requirement.
	child.awaitBoundary(t, endpoint.seen, "in-flight object HEAD")
	if err := child.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}
	child.awaitCleanExit(t) // the original post-SIGTERM 10s ceiling is unchanged
	select {
	case <-endpoint.cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("archive socket did not observe cancellation")
	}
	output := child.output.String()
	if !strings.Contains(output, "archive_error=") || !strings.Contains(output, "context canceled") {
		t.Fatalf("worker output must report the cancelled archive Put; output=%q", output)
	}
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatalf("archive SIGTERM left the state lock held: %v", err)
	}
	defer st.Close()
	if err := st.Read(context.Background(), func(data *store.Snapshot) error {
		receipt := data.Receipts[store.EventKey("tenant-a", "evt-hung-archive")]
		if receipt.Status != domain.StatusIndexed || !receipt.ArchivedAt.IsZero() {
			t.Fatal("cancelled archive was reported as committed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
