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
	"os/exec"
	"path/filepath"
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
	bucket string
	seen   chan struct{}
	once   sync.Once
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
	binary, err := buildWorkerBinary()
	if err != nil {
		t.Skipf("worker binary unavailable: %v", err)
	}
	endpoint := &hungS3Endpoint{bucket: "worm-audit", seen: make(chan struct{})}
	server := httptest.NewServer(endpoint)
	defer server.Close()

	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	seedHungArchiveState(t, statePath)
	cmd := exec.Command(binary,
		"-once", "-state", statePath, "-archive", filepath.Join(dir, "archive"),
		"-s3-endpoint", strings.TrimPrefix(server.URL, "http://"), "-s3-bucket", endpoint.bucket,
		"-s3-access-key", "access", "-s3-secret-key", "secret", "-archive-retention-days", "365",
	)
	cmd.Env = append(cleanAuditEnvironment(),
		"AUDIT_SIGNING_SECRET=signing-secret-0123456789abcdef0123456789abcdef",
		"AUDIT_ENCRYPTION_KEY=encryption-key-0123456789abcdef0123456789abcdef",
	)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()

	select {
	case <-endpoint.seen:
	case <-time.After(10 * time.Second):
		t.Fatal("worker never reached the in-flight object HEAD")
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	select {
	case err := <-wait:
		waited = true
		if err != nil {
			t.Fatalf("worker did not exit cleanly after SIGTERM: %v; output=%q", err, output.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not exit within 10s after SIGTERM")
	}
	if !strings.Contains(output.String(), "archive_error=") || !strings.Contains(output.String(), "context canceled") {
		t.Fatalf("worker output must report the cancelled archive Put; output=%q", output.String())
	}
}
