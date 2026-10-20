package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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

// Preparation and post-SIGTERM exit have separate guards. Both retain the
// original 10s ceiling; neither changes a production timeout.
const workerStartupGuard = 10 * time.Second

type workerChildOutput struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *workerChildOutput) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *workerChildOutput) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type workerChild struct {
	cmd    *exec.Cmd
	output workerChildOutput
	done   chan struct{}
	err    error // published by closing done; Wait is called exactly once
}

func startArchiveWorker(t *testing.T, endpoint, bucket string) (*workerChild, string) {
	t.Helper()
	binary, err := buildWorkerBinary()
	if err != nil {
		t.Fatalf("build worker: %v", err)
	}
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	seedHungArchiveState(t, statePath)
	// Materialize the real v2 fixture before starting the subprocess guard.
	// Legacy migration/fsync is preparation, not archive-cancellation latency.
	prepared, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary,
		"-once", "-state", statePath, "-archive", filepath.Join(dir, "archive"),
		"-s3-endpoint", strings.TrimPrefix(endpoint, "http://"), "-s3-bucket", bucket,
		"-archive-retention-days", "365",
	)
	cmd.Env = append(cleanAuditEnvironment(),
		"AUDIT_SIGNING_SECRET=signing-secret-0123456789abcdef0123456789abcdef",
		"AUDIT_ENCRYPTION_KEY=encryption-key-0123456789abcdef0123456789abcdef",
		"AUDIT_S3_ACCESS_KEY=access", "AUDIT_S3_SECRET_KEY=secret",
	)
	child := &workerChild{cmd: cmd, done: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = &child.output, &child.output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		child.err = cmd.Wait()
		close(child.done)
	}()
	t.Cleanup(func() {
		select {
		case <-child.done:
		default:
			_ = cmd.Process.Kill()
			<-child.done
		}
	})
	return child, statePath
}

func (c *workerChild) awaitBoundary(t *testing.T, ready <-chan struct{}, boundary string) {
	t.Helper()
	timer := time.NewTimer(workerStartupGuard)
	defer timer.Stop()
	select {
	case <-ready:
	case <-c.done:
		t.Fatalf("worker exited before %s: %v; output=%q", boundary, c.err, c.output.String())
	case <-timer.C:
		t.Fatalf("worker never reached %s; output=%q", boundary, c.output.String())
	}
}

func (c *workerChild) awaitCleanExit(t *testing.T) {
	t.Helper()
	select {
	case <-c.done:
		if c.err != nil {
			t.Fatalf("worker did not exit cleanly after SIGTERM: %v; output=%q", c.err, c.output.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("worker did not exit within 10s after SIGTERM; output=%q", c.output.String())
	}
}

type startupProbeEndpoint struct {
	hungS3Endpoint
	entered    chan struct{}
	cancelled  chan struct{}
	once       sync.Once
	cancelOnce sync.Once
}

func (s *startupProbeEndpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodHead && strings.TrimSuffix(r.URL.Path, "/") == "/"+s.bucket {
		s.once.Do(func() { close(s.entered) })
		<-r.Context().Done()
		s.cancelOnce.Do(func() { close(s.cancelled) })
		return
	}
	s.hungS3Endpoint.ServeHTTP(w, r)
}

// The real file lock is held while startup Ready is blocked, before the old
// late signal registration. SIGTERM must cancel that socket, close the store,
// and exit cleanly without ever starting a governance pass.
func TestSigtermDuringStartupProbeReleasesStateLock(t *testing.T) {
	endpoint := &startupProbeEndpoint{
		hungS3Endpoint: hungS3Endpoint{bucket: "worm-audit", seen: make(chan struct{})},
		entered:        make(chan struct{}), cancelled: make(chan struct{}),
	}
	server := httptest.NewServer(endpoint)
	t.Cleanup(server.Close)
	child, statePath := startArchiveWorker(t, server.URL, endpoint.bucket)
	child.awaitBoundary(t, endpoint.entered, "startup bucket HEAD")
	if probe, err := store.Open(statePath); err == nil {
		_ = probe.Close()
		t.Fatal("startup fixture did not hold the state lock")
	} else if !errors.Is(err, store.ErrStateFileLocked) {
		t.Fatalf("unexpected state lock error: %v", err)
	}
	if err := child.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	child.awaitCleanExit(t)
	select {
	case <-endpoint.cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("startup Ready socket did not observe cancellation")
	}
	if strings.Contains(child.output.String(), "tenant=tenant-a") {
		t.Fatal("cancelled startup ran a governance pass")
	}
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatalf("startup SIGTERM left the state lock held: %v", err)
	}
	defer st.Close()
	if err := st.Read(context.Background(), func(data *store.Snapshot) error {
		key := store.EventKey("tenant-a", "evt-hung-archive")
		if len(data.Events) != 1 || len(data.Receipts) != 1 || len(data.Exports) != 0 || len(data.AdminActions) != 0 ||
			data.Receipts[key].Status != domain.StatusIndexed || !data.Receipts[key].ArchivedAt.IsZero() {
			t.Fatal("cancelled startup mutated the seeded evidence or governance state")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
