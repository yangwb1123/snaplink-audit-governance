package main

// The end-to-end proof of the bounded drain: the real audit-api binary, the
// real state file lock, a real idle gRPC ingest stream, and a real SIGTERM.
// Pre-fix this test never completes — the process only ends by an external
// SIGKILL, and the state flock is never released deterministically.

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// childStoreOperationTimeout is the knob the process test sets. It is the
// drain window too, so the whole exit sequence finishes in about a second and
// the test can assert on the derivation rather than on a fixed sleep.
const childStoreOperationTimeout = time.Second

// syncBuffer is a concurrency-safe capture of a child process's combined
// output: the readiness gate polls it while the child is still writing.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// freeLoopbackAddr reserves and releases a loopback port so the child can bind
// one without the non-loopback plaintext gate rejecting it.
func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// signDrainJWT mints a locally-signable HS256 access token. There is no
// exported mint helper anywhere in the repository, so the fixture owns the
// three-line JWS compact serialization it needs.
func signDrainJWT(t *testing.T, secret string, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]any{"alg": "HS256", "typ": "at+jwt"})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	signed := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(body)
	mac := hmac.New(sha256.New, []byte(secret))
	if _, err := mac.Write([]byte(signed)); err != nil {
		t.Fatal(err)
	}
	return signed + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// farFutureJWT is the production-shaped hang: a long-lived signed token whose
// effective expiry is years away, so the stream receive parks on a
// ~70-year timer rather than on the zero-expiry dev branch. Both shapes must
// be bounded, so both are exercised.
func farFutureJWT(t *testing.T) string {
	t.Helper()
	return signDrainJWT(t, testJWTSecret, map[string]any{
		"sub":       "demo",
		"tenant_id": "demo",
		"client_id": "demo",
		"roles":     []string{"service"},
		"exp":       4_100_000_000,
	})
}

func drainChildEvent(eventID string) *auditv1.EventEnvelope {
	return &auditv1.EventEnvelope{
		EventId:            eventID,
		SourceSystem:       "demo",
		EventType:          "audit.event",
		SchemaId:           "audit.event",
		SchemaVersion:      1,
		OccurredAt:         timestamppb.New(time.Now().UTC()),
		Actor:              &auditv1.Actor{Id: "service"},
		Action:             "write",
		Outcome:            "success",
		PayloadJson:        []byte(`{"value":1}`),
		DataClassification: "internal",
		RetentionClass:     "standard",
		IdempotencyKey:     eventID + "-idem",
	}
}

// TestSIGTERMDrainsIdleWriteStreamAndReleasesStateLock is AC-4: with an
// authenticated gRPC ingest stream left open, SIGTERM must still terminate the
// process within the derived drain bound, force-stop the stream, and release
// the state file lock — and the paired control proves the force path is not
// reported unconditionally.
func TestSIGTERMDrainsIdleWriteStreamAndReleasesStateLock(t *testing.T) {
	binary, err := buildAPIBinary()
	if err != nil {
		t.Skipf("api binary unavailable: %v", err)
	}
	cases := []struct {
		name       string
		token      func(t *testing.T) string
		idleStream bool
		wantDrain  string
	}{
		{name: "dev token parks with no timer", token: func(*testing.T) string { return "dev:demo:service:demo" }, idleStream: true, wantDrain: "force_stopped"},
		{name: "far-future JWT parks on the expiry timer", token: farFutureJWT, idleStream: true, wantDrain: "force_stopped"},
		{name: "control: no idle stream drains cleanly", token: func(*testing.T) string { return "dev:demo:service:demo" }, idleStream: false, wantDrain: "drained"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runSIGTERMDrainCase(t, binary, tc.token(t), tc.idleStream, tc.wantDrain)
		})
	}
}

// runSIGTERMDrainCase owns one child process from startup to SIGTERM.
func runSIGTERMDrainCase(t *testing.T, binary, token string, idleStream bool, wantDrain string) {
	t.Helper()
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	archiveDir := filepath.Join(dir, "archive")
	eventID := "evt-sigterm-drain"
	request := &auditv1.WriteRequest{Event: drainChildEvent(eventID), WaitFor: "ledgered"}
	seededReceipt, err := prepareDrainFileState(statePath, drainFileConfig(archiveDir), request)
	if err != nil {
		t.Fatal("could not finish the original authenticated file commit before drain startup")
	}
	httpAddr := freeLoopbackAddr(t)
	grpcAddr := freeLoopbackAddr(t)

	cmd := exec.Command(binary,
		"-listen", httpAddr,
		"-grpc-listen", grpcAddr,
		"-state", statePath,
		"-archive", archiveDir,
		"-bootstrap-tenant", "demo",
		"-allow-dev-auth",
	)
	// An explicit environment (rather than os.Environ()) keeps the child
	// hermetic: an ambient AUDIT_POSTGRES_DSN or AUDIT_LEDGERED_BROKERS would
	// silently bypass the file backend this test asserts the lock of.
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"GOMAXPROCS=2",
		"AUDIT_SIGNING_SECRET=" + drainChildSigningSecret,
		"AUDIT_ENCRYPTION_KEY=" + drainChildEncryptionKey,
		"AUDIT_JWT_SECRET=" + testJWTSecret,
		"AUDIT_ALLOW_LOCAL_HS256=true",
		"AUDIT_ALLOW_DEV_AUTH=true",
		"AUDIT_STORE_OPERATION_TIMEOUT=" + childStoreOperationTimeout.String(),
		"AUDIT_LEDGERED_BROKERS=",
		"AUDIT_POSTGRES_DSN=",
		"AUDIT_OTLP_ENDPOINT=",
	}
	var output syncBuffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var waitErr error
	done := make(chan struct{})
	go func() {
		waitErr = cmd.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		select {
		case <-done:
		default:
			_ = cmd.Process.Kill()
			<-done
		}
	})

	// Readiness: the listener is already bound when grpc_listen= is printed, so
	// this gate is not racy.
	waitForChildLine(t, &output, "grpc_listen=", 30*time.Second)

	// Lock oracle: while the child is alive the state file is exclusively
	// locked, which is what the drain must eventually release.
	if probe, err := store.Open(statePath); err == nil {
		_ = probe.Close()
		t.Fatal("the live child did not hold the state file lock; the drain assertion would be vacuous")
	} else if !errors.Is(err, store.ErrStateFileLocked) {
		t.Fatalf("second store.Open error=%v, want ErrStateFileLocked", err)
	}

	connection, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := auditv1.NewIngestClient(connection)
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))

	// The original new Write is already durable in this exact file ledger.
	// Make the measured child authenticate the same request and acknowledge
	// its duplicate before SIGTERM. New file/archival commits are preparation,
	// not a 1s drain performance SLA; auth and the real parked stream remain.
	preparation, cancelPreparation := context.WithTimeout(ctx, 30*time.Second)
	defer cancelPreparation()
	preparedReceipt, release, err := prepareDrainIngest(preparation, client, request, idleStream)
	if err != nil {
		t.Fatalf("prepare drain exchange err=%v\nchild output:\n%s", err, output.String())
	}
	t.Cleanup(release)
	cancelPreparation() // must not cancel the successfully parked stream
	if preparedReceipt.GetEventId() != eventID || preparedReceipt.GetTenantId() != "demo" || preparedReceipt.GetSequence() != 1 || !preparedReceipt.GetDuplicate() || preparedReceipt.GetHash() != seededReceipt.GetHash() || preparedReceipt.GetStreamId() != seededReceipt.GetStreamId() || preparedReceipt.GetStatus() != seededReceipt.GetStatus() {
		t.Fatal("measured child did not authenticate the exact original durable receipt")
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v\nchild output:\n%s", err, output.String())
	}
	// The derived bound is one store operation; the HTTP drain and the tracer
	// flush are immediate for this process (no HTTP connections, tracing off),
	// so the ceiling below is the drain bound plus generous margin.
	ceiling := shutdownTimeout + childStoreOperationTimeout + 10*time.Second
	select {
	case <-done:
		if waitErr != nil {
			t.Fatalf("the child must exit cleanly after SIGTERM (exit=%v)\noutput:\n%s", waitErr, output.String())
		}
	case <-time.After(ceiling):
		_ = cmd.Process.Kill()
		t.Fatalf("SIGTERM hung: the gRPC drain is unbounded\noutput:\n%s", output.String())
	}

	text := output.String()
	if got := strings.Count(text, "grpc_drain="); got != 1 {
		t.Fatalf("want exactly one grpc_drain line, got %d\noutput:\n%s", got, text)
	}
	if !strings.Contains(text, "grpc_drain="+wantDrain) {
		t.Fatalf("want grpc_drain=%s\noutput:\n%s", wantDrain, text)
	}

	// The lock must be gone: a second Open on the same path succeeds.
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatalf("state lock not released after SIGTERM: %v\noutput:\n%s", err, text)
	}
	defer st.Close()
	if errors.Is(err, store.ErrStateFileLocked) {
		t.Fatal("the state file is still locked after the child exited")
	}

	// Durability of the pre-drain work: exactly one receipt for the event. The
	// archive is configured, so a successful archive legitimately evicts the
	// hot event body — ArchivedAt is therefore the witness that an absent event
	// is expected rather than lost.
	key := store.EventKey("demo", eventID)
	if err := st.Read(testCtx, func(data *store.Snapshot) error {
		receipt, ok := data.Receipts[key]
		if !ok {
			return fmt.Errorf("no durable receipt for %s", key)
		}
		if len(data.Receipts) != 1 || receipt.EventID != eventID || receipt.TenantID != "demo" || receipt.Sequence != 1 || receipt.Hash != preparedReceipt.GetHash() {
			return fmt.Errorf("drain preparation did not preserve exactly one original receipt")
		}
		_, eventPresent := data.Events[key]
		archived := !receipt.ArchivedAt.IsZero()
		if archived == eventPresent {
			return fmt.Errorf("receipt.ArchivedAt is set=%t but the hot event is present=%t; a durable archive must evict the event body and a non-archived receipt must keep it", archived, eventPresent)
		}
		return nil
	}); err != nil {
		t.Fatalf("%v\noutput:\n%s", err, text)
	}
}

// waitForChildLine blocks until needle appears in the child's output or the
// bound expires; a startup failure is reported with the captured output so the
// assertion never degrades into a bare timeout.
func waitForChildLine(t *testing.T, output *syncBuffer, needle string, bound time.Duration) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		if strings.Contains(output.String(), needle) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("child never printed %q within %s\noutput:\n%s", needle, bound, output.String())
}
