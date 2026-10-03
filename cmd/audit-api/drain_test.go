package main

// Bounded SIGTERM drain: the unit-level evidence for cmd/audit-api's exit
// sequence. The subprocess proof that a real process actually exits and
// releases its state flock lives in sigterm_drain_process_test.go; everything
// here is fast, deterministic and fixture-controlled.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/grpcapi"
	"github.com/snaplink/audit-governance/internal/runtimeconfig"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
	"github.com/snaplink/audit-governance/internal/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	protoemptypb "google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// hangingStreamService is a test-only streaming handler that parks until the
// test releases it — the exact shape of an authenticated-but-idle
// WriteStream, whose Recv has no timer for a non-expiring token. It
// registers on a real grpc.Server over a loopback listener so the drain helper
// is exercised against real grpc-go connection and RPC bookkeeping, not a stub.
type hangingStreamService struct {
	entered chan struct{}
	release chan struct{}
	// ignoreCancel parks on release alone. It models a handler that cannot be
	// released by a cancelled context at all, which is the shape that exposes
	// grpc-go parking Stop behind a concurrent GracefulStop.
	ignoreCancel bool
}

// parkingStream is the handler seam grpc's RegisterService requires an
// interface for (HandlerType must be an interface type).
type parkingStream interface {
	park(any, grpc.ServerStream) error
}

// park mirrors the production WriteStream handler: it stays in flight until
// the client ends the stream or the transport cancels the handler context, and
// it never ends on its own. That is exactly what makes the graceful stop
// unbounded — and exactly what the forced stop has to release.
func (s hangingStreamService) park(_ any, stream grpc.ServerStream) error {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	if s.ignoreCancel {
		<-s.release
		return nil
	}
	select {
	case <-s.release:
	case <-stream.Context().Done():
	}
	return nil
}

// parkStream adapts the implementation to grpc's StreamHandler shape.
func parkStream(srv any, stream grpc.ServerStream) error {
	return srv.(parkingStream).park(srv, stream)
}

var hangingStreamDesc = grpc.ServiceDesc{
	ServiceName: "drain.Hang",
	HandlerType: (*parkingStream)(nil),
	Streams: []grpc.StreamDesc{{
		StreamName:    "Park",
		Handler:       parkStream,
		ServerStreams: true,
		ClientStreams: true,
	}},
}

// drainHarness is a served grpc.Server plus a client connection to it and the
// handler-entry signal used to prove a stream really reached the server.
type drainHarness struct {
	server  *grpc.Server
	conn    *grpc.ClientConn
	entered chan struct{}
}

// startHangingGRPCServer serves one parking stream and returns the harness. The
// server is stopped and the handler released in cleanup so a failing assertion
// can neither leak a listener nor leak a blocked handler goroutine.
func startHangingGRPCServer(t *testing.T) drainHarness {
	t.Helper()
	return startDrainGRPCServer(t, false)
}

func startDrainGRPCServer(t *testing.T, ignoreCancel bool) drainHarness {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	impl := hangingStreamService{entered: make(chan struct{}, 1), release: make(chan struct{}), ignoreCancel: ignoreCancel}
	server := grpc.NewServer()
	server.RegisterService(&hangingStreamDesc, impl)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		// Release the parked handler BEFORE stopping the server: a concurrent
		// GracefulStop holds the server mutex while it waits for handlers, so a
		// Stop issued first would deadlock against the handler this closes.
		close(impl.release)
		server.Stop()
		_ = listener.Close()
	})
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return drainHarness{server: server, conn: connection, entered: impl.entered}
}

// openParkingStream opens one client stream to the parking handler and blocks
// until the server has actually dispatched it. That handshake is what makes the
// force-path fixture non-vacuous: with no stream in flight the graceful stop
// returns immediately and the bound could never be observed.
func openParkingStream(t *testing.T, h drainHarness) grpc.ClientStream {
	t.Helper()
	stream, err := h.conn.NewStream(context.Background(), &hangingStreamDesc.Streams[0], "/drain.Hang/Park")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the server never dispatched the parked stream; the fixture is vacuous")
	}
	return stream
}

// TestDrainGRPCForcesStopWhenGracefulStopNeverReturns is the reproduction of
// the defect: with an in-flight stream that never returns, an unbounded graceful
// stop hangs forever. drainGRPC must honour the caller-supplied bound, force the
// stop, report it in exactly one bounded operator line, and release the client.
func TestDrainGRPCForcesStopWhenGracefulStopNeverReturns(t *testing.T) {
	h := startHangingGRPCServer(t)
	stream := openParkingStream(t, h)
	var logs bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	forced := drainGRPC(ctx, h.server, log.New(&logs, "", 0))
	elapsed := time.Since(start)

	if !forced {
		t.Fatalf("drainGRPC reported a clean drain while a stream was still in flight; logs: %q", logs.String())
	}
	// The bound is honoured, not "eventually": two orders of magnitude over
	// the 200ms budget is already a regression.
	if elapsed >= 2*time.Second {
		t.Fatalf("drainGRPC took %s, want well inside the 2s ceiling over the 200ms bound", elapsed)
	}
	assertDrainOperatorLine(t, logs.String(), "force_stopped")
	// Stop() really took effect: the transport is closed, so the parked client
	// stream terminates instead of hanging forever.
	recvDone := make(chan error, 1)
	go func() { recvDone <- stream.RecvMsg(new(protoemptypb.Empty)) }()
	select {
	case err := <-recvDone:
		if err == nil {
			t.Fatal("the parked client stream received a message after the forced stop")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the parked client stream was never released by the forced stop")
	}
}

// TestDrainGRPCReturnsWithinBoundWhenTheHandlerIgnoresCancellation pins the
// unconditional half of the bound: grpc-go's forced Stop parks behind a
// concurrent GracefulStop when a handler ignores its cancelled context, so
// drainGRPC must not wait for Stop either. Before this shape existed the
// helper could sit in Stop indefinitely, which is the very hang it exists to
// remove.
func TestDrainGRPCReturnsWithinBoundWhenTheHandlerIgnoresCancellation(t *testing.T) {
	h := startDrainGRPCServer(t, true)
	openParkingStream(t, h)
	var logs bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	forced := drainGRPC(ctx, h.server, log.New(&logs, "", 0))
	elapsed := time.Since(start)

	if !forced {
		t.Fatalf("drainGRPC reported a clean drain with an uncancellable handler in flight; logs: %q", logs.String())
	}
	if elapsed >= 2*time.Second {
		t.Fatalf("drainGRPC took %s; it must return on the bound, not after grpc-go's Stop unblocks", elapsed)
	}
	assertDrainOperatorLine(t, logs.String(), "force_stopped")
}

// TestDrainGRPCDrainedWhenNothingIsInFlight is the non-regression control for
// the happy path: with no in-flight stream the drain is immediate and reports
// the clean outcome, so the force path can never silently become the default.
func TestDrainGRPCDrainedWhenNothingIsInFlight(t *testing.T) {
	h := startHangingGRPCServer(t)
	var logs bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	forced := drainGRPC(ctx, h.server, log.New(&logs, "", 0))
	elapsed := time.Since(start)

	if forced {
		t.Fatalf("drainGRPC force-stopped a server with no in-flight stream; logs: %q", logs.String())
	}
	if elapsed >= 5*time.Second {
		t.Fatalf("an idle graceful stop took %s, want it immediate", elapsed)
	}
	assertDrainOperatorLine(t, logs.String(), "drained")
}

// TestDrainGRPCNilServerIsNoOp pins the no-listener path: the binary prints no
// grpc_drain line at all when no gRPC listener was configured, so an operator
// cannot read absence as failure.
func TestDrainGRPCNilServerIsNoOp(t *testing.T) {
	var logs bytes.Buffer
	if drainGRPC(context.Background(), nil, log.New(&logs, "", 0)) {
		t.Fatal("a nil server must never be reported as force-stopped")
	}
	if logs.Len() != 0 {
		t.Fatalf("a nil server must log nothing, got %q", logs.String())
	}
}

// TestDrainGRPCNilLoggerIsSafe keeps the shutdown path panic-free when the
// operator wiring has no logger.
func TestDrainGRPCNilLoggerIsSafe(t *testing.T) {
	h := startHangingGRPCServer(t)
	openParkingStream(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if !drainGRPC(ctx, h.server, nil) {
		t.Fatal("drainGRPC with a nil logger must still report the force stop")
	}
}

// TestDrainEmitsExactlyOneOperatorLine pins the operator contract for both
// outcomes: exactly one line, a value from the closed two-value vocabulary, and
// no tenant, client, event, route or URL label (bounded cardinality).
func TestDrainEmitsExactlyOneOperatorLine(t *testing.T) {
	t.Run("drained", func(t *testing.T) {
		h := startHangingGRPCServer(t)
		var logs bytes.Buffer
		if drainGRPC(context.Background(), h.server, log.New(&logs, "", 0)) {
			t.Fatal("an idle server must drain cleanly")
		}
		assertDrainOperatorLine(t, logs.String(), "drained")
	})
	t.Run("force_stopped", func(t *testing.T) {
		h := startHangingGRPCServer(t)
		openParkingStream(t, h)
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		var logs bytes.Buffer
		if !drainGRPC(ctx, h.server, log.New(&logs, "", 0)) {
			t.Fatal("an in-flight stream must be force-stopped by the bound")
		}
		assertDrainOperatorLine(t, logs.String(), "force_stopped")
	})
}

// assertDrainOperatorLine asserts the bounded single-line operator contract.
func assertDrainOperatorLine(t *testing.T, out, want string) {
	t.Helper()
	if got := strings.Count(out, "grpc_drain="); got != 1 {
		t.Fatalf("want exactly one grpc_drain line, got %d: %q", got, out)
	}
	value := strings.TrimSpace(strings.SplitN(out, "grpc_drain=", 2)[1])
	if value != "drained" && value != "force_stopped" {
		t.Fatalf("grpc_drain=%s is outside the closed two-value vocabulary", value)
	}
	if value != want {
		t.Fatalf("grpc_drain=%s, want %s", value, want)
	}
	for _, label := range []string{"tenant=", "client_id=", "event_id=", "url=", "endpoint=", "path="} {
		if strings.Contains(out, label) {
			t.Fatalf("the drain line carries the unbounded label %q: %q", label, out)
		}
	}
}

// TestGRPCDrainBoundDerivedFromStoreOperationTimeout is AC-2: the drain window
// is derived from the existing operator knob, never a second invented number,
// and the same validator openStore/prepareServer already enforce keeps it below
// the HTTP socket budget.
func TestGRPCDrainBoundDerivedFromStoreOperationTimeout(t *testing.T) {
	cases := []struct {
		name     string
		env      string
		nilStore bool
		want     time.Duration
	}{
		{name: "shipped default is the store default", want: store.DefaultOperationTimeout},
		{name: "operator override tracks the knob", env: "2s", want: 2 * time.Second},
		{name: "nil store falls back to shutdownTimeout", nilStore: true, want: shutdownTimeout},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(runtimeconfig.EnvStoreOperationTimeout, tc.env)
			resolved, err := runtimeconfig.ResolveStoreOperationTimeout(store.DefaultOperationTimeout)
			if err != nil {
				t.Fatal(err)
			}
			var st *store.Store
			if !tc.nilStore {
				st, err = store.OpenWithOptions(filepath.Join(t.TempDir(), "state.json"), store.Options{OperationTimeout: resolved})
				if err != nil {
					t.Fatal(err)
				}
				defer st.Close()
			}
			got := grpcDrainBound(st)
			if got != tc.want {
				t.Fatalf("grpcDrainBound=%s, want %s", got, tc.want)
			}
			if st != nil && got < st.OperationTimeout() {
				t.Fatalf("grpcDrainBound=%s must be at least the store operation bound %s", got, st.OperationTimeout())
			}
			// Single-source tie-in: the same validator openStore and
			// prepareServer already run at startup, so the drain window can
			// never outgrow the HTTP socket budget.
			if err := runtimeconfig.ValidateStoreOperationTimeoutBelow(got, httpWriteTimeout); err != nil {
				t.Fatalf("drain bound %s fails the startup invariant: %v", got, err)
			}
		})
	}
}

// recordingTracer captures the context shutdownTracer handed it, sampled
// during the call: shutdownTracer cancels its context on return, so the
// recorded Context would otherwise only ever show a cancelled value.
type recordingTracer struct {
	seen      context.Context
	errAtCall error
	deadline  time.Time
	hasDL     bool
	err       error
}

func (r *recordingTracer) Shutdown(ctx context.Context) error {
	r.seen = ctx
	r.errAtCall = ctx.Err()
	r.deadline, r.hasDL = ctx.Deadline()
	return r.err
}

// TestShutdownTracerUsesItsOwnContext is AC-6: the tracer flush must never
// inherit a context the HTTP drain already spent.
func TestShutdownTracerUsesItsOwnContext(t *testing.T) {
	stub := &recordingTracer{}
	shutdownTracer(stub, log.New(io.Discard, "", 0))
	if stub.seen == nil {
		t.Fatal("shutdownTracer never called the tracer")
	}
	if stub.errAtCall != nil {
		t.Fatalf("the tracer inherited a spent context (err=%v); the flush must derive from context.Background()", stub.errAtCall)
	}
	if !stub.hasDL {
		t.Fatal("the tracer context carries no deadline; the flush must be bounded by shutdownTimeout")
	}
	if time.Until(stub.deadline) <= 0 {
		t.Fatalf("the tracer deadline %s is not in the future", stub.deadline)
	}
}

// TestShutdownTracerNilIsNoOp covers both shapes an absent tracer reaches the
// helper as: the untyped-nil interface a test passes, and the typed-nil
// *telemetry.Tracer telemetry.Init returns when tracing is disabled. Only the
// second one is what production actually does, so only the second one proves
// the nil-receiver contract the helper relies on.
func TestShutdownTracerNilIsNoOp(t *testing.T) {
	var logs bytes.Buffer
	logger := log.New(&logs, "", 0)
	shutdownTracer(nil, logger)
	shutdownTracer((*telemetry.Tracer)(nil), logger)
	if logs.Len() != 0 {
		t.Fatalf("an absent tracer must log nothing, got %q", logs.String())
	}
}

// TestTracerShutdownFailureIsNotSilentlyDiscarded pins the replacement for the
// old discarded call: the failure is surfaced, without the error text an OTLP
// error could carry (including the collector endpoint URL startup already logs).
func TestTracerShutdownFailureIsNotSilentlyDiscarded(t *testing.T) {
	var logs bytes.Buffer
	shutdownTracer(&recordingTracer{err: errors.New("export failed: post to http://collector.internal:4318/v1/traces")}, log.New(&logs, "", 0))
	out := logs.String()
	if strings.Count(out, "tracing_shutdown=failed") != 1 {
		t.Fatalf("want exactly one tracing_shutdown=failed line, got %q", out)
	}
	for _, leak := range []string{"error=", "collector.internal", "http://", "url=", "endpoint="} {
		if strings.Contains(out, leak) {
			t.Fatalf("the failure line leaks %q: %q", leak, out)
		}
	}
}

// TestMainGoDoesNotDiscardTracerShutdown is the static regression pin: the
// shutdown path must route the tracer through shutdownTracer and must never go
// back to discarding its error against the shared HTTP drain context.
func TestMainGoDoesNotDiscardTracerShutdown(t *testing.T) {
	text := readMainGo(t)
	if strings.Contains(text, "_ = tracer.Shutdown(") {
		t.Fatal("main.go discards the tracer shutdown error again")
	}
	if !strings.Contains(text, "shutdownTracer(deps.tracer, deps.logger)") {
		t.Fatal("main.go no longer routes the tracer through shutdownTracer")
	}
}

// TestMainGoShutdownLiteralOrdering is the guard for the pinned static test in
// internal/grpcapi: strings.Index reports the FIRST match over the whole file,
// comments included, so any literal mentioned in a comment placed before the
// health drain silently breaks that pin.
func TestMainGoShutdownLiteralOrdering(t *testing.T) {
	text := readMainGo(t)
	health := strings.Index(text, "healthServer.Shutdown()")
	graceful := strings.Index(text, "grpcServer.GracefulStop()")
	if health < 0 || graceful < 0 {
		t.Fatalf("shutdown literals missing: health=%d graceful=%d", health, graceful)
	}
	if health >= graceful {
		t.Fatalf("healthServer.Shutdown() (index %d) must precede grpcServer.GracefulStop() (index %d)", health, graceful)
	}
}

func readMainGo(t *testing.T) string {
	t.Helper()
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	return string(source)
}

// seededDrainService builds the ingest service the commit-convergence test
// needs: the tenant/source/schema control plane bootstrap() creates. ArchiveDir
// is deliberately left empty so nothing is archived and no event body is
// evicted — the invariant under test is receipt accounting, not archival.
func seededDrainService(t *testing.T, st *store.Store) *service.Service {
	t.Helper()
	svc, err := service.New(st, service.Config{AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant(testCtx, "test", domain.Tenant{ID: "demo", Name: "Demo", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "demo", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Classification: "internal", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource(testCtx, "test", domain.SourceSystem{TenantID: "demo", ID: "demo", Name: "Demo source", Active: true}); err != nil {
		t.Fatal(err)
	}
	return svc
}

// startIngestServer serves the production grpcapi ingest surface over a loopback
// listener with dev auth enabled and returns the server handle plus a client —
// the forced stop must reach the very server that owns the in-flight RPC.
func startIngestServer(t *testing.T, svc *service.Service) (*grpc.Server, auditv1.IngestClient) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	grpcapi.Register(server, svc, auth.Authenticator{AllowDev: true})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return server, auditv1.NewIngestClient(connection)
}

// devAuthContext is the outgoing metadata a dev token needs.
func devAuthContext() context.Context {
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer dev:demo:service:demo"))
}

func drainTestEvent(eventID string) *auditv1.EventEnvelope {
	return &auditv1.EventEnvelope{
		EventId:            eventID,
		SourceSystem:       "demo",
		EventType:          "audit.event",
		SchemaId:           "audit.event",
		SchemaVersion:      1,
		OccurredAt:         timestamppb.New(time.Unix(1_700_000_000, 0).UTC()),
		Actor:              &auditv1.Actor{Id: "service"},
		Action:             "write",
		Outcome:            "success",
		PayloadJson:        []byte(`{"value":1}`),
		DataClassification: "internal",
		RetentionClass:     "standard",
		IdempotencyKey:     eventID + "-idem",
	}
}

// drainSnapshotCounts reads the durable receipt/event totals plus the presence
// of one key, so a duplicate receipt or a lost event is visible at the store
// level and not only through the RPC response.
func drainSnapshotCounts(t *testing.T, st *store.Store, eventID string) (receipts, events int, receiptFound, eventFound bool) {
	t.Helper()
	key := store.EventKey("demo", eventID)
	if err := st.Read(testCtx, func(data *store.Snapshot) error {
		receipts = len(data.Receipts)
		events = len(data.Events)
		_, receiptFound = data.Receipts[key]
		_, eventFound = data.Events[key]
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return receipts, events, receiptFound, eventFound
}

// waitForDurableState polls the store until the key reaches a settled state
// (a receipt is durable), so no assertion races the detached commit's own write.
func waitForDurableState(t *testing.T, st *store.Store, eventID string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, _, found, _ := drainSnapshotCounts(t, st, eventID); found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the detached commit never became durable")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestForcedStopDuringIngestCommitLeavesExactlyOneReceipt is AC-5: force-stopping
// mid-commit is at-least-once safe. Holding the store write lock from the test
// side is exactly what makes the ingest commit "in flight" — Store.begin
// derives the operation deadline before the mutex is taken, so the blocked
// writer is already bounded — and the identical retry must converge to exactly
// one receipt.
func TestForcedStopDuringIngestCommitLeavesExactlyOneReceipt(t *testing.T) {
	const eventID = "evt-drain-1"
	st, err := store.OpenWithOptions(filepath.Join(t.TempDir(), "state.json"), store.Options{OperationTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	svc := seededDrainService(t, st)
	ctx := devAuthContext()

	// Negative control first: without a forced stop the very same sequence
	// commits one receipt and converges on retry. A vacuous forced-stop
	// fixture would still pass without this cell.
	controlClient := startIngestClientOnly(t, svc)
	controlID := eventID + "-control"
	if receipt, err := controlClient.Write(ctx, &auditv1.WriteRequest{Event: drainTestEvent(controlID)}); err != nil {
		t.Fatalf("control Write err=%v", err)
	} else if receipt.GetDuplicate() {
		t.Fatal("the control Write must report a fresh receipt")
	}
	controlRetry, err := controlClient.Write(ctx, &auditv1.WriteRequest{Event: drainTestEvent(controlID)})
	if err != nil {
		t.Fatalf("control retry err=%v", err)
	}
	if !controlRetry.GetDuplicate() {
		t.Fatal("the control retry must report Duplicate=true")
	}
	if receipts, events, _, _ := drainSnapshotCounts(t, st, controlID); receipts != 1 || events != 1 {
		t.Fatalf("control state receipts=%d events=%d, want exactly one of each", receipts, events)
	}

	// Gate the store write lock so the ingest commit cannot complete.
	gate := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseGate)
	go func() {
		_ = st.Update(context.Background(), func(*store.Snapshot) error {
			close(gate)
			<-release
			return nil
		})
	}()
	select {
	case <-gate:
	case <-time.After(15 * time.Second):
		t.Fatal("the store write-lock gate never engaged")
	}

	server, client := startIngestServer(t, svc)
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := client.Write(ctx, &auditv1.WriteRequest{Event: drainTestEvent(eventID)})
		writeDone <- writeErr
	}()
	// In-flight proof: the RPC cannot answer while the write lock is held,
	// because every store read and write queues behind the same mutex.
	select {
	case err := <-writeDone:
		t.Fatalf("Write answered while the store write lock was held (err=%v); the fixture is vacuous", err)
	case <-time.After(200 * time.Millisecond):
	}

	var logs bytes.Buffer
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelDrain()
	if !drainGRPC(drainCtx, server, log.New(&logs, "", 0)) {
		t.Fatalf("drainGRPC did not force-stop an in-flight commit; logs: %q", logs.String())
	}
	assertDrainOperatorLine(t, logs.String(), "force_stopped")

	// The client must be released, not left hanging: a force stop either
	// answers with its receipt or terminates the RPC.
	select {
	case err := <-writeDone:
		t.Logf("the in-flight Write terminated as expected: err=%v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("the in-flight Write never terminated after the forced stop")
	}

	releaseGate()
	waitForDurableState(t, st, eventID)

	// The forced stop may only leave the commit all-or-nothing: a receipt
	// always has its event, and never two of either.
	receipts, events, receiptFound, eventFound := drainSnapshotCounts(t, st, eventID)
	if receiptFound != eventFound {
		t.Fatalf("post-force-stop receipt=%t event=%t: a commit must be all-or-nothing", receiptFound, eventFound)
	}
	if receipts != 2 || events != 2 {
		t.Fatalf("post-force-stop receipts=%d events=%d, want one control plus one forced-stop commit", receipts, events)
	}

	// Convergence: the identical retry must land on the stored receipt, with
	// no second ledger entry.
	retryClient := startIngestClientOnly(t, svc)
	retry, err := retryClient.Write(ctx, &auditv1.WriteRequest{Event: drainTestEvent(eventID)})
	if err != nil {
		t.Fatalf("idempotent retry after the forced stop err=%v", err)
	}
	if !retry.GetDuplicate() {
		t.Fatal("the idempotent retry must converge on the stored receipt (Duplicate=true)")
	}
	if receipts, events, receiptFound, eventFound := drainSnapshotCounts(t, st, eventID); !receiptFound || !eventFound || receipts != 2 || events != 2 {
		t.Fatalf("after retry receipts=%d events=%d receipt=%t event=%t, want exactly one added receipt and no duplicate",
			receipts, events, receiptFound, eventFound)
	}
}

// startIngestClientOnly serves a second listener for the same service, used by
// the idempotent retry: the forced stop closed the first server's connections,
// which is exactly what a client would observe in production before it retries
// on a new connection.
func startIngestClientOnly(t *testing.T, svc *service.Service) auditv1.IngestClient {
	t.Helper()
	_, client := startIngestServer(t, svc)
	return client
}
