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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/auth"
)

// End-to-end caller-withdrawal regression over the existing blocking-archive
// seam (export_gate_cost_test.go: gateHTTPArchive / gateCostServer /
// gateSeedEvictedTenant / gateComplianceToken).
//
// The download handler used to answer an archive read failure with a literal
// 500 that bypassed statusForError entirely, so a mid-read cancellation was
// both mislabelled ("internal server error") and counted into
// audit_http_errors_total — the P1 availability signal. These tests drive the
// real handlers and assert the wire class, the redacted body and the counter
// split.
//
// Two levels are used deliberately:
//
//   - recorder level, for the response CLASS and the BODY, which a cancelled
//     socket can never deliver;
//   - socket level, for the COUNTERS, which is the only faithful way to make a
//     client vanish mid-read.
//
// Together they cover the criterion without a flaky read of a dead socket.

// withdrawalFixture is one seeded server plus its handler: the same *Server
// serves both the live socket (for /metrics scrapes and the disconnect test)
// and the recorder-driven class assertions, so the counters are shared.
type withdrawalFixture struct {
	api     *Server
	handler http.Handler
	live    *httptest.Server
	jobID   string
}

// newWithdrawalFixture seeds an evicted event and a completed export over the
// gate-cost fixture, then serves it. blocking is the archive mode to install.
func newWithdrawalFixture(t *testing.T, blocking bool) (*withdrawalFixture, *gateHTTPArchive) {
	t.Helper()
	seed, svc, _ := gateCostServer(t)
	job := gateSeedEvictedTenant(t, seed.URL)
	// gateCostServer already closed its own listener through t.Cleanup; the
	// shared *Server is rebuilt on the same service so the counters under test
	// belong to exactly one process.
	seed.Close()

	archive := newGateHTTPArchive(archiveDirOf(t, svc), false)
	if blocking {
		archive.blockUntilCancelled()
	}
	svc.Config.Archive = archive
	api := NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, log.New(io.Discard, "", 0))
	handler := api.Handler()
	live := httptest.NewServer(handler)
	t.Cleanup(live.Close)
	return &withdrawalFixture{api: api, handler: handler, live: live, jobID: job.ID}, archive
}

// metrics scrapes /metrics from the live server.
func (f *withdrawalFixture) metrics(t *testing.T) string {
	t.Helper()
	request, _ := http.NewRequest(http.MethodGet, f.live.URL+"/metrics", nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func (f *withdrawalFixture) counter(t *testing.T, text, name string) float64 {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, name) {
			continue
		}
		fields := strings.Fields(line)
		var value float64
		if _, err := fmt.Sscanf(fields[len(fields)-1], "%g", &value); err != nil {
			t.Fatalf("cannot parse metric line %q: %v", line, err)
		}
		return value
	}
	t.Fatalf("metric %q missing from /metrics:\n%s", name, text)
	return 0
}

// withdrawMode selects how the caller withdraws mid-read.
type withdrawMode int

const (
	withdrawByCancel withdrawMode = iota
	withdrawByDeadline
)

// deferredDeadline is a caller context whose deadline expires only when the
// test releases it. Deadline() reports none, so no store operation or archive
// bound can pre-expire it, and the request reaches the archive read in flight
// first. That makes the caller-deadline case deterministic (no sleeping) and
// keeps it the documented coverage boundary: a deadline consumed INSIDE a
// store operation is store_timeout, not a withdrawal.
type deferredDeadline struct {
	context.Context
	done chan struct{}
	mu   sync.Mutex
	err  error
}

func (d *deferredDeadline) Deadline() (time.Time, bool) { return time.Time{}, false }
func (d *deferredDeadline) Done() <-chan struct{}       { return d.done }

func (d *deferredDeadline) Err() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.err
}

func (d *deferredDeadline) expire() {
	d.mu.Lock()
	d.err = context.DeadlineExceeded
	d.mu.Unlock()
	close(d.done)
}

// withdraw drives one request whose context is withdrawn while the blocking
// archive read is in flight, and returns the recorder holding the response.
func withdraw(t *testing.T, f *withdrawalFixture, archive *gateHTTPArchive, path, token string, mode withdrawMode) *httptest.ResponseRecorder {
	t.Helper()
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := context.Context(base)
	release := cancel
	if mode == withdrawByDeadline {
		deferred := &deferredDeadline{Context: base, done: make(chan struct{})}
		ctx, release = deferred, deferred.expire
	}
	entered := archive.blocked()
	go func() {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
		}
		release()
	}()
	request := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	f.handler.ServeHTTP(recorder, request)
	return recorder
}

// assertWithdrawalBody asserts the withdrawal envelope and that no internal
// identifier reached the client.
func assertWithdrawalBody(t *testing.T, recorder *httptest.ResponseRecorder, wantCode, wantMessage string, forbidden ...string) {
	t.Helper()
	if recorder.Code != statusClientClosedRequest {
		t.Fatalf("status=%d, want %d", recorder.Code, statusClientClosedRequest)
	}
	var envelope errorEnvelope
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("error body decode: %v (body=%s)", err, recorder.Body.String())
	}
	if envelope.Error.Code != wantCode {
		t.Fatalf("code=%q, want %q (body=%s)", envelope.Error.Code, wantCode, recorder.Body.String())
	}
	if envelope.Error.Message != wantMessage {
		t.Fatalf("message=%q, want the fixed %q", envelope.Error.Message, wantMessage)
	}
	for _, token := range forbidden {
		if strings.Contains(recorder.Body.String(), token) {
			t.Fatalf("withdrawal body leaked %q: %s", token, recorder.Body.String())
		}
	}
}

// TestDownloadWithdrawalReturnsClientWithdrawalClass is AC-2: a client that
// cancels GET /api/v1/exports/{id}/download while the archive object read is
// in flight is answered with the withdrawal class, and the body carries
// neither the job id, the archive key, the tenant nor the word "internal".
func TestDownloadWithdrawalReturnsClientWithdrawalClass(t *testing.T) {
	f, archive := newWithdrawalFixture(t, true)
	before := f.counter(t, f.metrics(t), "audit_http_errors_total")

	recorder := withdraw(t, f, archive, "/api/v1/exports/"+f.jobID+"/download", gateComplianceToken, withdrawByCancel)
	assertWithdrawalBody(t, recorder, "client_cancelled", cancelledMessage,
		f.jobID, "exports/", "tenant-a", "internal", "context canceled")

	if archive.count()["exports/"] == 0 {
		t.Fatalf("the download must have reached the archive read, counts=%v", archive.count())
	}
	after := f.counter(t, f.metrics(t), "audit_http_errors_total")
	if after != before {
		t.Fatalf("audit_http_errors_total moved %v -> %v: a withdrawal must not consume the availability signal", before, after)
	}
	if got := f.counter(t, f.metrics(t), `audit_http_request_cancelled_total{reason="cancelled"}`); got < 1 {
		t.Fatalf("withdrawal cancelled counter=%v, want >= 1", got)
	}
	if got := f.counter(t, f.metrics(t), `audit_http_request_cancelled_total{reason="deadline_exceeded"}`); got != 0 {
		t.Fatalf("withdrawal deadline counter=%v, want 0 for a cancellation", got)
	}
}

// TestCallerDeadlineReturnsDeadlineClass is the deadline half of AC-2: a
// caller-context deadline that reaches the boundary as the sentinel is its own
// code, distinct from a cancellation. Coverage boundary, stated: a deadline
// consumed INSIDE a store operation is classified store_timeout (503) — see
// TestWithdrawalMappingDoesNotStealStoreClasses — so this route (whose archive
// read is the caller's own read) is where the class is observable.
func TestCallerDeadlineReturnsDeadlineClass(t *testing.T) {
	f, archive := newWithdrawalFixture(t, true)
	before := f.counter(t, f.metrics(t), "audit_http_errors_total")

	recorder := withdraw(t, f, archive, "/api/v1/exports/"+f.jobID+"/download", gateComplianceToken, withdrawByDeadline)
	assertWithdrawalBody(t, recorder, "client_deadline_exceeded", deadlineExceededMessage,
		f.jobID, "exports/", "tenant-a", "internal", "context deadline exceeded")

	after := f.counter(t, f.metrics(t), "audit_http_errors_total")
	if after != before {
		t.Fatalf("audit_http_errors_total moved %v -> %v", before, after)
	}
	if got := f.counter(t, f.metrics(t), `audit_http_request_cancelled_total{reason="deadline_exceeded"}`); got < 1 {
		t.Fatalf("withdrawal deadline counter=%v, want >= 1", got)
	}
}

// TestEventReadWithdrawalReturnsClientWithdrawalClass is the second AC-2 route.
// The seeded event was hot-evicted by its own archived commit, so the read
// falls into archivedEventContext, whose already-exhausted-bound guard returns
// fmt.Errorf("archived event %s: %w", receipt.EventID, err): the event id is
// inside the wrapped cause and must never reach the body.
func TestEventReadWithdrawalReturnsClientWithdrawalClass(t *testing.T) {
	f, _ := newWithdrawalFixture(t, false)
	before := f.counter(t, f.metrics(t), "audit_http_errors_total")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/events/gate-event", nil).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer "+gateComplianceToken)
	recorder := httptest.NewRecorder()
	f.handler.ServeHTTP(recorder, request)

	assertWithdrawalBody(t, recorder, "client_cancelled", cancelledMessage,
		"gate-event", "tenant-a", "internal", "context canceled", "events/")
	if after := f.counter(t, f.metrics(t), "audit_http_errors_total"); after != before {
		t.Fatalf("audit_http_errors_total moved %v -> %v", before, after)
	}
	if got := f.counter(t, f.metrics(t), `audit_http_request_cancelled_total{reason="cancelled"}`); got < 1 {
		t.Fatalf("withdrawal cancelled counter=%v, want >= 1", got)
	}
}

// TestClientDisconnectDoesNotRaiseAvailabilityError is the socket-level half:
// a client that vanishes mid-read cannot receive a response, so only the
// counters are asserted. Before the change this request produced a 500 counted
// into audit_http_errors_total — a P1 availability page for a healthy store.
func TestClientDisconnectDoesNotRaiseAvailabilityError(t *testing.T) {
	f, archive := newWithdrawalFixture(t, true)
	before := f.metrics(t)
	errorsBefore := f.counter(t, before, "audit_http_errors_total")
	requestsBefore := f.counter(t, before, "audit_http_requests_total")
	withdrawalsBefore := f.counter(t, before, `audit_http_request_cancelled_total{reason="cancelled"}`)

	address := strings.TrimPrefix(f.live.URL, "http://")
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	request := fmt.Sprintf(
		"GET /api/v1/exports/%s/download HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nConnection: close\r\n\r\n",
		f.jobID, address, gateComplianceToken)
	if _, err := conn.Write([]byte(request)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-archive.blocked():
	case <-time.After(2 * time.Second):
		t.Fatal("the archive read was never entered")
	}
	// The caller withdraws: the socket closes with the read in flight.
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		current := f.metrics(t)
		withdrawals := f.counter(t, current, `audit_http_request_cancelled_total{reason="cancelled"}`)
		requests := f.counter(t, current, "audit_http_requests_total")
		if withdrawals >= withdrawalsBefore+1 && requests > requestsBefore {
			if got := f.counter(t, current, "audit_http_errors_total"); got != errorsBefore {
				t.Fatalf("audit_http_errors_total moved %v -> %v on a client disconnect", errorsBefore, got)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("withdrawal counter=%v (want >= %v) / requests=%v (want > %v) after 2s:\n%s",
				withdrawals, withdrawalsBefore+1, requests, requestsBefore, current)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestWithdrawalMetricCardinality is AC-4: the new family is exactly two
// fixed-vocabulary lines, rendered unconditionally, with no tenant, event,
// route, URL or DSN material anywhere in /metrics.
func TestWithdrawalMetricCardinality(t *testing.T) {
	f, _ := newWithdrawalFixture(t, false)
	text := f.metrics(t)

	lines := 0
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "audit_http_request_cancelled_total") {
			lines++
			if !strings.HasPrefix(line, `audit_http_request_cancelled_total{reason="cancelled"} `) &&
				!strings.HasPrefix(line, `audit_http_request_cancelled_total{reason="deadline_exceeded"} `) {
				t.Fatalf("unexpected withdrawal series %q: the only label is the closed two-value reason", line)
			}
		}
	}
	if lines != 2 {
		t.Fatalf("audit_http_request_cancelled_total lines=%d, want exactly 2 (one per reason, rendered even at 0):\n%s", lines, text)
	}
	if !strings.Contains(text, `audit_http_request_cancelled_total{reason="cancelled"} 0`) ||
		!strings.Contains(text, `audit_http_request_cancelled_total{reason="deadline_exceeded"} 0`) {
		t.Fatalf("withdrawal series must be rendered unconditionally at 0:\n%s", text)
	}
	for _, forbidden := range []string{"tenant-a", "gate-event", "audit_tenant", "/api/v1/", "postgres://", "dsn=", "exports/"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("/metrics leaked %q:\n%s", forbidden, text)
		}
	}
}
