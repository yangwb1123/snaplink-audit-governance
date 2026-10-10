package httpapi

// Regression coverage for audit_ledgered_publish_deferred_total and for the
// HTTP ingest contract while the background ledgered-outbox flusher is
// blocked inside a publish.
//
// The module-level contract under test (requirements FR-1..FR-5, AS-1..AS-3):
//   - the post-commit publish gate is per pending event and is never waited on,
//     so a blocked flush round cannot delay POST /api/v1/events — and cannot
//     multiply that delay across the members of POST /api/v1/events:batch;
//   - the deferral is observable as an unlabelled monotonic counter that the
//     scrape path reads with a single atomic load, so GET /metrics never waits
//     on the publication gate or the store either;
//   - nothing about the ingest response changes: same 202, same receipt shape.
//
// The fixture uses a dev-token principal and a file-backed store, exactly like
// testHTTPServerWithStore, and adds only the ledgered publisher port.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

// gateTestAuth and gateDiscardLogger reproduce the standard harness
// authenticator and logger without touching testHTTPServerWithStore, which
// every other test in this package shares.
func gateTestAuth() auth.Authenticator {
	return auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}
}

func gateDiscardLogger() *log.Logger { return log.New(io.Discard, "", 0) }

// httpGateTimeout is the backstop for an HTTP call that must return while a
// flush round is parked inside a publish. It is far below any write timeout,
// so a regression fails with a named message instead of hanging.
const httpGateTimeout = 3 * time.Second

// gatedHTTPPublisher blocks the ids in block and reports the first entry for
// each through entered; every other id returns immediately.
type gatedHTTPPublisher struct {
	mu      sync.Mutex
	block   map[string]chan struct{}
	entered map[string]chan struct{}
	seen    map[string]int
}

func (p *gatedHTTPPublisher) Publish(ctx context.Context, event domain.Event) error {
	p.mu.Lock()
	p.seen[event.EventID]++
	gate, gated := p.block[event.EventID]
	entered := p.entered[event.EventID]
	if gated && entered != nil {
		select {
		case <-entered:
		default:
			close(entered)
		}
	}
	p.mu.Unlock()
	if !gated {
		return nil
	}
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *gatedHTTPPublisher) count(id string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.seen[id]
}

// ledgeredGateFixture is the standard HTTP harness plus a ledgered publisher
// whose blockedEventId is parked inside Publish for the life of the test.
type ledgeredGateFixture struct {
	server    *httptest.Server
	publisher *gatedHTTPPublisher
	blockedID string
	release   chan struct{}
	entered   chan struct{}
	stopped   chan struct{}
}

const (
	// gateTestToken is a dev token whose client id equals the source system
	// id, which is the binding AddSource creates by default.
	gateTestToken = "dev:tenant-a:service:crm"
	// ledgeredGateBlockedID is the seeded event whose publish the round parks
	// inside. Its durable intent exists with no receipt, so ingesting the SAME
	// event id is a fresh commit whose key the round then owns — the only way
	// the request path can defer.
	ledgeredGateBlockedID = "hl-metrics-blocked"
)

// newLedgeredGateFixture starts a flush round that is parked inside the
// publish of ledgeredGateBlockedID. Callers must always call Close.
func newLedgeredGateFixture(t *testing.T, blockedID string) *ledgeredGateFixture {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	publisher := &gatedHTTPPublisher{
		block:   map[string]chan struct{}{blockedID: make(chan struct{})},
		entered: map[string]chan struct{}{blockedID: make(chan struct{})},
		seen:    map[string]int{},
	}
	svc, err := service.New(st, service.Config{
		ArchiveDir:        filepath.Join(t.TempDir(), "archive"),
		LedgeredPublisher: publisher,
		Now:               func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
		AllowDevSecrets:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, setup := range []func() error{
		func() error {
			return svc.CreateTenant(testCtx, "test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true})
		},
		func() error {
			return svc.CreateTenant(testCtx, "test", domain.Tenant{ID: "tenant-b", Name: "Tenant B", Active: true})
		},
		func() error {
			return svc.AddSource(testCtx, "test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true})
		},
		func() error {
			return svc.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true})
		},
	} {
		if err := setup(); err != nil {
			t.Fatal(err)
		}
	}
	// One durable intent, exactly as an ingest that published during a broker
	// outage would leave behind.
	seeded := httpReadSeedEvent()
	seeded.EventID = blockedID
	seeded.TenantID = "tenant-a"
	seeded.StreamID, seeded.Sequence, seeded.Hash = "tenant-a:source:crm", 1, "seed-ledger-hash"
	if err := st.Update(testCtx, func(data *store.Snapshot) error {
		data.LedgeredOutbox[store.EventKey(seeded.TenantID, seeded.EventID)] = seeded
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	fixture := &ledgeredGateFixture{
		server:    httptest.NewServer(NewServer(svc, gateTestAuth(), gateDiscardLogger()).Handler()),
		publisher: publisher,
		blockedID: blockedID,
		release:   publisher.block[blockedID],
		entered:   publisher.entered[blockedID],
		stopped:   make(chan struct{}),
	}
	go func() {
		defer close(fixture.stopped)
		if _, err := svc.FlushLedgeredOutbox(testCtx, 100); err != nil {
			t.Errorf("flush round failed: %v", err)
		}
	}()
	select {
	case <-fixture.entered:
	case <-time.After(httpGateTimeout):
		t.Fatal("the flush round never entered its blocked publish")
	}
	t.Cleanup(fixture.Close)
	return fixture
}

// Close releases the round and shuts the server down so no test leaves a
// goroutine parked inside a publish.
func (f *ledgeredGateFixture) Close() {
	close(f.release)
	<-f.stopped
	f.server.Close()
}

// scrapeMetrics returns the body and content type of one GET /metrics.
func scrapeMetrics(t *testing.T, baseURL string) (string, string) {
	t.Helper()
	response, err := http.Get(baseURL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body), response.Header.Get("Content-Type")
}

// metricValue reads the integer value of an unlabelled metric line.
func metricValue(t *testing.T, text, name string) (int, bool) {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, name+" ") {
			continue
		}
		if strings.Contains(line, "{") {
			t.Fatalf("%s must carry no labels, got %q", name, line)
		}
		value, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, name+" ")))
		if err != nil {
			t.Fatalf("%s line %q is not an integral counter", name, line)
		}
		return value, true
	}
	return 0, false
}

// postHTTPEvent posts one event and returns the status and decoded body.
func postHTTPEvent(t *testing.T, baseURL, token string, event domain.Event) (int, map[string]any) {
	t.Helper()
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodPost, baseURL+"/api/v1/events", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode %s: %v (body %s)", event.EventID, err, data)
	}
	return response.StatusCode, decoded
}

// gateEvent returns a postable event for the ingest routes.
func gateEvent(id string) domain.Event {
	event := httpReadSeedEvent()
	event.EventID = id
	event.OperationID = id + "-op"
	event.IdempotencyKey = id + "-idem"
	return event
}

// receiptKeys lists the JSON keys of a receipt object, sorted.
func receiptKeys(t *testing.T, receipt map[string]any) []string {
	t.Helper()
	keys := make([]string, 0, len(receipt))
	for key := range receipt {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// TestMetricsWithoutServiceOmitsLedgeredPublishDeferrals pins the nil guard
// for the new counter: a Server with no Service behind it has never ingested
// anything, so the honest answer is an absent line rather than a fabricated 0.
func TestMetricsWithoutServiceOmitsLedgeredPublishDeferrals(t *testing.T) {
	recorder := httptest.NewRecorder()
	(&Server{}).metrics(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	body, err := io.ReadAll(recorder.Result().Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"audit_ledgered_intent_repaired_total", "audit_ledgered_publish_deferred_total"} {
		if strings.Contains(string(body), name) {
			t.Fatalf("a server without a service must not report %s:\n%s", name, body)
		}
	}
}

// TestHTTPMetricsReportsLedgeredPublishDeferrals is AC-4/FR-4/FR-5/AS-3: the
// deferral is visible on /metrics as an unlabelled monotonic counter, the
// sibling ledgered family is untouched, and the scrape itself is served while a
// flush round is still parked inside a publish — which a getter that took the
// publication gate or read the store could not do.
func TestHTTPMetricsReportsLedgeredPublishDeferrals(t *testing.T) {
	fixture := newLedgeredGateFixture(t, ledgeredGateBlockedID)
	token := gateTestToken

	first, contentType := scrapeMetrics(t, fixture.server.URL)
	if first == "" {
		t.Fatal("empty first scrape")
	}
	if contentType != "text/plain; version=0.0.4" {
		t.Fatalf("metrics Content-Type=%q, want text/plain; version=0.0.4", contentType)
	}

	// An ingest of an UNRELATED event while the round is parked: per-event keys
	// mean it never contends, so the counter must stay at zero. This is the
	// FR-1 success condition — the flusher must not be able to push the request
	// path into a deferral.
	status, _ := postHTTPEvent(t, fixture.server.URL, token, gateEvent("hl-metrics-probe"))
	if status != http.StatusAccepted {
		t.Fatalf("unrelated ingest status=%d, want 202 while a flush round was blocked", status)
	}

	started := time.Now()
	second, _ := scrapeMetrics(t, fixture.server.URL)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("a scrape issued while a flush round was blocked took %v, want under 1s (the getter must be lock-free)", elapsed)
	}
	third, _ := scrapeMetrics(t, fixture.server.URL)

	for _, text := range []string{first, second, third} {
		if !strings.Contains(text, "audit_ledgered_publish_deferred_total") {
			t.Fatalf("metrics missing the deferral counter in:\n%s", text)
		}
		if !strings.Contains(text, "audit_ledgered_intent_repaired_total") {
			t.Fatalf("metrics lost the sibling ledgered counter in:\n%s", text)
		}
		if got, ok := metricValue(t, text, "audit_ledgered_publish_deferred_total"); !ok || got != 0 {
			t.Fatalf("deferrals=%d present=%t for an unrelated ingest while the round held its own key, want 0 (per-event keys never contend)", got, ok)
		}
	}
	// Monotonic across scrapes: a counter that could go down would make a
	// restart or a scrape race indistinguishable from a lost count.
	before, _ := metricValue(t, first, "audit_ledgered_publish_deferred_total")
	after, _ := metricValue(t, third, "audit_ledgered_publish_deferred_total")
	if after < before {
		t.Fatalf("deferral counter went backwards: %d then %d", before, after)
	}
	for _, banned := range []string{"tenant_id=", "event_id=", "topic=", "route="} {
		if strings.Contains(second, banned) {
			t.Fatalf("metrics must stay label-free, found %q in:\n%s", banned, second)
		}
	}
}

// TestHTTPMetricsRendersLedgeredPublishDeferralsUnlabelled covers the branch the
// test above cannot reach over HTTP: a NON-ZERO value, which is the only state
// that proves the counter is wired to the real deferral branch. Same-KEY
// contention is the only way to defer, so the round is parked inside the publish
// of an event that has a durable intent but no receipt, and the ingest of that
// SAME event id then finds its own key owned. The 202 and its receipt are
// unchanged (AS-2/FR-3).
func TestHTTPMetricsRendersLedgeredPublishDeferralsUnlabelled(t *testing.T) {
	const contended = "hl-same-key"
	fixture := newLedgeredGateFixture(t, contended)

	status, body := postHTTPEvent(t, fixture.server.URL, gateTestToken, gateEvent(contended))
	if status != http.StatusAccepted {
		t.Fatalf("deferred ingest status=%d, want 202: a deferral must not change the response", status)
	}
	receipt, ok := body["receipt"].(map[string]any)
	if !ok || receipt["event_id"] != contended || receipt["stream_id"] == "" || receipt["hash"] == "" {
		t.Fatalf("deferred ingest body=%+v, want a normal committed receipt", body)
	}
	if _, ok := body["receipt_url"]; !ok {
		t.Fatalf("deferred ingest lost receipt_url: %+v", body)
	}

	text, contentType := scrapeMetrics(t, fixture.server.URL)
	if contentType != "text/plain; version=0.0.4" {
		t.Fatalf("metrics Content-Type=%q, want text/plain; version=0.0.4", contentType)
	}
	if value, ok := metricValue(t, text, "audit_ledgered_publish_deferred_total"); !ok || value != 1 {
		t.Fatalf("deferred_total=%d present=%t, want 1\n%s", value, ok, text)
	}
}

// TestHTTPIngestNotDelayedByFlushRound is AC-1/AS-1 over the real HTTP surface:
// POST /api/v1/events must answer 202 while a flush round is parked inside a
// publish, and its receipt must be a normal committed receipt.
func TestHTTPIngestNotDelayedByFlushRound(t *testing.T) {
	fixture := newLedgeredGateFixture(t, ledgeredGateBlockedID)
	token := gateTestToken

	type outcome struct {
		status int
		body   map[string]any
	}
	done := make(chan outcome, 1)
	go func() {
		status, body := postHTTPEvent(t, fixture.server.URL, token, gateEvent("hl-http-single"))
		done <- outcome{status, body}
	}()
	select {
	case got := <-done:
		if got.status != http.StatusAccepted {
			t.Fatalf("ingest status=%d, want 202 while a flush round was blocked", got.status)
		}
		receipt, ok := got.body["receipt"].(map[string]any)
		if !ok {
			t.Fatalf("response body has no receipt object: %+v", got.body)
		}
		if receipt["event_id"] != "hl-http-single" || receipt["tenant_id"] != "tenant-a" {
			t.Fatalf("receipt=%+v, want the committed event identity", receipt)
		}
		if receipt["stream_id"] == "" || receipt["hash"] == "" {
			t.Fatalf("receipt=%+v, want the server-derived chain facts", receipt)
		}
		if _, ok := got.body["receipt_url"]; !ok {
			t.Fatalf("response body lost receipt_url: %+v", got.body)
		}
	case <-time.After(httpGateTimeout):
		t.Fatal("POST /api/v1/events did not return while a flush round was blocked inside its publish")
	}
}

// TestHTTPBatchNotDelayedByFlushRound is FR-2: postBatch calls Service.Ingest
// once per member over one shared commit context, so a per-member wait would
// multiply into a response time that grows with the number of pending outbox
// events. Every member must answer 202, in the same bounded time, while the
// round stays parked — and each receipt must have the same shape as an
// uncontended control ingest (AS-2/FR-3).
func TestHTTPBatchNotDelayedByFlushRound(t *testing.T) {
	fixture := newLedgeredGateFixture(t, ledgeredGateBlockedID)
	token := gateTestToken

	// Control: an ingest with no round in flight, for the receipt shape.
	controlStatus, controlBody := postHTTPEvent(t, fixture.server.URL, token, gateEvent("hl-batch-control"))
	if controlStatus != http.StatusAccepted {
		t.Fatalf("control ingest status=%d, want 202", controlStatus)
	}
	controlReceipt, ok := controlBody["receipt"].(map[string]any)
	if !ok {
		t.Fatalf("control body has no receipt object: %+v", controlBody)
	}
	controlKeys := receiptKeys(t, controlReceipt)

	events := []domain.Event{
		gateEvent("hl-batch-1"),
		gateEvent("hl-batch-2"),
		gateEvent("hl-batch-3"),
	}
	started := time.Now()
	status, body := postBatchEvents(t, fixture.server.URL, token, events)
	elapsed := time.Since(started)
	if status != http.StatusAccepted {
		t.Fatalf("batch status=%d, want 202 while a flush round was blocked", status)
	}
	if elapsed > httpGateTimeout {
		t.Fatalf("batch took %v, want under %v while a flush round was blocked", elapsed, httpGateTimeout)
	}
	receipts, ok := body["receipts"].([]any)
	if !ok || len(receipts) != len(events) {
		t.Fatalf("body receipts=%v, want %d entries", body["receipts"], len(events))
	}
	for index, raw := range receipts {
		receipt, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("receipt %d is not an object: %+v", index, raw)
		}
		if receipt["event_id"] != events[index].EventID {
			t.Fatalf("receipt %d event_id=%v, want %s in order", index, receipt["event_id"], events[index].EventID)
		}
		if receipt["tenant_id"] != "tenant-a" || receipt["stream_id"] == "" || receipt["hash"] == "" {
			t.Fatalf("receipt %d=%+v, want the committed chain facts", index, receipt)
		}
		// AS-2: deferral must not change the receipt contract, so the key set
		// and the status progression are identical to the control ingest.
		if keys := receiptKeys(t, receipt); !equalStrings(keys, controlKeys) {
			t.Fatalf("receipt %d keys=%v, want the control shape %v", index, keys, controlKeys)
		}
		if receipt["status"] != controlReceipt["status"] {
			t.Fatalf("receipt %d status=%v, want the control status %v", index, receipt["status"], controlReceipt["status"])
		}
	}
	// The batch loop ran three publications while the round was parked, so the
	// round is provably still parked rather than having been waited on.
	if fixture.publisher.count(ledgeredGateBlockedID) != 1 {
		t.Fatalf("the blocked flush event was published %d times, want 1 (the entry only)", fixture.publisher.count(ledgeredGateBlockedID))
	}
}

// postBatchEvents posts a batch and returns the status and decoded body.
func postBatchEvents(t *testing.T, baseURL, token string, events []domain.Event) (int, map[string]any) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"events": events})
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodPost, baseURL+"/api/v1/events:batch", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode batch response: %v (body %s)", err, data)
	}
	return response.StatusCode, decoded
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
