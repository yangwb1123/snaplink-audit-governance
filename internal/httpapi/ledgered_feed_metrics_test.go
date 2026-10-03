package httpapi

// Regression coverage for the publisher-side /metrics lines:
// audit_ledgered_publish_failures_total, audit_ledgered_publish_success_total
// and audit_ledgered_outbox_pending.
//
// The post-ledger publication is the only channel feeding the ClickHouse
// projection and every one of its failure modes used to be log-only, so these
// three series are the only machine-readable signal a broker outage produces.
// What the tests below pin is exactly the property that makes them usable and
// cheap: unlabelled output (bounded cardinality), omission rather than
// zero-fill when publication is not configured, an at-least-once lifecycle
// (failure retains the intent, a successful flush drains it and the gauge
// returns to zero), and a scrape that can never block on the store.

import (
	"context"
	"errors"
	"io"
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

// ledgeredFeedSeries are the three publisher-side series, so every assertion
// below iterates one list rather than three hand-copied names.
var ledgeredFeedSeries = []string{
	"audit_ledgered_publish_failures_total",
	"audit_ledgered_publish_success_total",
	"audit_ledgered_outbox_pending",
}

// countingLedgeredPublisher delivers or refuses every publication, and counts
// both, so a test can tell a delivery from an acknowledgement.
type countingLedgeredPublisher struct {
	calls int
	err   error
}

func (p *countingLedgeredPublisher) Publish(_ context.Context, _ domain.Event) error {
	p.calls++
	return p.err
}

// ledgeredFeedService builds a file-backed service whose tenant-a/crm domain is
// registered, with the publisher supplied by the caller (nil means "publication
// is not configured", which is the reference deployment).
func ledgeredFeedService(t *testing.T, publisher service.LedgeredPublisher) (*service.Service, *store.Store) {
	t.Helper()
	st, err := store.OpenWithOptions(filepath.Join(t.TempDir(), "state.json"), store.Options{OperationTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	svc, err := service.New(st, service.Config{
		ArchiveDir:        filepath.Join(t.TempDir(), "archive"),
		SigningSecret:     "test-secret",
		AllowDevSecrets:   true,
		LedgeredPublisher: publisher,
		Now:               func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
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
	if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}}); err != nil {
		t.Fatal(err)
	}
	return svc, st
}

func ledgeredFeedIngest(t *testing.T, svc *service.Service, eventID string) {
	t.Helper()
	event := domain.Event{
		EventID: eventID, SourceSystem: "crm", EventType: "audit.event",
		SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: time.Unix(1_700_000_100, 0).UTC(),
		OperationID: "op-ledgered-feed", AggregateType: "invoice", AggregateID: "inv-1",
		AggregateVersion: 1, Actor: domain.Actor{ID: "user-1", Roles: []string{"operator"}},
		Action: "update", Outcome: "success", DataClassification: "internal",
		RetentionClass: "standard", IdempotencyKey: "idem-" + eventID,
		Payload: map[string]any{"resource": "invoice", "value": 10},
	}
	if _, err := svc.Ingest(testCtx, "tenant-a", domain.IngestPrincipal{ClientID: "crm"}, event, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
}

func assertNoLedgeredFeedSeries(t *testing.T, body, context string) {
	t.Helper()
	for _, name := range ledgeredFeedSeries {
		if strings.Contains(body, name) {
			t.Fatalf("%s: %s must be absent, got:\n%s", context, name, body)
		}
	}
}

// TestMetricsWithoutServiceOmitsLedgeredFeed pins the nil guard: the metrics
// handler must stay safe on a Server with no Service behind it, which several
// handler tests construct directly. The series are then simply absent (no data),
// not a fabricated zero from a process that cannot publish at all.
func TestMetricsWithoutServiceOmitsLedgeredFeed(t *testing.T) {
	recorder := httptest.NewRecorder()
	(&Server{}).metrics(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	body, err := io.ReadAll(recorder.Result().Body)
	if err != nil {
		t.Fatal(err)
	}
	assertNoLedgeredFeedSeries(t, string(body), "a server without a service")
}

// TestMetricsOmitsLedgeredFeedWithoutPublisher pins the omission rule: with no
// LedgeredPublisher configured the three series are absent rather than
// zero-filled. A process that cannot publish anything must not report "0
// failures since boot" as if it were healthy — that is the whole point of the
// guard, and it is also why the shipped reference deployment (no broker) stays
// byte-identical on /metrics. The default testHTTPServer fixture is already in
// this state, so every other /metrics test passes by staying green.
func TestMetricsOmitsLedgeredFeedWithoutPublisher(t *testing.T) {
	svc, _ := ledgeredFeedService(t, nil)
	assertNoLedgeredFeedSeries(t, renderStoreMetrics(t, svc), "a service with no publisher")

	// A flush round on a publisher-less service is a no-op and must not make
	// the gauge appear either.
	if flushed, err := svc.FlushLedgeredOutbox(testCtx, 10); err != nil || flushed != 0 {
		t.Fatalf("flush=%d err=%v, want a no-op", flushed, err)
	}
	assertNoLedgeredFeedSeries(t, renderStoreMetrics(t, svc), "a service with no publisher after a flush")
}

// TestMetricsReportsLedgeredFeedBoundedCardinality pins presence and bounded
// cardinality: all three series appear with exact values, and none of their
// lines carries a label. A tenant, event, broker or reason label would make
// /metrics grow with traffic, and no value identifying a tenant or a DSN may
// leak through it.
func TestMetricsReportsLedgeredFeedBoundedCardinality(t *testing.T) {
	publisher := &countingLedgeredPublisher{}
	svc, _ := ledgeredFeedService(t, publisher)
	ledgeredFeedIngest(t, svc, "ledgered-feed-bounded")
	if publisher.calls != 1 {
		t.Fatalf("publisher calls=%d, want 1 (the request path delivers directly)", publisher.calls)
	}
	// One flush round samples the now-empty durable outbox, which is what
	// makes the gauge reportable at all.
	if flushed, err := svc.FlushLedgeredOutbox(testCtx, 10); err != nil || flushed != 0 {
		t.Fatalf("flush=%d err=%v, want an empty outbox", flushed, err)
	}

	body := renderStoreMetrics(t, svc)
	for _, want := range []string{
		"audit_ledgered_publish_failures_total 0",
		"audit_ledgered_publish_success_total 1",
		"audit_ledgered_outbox_pending 0",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
	for _, line := range strings.Split(body, "\n") {
		for _, name := range ledgeredFeedSeries {
			if strings.HasPrefix(line, name) && strings.Contains(line, "{") {
				t.Fatalf("%s must carry no labels, got %q", name, line)
			}
		}
	}
	for _, leak := range []string{"tenant-a", "audit_tenant", "invoice", "user-1", "postgres://", "AUDIT_POSTGRES_DSN", "dsn="} {
		if strings.Contains(body, leak) {
			t.Fatalf("metrics leaked %q:\n%s", leak, body)
		}
	}
}

// TestMetricsLedgeredFeedFailureDrainsToZero pins the at-least-once lifecycle
// the series measure: a refused publication is counted and its durable intent
// survives; once the broker recovers a flush round drains it, the success
// counter moves, and the gauge returns to zero on the following round (it is a
// round-start sample, so the drain is visible one tick later — audit-api flushes
// every second).
func TestMetricsLedgeredFeedFailureDrainsToZero(t *testing.T) {
	publisher := &countingLedgeredPublisher{err: errors.New("broker unavailable")}
	svc, st := ledgeredFeedService(t, publisher)

	// The ledger stays authoritative: a refused publication must not fail the
	// ingest or answer it differently.
	ledgeredFeedIngest(t, svc, "ledgered-feed-outage")
	if failures := svc.LedgeredPublishFailures(); failures != 1 {
		t.Fatalf("failures=%d, want 1 for the refused publication", failures)
	}
	if successes := svc.LedgeredPublishSuccesses(); successes != 0 {
		t.Fatalf("successes=%d, want 0 while the broker refuses", successes)
	}
	if pending := pendingLedgeredOutbox(t, st); len(pending) != 1 {
		t.Fatalf("pending outbox=%d, want the intent to survive at-least-once", len(pending))
	}
	// Before any round the gauge is unreportable, so the series is absent even
	// though the two counters are live.
	if body := renderStoreMetrics(t, svc); strings.Contains(body, "audit_ledgered_outbox_pending") {
		t.Fatalf("gauge must be omitted before the first flush round samples it:\n%s", body)
	}

	// A round against the dead broker: the gauge samples the backlog, the
	// failure is counted again, nothing is delivered.
	if flushed, err := svc.FlushLedgeredOutbox(testCtx, 10); err != nil || flushed != 0 {
		t.Fatalf("flush=%d err=%v, want nothing published while the broker refuses", flushed, err)
	}
	if failures := svc.LedgeredPublishFailures(); failures != 2 {
		t.Fatalf("failures=%d, want 2 after the round's refused publication", failures)
	}
	if pending, sampled := svc.LedgeredOutboxPending(); !sampled || pending != 1 {
		t.Fatalf("pending=%d sampled=%v, want the sampled backlog 1", pending, sampled)
	}

	// Broker restored: the round drains the intent.
	publisher.err = nil
	if flushed, err := svc.FlushLedgeredOutbox(testCtx, 10); err != nil || flushed != 1 {
		t.Fatalf("recovery flush=%d err=%v, want the retained intent published", flushed, err)
	}
	if pending := pendingLedgeredOutbox(t, st); len(pending) != 0 {
		t.Fatalf("pending after recovery=%d, want an empty outbox", len(pending))
	}
	if failures := svc.LedgeredPublishFailures(); failures != 2 {
		t.Fatalf("failures=%d, want the recovery to add no failure", failures)
	}
	if successes := svc.LedgeredPublishSuccesses(); successes != 1 {
		t.Fatalf("successes=%d, want 1 for the delivered AND acknowledged flush", successes)
	}
	// The gauge is a round-start sample: this round drained the backlog, so the
	// drained value lands on the next tick.
	if pending, sampled := svc.LedgeredOutboxPending(); !sampled || pending != 1 {
		t.Fatalf("pending=%d sampled=%v, want the previous round's backlog 1", pending, sampled)
	}
	if flushed, err := svc.FlushLedgeredOutbox(testCtx, 10); err != nil || flushed != 0 {
		t.Fatalf("drain flush=%d err=%v, want an empty outbox", flushed, err)
	}

	body := renderStoreMetrics(t, svc)
	for _, want := range []string{
		"audit_ledgered_publish_failures_total 2",
		"audit_ledgered_publish_success_total 1",
		"audit_ledgered_outbox_pending 0",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}

// TestMetricsScrapeDoesNotBlockOnStoreWriteLock pins that /metrics is
// store-free: the gauge is sampled by the flush round, so a scrape never takes
// store.Store.mu and never issues a statement. Store.Update runs its closure
// while holding that mutex, so a metrics path that touched the store would
// deadlock here instead of merely being slow.
func TestMetricsScrapeDoesNotBlockOnStoreWriteLock(t *testing.T) {
	publisher := &countingLedgeredPublisher{}
	svc, st := ledgeredFeedService(t, publisher)
	ledgeredFeedIngest(t, svc, "ledgered-feed-store-lock")
	if _, err := svc.FlushLedgeredOutbox(testCtx, 10); err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	locked := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = st.Update(testCtx, func(*store.Snapshot) error {
			close(locked)
			<-release
			return nil
		})
	}()
	<-locked

	scraped := make(chan struct {
		body string
		code int
	}, 1)
	// Rendering runs on another goroutine, so it must not touch *testing.T:
	// a failure there has to be reported by the test goroutine.
	go func() {
		body, code := renderMetricsBody(svc)
		scraped <- struct {
			body string
			code int
		}{body, code}
	}()
	var outcome struct {
		body string
		code int
	}
	select {
	case outcome = <-scraped:
	case <-time.After(2 * time.Second):
		close(release)
		<-done
		t.Fatal("/metrics blocked on store.Store.mu; the gauge must be sampled out of band, not on the scrape path")
	}
	close(release)
	<-done

	if outcome.code != http.StatusOK {
		t.Fatalf("/metrics status=%d, want 200", outcome.code)
	}
	// The scrape still reports the last sample rather than a fabricated value.
	if !strings.Contains(outcome.body, "audit_ledgered_outbox_pending 0") {
		t.Fatalf("scrape under a held store write lock must still carry the last sampled gauge:\n%s", outcome.body)
	}
}

// renderMetricsBody renders GET /metrics without touching *testing.T, so it is
// safe to call from the goroutine a blocking-store test drives.
func renderMetricsBody(svc *service.Service) (string, int) {
	server := NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, nil)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return recorder.Body.String(), recorder.Code
}

// pendingLedgeredOutbox reads the durable intents straight from the snapshot,
// the same walk the flush round uses.
func pendingLedgeredOutbox(t *testing.T, st *store.Store) map[string]domain.Event {
	t.Helper()
	pending := map[string]domain.Event{}
	if err := st.Read(testCtx, func(data *store.Snapshot) error {
		for key, event := range data.LedgeredOutbox {
			pending[key] = event
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return pending
}
