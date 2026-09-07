package outbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/httpapi"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

type fakeStore struct {
	mu        sync.Mutex
	records   map[int64]Record
	corrupt   map[int64]CorruptRecord // pending rows reported as corrupt
	updateErr map[int64]error         // forced Update failures (T3)
	updates   []Update
}

func newFakeStore(records ...Record) *fakeStore {
	store := &fakeStore{
		records:   map[int64]Record{},
		corrupt:   map[int64]CorruptRecord{},
		updateErr: map[int64]error{},
	}
	for _, record := range records {
		store.records[record.ID] = record
	}
	return store
}

// withCorrupt marks a pending row so ListPending reports it as corrupt
// (for example, an undecodable payload or identity mismatch) instead of
// returning it as a deliverable record.
func (f *fakeStore) withCorrupt(id int64, err error) *fakeStore {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.corrupt[id] = CorruptRecord{ID: id, Attempts: f.records[id].Attempts, Err: err}
	return f
}

// withUpdateErr forces the next Update on id to fail (simulates a DB error).
func (f *fakeStore) withUpdateErr(id int64, err error) *fakeStore {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateErr[id] = err
	return f
}

func (f *fakeStore) ListPending(_ context.Context, limit int) ([]Record, []CorruptRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	var records []Record
	var corrupt []CorruptRecord
	for _, record := range f.records {
		if record.Status != StatusPending || record.NextAttemptAt.After(now) {
			continue
		}
		if c, ok := f.corrupt[record.ID]; ok {
			corrupt = append(corrupt, c)
			continue
		}
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	sort.Slice(corrupt, func(i, j int) bool { return corrupt[i].ID < corrupt[j].ID })
	if len(records)+len(corrupt) > limit {
		// Keep deliverable records first, mirroring the SQL LIMIT on the
		// full candidate set; corrupt rows simply stay pending for the
		// next poll.
		if len(records) > limit {
			records = records[:limit]
			corrupt = nil
		} else {
			corrupt = corrupt[:limit-len(records)]
		}
	}
	return records, corrupt, nil
}

func (f *fakeStore) Update(_ context.Context, id int64, patch Update) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if forced := f.updateErr[id]; forced != nil {
		delete(f.updateErr, id)
		return false, forced
	}
	record, ok := f.records[id]
	if !ok || record.Status != StatusPending {
		return false, nil
	}
	record.Status = patch.Status
	record.Attempts = patch.Attempts
	record.NextAttemptAt = patch.NextAttemptAt
	record.LastError = patch.LastError
	f.records[id] = record
	f.updates = append(f.updates, patch)
	return true, nil
}

func (f *fakeStore) record(id int64) Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.records[id]
}

func testRecord(id int64) Record {
	return Record{
		ID:            id,
		Status:        StatusPending,
		Attempts:      0,
		NextAttemptAt: time.Now().Add(-time.Minute),
		CreatedAt:     time.Now().Add(-time.Hour),
		Event: domain.Event{
			EventID:      fmt.Sprintf("event-%d", id),
			TenantID:     "demo",
			SourceSystem: "demo",
			EventType:    "audit.event",
			Action:       "update",
			Outcome:      "success",
			OccurredAt:   time.Now().Add(-time.Hour),
			Actor:        domain.Actor{ID: "user-1"},
		},
	}
}

func fixedRelay(store Store, deliver DeliverFunc) *Relay {
	return &Relay{
		Store:       store,
		Deliver:     deliver,
		BatchSize:   10,
		MaxAttempts: 8,
		BaseBackoff: time.Second,
		MaxBackoff:  time.Minute,
		Now:         func() time.Time { return time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC) },
	}
}

func TestRelayRunOnceDelivers(t *testing.T) {
	store := newFakeStore(testRecord(1), testRecord(2), testRecord(3))
	delivered := 0
	relay := fixedRelay(store, func(_ context.Context, event domain.Event) (*domain.EventReceipt, error) {
		delivered++
		return nil, nil
	})
	handled, err := relay.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if handled != 3 || delivered != 3 {
		t.Fatalf("handled=%d delivered=%d, want 3/3", handled, delivered)
	}
	for _, id := range []int64{1, 2, 3} {
		record := store.record(id)
		if record.Status != StatusDelivered {
			t.Fatalf("record %d status=%s, want delivered", id, record.Status)
		}
		if record.Attempts != 1 {
			t.Fatalf("record %d attempts=%d, want 1", id, record.Attempts)
		}
	}
}

func TestRelayRunOnceRetriesFailures(t *testing.T) {
	store := newFakeStore(testRecord(1))
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) (*domain.EventReceipt, error) {
		return nil, errors.New("connection refused")
	})
	if _, err := relay.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	record := store.record(1)
	if record.Status != StatusPending {
		t.Fatalf("status=%s, want pending for retryable failure", record.Status)
	}
	if record.Attempts != 1 {
		t.Fatalf("attempts=%d, want 1", record.Attempts)
	}
	if !record.NextAttemptAt.After(relay.Now()) {
		t.Fatalf("next_attempt_at=%v, want future retry", record.NextAttemptAt)
	}
	if !strings.Contains(record.LastError, "connection refused") {
		t.Fatalf("last_error=%q", record.LastError)
	}
}

func TestRelayDeadLettersAfterMaxAttempts(t *testing.T) {
	store := newFakeStore(testRecord(1))
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) (*domain.EventReceipt, error) {
		return nil, errors.New("api unavailable")
	})
	relay.MaxAttempts = 2
	for attempt := 1; attempt <= 2; attempt++ {
		if _, err := relay.RunOnce(context.Background()); err != nil {
			t.Fatalf("RunOnce attempt %d: %v", attempt, err)
		}
		if attempt == 1 {
			// Second run must see the record due again.
			record := store.record(1)
			record.NextAttemptAt = time.Now().Add(-time.Minute)
			store.mu.Lock()
			store.records[1] = record
			store.mu.Unlock()
		}
	}
	record := store.record(1)
	if record.Status != StatusFailed {
		t.Fatalf("status=%s, want failed after max attempts", record.Status)
	}
	if record.Attempts != 2 {
		t.Fatalf("attempts=%d, want 2", record.Attempts)
	}
}

func TestRelayDeadLettersPermanentError(t *testing.T) {
	store := newFakeStore(testRecord(1))
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) (*domain.EventReceipt, error) {
		return nil, &DeliveryError{Permanent: true, Err: errors.New("audit api returned 400 Bad Request")}
	})
	if _, err := relay.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	record := store.record(1)
	if record.Status != StatusFailed {
		t.Fatalf("status=%s, want immediate failed", record.Status)
	}
	if record.Attempts != 1 {
		t.Fatalf("attempts=%d, want 1", record.Attempts)
	}
}

func TestRelayRunOnceHonorsBatchSize(t *testing.T) {
	store := newFakeStore(testRecord(1), testRecord(2), testRecord(3))
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) (*domain.EventReceipt, error) { return nil, nil })
	relay.BatchSize = 2
	handled, err := relay.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if handled != 2 {
		t.Fatalf("handled=%d, want batch size 2", handled)
	}
	if store.record(3).Status != StatusPending {
		t.Fatalf("record 3 must stay pending after one limited batch")
	}
}

func TestRelayBackoffCaps(t *testing.T) {
	relay := &Relay{BaseBackoff: time.Second, MaxBackoff: time.Minute}
	if got := relay.backoff(1); got != time.Second {
		t.Fatalf("backoff(1)=%v, want 1s", got)
	}
	if got := relay.backoff(2); got != 2*time.Second {
		t.Fatalf("backoff(2)=%v, want 2s", got)
	}
	if got := relay.backoff(6); got != 32*time.Second {
		t.Fatalf("backoff(6)=%v, want 32s", got)
	}
	if got := relay.backoff(20); got != time.Minute {
		t.Fatalf("backoff(20)=%v, want capped 1m", got)
	}
}

func TestHTTPDeliverer(t *testing.T) {
	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/events" {
			t.Errorf("path=%s", r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer dev:demo:service" {
			t.Errorf("authorization=%q", auth)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"receipt":{"event_id":"event-1","status":"ledgered","ledgered_at":"2026-08-04T12:00:01Z","hash":"abc123"}}`)
	}))
	defer authServer.Close()

	deliver := HTTPDeliverer(authServer.URL, "dev:demo:service", nil)
	event := domain.Event{EventID: "event-1"}
	receipt, err := deliver(context.Background(), event)
	if err != nil {
		t.Fatalf("200 delivery failed: %v", err)
	}
	if receipt == nil || receipt.Status != "ledgered" || receipt.Hash != "abc123" {
		t.Fatalf("parsed receipt=%+v, want status ledgered and hash abc123", receipt)
	}

	newStatusServer := func(status int) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
		}))
	}
	conflictServer := newStatusServer(http.StatusConflict)
	defer conflictServer.Close()
	serverErrorServer := newStatusServer(http.StatusInternalServerError)
	defer serverErrorServer.Close()
	rateLimitServer := newStatusServer(http.StatusTooManyRequests)
	defer rateLimitServer.Close()
	unauthorizedServer := newStatusServer(http.StatusUnauthorized)
	defer unauthorizedServer.Close()

	deliverStatus := func(server *httptest.Server) error {
		_, err := HTTPDeliverer(server.URL, "", nil)(context.Background(), event)
		return err
	}
	if err := deliverStatus(conflictServer); err == nil {
		t.Fatal("409 must fail")
	} else {
		var deliveryErr *DeliveryError
		if !errors.As(err, &deliveryErr) || !deliveryErr.Permanent {
			t.Fatalf("409 error=%v, want permanent delivery error", err)
		}
	}
	if err := deliverStatus(serverErrorServer); err == nil {
		t.Fatal("500 must fail")
	} else {
		var deliveryErr *DeliveryError
		if !errors.As(err, &deliveryErr) || deliveryErr.Permanent {
			t.Fatalf("500 error=%v, want retryable delivery error", err)
		}
	}
	if err := deliverStatus(rateLimitServer); err == nil {
		t.Fatal("429 must fail")
	} else {
		var deliveryErr *DeliveryError
		if !errors.As(err, &deliveryErr) || deliveryErr.Permanent {
			t.Fatalf("429 error=%v, want retryable delivery error", err)
		}
	}
	if err := deliverStatus(unauthorizedServer); err == nil {
		t.Fatal("401 must fail")
	} else {
		var deliveryErr *DeliveryError
		if !errors.As(err, &deliveryErr) || deliveryErr.Permanent {
			t.Fatalf("401 error=%v, want retryable delivery error (token expiry must not dead-letter the backlog)", err)
		}
	}
}

// F-01: DeliveryError.StatusCode is populated at the classification site so
// the consumer can discriminate 401 (credential problem) from other
// retryable classes without parsing the message. A refactor that drops the
// field silently degrades 401 classification to attempts_exhausted — this
// test pins the end-to-end population.
func TestHTTPDelivererPopulatesStatusCode(t *testing.T) {
	newStatusServer := func(status int) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
		}))
	}
	event := domain.Event{EventID: "status-code-test"}
	assertCode := func(server *httptest.Server, wantCode int, wantPermanent bool) {
		t.Helper()
		_, err := HTTPDeliverer(server.URL, "token", nil)(context.Background(), event)
		if err == nil {
			t.Fatalf("status %d must fail", wantCode)
		}
		var deliveryErr *DeliveryError
		if !errors.As(err, &deliveryErr) {
			t.Fatalf("status %d error=%v, want *DeliveryError", wantCode, err)
		}
		if deliveryErr.StatusCode != wantCode {
			t.Fatalf("status %d: StatusCode=%d, want %d", wantCode, deliveryErr.StatusCode, wantCode)
		}
		if deliveryErr.Permanent != wantPermanent {
			t.Fatalf("status %d: Permanent=%v, want %v", wantCode, deliveryErr.Permanent, wantPermanent)
		}
	}
	cases := []struct {
		status    int
		permanent bool
	}{
		{http.StatusUnauthorized, false},
		{http.StatusTooManyRequests, false},
		{http.StatusInternalServerError, false},
		{http.StatusForbidden, true},
		{http.StatusConflict, true},
	}
	for _, tc := range cases {
		server := newStatusServer(tc.status)
		assertCode(server, tc.status, tc.permanent)
		server.Close()
	}
	// 2xx-unverified receipt: StatusCode is threaded through (data honesty —
	// a 2xx is never 401, so classification is unaffected) and stays
	// retryable.
	unverifiedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK) // empty body: no receipt to verify
	}))
	defer unverifiedServer.Close()
	_, err := HTTPDeliverer(unverifiedServer.URL, "token", nil)(context.Background(), event)
	var deliveryErr *DeliveryError
	if !errors.As(err, &deliveryErr) {
		t.Fatalf("2xx-unverified error=%v, want *DeliveryError", err)
	}
	if deliveryErr.StatusCode != http.StatusOK || deliveryErr.Permanent {
		t.Fatalf("2xx-unverified StatusCode=%d Permanent=%v, want 200/false", deliveryErr.StatusCode, deliveryErr.Permanent)
	}
	// Non-HTTP failure: no DeliveryError at all, StatusCode stays 0.
	closed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	closedURL := closed.URL
	closed.Close() // connect-refused transport error
	_, err = HTTPDeliverer(closedURL, "token", nil)(context.Background(), event)
	if err == nil || errors.As(err, &deliveryErr) {
		t.Fatalf("transport error=%v, want plain non-DeliveryError", err)
	}
}

func TestHTTPDelivererTrailingSlash(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/events" {
			t.Errorf("path=%s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"receipt":{"event_id":"event-2","status":"indexed","ledgered_at":"2026-08-04T12:00:01Z","hash":"abc123"}}`)
	}))
	defer server.Close()
	deliver := HTTPDeliverer(server.URL+"/", "", nil)
	receipt, err := deliver(context.Background(), domain.Event{EventID: "event-2"})
	if err != nil {
		t.Fatalf("delivery failed: %v", err)
	}
	if receipt == nil || receipt.Status != "indexed" {
		t.Fatalf("parsed receipt=%+v, want status indexed", receipt)
	}
}

func corruptErr(id int64) error {
	return fmt.Errorf("decode outbox payload id=%d: json: cannot unmarshal array into Go value of type domain.Event", id)
}

func corruptLogger(relay *Relay) *bytes.Buffer {
	var buf bytes.Buffer
	relay.Logger = log.New(&buf, "", 0)
	return &buf
}

// T1 (AC-1): a corrupt row must not block delivery of valid rows; it is
// dead-lettered with the decode error in last_error.
func TestRelayRunOnceQuarantinesCorruptAndDeliversValid(t *testing.T) {
	store := newFakeStore(testRecord(1), testRecord(2), testRecord(3), testRecord(4))
	store.withCorrupt(4, corruptErr(4))
	delivered := 0
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) (*domain.EventReceipt, error) {
		delivered++
		return nil, nil
	})
	logBuf := corruptLogger(relay)

	handled, err := relay.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if handled != 4 {
		t.Fatalf("handled=%d, want 4 (3 delivered + 1 quarantined)", handled)
	}
	if delivered != 3 {
		t.Fatalf("delivered=%d, want 3", delivered)
	}
	// Valid rows delivered as usual.
	for _, id := range []int64{1, 2, 3} {
		if record := store.record(id); record.Status != StatusDelivered {
			t.Fatalf("record %d status=%s, want delivered", id, record.Status)
		}
	}
	// Corrupt row dead-lettered with attempts accounting and decode error.
	corrupt := store.record(4)
	if corrupt.Status != StatusFailed {
		t.Fatalf("corrupt record status=%s, want failed", corrupt.Status)
	}
	if corrupt.Attempts != 1 {
		t.Fatalf("corrupt record attempts=%d, want 1", corrupt.Attempts)
	}
	if !strings.Contains(corrupt.LastError, "decode outbox payload id=4") {
		t.Fatalf("corrupt record last_error=%q, want decode error text", corrupt.LastError)
	}
	if corrupt.NextAttemptAt.After(relay.Now()) {
		t.Fatalf("corrupt record next_attempt_at=%v, want not in the future", corrupt.NextAttemptAt)
	}
	if !strings.Contains(logBuf.String(), "corrupt payload quarantined") {
		t.Fatalf("log=%q, want quarantine message", logBuf.String())
	}
}

// T2 (AC-1): an all-corrupt batch must not abort; every row is quarantined.
func TestRelayRunOnceQuarantinesOnlyCorruptBatch(t *testing.T) {
	store := newFakeStore(testRecord(1), testRecord(2))
	store.withCorrupt(1, corruptErr(1)).withCorrupt(2, corruptErr(2))
	delivered := 0
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) (*domain.EventReceipt, error) {
		delivered++
		return nil, nil
	})
	corruptLogger(relay)

	handled, err := relay.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if handled != 2 {
		t.Fatalf("handled=%d, want 2 quarantined", handled)
	}
	if delivered != 0 {
		t.Fatalf("delivered=%d, want 0 (nothing deliverable)", delivered)
	}
	for _, id := range []int64{1, 2} {
		record := store.record(id)
		if record.Status != StatusFailed {
			t.Fatalf("record %d status=%s, want failed", id, record.Status)
		}
		if record.Attempts != 1 {
			t.Fatalf("record %d attempts=%d, want 1", id, record.Attempts)
		}
	}
}

// T3 (AC-1): a failed quarantine Update must be logged and must not block
// delivery of the valid rows; the corrupt row stays pending for the next
// poll to re-report.
func TestRelayRunOnceQuarantineFailureDoesNotBlockDelivery(t *testing.T) {
	store := newFakeStore(testRecord(1), testRecord(2), testRecord(3))
	store.withCorrupt(3, corruptErr(3)).withUpdateErr(3, errors.New("db down"))
	delivered := 0
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) (*domain.EventReceipt, error) {
		delivered++
		return nil, nil
	})
	logBuf := corruptLogger(relay)

	handled, err := relay.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if handled != 3 {
		t.Fatalf("handled=%d, want 3", handled)
	}
	if delivered != 2 {
		t.Fatalf("delivered=%d, want 2", delivered)
	}
	if !strings.Contains(logBuf.String(), "quarantine failed") {
		t.Fatalf("log=%q, want quarantine failure message", logBuf.String())
	}
	// Quarantine failed: the row must remain pending (re-reported next poll).
	if record := store.record(3); record.Status != StatusPending {
		t.Fatalf("corrupt record status=%s, want pending after failed quarantine", record.Status)
	}
}

// T5 (AC-2): after a corrupt row is quarantined it never reappears, and the
// relay keeps making progress on subsequent polls.
func TestRelayRunOnceProgressAfterQuarantine(t *testing.T) {
	store := newFakeStore(testRecord(1), testRecord(2), testRecord(3), testRecord(4))
	store.withCorrupt(4, corruptErr(4))
	delivered := 0
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) (*domain.EventReceipt, error) {
		delivered++
		return nil, nil
	})
	corruptLogger(relay)

	if handled, err := relay.RunOnce(context.Background()); err != nil || handled != 4 {
		t.Fatalf("first RunOnce handled=%d err=%v", handled, err)
	}
	if record := store.record(4); record.Status != StatusFailed {
		t.Fatalf("record 4 status=%s, want failed", record.Status)
	}

	// Second poll: only the not-yet-delivered valid row is due.
	records, corrupt, err := store.ListPending(context.Background(), 10)
	if err != nil {
		t.Fatalf("second ListPending: %v", err)
	}
	if len(corrupt) != 0 {
		t.Fatalf("corrupt report=%+v, want empty after quarantine", corrupt)
	}
	if len(records) != 0 {
		t.Fatalf("second ListPending records=%d, want 0 (all valid rows already delivered)", len(records))
	}
	handled, err := relay.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("second RunOnce: %v", err)
	}
	if handled != 0 {
		t.Fatalf("second RunOnce handled=%d, want 0", handled)
	}
	if delivered != 3 {
		t.Fatalf("total delivered=%d, want 3", delivered)
	}
}

// TestRelayRunOnceProgressWithMixedDueRows: after quarantine the relay still
// delivers valid rows that become due on a later poll (AC-2 progress).
func TestRelayRunOnceProgressWithMixedDueRows(t *testing.T) {
	store := newFakeStore(testRecord(1), testRecord(2), testRecord(3))
	store.withCorrupt(3, corruptErr(3))
	delivered := 0
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) (*domain.EventReceipt, error) {
		delivered++
		return nil, nil
	})
	corruptLogger(relay)

	if handled, err := relay.RunOnce(context.Background()); err != nil || handled != 3 {
		t.Fatalf("first RunOnce handled=%d err=%v", handled, err)
	}
	// A new valid record becomes due; it must be delivered with no wedge.
	late := testRecord(5)
	store.mu.Lock()
	store.records[5] = late
	store.mu.Unlock()
	handled, err := relay.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("second RunOnce: %v", err)
	}
	if handled != 1 || delivered != 3 {
		t.Fatalf("second RunOnce handled=%d delivered=%d, want 1/3", handled, delivered)
	}
	if record := store.record(5); record.Status != StatusDelivered {
		t.Fatalf("record 5 status=%s, want delivered", record.Status)
	}
}

// receiptBody renders a 2xx ReceiptResponse body for the given receipt
// fields. ledgeredAt may be empty to produce the API's not-honored shape.
func receiptBody(eventID, status, ledgeredAt, hash string) string {
	return fmt.Sprintf(`{"receipt":{"event_id":%q,"status":%q,"ledgered_at":%q,"hash":%q}}`,
		eventID, status, ledgeredAt, hash)
}

// lastAppliedUpdate returns the most recent Update patch the fakeStore
// applied for the given record id.
func lastAppliedUpdate(store *fakeStore, id int64) (Update, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	for i := len(store.updates) - 1; i >= 0; i-- {
		if store.updates[i].Status == StatusDelivered {
			return store.updates[i], true
		}
	}
	return Update{}, false
}

// AC-1: a 2xx whose receipt event_id mismatches the payload must fail
// delivery (retryable unverified) and leave the record pending.
func TestHTTPDelivererReceiptEventIDMismatchFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("wait_for"); got != "ledgered" {
			t.Errorf("wait_for=%q, want ledgered", got)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, receiptBody("other-event", "ledgered", "2026-08-04T12:00:01Z", "abc123"))
	}))
	defer server.Close()

	deliver := HTTPDeliverer(server.URL, "", nil)
	_, err := deliver(context.Background(), domain.Event{EventID: "event-1"})
	if err == nil {
		t.Fatal("mismatched receipt must fail delivery")
	}
	var deliveryErr *DeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.Permanent {
		t.Fatalf("error=%v, want retryable unverified delivery error", err)
	}
	if !strings.Contains(err.Error(), "does not match payload event_id") {
		t.Fatalf("error=%q, want event_id mismatch reason", err)
	}

	// Relay level: the row must stay pending with attempts incremented and
	// no fabricated delivery fields written.
	store := newFakeStore(testRecord(1))
	relay := fixedRelay(store, deliver)
	if _, err := relay.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	record := store.record(1)
	if record.Status != StatusPending || record.Attempts != 1 {
		t.Fatalf("record status=%s attempts=%d, want pending/1", record.Status, record.Attempts)
	}
	if !strings.Contains(record.LastError, "without a verified receipt") {
		t.Fatalf("last_error=%q, want unverified receipt reason", record.LastError)
	}
	for _, patch := range store.updates {
		if patch.DeliveredEventID != "" || patch.APIStatus != "" {
			t.Fatalf("update wrote fabricated fields: %+v", patch)
		}
	}
}

// AC-2: DeliverFunc returns the parsed receipt and succeed persists
// receipt-derived fields only — never the old hardcoded "accepted".
func TestRelaySucceedStoresVerifiedReceiptFields(t *testing.T) {
	// Compile-time pin: the new DeliverFunc shape is in effect.
	var _ DeliverFunc = func(_ context.Context, _ domain.Event) (*domain.EventReceipt, error) { return nil, nil }

	store := newFakeStore(testRecord(1))
	receipt := &domain.EventReceipt{EventID: "event-1", Status: domain.StatusIndexed,
		LedgeredAt: time.Date(2026, 8, 4, 12, 0, 1, 0, time.UTC), Hash: "deadbeef", StreamID: "s1", Sequence: 7}
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) (*domain.EventReceipt, error) {
		return receipt, nil
	})
	if _, err := relay.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if record := store.record(1); record.Status != StatusDelivered {
		t.Fatalf("record status=%s, want delivered", record.Status)
	}
	patch, ok := lastAppliedUpdate(store, 1)
	if !ok {
		t.Fatal("no delivered update captured")
	}
	if patch.APIStatus != "indexed" {
		t.Fatalf("api_status=%q, want indexed from receipt (not hardcoded accepted)", patch.APIStatus)
	}
	if patch.DeliveredEventID != "event-1" {
		t.Fatalf("delivered_event_id=%q, want event-1 from receipt", patch.DeliveredEventID)
	}

	// Nil-receipt contract (Kafka): delivered with empty API-derived fields.
	nilStore := newFakeStore(testRecord(2))
	nilRelay := fixedRelay(nilStore, func(_ context.Context, _ domain.Event) (*domain.EventReceipt, error) {
		return nil, nil
	})
	if _, err := nilRelay.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	nilPatch, ok := lastAppliedUpdate(nilStore, 2)
	if !ok {
		t.Fatal("no nil-receipt update captured")
	}
	if nilPatch.APIStatus != "" || nilPatch.DeliveredEventID != "" {
		t.Fatalf("nil receipt must not fabricate fields, got api_status=%q delivered_event_id=%q",
			nilPatch.APIStatus, nilPatch.DeliveredEventID)
	}
}

// AC-3 A: the deliverer sends wait_for=ledgered and returns the parsed
// receipt instead of discarding it.
func TestHTTPDelivererSendsWaitForLedgeredAndParsesReceipt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("wait_for"); got != "ledgered" {
			t.Errorf("wait_for=%q, want ledgered", got)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, receiptBody("event-1", "ledgered", "2026-08-04T12:00:01Z", "abc"))
	}))
	defer server.Close()

	deliver := HTTPDeliverer(server.URL, "", nil)
	receipt, err := deliver(context.Background(), domain.Event{EventID: "event-1"})
	if err != nil {
		t.Fatalf("delivery failed: %v", err)
	}
	if receipt == nil || receipt.Status != "ledgered" || receipt.Hash != "abc" {
		t.Fatalf("parsed receipt=%+v, want status ledgered hash abc", receipt)
	}
}

// AC-3 B: the relay delivers only on a receipt proving ledgering; "accepted"
// and "received" (the wait not honored) keep the record pending.
func TestRelayDeliversOnlyOnLedgeredReceipt(t *testing.T) {
	for _, status := range []string{domain.StatusAccepted, domain.StatusReceived} {
		t.Run("rejects_"+status, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				// Non-zero ledgered_at/hash isolate V2: the status alone must
				// fail, even though the rest of the receipt is verifiable.
				fmt.Fprint(w, receiptBody("event-1", status, "2026-08-04T12:00:01Z", "abc"))
			}))
			defer server.Close()
			store := newFakeStore(testRecord(1))
			relay := fixedRelay(store, HTTPDeliverer(server.URL, "", nil))
			if _, err := relay.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			record := store.record(1)
			if record.Status != StatusPending {
				t.Fatalf("status=%s, want pending for %q receipt", record.Status, status)
			}
			if !strings.Contains(record.LastError, "does not prove ledgering") {
				t.Fatalf("last_error=%q, want V2 rejection", record.LastError)
			}
		})
	}
	for _, status := range []string{domain.StatusLedgered, domain.StatusIndexed, domain.StatusArchived} {
		t.Run("delivers_"+status, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, receiptBody("event-1", status, "2026-08-04T12:00:01Z", "abc"))
			}))
			defer server.Close()
			store := newFakeStore(testRecord(1))
			relay := fixedRelay(store, HTTPDeliverer(server.URL, "", nil))
			if _, err := relay.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			record := store.record(1)
			if record.Status != StatusDelivered {
				t.Fatalf("status=%s, want delivered for %q receipt", record.Status, status)
			}
			patch, _ := lastAppliedUpdate(store, 1)
			if patch.APIStatus != status {
				t.Fatalf("api_status=%q, want %q", patch.APIStatus, status)
			}
		})
	}
}

// newRealAuditAPI builds the in-process audit API harness used by the E2E
// tests (tenant tenant-a, source crm, schema audit.event v1; dev token
// dev:tenant-a:service:crm). It returns the server URL.
func newRealAuditAPI(t *testing.T) string {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(httpapi.NewServer(svc, auth.Authenticator{AllowDev: true}, log.New(io.Discard, "", 0)).Handler())
	t.Cleanup(server.Close)
	return server.URL
}

// e2eEvent is a ValidateBasic-complete event the real API accepts for
// tenant-a/source crm/schema audit.event v1. TenantID is deliberately empty:
// the server resolves it from the authenticated client.
func e2eEvent(eventID string) domain.Event {
	return domain.Event{
		EventID:            eventID,
		SourceSystem:       "crm",
		EventType:          "audit.event",
		SchemaID:           "audit.event",
		SchemaVersion:      1,
		OccurredAt:         time.Unix(1_700_000_010, 0).UTC(),
		Actor:              domain.Actor{ID: "user-1"},
		Action:             "update",
		Outcome:            "success",
		DataClassification: "internal",
		RetentionClass:     "standard",
		IdempotencyKey:     "relay-e2e-idem-" + eventID,
		Payload:            map[string]any{"value": 1},
	}
}

// AC-3 C: end-to-end through the real audit API. The event is genuinely
// ingested and the record is marked delivered only with the API's verified
// receipt status ("indexed" — archive is not configured in the harness).
// A re-delivered duplicate (F-5) must also verify via the stored receipt.
func TestRelayIntegrationWithRealAuditAPI(t *testing.T) {
	apiURL := newRealAuditAPI(t)
	deliver := HTTPDeliverer(apiURL, "dev:tenant-a:service:crm", nil)

	store := newFakeStore(
		Record{ID: 1, Status: StatusPending, Attempts: 0, NextAttemptAt: time.Now().Add(-time.Minute), Event: e2eEvent("relay-e2e-1")},
		Record{ID: 2, Status: StatusPending, Attempts: 0, NextAttemptAt: time.Now().Add(-time.Minute), Event: e2eEvent("relay-e2e-1")},
	)
	relay := fixedRelay(store, deliver)
	if handled, err := relay.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	} else if handled != 2 {
		t.Fatalf("handled=%d, want 2", handled)
	}
	for _, id := range []int64{1, 2} {
		record := store.record(id)
		if record.Status != StatusDelivered {
			t.Fatalf("record %d status=%s, want delivered", id, record.Status)
		}
		patch, ok := lastAppliedUpdate(store, id)
		if !ok {
			t.Fatalf("record %d has no delivered update", id)
		}
		if patch.DeliveredEventID != "relay-e2e-1" {
			t.Fatalf("record %d delivered_event_id=%q, want relay-e2e-1", id, patch.DeliveredEventID)
		}
		if patch.APIStatus != domain.StatusIndexed && patch.APIStatus != domain.StatusArchived {
			t.Fatalf("record %d api_status=%q, want indexed/archived (real API post-ledger status)", id, patch.APIStatus)
		}
		if patch.DeliveredAt.IsZero() {
			t.Fatalf("record %d delivered_at zero", id)
		}
	}
}

// AC-5: a record whose delivery is unverified stays pending and re-delivers
// on a later poll; it never leaves ListPending's view.
func TestRelayUnverifiedStaysPendingThenRedelivers(t *testing.T) {
	store := newFakeStore(testRecord(1))
	calls := 0
	relay := fixedRelay(store, func(_ context.Context, event domain.Event) (*domain.EventReceipt, error) {
		calls++
		if calls == 1 {
			return nil, &DeliveryError{Permanent: false,
				Err: errors.New("audit api returned 202 Accepted without a verified receipt: receipt event_id mismatch")}
		}
		return &domain.EventReceipt{EventID: event.EventID, Status: "indexed",
			LedgeredAt: time.Date(2026, 8, 4, 12, 0, 1, 0, time.UTC), Hash: "abc"}, nil
	})
	if _, err := relay.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce #1: %v", err)
	}
	record := store.record(1)
	if record.Status != StatusPending || record.Attempts != 1 {
		t.Fatalf("after unverified: status=%s attempts=%d, want pending/1", record.Status, record.Attempts)
	}
	if !record.NextAttemptAt.After(relay.Now()) {
		t.Fatalf("next_attempt_at=%v, want future retry", record.NextAttemptAt)
	}
	if !strings.Contains(record.LastError, "without a verified receipt") {
		t.Fatalf("last_error=%q", record.LastError)
	}
	// Make the record due again and re-deliver.
	record.NextAttemptAt = time.Now().Add(-time.Minute)
	store.mu.Lock()
	store.records[1] = record
	store.mu.Unlock()
	if _, err := relay.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce #2: %v", err)
	}
	if record := store.record(1); record.Status != StatusDelivered {
		t.Fatalf("after re-delivery: status=%s, want delivered", record.Status)
	}
	patch, ok := lastAppliedUpdate(store, 1)
	if !ok || patch.APIStatus != "indexed" || patch.DeliveredEventID != "event-1" {
		t.Fatalf("re-delivered patch=%+v ok=%v, want indexed/event-1", patch, ok)
	}
}

// qa F-1: a 2xx with an empty or truncated body is retryable-unverified —
// the primary proxy-strip symptom must never dead-letter the backlog.
func TestHTTPDelivererEmptyReceiptBodyIsRetryableUnverified(t *testing.T) {
	for _, body := range []string{"", `{"receipt": {"event_id":"ev`, `not json at all`} {
		t.Run(fmt.Sprintf("body_%d", len(body)), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusAccepted)
				if body != "" {
					fmt.Fprint(w, body)
				}
			}))
			defer server.Close()
			_, err := HTTPDeliverer(server.URL, "", nil)(context.Background(), domain.Event{EventID: "event-1"})
			if err == nil {
				t.Fatal("2xx without a decodable receipt must fail")
			}
			var deliveryErr *DeliveryError
			if !errors.As(err, &deliveryErr) || deliveryErr.Permanent {
				t.Fatalf("error=%v, want retryable unverified", err)
			}
			if !strings.Contains(err.Error(), "without a verified receipt") {
				t.Fatalf("error=%q, want unverified-receipt wording", err)
			}
		})
	}
}

// qa F-3: every verifyReceipt branch is exercised directly — V1 mismatch,
// V2 status rejection, V3 zero ledgered_at, V4 empty hash, and the pass case.
func TestVerifyReceipt(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 1, 0, time.UTC)
	event := domain.Event{EventID: "event-1"}
	tests := []struct {
		name    string
		receipt domain.EventReceipt
		wantErr string
	}{
		{"V1 mismatch", domain.EventReceipt{EventID: "other", Status: "indexed", LedgeredAt: now, Hash: "h"}, "does not match payload event_id"},
		{"V2 accepted", domain.EventReceipt{EventID: "event-1", Status: "accepted", LedgeredAt: now, Hash: "h"}, "does not prove ledgering"},
		{"V2 received", domain.EventReceipt{EventID: "event-1", Status: "received", LedgeredAt: now, Hash: "h"}, "does not prove ledgering"},
		{"V3 zero ledgered_at", domain.EventReceipt{EventID: "event-1", Status: "indexed", LedgeredAt: time.Time{}, Hash: "h"}, "ledgered_at is zero"},
		{"V4 empty hash", domain.EventReceipt{EventID: "event-1", Status: "indexed", LedgeredAt: now, Hash: ""}, "hash is empty"},
		{"pass ledgered", domain.EventReceipt{EventID: "event-1", Status: "ledgered", LedgeredAt: now, Hash: "h"}, ""},
		{"pass indexed", domain.EventReceipt{EventID: "event-1", Status: "indexed", LedgeredAt: now, Hash: "h"}, ""},
		{"pass archived", domain.EventReceipt{EventID: "event-1", Status: "archived", LedgeredAt: now, Hash: "h"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := verifyReceipt(event, tt.receipt)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("verifyReceipt=%v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("verifyReceipt err=%v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

// qa F-4: when the delivery succeeded and was verified but the status Update
// fails, the row must stay pending (idempotent re-delivery on the next poll),
// never persist a delivered state.
func TestRelaySucceedUpdateErrorKeepsPending(t *testing.T) {
	store := newFakeStore(testRecord(1)).withUpdateErr(1, errors.New("db down"))
	deliveries := 0
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) (*domain.EventReceipt, error) {
		deliveries++
		return &domain.EventReceipt{EventID: "event-1", Status: "indexed",
			LedgeredAt: time.Date(2026, 8, 4, 12, 0, 1, 0, time.UTC), Hash: "abc"}, nil
	})
	logBuf := corruptLogger(relay)
	if _, err := relay.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	record := store.record(1)
	if record.Status != StatusPending {
		t.Fatalf("status=%s, want pending after Update failure", record.Status)
	}
	if !strings.Contains(logBuf.String(), "delivered but status update failed") {
		t.Fatalf("log=%q, want update-failure message", logBuf.String())
	}
	// Next poll: the row is still pending and re-delivers, storing the
	// receipt fields.
	if _, err := relay.RunOnce(context.Background()); err != nil {
		t.Fatalf("second RunOnce: %v", err)
	}
	if record := store.record(1); record.Status != StatusDelivered {
		t.Fatalf("status=%s, want delivered on second poll", record.Status)
	}
	patch, ok := lastAppliedUpdate(store, 1)
	if !ok || patch.APIStatus != "indexed" {
		t.Fatalf("patch=%+v ok=%v, want indexed api_status", patch, ok)
	}
	if deliveries != 2 {
		t.Fatalf("deliveries=%d, want 2 (re-delivered after failed Update)", deliveries)
	}
}

// qa F-2: a large but legal event_id (several KB) must verify successfully —
// the receipt cap covers it — and a body beyond the cap must fail
// retryably, never permanently (no false dead-letter of an in-ledger event).
func TestHTTPDelivererOversizedReceiptIsRetryableUnverified(t *testing.T) {
	largeID := strings.Repeat("e", 5000)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"receipt":{"event_id":%q,"status":"indexed","ledgered_at":"2026-08-04T12:00:01Z","hash":"abc"}}`, largeID)
	}))
	defer server.Close()
	receipt, err := HTTPDeliverer(server.URL, "", nil)(context.Background(), domain.Event{EventID: largeID})
	if err != nil {
		t.Fatalf("large event_id receipt must verify: %v", err)
	}
	if receipt == nil || receipt.EventID != largeID {
		t.Fatalf("receipt=%+v, want echoed large event_id", receipt)
	}

	// A receipt body beyond the read cap (event_id large enough to push the
	// echo past 64 KB) is truncated mid-JSON: retryable, never permanent.
	hugeID := strings.Repeat("e", 70*1024)
	hugeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"receipt":{"event_id":%q,"status":"indexed","ledgered_at":"2026-08-04T12:00:01Z","hash":"abc"}}`, hugeID)
	}))
	defer hugeServer.Close()
	_, err = HTTPDeliverer(hugeServer.URL, "", nil)(context.Background(), domain.Event{EventID: hugeID})
	if err == nil {
		t.Fatal("over-cap receipt must fail")
	}
	var deliveryErr *DeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.Permanent {
		t.Fatalf("over-cap error=%v, want retryable unverified (never permanent)", err)
	}
}

// F-L2: redirects are not followed; any 3xx classifies as retryable and the
// audit body is never re-POSTed to the redirect target (ErrUseLastResponse).
func TestHTTPDelivererRedirectNotFollowed(t *testing.T) {
	var requests atomic.Int32
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Redirect(w, r, "/target", http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	_, err := HTTPDeliverer(redirect.URL, "", nil)(context.Background(), domain.Event{EventID: "event-1"})
	if err == nil {
		t.Fatal("3xx must fail delivery")
	}
	var deliveryErr *DeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.Permanent {
		t.Fatalf("3xx error=%v, want retryable", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("server saw %d requests, want 1 (redirect must not be followed, body not forwarded)", requests.Load())
	}
}

// F-8: two concurrent relay instances sharing one store — exactly one
// delivered Update applies; the optimistic status='pending' update absorbs
// the duplicate.
func TestRelayConcurrentInstancesSingleApplied(t *testing.T) {
	store := newFakeStore(testRecord(1))
	var deliveries atomic.Int32
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) (*domain.EventReceipt, error) {
		deliveries.Add(1)
		return nil, nil
	})
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := relay.RunOnce(context.Background()); err != nil {
				t.Errorf("RunOnce: %v", err)
			}
		}()
	}
	wg.Wait()
	applied := 0
	store.mu.Lock()
	for _, patch := range store.updates {
		if patch.Status == StatusDelivered {
			applied++
		}
	}
	store.mu.Unlock()
	if applied != 1 {
		t.Fatalf("applied delivered updates=%d, want exactly 1", applied)
	}
	if store.record(1).Status != StatusDelivered {
		t.Fatalf("record status=%s, want delivered", store.record(1).Status)
	}
	if deliveries.Load() < 1 {
		t.Fatalf("deliveries=%d, want >= 1", deliveries.Load())
	}
}
