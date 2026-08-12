package outbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
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
// (undecodable payload) instead of returning it as a deliverable record.
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
	relay := fixedRelay(store, func(_ context.Context, event domain.Event) error {
		delivered++
		return nil
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
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) error {
		return errors.New("connection refused")
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
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) error {
		return errors.New("api unavailable")
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
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) error {
		return &DeliveryError{Permanent: true, Err: errors.New("audit api returned 400 Bad Request")}
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
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) error { return nil })
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
		w.WriteHeader(http.StatusAccepted)
	}))
	defer authServer.Close()

	deliver := HTTPDeliverer(authServer.URL, "dev:demo:service", nil)
	event := domain.Event{EventID: "event-1"}
	if err := deliver(context.Background(), event); err != nil {
		t.Fatalf("200 delivery failed: %v", err)
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

	deliverStatus := func(server *httptest.Server) error {
		return HTTPDeliverer(server.URL, "", nil)(context.Background(), event)
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
}

func TestHTTPDelivererTrailingSlash(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/events" {
			t.Errorf("path=%s", r.URL.Path)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	deliver := HTTPDeliverer(server.URL+"/", "", nil)
	if err := deliver(context.Background(), domain.Event{EventID: "event-2"}); err != nil {
		t.Fatalf("delivery failed: %v", err)
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
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) error {
		delivered++
		return nil
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
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) error {
		delivered++
		return nil
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
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) error {
		delivered++
		return nil
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
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) error {
		delivered++
		return nil
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
	relay := fixedRelay(store, func(_ context.Context, _ domain.Event) error {
		delivered++
		return nil
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
