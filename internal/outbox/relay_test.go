package outbox

import (
	"context"
	"errors"
	"fmt"
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
	mu      sync.Mutex
	records map[int64]Record
	updates []Update
}

func newFakeStore(records ...Record) *fakeStore {
	store := &fakeStore{records: map[int64]Record{}}
	for _, record := range records {
		store.records[record.ID] = record
	}
	return store
}

func (f *fakeStore) ListPending(_ context.Context, limit int) ([]Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	var due []Record
	for _, record := range f.records {
		if record.Status == StatusPending && !record.NextAttemptAt.After(now) {
			due = append(due, record)
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].ID < due[j].ID })
	if len(due) > limit {
		due = due[:limit]
	}
	return due, nil
}

func (f *fakeStore) Update(_ context.Context, id int64, patch Update) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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
