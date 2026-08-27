package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

type recordingLedgeredPublisher struct {
	events []domain.Event
	err    error
}

func (p *recordingLedgeredPublisher) Publish(_ context.Context, event domain.Event) error {
	if p.err != nil {
		return p.err
	}
	p.events = append(p.events, event)
	return nil
}

func TestIngestPublishesLedgeredExactlyOnce(t *testing.T) {
	svc := testService(t, false)
	publisher := &recordingLedgeredPublisher{}
	svc.Config.LedgeredPublisher = publisher
	at := time.Unix(1_700_000_500, 0).UTC()

	event := testEvent("ledgered-publish-1", "op-ledgered", at)
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err != nil {
		t.Fatal(err)
	}
	if len(publisher.events) != 1 {
		t.Fatalf("publish count=%d, want 1", len(publisher.events))
	}
	published := publisher.events[0]
	if published.StreamID != receipt.StreamID || published.Sequence != receipt.Sequence || published.Hash != receipt.Hash {
		t.Fatalf("published chain=%+v, receipt=%+v", published, receipt)
	}

	duplicate, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err != nil || !duplicate.Duplicate {
		t.Fatalf("duplicate receipt=%+v err=%v", duplicate, err)
	}
	if len(publisher.events) != 1 {
		t.Fatalf("duplicate publish count=%d, want 1", len(publisher.events))
	}

	forged := testEvent("ledgered-publish-2", "op-ledgered", at.Add(time.Second))
	forged.StreamID, forged.Sequence, forged.Hash = "client:forged", 999, "forged-hash"
	forgedReceipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, forged, domain.StatusLedgered)
	if err != nil {
		t.Fatal(err)
	}
	if len(publisher.events) != 2 {
		t.Fatalf("fresh publish count=%d, want 2", len(publisher.events))
	}
	published = publisher.events[1]
	if published.StreamID == "client:forged" || published.Sequence == 999 || published.Hash == "forged-hash" {
		t.Fatalf("published forged chain state: %+v", published)
	}
	if published.StreamID != forgedReceipt.StreamID || published.Sequence != forgedReceipt.Sequence || published.Hash != forgedReceipt.Hash {
		t.Fatalf("published chain=%+v, receipt=%+v", published, forgedReceipt)
	}
}

func TestIngestRejectionsNeverPublish(t *testing.T) {
	cases := []struct {
		name  string
		setup func() domain.Event
		want  error
	}{
		{name: "tenant mismatch", setup: func() domain.Event {
			event := testEvent("ledgered-reject-tenant", "op-reject", time.Unix(1_700_000_510, 0).UTC())
			event.TenantID = "other-tenant"
			return event
		}, want: domain.ErrTenantMismatch},
		{name: "invalid schema payload", setup: func() domain.Event {
			event := testEvent("ledgered-reject-invalid", "op-reject", time.Unix(1_700_000_511, 0).UTC())
			delete(event.Payload, "resource")
			return event
		}, want: domain.ErrInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := testService(t, false)
			publisher := &recordingLedgeredPublisher{}
			svc.Config.LedgeredPublisher = publisher
			if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, tc.setup(), domain.StatusLedgered); !errors.Is(err, tc.want) {
				t.Fatalf("error=%v, want %v", err, tc.want)
			}
			if len(publisher.events) != 0 {
				t.Fatalf("rejected publish count=%d, want 0", len(publisher.events))
			}
		})
	}

	t.Run("content conflict", func(t *testing.T) {
		svc := testService(t, false)
		publisher := &recordingLedgeredPublisher{}
		svc.Config.LedgeredPublisher = publisher
		at := time.Unix(1_700_000_512, 0).UTC()
		first := testEvent("ledgered-reject-conflict", "op-reject", at)
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, first, domain.StatusLedgered); err != nil {
			t.Fatal(err)
		}
		conflicting := testEvent("ledgered-reject-conflict", "op-reject", at)
		conflicting.Payload["value"] = 11
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, conflicting, domain.StatusLedgered); !errors.Is(err, domain.ErrConflict) {
			t.Fatalf("error=%v, want conflict", err)
		}
		if len(publisher.events) != 1 {
			t.Fatalf("conflict publish count=%d, want 1", len(publisher.events))
		}
	})
}

type blockingLedgeredPublisher struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *blockingLedgeredPublisher) Publish(ctx context.Context, _ domain.Event) error {
	p.once.Do(func() { close(p.started) })
	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type sequenceLedgeredPublisher struct {
	failOn int
	calls  int
	events []domain.Event
}

func (p *sequenceLedgeredPublisher) Publish(_ context.Context, event domain.Event) error {
	p.calls++
	if p.failOn > 0 && p.calls == p.failOn {
		return errors.New("broker unavailable")
	}
	p.events = append(p.events, event)
	return nil
}

func pendingLedgeredEvents(t *testing.T, svc *Service) map[string]domain.Event {
	t.Helper()
	var pending map[string]domain.Event
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		pending = make(map[string]domain.Event, len(data.LedgeredOutbox))
		for key, event := range data.LedgeredOutbox {
			pending[key] = event
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return pending
}

func TestLedgeredPublishTimeoutRetainsOutbox(t *testing.T) {
	svc := testService(t, false)
	publisher := &blockingLedgeredPublisher{started: make(chan struct{}), release: make(chan struct{})}
	svc.Config.LedgeredPublisher = publisher
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan struct {
		receipt domain.EventReceipt
		err     error
	}, 1)
	go func() {
		receipt, err := svc.Ingest(ctx, "tenant-a", crmPrincipal, testEvent("ledgered-timeout", "op-timeout", time.Unix(1_700_000_530, 0).UTC()), domain.StatusLedgered)
		result <- struct {
			receipt domain.EventReceipt
			err     error
		}{receipt, err}
	}()
	select {
	case <-publisher.started:
	case <-time.After(time.Second):
		t.Fatal("publisher was not called")
	}
	cancel()
	select {
	case outcome := <-result:
		if outcome.err != nil || outcome.receipt.EventID != "ledgered-timeout" || outcome.receipt.StreamID == "" {
			t.Fatalf("receipt=%+v err=%v, want committed ledger despite canceled publication", outcome.receipt, outcome.err)
		}
	case <-time.After(time.Second):
		t.Fatal("ingest did not finish after publication context cancellation")
	}
	pending := pendingLedgeredEvents(t, svc)
	if len(pending) != 1 {
		t.Fatalf("pending outbox=%d, want 1 after timeout-uncertain publication", len(pending))
	}
	close(publisher.release)
}

func TestFlushLedgeredOutboxStopsAtFirstFailure(t *testing.T) {
	svc := testService(t, false)
	events := []domain.Event{
		testEvent("ledgered-flush-a", "op-flush", time.Unix(1_700_000_540, 0).UTC()),
		testEvent("ledgered-flush-b", "op-flush", time.Unix(1_700_000_541, 0).UTC()),
	}
	for index := range events {
		events[index].TenantID = "tenant-a"
		events[index].StreamID = "tenant-a:source:crm"
		events[index].Sequence = int64(index + 1)
		events[index].Hash = fmt.Sprintf("hash-%d", index+1)
	}
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		for _, event := range events {
			data.LedgeredOutbox[store.EventKey(event.TenantID, event.EventID)] = event
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	publisher := &sequenceLedgeredPublisher{failOn: 2}
	svc.Config.LedgeredPublisher = publisher
	flushed, err := svc.FlushLedgeredOutbox(context.Background(), 10)
	if err != nil || flushed != 1 {
		t.Fatalf("flush=%d err=%v, want one publication before first failure", flushed, err)
	}
	pending := pendingLedgeredEvents(t, svc)
	if len(pending) != 1 || pending[store.EventKey("tenant-a", "ledgered-flush-b")].EventID != "ledgered-flush-b" {
		t.Fatalf("pending=%v, want only second event retained", pending)
	}
	publisher.failOn = 0
	flushed, err = svc.FlushLedgeredOutbox(context.Background(), 10)
	if err != nil || flushed != 1 {
		t.Fatalf("recovery flush=%d err=%v, want retained event published", flushed, err)
	}
	if pending := pendingLedgeredEvents(t, svc); len(pending) != 0 {
		t.Fatalf("pending after recovery=%v, want empty", pending)
	}
}

func TestLedgeredAckFailureRetainsOutbox(t *testing.T) {
	backend := newFailSaveBackend()
	svc := newServiceOn(t, store.NewWithBackend(backend))
	seedTestDomain(t, svc)
	publisher := &recordingLedgeredPublisher{}
	svc.Config.LedgeredPublisher = publisher
	backend.mu.Lock()
	ackSave := backend.saves + 2 // ledger commit, then publication acknowledgement
	backend.errorFrom, backend.errorTo, backend.sentinel = ackSave, ackSave, errors.New("ack persistence unavailable")
	backend.mu.Unlock()

	event := testEvent("ledgered-ack-failure", "op-ack", time.Unix(1_700_000_550, 0).UTC())
	receipt, err := svc.Ingest(context.Background(), "tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err != nil || receipt.EventID != event.EventID || receipt.StreamID == "" {
		t.Fatalf("receipt=%+v err=%v, want successful ingest despite ack failure", receipt, err)
	}
	pending := pendingLedgeredEvents(t, svc)
	if len(pending) != 1 {
		t.Fatalf("pending outbox=%d, want 1 after acknowledgement failure", len(pending))
	}
}

func TestPublishFailureDoesNotFailIngest(t *testing.T) {
	svc := testService(t, false)
	publisher := &recordingLedgeredPublisher{err: errors.New("broker unavailable")}
	var logs []string
	svc.Config.LedgeredPublisher = publisher
	svc.Config.Logf = func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	}
	event := testEvent("ledgered-publish-failure", "op-ledgered", time.Unix(1_700_000_520, 0).UTC())
	if receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil || receipt.Status != domain.StatusIndexed {
		t.Fatalf("receipt=%+v err=%v, want successful indexed ingest", receipt, err)
	}
	if len(logs) != 1 || !strings.Contains(logs[0], event.EventID) || !strings.Contains(logs[0], "ledgered publish failed") {
		t.Fatalf("logs=%v, want bounded publication failure diagnostic", logs)
	}
	var pending int
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		pending = len(data.LedgeredOutbox)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("failed publication must remain durable, pending=%d", pending)
	}

	publisher.err = nil
	flushed, err := svc.FlushLedgeredOutbox(context.Background(), 10)
	if err != nil || flushed != 1 {
		t.Fatalf("flush=%d err=%v, want one recovered publication", flushed, err)
	}
	if len(publisher.events) != 1 || publisher.events[0].EventID != event.EventID {
		t.Fatalf("recovered publications=%+v", publisher.events)
	}
	if flushed, err := svc.FlushLedgeredOutbox(context.Background(), 10); err != nil || flushed != 0 {
		t.Fatalf("second flush=%d err=%v, want empty queue", flushed, err)
	}
}
