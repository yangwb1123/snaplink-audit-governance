package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
