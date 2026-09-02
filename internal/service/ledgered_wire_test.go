package service

import (
	"bytes"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/kafka"
)

func TestIngestPublishesWireValidLedgeredEvents(t *testing.T) {
	svc := testService(t, false)
	publisher := &recordingLedgeredPublisher{}
	svc.Config.LedgeredPublisher = publisher
	at := time.Unix(1_700_000_600, 0).UTC()

	first := testEvent("ledgered-wire-1", "op-wire", at)
	second := testEvent("ledgered-wire-2", "op-wire", at.Add(time.Second))
	for _, event := range []domain.Event{first, second} {
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatal(err)
		}
	}
	if len(publisher.events) != 2 {
		t.Fatalf("publish count=%d, want 2", len(publisher.events))
	}

	for index, event := range publisher.events {
		value, err := kafka.EncodeEventForTopic(kafka.TopicLedgered, event)
		if err != nil {
			t.Fatalf("event %d wire encoding: %v", index, err)
		}
		wire, err := kafka.ValidateEventJSON(kafka.LedgeredEventSchema, value)
		if err != nil {
			t.Fatalf("event %d wire validation: %v", index, err)
		}
		if wire.EventID != event.EventID || wire.StreamID != event.StreamID || wire.Sequence != event.Sequence || wire.Hash != event.Hash || wire.PrevHash != event.PrevHash {
			t.Fatalf("event %d wire chain=%+v, published=%+v", index, wire, event)
		}
		if event.Sequence == 1 && !bytes.Contains(value, []byte(`"prev_hash":""`)) {
			t.Fatalf("event %d wire bytes=%s, want explicit empty prev_hash", index, value)
		}
	}
	if publisher.events[0].Sequence != 1 || publisher.events[1].Sequence != 2 {
		t.Fatalf("published sequences=%d,%d, want 1,2", publisher.events[0].Sequence, publisher.events[1].Sequence)
	}
	if publisher.events[0].PrevHash != "" || publisher.events[1].PrevHash != publisher.events[0].Hash {
		t.Fatalf("published predecessor hashes=%q,%q, want empty and first hash %q", publisher.events[0].PrevHash, publisher.events[1].PrevHash, publisher.events[0].Hash)
	}
}
