package kafka

import (
	"context"
	"io"
	"log"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestReplayPermanentCodeTimeoutRemainsPending(t *testing.T) {
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	dlqMessages := []kafka.Message{tenantFailureMessage("evt-permanent-timeout", "tenant-a", ErrorCodePermanentError, 0)}
	acceptedMessages := []kafka.Message{tenantAcceptedMessage("evt-permanent-timeout", "tenant-a", 0)}
	newDLQ := func() messageReader {
		return &fakeReplayReader{topic: TopicDLQ, messages: dlqMessages}
	}
	newAccepted := func() messageReader {
		return &fakeReplayReader{topic: TopicAccepted, messages: acceptedMessages}
	}
	attempts := 0
	replayer := newTenantAwareReplayerWithFactories(newDLQ, newAccepted, state, func(context.Context, []byte, []byte) error {
		attempts++
		if attempts == 1 {
			return context.DeadlineExceeded
		}
		return nil
	}, log.New(io.Discard, "", 0))
	replayer.drainTimeout = 5 * time.Millisecond

	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("first RunOnce error=%v, want pending timeout uncertainty", err)
	}
	if count != 0 || attempts != 1 || state.Replayed["evt-permanent-timeout"] {
		t.Fatalf("first round count=%d attempts=%d marked=%v, want 0/1/false", count, attempts, state.Replayed["evt-permanent-timeout"])
	}

	count, err = replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("second RunOnce error=%v, want retry success", err)
	}
	if count != 1 || attempts != 2 || !state.Replayed["evt-permanent-timeout"] {
		t.Fatalf("second round count=%d attempts=%d marked=%v, want 1/2/true", count, attempts, state.Replayed["evt-permanent-timeout"])
	}
}
