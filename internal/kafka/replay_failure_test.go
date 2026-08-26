package kafka

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func malformedReplayMessage(value string, offset int64) kafka.Message {
	return kafka.Message{
		Topic: TopicDLQ, Key: []byte("key-fallback"), Value: []byte(value), Partition: 0, Offset: offset,
	}
}

func TestDecodeStrictFailureMatrix(t *testing.T) {
	valid := `{"event_id":"evt-1","error_code":"","error_message":""}`
	tests := []struct {
		name   string
		value  string
		reason failureReason
	}{
		{"missing event id", `{"error_code":"x","error_message":"x"}`, reasonMissingField},
		{"missing error code", `{"event_id":"evt-1","error_message":"x"}`, reasonMissingField},
		{"missing error message", `{"event_id":"evt-1","error_code":"x"}`, reasonMissingField},
		{"empty event id", `{"event_id":"","error_code":"x","error_message":"x"}`, reasonEmptyEventID},
		{"wrong required type", `{"event_id":"evt-1","error_code":null,"error_message":"x"}`, reasonInvalidType},
		{"invalid json", `{`, reasonInvalidJSON},
		{"trailing data", valid + ` trailing`, reasonTrailingData},
		{"multiple values", valid + ` {}`, reasonMultipleValues},
		{"invalid optional tenant", `{"event_id":"evt-1","error_code":"x","error_message":"x","tenant_id":null}`, reasonInvalidType},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			validation := decodeStrictFailure([]byte(test.value))
			if validation.Reason != test.reason {
				t.Fatalf("reason=%q, want %q", validation.Reason, test.reason)
			}
		})
	}
	if validation := decodeStrictFailure([]byte(valid + " \n\t")); validation.Reason != "" {
		t.Fatalf("trailing whitespace reason=%q, want valid", validation.Reason)
	}
}

func TestMalformedFailureIsQuarantinedBeforeReplayClassification(t *testing.T) {
	values := []string{
		`{"error_code":"x","error_message":"x"}`,
		`{"event_id":"","error_code":"x","error_message":"x"}`,
		`{"event_id":"evt-1","error_code":"x"} trailing-secret`,
	}
	for _, tenantAware := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "tenant-aware"}[tenantAware], func(t *testing.T) {
			for offset, value := range values {
				t.Run(string(rune('a'+offset)), func(t *testing.T) {
					dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{malformedReplayMessage(value, int64(offset))}}
					accepted := []kafka.Message{acceptedMessage("evt-1")}
					state, err := LoadReplayState("")
					if err != nil {
						t.Fatal(err)
					}
					republished := 0
					deliver := func(context.Context, []byte, []byte) error {
						republished++
						return nil
					}
					var replayer *Replayer
					if tenantAware {
						replayer = newTenantAwareReplayerWithFactories(
							func() messageReader { return dlq },
							freshAcceptedReader(tenantAcceptedMessages("evt-1", "tenant-a"), nil),
							state, deliver, nil)
					} else {
						replayer = newReplayerWithReaders(dlq, &fakeReplayReader{topic: TopicAccepted, messages: accepted}, state, deliver)
					}
					replayer.drainTimeout = 5 * time.Millisecond
					if _, err := replayer.RunOnce(context.Background()); err != nil {
						t.Fatal(err)
					}
					metrics := replayer.Metrics()
					if republished != 0 || len(state.Replayed) != 0 || len(dlq.commits) != 1 {
						t.Fatalf("republished=%d state=%v commits=%d, want 0/empty/1", republished, state.Replayed, len(dlq.commits))
					}
					if metrics.Malformed != 1 || metrics.DLQRecords != 0 || metrics.Replayed != 0 || metrics.Permanent != 0 || metrics.Unresolvable != 0 || metrics.UnparsableMarks != 0 {
						t.Fatalf("metrics=%+v, want only malformed=1", metrics)
					}
				})
			}
		})
	}
}

func tenantAcceptedMessages(eventID, tenantID string) []kafka.Message {
	return []kafka.Message{{
		Topic: TopicAccepted, Key: []byte(eventID), Value: []byte(`{"event_id":"` + eventID + `","tenant_id":"` + tenantID + `"}`), Partition: 0, Offset: 0,
	}}
}

func TestStrictFailureCompatibilityAndTrailingWhitespace(t *testing.T) {
	for _, tenantAware := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "tenant-aware"}[tenantAware], func(t *testing.T) {
			value := `{"event_id":"evt-1","error_code":"","error_message":""}` + " \n\t"
			dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{{Topic: TopicDLQ, Key: []byte("stale-key"), Value: []byte(value), Partition: 0, Offset: 0}}}
			state, err := LoadReplayState("")
			if err != nil {
				t.Fatal(err)
			}
			republished := 0
			var replayer *Replayer
			if tenantAware {
				replayer = newTenantAwareReplayerWithFactories(func() messageReader { return dlq }, freshAcceptedReader(tenantAcceptedMessages("evt-1", "tenant-a"), nil), state, func(context.Context, []byte, []byte) error {
					republished++
					return nil
				}, nil)
			} else {
				replayer = newReplayerWithReaders(dlq, &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{acceptedMessage("evt-1")}}, state, func(context.Context, []byte, []byte) error {
					republished++
					return nil
				})
			}
			replayer.drainTimeout = 5 * time.Millisecond
			if count, err := replayer.RunOnce(context.Background()); err != nil || count != 1 {
				t.Fatalf("count=%d err=%v, want 1/nil", count, err)
			}
			if republished != 1 || !state.Replayed["evt-1"] {
				t.Fatalf("republished=%d state=%v, want one replay of evt-1", republished, state.Replayed)
			}
			if metrics := replayer.Metrics(); metrics.Malformed != 0 || metrics.DLQRecords != 1 {
				t.Fatalf("metrics=%+v, want malformed=0 dlq_records=1", metrics)
			}
		})
	}
}

func TestMalformedDiagnosticsAreBoundedAndSanitized(t *testing.T) {
	const secret = "payload-secret-that-must-not-be-logged"
	messages := make([]kafka.Message, 25)
	for i := range messages {
		messages[i] = malformedReplayMessage("not-json-"+secret, int64(i))
	}
	var logs bytes.Buffer
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: messages}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	replayer := newReplayerWithReadersAndLogger(dlq, &fakeReplayReader{topic: TopicAccepted}, state, func(context.Context, []byte, []byte) error {
		t.Fatal("malformed record must not be republished")
		return nil
	}, log.New(&logs, "", 0))
	replayer.drainTimeout = 5 * time.Millisecond
	if _, err := replayer.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	details, summaries := 0, 0
	for _, line := range lines {
		if strings.Contains(line, "dlq record malformed") {
			details++
		}
		if strings.Contains(line, "dlq round: malformed_records=") {
			summaries++
		}
	}
	if details != maxMalformedLogDetails || summaries != 1 {
		t.Fatalf("details=%d summaries=%d logs=%q", details, summaries, logs.String())
	}
	if strings.Contains(logs.String(), secret) || strings.Contains(logs.String(), "invalid character") {
		t.Fatalf("diagnostics leaked payload/parser detail: %q", logs.String())
	}
	if got := replayer.Metrics().Malformed; got != uint64(len(messages)) {
		t.Fatalf("malformed=%d, want %d", got, len(messages))
	}
}

func TestMalformedQuarantineKeepsPartitionBarriersIndependent(t *testing.T) {
	dlq := &fakeReplayReader{topic: TopicDLQ}
	replayer := &Replayer{dlqReader: dlq}
	malformed := malformedReplayMessage(`{"event_id":"bad","error_code":"x"}`, 1)
	pending := dlqMessageAt("evt-pending", 1)
	pending.Partition = 1
	collected := []dlqRecord{
		{class: dlqRecordMalformed, reason: reasonMissingField, id: dlqRecordID{topic: TopicDLQ, partition: 0, offset: 1}, message: malformed},
		{class: dlqRecordValid, wanted: true, id: dlqRecordID{topic: TopicDLQ, partition: 1, offset: 1}, message: pending},
	}
	if err := replayer.commitResolved(context.Background(), collected, nil); err != nil {
		t.Fatal(err)
	}
	if len(dlq.commits) != 1 || dlq.commits[0].Partition != 0 {
		t.Fatalf("commits=%v, want only malformed record from independent partition", dlq.commits)
	}
}

func TestMalformedRecordsRespectPartitionPendingBarriers(t *testing.T) {
	t.Run("malformed behind pending record is redelivered", func(t *testing.T) {
		broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{
			dlqMessageAt("evt-pending", 1), malformedReplayMessage(`{"event_id":"bad","error_code":"x"}`, 2),
		}}
		state, err := LoadReplayState("")
		if err != nil {
			t.Fatal(err)
		}
		attempts := 0
		replayer := newReplayerWithFactories(broker.freshDLQSession(), freshAcceptedReader([]kafka.Message{acceptedMessage("evt-pending")}, nil), state, func(context.Context, []byte, []byte) error {
			attempts++
			if attempts == 1 {
				return errors.New("transient")
			}
			return nil
		}, log.New(io.Discard, "", 0))
		replayer.drainTimeout = 5 * time.Millisecond
		if _, err := replayer.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if broker.committed != 0 {
			t.Fatalf("committed=%d, want 0 behind pending offset", broker.committed)
		}
		if _, err := replayer.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if broker.committed != 3 || replayer.Metrics().Malformed != 2 {
			t.Fatalf("committed=%d malformed=%d, want 3/2 after redelivery", broker.committed, replayer.Metrics().Malformed)
		}
		if state.Replayed["bad"] || state.Replayed["key-fallback"] {
			t.Fatal("malformed record must never create a state mark")
		}
	})

	t.Run("malformed below pending record is quarantined", func(t *testing.T) {
		broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{
			malformedReplayMessage(`{"event_id":"bad","error_code":"x"}`, 1), dlqMessageAt("evt-pending", 2),
		}}
		state, err := LoadReplayState("")
		if err != nil {
			t.Fatal(err)
		}
		replayer := newReplayerWithFactories(broker.freshDLQSession(), freshAcceptedReader([]kafka.Message{acceptedMessage("evt-pending")}, nil), state, func(context.Context, []byte, []byte) error {
			return errors.New("transient")
		}, log.New(io.Discard, "", 0))
		replayer.drainTimeout = 5 * time.Millisecond
		if _, err := replayer.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if broker.committed != 2 || len(broker.commits) != 1 || broker.commits[0].Offset != 1 {
			t.Fatalf("committed=%d commits=%v, want malformed offset 1 only", broker.committed, broker.commits)
		}
		if state.Replayed["bad"] || replayer.Metrics().Malformed != 1 {
			t.Fatalf("malformed state/metric=%v/%d, want false/1", state.Replayed["bad"], replayer.Metrics().Malformed)
		}
	})
}
