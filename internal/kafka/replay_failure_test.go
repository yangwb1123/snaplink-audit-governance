package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
		messages[i].Topic = TopicDLQ + "\nforged"
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
	for round := 1; round <= 2; round++ {
		if _, err := replayer.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		dlq.index = 0 // model redelivery of the uncommitted malformed records
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	details, summaries := 0, 0
	for _, line := range lines {
		if strings.Contains(line, "dlq record malformed") {
			details++
			if !strings.Contains(line, "reason=invalid_json") || !strings.Contains(line, "topic="+TopicDLQ+"?forged") || strings.Contains(line, "\n") {
				t.Fatalf("detail has unstable reason or unsanitized topic: %q", line)
			}
		}
		if strings.Contains(line, "dlq round: malformed_records=") {
			summaries++
			if line != "dlq round: malformed_records=25" {
				t.Fatalf("summary=%q, want exact per-round total", line)
			}
		}
	}
	if details != 2*maxMalformedLogDetails || summaries != 2 {
		t.Fatalf("details=%d summaries=%d logs=%q", details, summaries, logs.String())
	}
	if strings.Contains(logs.String(), secret) || strings.Contains(logs.String(), "invalid character") || strings.Contains(logs.String(), "topic="+TopicDLQ+"\nforged") {
		t.Fatalf("diagnostics leaked payload/parser detail or raw metadata: %q", logs.String())
	}
	if got := replayer.Metrics().Malformed; got != uint64(2*len(messages)) {
		t.Fatalf("malformed=%d, want %d across redelivery", got, 2*len(messages))
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

func TestStrictFailureCompatibilityVariants(t *testing.T) {
	cases := []struct {
		name  string
		value string
	}{
		{"legacy three-field", `{"event_id":"evt-1","error_code":"attempts_exhausted","error_message":"temporary failure"}`},
		{"tenant-bearing", `{"event_id":"evt-1","error_code":"attempts_exhausted","error_message":"temporary failure","tenant_id":"tenant-a"}`},
		{"empty optional tenant", `{"event_id":"evt-1","error_code":"attempts_exhausted","error_message":"temporary failure","tenant_id":""}`},
		{"unknown property", `{"event_id":"evt-1","error_code":"attempts_exhausted","error_message":"temporary failure","future":"ignored"}`},
	}
	for _, tenantAware := range []bool{false, true} {
		mode := map[bool]string{false: "compatibility", true: "tenant-aware"}[tenantAware]
		for _, tc := range cases {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{{Topic: TopicDLQ, Key: []byte("evt-1"), Value: []byte(tc.value), Partition: 0, Offset: 0}}}
				state, err := LoadReplayState("")
				if err != nil {
					t.Fatal(err)
				}
				var republished int
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
				if republished != 1 || replayer.Metrics().Malformed != 0 || !state.Replayed["evt-1"] {
					t.Fatalf("republished=%d metrics=%+v state=%v, want one valid replay", republished, replayer.Metrics(), state.Replayed)
				}
				if tenantAware && !state.Marked("tenant-a", "evt-1") {
					t.Fatal("tenant-aware compatibility record must create the canonical scoped mark")
				}
			})
		}
	}
}

func TestMalformedCommitFailureLeavesOffsetPending(t *testing.T) {
	sentinel := errors.New("injected malformed commit failure")
	dlq := &fakeReplayReader{
		topic:     TopicDLQ,
		messages:  []kafka.Message{malformedReplayMessage(`{"event_id":"bad","error_code":"x"}`, 0)},
		commitErr: sentinel,
	}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	replayer := newReplayerWithReaders(dlq, &fakeReplayReader{topic: TopicAccepted}, state, func(context.Context, []byte, []byte) error {
		t.Fatal("malformed record must not be republished")
		return nil
	})
	replayer.drainTimeout = 5 * time.Millisecond
	if _, err := replayer.RunOnce(context.Background()); !errors.Is(err, sentinel) {
		t.Fatalf("RunOnce error=%v, want commit failure %v", err, sentinel)
	}
	if len(dlq.commits) != 0 {
		t.Fatalf("failed commit recorded %d commits, want 0", len(dlq.commits))
	}
	if len(state.Replayed) != 0 || replayer.Metrics().Malformed != 1 {
		t.Fatalf("failed quarantine changed state/metric=%v/%d, want empty/1", state.Replayed, replayer.Metrics().Malformed)
	}

	// A fresh round re-reads the fetched-but-uncommitted physical record once
	// the commit fault is removed; quarantine then succeeds without a mark.
	dlq.commitErr = nil
	dlq.index = 0
	if _, err := replayer.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(dlq.commits) != 1 || dlq.commits[0].Offset != 0 {
		t.Fatalf("successful retry commits=%v, want the malformed offset 0", dlq.commits)
	}
	if len(state.Replayed) != 0 || replayer.Metrics().Malformed != 2 {
		t.Fatalf("retry state/metric=%v/%d, want empty/2", state.Replayed, replayer.Metrics().Malformed)
	}
}

func TestMalformedFailureMatrixThroughReplayer(t *testing.T) {
	valid := `{"event_id":"evt-1","error_code":"x","error_message":"x"}`
	cases := []struct {
		name   string
		value  string
		reason failureReason
	}{
		{"array value", `[1]`, reasonInvalidType},
		{"null value", `null`, reasonInvalidType},
		{"string value", `"failure"`, reasonInvalidType},
		{"number value", `42`, reasonInvalidType},
		{"missing event id", `{"error_code":"x","error_message":"x"}`, reasonMissingField},
		{"missing error code", `{"event_id":"evt-1","error_message":"x"}`, reasonMissingField},
		{"missing error message", `{"event_id":"evt-1","error_code":"x"}`, reasonMissingField},
		{"empty event id", `{"event_id":"","error_code":"x","error_message":"x"}`, reasonEmptyEventID},
		{"event id wrong type", `{"event_id":1,"error_code":"x","error_message":"x"}`, reasonInvalidType},
		{"event id null", `{"event_id":null,"error_code":"x","error_message":"x"}`, reasonInvalidType},
		{"error code wrong type", `{"event_id":"evt-1","error_code":true,"error_message":"x"}`, reasonInvalidType},
		{"error code null", `{"event_id":"evt-1","error_code":null,"error_message":"x"}`, reasonInvalidType},
		{"error message wrong type", `{"event_id":"evt-1","error_code":"x","error_message":[]}`, reasonInvalidType},
		{"error message null", `{"event_id":"evt-1","error_code":"x","error_message":null}`, reasonInvalidType},
		{"invalid json", `{`, reasonInvalidJSON},
		{"trailing text", valid + ` trailing`, reasonTrailingData},
		{"multiple values", valid + ` {}`, reasonMultipleValues},
		{"tenant wrong type", valid[:len(valid)-1] + `,"tenant_id":1}`, reasonInvalidType},
		{"tenant null", valid[:len(valid)-1] + `,"tenant_id":null}`, reasonInvalidType},
	}
	for _, tenantAware := range []bool{false, true} {
		mode := map[bool]string{false: "compatibility", true: "tenant-aware"}[tenantAware]
		for index, tc := range cases {
			t.Run(fmt.Sprintf("%s/%02d-%s", mode, index, tc.name), func(t *testing.T) {
				message := malformedReplayMessage(tc.value, int64(index))
				message.Key = []byte("key-fallback")
				dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{message}}
				state, err := LoadReplayState("")
				if err != nil {
					t.Fatal(err)
				}
				var logs bytes.Buffer
				deliver := func(context.Context, []byte, []byte) error {
					t.Fatal("malformed record must not be republished")
					return nil
				}
				var replayer *Replayer
				if tenantAware {
					replayer = newTenantAwareReplayerWithFactories(
						func() messageReader { return dlq },
						func() messageReader { return &fakeReplayReader{topic: TopicAccepted} },
						state, deliver, log.New(&logs, "", 0))
				} else {
					replayer = newReplayerWithReadersAndLogger(dlq, &fakeReplayReader{topic: TopicAccepted}, state, deliver, log.New(&logs, "", 0))
				}
				replayer.drainTimeout = 5 * time.Millisecond
				if _, err := replayer.RunOnce(context.Background()); err != nil {
					t.Fatal(err)
				}
				if len(dlq.commits) != 1 || dlq.commits[0].Key != nil || dlq.commits[0].Value != nil {
					t.Fatalf("quarantine commit=%v, want one minimal identity-only commit", dlq.commits)
				}
				if replayer.acceptedSeen.Load() != 0 || len(state.Replayed) != 0 || state.Marked("tenant-a", "evt-1") {
					t.Fatalf("malformed record entered replay path: accepted=%d state=%v scoped=%v", replayer.acceptedSeen.Load(), state.Replayed, state.Marked("tenant-a", "evt-1"))
				}
				metrics := replayer.Metrics()
				if metrics.Malformed != 1 || metrics.DLQRecords != 0 || metrics.Replayed != 0 || metrics.Permanent != 0 || metrics.Unresolvable != 0 || metrics.UnparsableMarks != 0 || metrics.RepublishFailures != 0 || metrics.AttemptsExhausted != 0 || metrics.AuthBlocked != 0 || metrics.TenantScopeMismatches != 0 {
					t.Fatalf("malformed metrics=%+v, want only Malformed=1", metrics)
				}
				if !strings.Contains(logs.String(), "reason="+string(tc.reason)) {
					t.Fatalf("diagnostic missing fixed reason %q: %s", tc.reason, logs.String())
				}
			})
		}
	}
}

func TestMalformedCollectionRetainsOnlyBoundedMetadata(t *testing.T) {
	messages := make([]kafka.Message, maxMalformedRecordsPerRound+1)
	for i := range messages {
		messages[i] = malformedReplayMessage("not-json", int64(i))
	}
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: messages}
	replayer := &Replayer{dlqReader: dlq, logger: log.New(io.Discard, "", 0), drainTimeout: time.Millisecond}
	collected, err := replayer.collectFailures(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(collected) != maxMalformedRecordsPerRound {
		t.Fatalf("collected=%d, want bounded %d", len(collected), maxMalformedRecordsPerRound)
	}
	for _, record := range collected {
		if record.class != dlqRecordMalformed || record.reason != reasonInvalidJSON || record.message.Key != nil || record.message.Value != nil {
			t.Fatalf("malformed record retained payload or lost classification: %+v", record)
		}
	}
	if got := replayer.Metrics().Malformed; got != maxMalformedRecordsPerRound+1 {
		t.Fatalf("malformed metric=%d, want %d fetched classifications", got, maxMalformedRecordsPerRound+1)
	}
	if len(replayer.malformedOverflow) != 1 || replayer.malformedOverflow[0].message.Key != nil || replayer.malformedOverflow[0].message.Value != nil {
		t.Fatalf("overflow malformed record retained payload or was lost: %+v", replayer.malformedOverflow)
	}

	// A single oversized malformed input is classified and retained only as
	// identity metadata, then can be safely quarantined without the payload
	// budget causing an otherwise safe record to be redelivered forever.
	over := malformedReplayMessage("not-json", 0)
	over.Key = []byte(strings.Repeat("k", maxMalformedRecordBytes))
	overReader := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{over}}
	overReplayer := &Replayer{dlqReader: overReader, logger: log.New(io.Discard, "", 0), drainTimeout: time.Millisecond}
	overCollected, err := overReplayer.collectFailures(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(overCollected) != 0 || len(overReplayer.malformedOverflow) != 1 || overReplayer.Metrics().Malformed != 1 {
		t.Fatalf("oversized malformed collection=%d overflow=%d metric=%d, want 0/1/1", len(overCollected), len(overReplayer.malformedOverflow), overReplayer.Metrics().Malformed)
	}
	if err := overReplayer.commitResolved(context.Background(), append(overCollected, overReplayer.malformedOverflow...), nil); err != nil {
		t.Fatal(err)
	}
	if len(overReader.commits) != 1 || overReader.commits[0].Offset != 0 || overReader.commits[0].Value != nil || overReader.commits[0].Key != nil {
		t.Fatalf("oversized quarantine commit=%v, want identity-only offset 0", overReader.commits)
	}
}

func FuzzDecodeStrictFailure(f *testing.F) {
	for _, seed := range []string{
		`{"event_id":"evt-1","error_code":"x","error_message":"x"}`,
		`{"event_id":"evt-1","error_code":"x","error_message":"x"}` + " \n\t",
		`not-json`, `[]`, `null`, `{"event_id":"evt-1"} {}`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		validation := decodeStrictFailure([]byte(value))
		if validation.Reason == "" {
			// Empty error_code and error_message are valid independently;
			// only the required event ID is constrained by minLength.
			if validation.Failure.EventID == "" {
				t.Fatalf("valid result has empty event_id for input %q", value)
			}
			return
		}
		if validation.Failure != (Failure{}) {
			t.Fatalf("malformed result retained parsed metadata: reason=%q failure=%+v", validation.Reason, validation.Failure)
		}
	})
}
