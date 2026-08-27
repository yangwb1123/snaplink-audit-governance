package kafka

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"strings"

	"github.com/segmentio/kafka-go"
)

type failureReason string

const (
	reasonInvalidJSON    failureReason = "invalid_json"
	reasonMissingField   failureReason = "missing_field"
	reasonInvalidType    failureReason = "invalid_type"
	reasonEmptyEventID   failureReason = "empty_event_id"
	reasonTrailingData   failureReason = "trailing_data"
	reasonMultipleValues failureReason = "multiple_values"
)

type failureValidation struct {
	Failure Failure
	Reason  failureReason
}

// decodeStrictFailure validates the complete wire representation of a DLQ
// Failure. Raw object members are used so missing fields, null, and empty
// strings remain distinguishable.
func decodeStrictFailure(value []byte) failureValidation {
	decoder := json.NewDecoder(bytes.NewReader(value))
	var first json.RawMessage
	if err := decoder.Decode(&first); err != nil {
		return failureValidation{Reason: reasonInvalidJSON}
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != nil {
		if err != io.EOF {
			return failureValidation{Reason: reasonTrailingData}
		}
	} else {
		return failureValidation{Reason: reasonMultipleValues}
	}

	trimmed := bytes.TrimSpace(first)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return failureValidation{Reason: reasonInvalidType}
	}
	members := map[string]json.RawMessage{}
	if err := json.Unmarshal(trimmed, &members); err != nil {
		return failureValidation{Reason: reasonInvalidJSON}
	}
	for _, name := range []string{"event_id", "error_code", "error_message"} {
		if _, ok := members[name]; !ok {
			return failureValidation{Reason: reasonMissingField}
		}
	}
	eventID, ok := strictFailureString(members["event_id"])
	if !ok {
		return failureValidation{Reason: reasonInvalidType}
	}
	errorCode, ok := strictFailureString(members["error_code"])
	if !ok {
		return failureValidation{Reason: reasonInvalidType}
	}
	errorMessage, ok := strictFailureString(members["error_message"])
	if !ok {
		return failureValidation{Reason: reasonInvalidType}
	}
	if eventID == "" {
		return failureValidation{Reason: reasonEmptyEventID}
	}
	tenantID := ""
	if raw, present := members["tenant_id"]; present {
		var valid bool
		tenantID, valid = strictFailureString(raw)
		if !valid {
			return failureValidation{Reason: reasonInvalidType}
		}
	}
	return failureValidation{Failure: Failure{
		EventID: eventID, ErrorCode: errorCode, ErrorMessage: errorMessage, TenantID: tenantID,
	}}
}

func strictFailureString(raw json.RawMessage) (string, bool) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", false
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", false
	}
	return value, true
}

const (
	maxMalformedLogDetails      = 10
	maxMalformedRecordsPerRound = 1024
	maxMalformedRecordBytes     = 256 << 10
	maxMalformedBytesPerRound   = 1 << 20
	maxDLQRecordsPerRound       = 4096
)

// malformedCollectionExceeded bounds the amount of malformed DLQ work kept
// in one round. Malformed payload bytes are never retained in dlqRecord; the
// byte budget also stops a flood before its physical identities accumulate
// without bound. The triggering record remains uncommitted for redelivery.
func malformedPayloadBytes(message kafka.Message) uint64 {
	return uint64(len(message.Key) + len(message.Value))
}

func malformedCollectionExceeded(count int, bytes uint64, message kafka.Message) bool {
	return count > maxMalformedRecordsPerRound ||
		malformedPayloadBytes(message) > maxMalformedRecordBytes ||
		bytes > maxMalformedBytesPerRound
}

// dlqRecordIdentity is the only commit identity needed after collection.
// Production collection deliberately does not retain the fetched message's
// key/value bytes, including for valid records.
func dlqRecordIdentity(record dlqRecord) dlqRecordID {
	if record.id != (dlqRecordID{}) {
		return record.id
	}
	return dlqRecordID{topic: record.message.Topic, partition: record.message.Partition, offset: record.message.Offset}
}

func dlqRecordCommitMessage(record dlqRecord) kafka.Message {
	id := dlqRecordIdentity(record)
	return kafka.Message{Topic: id.topic, Partition: id.partition, Offset: id.offset}
}

func logMalformedRecord(logger *log.Logger, topic string, partition int, offset int64, reason failureReason) {
	logger.Printf("dlq record malformed topic=%s partition=%d offset=%d reason=%s",
		sanitizeLogField(topic, 64), partition, offset, reason)
}

func logMalformedSummary(logger *log.Logger, count int) {
	logger.Printf("dlq round: malformed_records=%d", count)
}

// sanitizeLogField strips control characters and limits attacker-influenced
// metadata before it is written to a log.
func sanitizeLogField(value string, limit int) string {
	sanitized := strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '?'
		}
		return r
	}, value)
	runes := []rune(sanitized)
	if len(runes) > limit {
		runes = runes[:limit]
	}
	return string(runes)
}

type dlqRecordClass uint8

const (
	dlqRecordValid dlqRecordClass = iota
	dlqRecordMalformed
)
