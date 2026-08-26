package kafka

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"strings"
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

const maxMalformedLogDetails = 10

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
