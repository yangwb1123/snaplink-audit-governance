package outbox

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

// rowIdentity is the identity duplicated in audit_outbox columns. The valid
// bits let the reader report a legacy/null value as corruption instead of
// treating it as a query failure or silently accepting it.
type rowIdentity struct {
	EventID            string
	EventIDValid       bool
	TenantID           string
	TenantIDValid      bool
	IdempotencyKey     string
	IdemKeyValid       bool
	OccurredAt         time.Time
	OccurredValid      bool
	OccurredAtRaw      string
	OccurredAtRawValid bool
}

// scanRowOccurredAt converts the driver value for a PostgreSQL timestamptz
// into a finite time. pgx returns ordinary values as time.Time but exposes
// PostgreSQL infinity sentinels as strings; unsupported or non-finite values
// are retained for a useful mismatch diagnostic and treated as invalid.
func scanRowOccurredAt(value any) (time.Time, bool, string, bool) {
	switch typed := value.(type) {
	case nil:
		return time.Time{}, false, "", false
	case time.Time:
		return typed, true, "", false
	case string:
		return parseRowOccurredAt(typed)
	case []byte:
		return parseRowOccurredAt(string(typed))
	default:
		return time.Time{}, false, fmt.Sprintf("%v", typed), true
	}
}

func parseRowOccurredAt(value string) (time.Time, bool, string, bool) {
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05.999999Z07",
		"2006-01-02 15:04:05.999999Z07:00",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05.999999 -0700 MST",
	} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, true, "", false
		}
	}
	return time.Time{}, false, value, true
}

// identityMismatch compares only the fields duplicated outside payload. It
// deliberately leaves the decoded event untouched: the payload is the value
// delivered after this check passes.
func identityMismatch(id int64, row rowIdentity, payload domain.Event) error {
	fields := make([]string, 0, 4)
	if !row.EventIDValid || row.EventID == "" || payload.EventID == "" || row.EventID != payload.EventID {
		fields = append(fields, identityField("event_id", stringValue(row.EventID, row.EventIDValid), stringValue(payload.EventID, true)))
	}
	if !row.TenantIDValid || row.TenantID == "" || payload.TenantID == "" || row.TenantID != payload.TenantID {
		fields = append(fields, identityField("tenant_id", stringValue(row.TenantID, row.TenantIDValid), stringValue(payload.TenantID, true)))
	}
	if !row.IdemKeyValid || row.IdempotencyKey == "" || payload.IdempotencyKey == "" || row.IdempotencyKey != payload.IdempotencyKey {
		fields = append(fields, identityField("idempotency_key", stringValue(row.IdempotencyKey, row.IdemKeyValid), stringValue(payload.IdempotencyKey, true)))
	}
	if !row.OccurredValid || row.OccurredAt.IsZero() || payload.OccurredAt.IsZero() ||
		!row.OccurredAt.UTC().Equal(payload.OccurredAt.UTC()) {
		fields = append(fields, identityField("occurred_at", rowOccurredValue(row), timeValue(payload.OccurredAt, true)))
	}
	if len(fields) == 0 {
		return nil
	}
	return fmt.Errorf("outbox identity mismatch id=%d: %s", id, strings.Join(fields, "; "))
}

func identityField(name, rowValue, payloadValue string) string {
	return fmt.Sprintf("%s row=%s payload=%s", name, rowValue, payloadValue)
}

func stringValue(value string, valid bool) string {
	if !valid {
		return "<null>"
	}
	return strconv.Quote(value)
}

func rowOccurredValue(row rowIdentity) string {
	if row.OccurredAtRawValid {
		return strconv.Quote(row.OccurredAtRaw)
	}
	return timeValue(row.OccurredAt, row.OccurredValid)
}

func timeValue(value time.Time, valid bool) string {
	if !valid || value.IsZero() {
		return "<empty>"
	}
	return strconv.Quote(value.UTC().Format(time.RFC3339Nano))
}
