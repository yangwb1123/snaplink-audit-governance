package domain

import (
	"fmt"
	"sort"
)

// Ingest envelope caps shared by the gRPC and HTTP ingest surfaces. Every
// string field of the envelope that is persisted verbatim into the
// hash-chained ledger (reason, trace_id, actor.*, target.*, changed_fields
// JSON strings, payload_ref, ids, ...) is bounded by MaxEnvelopeFieldBytes
// (byte count, matching http.MaxBytesReader semantics); the repeated fields
// are bounded by their arity caps; a batch is bounded by MaxBatchEvents so
// one request cannot trigger an unbounded number of full-snapshot ledger
// updates. 500 x typical envelope stays well below the 512KB transport cap
// (MaxEventBytes*2), keeping the pre-flight count cap the effective bound
// for count (the transport cap remains the byte bound).
const (
	MaxEnvelopeFieldBytes = 8 * 1024

	MaxActorRoles      = 64  // repeated Actor.roles count
	MaxTargetsPerEvent = 64  // repeated Target targets count
	MaxChangedFields   = 256 // changed_fields unique-key count

	// MaxBatchEvents caps WriteBatchRequest.events / HTTP batch events.
	MaxBatchEvents = 500
)

// ErrEnvelopeTooLarge wraps ErrInvalid: an envelope (or batch) exceeded a
// configured size/arity cap. Distinct from other ErrInvalid classes so the
// gRPC WriteStream can skip-and-continue only this class while preserving
// the pinned termination semantics of every other rejection. On the HTTP
// surface it maps to 400 invalid_request through the existing ErrInvalid
// branch.
var ErrEnvelopeTooLarge = fmt.Errorf("%w: envelope size cap exceeded", ErrInvalid)

// CheckString rejects a verbatim-persisted string field above
// MaxEnvelopeFieldBytes with the shared "<name> is <N> bytes, max <M>"
// message. Both transports call this helper, so message format drift between
// the gRPC and HTTP validators is impossible by construction.
func CheckString(name, value string) error {
	if len(value) > MaxEnvelopeFieldBytes {
		return fmt.Errorf("%w: %s is %d bytes, max %d", ErrEnvelopeTooLarge, name, len(value), MaxEnvelopeFieldBytes)
	}
	return nil
}

// CheckCount rejects a repeated field above its arity cap with the shared
// "<name> count <N>, max <M>" message.
func CheckCount(name string, n, max int) error {
	if n > max {
		return fmt.Errorf("%w: %s count %d, max %d", ErrEnvelopeTooLarge, name, n, max)
	}
	return nil
}

// ValidateEventCaps is the HTTP-surface mirror of the gRPC proto validator
// (grpcapi.validateEnvelopeCaps) applied to the decoded domain.Event. Check
// ordering and message strings are identical to the proto validator, so the
// same logical violation reports the same text on both transports:
// event-level strings, actor strings, actor.roles[] elements, actor.roles
// count, targets count, target strings, changed_fields count, then each
// changed-field key and non-nil before/after value. changed_fields values
// are measured as len(CanonicalJSON(value)) — the same canonical
// serialization used for digest derivation; nil before/after are skipped
// (equivalent to the gRPC empty-string pass). Exempt: payload (bounded by
// the service-layer payload cap), occurred_at (422 horizon window),
// integers, and server-assigned fields. changed_fields keys are walked in
// sorted order so the reported first violation is deterministic.
func ValidateEventCaps(event Event) error {
	for _, f := range []struct{ name, value string }{
		{"event_id", event.EventID},
		{"source_system", event.SourceSystem},
		{"event_type", event.EventType},
		{"schema_id", event.SchemaID},
		{"operation_id", event.OperationID},
		{"causation_id", event.CausationID},
		{"correlation_id", event.CorrelationID},
		{"trace_id", event.TraceID},
		{"span_id", event.SpanID},
		{"aggregate_type", event.AggregateType},
		{"aggregate_id", event.AggregateID},
		{"action", event.Action},
		{"outcome", event.Outcome},
		{"reason", event.Reason},
		{"payload_ref", event.PayloadRef},
		{"data_classification", event.DataClassification},
		{"retention_class", event.RetentionClass},
		{"idempotency_key", event.IdempotencyKey},
		{"workflow_instance_id", event.WorkflowInstanceID},
		{"execution_run_id", event.ExecutionRunID},
	} {
		if err := CheckString(f.name, f.value); err != nil {
			return err
		}
	}
	for _, f := range []struct{ name, value string }{
		{"actor.id", event.Actor.ID},
		{"actor.type", event.Actor.Type},
		{"actor.name", event.Actor.Name},
		{"actor.department", event.Actor.Department},
	} {
		if err := CheckString(f.name, f.value); err != nil {
			return err
		}
	}
	for _, role := range event.Actor.Roles {
		if err := CheckString("actor.roles[]", role); err != nil {
			return err
		}
	}
	if err := CheckCount("actor.roles", len(event.Actor.Roles), MaxActorRoles); err != nil {
		return err
	}
	if err := CheckCount("targets", len(event.Targets), MaxTargetsPerEvent); err != nil {
		return err
	}
	for _, target := range event.Targets {
		for _, f := range []struct{ name, value string }{
			{"target.type", target.Type},
			{"target.id", target.ID},
			{"target.name", target.Name},
		} {
			if err := CheckString(f.name, f.value); err != nil {
				return err
			}
		}
	}
	if err := CheckCount("changed_fields", len(event.ChangedFields), MaxChangedFields); err != nil {
		return err
	}
	keys := make([]string, 0, len(event.ChangedFields))
	for field := range event.ChangedFields {
		keys = append(keys, field)
	}
	sort.Strings(keys)
	for _, field := range keys {
		if err := CheckString("changed_fields[].field", field); err != nil {
			return err
		}
		change := event.ChangedFields[field]
		if change.Before != nil {
			encoded, err := CanonicalJSON(change.Before)
			if err != nil {
				// Unreachable via JSON decode (decodeBody accepts any JSON
				// value), but a hand-built domain.Event could carry an
				// unsupported value; fail closed with ErrInvalid so nothing
				// persists and the response stays 400 invalid_request.
				return fmt.Errorf("%w: changed_fields[].before_json is invalid", ErrInvalid)
			}
			if err := CheckString("changed_fields[].before_json", string(encoded)); err != nil {
				return err
			}
		}
		if change.After != nil {
			encoded, err := CanonicalJSON(change.After)
			if err != nil {
				return fmt.Errorf("%w: changed_fields[].after_json is invalid", ErrInvalid)
			}
			if err := CheckString("changed_fields[].after_json", string(encoded)); err != nil {
				return err
			}
		}
	}
	return nil
}
