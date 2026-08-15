package grpcapi

import (
	"fmt"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/domain"
)

// MaxRecvBytes is the gRPC transport receive cap for Write/WriteBatch/
// WriteStream, set to the HTTP request-body cap (domain.MaxEventBytes*2 =
// 512KB) so both ingest transports bound a single request identically.
const MaxRecvBytes = domain.MaxEventBytes * 2

const (
	// MaxEnvelopeFieldBytes caps every string field of EventEnvelope that is
	// persisted verbatim into the hash-chained ledger (reason, trace_id,
	// actor.*, target.*, changed_fields JSON strings, payload_ref, ids, ...).
	// Byte count, matching http.MaxBytesReader semantics.
	MaxEnvelopeFieldBytes = 8 * 1024

	MaxActorRoles      = 64  // repeated Actor.roles count
	MaxTargetsPerEvent = 64  // repeated Target targets count
	MaxChangedFields   = 256 // repeated FieldChange changed_fields count

	// MaxBatchEvents caps WriteBatchRequest.events so one RPC cannot trigger
	// an unbounded number of full-snapshot ledger updates. 500 x typical
	// envelope stays well below MaxRecvBytes (512KB), keeping this pre-flight
	// cap the effective bound for count (the transport cap remains the byte
	// bound).
	MaxBatchEvents = 500
)

// ErrEnvelopeTooLarge wraps domain.ErrInvalid: an envelope (or batch)
// exceeded a configured size/arity cap. Distinct from other ErrInvalid
// classes so WriteStream can skip-and-continue only this class while
// preserving the pinned termination semantics of every other rejection.
// It maps to codes.InvalidArgument through the existing toStatus switch.
var ErrEnvelopeTooLarge = fmt.Errorf("%w: envelope size cap exceeded", domain.ErrInvalid)

func checkString(name, value string) error {
	if len(value) > MaxEnvelopeFieldBytes {
		return fmt.Errorf("%w: %s is %d bytes, max %d", ErrEnvelopeTooLarge, name, len(value), MaxEnvelopeFieldBytes)
	}
	return nil
}

func checkCount(name string, n, max int) error {
	if n > max {
		return fmt.Errorf("%w: %s count %d, max %d", ErrEnvelopeTooLarge, name, n, max)
	}
	return nil
}

// validateEnvelopeCaps rejects any verbatim-persisted envelope string above
// MaxEnvelopeFieldBytes and any repeated field above its arity cap. Check
// ordering is deterministic: fromProto checks nil envelope, then this
// function, then the existing occurred_at/actor/decode checks. payload_json
// is exempt: bounded by the service-layer payload cap and the transport cap.
func validateEnvelopeCaps(input *auditv1.EventEnvelope) error {
	for _, f := range []struct{ name, value string }{
		{"event_id", input.GetEventId()},
		{"source_system", input.GetSourceSystem()},
		{"event_type", input.GetEventType()},
		{"schema_id", input.GetSchemaId()},
		{"operation_id", input.GetOperationId()},
		{"causation_id", input.GetCausationId()},
		{"correlation_id", input.GetCorrelationId()},
		{"trace_id", input.GetTraceId()},
		{"span_id", input.GetSpanId()},
		{"aggregate_type", input.GetAggregateType()},
		{"aggregate_id", input.GetAggregateId()},
		{"action", input.GetAction()},
		{"outcome", input.GetOutcome()},
		{"reason", input.GetReason()},
		{"payload_ref", input.GetPayloadRef()},
		{"data_classification", input.GetDataClassification()},
		{"retention_class", input.GetRetentionClass()},
		{"idempotency_key", input.GetIdempotencyKey()},
		{"workflow_instance_id", input.GetWorkflowInstanceId()},
		{"execution_run_id", input.GetExecutionRunId()},
	} {
		if err := checkString(f.name, f.value); err != nil {
			return err
		}
	}
	if actor := input.GetActor(); actor != nil {
		for _, f := range []struct{ name, value string }{
			{"actor.id", actor.GetId()},
			{"actor.type", actor.GetType()},
			{"actor.name", actor.GetName()},
			{"actor.department", actor.GetDepartment()},
		} {
			if err := checkString(f.name, f.value); err != nil {
				return err
			}
		}
		for _, role := range actor.GetRoles() {
			if err := checkString("actor.roles[]", role); err != nil {
				return err
			}
		}
		if err := checkCount("actor.roles", len(actor.GetRoles()), MaxActorRoles); err != nil {
			return err
		}
	}
	if err := checkCount("targets", len(input.GetTargets()), MaxTargetsPerEvent); err != nil {
		return err
	}
	for _, t := range input.GetTargets() {
		for _, f := range []struct{ name, value string }{
			{"target.type", t.GetType()},
			{"target.id", t.GetId()},
			{"target.name", t.GetName()},
		} {
			if err := checkString(f.name, f.value); err != nil {
				return err
			}
		}
	}
	if err := checkCount("changed_fields", len(input.GetChangedFields()), MaxChangedFields); err != nil {
		return err
	}
	for _, c := range input.GetChangedFields() {
		for _, f := range []struct{ name, value string }{
			{"changed_fields[].field", c.GetField()},
			{"changed_fields[].before_json", c.GetBeforeJson()},
			{"changed_fields[].after_json", c.GetAfterJson()},
		} {
			if err := checkString(f.name, f.value); err != nil {
				return err
			}
		}
	}
	return nil
}

// truncateEventID bounds an attacker-controlled event_id in server log lines
// to 64 bytes, so one skipped message can never push an unbounded,
// un-attributable blob into the operator log (the rejection detail still
// carries the full field/size reason).
func truncateEventID(eventID string) string {
	if len(eventID) > 64 {
		return eventID[:64]
	}
	return eventID
}
