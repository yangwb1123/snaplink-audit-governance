package grpcapi

import (
	"fmt"
	"strings"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/domain"
)

// MaxRecvBytes is the gRPC transport receive cap for Write/WriteBatch/
// WriteStream, set to the HTTP request-body cap (domain.MaxEventBytes*2 =
// 512KB) so both ingest transports bound a single request identically.
const MaxRecvBytes = domain.MaxEventBytes * 2

const (
	// These aliases preserve the grpcapi package API while making the domain
	// package the single source of truth for every transport's cap values.
	MaxEnvelopeFieldBytes = domain.MaxEnvelopeFieldBytes
	MaxActorRoles         = domain.MaxActorRoles
	MaxTargetsPerEvent    = domain.MaxTargetsPerEvent
	MaxChangedFields      = domain.MaxChangedFields
	MaxBatchEvents        = domain.MaxBatchEvents
)

// ErrEnvelopeTooLarge wraps domain.ErrInvalid: an envelope (or batch)
// exceeded a configured size/arity cap. Distinct from other ErrInvalid
// classes so WriteStream can skip-and-continue only this class while
// preserving the pinned termination semantics of every other rejection.
// It maps to codes.InvalidArgument through the existing toStatus switch.
var ErrEnvelopeTooLarge = domain.ErrEnvelopeTooLarge

func checkString(name, value string) error {
	return domain.CheckString(name, value)
}

// checkCanonicalJSON validates the wire spelling of a changed-field value,
// but applies the cap to the canonical value that reaches the domain event.
// Empty strings retain the proto contract's "value omitted" meaning. Parsing
// uses the same strict UseNumber decoder as fromProto so validation cannot
// accept a value that conversion later rejects.
func checkCanonicalJSON(name, value string) error {
	if value == "" {
		return nil
	}
	var decoded any
	invalidName := strings.TrimPrefix(name, "changed_fields[].")
	if err := decodeJSONNumber([]byte(value), &decoded); err != nil {
		return fmt.Errorf("%w: invalid %s", domain.ErrInvalid, invalidName)
	}
	canonical, err := domain.CanonicalJSON(decoded)
	if err != nil {
		return fmt.Errorf("%w: invalid %s", domain.ErrInvalid, invalidName)
	}
	return domain.CheckString(name, string(canonical))
}

func checkCount(name string, n, max int) error {
	return domain.CheckCount(name, n, max)
}

// validateEnvelopeCaps rejects any verbatim-persisted envelope string above
// MaxEnvelopeFieldBytes and any repeated field above its arity cap. Check
// ordering is deterministic: fromProto checks nil envelope, unknown supported
// messages, then this function. changed_fields count and field-name checks are
// kept separate from JSON validation so duplicate names are rejected before
// any changed-field JSON is decoded. payload_json is exempt: bounded by the
// service-layer payload cap and the transport cap.
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
		if err := checkString("changed_fields[].field", c.GetField()); err != nil {
			return err
		}
	}
	return nil
}

// validateChangedFieldCaps applies the existing canonical JSON size checks
// after duplicate names have been rejected by fromProto. Parsing here is only
// for cap measurement; conversion retains its own strict decode and error
// messages.
func validateChangedFieldCaps(input *auditv1.EventEnvelope) error {
	for _, c := range input.GetChangedFields() {
		if err := checkCanonicalJSON("changed_fields[].before_json", c.GetBeforeJson()); err != nil {
			return err
		}
		if err := checkCanonicalJSON("changed_fields[].after_json", c.GetAfterJson()); err != nil {
			return err
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
