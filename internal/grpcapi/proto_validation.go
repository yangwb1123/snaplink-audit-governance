package grpcapi

import (
	"fmt"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/domain"
	"google.golang.org/protobuf/proto"
)

func rejectUnknown(path string, message proto.Message) error {
	if len(message.ProtoReflect().GetUnknown()) != 0 {
		return fmt.Errorf(
			"%w: unknown protobuf fields in %s",
			domain.ErrInvalid,
			path,
		)
	}
	return nil
}

func validateEnvelopeUnknownFields(input *auditv1.EventEnvelope) error {
	if err := rejectUnknown("EventEnvelope", input); err != nil {
		return err
	}
	if actor := input.GetActor(); actor != nil {
		if err := rejectUnknown("EventEnvelope.actor", actor); err != nil {
			return err
		}
	}
	for i, target := range input.GetTargets() {
		if target == nil {
			continue
		}
		if err := rejectUnknown(fmt.Sprintf("EventEnvelope.targets[%d]", i), target); err != nil {
			return err
		}
	}
	for i, change := range input.GetChangedFields() {
		if change == nil {
			continue
		}
		if err := rejectUnknown(fmt.Sprintf("EventEnvelope.changed_fields[%d]", i), change); err != nil {
			return err
		}
	}
	return nil
}

func rejectDuplicateChangedFields(changes []*auditv1.FieldChange) error {
	seen := make(map[string]struct{}, len(changes))
	for _, change := range changes {
		field := change.GetField()
		if _, exists := seen[field]; exists {
			return fmt.Errorf(
				"%w: duplicate changed_fields.field %q",
				domain.ErrInvalid,
				field,
			)
		}
		seen[field] = struct{}{}
	}
	return nil
}
