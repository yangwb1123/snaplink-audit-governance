package store

import (
	"encoding/json"
	"maps"
	"slices"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

// cloneControl copies only the control document, not evidence that a legacy
// caller may still carry in the same Snapshot. Dynamic outbox graphs retain
// CloneEvent's type/precision/typed-nil and fail-closed ownership contract.
// Keep the field coverage test in step with Snapshot and its nested types.
func cloneControl(control *Snapshot) (*Snapshot, error) {
	if err := validateControlCloneTimes(control); err != nil {
		return nil, err
	}
	clone := *control
	clone.Tenants = maps.Clone(control.Tenants)
	clone.Sources = maps.Clone(control.Sources)
	for key, source := range clone.Sources {
		source.AllowedClientIDs = cloneControlOptionalSlice(source.AllowedClientIDs)
		clone.Sources[key] = source
	}
	clone.Schemas = maps.Clone(control.Schemas)
	for key, schema := range clone.Schemas {
		schema.RequiredFields = cloneControlOptionalSlice(schema.RequiredFields)
		schema.AllowedFields = cloneControlOptionalSlice(schema.AllowedFields)
		schema.EncryptedFields = cloneControlOptionalSlice(schema.EncryptedFields)
		schema.SearchableFields = cloneControlOptionalSlice(schema.SearchableFields)
		clone.Schemas[key] = schema
	}
	clone.Policies = maps.Clone(control.Policies)
	clone.Events = map[string]domain.Event{}
	clone.Receipts = map[string]domain.EventReceipt{}
	clone.Streams = map[string]StreamState{}
	clone.Segments = map[string][]domain.Segment{}
	clone.Checkpoints = map[string][]domain.Checkpoint{}
	clone.LegalHolds = maps.Clone(control.LegalHolds)
	for key, hold := range clone.LegalHolds {
		hold.ReleasedAt = cloneControlTimePointer(hold.ReleasedAt)
		clone.LegalHolds[key] = hold
	}
	clone.Exports = maps.Clone(control.Exports)
	for key, job := range clone.Exports {
		job.StartedAt = cloneControlTimePointer(job.StartedAt)
		job.FinishedAt = cloneControlTimePointer(job.FinishedAt)
		clone.Exports[key] = job
	}
	clone.ExportLeases = maps.Clone(control.ExportLeases)
	clone.ExportCheckpoints = maps.Clone(control.ExportCheckpoints)
	clone.ExportPlans = maps.Clone(control.ExportPlans)
	for key, plan := range clone.ExportPlans {
		plan.Members = slices.Clone(plan.Members)
		plan.Chunks = cloneControlOptionalSlice(plan.Chunks)
		clone.ExportPlans[key] = plan
	}
	clone.RestoreRuns = maps.Clone(control.RestoreRuns)
	for key, run := range clone.RestoreRuns {
		run.ApprovedAt = cloneControlTimePointer(run.ApprovedAt)
		run.RejectedAt = cloneControlTimePointer(run.RejectedAt)
		clone.RestoreRuns[key] = run
	}
	var err error
	clone.LedgeredOutbox, err = cloneEventMap(control.LedgeredOutbox)
	if err != nil {
		return nil, err
	}
	clone.AdminActions = slices.Clone(control.AdminActions)
	clone.AggregateCheckpoints = slices.Clone(control.AggregateCheckpoints)
	for i := range clone.AggregateCheckpoints {
		checkpoint := &clone.AggregateCheckpoints[i]
		checkpoint.StreamRoots = cloneControlOptionalSlice(checkpoint.StreamRoots)
		checkpoint.CheckpointRefs = cloneControlOptionalSlice(checkpoint.CheckpointRefs)
	}
	clone.ArchiveConflictFailures = maps.Clone(control.ArchiveConflictFailures)
	clone.DeadLetters = maps.Clone(control.DeadLetters)
	clone.normalize()
	return &clone, nil
}

// The former JSON copy omitted empty optional control slices. Preserve that
// in-memory compatibility as well as normalize's non-nil top-level maps/lists.
// Required plan Members and dynamic event slices instead preserve nil/empty.
func cloneControlOptionalSlice[T any](values []T) []T {
	if len(values) == 0 {
		return nil
	}
	return slices.Clone(values)
}

func cloneControlTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

// Removing a JSON round trip must not let malformed metadata times enter an
// in-memory commit that the old clone rejected. AppendText has the same strict
// RFC3339 checks without allocating on success; marshal only on failure to
// preserve the original json.MarshalerError/Time.MarshalJSON error semantics.
// time.Time (including its Location) is otherwise an immutable value.
func checkControlCloneTimes(values ...time.Time) error {
	var buffer [64]byte
	for _, value := range values {
		if _, err := value.AppendText(buffer[:0]); err != nil {
			_, err = json.Marshal(value)
			return err
		}
	}
	return nil
}

func validateControlCloneTimes(data *Snapshot) error {
	for _, tenant := range data.Tenants {
		if err := checkControlCloneTimes(tenant.CreatedAt); err != nil {
			return err
		}
	}
	for _, source := range data.Sources {
		if err := checkControlCloneTimes(source.CreatedAt); err != nil {
			return err
		}
	}
	for _, schema := range data.Schemas {
		if err := checkControlCloneTimes(schema.CreatedAt); err != nil {
			return err
		}
	}
	for _, hold := range data.LegalHolds {
		if err := checkControlCloneTimes(hold.CreatedAt, hold.Filter.From, hold.Filter.To); err != nil {
			return err
		}
		if hold.ReleasedAt != nil {
			if err := checkControlCloneTimes(*hold.ReleasedAt); err != nil {
				return err
			}
		}
	}
	for _, job := range data.Exports {
		if err := checkControlCloneTimes(job.CreatedAt, job.Query.From, job.Query.To); err != nil {
			return err
		}
		for _, value := range []*time.Time{job.StartedAt, job.FinishedAt} {
			if value != nil {
				if err := checkControlCloneTimes(*value); err != nil {
					return err
				}
			}
		}
	}
	for _, lease := range data.ExportLeases {
		if err := checkControlCloneTimes(lease.HeartbeatAt, lease.ExpiresAt); err != nil {
			return err
		}
	}
	for _, checkpoint := range data.ExportCheckpoints {
		if err := checkControlCloneTimes(checkpoint.CreatedAt); err != nil {
			return err
		}
	}
	for _, plan := range data.ExportPlans {
		if err := validateControlClonePlanTimes(plan); err != nil {
			return err
		}
	}
	for _, run := range data.RestoreRuns {
		if err := checkControlCloneTimes(run.CreatedAt); err != nil {
			return err
		}
		for _, value := range []*time.Time{run.ApprovedAt, run.RejectedAt} {
			if value != nil {
				if err := checkControlCloneTimes(*value); err != nil {
					return err
				}
			}
		}
	}
	for _, action := range data.AdminActions {
		if err := checkControlCloneTimes(action.CreatedAt); err != nil {
			return err
		}
	}
	for _, checkpoint := range data.AggregateCheckpoints {
		if err := checkControlCloneTimes(checkpoint.CreatedAt); err != nil {
			return err
		}
	}
	for _, letter := range data.DeadLetters {
		if err := checkControlCloneTimes(letter.At); err != nil {
			return err
		}
	}
	return nil
}

func validateControlClonePlanTimes(plan ExportPlan) error {
	if err := checkControlCloneTimes(plan.CreatedAt); err != nil {
		return err
	}
	for _, member := range plan.Members {
		if err := checkControlCloneTimes(member.OccurredAt); err != nil {
			return err
		}
	}
	for _, chunk := range plan.Chunks {
		if err := checkControlCloneTimes(chunk.CreatedAt); err != nil {
			return err
		}
	}
	return nil
}
