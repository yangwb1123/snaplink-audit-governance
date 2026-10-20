package service

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func schemaControlCandidate(at time.Time) domain.EventSchema {
	return domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 2,
		EventType: "audit.event", RequiredFields: []string{"resource"},
		AllowedFields: []string{"resource", "value", "extra"}, CreatedAt: at, Active: true}
}

// The new exact definition and one original fact are the entire delta. Inspect
// a private copy; never rewrite the database or source to satisfy the oracle.
func assertSchemaControlDelta(t *testing.T, before, after *store.Snapshot, schema domain.EventSchema) {
	t.Helper()
	key := store.SchemaKey(schema.TenantID, schema.SchemaID, schema.Version)
	if !reflect.DeepEqual(after.Schemas[key], schema) || len(after.AdminActions) != len(before.AdminActions)+1 {
		t.Fatal("schema registration lost its exact definition or one atomic fact")
	}
	fact := after.AdminActions[len(after.AdminActions)-1]
	if fact.ID == "" || fact.TenantID != schema.TenantID || fact.Actor != "schema-manager" ||
		fact.Action != domain.AdminActionSchemaCreated || fact.TargetType != "event_schema" ||
		fact.TargetID != schema.SchemaID || fact.Detail != fmt.Sprintf("version %d", schema.Version) || fact.CreatedAt.IsZero() {
		t.Error("schema registration changed its original fact identity, scope or encoding")
	}
	copy, err := cloneTestSnapshot(after)
	if err != nil {
		t.Fatal("could not inspect owned schema delta")
	}
	delete(copy.Schemas, key)
	copy.AdminActions = copy.AdminActions[:len(copy.AdminActions)-1]
	if !reflect.DeepEqual(copy, before) {
		t.Error("schema registration replaced prior definitions, evidence or unrelated control")
	}
}

// Reuse the actual Backend.Save seam. Unknown outcomes really persist the
// definition and fact before reporting failure; they are not rollback proof.
func TestSchemaControlSaveOutcomesKeepAtomicFactAndOriginalBudget(t *testing.T) {
	for _, mode := range []string{"success", "conflict", "exhausted", "failure", "unknown", "deadline", "cancelled", "withdrawn-save", "committed-withdrawal"} {
		t.Run(mode, func(t *testing.T) {
			backend := &restoreDecisionBackend{checkpointBackend: &checkpointBackend{data: store.NewSnapshot()}}
			svc := newServiceOn(t, store.NewWithBackend(backend))
			t.Cleanup(func() { _ = svc.Store.Close() })
			seedTestDomain(t, svc)
			before := restoreCreationSnapshot(t, svc)
			objects := &checkpointArchive{}
			svc.Config.Archive = objects // zero-I/O oracle; any attempted source access fails
			backend.calls, backend.loads, backend.writes, backend.mode = 0, nil, nil, mode
			if mode == "conflict" {
				backend.conflicts = 2
			} else if mode == "exhausted" {
				backend.conflicts = 4
			}
			if mode == "deadline" {
				svc.Store = store.NewWithBackendOptions(backend, store.Options{OperationTimeout: 200 * time.Millisecond})
			}
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			backend.withdraw = cancel
			if mode == "cancelled" {
				cancel()
			}
			schema := schemaControlCandidate(svc.Now())
			err := svc.RegisterSchema(ctx, "schema-manager", schema)
			known := mode == "success" || mode == "conflict"
			if known && err != nil || !known && err == nil {
				t.Error("schema Save incorrectly reported its known or failed outcome")
			}
			if (mode == "failure" || mode == "unknown") && !errors.Is(err, errRestoreDecisionSave) {
				t.Error("schema registration hid the original Save failure")
			}
			if mode == "exhausted" && !errors.Is(err, store.ErrSnapshotConflict) {
				t.Error("schema registration lost finite conflict classification")
			}
			if mode == "deadline" && (!errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || svc.Store.OperationDeadlineExhaustions() != 1) {
				t.Error("schema registration restarted or misclassified the original deadline")
			}
			if (mode == "cancelled" || mode == "withdrawn-save" || mode == "committed-withdrawal") && !errors.Is(err, context.Canceled) {
				t.Error("schema registration lost caller withdrawal")
			}
			wantSaves := 1
			switch mode {
			case "conflict":
				wantSaves = 3
			case "exhausted":
				wantSaves = 4
			case "cancelled":
				wantSaves = 0
			}
			if backend.calls != wantSaves || objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(readFacts(t, svc)) != 0 {
				t.Error("schema registration changed finite Saves, performed source I/O or invented a read fact")
			}
			if len(backend.writes) > 0 {
				for _, deadline := range append(append([]time.Time{}, backend.loads...), backend.writes...) {
					if deadline.IsZero() || !deadline.Equal(backend.writes[0]) {
						t.Error("schema registration retry restarted its operation budget")
					}
				}
			}
			current := restoreCreationSnapshot(t, svc)
			if known || mode == "unknown" || mode == "committed-withdrawal" {
				assertSchemaControlDelta(t, before, current, schema)
			} else if !reflect.DeepEqual(current, before) {
				t.Error("uncommitted schema leaked a definition or a fact")
			}
		})
	}
}
