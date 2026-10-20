package service

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func retentionControlPolicy() domain.RetentionPolicy {
	return domain.RetentionPolicy{TenantID: "tenant-a", HotDays: 7, WarmDays: 30, ArchiveDays: 365, RetentionClass: "standard"}
}

// Inspect a private copy only. Policy and its one original fact are the entire
// intended delta; all source evidence and current execution authority survive.
func assertRetentionControlDelta(t *testing.T, before, after *store.Snapshot, policy domain.RetentionPolicy) {
	t.Helper()
	if after.Policies[policy.TenantID] != policy || len(after.AdminActions) != len(before.AdminActions)+1 {
		t.Fatal("policy lost its exact configuration or one atomic fact")
	}
	fact := after.AdminActions[len(after.AdminActions)-1]
	if fact.ID == "" || fact.TenantID != policy.TenantID || fact.Actor != "policy-manager" ||
		fact.Action != domain.AdminActionRetentionPolicySet || fact.TargetType != "retention_policy" ||
		fact.TargetID != policy.RetentionClass || fact.Detail != "hot=7 warm=30 archive=365" {
		t.Error("policy changed its original fact identity, scope or encoding")
	}
	copy, err := cloneTestSnapshot(after)
	if err != nil {
		t.Fatal("could not inspect owned policy delta")
	}
	if previous, ok := before.Policies[policy.TenantID]; ok {
		copy.Policies[policy.TenantID] = previous
	} else {
		delete(copy.Policies, policy.TenantID)
	}
	copy.AdminActions = copy.AdminActions[:len(copy.AdminActions)-1]
	if !reflect.DeepEqual(copy, before) {
		t.Error("policy Save replaced unrelated control or original evidence")
	}
}

func TestRetentionControlFilePreservesEvidenceAndUnknownTemp(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		name := "committed-restart"
		if blocked {
			name = "unknown-temp"
		}
		t.Run(name, func(t *testing.T) {
			svc, path, _ := restoreDecisionFileFixture(t)
			before := restoreCreationSnapshot(t, svc)
			images := admissionEvidenceFiles(t, filepath.Dir(path))
			if len(images) != 4 || len(before.Events) != 0 || len(before.ExportPlans["admission-neighbor"].Members) != 1 {
				t.Fatal("policy oracle requires two real tenant layouts, evicted source and authenticated selected stage")
			}
			reads := len(readFacts(t, svc))
			objects := &checkpointArchive{Store: svc.Config.Archive}
			svc.Config.Archive = objects
			unknown := []byte("owned-retention-control-unknown-temp")
			if blocked {
				if err := os.WriteFile(path+".tmp", unknown, 0o600); err != nil {
					t.Fatal("could not establish owned unknown control temp")
				}
			}
			policy := retentionControlPolicy()
			err := svc.SetRetentionPolicy(testCtx, "policy-manager", policy)
			if objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(readFacts(t, svc)) != reads {
				t.Error("policy configuration performed source I/O or changed independent read facts")
			}
			assertAdmissionEvidenceFilesUnchanged(t, images)
			if blocked {
				raw, readErr := os.ReadFile(path + ".tmp")
				if err == nil || readErr != nil || !bytes.Equal(raw, unknown) || !reflect.DeepEqual(restoreCreationSnapshot(t, svc), before) {
					t.Error("failed policy Save discarded unknown bytes or published partial configuration/facts")
				}
			} else {
				if err != nil {
					t.Fatal("original policy configuration did not commit")
				}
				assertRetentionControlDelta(t, before, restoreCreationSnapshot(t, svc), policy)
				if err := svc.Store.Close(); err != nil {
					t.Fatal("could not close original policy writer")
				}
				reopened := reopenHoldControlFile(t, path)
				reopened.Config.Archive = objects
				assertRetentionControlDelta(t, before, restoreCreationSnapshot(t, reopened), policy)
				assertAdmissionEvidenceFilesUnchanged(t, images)
				if len(readFacts(t, reopened)) != reads {
					t.Error("policy restart changed independent read facts")
				}
				if _, err := reopened.GetExport(testCtx, "tenant-a", "admission-neighbor"); !errors.Is(err, domain.ErrForbidden) {
					t.Error("policy configuration bypassed the independent current Hold")
				}
			}
		})
	}
}
