package service

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestExportAccessDetailObservesForwardSealedCompletion(t *testing.T) {
	svc, _, objects, _, cp := checkpointHoldFixture(t, "")
	accessNonmatchingHold(t, svc)
	other, err := New(svc.Store, svc.Config)
	if err != nil {
		t.Fatal("could not create independent executor")
	}
	data := leaseSnapshot(t, svc)
	claim := exportClaim{ExportJob: data.Exports[cp.JobID], Owner: data.ExportLeases[cp.JobID].Owner, Checkpoint: cp}
	boundary := newCheckpointHoldBoundaryArchive(objects)
	svc.Config.Archive = boundary
	var once sync.Once
	release := func() { once.Do(func() { close(boundary.release) }) }
	done, exited := make(chan error, 1), make(chan struct{})
	var observed domain.ExportJob
	t.Cleanup(func() { release(); <-exited })
	go func() {
		defer close(exited)
		var err error
		observed, err = svc.GetExport(testCtx, "tenant-a", cp.JobID)
		done <- err
	}()
	requireCheckpointContext(t, boundary.entered)
	puts := objects.puts.Load()
	other.finalizeExportCheckpoint(exportClaimContext(testCtx, claim), claim, nil)
	release()
	if err := awaitExportAccess(t, done); err != nil || observed.Status != "completed" || observed.ObjectPath != cp.ObjectPath || observed.Digest != cp.Digest || observed.EventCount != cp.EventCount {
		t.Fatal("normal forward progress was rejected or status polling returned an obsolete job")
	}
	if objects.puts.Load() != puts || checkpointFactCount(t, svc, domain.AdminActionExportCompleted) != 1 {
		t.Fatal("detail access regenerated the artifact or duplicated completion")
	}
}

func TestExportAccessDetailRejectsSelectionAndBackwardDrift(t *testing.T) {
	for _, drift := range []string{"query", "requester", "lineage", "creation-time", "map-identity", "backward-state"} {
		t.Run(drift, func(t *testing.T) {
			svc, _, objects, _, cp := checkpointHoldFixture(t, "")
			accessNonmatchingHold(t, svc)
			boundary := newCheckpointHoldBoundaryArchive(objects)
			svc.Config.Archive = boundary
			var once sync.Once
			release := func() { once.Do(func() { close(boundary.release) }) }
			done, exited := make(chan error, 1), make(chan struct{})
			t.Cleanup(func() { release(); <-exited })
			go func() { defer close(exited); done <- callExportAccess(t, testCtx, svc, "detail", "tenant-a", cp.JobID) }()
			requireCheckpointContext(t, boundary.entered)
			if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
				job := data.Exports[cp.JobID]
				switch drift {
				case "query":
					job.Query.ActorID = "obsolete-writer-drift"
				case "requester":
					job.RequestedBy = "different-requester"
				case "lineage":
					job.RetryOf = "different-source"
				case "creation-time":
					job.CreatedAt = job.CreatedAt.AddDate(0, 0, 1)
				case "map-identity":
					job.ID = "different-job"
				case "backward-state":
					job.Status = "pending"
				}
				data.Exports[cp.JobID] = job
				return nil
			}); err != nil {
				t.Fatal("could not install owned control drift at actual source Get")
			}
			release()
			if err := awaitExportAccess(t, done); !errors.Is(err, domain.ErrConflict) {
				t.Fatal("changed access scope or backwards state was accepted")
			}
			if leaseSnapshot(t, svc).ExportCheckpoints[cp.JobID] != cp {
				t.Fatal("detail verification overwrote or discarded execution metadata")
			}
		})
	}
}

func TestExportAccessUnknownAndForeignJobsReadNoSource(t *testing.T) {
	for _, surface := range []string{"detail", "download-audit"} {
		t.Run(surface, func(t *testing.T) {
			svc, _, objects, _, job := completedCheckpointAccessFixture(t, "", 10)
			accessNonmatchingHold(t, svc)
			gets, puts := objects.gets.Load(), objects.puts.Load()
			for _, identity := range [][2]string{{"tenant-b", job.ID}, {"tenant-a", "unknown-job"}} {
				if err := callExportAccess(t, testCtx, svc, surface, identity[0], identity[1]); !errors.Is(err, domain.ErrNotFound) {
					t.Fatal("foreign or unknown job was not existence-masked")
				}
			}
			if objects.gets.Load() != gets || objects.puts.Load() != puts || !reflect.DeepEqual(leaseSnapshot(t, svc).Exports[job.ID], job) || checkpointFactCount(t, svc, domain.AdminActionEventExport) != 0 {
				t.Fatal("foreign or unknown access touched source/output or recorded success")
			}
		})
	}
}
