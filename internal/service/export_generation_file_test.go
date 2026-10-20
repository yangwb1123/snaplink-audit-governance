package service

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestExportGenerationFileRestartContinuesBytesBeyondQueryCursorLifetime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	svc := newServiceOn(t, st)
	seedTestDomain(t, svc)
	objects := &checkpointArchive{Store: &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")}}
	svc.Config.Archive = objects
	seedGenerationEvents(t, svc, 0, store.ExportGenerationPageEvents+1)
	seedLeaseExport(t, svc, "checkpoint-job", "tenant-a")
	if err := st.UpdateControl(testCtx, func(data *store.Snapshot) error {
		job := data.Exports["checkpoint-job"]
		job.Query.Cursor = "retired-query-cursor-fixture"
		data.Exports[job.ID] = job
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	old := leaveGeneratedPage(t, svc) // real Put/Save, stop before the next page
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted := newServiceOn(t, reopened)
	restarted.Config.Archive = objects
	restarted.Config.Now = func() time.Time { return pinnedNow.Add(time.Hour) }
	if !reflect.DeepEqual(leaseSnapshot(t, restarted).ExportPlans[old.ID], old.Plan) {
		t.Fatal("file restart lost exact generated prefix")
	}
	late, err := restarted.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("generation-file-late", "later-operation", pinnedNow.Add(30*time.Second)), domain.StatusArchived)
	if err != nil {
		t.Fatal(err)
	}
	probe := &generationArchive{Store: objects, forbiddenSources: generatedSourceKeys(old.Plan)}
	probe.forbiddenSources[eventArchiveKey("tenant-a", late.StreamID, late.Sequence, late.EventID)] = true
	restarted.Config.Archive = probe
	restarted.runExport(testCtx, old.ID)
	data := leaseSnapshot(t, restarted)
	job := data.Exports[old.ID]
	if job.Status != "completed" || job.EventCount != len(old.Plan.Members) || !job.StartedAt.Equal(*old.StartedAt) || len(data.ExportPlans) != 0 || len(data.ExportCheckpoints) != 0 || len(data.ExportLeases) != 0 || probe.sourceGets.Load() != 1 || probe.chunkGets.Load() != 1 || probe.chunkPuts.Load() != 1 || probe.finalPuts.Load() != 1 {
		t.Fatal("restarted executor reselected/regenerated old bytes or lost its durable phase transition")
	}
	sealed, err := objects.Get(testCtx, job.ObjectPath)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := restarted.VerifyExportDownload(testCtx, old.TenantID, old.ID, sealed)
	if err != nil || !exportPlainMatchesPlan(plain, old.Plan) {
		t.Fatal("file restart did not preserve original ordered JSONL evidence")
	}
	if checkpointFactCount(t, restarted, domain.AdminActionExportProgressed) != 2 || checkpointFactCount(t, restarted, domain.AdminActionExportResumed) != 1 || checkpointFactCount(t, restarted, domain.AdminActionExportCompleted) != 1 {
		t.Fatal("file restart lost or duplicated durable stage facts")
	}
	if _, err := objects.Get(testCtx, old.Plan.Chunks[0].ObjectPath); err != nil {
		t.Fatal("completion deleted immutable generated page")
	}
}
