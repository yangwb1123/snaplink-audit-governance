package service

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestCoordinateEvidenceFileRestartKeepsExactCatalogAndOriginalMembership(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal("could not open owned file coordinate fixture")
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := newServiceOn(t, st)
	seedFileTestDomain(t, svc)
	objects := &archive.FileStore{Dir: filepath.Join(dir, "archive")}
	svc.Config.Archive = objects
	for i, id := range []string{"initial-a", "initial-b"} {
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent(id, "initial-operation", pinnedNow.Add(time.Duration(i)*time.Second)), domain.StatusArchived); err != nil {
			t.Fatal("could not commit real file coordinate evidence")
		}
	}
	query := initialQuery()
	first, err := svc.QueryEvents(testCtx, "tenant-a", "reader", query)
	if err != nil || first.Count != 2 || first.NextCursor == "" {
		t.Fatal("could not persist original file membership before catalog fault")
	}
	original, err := st.Snapshot(testCtx)
	if err != nil {
		t.Fatal("could not capture original file receipts and exact catalog")
	}
	key := store.SchemaKey("tenant-a", "audit.event", 1)
	if err := st.UpdateControl(testCtx, func(data *store.Snapshot) error {
		schema := data.Schemas[key]
		schema.SchemaID = "misbound-file-schema"
		data.Schemas[key] = schema
		return nil
	}); err != nil || st.Close() != nil {
		t.Fatal("could not persist owned catalog fault and release file writer")
	}
	reopened, err := store.Open(path)
	if err != nil {
		t.Fatal("could not reopen original file coordinate state")
	}
	defer reopened.Close()
	next := newServiceOn(t, reopened)
	next.Config = svc.Config
	persisted, err := reopened.Snapshot(testCtx)
	if err != nil || persisted.Schemas[key].SchemaID != "misbound-file-schema" || !reflect.DeepEqual(persisted.Receipts, original.Receipts) {
		t.Fatal("restart oracle lost the actual catalog fault or changed cold receipts")
	}
	event, err := next.GetEvent(testCtx, "tenant-a", "reader", "initial-b")
	if !errors.Is(err, domain.ErrConflict) || !reflect.DeepEqual(event, domain.Event{}) {
		t.Fatal("file single-event read accepted persisted exact-Schema identity drift")
	}
	query.Cursor = first.NextCursor
	page, err := next.QueryEvents(testCtx, "tenant-a", "reader", query)
	if !errors.Is(err, domain.ErrConflict) || !reflect.DeepEqual(page, domain.QueryResult{}) || len(readFacts(t, next)) != 1 {
		t.Fatal("file continuation reselected, accepted catalog drift or appended a success fact")
	}
	// Repair only this disposable fixture's exact catalog; original immutable
	// evidence and durable membership are never rewritten or reselected.
	if err := reopened.UpdateControl(testCtx, func(data *store.Snapshot) error {
		schema := original.Schemas[key]
		schema.Active = false
		data.Schemas[key] = schema
		return nil
	}); err != nil {
		t.Fatal("could not restore owned historical exact definition")
	}
	recorder := &queryRecordingArchive{Store: objects, forbiddenID: "initial-a"}
	next.Config.Archive = recorder
	page, err = next.QueryEvents(testCtx, "tenant-a", "reader", query)
	current, snapshotErr := reopened.Snapshot(testCtx)
	if err != nil || page.Count != 2 || len(page.Items) != 1 || page.Items[0].EventID != "initial-b" || page.NextCursor != "" || recorder.reads != 1 || snapshotErr != nil || !reflect.DeepEqual(current.Receipts, original.Receipts) || len(readFacts(t, next)) != 2 {
		t.Fatal("file continuation lost original membership, reread its served prefix or rewrote original receipts")
	}
}
