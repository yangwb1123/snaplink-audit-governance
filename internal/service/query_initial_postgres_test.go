package service

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

func postgresInitialQueryFixture(t *testing.T) (*Service, *Service, *archive.FileStore, domain.Query) {
	t.Helper()
	first, second := newExportLeasePostgres(t) // explicitly disposable database only
	seedTestDomain(t, first)
	schema := domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 2, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}, SearchableFields: []string{"value"}, EncryptedFields: []string{"value"}}
	if err := first.RegisterSchema(testCtx, "test", schema); err != nil {
		t.Fatal("could not register exact PG initial-query Schema")
	}
	objects := &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")}
	first.Config.Archive, second.Config.Archive = objects, objects
	for i, id := range []string{"initial-a", "initial-b"} {
		event := testEvent(id, "initial-operation", pinnedNow.Add(time.Duration(i)*time.Second))
		event.SchemaVersion, event.Payload["value"] = 2, json.Number("9007199254740993")
		if _, err := first.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived); err != nil {
			t.Fatal("could not commit PG-anchored real file query evidence")
		}
	}
	query := initialQuery()
	query.PayloadField = "value"
	var err error
	query.PayloadDigest, err = security.SearchDigest(json.Number("9007199254740993"), first.Config.EncryptionKey)
	if err != nil {
		t.Fatal("could not derive synthetic PG legacy condition")
	}
	return first, second, objects, query
}

func TestPostgresInitialQueryAuthenticatedMembershipAcrossIndependentPools(t *testing.T) {
	first, second, _, query := postgresInitialQueryFixture(t)
	original, err := first.Store.Snapshot(testCtx)
	if err != nil || original.LayoutVersion != 2 {
		t.Fatal("initial-query integration must use independent pools with v2 committed receipts")
	}
	key := store.EventKey("tenant-a", "initial-a")
	source, err := first.archivedEventContext(testCtx, original.Receipts[key])
	if err != nil {
		t.Fatal("could not read original PG-anchored query evidence")
	}
	delete(source.Payload, "value"+security.SearchDigestSuffix)
	if err := first.Store.UpdateTenant(testCtx, "tenant-a", store.HotFirst, func(view *store.TenantView) error { view.Hot.Events[key] = source; return nil }); err != nil {
		t.Fatal("could not persist owned hot sidecar fault without rewriting cold receipts")
	}
	page, err := second.QueryEvents(testCtx, "tenant-a", "reader", query)
	if err != nil || page.Count != 2 || len(page.Items) != 1 || page.Items[0].EventID != "initial-a" || page.NextCursor == "" {
		t.Fatal("independent query replica trusted sidecar or lost encrypted legacy-format authenticated matching")
	}
	// A continuation must not rescan the already served prefix. The next
	// fresh selection must authenticate and reject this now-corrupt hot body.
	if err := first.Store.UpdateTenant(testCtx, "tenant-a", store.HotFirst, func(view *store.TenantView) error {
		event := view.Hot.Events[key]
		event.Actor.ID = "hidden-actor"
		view.Hot.Events[key] = event
		return nil
	}); err != nil {
		t.Fatal("could not persist owned query-predicate fault")
	}
	nextQuery := query
	nextQuery.Cursor = page.NextCursor
	next, err := first.QueryEvents(testCtx, "tenant-a", "reader", nextQuery)
	if err != nil || next.Count != 2 || len(next.Items) != 1 || next.Items[0].EventID != "initial-b" || next.NextCursor != "" {
		t.Fatal("independent-pool continuation reselected or lost durable authenticated membership")
	}
	query.ActorID = "user-1"
	rejected, err := second.QueryEvents(testCtx, "tenant-a", "reader", query)
	if err == nil || !reflect.DeepEqual(rejected, domain.QueryResult{}) || len(readFacts(t, first)) != 2 {
		t.Fatal("fresh PG initial query hid corrupt predicate content or appended a successful read fact")
	}
	current, err := second.Store.Snapshot(testCtx)
	if err != nil || !reflect.DeepEqual(original.Receipts, current.Receipts) || !reflect.DeepEqual(original.Schemas, current.Schemas) || current.Events[key].Actor.ID != "hidden-actor" {
		t.Fatal("PG query rewrote original receipt/Schema or the actual persisted fault disappeared")
	}
}

func TestPostgresInitialQueryOwnedCapturePreservesCurrentTerminalControl(t *testing.T) {
	first, second, objects, query := postgresInitialQueryFixture(t)
	seedLeaseExport(t, first, "initial-neighbor", "tenant-a")
	claim, won, err := first.claimPendingExport(testCtx, "initial-neighbor")
	if err != nil || !won {
		t.Fatal("could not prepare real PG neighboring leased work")
	}
	paused := &queryPausedArchive{Store: objects, entered: make(chan struct{}, 1), release: make(chan struct{})}
	second.Config.Archive = paused
	release := sync.OnceFunc(func() { close(paused.release) })
	done, exited := make(chan error, 1), make(chan struct{})
	t.Cleanup(func() { release(); <-exited })
	var page domain.QueryResult
	go func() {
		defer close(exited)
		var err error
		page, err = second.QueryEvents(testCtx, "tenant-a", "reader", query)
		done <- err
	}()
	select {
	case <-paused.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("PG initial query missed its actual source Get")
	}
	if err := second.renewExportLease(testCtx, claim); err != nil {
		t.Fatal("initial-query Get blocked same-Store lease renewal")
	}
	if _, err := first.CancelExport(testCtx, "tenant-a", "operator", claim.ID); err != nil {
		t.Fatal("independent pool could not revoke neighboring work")
	}
	if _, err := first.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", Name: "Current Hold", Reason: "case"}); err != nil {
		t.Fatal("independent pool could not preserve current Hold")
	}
	if err := first.SetRetentionPolicy(testCtx, "case-worker", domain.RetentionPolicy{TenantID: "tenant-a", HotDays: 2, WarmDays: 3, ArchiveDays: 365, RetentionClass: "standard"}); err != nil {
		t.Fatal("independent pool could not preserve current policy")
	}
	release()
	if err := <-done; err != nil || page.Count != 2 || len(page.Items) != 1 || page.NextCursor == "" {
		t.Fatal("PG query did not resume its authenticated captured membership")
	}
	second.finishExport(exportClaimContext(testCtx, claim), claim.ID, "completed", "late-object", "late-digest", 1, "")
	current := leaseSnapshot(t, first)
	if current.Exports[claim.ID].Status != domain.ExportStatusCancelled || current.Exports[claim.ID].ObjectPath != "" || len(current.ExportLeases) != 0 || current.Policies["tenant-a"].ArchiveDays != 365 || len(current.LegalHolds) != 1 || len(readFacts(t, first)) != 1 {
		t.Fatal("initial-query capture overwrote terminal/lease/Hold/policy or allowed late owner publication")
	}
}
