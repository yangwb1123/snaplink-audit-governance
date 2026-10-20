package service

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestPostgresCoordinateEvidenceExactSchemaAcrossIndependentPools(t *testing.T) {
	for _, field := range []string{"id", "version"} {
		t.Run(field, func(t *testing.T) {
			first, second, _, query := postgresInitialQueryFixture(t)
			page, err := first.QueryEvents(testCtx, "tenant-a", "reader", query)
			if err != nil || page.Count != 2 || page.NextCursor == "" {
				t.Fatal("could not persist independent-pool original membership")
			}
			original, err := first.Store.Snapshot(testCtx)
			if err != nil || original.LayoutVersion != 2 {
				t.Fatal("coordinate integration requires actual v2 committed receipts")
			}
			key := store.SchemaKey("tenant-a", "audit.event", 2)
			if err := first.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
				schema := data.Schemas[key]
				if field == "id" {
					schema.SchemaID = "misbound-pg-schema"
				} else {
					schema.Version++
				}
				data.Schemas[key] = schema
				return nil
			}); err != nil {
				t.Fatal("could not persist owned PG catalog identity fault")
			}
			event, err := second.GetEvent(testCtx, "tenant-a", "reader", "initial-b")
			if !errors.Is(err, domain.ErrConflict) || !reflect.DeepEqual(event, domain.Event{}) {
				t.Fatal("independent event reader accepted persisted Schema body identity drift")
			}
			query.Cursor = page.NextCursor
			rejected, err := second.QueryEvents(testCtx, "tenant-a", "reader", query)
			if !errors.Is(err, domain.ErrConflict) || !reflect.DeepEqual(rejected, domain.QueryResult{}) || len(readFacts(t, first)) != 1 {
				t.Fatal("independent continuation accepted drift, reselected membership or appended success")
			}
			current, err := second.Store.Snapshot(testCtx)
			if err != nil || !reflect.DeepEqual(current.Receipts, original.Receipts) || reflect.DeepEqual(current.Schemas[key], original.Schemas[key]) {
				t.Fatal("PG coordinate fault was not actually visible or the read rewrote original cold receipts")
			}
		})
	}
}

func TestPostgresCoordinateEvidenceOwnedCaptureAndTerminalFencing(t *testing.T) {
	for _, surface := range []string{"event", "continuation"} {
		t.Run(surface, func(t *testing.T) {
			first, second, objects, _ := postgresInitialQueryFixture(t)
			read := coordinateReader(t, second, surface, "reader")
			facts := len(readFacts(t, first))
			seedLeaseExport(t, first, "coordinate-neighbor", "tenant-a")
			claim, won, err := first.claimPendingExport(testCtx, "coordinate-neighbor")
			if err != nil || !won {
				t.Fatal("could not prepare real leased PG neighboring work")
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
				page, err = read(testCtx)
				done <- err
			}()
			select {
			case <-paused.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("PG coordinate read missed its real FileStore Get")
			}
			if err := second.renewExportLease(testCtx, claim); err != nil {
				t.Fatal("coordinate source Get blocked same-Store lease renewal")
			}
			if _, err := first.CancelExport(testCtx, "tenant-a", "operator", claim.ID); err != nil {
				t.Fatal("independent pool could not cancel neighboring leased work")
			}
			key := store.SchemaKey("tenant-a", "audit.event", 2)
			if err := first.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
				schema := data.Schemas[key]
				schema.SchemaID = "drifted-pg-capture"
				data.Schemas[key] = schema
				data.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a", ArchiveDays: 365, RetentionClass: "standard"}
				data.LegalHolds["coordinate-hold"] = domain.LegalHold{ID: "coordinate-hold", TenantID: "tenant-a", Name: "Current Hold", CreatedAt: first.Now()}
				return nil
			}); err != nil {
				t.Fatal("independent pool could not preserve current catalog/Hold/policy")
			}
			release()
			if err := <-done; err != nil || len(page.Items) != 1 || page.Items[0].EventID != "initial-b" {
				t.Fatal("PG coordinate capture mixed in a later catalog or lost its selected member")
			}
			second.finishExport(exportClaimContext(testCtx, claim), claim.ID, "completed", "late-object", "late-digest", 1, "")
			current := leaseSnapshot(t, first)
			if current.Exports[claim.ID].Status != domain.ExportStatusCancelled || current.Exports[claim.ID].ObjectPath != "" || len(current.ExportLeases) != 0 || current.Policies["tenant-a"].ArchiveDays != 365 || current.LegalHolds["coordinate-hold"].ID == "" || current.Schemas[key].SchemaID != "drifted-pg-capture" || len(readFacts(t, first)) != facts+1 {
				t.Fatal("coordinate read overwrote current terminal/lease/Hold/policy/catalog or allowed late-owner publication")
			}
			rejected, err := read(testCtx)
			if !errors.Is(err, domain.ErrConflict) || !reflect.DeepEqual(rejected, domain.QueryResult{}) || len(readFacts(t, first)) != facts+1 {
				t.Fatal("subsequent PG coordinate read accepted the current misbound catalog")
			}
		})
	}
}
