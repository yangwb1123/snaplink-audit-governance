package service

import (
	"encoding/json"
	"errors"
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

func postgresPreflightFixture(t *testing.T) (*Service, *Service, *checkpointArchive, domain.Event, domain.Query) {
	t.Helper()
	first, second := newExportLeasePostgres(t) // only an explicitly disposable DB
	seedTestDomain(t, first)
	for _, svc := range []*Service{first, second} {
		pauseExportDispatch(svc)
	}
	if err := first.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "preflight.pg", Version: 1, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}, EncryptedFields: []string{"value"}, SearchableFields: []string{"value"}}); err != nil {
		t.Fatal("could not register exact PG preflight Schema")
	}
	objects := &checkpointArchive{Store: &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")}}
	first.Config.Archive, second.Config.Archive = objects, objects
	event := testEvent("preflight-pg-source", "preflight-operation", pinnedNow.Add(10*time.Second))
	event.SchemaID, event.Payload["value"] = "preflight.pg", json.Number("9007199254740993")
	if _, err := first.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived); err != nil {
		t.Fatal("could not commit real PG/WORM preflight evidence")
	}
	data, err := first.Store.Snapshot(testCtx)
	if err != nil || data.LayoutVersion != 2 {
		t.Fatal("preflight must exercise independent SQL pools and v2 control CAS")
	}
	source, err := first.archivedEventContext(testCtx, data.Receipts[store.EventKey("tenant-a", event.EventID)])
	if err != nil {
		t.Fatal("could not read exact original PG-anchored file evidence")
	}
	return first, second, objects, source, domain.Query{From: pinnedNow, To: pinnedNow.Add(time.Minute)}
}

func TestPostgresExportPreflightCurrentHoldChangeFencesNewJobAcrossPools(t *testing.T) {
	for _, surface := range []string{"create", "retry"} {
		t.Run(surface, func(t *testing.T) {
			first, second, objects, _, query := postgresPreflightFixture(t)
			parent := seedPreflightParent(t, first, query)
			accessNonmatchingHold(t, first)
			seedActionExport(t, first, "preflight-neighbor", "pending")
			claim, won, err := first.claimPendingExport(testCtx, "preflight-neighbor")
			if err != nil || !won {
				t.Fatal("could not claim real neighboring executor")
			}
			boundary := &exportPreflightArchive{Store: objects, entered: make(chan struct{}), release: make(chan struct{}), deadlines: make(chan time.Time, 8)}
			second.Config.Archive = boundary
			release := sync.OnceFunc(func() { close(boundary.release) })
			done, exited := make(chan error, 1), make(chan struct{})
			t.Cleanup(func() { release(); <-exited })
			go func() {
				defer close(exited)
				_, err := callExportPreflight(t, testCtx, second, surface, query)
				done <- err
			}()
			select {
			case <-boundary.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("PG preflight never reached its current-Hold source Get")
			}
			queryDeadline, holdDeadline := <-boundary.deadlines, <-boundary.deadlines
			if queryDeadline.IsZero() || !queryDeadline.Equal(holdDeadline) {
				t.Fatal("PG current-Hold Get restarted the original query deadline")
			}
			// The same Store must be free, and the independent pool's current
			// Hold/cancel/policy commit must survive the stale owned snapshot.
			if err := second.renewExportLease(testCtx, claim); err != nil {
				t.Fatal("source Get blocked same-Store lease renewal")
			}
			if _, err := first.CancelExport(testCtx, "tenant-a", "operator", claim.ID); err != nil {
				t.Fatal("independent pool could not revoke neighboring work")
			}
			if _, err := first.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", Name: "matching", Reason: "case"}); err != nil {
				t.Fatal("independent pool could not change current Hold")
			}
			if err := first.SetRetentionPolicy(testCtx, "case-worker", domain.RetentionPolicy{TenantID: "tenant-a", HotDays: 2, WarmDays: 3, ArchiveDays: 4, RetentionClass: "standard"}); err != nil {
				t.Fatal("independent pool could not change policy")
			}
			release()
			if err := awaitExportAccess(t, done); !errors.Is(err, domain.ErrConflict) {
				t.Fatal("new-job CAS reused stale authenticated Hold across independent SQL pools")
			}
			second.finishExport(exportClaimContext(testCtx, claim), claim.ID, "completed", "late-object", "late-digest", 1, "")
			data := leaseSnapshot(t, first)
			if len(data.Exports) != 2 || !reflect.DeepEqual(data.Exports[parent.ID], parent) || data.Exports[claim.ID].Status != domain.ExportStatusCancelled || data.Exports[claim.ID].ObjectPath != "" || len(data.ExportLeases) != 0 || data.Policies["tenant-a"].HotDays != 2 {
				t.Fatal("stale preflight lost current Hold, cancellation, terminal fencing or policy")
			}
			for _, fact := range data.AdminActions {
				if fact.Action == domain.AdminActionExportCreated || fact.Action == domain.AdminActionExportRetried {
					t.Fatal("stale preflight committed new-job success")
				}
			}
			if id, err := callExportPreflight(t, testCtx, second, surface, query); id != "" || !errors.Is(err, domain.ErrForbidden) {
				t.Fatal("fresh explicit cross-pool attempt ignored the current matching Hold")
			}
		})
	}
}

func TestPostgresExportPreflightAuthenticatesPersistedSidecarAcrossPools(t *testing.T) {
	for _, surface := range []string{"create", "retry"} {
		t.Run(surface, func(t *testing.T) {
			first, second, _, source, query := postgresPreflightFixture(t)
			query.PayloadField = "value"
			var err error
			query.PayloadDigest, err = security.SearchDigestBound(json.Number("9007199254740993"), first.Config.EncryptionKey, "tenant-a", "value")
			if err != nil {
				t.Fatal("could not derive exact PG preflight predicate")
			}
			parent := seedPreflightParent(t, first, query)
			hold, err := first.CreateLegalHold(testCtx, domain.LegalHold{TenantID: "tenant-a", Name: "case", Reason: "case", Filter: query})
			if err != nil {
				t.Fatal("could not seed PG preflight Hold")
			}
			original, err := first.Store.Snapshot(testCtx)
			if err != nil {
				t.Fatal("could not capture original PG receipt evidence")
			}
			delete(source.Payload, "value"+security.SearchDigestSuffix)
			if err := first.Store.UpdateTenant(testCtx, "tenant-a", store.HotFirst, func(view *store.TenantView) error {
				view.Hot.Events[store.EventKey("tenant-a", source.EventID)] = source
				return nil
			}); err != nil {
				t.Fatal("could not persist owned hot sidecar fault in PG without rewriting cold evidence")
			}
			id, err := callExportPreflight(t, testCtx, second, surface, query)
			var denied interface{ LegalHoldID() string }
			if id != "" || !errors.Is(err, domain.ErrForbidden) || !errors.As(err, &denied) || denied.LegalHoldID() != hold.ID {
				t.Fatal("independent replica trusted a saved search sidecar for Hold admission")
			}
			assertPreflightNoCreation(t, first, parent)
			current, err := second.Store.Snapshot(testCtx)
			if err != nil || !reflect.DeepEqual(current.Receipts, original.Receipts) || !reflect.DeepEqual(current.Schemas, original.Schemas) || current.Events[store.EventKey("tenant-a", source.EventID)].EventID != source.EventID {
				t.Fatal("preflight rewrote cold receipt/Schema or the oracle lost its persisted fault")
			}
			if _, exists := current.Events[store.EventKey("tenant-a", source.EventID)].Payload["value"+security.SearchDigestSuffix]; exists {
				t.Fatal("PG oracle silently restored the search sidecar before verification")
			}
		})
	}
}
