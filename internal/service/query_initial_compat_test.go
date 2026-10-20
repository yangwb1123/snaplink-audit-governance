package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestInitialQueryCommittedScopeEmptyResultsAndOwnedOutput(t *testing.T) {
	for _, layout := range []int{0, 2} {
		t.Run(fmt.Sprintf("layout-%d", layout), func(t *testing.T) {
			svc, backend, _ := initialQueryFixture(t, false, false)
			if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
				data.LayoutVersion = layout
				event, err := domain.CloneEvent(data.Events[store.EventKey("tenant-a", "initial-a")])
				if err != nil {
					return err
				}
				event.EventID = "uncommitted-hot-only"
				data.Events[store.EventKey("tenant-a", event.EventID)] = event
				data.Receipts[store.EventKey("tenant-b", "foreign-corrupt")] = domain.EventReceipt{TenantID: "tenant-b", EventID: "foreign-corrupt", Status: domain.StatusArchived}
				key := store.SchemaKey("tenant-a", "audit.event", 2)
				schema := data.Schemas[key]
				schema.Active = false
				data.Schemas[key] = schema
				return nil
			}); err != nil {
				t.Fatal("could not prepare exact historical query scope")
			}
			page, err := svc.QueryEvents(testCtx, "tenant-a", "reader", initialQuery())
			if err != nil || page.Count != 2 || len(page.Items) != 1 || page.Items[0].EventID != "initial-a" || page.Snapshot == nil || page.Snapshot.Completeness != "not_asserted" {
				t.Fatal("initial query promoted hot-only/foreign evidence or rejected an inactive exact historical Schema")
			}
			page.Items[0].Actor.Roles[0] = "caller-mutation"
			page.Items[0].Payload["value"] = "caller-mutation"
			page.Items[0].Targets[0].ID = "caller-mutation"
			again, err := svc.QueryEvents(testCtx, "tenant-a", "reader", initialQuery())
			if err != nil || again.Items[0].Actor.Roles[0] != "operator" || again.Items[0].Payload["value"] != json.Number("9007199254740993") || again.Items[0].Targets[0].ID != "initial-target" {
				t.Fatal("initial-query output aliases committed source objects")
			}
			query := initialQuery()
			query.ActorID = "unselected-actor"
			empty, err := svc.QueryEvents(testCtx, "tenant-a", "reader", query)
			if err != nil || empty.Count != 0 || empty.Items == nil || len(empty.Items) != 0 || empty.NextCursor != "" || empty.Snapshot == nil {
				t.Fatal("valid empty selection lost its audited fixed-query observation")
			}
			if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
				delete(data.Receipts, store.EventKey("tenant-a", "initial-a"))
				delete(data.Receipts, store.EventKey("tenant-a", "initial-b"))
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			empty, err = svc.QueryEvents(testCtx, "tenant-a", "reader", initialQuery())
			if err != nil || empty.Count != 0 || empty.Items == nil || empty.Snapshot == nil || backend.appendCalls != 4 {
				t.Fatal("initial query turned orphan/journal-free hot content into committed truth or lost empty read facts")
			}
		})
	}
}

func TestInitialQueryRejectsInvalidRequestsBeforeSourceIO(t *testing.T) {
	for _, mode := range []string{"empty-tenant", "invalid-tenant", "missing-window", "inverted-window", "oversize", "unpaired-digest", "old-cursor", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			svc, backend, objects := initialQueryFixture(t, true, true)
			query, tenant := initialQuery(), "tenant-a"
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			switch mode {
			case "empty-tenant":
				tenant = ""
			case "invalid-tenant":
				tenant = "invalid/tenant"
			case "missing-window":
				query.From = time.Time{}
			case "inverted-window":
				query.To = query.From
			case "oversize":
				query.PageSize = domain.MaxPageSize + 1
			case "unpaired-digest":
				query.PayloadField = "value"
			case "old-cursor":
				query.Cursor = "not-a-fixed-query-cursor"
			case "cancelled":
				cancel()
			}
			page, err := svc.QueryEvents(ctx, tenant, "reader", query)
			if err == nil || !reflect.DeepEqual(page, domain.QueryResult{}) || len(objects.deadlines) != 0 || backend.appendCalls != 0 {
				t.Fatal("invalid/withdrawn initial query performed evidence IO or served partial membership")
			}
		})
	}
}

func TestInitialQuerySensitiveConditionsRetainTenantFieldAndSearchabilityBinding(t *testing.T) {
	svc, backend, _ := initialQueryFixture(t, false, true)
	for _, mode := range []string{"foreign-tenant", "different-field", "not-searchable", "missing-field"} {
		query := initialQuery()
		query.PayloadField = "value"
		tenant, field, value := "tenant-a", "value", any(json.Number("9007199254740993"))
		switch mode {
		case "foreign-tenant":
			tenant = "tenant-b"
		case "different-field":
			field = "different-field"
		case "not-searchable":
			query.PayloadField, field, value = "resource", "resource", "invoice"
		case "missing-field":
			query.PayloadField, field = "absent", "absent"
		}
		var err error
		query.PayloadDigest, err = security.SearchDigestBound(value, svc.Config.EncryptionKey, tenant, field)
		if err != nil {
			t.Fatal("could not derive synthetic bound condition")
		}
		page, err := svc.QueryEvents(testCtx, "tenant-a", "reader", query)
		if err != nil || page.Count != 0 || len(page.Items) != 0 || page.NextCursor != "" {
			t.Fatal("authenticated query ignored tenant/field/searchable binding")
		}
	}
	if backend.appendCalls != 4 {
		t.Fatal("empty sensitive query observations were not separately audited")
	}
}

func TestInitialQueryMatchedBudgetDoesNotTruncateOrLimitConsole(t *testing.T) {
	svc := queryMemoryService(t, false)
	// Exact synthetic coordinates test the unchanged matched-identity boundary,
	// not ingest throughput, durability, scan capacity or completeness.
	events, receipts := map[string]domain.Event{}, map[string]domain.EventReceipt{}
	previous := ""
	for i := 0; i <= store.MaxQuerySnapshotEvents; i++ {
		event := testEvent(fmt.Sprintf("initial-limit-%05d", i), "initial-limit", pinnedNow)
		event.TenantID, event.StreamID = "tenant-a", "tenant-a:aggregate:invoice:inv-1"
		event.Sequence, event.PrevHash = int64(i+1), previous
		if i == store.MaxQuerySnapshotEvents {
			event.OperationID = "unselected-operation"
		}
		var err error
		event.SourceDigest, err = domain.EventContentDigest(event)
		if err != nil {
			t.Fatal("could not construct exact initial-query budget content")
		}
		event.Hash, err = svc.eventHash(event)
		if err != nil {
			t.Fatal("could not construct exact initial-query budget chain")
		}
		previous = event.Hash
		key := store.EventKey("tenant-a", event.EventID)
		events[key] = event
		receipts[key] = domain.EventReceipt{TenantID: "tenant-a", EventID: event.EventID, Status: domain.StatusIndexed, StreamID: event.StreamID, Sequence: event.Sequence, Hash: event.Hash, AcceptedAt: pinnedNow, LedgeredAt: pinnedNow, IndexedAt: pinnedNow}
	}
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error { data.Events, data.Receipts = events, receipts; return nil }); err != nil {
		t.Fatal("could not save exact initial-query budget fixture")
	}
	query := initialQuery()
	query.PageSize, query.OperationID = domain.MaxPageSize, "initial-limit"
	page, err := svc.QueryEvents(testCtx, "tenant-a", "reader", query)
	if err != nil || page.Count != store.MaxQuerySnapshotEvents || len(page.Items) != domain.MaxPageSize || page.NextCursor == "" || len(readFacts(t, svc)) != 1 {
		t.Fatal("initial query applied a source-scan cap or changed its exact matched-identity budget")
	}
	query.OperationID = ""
	page, err = svc.QueryEvents(testCtx, "tenant-a", "reader", query)
	if !errors.Is(err, domain.ErrQuotaExceeded) || !reflect.DeepEqual(page, domain.QueryResult{}) || len(readFacts(t, svc)) != 1 {
		t.Fatal("over-budget initial query truncated a success or appended a successful read fact")
	}
	facets, err := svc.QueryEventFacets(testCtx, "tenant-a", "reader", query)
	if err != nil || facets.Total != store.MaxQuerySnapshotEvents+1 || len(readFacts(t, svc)) != 2 {
		t.Fatal("shared authentication imposed the ordinary-query budget on existing Console facets")
	}
}

func TestInitialQueryFileRestartKeepsAuthenticatedMembershipWithMissingSidecar(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := newServiceOn(t, st)
	seedFileTestDomain(t, svc)
	schema := domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 2, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}, SearchableFields: []string{"value"}, EncryptedFields: []string{"value"}}
	if err := svc.RegisterSchema(testCtx, "test", schema); err != nil {
		t.Fatal("could not register exact file query Schema")
	}
	objects := &archive.FileStore{Dir: filepath.Join(dir, "archive")}
	svc.Config.Archive = objects
	for i, id := range []string{"initial-a", "initial-b"} {
		event := testEvent(id, "initial-operation", pinnedNow.Add(time.Duration(i)*time.Second))
		event.SchemaVersion, event.Payload["value"] = 2, json.Number("9007199254740993")
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived); err != nil {
			t.Fatal("could not commit real file query evidence")
		}
	}
	original, err := st.Snapshot(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	key := store.EventKey("tenant-a", "initial-a")
	source, err := svc.archivedEventContext(testCtx, original.Receipts[key])
	if err != nil {
		t.Fatal("could not capture exact original archived query source")
	}
	delete(source.Payload, "value"+security.SearchDigestSuffix)
	if err := st.UpdateTenant(testCtx, "tenant-a", store.HotFirst, func(view *store.TenantView) error { view.Hot.Events[key] = source; return nil }); err != nil || st.Close() != nil {
		t.Fatal("could not preserve owned sidecar fault and release original file writer")
	}
	reopened, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	next := newServiceOn(t, reopened)
	next.Config = svc.Config
	persisted, err := reopened.Snapshot(testCtx)
	if err != nil {
		t.Fatal("could not capture persisted hot/cold query evidence after restart")
	}
	if _, present := persisted.Events[key].Payload["value"+security.SearchDigestSuffix]; present || persisted.Events[key].EventID != source.EventID {
		t.Fatal("file restart oracle lost the actual persisted sidecar fault")
	}
	query := initialQuery()
	query.PayloadField = "value"
	query.PayloadDigest, err = security.SearchDigest(json.Number("9007199254740993"), next.Config.EncryptionKey)
	if err != nil {
		t.Fatal("could not derive synthetic file query condition")
	}
	first, err := next.QueryEvents(testCtx, "tenant-a", "reader", query)
	if err != nil || first.Count != 2 || len(first.Items) != 1 || first.Items[0].EventID != "initial-a" || first.NextCursor == "" {
		t.Fatal("real file restart trusted derived metadata instead of exact authenticated evidence")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedAgain, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedAgain.Close()
	last := newServiceOn(t, reopenedAgain)
	last.Config = svc.Config
	recorder := &queryRecordingArchive{Store: objects, forbiddenID: "initial-a"}
	last.Config.Archive = recorder
	query.Cursor = first.NextCursor
	page, err := last.QueryEvents(testCtx, "tenant-a", "reader", query)
	if err != nil || page.Count != 2 || len(page.Items) != 1 || page.Items[0].EventID != "initial-b" || page.NextCursor != "" || recorder.reads != 1 {
		t.Fatal("file query continuation lost its durable fixed membership or re-read the served prefix")
	}
	current, err := reopenedAgain.Snapshot(testCtx)
	if err != nil {
		t.Fatal("could not capture final original file query evidence")
	}
	if !reflect.DeepEqual(original.Receipts, current.Receipts) || !reflect.DeepEqual(original.Schemas, current.Schemas) || len(readFacts(t, last)) != 2 {
		t.Fatal("file query rewrote original receipt/Schema or lost one read fact per page")
	}
}
