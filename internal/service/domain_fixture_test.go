package service

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// seedFileTestDomain batches ONLY preparation of a new test domain. The same
// three real management APIs generate the exact catalog and mutation facts in
// an isolated backend; one target Save merges just those owned structures.
// No production Store/API behavior, fsync, timeout or test assertion is skipped.
// Generic fault/CAS fixtures keep seedTestDomain and its separate API commits.
func seedFileTestDomain(t *testing.T, svc *Service) {
	t.Helper()
	prepared, err := New(store.NewWithBackend(&checkpointBackend{data: store.NewSnapshot()}), svc.Config)
	if err != nil {
		t.Fatal(err)
	}
	seedTestDomain(t, prepared)
	seed, err := prepared.Store.Snapshot(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	sourceKey := store.SourceKey("tenant-a", "crm")
	schemaKey := store.SchemaKey("tenant-a", "audit.event", 1)
	if err := svc.Store.Update(testCtx, func(current *store.Snapshot) error {
		if _, exists := current.Tenants["tenant-a"]; exists {
			return fmt.Errorf("%w: test fixture tenant already exists", domain.ErrConflict)
		}
		if _, exists := current.Sources[sourceKey]; exists {
			return fmt.Errorf("%w: test fixture source already exists", domain.ErrConflict)
		}
		if _, exists := current.Schemas[schemaKey]; exists {
			return fmt.Errorf("%w: test fixture Schema already exists", domain.ErrConflict)
		}
		current.Tenants["tenant-a"] = seed.Tenants["tenant-a"]
		current.Sources[sourceKey] = seed.Sources[sourceKey]
		current.Schemas[schemaKey] = seed.Schemas[schemaKey]
		for _, fact := range seed.AdminActions {
			svc.appendAdminAction(current, fact)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFileDomainFixtureMatchesDirectManagementInitialization(t *testing.T) {
	makeService := func(t *testing.T) *Service {
		t.Helper()
		st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = st.Close() })
		return newServiceOn(t, st)
	}
	direct, batch := makeService(t), makeService(t)
	// These are fresh, uniquely owned fixtures. Unrelated retained control
	// must survive: the prepared seed is never written back as a snapshot.
	for _, svc := range []*Service{direct, batch} {
		if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
			data.Policies["tenant-other"] = domain.RetentionPolicy{TenantID: "tenant-other", ArchiveDays: 365, RetentionClass: "standard"}
			data.Exports["retained-terminal"] = domain.ExportJob{ID: "retained-terminal", TenantID: "tenant-other", Status: domain.ExportStatusCancelled}
			data.LegalHolds["retained-hold"] = domain.LegalHold{ID: "retained-hold", TenantID: "tenant-other"}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	seedTestDomain(t, direct)
	seedFileTestDomain(t, batch)
	left, right := leaseSnapshot(t, direct), leaseSnapshot(t, batch)
	if left.LayoutVersion != 2 || right.LayoutVersion != 2 || len(left.AdminActions) != 3 || len(right.AdminActions) != 3 {
		t.Fatal("fixture preparation changed file layout or omitted management facts")
	}
	for _, data := range []*store.Snapshot{left, right} {
		for i := range data.AdminActions {
			if data.AdminActions[i].ID == "" {
				t.Fatal("prepared management fact has no real identity")
			}
			data.AdminActions[i].ID = "" // random fact identities differ, not their facts
		}
	}
	if !reflect.DeepEqual(left, right) {
		t.Fatal("batched file preparation differs from the three real APIs or overwrote retained control")
	}
}

func TestFileDomainFixtureUsesOneTargetSaveWithoutChangingFacts(t *testing.T) {
	backend := &checkpointBackend{data: store.NewSnapshot()}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	seedFileTestDomain(t, svc)
	if backend.saves != 1 {
		t.Fatal("domain preparation still performs unrelated repeated target Saves")
	}
	data := leaseSnapshot(t, svc)
	if len(data.Tenants) != 1 || len(data.Sources) != 1 || len(data.Schemas) != 1 || len(data.AdminActions) != 3 || len(data.Events) != 0 || len(data.Receipts) != 0 {
		t.Fatal("single-Save preparation lost catalog/facts or created evidence")
	}
}
