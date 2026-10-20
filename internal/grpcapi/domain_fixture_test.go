package grpcapi

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

func seedGRPCDomain(t *testing.T, svc *service.Service) {
	t.Helper()
	if err := svc.CreateTenant(testCtx, "test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource(testCtx, "test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
		t.Fatal(err)
	}
}

// Only a new, harness-owned file domain uses batched preparation. Actual
// ingest remains on the original file Store, with every Save/append/fsync.
// Injected fault/CAS stores keep their original individual management Saves.
func seedGRPCFileDomain(t *testing.T, svc *service.Service) {
	t.Helper()
	memory, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer memory.Close()
	prepared, err := service.New(memory, svc.Config)
	if err != nil {
		t.Fatal(err)
	}
	seedGRPCDomain(t, prepared)
	seed, err := memory.Snapshot(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	sourceKey, schemaKey := store.SourceKey("tenant-a", "crm"), store.SchemaKey("tenant-a", "audit.event", 1)
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
		if _, exists := data.Tenants["tenant-a"]; exists {
			return errors.New("gRPC fixture tenant already exists")
		}
		if _, exists := data.Sources[sourceKey]; exists {
			return errors.New("gRPC fixture source already exists")
		}
		if _, exists := data.Schemas[schemaKey]; exists {
			return errors.New("gRPC fixture Schema already exists")
		}
		data.Tenants["tenant-a"] = seed.Tenants["tenant-a"]
		data.Sources[sourceKey] = seed.Sources[sourceKey]
		data.Schemas[schemaKey] = seed.Schemas[schemaKey]
		data.AdminActions = append(data.AdminActions, seed.AdminActions...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGRPCFileDomainPreparationMatchesDirectAPIs(t *testing.T) {
	makeService := func() (*service.Service, string) {
		path := filepath.Join(t.TempDir(), "state.json")
		st, err := store.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = st.Close() })
		svc, err := service.New(st, service.Config{AllowDevSecrets: true, Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }})
		if err != nil {
			t.Fatal(err)
		}
		if err := st.UpdateControl(testCtx, func(data *store.Snapshot) error {
			data.Exports["retained"] = domain.ExportJob{ID: "retained", TenantID: "other", Status: domain.ExportStatusCancelled}
			data.Policies["other"] = domain.RetentionPolicy{TenantID: "other", ArchiveDays: 365}
			data.LegalHolds["retained"] = domain.LegalHold{ID: "retained", TenantID: "other"}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return svc, path
	}
	direct, directPath := makeService()
	batch, batchPath := makeService()
	seedGRPCDomain(t, direct)
	seedGRPCFileDomain(t, batch)
	// Before any ingest, both artifacts must be independently restartable;
	// control-only seeding must not omit a new tenant's empty hot/cold files.
	for _, fixture := range []struct {
		svc  *service.Service
		path string
	}{{direct, directPath}, {batch, batchPath}} {
		if err := fixture.svc.Store.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := store.Open(fixture.path)
		if err != nil {
			t.Fatal("gRPC prepared file domain could not restart before ingest")
		}
		t.Cleanup(func() { _ = reopened.Close() })
		fixture.svc.Store = reopened
	}
	left, err := direct.Store.Snapshot(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	right, err := batch.Store.Snapshot(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range []*store.Snapshot{left, right} {
		if data.LayoutVersion != 2 || len(data.AdminActions) != 3 || len(data.Events) != 0 || len(data.Receipts) != 0 {
			t.Fatal("gRPC preparation changed layout, mutation facts or created ingest evidence")
		}
		for i := range data.AdminActions {
			if data.AdminActions[i].ID == "" {
				t.Fatal("prepared management fact has no actual identity")
			}
			data.AdminActions[i].ID = "" // only random fact identity differs
		}
	}
	if !reflect.DeepEqual(left, right) {
		t.Fatal("gRPC preparation differs from real management APIs or overwrote retained control")
	}
}
