package main

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

// The lock oracle starts at real flock acquisition, not at readiness. Finish
// unrelated empty-volume bootstrap/fsync first, so an early SIGTERM does not
// measure three management commits as a 10s shutdown performance requirement.
func prepareStateLockFixture(t *testing.T, statePath string) *store.Snapshot {
	t.Helper()
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	svc, err := service.New(st, service.Config{AllowDevSecrets: true, ArchiveDir: filepath.Join(filepath.Dir(statePath), "archive")})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant(testCtx, "bootstrap", domain.Tenant{ID: "demo", Name: "Local development tenant", HomeRegion: "local", DataRegion: "local", Active: true, EventsPerSecond: 1000, Burst: 1000}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema(testCtx, "bootstrap", domain.EventSchema{TenantID: "demo", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Classification: "internal", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource(testCtx, "bootstrap", domain.SourceSystem{TenantID: "demo", ID: "demo", Name: "Local development source", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Config.Archive.Ready(testCtx); err != nil {
		t.Fatal(err)
	}
	data, err := st.Snapshot(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal("could not close the preparation writer before starting the lock holder")
	}
	return data
}

func assertStateLockFixture(t *testing.T, statePath string, before *store.Snapshot) {
	t.Helper()
	reopened, err := store.Open(statePath)
	if err != nil {
		t.Fatal("holder shutdown did not release the real state lock")
	}
	defer reopened.Close()
	after, err := reopened.Snapshot(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.Tenants, after.Tenants) || !reflect.DeepEqual(before.Sources, after.Sources) || !reflect.DeepEqual(before.Schemas, after.Schemas) || len(after.Events) != 0 || len(after.Receipts) != 0 || len(after.Exports) != 0 {
		t.Fatal("lock contention/startup cancellation changed the prepared domain or created evidence")
	}
}
