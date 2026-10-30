package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

var errCatalogControlLoad = errors.New("injected catalog Load failure")

// This v1 Load oracle deliberately returns an independent, valid snapshot even
// when its context has ended. It tests service withdrawal, not v2 working-set
// separation or durability; the real file fixture below covers the v2 path.
// Every invocation is counted, including a prohibited pre-cancelled Load.
type catalogControlBackend struct {
	data           *store.Snapshot
	mode           string
	cancel         context.CancelFunc
	loads          int
	loadsForUpdate int
	saves          int
	appends        int
	deadline       time.Time
	loadedAt       time.Time
}

func (b *catalogControlBackend) Load(ctx context.Context) (*store.Snapshot, error) {
	b.loads++
	b.loadedAt = time.Now()
	b.deadline, _ = ctx.Deadline()
	data, err := cloneTestSnapshot(b.data)
	if err != nil {
		return nil, err
	}
	switch b.mode {
	case "load-error":
		return nil, errCatalogControlLoad
	case "load-error-with-snapshot":
		return data, errCatalogControlLoad
	case "cancel-with-snapshot":
		b.cancel()
	case "deadline-with-snapshot", "caller-deadline-with-snapshot":
		<-ctx.Done()
	}
	return data, nil
}

func (b *catalogControlBackend) LoadForUpdate(context.Context) (*store.Snapshot, error) {
	b.loadsForUpdate++
	return nil, errors.New("catalog reader attempted LoadForUpdate")
}

func (b *catalogControlBackend) Save(context.Context, *store.Snapshot) error {
	b.saves++
	return errors.New("catalog reader attempted Save")
}

func (b *catalogControlBackend) AppendAdminFact(context.Context, domain.AdminAction, int) error {
	b.appends++
	return errors.New("catalog reader attempted read self-audit")
}

func (b *catalogControlBackend) ReadAdminTrail(context.Context) ([]domain.AdminAction, error) {
	return nil, nil
}

func catalogControlSnapshot() *store.Snapshot {
	data := store.NewSnapshot()
	for _, tenant := range []domain.Tenant{
		{ID: "tenant-b", Name: "B inactive", HomeRegion: "home-b", DataRegion: "data-b", Active: false, EventsPerSecond: 17, Burst: 29, CreatedAt: pinnedNow.Add(-time.Hour)},
		{ID: "tenant-a", Name: "A active", HomeRegion: "home-a", DataRegion: "data-a", Active: true, EventsPerSecond: 31, Burst: 43, CreatedAt: pinnedNow},
	} {
		data.Tenants[tenant.ID] = tenant
	}
	for _, schema := range []domain.EventSchema{
		{TenantID: "tenant-a", SchemaID: "zeta", Version: 2, EventType: "zeta.changed", RequiredFields: []string{}, AllowedFields: []string{}, EncryptedFields: []string{}, SearchableFields: []string{}, Classification: "empty-optional", Active: false, CreatedAt: pinnedNow.Add(-time.Minute)},
		{TenantID: "tenant-a", SchemaID: "alpha", Version: 3, EventType: "alpha.changed.v3", RequiredFields: []string{"r3"}, AllowedFields: []string{"a3", "a4"}, EncryptedFields: []string{"e3"}, SearchableFields: []string{"s3"}, Classification: "current", Active: true, CreatedAt: pinnedNow},
		{TenantID: "tenant-a", SchemaID: "alpha", Version: 1, EventType: "alpha.changed.v1", RequiredFields: []string{"r1"}, AllowedFields: []string{"a1", "a2"}, EncryptedFields: []string{"e1"}, SearchableFields: []string{"s1"}, Classification: "historical", Active: false, CreatedAt: pinnedNow.Add(-time.Hour)},
		{TenantID: "tenant-b", SchemaID: "alpha", Version: 1, EventType: "neighbor.changed", Classification: "neighbor", Active: false, CreatedAt: pinnedNow},
		{TenantID: "tenant-ab", SchemaID: "alpha", Version: 1, EventType: "prefix-neighbor.changed", CreatedAt: pinnedNow},
		{TenantID: "unregistered", SchemaID: "orphan", Version: 7, EventType: "historical.changed", CreatedAt: pinnedNow},
	} {
		data.Schemas[store.SchemaKey(schema.TenantID, schema.SchemaID, schema.Version)] = schema
	}
	return data
}

func catalogControlWantTenants(data *store.Snapshot) []domain.Tenant {
	return []domain.Tenant{data.Tenants["tenant-a"], data.Tenants["tenant-b"]}
}

func catalogControlWantSchemas(data *store.Snapshot) []domain.EventSchema {
	zeta := data.Schemas[store.SchemaKey("tenant-a", "zeta", 2)]
	// The original JSON snapshot clone omitted all four empty optional lists.
	zeta.RequiredFields, zeta.AllowedFields, zeta.EncryptedFields, zeta.SearchableFields = nil, nil, nil, nil
	return []domain.EventSchema{data.Schemas[store.SchemaKey("tenant-a", "alpha", 1)], data.Schemas[store.SchemaKey("tenant-a", "alpha", 3)], zeta}
}

func catalogControlList(ctx context.Context, svc *Service, kind string) (any, error) {
	if kind == "tenants" {
		return svc.ListTenants(ctx)
	}
	return svc.ListSchemas(ctx, "tenant-a")
}

func TestCatalogControlLoadWithdrawalAndSingleBudget(t *testing.T) {
	for _, kind := range []string{"tenants", "schemas"} {
		for _, mode := range []string{"success", "load-error", "load-error-with-snapshot", "cancel-before", "cancel-with-snapshot", "deadline-with-snapshot", "caller-deadline-with-snapshot"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				data := catalogControlSnapshot()
				original, err := cloneTestSnapshot(data)
				if err != nil {
					t.Fatal(err)
				}
				stored, err := cloneTestSnapshot(original)
				if err != nil {
					t.Fatal(err)
				}
				backend := &catalogControlBackend{data: stored, mode: mode}
				svc := newServiceOn(t, store.NewWithBackendOptions(backend, store.Options{OperationTimeout: 200 * time.Millisecond}))
				t.Cleanup(func() { _ = svc.Store.Close() })
				objects := &checkpointArchive{}
				svc.Config.Archive = objects
				ctx, cancel := context.WithCancel(testCtx)
				defer cancel()
				var callerDeadline time.Time
				if mode == "caller-deadline-with-snapshot" {
					var stop context.CancelFunc
					ctx, stop = context.WithTimeout(ctx, 80*time.Millisecond)
					defer stop()
					callerDeadline, _ = ctx.Deadline()
				}
				backend.cancel = cancel
				if mode == "cancel-before" {
					cancel()
				}
				got, err := catalogControlList(ctx, svc, kind)
				value := reflect.ValueOf(got)
				t.Logf("catalog_load_oracle kind=%s mode=%s items=%d nil=%t err=%v loads=%d deadlines=%d", kind, mode, value.Len(), value.IsNil(), err, backend.loads, svc.Store.OperationDeadlineExhaustions())
				if mode == "success" {
					var want any = catalogControlWantTenants(data)
					if kind == "schemas" {
						want = catalogControlWantSchemas(data)
					}
					if err != nil || !reflect.DeepEqual(got, want) {
						t.Error("successful catalog read changed fields, filtering or ordering")
					}
				} else if err == nil || !value.IsNil() {
					t.Error("failed or withdrawn catalog read must return an error and a nil list, not partial success")
				}
				if (mode == "load-error" || mode == "load-error-with-snapshot") && err != errCatalogControlLoad {
					t.Error("catalog read replaced its original Load error")
				}
				deadlineMode := mode == "deadline-with-snapshot" || mode == "caller-deadline-with-snapshot"
				if deadlineMode {
					if !errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || svc.Store.OperationDeadlineExhaustions() != 1 {
						t.Error("actual deadline must be classified and counted exactly once")
					}
				} else if svc.Store.OperationDeadlineExhaustions() != 0 || errors.Is(err, store.ErrOperationDeadline) {
					t.Error("non-deadline catalog read invented a deadline classification")
				}
				if (mode == "cancel-before" || mode == "cancel-with-snapshot") && !errors.Is(err, context.Canceled) {
					t.Error("catalog read lost caller cancellation")
				}
				wantLoads := 1
				if mode == "cancel-before" {
					wantLoads = 0
				}
				if backend.loads != wantLoads || backend.loadsForUpdate != 0 || backend.saves != 0 || backend.appends != 0 || objects.gets.Load() != 0 || objects.puts.Load() != 0 {
					t.Error("catalog read loaded after pre-cancel, retried Load, wrote state/facts or touched WORM")
				}
				if wantLoads == 1 && (backend.deadline.IsZero() || backend.deadline.After(backend.loadedAt.Add(200*time.Millisecond))) {
					t.Error("catalog Load did not inherit the original operation bound")
				}
				if !callerDeadline.IsZero() && !backend.deadline.Equal(callerDeadline) {
					t.Error("catalog Load replaced the earlier caller deadline")
				}
				if !reflect.DeepEqual(backend.data, original) {
					t.Error("catalog observation changed original stored state")
				}
			})
		}
	}
}

func TestCatalogControlScopeOrderingFieldsAndEmptyNil(t *testing.T) {
	for _, layout := range []string{"v1-load", "v2-control"} {
		t.Run(layout, func(t *testing.T) {
			data := catalogControlSnapshot()
			backend := &catalogControlBackend{data: data}
			st := store.NewWithBackend(backend)
			if layout == "v2-control" {
				var err error
				st, err = store.Open("")
				if err != nil {
					t.Fatal(err)
				}
				if err := st.UpdateControl(testCtx, func(current *store.Snapshot) error {
					current.Tenants, current.Schemas = data.Tenants, data.Schemas
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() { _ = st.Close() })
			svc := newServiceOn(t, st)
			objects := &checkpointArchive{}
			svc.Config.Archive = objects
			if got, err := svc.ListTenants(testCtx); err != nil || !reflect.DeepEqual(got, catalogControlWantTenants(data)) {
				t.Error("tenant list must include the whole catalog, including inactive entries, ordered by ID")
			}
			if got, err := svc.ListSchemas(testCtx, "tenant-a"); err != nil || !reflect.DeepEqual(got, catalogControlWantSchemas(data)) {
				t.Error("schema list must retain exact fields, inactive/history entries and schema ID/version ordering")
			}
			for _, tenant := range []string{"tenant-b", "tenant-ab", "unregistered"} {
				got, err := svc.ListSchemas(testCtx, tenant)
				if err != nil || len(got) != 1 || got[0].TenantID != tenant {
					t.Error("schema list widened its tenant filter or required a current tenant registration")
				}
				if tenant == "tenant-b" && (got[0].RequiredFields != nil || got[0].AllowedFields != nil || got[0].EncryptedFields != nil || got[0].SearchableFields != nil) {
					t.Error("schema list changed original nil optional lists")
				}
			}
			if got, err := svc.ListSchemas(testCtx, "absent"); err != nil || got != nil {
				t.Error("a successful empty schema list must remain nil, not 404 or a non-nil empty list")
			}
			loads := backend.loads
			for _, tenant := range []string{"", "tenant/a", "tenant a", "tenant" + store.KeySeparator + "a"} {
				if got, err := svc.ListSchemas(testCtx, tenant); !errors.Is(err, domain.ErrInvalid) || got != nil {
					t.Error("schema list changed the existing requireTenantScope refusal")
				}
			}
			if backend.loads != loads || backend.saves != 0 || backend.loadsForUpdate != 0 || backend.appends != 0 || objects.gets.Load() != 0 || objects.puts.Load() != 0 {
				t.Error("scope refusal loaded state or catalog observation wrote facts/WORM")
			}
		})
	}
	for _, layout := range []string{"v1-load", "v2-control"} {
		t.Run(layout+"/empty", func(t *testing.T) {
			st := store.NewWithBackend(&catalogControlBackend{data: store.NewSnapshot()})
			if layout == "v2-control" {
				var err error
				st, err = store.Open("")
				if err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() { _ = st.Close() })
			svc := newServiceOn(t, st)
			if got, err := svc.ListTenants(testCtx); err != nil || got != nil {
				t.Error("a successful empty tenant catalog must retain its nil list")
			}
			if got, err := svc.ListSchemas(testCtx, "absent"); err != nil || got != nil {
				t.Error("an empty schema catalog must not introduce a tenant existence check")
			}
		})
	}
}

func catalogControlChangeSlices(schema *domain.EventSchema, value string) {
	schema.RequiredFields[0] = value + "-required"
	schema.AllowedFields[0] = value + "-allowed"
	schema.EncryptedFields[0] = value + "-encrypted"
	schema.SearchableFields[0] = value + "-searchable"
}

func TestCatalogControlBidirectionalOwnershipAndFreshReads(t *testing.T) {
	for _, layout := range []string{"v1-load", "v2-control"} {
		t.Run(layout, func(t *testing.T) {
			data := catalogControlSnapshot()
			backend := &catalogControlBackend{data: data}
			st := store.NewWithBackend(backend)
			change := func(fn func(*store.Snapshot)) { fn(backend.data) }
			if layout == "v2-control" {
				var err error
				st, err = store.Open("")
				if err != nil {
					t.Fatal(err)
				}
				change = func(fn func(*store.Snapshot)) {
					if err := st.UpdateControl(testCtx, func(current *store.Snapshot) error { fn(current); return nil }); err != nil {
						t.Fatal(err)
					}
				}
				change(func(current *store.Snapshot) { current.Tenants, current.Schemas = data.Tenants, data.Schemas })
			}
			t.Cleanup(func() { _ = st.Close() })
			svc := newServiceOn(t, st)
			first, err := svc.ListSchemas(testCtx, "tenant-a")
			if err != nil || !reflect.DeepEqual(first, catalogControlWantSchemas(data)) {
				t.Fatal("could not capture the original schema list")
			}
			second, err := svc.ListSchemas(testCtx, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			first[0].EventType, first[0].Classification, first[0].Active = "caller.changed", "caller", true
			catalogControlChangeSlices(&first[0], "caller")
			if !reflect.DeepEqual(second, catalogControlWantSchemas(data)) {
				t.Error("repeated reads share a result list or any of the four schema field slices")
			}
			if next, err := svc.ListSchemas(testCtx, "tenant-a"); err != nil || !reflect.DeepEqual(next, second) {
				t.Error("caller mutation escaped into stored schema metadata")
			}
			tenants, err := svc.ListTenants(testCtx)
			if err != nil {
				t.Fatal(err)
			}
			tenants[0].Name = "caller tenant"
			if next, err := svc.ListTenants(testCtx); err != nil || next[0].Name != "A active" {
				t.Fatal("tenant result list aliases stored metadata")
			}
			key := store.SchemaKey("tenant-a", "alpha", 1)
			change(func(current *store.Snapshot) {
				schema := current.Schemas[key]
				catalogControlChangeSlices(&schema, "current")
				schema.Classification = "new-control"
				current.Schemas[key] = schema
				tenant := current.Tenants["tenant-a"]
				tenant.Name = "new-control"
				current.Tenants[tenant.ID] = tenant
			})
			if second[0].RequiredFields[0] != "r1" || second[0].AllowedFields[0] != "a1" || second[0].EncryptedFields[0] != "e1" || second[0].SearchableFields[0] != "s1" || second[0].Classification != "historical" || tenants[0].Name != "caller tenant" {
				t.Error("a later control mutation changed a previously returned catalog")
			}
			want := second[0]
			want.RequiredFields, want.AllowedFields = []string{"current-required"}, []string{"current-allowed", "a2"}
			want.EncryptedFields, want.SearchableFields = []string{"current-encrypted"}, []string{"current-searchable"}
			want.Classification = "new-control"
			if next, err := svc.ListSchemas(testCtx, "tenant-a"); err != nil || !reflect.DeepEqual(next[0], want) {
				t.Error("a new schema list reused cached control instead of observing current metadata")
			}
			if next, err := svc.ListTenants(testCtx); err != nil || next[0].Name != "new-control" {
				t.Error("a new tenant list reused cached control")
			}
		})
	}
}

// Build a separate real v2 fixture. Do not replace, batch, or edit the old
// performance workloads. One source is really archived/evicted; the other
// tenant retains real hot events and committed cold receipt versions.
func catalogControlFileFixture(t *testing.T) (*Service, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("closing catalog file fixture: %v", err)
		}
	})
	svc := newServiceOn(t, st)
	seedTestDomain(t, svc)
	svc.Config.Archive = &archive.FileStore{Dir: filepath.Join(filepath.Dir(path), "archive")}
	if err := svc.CreateTenant(testCtx, "catalog-manager", domain.Tenant{ID: "tenant-b", Name: "Neighbor", HomeRegion: "home-b", DataRegion: "data-b", EventsPerSecond: 1000, Burst: 1000}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource(testCtx, "catalog-manager", domain.SourceSystem{TenantID: "tenant-b", ID: "crm", Name: "Neighbor source"}); err != nil {
		t.Fatal(err)
	}
	for _, schema := range []domain.EventSchema{
		{TenantID: "tenant-b", SchemaID: "audit.event", Version: 1, EventType: "audit.event", RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}},
		{TenantID: "tenant-a", SchemaID: "audit.event", Version: 3, EventType: "audit.event", RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}, EncryptedFields: []string{"value"}, SearchableFields: []string{"value"}, Classification: "historical-control"},
	} {
		if err := svc.RegisterSchema(testCtx, "catalog-manager", schema); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("catalog-archived", "catalog-operation", pinnedNow.Add(time.Second)), domain.StatusArchived); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GetReceipt(testCtx, "tenant-a", "original-reader", "catalog-archived"); err != nil {
		t.Fatal("could not prepare an original independent read fact")
	}
	if err := st.UpdateControl(testCtx, func(data *store.Snapshot) error {
		key := store.SchemaKey("tenant-a", "audit.event", 1)
		schema := data.Schemas[key]
		schema.Active = false // persisted historical metadata, not a new disable API
		data.Schemas[key] = schema
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !st.HotCold(testCtx) {
		t.Fatal("catalog file oracle did not open the real v2 split path")
	}
	return svc, path
}

func catalogControlCheckFileLists(t *testing.T, svc *Service, before *store.Snapshot) {
	t.Helper()
	wantTenants := []domain.Tenant{before.Tenants["tenant-a"], before.Tenants["tenant-b"]}
	wantSchemas := []domain.EventSchema{before.Schemas[store.SchemaKey("tenant-a", "audit.event", 1)], before.Schemas[store.SchemaKey("tenant-a", "audit.event", 3)]}
	if got, err := svc.ListTenants(testCtx); err != nil || !reflect.DeepEqual(got, wantTenants) {
		t.Error("real v2 tenant list lost original fields or ordering")
	}
	got, err := svc.ListSchemas(testCtx, "tenant-a")
	if err != nil || !reflect.DeepEqual(got, wantSchemas) {
		t.Fatal("real v2 schema list lost inactive/history metadata or ordering")
	}
	got[0].RequiredFields[0] = "caller"
	got[1].AllowedFields[0], got[1].EncryptedFields[0], got[1].SearchableFields[0] = "caller", "caller", "caller"
	if next, err := svc.ListSchemas(testCtx, "tenant-a"); err != nil || !reflect.DeepEqual(next, wantSchemas) {
		t.Error("real v2 returned schema slices are not independently owned")
	}
	if got, err := svc.ListSchemas(testCtx, "tenant-b"); err != nil || !reflect.DeepEqual(got, []domain.EventSchema{before.Schemas[store.SchemaKey("tenant-b", "audit.event", 1)]}) {
		t.Error("real v2 schema list mixed neighboring tenants")
	}
	if got, err := svc.ListSchemas(testCtx, "absent"); err != nil || got != nil {
		t.Error("real v2 no-match must remain a nil success")
	}
}

func catalogControlFileImage(t *testing.T, path string) admissionFileImage {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return admissionFileImage{info: info, raw: raw}
}

func catalogControlAssertFileImage(t *testing.T, path string, before admissionFileImage) {
	t.Helper()
	after := catalogControlFileImage(t, path)
	if !bytes.Equal(before.raw, after.raw) || !os.SameFile(before.info, after.info) {
		t.Error("catalog observation replaced original file bytes, versions or inode")
	}
}

func TestCatalogControlRealV2FileWorkingSetFactsAndRestart(t *testing.T) {
	svc, path := catalogControlFileFixture(t)
	sourceArchive := svc.Config.Archive
	svc.Config.Archive = nil // only additional neighbor samples deliberately stay hot
	ingest := func(i int) {
		t.Helper()
		event := testEvent(fmt.Sprintf("catalog-hot-%d", i), "catalog-neighbor-operation", pinnedNow.Add(time.Duration(i)*time.Second))
		event.TenantID = "tenant-b"
		event.Payload["value"] = json.Number("10")
		if _, err := svc.Ingest(testCtx, "tenant-b", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatal(err)
		}
	}
	measure := func() [2]float64 {
		t.Helper()
		var allocs [2]float64
		for i, kind := range []string{"tenants", "schemas"} {
			allocs[i] = testing.AllocsPerRun(3, func() {
				got, err := catalogControlList(testCtx, svc, kind)
				if err != nil || reflect.ValueOf(got).Len() != 2 {
					t.Fatal("catalog allocation sample changed its original result")
				}
			})
		}
		return allocs
	}
	ingest(0)
	control := catalogControlFileImage(t, path)
	var diskControl store.Snapshot
	if err := json.Unmarshal(control.raw, &diskControl); err != nil || diskControl.LayoutVersion != 2 || len(diskControl.Events) != 0 || len(diskControl.Receipts) != 0 {
		t.Fatal("file oracle needs a real control-only v2 document")
	}
	smallSnapshot, err := svc.Store.Snapshot(testCtx)
	if err != nil || len(smallSnapshot.Events) != 1 || len(smallSnapshot.Receipts) != 2 {
		t.Fatal("small oracle lost its real hot event or archived/committed receipts")
	}
	smallFiles := admissionEvidenceFiles(t, filepath.Dir(path))
	if len(smallFiles) != 4 {
		t.Fatal("file oracle requires two real hot documents and two cold ledgers")
	}
	small := measure()
	catalogControlAssertFileImage(t, path, control)
	assertAdmissionEvidenceFilesUnchanged(t, smallFiles)
	for i := 1; i < 32; i++ {
		ingest(i)
	}
	catalogControlAssertFileImage(t, path, control)
	before, err := svc.Store.Snapshot(testCtx)
	if err != nil || before.LayoutVersion != 2 || len(before.Events) != 32 || len(before.Receipts) != 33 || len(before.LedgeredOutbox) != 0 || len(before.AdminActions) != 7 {
		t.Fatal("large oracle lost real evidence, original management facts or fixed control workload")
	}
	images := admissionEvidenceFiles(t, filepath.Dir(path))
	reads, err := svc.Store.ReadAdminTrail(testCtx)
	if err != nil || len(reads) != 1 || reads[0].Action != domain.AdminActionEventRead {
		t.Fatal("file fixture must retain its original independent read fact")
	}
	trailPath := path + ".admin-trail.jsonl"
	trailImage := catalogControlFileImage(t, trailPath)
	objects := &checkpointArchive{Store: sourceArchive}
	svc.Config.Archive = objects
	unknown := path + ".tmp"
	if err := os.WriteFile(unknown, []byte("owned-catalog-unknown-temp"), 0o600); err != nil {
		t.Fatal(err)
	}
	unknownImage := catalogControlFileImage(t, unknown)
	large := measure()
	for i, kind := range []string{"tenants", "schemas"} {
		t.Logf("catalog_real_v2 kind=%s hot_samples=1,32 receipts=2,33 calls_each=4 control_bytes=%d allocs_small=%.0f allocs_large=%.0f", kind, len(control.raw), small[i], large[i])
		if large[i] > small[i]+16 {
			t.Error("catalog read allocated in proportion to unrelated real hot/cold evidence")
		}
	}
	check := func(current *Service) {
		t.Helper()
		catalogControlCheckFileLists(t, current, before)
		catalogControlAssertFileImage(t, path, control)
		catalogControlAssertFileImage(t, unknown, unknownImage)
		catalogControlAssertFileImage(t, trailPath, trailImage)
		assertAdmissionEvidenceFilesUnchanged(t, images)
		after, err := current.Store.Snapshot(testCtx)
		trail, trailErr := current.Store.ReadAdminTrail(testCtx)
		if err != nil || trailErr != nil || !reflect.DeepEqual(after, before) || !reflect.DeepEqual(trail, reads) || objects.gets.Load() != 0 || objects.puts.Load() != 0 {
			t.Error("catalog read/restart changed original facts/evidence/control or performed WORM I/O")
		}
	}
	check(svc)
	if err := svc.Store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := reopenHoldControlFile(t, path)
	reopened.Config.Archive = objects
	check(reopened)
	versions := map[string]int{}
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		if err := reopened.Store.LedgerScan(testCtx, tenant, func(record store.LedgerRecord) error {
			if record.RecordType == store.LedgerReceipt {
				if record.Version > versions[record.Key] {
					versions[record.Key] = record.Version
				}
				if record.Receipt == nil || record.Receipt.TenantID != tenant {
					t.Error("real cold receipt lost its tenant binding")
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if len(versions) != 33 || versions[store.EventKey("tenant-a", "catalog-archived")] != 2 || versions[store.EventKey("tenant-b", "catalog-hot-31")] != 2 {
		t.Error("file restart lost exact real cold receipt versions")
	}
	catalogControlAssertFileImage(t, path, control)
	assertAdmissionEvidenceFilesUnchanged(t, images)
}
