package service

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Reuse the actual Load oracle without changing its existing tests or modes.
// Record the context identity as well as its deadline, including nested calls.
type listSourcesOperationBackend struct {
	*catalogControlBackend
	loadCtx context.Context
}

func (b *listSourcesOperationBackend) Load(ctx context.Context) (*store.Snapshot, error) {
	b.loadCtx = ctx
	return b.catalogControlBackend.Load(ctx)
}

func listSourcesOperationSnapshot() *store.Snapshot {
	data := store.NewSnapshot()
	data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "A"}
	data.Tenants["tenant-b"] = domain.Tenant{ID: "tenant-b", Name: "B"}
	for _, source := range []domain.SourceSystem{
		{TenantID: "tenant-a", ID: "zeta", Name: "Inactive empty", AllowedClientIDs: []string{}, Active: false, CreatedAt: pinnedNow.Add(-time.Hour)},
		{TenantID: "tenant-a", ID: "alpha", Name: "Unnormalized historical list", AllowedClientIDs: []string{"relay-z", "relay-a"}, Active: true, CreatedAt: pinnedNow},
		{TenantID: "tenant-a", ID: "legacy", Name: "Legacy over cap", AllowedClientIDs: sequentialIDs(domain.MaxSourceClientIDs + 64), Active: false, CreatedAt: pinnedNow.Add(-time.Minute)},
		{TenantID: "tenant-b", ID: "alpha", Name: "Neighbor nil", Active: true, CreatedAt: pinnedNow},
		{TenantID: "tenant-ab", ID: "alpha", Name: "Prefix neighbor", CreatedAt: pinnedNow},
		{TenantID: "unregistered", ID: "orphan", Name: "No current tenant", CreatedAt: pinnedNow},
		{TenantID: "组织.1-_", ID: "valid", Name: "Valid non-ASCII scope", CreatedAt: pinnedNow},
	} {
		data.Sources[store.SourceKey(source.TenantID, source.ID)] = source
	}
	// ListSources historically filters the row's exact TenantID, not its map
	// key. Pin both directions so neither key-prefix nor key-only filtering
	// can silently replace that contract.
	data.Sources[store.SourceKey("tenant-a", "miskeyed-neighbor")] = domain.SourceSystem{TenantID: "tenant-ab", ID: "miskeyed-neighbor", Name: "Foreign field", CreatedAt: pinnedNow}
	data.Sources[store.SourceKey("tenant-ab", "field-owned")] = domain.SourceSystem{TenantID: "tenant-a", ID: "field-owned", Name: "Exact field", CreatedAt: pinnedNow}
	return data
}

func listSourcesOperationWant(data *store.Snapshot) []domain.SourceSystem {
	want := []domain.SourceSystem{
		data.Sources[store.SourceKey("tenant-a", "alpha")],
		data.Sources[store.SourceKey("tenant-ab", "field-owned")],
		data.Sources[store.SourceKey("tenant-a", "legacy")],
		data.Sources[store.SourceKey("tenant-a", "zeta")],
	}
	for i := range want {
		// Original JSON/control cloning omits empty optional allow-lists.
		want[i].AllowedClientIDs = append([]string(nil), want[i].AllowedClientIDs...)
	}
	return want
}

func listSourcesOperationNoEffects(t *testing.T, backend *catalogControlBackend, objects *checkpointArchive) {
	t.Helper()
	if backend.loadsForUpdate != 0 || backend.saves != 0 || backend.appends != 0 || objects.gets.Load() != 0 || objects.puts.Load() != 0 {
		t.Errorf("source observation wrote state/facts or touched WORM: LoadForUpdate=%d Save=%d append=%d Get=%d Put=%d", backend.loadsForUpdate, backend.saves, backend.appends, objects.gets.Load(), objects.puts.Load())
	}
}

func TestListSourcesOperationRealV2FileWithdrawalAndRestart(t *testing.T) {
	svc, path := catalogControlFileFixture(t)
	sourceArchive := svc.Config.Archive
	svc.Config.Archive = nil
	event := testEvent("sources-operation-hot", "sources-neighbor-operation", pinnedNow.Add(2*time.Second))
	event.TenantID = "tenant-b"
	event.Payload["value"] = json.Number("10")
	if _, err := svc.Ingest(testCtx, "tenant-b", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource(testCtx, "sources-manager", domain.SourceSystem{TenantID: "tenant-a", ID: "zeta", Name: "Zeta"}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpdateSource(testCtx, "sources-manager", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "Inactive CRM", AllowedClientIDs: []string{"relay-b", "relay-a"}, Active: false}); err != nil {
		t.Fatal(err)
	}
	control := catalogControlFileImage(t, path)
	var diskControl store.Snapshot
	if err := json.Unmarshal(control.raw, &diskControl); err != nil || diskControl.LayoutVersion != 2 || len(diskControl.Events) != 0 || len(diskControl.Receipts) != 0 || !svc.Store.HotCold(testCtx) {
		t.Fatal("source cancellation oracle requires the real control-only v2 file path")
	}
	before, err := svc.Store.Snapshot(testCtx)
	if err != nil || len(before.Events) != 1 || len(before.Receipts) != 2 || len(before.AdminActions) != 9 {
		t.Fatal("source file fixture lost original management facts or real hot/archived evidence")
	}
	reads, err := svc.Store.ReadAdminTrail(testCtx)
	if err != nil || len(reads) != 1 || reads[0].Action != domain.AdminActionEventRead {
		t.Fatal("source file fixture lost its original independent read fact")
	}
	images := admissionEvidenceFiles(t, filepath.Dir(path))
	if len(images) != 4 {
		t.Fatal("source file oracle needs both real tenant hot documents and cold ledgers")
	}
	images[path] = control
	images[path+".admin-trail.jsonl"] = catalogControlFileImage(t, path+".admin-trail.jsonl")
	archiveFiles := 0
	if err := filepath.WalkDir(filepath.Join(filepath.Dir(path), "archive"), func(ownedPath string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			images[ownedPath] = catalogControlFileImage(t, ownedPath)
			archiveFiles++
		}
		return nil
	}); err != nil || archiveFiles == 0 {
		t.Fatal("source file fixture needs actual archived object bytes")
	}
	ledger := func(current *Service) map[string][]store.LedgerRecord {
		t.Helper()
		records := map[string][]store.LedgerRecord{}
		versions := map[string]int{}
		for _, tenant := range []string{"tenant-a", "tenant-b"} {
			if err := current.Store.LedgerScan(testCtx, tenant, func(record store.LedgerRecord) error {
				records[tenant] = append(records[tenant], record)
				if record.RecordType == store.LedgerReceipt {
					if record.Receipt == nil || record.Receipt.TenantID != tenant {
						t.Error("source observation lost cold receipt tenant binding")
					}
					if record.Version > versions[record.Key] {
						versions[record.Key] = record.Version
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		if len(versions) != 2 || versions[store.EventKey("tenant-a", "catalog-archived")] != 2 || versions[store.EventKey("tenant-b", "sources-operation-hot")] != 2 {
			t.Error("source observation/restart lost exact committed receipt versions")
		}
		return records
	}
	originalLedger := ledger(svc)
	objects := &checkpointArchive{Store: sourceArchive}
	check := func(current *Service, phase string) {
		t.Helper()
		current.Config.Archive = objects
		ctx, cancel := context.WithCancel(testCtx)
		cancel()
		got, err := current.ListSources(ctx, "tenant-a")
		ids := make([]string, len(got))
		for i := range got {
			ids[i] = got[i].ID
		}
		t.Logf("real_v2_pre_cancel phase=%s layout=%d err=%v ids=%v nil=%t control_bytes=%d", phase, diskControl.LayoutVersion, err, ids, got == nil, len(control.raw))
		if !errors.Is(err, context.Canceled) || got != nil {
			t.Errorf("real_v2_pre_cancel phase=%s: err=%v ids=%v nil=%t; want context.Canceled and nil list", phase, err, ids, got == nil)
		}
		want := []domain.SourceSystem{before.Sources[store.SourceKey("tenant-a", "crm")], before.Sources[store.SourceKey("tenant-a", "zeta")]}
		got, err = current.ListSources(testCtx, "tenant-a")
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("real v2 source list changed original fields, inactive entries or ID order")
		}
		got[0].Name, got[0].AllowedClientIDs[0] = "caller", "caller"
		if next, err := current.ListSources(testCtx, "tenant-a"); err != nil || !reflect.DeepEqual(next, want) {
			t.Error("real v2 source results alias persisted metadata")
		}
		if got, err := current.ListSources(testCtx, "tenant-b"); err != nil || !reflect.DeepEqual(got, []domain.SourceSystem{before.Sources[store.SourceKey("tenant-b", "crm")]}) {
			t.Error("real v2 source list mixed neighboring tenants")
		}
		if got, err := current.ListSources(testCtx, "absent"); err != nil || got != nil {
			t.Error("real v2 empty source match must remain nil success")
		}
		after, snapshotErr := current.Store.Snapshot(testCtx)
		trail, trailErr := current.Store.ReadAdminTrail(testCtx)
		if snapshotErr != nil || trailErr != nil || !reflect.DeepEqual(after, before) || !reflect.DeepEqual(trail, reads) || !reflect.DeepEqual(ledger(current), originalLedger) {
			t.Error("source observation/restart changed original control, evidence, versions or facts")
		}
		assertAdmissionEvidenceFilesUnchanged(t, images)
		if objects.gets.Load() != 0 || objects.puts.Load() != 0 || current.Store.OperationDeadlineExhaustions() != 0 {
			t.Error("source observation touched WORM or counted cancellation as a deadline")
		}
	}
	check(svc, "before-close")
	if err := svc.Store.Close(); err != nil {
		t.Fatal(err)
	}
	check(reopenHoldControlFile(t, path), "after-restart")
}

func TestListSourcesOperationLoadWithdrawalAndSingleBudget(t *testing.T) {
	type callerValueKey struct{}
	for _, mode := range []string{"success", "load-error", "load-error-with-snapshot", "cancel-before", "cancel-with-snapshot", "deadline-with-snapshot", "caller-deadline-with-snapshot"} {
		t.Run(mode, func(t *testing.T) {
			data := listSourcesOperationSnapshot()
			base := &catalogControlBackend{data: data, mode: mode}
			backend := &listSourcesOperationBackend{catalogControlBackend: base}
			svc := newServiceOn(t, store.NewWithBackendOptions(backend, store.Options{OperationTimeout: 200 * time.Millisecond}))
			t.Cleanup(func() { _ = svc.Store.Close() })
			objects := &checkpointArchive{Store: &recordingArchive{}}
			svc.Config.Archive = objects
			value := new(int)
			ctx, cancel := context.WithCancel(context.WithValue(testCtx, callerValueKey{}, value))
			defer cancel()
			var callerDeadline time.Time
			if mode == "caller-deadline-with-snapshot" {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 80*time.Millisecond)
				defer stop()
				callerDeadline, _ = ctx.Deadline()
			}
			base.cancel = cancel
			if mode == "cancel-before" {
				cancel()
			}
			started := time.Now()
			got, err := svc.ListSources(ctx, "tenant-a")
			t.Logf("source_load_oracle mode=%s items=%d nil=%t err=%v loads=%d deadlines=%d", mode, len(got), got == nil, err, base.loads, svc.Store.OperationDeadlineExhaustions())
			if mode == "success" {
				if err != nil || !reflect.DeepEqual(got, listSourcesOperationWant(data)) {
					t.Error("successful source read changed fields, tenant filtering, legacy entries or ordering")
				}
			} else if err == nil || got != nil {
				t.Error("failed or withdrawn source read must return an error and nil list")
			}
			if (mode == "load-error" || mode == "load-error-with-snapshot") && err != errCatalogControlLoad {
				t.Error("source read replaced the original Load error")
			}
			deadlineMode := mode == "deadline-with-snapshot" || mode == "caller-deadline-with-snapshot"
			if deadlineMode {
				if !errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || svc.Store.OperationDeadlineExhaustions() != 1 {
					t.Error("actual source deadline must be classified and counted exactly once")
				}
			} else if svc.Store.OperationDeadlineExhaustions() != 0 || errors.Is(err, store.ErrOperationDeadline) {
				t.Error("source read invented a deadline classification")
			}
			if (mode == "cancel-before" || mode == "cancel-with-snapshot") && !errors.Is(err, context.Canceled) {
				t.Error("source read lost caller cancellation after a valid snapshot was captured")
			}
			wantLoads := 1
			if mode == "cancel-before" {
				wantLoads = 0
			}
			if base.loads != wantLoads {
				t.Errorf("Load calls=%d, want %d (no pre-cancel Load or retry)", base.loads, wantLoads)
			}
			if wantLoads == 1 {
				if backend.loadCtx == nil || backend.loadCtx.Value(callerValueKey{}) != value || base.deadline.IsZero() || base.deadline.After(base.loadedAt.Add(200*time.Millisecond)) {
					t.Error("source Load lost caller values or the operation deadline bound")
				}
				if !callerDeadline.IsZero() && !base.deadline.Equal(callerDeadline) {
					t.Error("source Load replaced the earlier caller deadline")
				}
				if callerDeadline.IsZero() && base.deadline.Before(started.Add(200*time.Millisecond)) {
					t.Error("source Load did not receive this call's operation budget")
				}
			}
			listSourcesOperationNoEffects(t, base, objects)
			if !reflect.DeepEqual(data, listSourcesOperationSnapshot()) {
				t.Error("source observation changed the original stored snapshot")
			}
		})
	}
}

func TestListSourcesOperationInheritsExistingOperation(t *testing.T) {
	type callerValueKey struct{}
	for _, mode := range []string{"success", "deadline-with-snapshot"} {
		t.Run(mode, func(t *testing.T) {
			base := &catalogControlBackend{data: listSourcesOperationSnapshot(), mode: mode}
			backend := &listSourcesOperationBackend{catalogControlBackend: base}
			svc := newServiceOn(t, store.NewWithBackendOptions(backend, store.Options{OperationTimeout: 200 * time.Millisecond}))
			t.Cleanup(func() { _ = svc.Store.Close() })
			objects := &checkpointArchive{Store: &recordingArchive{}}
			svc.Config.Archive = objects
			value := new(int)
			ctx := context.WithValue(testCtx, callerValueKey{}, value)
			var got []domain.SourceSystem
			err := svc.Store.WithOperation(ctx, func(opCtx context.Context) error {
				originalDeadline, _ := opCtx.Deadline()
				// Spend part of the original budget before ListSources. Context
				// identity/deadline equality, not wall-time speed, pins no retiming.
				timer := time.NewTimer(30 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-opCtx.Done():
					return opCtx.Err()
				}
				var readErr error
				got, readErr = svc.ListSources(opCtx, "tenant-a")
				if backend.loadCtx != opCtx || backend.loadCtx.Value(callerValueKey{}) != value || !base.deadline.Equal(originalDeadline) {
					t.Error("source Load replaced the original operation context, value or deadline")
				}
				return readErr
			})
			if mode == "success" {
				if err != nil || !reflect.DeepEqual(got, listSourcesOperationWant(base.data)) || svc.Store.OperationDeadlineExhaustions() != 0 {
					t.Error("source read failed within an existing active operation")
				}
			} else if !errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || got != nil || svc.Store.OperationDeadlineExhaustions() != 1 {
				t.Errorf("existing operation exhaustion: err=%v items=%d nil=%t deadlines=%d; want classified deadline, nil list and one count", err, len(got), got == nil, svc.Store.OperationDeadlineExhaustions())
			}
			if base.loads != 1 {
				t.Errorf("existing operation Load calls=%d, want exactly 1", base.loads)
			}
			listSourcesOperationNoEffects(t, base, objects)
		})
	}
}

func TestListSourcesOperationScopePriority(t *testing.T) {
	for _, signal := range []string{"active", "cancelled", "expired"} {
		t.Run(signal, func(t *testing.T) {
			base := &catalogControlBackend{data: listSourcesOperationSnapshot(), mode: "load-error-with-snapshot"}
			svc := newServiceOn(t, store.NewWithBackend(base))
			t.Cleanup(func() { _ = svc.Store.Close() })
			objects := &checkpointArchive{Store: &recordingArchive{}}
			svc.Config.Archive = objects
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			if signal == "cancelled" {
				cancel()
			} else if signal == "expired" {
				var stop context.CancelFunc
				ctx, stop = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer stop()
			}
			for _, tenant := range []string{"", "tenant/a", "tenant\\a", "tenant a", "tenant:a", "tenant" + store.KeySeparator + "a", "tenant\x00a", "tenant\x7fa", "tenant\na"} {
				wantErr := requireTenantScope(tenant)
				got, err := svc.ListSources(ctx, tenant)
				if !errors.Is(err, domain.ErrInvalid) || got != nil || wantErr == nil || err.Error() != wantErr.Error() {
					t.Errorf("scope %q with %s context: err=%v; original scope refusal must precede cancellation, deadline and Load error", tenant, signal, err)
				}
			}
			if base.loads != 0 || svc.Store.OperationDeadlineExhaustions() != 0 {
				t.Error("invalid source scope entered Load or an operation deadline before refusal")
			}
			listSourcesOperationNoEffects(t, base, objects)
		})
	}
}

func listSourcesOperationCatalog(t *testing.T, layout string) (*Service, *catalogControlBackend, func(func(*store.Snapshot))) {
	t.Helper()
	data := listSourcesOperationSnapshot()
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
		change(func(current *store.Snapshot) { current.Tenants, current.Sources = data.Tenants, data.Sources })
	}
	t.Cleanup(func() { _ = st.Close() })
	return newServiceOn(t, st), backend, change
}

func TestListSourcesOperationFieldsIsolationOrderingAndEmptyNil(t *testing.T) {
	for _, layout := range []string{"v1-load", "v2-control"} {
		t.Run(layout, func(t *testing.T) {
			svc, backend, _ := listSourcesOperationCatalog(t, layout)
			objects := &checkpointArchive{Store: &recordingArchive{}}
			svc.Config.Archive = objects
			if got, err := svc.ListSources(testCtx, "tenant-a"); err != nil || !reflect.DeepEqual(got, listSourcesOperationWant(backend.data)) {
				t.Error("source list changed exact tenant-field filtering, ID order, inactive/over-cap rows, fields or nil/empty optional semantics")
			}
			for tenant, count := range map[string]int{"tenant-b": 1, "tenant-ab": 2, "unregistered": 1, "组织.1-_": 1} {
				got, err := svc.ListSources(testCtx, tenant)
				if err != nil || len(got) != count {
					t.Errorf("valid source scope %q: items=%d err=%v, want %d without requiring tenant registration", tenant, len(got), err, count)
				}
				for i, source := range got {
					if source.TenantID != tenant || source.AllowedClientIDs != nil || (i > 0 && got[i-1].ID >= source.ID) {
						t.Error("neighbor source list widened its exact tenant field, lost ID order or changed nil allow-lists")
					}
				}
			}
			if got, err := svc.ListSources(testCtx, "absent"); err != nil || got != nil {
				t.Error("successful empty source match must stay nil, not 404 or a non-nil empty list")
			}
			listSourcesOperationNoEffects(t, backend, objects)
		})
		t.Run(layout+"/empty-catalog", func(t *testing.T) {
			svc, backend, change := listSourcesOperationCatalog(t, layout)
			change(func(data *store.Snapshot) { data.Tenants, data.Sources = nil, nil })
			if got, err := svc.ListSources(testCtx, "absent"); err != nil || got != nil {
				t.Error("empty source catalog introduced tenant existence checking or a non-nil result")
			}
			listSourcesOperationNoEffects(t, backend, &checkpointArchive{})
		})
	}
}

func TestListSourcesOperationBidirectionalOwnershipAndFreshReads(t *testing.T) {
	for _, layout := range []string{"v1-load", "v2-control"} {
		t.Run(layout, func(t *testing.T) {
			svc, backend, change := listSourcesOperationCatalog(t, layout)
			objects := &checkpointArchive{Store: &recordingArchive{}}
			svc.Config.Archive = objects
			want := listSourcesOperationWant(backend.data)
			first, err := svc.ListSources(testCtx, "tenant-a")
			if err != nil || !reflect.DeepEqual(first, want) {
				t.Fatal("could not capture the original source list")
			}
			second, err := svc.ListSources(testCtx, "tenant-a")
			if err != nil || !reflect.DeepEqual(second, want) {
				t.Fatal("could not independently repeat the source read")
			}
			first[0].Name, first[0].Active, first[0].AllowedClientIDs[0] = "caller", false, "caller"
			first[2].AllowedClientIDs[len(first[2].AllowedClientIDs)-1] = "caller-legacy"
			if !reflect.DeepEqual(second, want) {
				t.Error("repeated source results share their list or nested allow-list backing arrays")
			}
			if next, err := svc.ListSources(testCtx, "tenant-a"); err != nil || !reflect.DeepEqual(next, want) {
				t.Error("caller mutation escaped into stored source metadata")
			}
			change(func(data *store.Snapshot) {
				key := store.SourceKey("tenant-a", "alpha")
				source := data.Sources[key]
				source.Name, source.Active, source.AllowedClientIDs[0] = "current-control", false, "current-client"
				data.Sources[key] = source
			})
			if !reflect.DeepEqual(second, want) {
				t.Error("later stored source mutation changed a previously returned list or allow-list")
			}
			want[0].Name, want[0].Active, want[0].AllowedClientIDs[0] = "current-control", false, "current-client"
			if next, err := svc.ListSources(testCtx, "tenant-a"); err != nil || !reflect.DeepEqual(next, want) {
				t.Error("new source read reused cached metadata instead of current control")
			}
			if layout == "v1-load" && backend.loads != 4 {
				t.Errorf("four source calls performed %d Loads; want no retries or extra reads", backend.loads)
			}
			listSourcesOperationNoEffects(t, backend, objects)
		})
	}
}
