package service

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// The fixture observes the actual backend Load boundary, including a backend
// that returns a valid snapshot after the caller has withdrawn or expired.
type holdListReadControlBackend struct {
	*checkpointBackend
	mode     string
	cancel   context.CancelFunc
	loads    int
	appends  int
	deadline time.Time
	loadedAt time.Time
}

var errHoldListReadControlLoad = errors.New("injected hold list control Load failure")

func (b *holdListReadControlBackend) Load(ctx context.Context) (*store.Snapshot, error) {
	b.loads++
	b.loadedAt = time.Now()
	b.deadline, _ = ctx.Deadline()
	data, err := b.checkpointBackend.Load(ctx)
	if err != nil {
		return nil, err
	}
	switch b.mode {
	case "failure":
		return nil, errHoldListReadControlLoad
	case "cancel-with-snapshot":
		b.cancel()
	case "deadline-with-snapshot", "caller-deadline-with-snapshot":
		<-ctx.Done()
	}
	return data, nil
}

func (b *holdListReadControlBackend) AppendAdminFact(ctx context.Context, fact domain.AdminAction, cap int) error {
	b.appends++
	return b.checkpointBackend.AppendAdminFact(ctx, fact, cap)
}

func holdListReadControlHolds() []domain.LegalHold {
	released := pinnedNow.Add(-30 * time.Second)
	return []domain.LegalHold{
		{ID: "hold-list-later", TenantID: "tenant-a", Name: "later case", Reason: "second reason",
			Filter:    domain.Query{From: pinnedNow.Add(-2 * time.Hour), To: pinnedNow.Add(-time.Hour), EventType: "audit.event"},
			CreatedBy: "case-worker", CreatedAt: pinnedNow.Add(-time.Minute)},
		{ID: "hold-list-earlier", TenantID: "tenant-a", Name: "earlier case", Reason: "first reason",
			Filter:    domain.Query{From: pinnedNow.Add(-3 * time.Hour), To: pinnedNow.Add(-2 * time.Hour), SourceSystem: "crm"},
			CreatedBy: "case-worker", CreatedAt: pinnedNow.Add(-3 * time.Minute),
			ReleasedAt: &released, ReleasedBy: "release-worker"},
		{ID: "hold-list-neighbor", TenantID: "tenant-b", Name: "neighbor case", Reason: "neighbor reason",
			CreatedBy: "neighbor-worker", CreatedAt: pinnedNow.Add(-4 * time.Minute)},
	}
}

func TestHoldListControlLoadOutcomesKeepOneBudgetAndServeNoWithdrawnList(t *testing.T) {
	for _, mode := range []string{"success", "success-empty", "failure", "cancel-before", "cancel-with-snapshot", "deadline-with-snapshot", "caller-deadline-with-snapshot"} {
		t.Run(mode, func(t *testing.T) {
			data := store.NewSnapshot()
			for _, hold := range holdListReadControlHolds() {
				data.LegalHolds[hold.ID] = hold
			}
			original, err := cloneTestSnapshot(data)
			if err != nil {
				t.Fatal("could not capture original Hold metadata")
			}
			backend := &holdListReadControlBackend{checkpointBackend: &checkpointBackend{data: data}, mode: mode}
			svc := newServiceOn(t, store.NewWithBackendOptions(backend, store.Options{OperationTimeout: 200 * time.Millisecond}))
			t.Cleanup(func() {
				if err := svc.Store.Close(); err != nil {
					t.Errorf("closing scripted Hold list control store: %v", err)
				}
			})
			objects := &checkpointArchive{}
			svc.Config.Archive = objects
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			var callerDeadline time.Time
			if mode == "caller-deadline-with-snapshot" {
				var earlierCancel context.CancelFunc
				ctx, earlierCancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer earlierCancel()
				callerDeadline, _ = ctx.Deadline()
			}
			backend.cancel = cancel
			if mode == "cancel-before" {
				cancel()
			}
			tenant := "tenant-a"
			if mode == "success-empty" {
				tenant = "tenant-missing"
			}
			got, err := svc.ListLegalHolds(ctx, tenant)
			switch mode {
			case "success":
				want := []domain.LegalHold{holdListReadControlHolds()[1], holdListReadControlHolds()[0]}
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Error("Hold list changed its original sorted tenant metadata")
				}
			case "success-empty":
				// A successful no-match is an empty list, never nil: the HTTP
				// transport renders a nil slice as items:null.
				if err != nil || got == nil || len(got) != 0 {
					t.Error("successful no-match Hold list exposed nil instead of an empty list")
				}
			default:
				// A failed, withdrawn or expired read owns no partial result.
				if err == nil || got != nil {
					t.Error("failed or withdrawn Hold list exposed a successful list")
				}
			}
			if mode == "failure" && !errors.Is(err, errHoldListReadControlLoad) {
				t.Error("Hold list hid its original Load failure")
			}
			deadlineMode := mode == "deadline-with-snapshot" || mode == "caller-deadline-with-snapshot"
			if deadlineMode {
				if !errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || svc.Store.OperationDeadlineExhaustions() != 1 {
					t.Error("Hold list lost the original single deadline classification")
				}
			} else if svc.Store.OperationDeadlineExhaustions() != 0 {
				t.Error("Hold list invented a deadline exhaustion")
			}
			if (mode == "cancel-before" || mode == "cancel-with-snapshot") && !errors.Is(err, context.Canceled) {
				t.Error("Hold list lost caller withdrawal")
			}
			wantLoads := 1
			if mode == "cancel-before" {
				wantLoads = 0
			}
			if backend.loads != wantLoads || backend.saves != 0 || backend.appends != 0 || objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(backend.trail) != 0 {
				t.Error("Hold list retried Load, wrote control/facts or performed WORM I/O")
			}
			if wantLoads == 1 && (backend.deadline.IsZero() || backend.deadline.After(backend.loadedAt.Add(200*time.Millisecond))) {
				t.Error("Hold list restarted its original operation budget")
			}
			if !callerDeadline.IsZero() && !backend.deadline.Equal(callerDeadline) {
				t.Error("Hold list replaced the earlier caller deadline")
			}
			if !reflect.DeepEqual(backend.data, original) {
				t.Error("Hold list changed original stored metadata")
			}
		})
	}
}

func TestHoldListControlPreservesScopeOrderFieldsAndOwnedReleasedAt(t *testing.T) {
	st, err := store.Open("")
	if err != nil {
		t.Fatal("could not open owned Hold control fixture")
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("closing owned Hold list control fixture: %v", err)
		}
	})
	svc := newServiceOn(t, st)
	holds := holdListReadControlHolds()
	if err := st.UpdateControl(testCtx, func(data *store.Snapshot) error {
		for _, hold := range holds {
			data.LegalHolds[hold.ID] = hold
		}
		return nil
	}); err != nil {
		t.Fatal("could not prepare historical Hold metadata")
	}
	before := restoreCreationSnapshot(t, svc)
	objects := &checkpointArchive{}
	svc.Config.Archive = objects
	got, err := svc.ListLegalHolds(testCtx, "tenant-a")
	want := []domain.LegalHold{holds[1], holds[0]}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("Hold list reinterpreted fields, scope or CreatedAt ordering")
	}
	if got[0].ReleasedAt == nil || got[0].ReleasedBy != "release-worker" || got[1].ReleasedAt != nil {
		t.Fatal("Hold list lost active/released field preservation")
	}
	*got[0].ReleasedAt = pinnedNow
	got[0].Name = "caller-copy"
	next, err := svc.ListLegalHolds(testCtx, "tenant-a")
	if err != nil || !reflect.DeepEqual(next, want) {
		t.Error("returned Hold list or its ReleasedAt pointer was not independently owned")
	}
	if values, err := svc.ListLegalHolds(testCtx, "tenant-b"); err != nil || !reflect.DeepEqual(values, []domain.LegalHold{holds[2]}) {
		t.Error("Hold list mixed independent tenant Holds")
	}
	if values, err := svc.ListLegalHolds(testCtx, "tenant-missing"); err != nil || values == nil || len(values) != 0 {
		t.Error("Hold list invented a no-match entry or returned nil for an empty success")
	}
	if values, err := svc.ListLegalHolds(testCtx, ""); !errors.Is(err, domain.ErrInvalid) || len(values) != 0 {
		t.Error("Hold list changed empty-scope refusal")
	}
	if !reflect.DeepEqual(restoreCreationSnapshot(t, svc), before) || objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(readFacts(t, svc)) != 0 {
		t.Error("Hold list changed control/evidence or its original silent-read policy")
	}
}
