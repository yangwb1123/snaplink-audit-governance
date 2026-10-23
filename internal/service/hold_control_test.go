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

func reopenHoldControlFile(t *testing.T, path string) *Service {
	t.Helper()
	st, err := store.Open(path)
	if err != nil {
		t.Fatal("could not reopen original Hold file store")
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("closing reopened Hold control file store: %v", err)
		}
	})
	svc := newServiceOn(t, st)
	pauseExportDispatch(svc)
	return svc
}

func assertHoldControlFacts(t *testing.T, facts []domain.AdminAction, id string, created, released int) {
	t.Helper()
	counts := map[string]int{}
	for _, fact := range facts {
		if fact.TargetID != id || (fact.Action != domain.AdminActionLegalHoldCreated && fact.Action != domain.AdminActionLegalHoldReleased) {
			continue
		}
		if fact.TenantID != "tenant-a" || fact.TargetType != "legal_hold" || fact.ID == "" {
			t.Fatal("Hold fact lost its original identity/scope encoding")
		}
		counts[fact.Action]++
	}
	if counts[domain.AdminActionLegalHoldCreated] != created || counts[domain.AdminActionLegalHoldReleased] != released {
		t.Fatal("Hold mutation lost or duplicated its atomic fact")
	}
}

var errHoldControlSave = errors.New("injected Hold control Save outcome")

type holdControlOutcomeBackend struct {
	*checkpointBackend
	mode      string
	calls     int
	reads     []time.Time
	writes    []time.Time
	conflicts int
}

func (b *holdControlOutcomeBackend) LoadForUpdate(ctx context.Context) (*store.Snapshot, error) {
	deadline, _ := ctx.Deadline()
	b.reads = append(b.reads, deadline)
	return b.checkpointBackend.LoadForUpdate(ctx)
}

func (b *holdControlOutcomeBackend) Save(ctx context.Context, data *store.Snapshot) error {
	b.calls++
	deadline, _ := ctx.Deadline()
	b.writes = append(b.writes, deadline)
	if b.mode == "failure" {
		return errHoldControlSave
	}
	if b.mode == "deadline" {
		<-ctx.Done()
		return ctx.Err()
	}
	if b.conflicts > 0 {
		b.conflicts--
		return store.ErrSnapshotConflict
	}
	if err := b.checkpointBackend.Save(ctx, data); err != nil {
		return err
	}
	if b.mode == "unknown" {
		return errHoldControlSave // actual commit, not a simulated rollback
	}
	return nil
}

func TestHoldControlSaveOutcomesKeepAtomicFactsAndOriginalBudget(t *testing.T) {
	for _, release := range []bool{false, true} {
		for _, mode := range []string{"success", "conflict", "failure", "unknown", "deadline", "cancelled"} {
			name := "create/"
			if release {
				name = "release/"
			}
			t.Run(name+mode, func(t *testing.T) {
				backend := &holdControlOutcomeBackend{checkpointBackend: &checkpointBackend{data: store.NewSnapshot()}}
				svc := newServiceOn(t, store.NewWithBackend(backend))
				seedTestDomain(t, svc)
				hold := domain.LegalHold{ID: "hold-control-outcome", TenantID: "tenant-a", Name: "case", Reason: "case", CreatedBy: "case-worker"}
				if release {
					if _, err := svc.CreateLegalHold(testCtx, hold); err != nil {
						t.Fatal("could not prepare active Hold")
					}
				}
				before := leaseSnapshot(t, svc)
				backend.calls, backend.reads, backend.writes = 0, nil, nil
				backend.mode = mode
				if mode == "conflict" {
					backend.conflicts = 2
				}
				if mode == "deadline" {
					svc.Store = store.NewWithBackendOptions(backend, store.Options{OperationTimeout: 200 * time.Millisecond})
				}
				ctx, cancel := context.WithCancel(testCtx)
				defer cancel()
				if mode == "cancelled" {
					cancel()
				}
				var got domain.LegalHold
				var err error
				if release {
					got, err = svc.ReleaseLegalHold(ctx, "tenant-a", hold.ID, "case-worker")
				} else {
					got, err = svc.CreateLegalHold(ctx, hold)
				}
				committed := mode == "success" || mode == "conflict" || mode == "unknown"
				if mode == "success" || mode == "conflict" {
					if err != nil || got.ID != hold.ID || (release && got.ReleasedAt == nil) {
						t.Fatal("successful Hold control Save lost its decision")
					}
				} else if err == nil || !reflect.DeepEqual(got, domain.LegalHold{}) {
					t.Fatal("failed/unknown Hold Save returned a successful decision")
				}
				if (mode == "failure" || mode == "unknown") && !errors.Is(err, errHoldControlSave) {
					t.Fatal("Hold control Save hid its original error")
				}
				if mode == "deadline" && (!errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || svc.Store.OperationDeadlineExhaustions() != 1) {
					t.Fatal("Hold control Save lost/doubled original timeout classification")
				}
				if mode == "cancelled" && (!errors.Is(err, context.Canceled) || backend.calls != 0) {
					t.Fatal("withdrawn Hold mutation entered Save")
				}
				wantCalls := 1
				if mode == "conflict" {
					wantCalls = 3
				} else if mode == "cancelled" {
					wantCalls = 0
				}
				if backend.calls != wantCalls {
					t.Fatal("Hold control Save changed bounded retry count")
				}
				if len(backend.writes) != 0 {
					deadline := backend.writes[0]
					for _, at := range append(append([]time.Time{}, backend.reads...), backend.writes...) {
						if at.IsZero() || !at.Equal(deadline) {
							t.Fatal("Hold CAS retry restarted the original operation budget")
						}
					}
				}
				current := leaseSnapshot(t, svc)
				created, released := 0, 0
				if release {
					created = 1
				}
				if committed {
					created = 1
					if release {
						released = 1
					}
					stored, exists := current.LegalHolds[hold.ID]
					if !exists || (release && stored.ReleasedBy != "case-worker") || (!release && stored.CreatedBy != "case-worker") {
						t.Fatal("actual/unknown Hold commit lost its durable decision")
					}
				} else if !reflect.DeepEqual(current, before) {
					t.Fatal("failed Hold control Save leaked partial control or facts")
				}
				assertHoldControlFacts(t, current.AdminActions, hold.ID, created, released)
			})
		}
	}
}

func TestHoldControlRepeatedReleaseIsKnownReadOnlyDecision(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	hold, err := svc.CreateLegalHold(testCtx, domain.LegalHold{ID: "hold-control-repeat", TenantID: "tenant-a", Name: "case", Reason: "case"})
	if err != nil {
		t.Fatal("could not prepare active Hold")
	}
	released, err := svc.ReleaseLegalHold(testCtx, "tenant-a", hold.ID, "first-releaser")
	if err != nil {
		t.Fatal("could not prepare released Hold")
	}
	saves := backend.saves
	backend.saveErr = errHoldControlSave
	again, err := svc.ReleaseLegalHold(testCtx, "tenant-a", hold.ID, "later-releaser")
	if err != nil || !reflect.DeepEqual(again, released) || backend.saves != saves {
		t.Fatal("already-released Hold attempted Save or replaced its first decision")
	}
	if _, err := svc.ReleaseLegalHold(testCtx, "tenant-b", hold.ID, "later-releaser"); !errors.Is(err, domain.ErrNotFound) || backend.saves != saves {
		t.Fatal("read-only repeated release crossed its tenant boundary")
	}
	assertHoldControlFacts(t, leaseSnapshot(t, svc).AdminActions, hold.ID, 1, 1)
}
