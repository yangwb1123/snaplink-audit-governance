package service

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func sourceCreationControlCandidate() domain.SourceSystem {
	return domain.SourceSystem{TenantID: "tenant-a", ID: "registration-source", Name: "New source",
		AllowedClientIDs: []string{"relay-z", "relay-a"}}
}

func expectedSourceCreation(request domain.SourceSystem, at time.Time) domain.SourceSystem {
	request.AllowedClientIDs = append([]string(nil), request.AllowedClientIDs...)
	sort.Strings(request.AllowedClientIDs)
	if request.CreatedAt.IsZero() {
		request.CreatedAt = at
	}
	request.Active = true // retain the original creation default, not UpdateSource disablement
	return request
}

// Inspect only an owned expected delta; the oracle never modifies the actual
// Store or database. Registration and its original fact are the entire change.
func assertSourceCreationControlDelta(t *testing.T, before, after *store.Snapshot, want domain.SourceSystem) {
	t.Helper()
	key := store.SourceKey(want.TenantID, want.ID)
	if _, existed := before.Sources[key]; existed {
		t.Fatal("creation delta oracle requires a previously absent source key")
	}
	if !reflect.DeepEqual(after.Sources[key], want) || len(after.AdminActions) != len(before.AdminActions)+1 {
		t.Fatal("source creation lost its normalized registration or one atomic fact")
	}
	fact := after.AdminActions[len(after.AdminActions)-1]
	if fact.ID == "" || fact.TenantID != want.TenantID || fact.Actor != "source-manager" ||
		fact.Action != domain.AdminActionSourceCreated || fact.TargetType != "source" || fact.TargetID != want.ID ||
		fact.Detail != fmt.Sprintf("allowed_client_ids=%v", want.AllowedClientIDs) || fact.CreatedAt.IsZero() {
		t.Error("source creation changed its original fact identity, scope or encoding")
	}
	copy, err := cloneTestSnapshot(after)
	if err != nil {
		t.Fatal("could not inspect private source creation delta")
	}
	delete(copy.Sources, key)
	copy.AdminActions = copy.AdminActions[:len(copy.AdminActions)-1]
	if !reflect.DeepEqual(copy, before) {
		t.Error("source creation replaced original evidence or unrelated current control")
	}
}

// This scripts actual Backend.Save, including a commit followed by an error.
// A failed response never proves rollback or authorizes automatic re-creation.
func TestSourceCreationControlSaveOutcomesKeepAtomicFactAndOriginalBudget(t *testing.T) {
	for _, mode := range []string{"success", "conflict", "exhausted", "failure", "unknown", "deadline", "cancelled", "withdrawn-save", "committed-withdrawal"} {
		t.Run(mode, func(t *testing.T) {
			backend := &restoreDecisionBackend{checkpointBackend: &checkpointBackend{data: store.NewSnapshot()}}
			svc := newServiceOn(t, store.NewWithBackend(backend))
			t.Cleanup(func() { _ = svc.Store.Close() })
			seedTestDomain(t, svc)
			before := restoreCreationSnapshot(t, svc)
			objects := &checkpointArchive{}
			svc.Config.Archive = objects
			backend.calls, backend.loads, backend.writes, backend.mode = 0, nil, nil, mode
			if mode == "conflict" {
				backend.conflicts = 2
			} else if mode == "exhausted" {
				backend.conflicts = 4
			}
			if mode == "deadline" {
				svc.Store = store.NewWithBackendOptions(backend, store.Options{OperationTimeout: 200 * time.Millisecond})
			}
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			backend.withdraw = cancel
			if mode == "cancelled" {
				cancel()
			}
			request := sourceCreationControlCandidate()
			want := expectedSourceCreation(request, svc.Now())
			err := svc.AddSource(ctx, "source-manager", request)
			known := mode == "success" || mode == "conflict"
			if known && err != nil || !known && err == nil {
				t.Error("source creation misreported its known or failed Save outcome")
			}
			if (mode == "failure" || mode == "unknown") && !errors.Is(err, errRestoreDecisionSave) {
				t.Error("source creation hid the original Save failure")
			}
			if mode == "exhausted" && !errors.Is(err, store.ErrSnapshotConflict) {
				t.Error("source creation changed finite conflict classification")
			}
			if mode == "deadline" && (!errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || svc.Store.OperationDeadlineExhaustions() != 1) {
				t.Error("source creation restarted or misclassified the original operation budget")
			}
			if (mode == "cancelled" || mode == "withdrawn-save" || mode == "committed-withdrawal") && !errors.Is(err, context.Canceled) {
				t.Error("source creation lost caller withdrawal")
			}
			wantSaves := 1
			switch mode {
			case "conflict":
				wantSaves = 3
			case "exhausted":
				wantSaves = 4
			case "cancelled":
				wantSaves = 0
			}
			if backend.calls != wantSaves || objects.gets.Load() != 0 || objects.puts.Load() != 0 || len(readFacts(t, svc)) != 0 {
				t.Error("source creation changed finite Saves, performed WORM I/O or invented a read fact")
			}
			if len(backend.writes) > 0 {
				for _, deadline := range append(append([]time.Time{}, backend.loads...), backend.writes...) {
					if deadline.IsZero() || !deadline.Equal(backend.writes[0]) {
						t.Error("source creation retry restarted its original operation deadline")
					}
				}
			}
			current := restoreCreationSnapshot(t, svc)
			if known || mode == "unknown" || mode == "committed-withdrawal" {
				assertSourceCreationControlDelta(t, before, current, want)
				// Explicitly inspect an unknown commit without blindly replaying it.
			} else if !reflect.DeepEqual(current, before) {
				t.Error("uncommitted source creation leaked registration or a success fact")
			}
		})
	}
}

func TestSourceCreationControlRetainsDefaultsBindingsAndDuplicateRefusal(t *testing.T) {
	for _, mode := range []string{"explicit-clients", "default-client", "supplied-time"} {
		t.Run(mode, func(t *testing.T) {
			backend := &restoreDecisionBackend{checkpointBackend: &checkpointBackend{data: store.NewSnapshot()}}
			svc := newServiceOn(t, store.NewWithBackend(backend))
			t.Cleanup(func() { _ = svc.Store.Close() })
			seedTestDomain(t, svc)
			before := restoreCreationSnapshot(t, svc)
			request := sourceCreationControlCandidate()
			if mode == "default-client" {
				request.AllowedClientIDs = nil
			} else if mode == "supplied-time" {
				request.CreatedAt, request.Active = svc.Now().Add(-time.Hour), true
			}
			input := append([]string(nil), request.AllowedClientIDs...)
			want := expectedSourceCreation(request, svc.Now())
			if err := svc.AddSource(testCtx, "source-manager", request); err != nil || !reflect.DeepEqual(input, request.AllowedClientIDs) {
				t.Fatal("source creation rejected original defaults or mutated caller input")
			}
			after := restoreCreationSnapshot(t, svc)
			assertSourceCreationControlDelta(t, before, after, want)
			if len(request.AllowedClientIDs) > 0 {
				request.AllowedClientIDs[0] = "mutated-input"
				if !reflect.DeepEqual(restoreCreationSnapshot(t, svc), after) {
					t.Fatal("caller allow-list aliases committed registration")
				}
			}
			saves := backend.calls
			if err := svc.AddSource(testCtx, "source-manager", want); !errors.Is(err, domain.ErrConflict) || backend.calls != saves || !reflect.DeepEqual(restoreCreationSnapshot(t, svc), after) {
				t.Fatal("duplicate registration became a replacement, idempotent success or another fact")
			}
			for _, client := range []string{want.ID, "relay-a", "relay-z", "unregistered"} {
				allowed := mode == "default-client" && client == want.ID || mode != "default-client" && (client == "relay-a" || client == "relay-z")
				for _, hint := range []string{"tenant-a", ""} {
					resolved, err := svc.resolveIngestTenant(testCtx, hint, client, want.ID)
					if allowed && (err != nil || resolved != "tenant-a") || !allowed && (!errors.Is(err, domain.ErrForbidden) || resolved != "") {
						t.Error("registration changed original signed-client binding or fail-closed resolution")
					}
				}
			}
		})
	}
}

func TestSourceCreationControlRefusalsAreZeroWrite(t *testing.T) {
	backend := &restoreDecisionBackend{checkpointBackend: &checkpointBackend{data: store.NewSnapshot()}}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	t.Cleanup(func() { _ = svc.Store.Close() })
	seedTestDomain(t, svc)
	for _, mode := range []string{"missing-tenant", "duplicate", "empty-tenant", "duplicate-client", "untrimmed-client", "framing"} {
		t.Run(mode, func(t *testing.T) {
			request := sourceCreationControlCandidate()
			want := domain.ErrInvalid
			switch mode {
			case "missing-tenant":
				request.TenantID, want = "tenant-b", domain.ErrNotFound
			case "duplicate":
				request.ID, want = "crm", domain.ErrConflict
			case "empty-tenant":
				request.TenantID = ""
			case "duplicate-client":
				request.AllowedClientIDs = []string{"relay", "relay"}
			case "untrimmed-client":
				request.AllowedClientIDs = []string{" relay"}
			case "framing":
				request.ID = "bad\x1fsource"
			}
			before, saves := restoreCreationSnapshot(t, svc), backend.calls
			if err := svc.AddSource(testCtx, "source-manager", request); !errors.Is(err, want) || backend.calls != saves || !reflect.DeepEqual(restoreCreationSnapshot(t, svc), before) {
				t.Error("refused registration changed error precedence, wrote control or exposed a partial fact")
			}
		})
	}
}

func TestSourceCreationControlAmbiguousUnscopedBindingStillFailsClosed(t *testing.T) {
	svc := newServiceOn(t, store.NewWithBackend(&checkpointBackend{data: store.NewSnapshot()}))
	t.Cleanup(func() { _ = svc.Store.Close() })
	seedTestDomain(t, svc)
	if err := svc.CreateTenant(testCtx, "test", domain.Tenant{ID: "tenant-b", Name: "Neighbor"}); err != nil {
		t.Fatal("could not prepare independent registered tenant")
	}
	request := sourceCreationControlCandidate()
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		request.TenantID = tenant
		if err := svc.AddSource(testCtx, "source-manager", request); err != nil {
			t.Fatal("registration changed tenant-scoped source-key uniqueness")
		}
		if resolved, err := svc.resolveIngestTenant(testCtx, tenant, "relay-a", request.ID); err != nil || resolved != tenant {
			t.Fatal("signed tenant no longer narrows the original client/source resolution")
		}
	}
	if resolved, err := svc.resolveIngestTenant(testCtx, "", "relay-a", request.ID); !errors.Is(err, domain.ErrForbidden) || resolved != "" {
		t.Fatal("ambiguous client/source registration acquired unscoped ingest authority")
	}
}
