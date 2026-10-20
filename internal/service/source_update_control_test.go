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

func sourceUpdateControlCandidate(at time.Time) domain.SourceSystem {
	return domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "Revised source", Active: true,
		AllowedClientIDs: []string{"relay-z", "relay-a"}, CreatedAt: at.Add(time.Hour)}
}

func expectedSourceUpdate(before *store.Snapshot, request domain.SourceSystem) domain.SourceSystem {
	request.AllowedClientIDs = append([]string(nil), request.AllowedClientIDs...)
	sort.Strings(request.AllowedClientIDs)
	request.CreatedAt = before.Sources[store.SourceKey(request.TenantID, request.ID)].CreatedAt
	return request
}

// Only the requested source replacement and one original mutation fact may
// change. The oracle uses a private copy, never writes state to satisfy itself.
func assertSourceUpdateControlDelta(t *testing.T, before, after *store.Snapshot, request domain.SourceSystem) {
	t.Helper()
	key := store.SourceKey(request.TenantID, request.ID)
	want := expectedSourceUpdate(before, request)
	if !reflect.DeepEqual(after.Sources[key], want) || len(after.AdminActions) != len(before.AdminActions)+1 {
		t.Fatal("source update lost its normalized replacement, current creation time or atomic fact")
	}
	fact := after.AdminActions[len(after.AdminActions)-1]
	if fact.ID == "" || fact.TenantID != want.TenantID || fact.Actor != "source-manager" ||
		fact.Action != domain.AdminActionSourceUpdated || fact.TargetType != "source" || fact.TargetID != want.ID ||
		fact.Detail != fmt.Sprintf("allowed_client_ids=%v active=%v", want.AllowedClientIDs, want.Active) || fact.CreatedAt.IsZero() {
		t.Error("source update changed original fact identity, scope or encoding")
	}
	copy, err := cloneTestSnapshot(after)
	if err != nil {
		t.Fatal("could not inspect private source update delta")
	}
	copy.Sources[key] = before.Sources[key]
	copy.AdminActions = copy.AdminActions[:len(copy.AdminActions)-1]
	if !reflect.DeepEqual(copy, before) {
		t.Error("source update replaced original evidence or unrelated current control")
	}
}

// Unknown Save really commits before returning an error. Neither the result
// nor cancellation is proof of rollback; all retries retain one original budget.
func TestSourceUpdateControlSaveOutcomesKeepAtomicFactAndOriginalBudget(t *testing.T) {
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
			request := sourceUpdateControlCandidate(svc.Now())
			got, err := svc.UpdateSource(ctx, "source-manager", request)
			known := mode == "success" || mode == "conflict"
			if known {
				if err != nil || !reflect.DeepEqual(got, expectedSourceUpdate(before, request)) {
					t.Error("known source update lost its committed normalized result")
				}
			} else if err == nil || !reflect.DeepEqual(got, domain.SourceSystem{}) {
				t.Error("failed or unknown source Save exposed an unconfirmed result")
			}
			if (mode == "failure" || mode == "unknown") && !errors.Is(err, errRestoreDecisionSave) {
				t.Error("source update hid the original Save failure")
			}
			if mode == "exhausted" && !errors.Is(err, store.ErrSnapshotConflict) {
				t.Error("source update changed finite conflict classification")
			}
			if mode == "deadline" && (!errors.Is(err, store.ErrOperationDeadline) || !errors.Is(err, context.DeadlineExceeded) || svc.Store.OperationDeadlineExhaustions() != 1) {
				t.Error("source update restarted or misclassified the original operation budget")
			}
			if (mode == "cancelled" || mode == "withdrawn-save" || mode == "committed-withdrawal") && !errors.Is(err, context.Canceled) {
				t.Error("source update lost caller withdrawal")
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
				t.Error("source update changed finite Saves, performed source I/O or invented a read fact")
			}
			if len(backend.writes) > 0 {
				for _, deadline := range append(append([]time.Time{}, backend.loads...), backend.writes...) {
					if deadline.IsZero() || !deadline.Equal(backend.writes[0]) {
						t.Error("source update retry restarted its original operation deadline")
					}
				}
			}
			current := restoreCreationSnapshot(t, svc)
			if known || mode == "unknown" || mode == "committed-withdrawal" {
				assertSourceUpdateControlDelta(t, before, current, request)
			} else if !reflect.DeepEqual(current, before) {
				t.Error("uncommitted source update leaked replacement or success fact")
			}
		})
	}
}

func TestSourceUpdateControlRetainsNormalizationAuthorizationAndSameValueFacts(t *testing.T) {
	for _, mode := range []string{"explicit-clients", "default-client", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			svc := newServiceOn(t, store.NewWithBackend(&checkpointBackend{data: store.NewSnapshot()}))
			t.Cleanup(func() { _ = svc.Store.Close() })
			seedTestDomain(t, svc)
			before := restoreCreationSnapshot(t, svc)
			request := sourceUpdateControlCandidate(svc.Now())
			if mode == "default-client" {
				request.AllowedClientIDs = nil
			} else if mode == "disabled" {
				request.Active = false
			}
			input := append([]string(nil), request.AllowedClientIDs...)
			for attempt := 0; attempt < 2; attempt++ {
				got, err := svc.UpdateSource(testCtx, "source-manager", request)
				if err != nil || !reflect.DeepEqual(got, expectedSourceUpdate(before, request)) || !reflect.DeepEqual(input, request.AllowedClientIDs) {
					t.Fatal("source normalization mutated caller input, defaulted disabled or replaced creation time")
				}
				after := restoreCreationSnapshot(t, svc)
				assertSourceUpdateControlDelta(t, before, after, request)
				if len(got.AllowedClientIDs) > 0 {
					got.AllowedClientIDs[0] = "mutated-result"
					if !reflect.DeepEqual(restoreCreationSnapshot(t, svc), after) {
						t.Fatal("returned source aliases committed control")
					}
				}
				before = after
			}
			for _, client := range []string{"crm", "relay-a", "relay-z", "unregistered"} {
				allowed := mode == "default-client" && client == "crm" || mode == "explicit-clients" && (client == "relay-a" || client == "relay-z")
				for _, hint := range []string{"tenant-a", ""} {
					resolved, err := svc.resolveIngestTenant(testCtx, hint, client, "crm")
					if allowed && (err != nil || resolved != "tenant-a") || !allowed && (!errors.Is(err, domain.ErrForbidden) || resolved != "") {
						t.Error("source update changed original signed-client binding or disabled-source refusal")
					}
				}
			}
		})
	}
}

func TestSourceUpdateControlRefusalsAreZeroWrite(t *testing.T) {
	backend := &restoreDecisionBackend{checkpointBackend: &checkpointBackend{data: store.NewSnapshot()}}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	seedTestDomain(t, svc)
	for _, mode := range []string{"missing-tenant", "missing-source", "empty-tenant", "duplicate-client", "untrimmed-client"} {
		t.Run(mode, func(t *testing.T) {
			request := sourceUpdateControlCandidate(svc.Now())
			want := domain.ErrInvalid
			switch mode {
			case "missing-tenant":
				request.TenantID, want = "tenant-b", domain.ErrNotFound
			case "missing-source":
				request.ID, want = "missing", domain.ErrNotFound
			case "empty-tenant":
				request.TenantID = ""
			case "duplicate-client":
				request.AllowedClientIDs = []string{"relay", "relay"}
			case "untrimmed-client":
				request.AllowedClientIDs = []string{" relay"}
			}
			before, saves := restoreCreationSnapshot(t, svc), backend.calls
			got, err := svc.UpdateSource(testCtx, "source-manager", request)
			if !errors.Is(err, want) || !reflect.DeepEqual(got, domain.SourceSystem{}) || backend.calls != saves || !reflect.DeepEqual(restoreCreationSnapshot(t, svc), before) {
				t.Error("refused source update changed error, wrote control or exposed an unconfirmed result")
			}
		})
	}
}
