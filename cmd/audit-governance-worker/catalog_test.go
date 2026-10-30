package main

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

type passCatalogContextKey struct{}

type passCatalogBackend struct {
	*scriptedConflictBackend
	loads       int
	loadErr     error
	observedErr error
	observedTag any
	deadline    time.Time
}

func (b *passCatalogBackend) Load(ctx context.Context) (*store.Snapshot, error) {
	b.loads++
	b.observedErr = ctx.Err()
	b.observedTag = ctx.Value(passCatalogContextKey{})
	b.deadline, _ = ctx.Deadline()
	return b.data, b.loadErr
}

func TestGovernancePassCatalogKeepsPublicAndPassWithdrawalSeparate(t *testing.T) {
	for _, withdrawal := range []string{"canceled", "deadline"} {
		t.Run(withdrawal, func(t *testing.T) {
			data := store.NewSnapshot()
			data.Tenants["tenant-b"] = domain.Tenant{ID: "tenant-b", Name: "B", Active: false}
			data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "A", Active: true}
			backend := &passCatalogBackend{scriptedConflictBackend: &scriptedConflictBackend{data: data}}
			st := store.NewWithBackend(backend)
			t.Cleanup(func() {
				if err := st.Close(); err != nil {
					t.Error(err)
				}
			})
			svc, err := service.New(st, service.Config{AllowDevSecrets: true})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.WithValue(context.Background(), passCatalogContextKey{}, "original-pass")
			var stop context.CancelFunc
			if withdrawal == "deadline" {
				ctx, stop = context.WithDeadline(ctx, time.Now().Add(-time.Second))
			} else {
				ctx, stop = context.WithCancel(ctx)
				stop()
			}
			defer stop()
			if got, err := svc.ListTenants(ctx); got != nil || !errors.Is(err, ctx.Err()) || backend.loads != 0 {
				t.Fatal("public catalog must reject withdrawal before any backend Load")
			}
			want := []domain.Tenant{data.Tenants["tenant-a"], data.Tenants["tenant-b"]}
			got, err := governancePassTenants(ctx, svc)
			if err != nil || !reflect.DeepEqual(got, want) || backend.loads != 1 || backend.observedErr != ctx.Err() || backend.observedTag != "original-pass" {
				t.Fatal("private pass enumeration detached context, changed ordering, retried, or inherited the public gate")
			}
			if bound, ok := ctx.Deadline(); ok && !backend.deadline.Equal(bound) {
				t.Fatal("private catalog replaced the earlier caller deadline")
			}
			got[0].Name = "caller-owned"
			if data.Tenants["tenant-a"].Name != "A" {
				t.Fatal("pass catalog result aliases the stored tenant metadata")
			}
			backend.loadErr = errors.New("actual backend catalog error")
			got, err = governancePassTenants(ctx, svc)
			if got != nil || !errors.Is(err, backend.loadErr) || backend.loads != 2 {
				t.Fatal("pass catalog swallowed an actual backend error or returned partial success")
			}
			if backend.saves != 0 || len(data.AdminActions) != 0 || len(data.Exports) != 0 || len(data.Receipts) != 0 {
				t.Fatal("pass catalog changed control, facts, jobs or ledger evidence")
			}
		})
	}
}
