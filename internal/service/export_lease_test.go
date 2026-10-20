package service

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func seedLeaseExport(t *testing.T, svc *Service, id, tenant string) {
	t.Helper()
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		data.Exports[id] = domain.ExportJob{ID: id, TenantID: tenant, RequestedBy: "operator", Status: "pending",
			CreatedAt: svc.Now(), Query: domain.Query{From: svc.Now(), To: svc.Now().Add(time.Minute)}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func leaseSnapshot(t *testing.T, svc *Service) *store.Snapshot {
	t.Helper()
	var result *store.Snapshot
	if err := svc.Store.ReadControl(testCtx, func(data *store.Snapshot) error { result = data; return nil }); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestExportLeaseClaimAndOwnerTenantFences(t *testing.T) {
	svc := testService(t, true)
	other, err := New(svc.Store, svc.Config)
	if err != nil {
		t.Fatal(err)
	}
	seedLeaseExport(t, svc, "lease-claim", "tenant-a")
	start := make(chan struct{})
	claims := make(chan exportClaim, 2)
	var wg sync.WaitGroup
	for _, replica := range []*Service{svc, other} {
		wg.Add(1)
		go func(s *Service) {
			defer wg.Done()
			<-start
			claim, won, err := s.claimPendingExport(testCtx, "lease-claim")
			if err != nil {
				t.Error(err)
			}
			if won {
				claims <- claim
			}
		}(replica)
	}
	close(start)
	wg.Wait()
	close(claims)
	if len(claims) != 1 {
		t.Fatal("shared claim did not have exactly one owner")
	}
	claim := <-claims
	data := leaseSnapshot(t, svc)
	lease := data.ExportLeases[claim.ID]
	if !validExportLease(data.Exports[claim.ID], lease) || lease.Owner != claim.Owner ||
		!lease.HeartbeatAt.Equal(svc.Now()) || !lease.ExpiresAt.Equal(svc.Now().Add(ExportLeaseDuration)) {
		t.Fatal("claim and lease were not saved together")
	}
	wrong := claim
	wrong.Owner = "not-the-owner"
	foreign := claim
	foreign.TenantID = "tenant-b"
	for _, stale := range []exportClaim{wrong, foreign} {
		if err := other.renewExportLease(testCtx, stale); !errors.Is(err, errExportLeaseLost) {
			t.Fatal("a different owner/tenant renewed this lease")
		}
		other.finishExport(exportClaimContext(testCtx, stale), claim.ID, "completed", "foreign-path", "foreign-digest", 99, "")
	}
	svc.finishExport(testCtx, claim.ID, "completed", "unowned-path", "unowned-digest", 1, "")
	if got := leaseSnapshot(t, svc); got.Exports[claim.ID].Status != "running" || got.ExportLeases[claim.ID] != lease {
		t.Fatal("missing/foreign ownership changed job or lease")
	}
	job, err := svc.GetExport(testCtx, claim.TenantID, claim.ID)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(job)
	if err != nil || strings.Contains(string(raw), claim.Owner) || strings.Contains(string(raw), "export_leases") || strings.Contains(string(raw), "heartbeat_at") {
		t.Fatal("public job exposed execution ownership")
	}
	other.finishExport(exportClaimContext(testCtx, claim), claim.ID, "completed", "sealed-object", "digest", 1, "")
	if got := leaseSnapshot(t, svc); got.Exports[claim.ID].Status != "completed" || len(got.ExportLeases) != 0 {
		t.Fatal("owned completion did not atomically retire lease")
	}
}

func TestExportLeaseRenewalExpiryAndLegacyRecoveryBoundary(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	var clock atomic.Int64
	clock.Store(pinnedNow.UnixNano())
	svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	seedLeaseExport(t, svc, "leased", "tenant-a")
	claim, won, err := svc.claimPendingExport(testCtx, "leased")
	if err != nil || !won {
		t.Fatal(err)
	}
	initial := leaseSnapshot(t, svc).ExportLeases[claim.ID]
	clock.Store(pinnedNow.Add(ExportHeartbeatInterval).UnixNano())
	if err := svc.renewExportLease(testCtx, claim); err != nil {
		t.Fatal(err)
	}
	renewed := leaseSnapshot(t, svc).ExportLeases[claim.ID]
	if !renewed.HeartbeatAt.Equal(svc.Now()) || !renewed.ExpiresAt.After(initial.ExpiresAt) {
		t.Fatal("heartbeat did not durably extend expiry")
	}
	// Age recovery must not kill a healthy leased job even with a short old
	// age setting; legacy jobs still use the strict StartedAt/CreatedAt rule.
	svc.Config.StuckExportAge = time.Second
	if n, err := svc.RecoverStuckExports(testCtx, "tenant-a"); err != nil || n != 0 {
		t.Fatalf("healthy lease was failed by old running age: %d %v", n, err)
	}
	clock.Store(renewed.ExpiresAt.Add(-time.Nanosecond).UnixNano())
	if n, err := svc.recoverExpiredExportLeases(testCtx); err != nil || n != 0 {
		t.Fatal("reaped before expiry")
	}
	clock.Store(renewed.ExpiresAt.UnixNano())
	if err := svc.renewExportLease(testCtx, claim); !errors.Is(err, errExportLeaseLost) {
		t.Fatal("expired lease was revived")
	}
	svc.finishExport(exportClaimContext(testCtx, claim), claim.ID, "completed", "late-path", "late-digest", 1, "")
	if got := leaseSnapshot(t, svc); got.Exports[claim.ID].Status != "running" {
		t.Fatal("expired owner published terminal success")
	}
	n, err := svc.recoverExpiredExportLeases(testCtx)
	if err != nil || n != 1 {
		t.Fatalf("expiry recovery: %d %v", n, err)
	}
	got := leaseSnapshot(t, svc)
	if got.Exports[claim.ID].Status != "failed" || got.Exports[claim.ID].ObjectPath != "" || len(got.ExportLeases) != 0 {
		t.Fatal("expiry did not fail/retire metadata atomically")
	}
	facts := 0
	for _, fact := range got.AdminActions {
		if fact.Action == domain.AdminActionExportRecovered {
			facts++
			if fact.Detail != "lease_expired status=failed" || strings.Contains(fact.Detail, claim.Owner) {
				t.Fatal("recovery fact lost reason or leaked ownership")
			}
		}
	}
	if facts != 1 {
		t.Fatal("expiry recovery fact was not singular")
	}
	saves := backend.saves
	if n, err := svc.recoverExpiredExportLeases(testCtx); err != nil || n != 0 || backend.saves != saves {
		t.Fatal("idle/terminal recovery wrote state")
	}
}

func TestExportLeaseCASFailureReportsNoClaimOrPartialRecovery(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	seedLeaseExport(t, svc, "fail-claim", "tenant-a")
	primary := errors.New("save unavailable")
	backend.saveErr = primary
	if claim, won, err := svc.claimPendingExport(testCtx, "fail-claim"); !errors.Is(err, primary) || won || claim.Owner != "" {
		t.Fatal("failed Save returned an owned claim")
	}
	backend.saveErr = nil
	backend.conflicts = 1
	claim, won, err := svc.claimPendingExport(testCtx, "fail-claim")
	if err != nil || !won {
		t.Fatal(err)
	}
	svc.Config.Now = func() time.Time { return pinnedNow.Add(ExportLeaseDuration) }
	backend.saveErr = primary
	if n, err := svc.recoverExpiredExportLeases(testCtx); !errors.Is(err, primary) || n != 0 {
		t.Fatal("failed/unknown Save reported recovered count")
	}
	if backend.data.Exports[claim.ID].Status != "running" || backend.data.ExportLeases[claim.ID].Owner != claim.Owner {
		t.Fatal("failed recovery published its mutations")
	}
	backend.saveErr = nil
	backend.conflicts = 1
	if n, err := svc.recoverExpiredExportLeases(testCtx); err != nil || n != 1 {
		t.Fatal("recovery conflict retry did not count committed attempt once")
	}
}

func TestExportLeaseWrongIdentityInvalidMetadataAndTenantRecoveryFailClosed(t *testing.T) {
	svc := testService(t, true)
	seedLeaseExport(t, svc, "bad-lease", "tenant-a")
	claim, won, err := svc.claimPendingExport(testCtx, "bad-lease")
	if err != nil || !won {
		t.Fatal(err)
	}
	original := leaseSnapshot(t, svc).ExportLeases[claim.ID]
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		lease := data.ExportLeases[claim.ID]
		lease.TenantID = "tenant-b"
		data.ExportLeases[claim.ID] = lease
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	svc.Config.Now = func() time.Time { return pinnedNow.Add(ExportLeaseDuration) }
	if n, err := svc.RecoverStuckExports(testCtx, "tenant-b"); err != nil || n != 0 {
		t.Fatal("tenant recovery inspected another tenant job")
	}
	if n, err := svc.RecoverStuckExports(testCtx, "tenant-a"); !errors.Is(err, domain.ErrConflict) || n != 0 {
		t.Fatal("misbound lease was inferred into success")
	}
	if err := svc.renewExportLease(testCtx, claim); !errors.Is(err, errExportLeaseLost) {
		t.Fatal("misbound lease renewed")
	}
	if leaseSnapshot(t, svc).Exports[claim.ID].Status != "running" {
		t.Fatal("invalid metadata returned partial recovery")
	}
	// Restoring the exact captured valid metadata is a test repair, not a
	// production recovery path that guesses a lease from the job body.
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error { data.ExportLeases[claim.ID] = original; return nil }); err != nil {
		t.Fatal(err)
	}
	if n, err := svc.RecoverStuckExports(testCtx, "tenant-a"); err != nil || n != 1 {
		t.Fatal("valid expired original lease did not recover")
	}
}
