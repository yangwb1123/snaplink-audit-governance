package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// acrossPausedArchive holds the WORM boundary open until the test releases it,
// so a concurrent control mutation is a deterministic lock oracle.
type acrossPausedArchive struct {
	archive.Store
	entered chan struct{}
	release chan struct{}
}

func (a *acrossPausedArchive) Get(ctx context.Context, key string) ([]byte, error) {
	select {
	case a.entered <- struct{}{}:
	default:
	}
	select {
	case <-a.release:
		return a.Store.Get(ctx, key)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type acrossFailingArchive struct{ archive.Store }

func (acrossFailingArchive) Get(context.Context, string) ([]byte, error) {
	return nil, errors.New("worm unavailable")
}

// TestAcrossTenantsReadReleasesStoreLockForArchiveIO pins the remaining legacy
// helper: archived-event authentication must happen outside Store.mu, so a slow
// WORM read cannot block unrelated control writes for every tenant.
func TestAcrossTenantsReadReleasesStoreLockForArchiveIO(t *testing.T) {
	svc := queryMemoryService(t, true)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("across-lock-free", "op-across", at), domain.StatusArchived); err != nil {
		t.Fatal(err)
	}
	paused := &acrossPausedArchive{Store: svc.Config.Archive, entered: make(chan struct{}, 1), release: make(chan struct{})}
	svc.Config.Archive = paused
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done, exited := make(chan error, 1), make(chan struct{})
	release := sync.OnceFunc(func() { close(paused.release) })
	defer func() { release(); cancel(); <-exited }()
	go func() {
		defer close(exited)
		event, err := svc.GetEventAcrossTenants(ctx, "platform-auditor", "across-lock-free")
		if err == nil && event.EventID != "across-lock-free" {
			err = errors.New("cross-tenant read lost original evidence")
		}
		done <- err
	}()
	select {
	case <-paused.entered:
	case <-ctx.Done():
		t.Fatal("archive boundary not entered")
	}
	updated, writerExited := make(chan error, 1), make(chan struct{})
	defer func() { cancel(); <-writerExited }()
	go func() {
		defer close(writerExited)
		updated <- svc.Store.UpdateControl(ctx, func(data *store.Snapshot) error {
			data.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a"}
			return nil
		})
	}()
	select {
	case err := <-updated:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cross-tenant archive IO retained Store.mu")
	}
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(errors.New("cross-tenant read did not resume"))
	}
	if got := len(readFacts(t, svc)); got != 1 {
		t.Fatalf("read facts=%d, want exactly one", got)
	}
}

// TestAcrossTenantsReadFailsClosedWithoutPartialEvent keeps a failed archive
// authentication from leaking a partial answer or a success read fact.
func TestAcrossTenantsReadFailsClosedWithoutPartialEvent(t *testing.T) {
	svc := queryMemoryService(t, true)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("across-fail-closed", "op-across", at), domain.StatusArchived); err != nil {
		t.Fatal(err)
	}
	svc.Config.Archive = acrossFailingArchive{Store: svc.Config.Archive}
	event, err := svc.GetEventAcrossTenants(testCtx, "platform-auditor", "across-fail-closed")
	if err == nil || event.EventID != "" {
		t.Fatalf("failed archive read exposed a partial event: %+v err=%v", event, err)
	}
	if facts := len(readFacts(t, svc)); facts != 0 {
		t.Fatalf("failed read appended %d read facts", facts)
	}
}

// TestAcrossTenantsReadSharesOneCallerDeadline proves the cancelled caller is
// the tighter bound and that the withdrawal is not recorded as a read fact.
func TestAcrossTenantsReadSharesOneCallerDeadline(t *testing.T) {
	svc := queryMemoryService(t, true)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("across-deadline", "op-across", at), domain.StatusArchived); err != nil {
		t.Fatal(err)
	}
	paused := &acrossPausedArchive{Store: svc.Config.Archive, entered: make(chan struct{}, 1), release: make(chan struct{})}
	svc.Config.Archive = paused
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := svc.GetEventAcrossTenants(ctx, "platform-auditor", "across-deadline"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v, want caller deadline", err)
	}
	if facts := len(readFacts(t, svc)); facts != 0 {
		t.Fatalf("withdrawn read appended %d read facts", facts)
	}
}
