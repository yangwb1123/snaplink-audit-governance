package service

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Pure authentication/condition/budget matrices need exact Put/Get bytes, not
// hundreds of unrelated file fsyncs before the boundary being exercised. File
// Get/control exclusion remains explicit in consoleFileEvidenceFixture; HTTP
// and opt-in PostgreSQL cases also keep real FileStore archive objects.
// This fixture makes no crash-durability claim and never skips authentication.

type consoleMemoryArchive struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func newConsoleMemoryArchive() *consoleMemoryArchive {
	return &consoleMemoryArchive{objects: map[string][]byte{}}
}

func (a *consoleMemoryArchive) Ready(ctx context.Context) error { return ctx.Err() }

func (a *consoleMemoryArchive) Put(ctx context.Context, key string, raw []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.objects[key] = append([]byte(nil), raw...)
	return nil
}

func (a *consoleMemoryArchive) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	raw, exists := a.objects[key]
	if !exists {
		return nil, errors.New("Console fixture object unavailable")
	}
	return append([]byte(nil), raw...), nil
}

func consoleEvidenceFixture(t *testing.T, archived bool) (*Service, *checkpointBackend) {
	t.Helper()
	var objects archive.Store
	if archived {
		objects = newConsoleMemoryArchive()
	}
	return consoleEvidenceFixtureOn(t, objects)
}

func consoleFileEvidenceFixture(t *testing.T) (*Service, *checkpointBackend) {
	t.Helper()
	return consoleEvidenceFixtureOn(t, &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")})
}

func consoleEvidenceFixtureOn(t *testing.T, objects archive.Store) (*Service, *checkpointBackend) {
	t.Helper()
	backend := &checkpointBackend{data: store.NewSnapshot()}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	t.Cleanup(func() { _ = svc.Store.Close() })
	seedTestDomain(t, svc)
	svc.Config.Archive = objects
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("console-source", "console-operation", pinnedNow), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	return svc, backend
}

func consoleBudgetFixture(t *testing.T) (*Service, *timelineBudgetBackend, *timelineBudgetArchive) {
	t.Helper()
	backend := &timelineBudgetBackend{checkpointBackend: &checkpointBackend{data: store.NewSnapshot()}}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	t.Cleanup(func() { _ = svc.Store.Close() })
	seedTestDomain(t, svc)
	objects := &timelineBudgetArchive{Store: newConsoleMemoryArchive()}
	svc.Config.Archive = objects
	for i, id := range []string{"console-budget-a", "console-budget-b"} {
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent(id, "console-operation", pinnedNow.Add(time.Duration(i)*time.Second)), domain.StatusArchived); err != nil {
			t.Fatal(err)
		}
	}
	backend.readDeadline, backend.auditDeadline = time.Time{}, time.Time{}
	return svc, backend, objects
}
