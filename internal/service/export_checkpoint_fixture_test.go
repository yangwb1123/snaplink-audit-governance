package service

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Faults live at actual Save/append boundaries. Unknown checkpoint commits
// before reporting failure; terminal failure leaves a durable sealed stage.
type checkpointBackend struct {
	mu                sync.Mutex
	data              *store.Snapshot
	trail             []domain.AdminAction
	saves             int
	failCheckpoint    atomic.Bool
	unknownCheckpoint atomic.Bool
	failTerminal      atomic.Bool
	failTrail         atomic.Bool
	trailEntered      chan context.Context
	trailRelease      chan struct{}
}

func (b *checkpointBackend) Load(ctx context.Context) (*store.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return cloneTestSnapshot(b.data)
}

func (b *checkpointBackend) LoadForUpdate(ctx context.Context) (*store.Snapshot, error) {
	return b.Load(ctx)
}

func (b *checkpointBackend) Save(ctx context.Context, data *store.Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.saves++
	checkpointing, terminal := false, false
	for id := range data.ExportCheckpoints {
		if _, exists := b.data.ExportCheckpoints[id]; !exists {
			checkpointing = true
		}
	}
	for id := range b.data.ExportCheckpoints {
		if job := data.Exports[id]; job.Status == "completed" || job.Status == "failed" {
			terminal = true
		}
	}
	if (checkpointing && b.failCheckpoint.Load()) || (terminal && b.failTerminal.Load()) {
		return errors.New("injected checkpoint control Save failure")
	}
	copyData, err := cloneTestSnapshot(data)
	if err != nil {
		return err
	}
	b.data = copyData
	if checkpointing && b.unknownCheckpoint.Load() {
		return errors.New("checkpoint Save result unknown")
	}
	return nil
}

func (b *checkpointBackend) AppendAdminFact(ctx context.Context, fact domain.AdminAction, _ int) error {
	if fact.TargetType == "export" && fact.Detail == "stage=sealed verification" {
		if b.trailEntered != nil {
			b.trailEntered <- ctx
			select {
			case <-b.trailRelease:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if b.failTrail.Load() {
			return errors.New("injected checkpoint read audit failure")
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.trail = append(b.trail, fact)
	return nil
}

func (b *checkpointBackend) ReadAdminTrail(ctx context.Context) ([]domain.AdminAction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]domain.AdminAction(nil), b.trail...), nil
}

// Once armed, every new Put or source-object Get is a test failure. Recovery
// can read ONLY the original sealed object; no re-selection or resealing hides
// behind a successful terminal status.
type checkpointArchive struct {
	archive.Store
	armed       atomic.Bool
	puts        atomic.Int64
	gets        atomic.Int64
	entered     chan context.Context
	release     chan struct{}
	corrupt     atomic.Bool
	unavailable atomic.Bool
	override    []byte // installed before the executor starts, never logged
}

func (a *checkpointArchive) Put(ctx context.Context, key string, data []byte) error {
	a.puts.Add(1)
	if a.armed.Load() {
		return errors.New("unexpected Put after sealed checkpoint")
	}
	return a.Store.Put(ctx, key, data)
}

func (a *checkpointArchive) Get(ctx context.Context, key string) ([]byte, error) {
	a.gets.Add(1)
	if a.armed.Load() && strings.HasPrefix(key, "events/") {
		return nil, errors.New("unexpected source re-selection after sealed checkpoint")
	}
	if strings.HasPrefix(key, "exports/") {
		if a.entered != nil {
			a.entered <- ctx
			select {
			case <-a.release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if a.unavailable.Load() {
			return nil, errors.New("injected archive unavailable")
		}
		if a.override != nil {
			return append([]byte(nil), a.override...), nil
		}
	}
	raw, err := a.Store.Get(ctx, key)
	if err == nil && a.corrupt.Load() && strings.HasPrefix(key, "exports/") {
		raw = append([]byte(nil), raw...)
		raw[len(raw)-1] ^= 1
	}
	return raw, err
}

func newCheckpointService(t *testing.T) (*Service, *checkpointBackend, *checkpointArchive) {
	t.Helper()
	backend := &checkpointBackend{data: store.NewSnapshot()}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	seedTestDomain(t, svc)
	objects := &checkpointArchive{Store: &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")}}
	svc.Config.Archive = objects
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("checkpoint-source", "checkpoint-operation", pinnedNow.Add(10*time.Second)), domain.StatusArchived); err != nil {
		t.Fatal(err)
	}
	seedLeaseExport(t, svc, "checkpoint-job", "tenant-a")
	return svc, backend, objects
}

func leaveSealedCheckpoint(t *testing.T, svc *Service, backend *checkpointBackend) store.ExportCheckpoint {
	t.Helper()
	backend.failTerminal.Store(true)
	svc.runExport(testCtx, "checkpoint-job")
	data := leaseSnapshot(t, svc)
	cp, exists := data.ExportCheckpoints["checkpoint-job"]
	if !exists || !svc.validExportCheckpoint(data.Exports["checkpoint-job"], cp, svc.Now()) || data.Exports["checkpoint-job"].Status != "running" || data.Exports["checkpoint-job"].ObjectPath != "" {
		t.Fatal("terminal Save fault did not leave an authenticated private sealed checkpoint")
	}
	backend.failTerminal.Store(false)
	return cp
}

func expireCheckpoint(svc *Service) {
	now := svc.Now().Add(ExportLeaseDuration)
	svc.Config.Now = func() time.Time { return now }
}

func checkpointFactCount(t *testing.T, svc *Service, action string) int {
	t.Helper()
	return countAdminActions(t, svc, func(fact domain.AdminAction) bool { return fact.Action == action && fact.TargetID == "checkpoint-job" })
}
