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

type leaseHeartbeatBackend struct {
	*scriptedConflictBackend
	failRenew atomic.Bool
	renewed   chan time.Time
}

func (b *leaseHeartbeatBackend) Save(ctx context.Context, data *store.Snapshot) error {
	renewal := false
	for id, lease := range data.ExportLeases {
		previous, existed := b.data.ExportLeases[id]
		if existed && lease.HeartbeatAt.After(previous.HeartbeatAt) {
			renewal = true
			if b.failRenew.Load() {
				return errors.New("injected renewal Save failure")
			}
		}
	}
	if err := b.scriptedConflictBackend.Save(ctx, data); err != nil {
		return err
	}
	if renewal {
		b.renewed <- data.ExportLeases["heartbeat-job"].HeartbeatAt // after actual Save
	}
	return nil
}

type leasePutObservation struct {
	ctx context.Context
	key string
}

type leasePausedArchive struct {
	archive.Store
	entered      chan leasePutObservation
	release      chan struct{}
	ignoreCancel bool
}

func (a *leasePausedArchive) Put(ctx context.Context, key string, data []byte) error {
	a.entered <- leasePutObservation{ctx: ctx, key: key}
	if a.ignoreCancel {
		<-a.release
		return a.Store.Put(context.Background(), key, data) // simulate late external commit
	}
	select {
	case <-a.release:
		return a.Store.Put(ctx, key, data)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestExportHeartbeatRenewsWhilePutBlockedAndCancelsOnSaveFailure(t *testing.T) {
	for _, failRenew := range []bool{false, true} {
		backend := &leaseHeartbeatBackend{scriptedConflictBackend: &scriptedConflictBackend{data: store.NewSnapshot()}, renewed: make(chan time.Time, 1)}
		svc := newServiceOn(t, store.NewWithBackend(backend))
		seedTestDomain(t, svc)
		var clock atomic.Int64
		clock.Store(pinnedNow.UnixNano())
		svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
		ticks, stopped := make(chan time.Time, 1), make(chan struct{})
		svc.exportHeartbeat = func() (<-chan time.Time, func()) { return ticks, func() { close(stopped) } }
		paused := &leasePausedArchive{Store: &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")}, entered: make(chan leasePutObservation, 1), release: make(chan struct{})}
		svc.Config.Archive = paused
		seedLeaseExport(t, svc, "heartbeat-job", "tenant-a")
		done := make(chan struct{})
		executorCtx, cancel := context.WithCancel(testCtx)
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(paused.release) }) }
		t.Cleanup(func() { cancel(); release(); <-done })
		go func() { svc.runExport(executorCtx, "heartbeat-job"); close(done) }()
		var put leasePutObservation
		select {
		case put = <-paused.entered:
		case <-time.After(10 * time.Second):
			t.Fatal("executor did not reach real Put boundary")
		}
		backend.failRenew.Store(failRenew)
		clock.Store(pinnedNow.Add(ExportHeartbeatInterval).UnixNano())
		ticks <- svc.Now()
		if failRenew {
			select {
			case <-put.ctx.Done():
			case <-time.After(10 * time.Second):
				t.Fatal("failed renewal did not cancel Put")
			}
		} else {
			select {
			case committed := <-backend.renewed:
				if !committed.Equal(svc.Now()) {
					t.Fatal("renewal acknowledgement preceded correct Save")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("heartbeat did not persist at Save boundary")
			}
			svc.Config.StuckExportAge = time.Second
			if n, err := svc.RecoverStuckExports(testCtx, "tenant-a"); err != nil || n != 0 {
				t.Fatal("slow healthy Put failed by legacy age")
			}
			release()
		}
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("executor did not release heartbeat")
		}
		<-stopped
		if failRenew {
			if leaseSnapshot(t, svc).Exports["heartbeat-job"].Status != "running" {
				t.Fatal("canceled executor published terminal success/failure")
			}
			clock.Store(pinnedNow.Add(ExportLeaseDuration).UnixNano())
			if n, err := svc.RecoverStuckExports(testCtx, "tenant-a"); err != nil || n != 0 {
				t.Fatal("failed renewal discarded the durable selected stage")
			}
			if len(leaseSnapshot(t, svc).ExportPlans) != 1 {
				t.Fatal("Put began without a durable selected checkpoint")
			}
			backend.failRenew.Store(false)
			replacement, err := New(svc.Store, svc.Config)
			if err != nil {
				t.Fatal(err)
			}
			replacement.Config.Archive = paused.Store
			replacement.runExport(testCtx, "heartbeat-job")
			if got := leaseSnapshot(t, svc); got.Exports["heartbeat-job"].Status != "completed" || len(got.ExportPlans) != 0 || len(got.ExportLeases) != 0 {
				t.Fatal("expired selected-stage continuation did not converge under a new owner")
			}
		} else if got := leaseSnapshot(t, svc); got.Exports["heartbeat-job"].Status != "completed" || len(got.ExportLeases) != 0 {
			t.Fatal("renewed executor did not complete and retire lease")
		}
	}
}

func TestExportExpiredOwnerLatePutCannotPublishOrDeleteWORM(t *testing.T) {
	svc := testService(t, true)
	var clock atomic.Int64
	clock.Store(pinnedNow.UnixNano())
	svc.Config.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	other, err := New(svc.Store, svc.Config)
	if err != nil {
		t.Fatal(err)
	}
	ticks := make(chan time.Time, 1)
	svc.exportHeartbeat = func() (<-chan time.Time, func()) { return ticks, func() {} }
	paused := &leasePausedArchive{Store: svc.Config.Archive, entered: make(chan leasePutObservation, 1), release: make(chan struct{}), ignoreCancel: true}
	svc.Config.Archive = paused
	seedLeaseExport(t, svc, "stale-job", "tenant-a")
	done := make(chan struct{})
	executorCtx, cancel := context.WithCancel(testCtx)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(paused.release) }) }
	t.Cleanup(func() { cancel(); release(); <-done })
	go func() { svc.runExport(executorCtx, "stale-job"); close(done) }()
	var put leasePutObservation
	select {
	case put = <-paused.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("stale executor did not reach Put")
	}
	owner := leaseSnapshot(t, svc).ExportLeases["stale-job"].Owner
	if strings.Contains(put.key, owner) {
		t.Fatal("public object identity exposed owner fence")
	}
	clock.Store(pinnedNow.Add(ExportLeaseDuration).UnixNano())
	if n, err := other.recoverExpiredExportLeases(testCtx); err != nil || n != 0 {
		t.Fatal("other replica discarded durable selected stage")
	}
	replacement, won, err := other.claimExport(testCtx, "stale-job", true)
	if err != nil || !won || replacement.Owner == owner || replacement.Plan.Version != 1 {
		t.Fatal("other replica did not fence expired selected-stage owner")
	}
	ticks <- svc.Now()
	select {
	case <-put.ctx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("stale executor was not canceled by real lease check")
	}
	release()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("late Put did not return")
	}
	job := leaseSnapshot(t, svc).Exports["stale-job"]
	if got := leaseSnapshot(t, svc); job.Status != "running" || job.ObjectPath != "" || job.Digest != "" || job.EventCount != 0 ||
		got.ExportLeases[job.ID].Owner != replacement.Owner || got.ExportPlans[job.ID].Signature != replacement.Plan.Signature || len(got.ExportCheckpoints) != 0 {
		t.Fatal("late Put overwrote the replacement owner or advanced its selected stage")
	}
	if sealed, err := paused.Store.Get(testCtx, put.key); err != nil || len(sealed) == 0 {
		t.Fatal("lease failure pretended external Put rollback or deleted WORM")
	}
	if _, err := svc.ReadExportObject(testCtx, "tenant-a", "stale-job"); !errors.Is(err, domain.ErrConflict) {
		t.Fatal("unreferenced late object became a downloadable job")
	}
	clock.Store(pinnedNow.Add(2 * ExportLeaseDuration).UnixNano())
	other.runExport(testCtx, "stale-job")
	job = leaseSnapshot(t, svc).Exports["stale-job"]
	if job.Status != "completed" || job.ObjectPath == put.key || len(leaseSnapshot(t, svc).ExportPlans) != 0 {
		t.Fatal("new owner did not generate a distinct fenced artifact from the known plan")
	}
	if _, err := paused.Store.Get(testCtx, put.key); err != nil {
		t.Fatal("successful continuation deleted the old unknown-result WORM object")
	}
}
