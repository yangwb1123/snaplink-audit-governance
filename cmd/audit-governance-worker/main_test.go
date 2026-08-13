package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/runtimeconfig"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

// scriptedConflictBackend is the worker-test counterpart of the service
// package's double: LoadForUpdate returns a deep copy (a failed Save must
// not leak closure mutations into the committed snapshot), Save fails with
// ErrSnapshotConflict while `conflicts` remain without committing, then
// commits. Save calls are counted so tests can prove whether the counter
// write happened at all.
type scriptedConflictBackend struct {
	data      *store.Snapshot
	conflicts int // remaining conflicts before Save commits
	saves     int
}

func (b *scriptedConflictBackend) Load() (*store.Snapshot, error) { return b.data, nil }

func (b *scriptedConflictBackend) LoadForUpdate() (*store.Snapshot, error) {
	encoded, err := json.Marshal(b.data)
	if err != nil {
		return nil, err
	}
	copyData := store.NewSnapshot()
	if err := json.Unmarshal(encoded, copyData); err != nil {
		return nil, err
	}
	return copyData, nil
}

func (b *scriptedConflictBackend) Save(data *store.Snapshot) error {
	b.saves++
	if b.conflicts > 0 {
		b.conflicts--
		return store.ErrSnapshotConflict
	}
	b.data = data
	return nil
}

// workerService builds a Service over the scripted backend; the conflict
// counter needs no tenant domain (tenant IDs are plain map keys).
func workerService(t *testing.T, backend *scriptedConflictBackend) *service.Service {
	t.Helper()
	svc, err := service.New(store.NewWithBackend(backend), service.Config{SigningSecret: "test-secret", AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func TestHandleArchiveErrorPersistsExhaustedConflict(t *testing.T) {
	// An exhausted ErrSnapshotConflict pass must increment the persisted
	// counter and log it as conflict_failures=1.
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := workerService(t, backend)
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	handleArchiveError(logger, svc, "tenant-a", 0, store.ErrSnapshotConflict)

	if backend.saves != 1 {
		t.Fatalf("Save calls=%d, want 1 (one counter-increment window)", backend.saves)
	}
	if n, err := svc.ArchivePassConflictFailures("tenant-a"); err != nil || n != 1 {
		t.Fatalf("counter=%d, %v; want 1", n, err)
	}
	out := buf.String()
	if !strings.Contains(out, "archive_error=state snapshot changed concurrently") || !strings.Contains(out, "conflict_failures=1") {
		t.Fatalf("log line missing exhaustion signal, got: %q", out)
	}
}

func TestHandleArchiveErrorSkipsNonConflictErrors(t *testing.T) {
	// A non-conflict pass error must not record anything: the counter stays
	// untouched and no counter write window is opened.
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := workerService(t, backend)
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	handleArchiveError(logger, svc, "tenant-a", 3, domain.ErrInvalid)

	if backend.saves != 0 {
		t.Fatalf("Save calls=%d, want 0 (no counter write for a non-conflict error)", backend.saves)
	}
	if n, err := svc.ArchivePassConflictFailures("tenant-a"); err != nil || n != 0 {
		t.Fatalf("counter=%d, %v; want 0 untouched", n, err)
	}
	out := buf.String()
	if !strings.Contains(out, "archive_error=invalid") || !strings.Contains(out, "archived=3") || !strings.Contains(out, "conflict_failures=0") {
		t.Fatalf("log line malformed, got: %q", out)
	}
	if strings.Contains(out, "archive_conflict_record_error") {
		t.Fatalf("record error logged for a non-conflict error, got: %q", out)
	}
}

func TestHandleArchiveErrorCounterWriteFailureDoesNotMaskPassError(t *testing.T) {
	// When even the counter increment exhausts under sustained load, the
	// record error is logged separately, the log still reports the original
	// pass error, and the counter reads back at its previous value.
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := workerService(t, backend)
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	backend.conflicts = 100 // the increment write itself can never commit
	handleArchiveError(logger, svc, "tenant-a", 0, store.ErrSnapshotConflict)

	if n, err := svc.ArchivePassConflictFailures("tenant-a"); err != nil || n != 0 {
		t.Fatalf("counter=%d, %v; want 0 (increment failed atomically)", n, err)
	}
	out := buf.String()
	if !strings.Contains(out, "archive_conflict_record_error") {
		t.Fatalf("counter-write failure not logged, got: %q", out)
	}
	if !strings.Contains(out, "archive_error=state snapshot changed concurrently") || !strings.Contains(out, "conflict_failures=0") {
		t.Fatalf("original pass error or counter missing from log, got: %q", out)
	}
}

// scriptedS3Client is the worker-package counterpart of the archive package's
// fakeS3Client: it implements the exported archive.S3Client with scripted
// bucket configuration so runCheckConfig and probeArchiveReady can be driven
// without a real endpoint (REQ-4). bucketExistsHook lets a test intercept the
// probe — T7 blocks there until the probe context is cancelled.
type scriptedS3Client struct {
	bucket           bool
	lockErr          error
	lockStatus       string
	versioning       minio.BucketVersioningConfiguration
	bucketExistsHook func(ctx context.Context) (bool, error)
}

func (c *scriptedS3Client) StatObject(context.Context, string, string, minio.StatObjectOptions) (minio.ObjectInfo, error) {
	return minio.ObjectInfo{}, errors.New("scripted client: StatObject not used")
}

func (c *scriptedS3Client) PutObject(context.Context, string, string, io.Reader, int64, minio.PutObjectOptions) (minio.UploadInfo, error) {
	return minio.UploadInfo{}, errors.New("scripted client: PutObject not used")
}

func (c *scriptedS3Client) GetObject(context.Context, string, string, minio.GetObjectOptions) (io.ReadCloser, error) {
	return nil, errors.New("scripted client: GetObject not used")
}

func (c *scriptedS3Client) BucketExists(ctx context.Context, _ string) (bool, error) {
	if c.bucketExistsHook != nil {
		return c.bucketExistsHook(ctx)
	}
	return c.bucket, nil
}

func (c *scriptedS3Client) GetObjectLockConfig(context.Context, string) (string, *minio.RetentionMode, *uint, *minio.ValidityUnit, error) {
	if c.lockErr != nil {
		return "", nil, nil, nil, c.lockErr
	}
	return c.lockStatus, nil, nil, nil, nil
}

func (c *scriptedS3Client) GetBucketVersioning(context.Context, string) (minio.BucketVersioningConfiguration, error) {
	return c.versioning, nil
}

// lockedScriptedS3 returns a scripted client that Ready passes: bucket exists,
// Object Lock "Enabled", versioning "Enabled" (the lockedFakeS3 equivalent).
func lockedScriptedS3() *scriptedS3Client {
	return &scriptedS3Client{
		bucket:     true,
		lockStatus: "Enabled",
		versioning: minio.BucketVersioningConfiguration{Status: "Enabled"},
	}
}

// flakyArchive is the recordingArchive pattern with a switchable Ready
// result: Ready fails while readyErr is set, and every Put key is recorded so
// tests can assert whether the archiving step ran at all (AC-3 observables).
type flakyArchive struct {
	readyErr error
	puts     []string
}

func (f *flakyArchive) Put(_ context.Context, key string, _ []byte) error {
	f.puts = append(f.puts, key)
	return nil
}

func (f *flakyArchive) Get(context.Context, string) ([]byte, error) { return nil, nil }

func (f *flakyArchive) Ready(context.Context) error { return f.readyErr }

// swapArchiveStore replaces the newArchiveStore seam for one test and
// restores it on cleanup. The seam is mutable package state: tests that swap
// it must never run in parallel (R-5).
func swapArchiveStore(t *testing.T, build func(runtimeconfig.SigningArchive) (archive.Store, error)) {
	t.Helper()
	original := newArchiveStore
	newArchiveStore = build
	t.Cleanup(func() { newArchiveStore = original })
}

// TestRunCheckConfigProbesArchiveDestination is T1 (AC-1, REQ-2): the
// destination is probed between store construction and the check_config=ok
// print; check_config=ok appears only when Ready passes. Cases (a)-(d) are
// the S3 misconfigurations already pinned at the archive level by
// TestS3StoreReadyVerifiesObjectLockAndVersioning — T1 proves the worker
// invokes that probe; (f)/(g) cover the FileStore variant at runCheckConfig
// level (QA-5).
func TestRunCheckConfigProbesArchiveDestination(t *testing.T) {
	s3Store := func(mutate func(*scriptedS3Client)) archive.Store {
		client := lockedScriptedS3()
		mutate(client)
		return archive.NewS3StoreWithClient(client, "worm-audit")
	}
	occupiedPath := func(t *testing.T) string {
		t.Helper()
		occupied := filepath.Join(t.TempDir(), "occupied")
		if err := os.WriteFile(occupied, []byte("occupied"), 0o640); err != nil {
			t.Fatal(err)
		}
		return occupied
	}
	cases := []struct {
		name     string
		store    func(t *testing.T) archive.Store
		wantExit int
		wantLog  []string
		notLog   []string
	}{
		{"missing bucket", func(t *testing.T) archive.Store { return s3Store(func(c *scriptedS3Client) { c.bucket = false }) },
			1, []string{"archive_ready=failed", "does not exist"}, []string{"check_config=ok"}},
		{"no lock config", func(t *testing.T) archive.Store {
			return s3Store(func(c *scriptedS3Client) { c.lockErr = errors.New("NoSuchObjectLockConfiguration") })
		},
			1, []string{"archive_ready=failed", "no object lock configuration"}, []string{"check_config=ok"}},
		{"lock disabled", func(t *testing.T) archive.Store { return s3Store(func(c *scriptedS3Client) { c.lockStatus = "" }) },
			1, []string{"archive_ready=failed", "object lock is not enabled"}, []string{"check_config=ok"}},
		{"versioning disabled", func(t *testing.T) archive.Store {
			return s3Store(func(c *scriptedS3Client) { c.versioning = minio.BucketVersioningConfiguration{} })
		},
			1, []string{"archive_ready=failed", "versioning"}, []string{"check_config=ok"}},
		{"healthy s3", func(t *testing.T) archive.Store { return s3Store(func(*scriptedS3Client) {}) },
			0, []string{"archive_ready=ok", "check_config=ok", "archive=s3"}, nil},
		{"healthy file store", func(t *testing.T) archive.Store { return &archive.FileStore{Dir: t.TempDir()} },
			0, []string{"archive_ready=ok", "check_config=ok", "archive=file"}, nil},
		{"occupied file store", func(t *testing.T) archive.Store { return &archive.FileStore{Dir: occupiedPath(t)} },
			1, []string{"archive_ready=failed"}, []string{"check_config=ok"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tcStore := tc.store(t)
			swapArchiveStore(t, func(runtimeconfig.SigningArchive) (archive.Store, error) { return tcStore, nil })
			var buf bytes.Buffer
			logger := log.New(&buf, "", 0)
			exit := runCheckConfig(logger, service.Config{SigningSecret: "test-secret", EncryptionKey: "test-key", AllowDevSecrets: true}, runtimeconfig.SigningArchive{})
			if exit != tc.wantExit {
				t.Fatalf("exit=%d, want %d; log: %q", exit, tc.wantExit, buf.String())
			}
			out := buf.String()
			for _, want := range tc.wantLog {
				if !strings.Contains(out, want) {
					t.Fatalf("log must contain %q, got: %q", want, out)
				}
			}
			for _, not := range tc.notLog {
				if strings.Contains(out, not) {
					t.Fatalf("log must not contain %q, got: %q", not, out)
				}
			}
		})
	}
}

// TestProbeArchiveReady is T2 (AC-2, REQ-1): the extracted startup-probe
// helper fails on a misconfigured destination and passes on a healthy one;
// unconfigured stores are skipped (OQ-1). The main wiring is thin
// logger.Fatalf glue; the end-to-end exit-code observable is covered by
// TestStartupProbeSubprocessFatal.
func TestProbeArchiveReady(t *testing.T) {
	t.Run("s3 misconfigured fails", func(t *testing.T) {
		client := lockedScriptedS3()
		client.lockStatus = "" // Object Lock disabled
		s3 := archive.NewS3StoreWithClient(client, "worm-audit")
		var buf bytes.Buffer
		if err := probeArchiveReady(s3, log.New(&buf, "", 0)); err == nil {
			t.Fatal("probe must fail on a misconfigured S3 store")
		}
		if !strings.Contains(buf.String(), "archive_ready=failed store=*archive.S3Store") {
			t.Fatalf("log must identify the destination type, got: %q", buf.String())
		}
	})
	t.Run("s3 healthy passes", func(t *testing.T) {
		s3 := archive.NewS3StoreWithClient(lockedScriptedS3(), "worm-audit")
		var buf bytes.Buffer
		if err := probeArchiveReady(s3, log.New(&buf, "", 0)); err != nil {
			t.Fatalf("probe must pass on a locked bucket: %v", err)
		}
		if !strings.Contains(buf.String(), "archive_ready=ok") {
			t.Fatalf("log must report archive_ready=ok, got: %q", buf.String())
		}
	})
	t.Run("occupied file store fails", func(t *testing.T) {
		occupied := filepath.Join(t.TempDir(), "occupied")
		if err := os.WriteFile(occupied, []byte("occupied"), 0o640); err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		err := probeArchiveReady(&archive.FileStore{Dir: occupied}, log.New(&buf, "", 0))
		if err == nil {
			t.Fatal("probe must fail when the archive dir is occupied by a regular file")
		}
		if !strings.Contains(buf.String(), "archive_ready=failed") {
			t.Fatalf("log must report archive_ready=failed, got: %q", buf.String())
		}
	})
	t.Run("writable file store passes", func(t *testing.T) {
		dir := t.TempDir()
		var buf bytes.Buffer
		if err := probeArchiveReady(&archive.FileStore{Dir: dir}, log.New(&buf, "", 0)); err != nil {
			t.Fatalf("probe must pass on a writable dir: %v", err)
		}
		if !strings.Contains(buf.String(), "archive_ready=ok") {
			t.Fatalf("log must report archive_ready=ok, got: %q", buf.String())
		}
	})
	t.Run("unconfigured store skipped", func(t *testing.T) {
		var buf bytes.Buffer
		logger := log.New(&buf, "", 0)
		if err := probeArchiveReady(&archive.FileStore{Dir: ""}, logger); err != nil {
			t.Fatalf("empty-dir FileStore must be skipped, got error: %v", err)
		}
		if err := probeArchiveReady(nil, logger); err != nil {
			t.Fatalf("nil store must be skipped, got error: %v", err)
		}
		if strings.Count(buf.String(), "archive_ready=skipped") != 2 {
			t.Fatalf("log must report archive_ready=skipped twice, got: %q", buf.String())
		}
	})
}

// TestProbeArchiveReadyBoundedByTimeout is T7 (REQ-7): a hung S3 endpoint
// must not block the probe beyond archiveReadyTimeout. The hook blocks until
// the probe context is cancelled, so the test is deterministic: the probe
// always returns the context deadline error after the 5 s constant.
func TestProbeArchiveReadyBoundedByTimeout(t *testing.T) {
	client := lockedScriptedS3()
	client.bucketExistsHook = func(ctx context.Context) (bool, error) {
		<-ctx.Done()
		return false, ctx.Err()
	}
	s3 := archive.NewS3StoreWithClient(client, "worm-audit")
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	started := time.Now()
	err := probeArchiveReady(s3, logger)
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("probe must fail when the endpoint hangs")
	}
	if elapsed < 4*time.Second || elapsed > 8*time.Second {
		t.Fatalf("probe elapsed=%v, want bounded by the 5s constant (4-8s)", elapsed)
	}
	if !strings.Contains(buf.String(), "archive_ready=failed") {
		t.Fatalf("log must surface archive_ready=failed, got: %q", buf.String())
	}
}

// TestRunEvaluatePassRecoversStuckExports is the AC-1 pass-level variant:
// a job stuck in "running" past the threshold is failed by the pass with a
// clear error, the per-tenant log line reports the recovered count, and the
// export.recovered fact is appended atomically.
func TestRunEvaluatePassRecoversStuckExports(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := workerService(t, backend)
	base := time.Unix(1_700_000_000, 0).UTC()
	svc.Config.Now = func() time.Time { return base }
	svc.Config.StuckExportAge = 24 * time.Hour
	backend.data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}
	backend.data.Exports["export-1"] = domain.ExportJob{
		ID: "export-1", TenantID: "tenant-a", RequestedBy: "compliance-1",
		Query:     domain.Query{From: base.Add(-48 * time.Hour), To: base.Add(24 * time.Hour), PageSize: 100},
		Status:    "running",
		CreatedAt: base.Add(-25 * time.Hour),
	}
	savesBefore := backend.saves
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	runEvaluatePass(logger, svc)

	if !strings.Contains(buf.String(), "tenant=tenant-a stuck_exports_recovered=1") {
		t.Fatalf("log must report stuck_exports_recovered=1, got: %q", buf.String())
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	job := snap.Exports["export-1"]
	if job.Status != "failed" {
		t.Fatalf("status=%s, want failed", job.Status)
	}
	if !strings.Contains(job.Error, "stuck in running since") || !strings.Contains(job.Error, "re-request the export") {
		t.Fatalf("error must be clear, got: %q", job.Error)
	}
	if job.FinishedAt == nil || job.FinishedAt.After(base) {
		t.Fatalf("finished_at=%v, want set and <= now", job.FinishedAt)
	}
	facts := 0
	for _, action := range snap.AdminActions {
		if action.Action == domain.AdminActionExportRecovered {
			facts++
			if action.TenantID != "tenant-a" || action.TargetID != "export-1" || action.Actor != "governance-worker" {
				t.Fatalf("fact fields wrong: %+v", action)
			}
		}
	}
	if facts != 1 {
		t.Fatalf("export.recovered facts=%d, want exactly 1", facts)
	}
	if backend.saves != savesBefore+1 {
		t.Fatalf("saves=%d, want %d (one atomic recovery window)", backend.saves, savesBefore+1)
	}
}

// TestRunEvaluatePassRecoveryProbeIndependent proves the recovery step needs
// no archive I/O: with the readiness probe failing, the stuck job is still
// failed (stuck_exports_recovered=1), the archiving step is skipped, and zero
// archive Puts occur.
func TestRunEvaluatePassRecoveryProbeIndependent(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := workerService(t, backend)
	base := time.Unix(1_700_000_000, 0).UTC()
	svc.Config.Now = func() time.Time { return base }
	svc.Config.StuckExportAge = 24 * time.Hour
	backend.data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}
	backend.data.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a", RetentionClass: "default", ArchiveDays: 365}
	backend.data.Exports["export-1"] = domain.ExportJob{
		ID: "export-1", TenantID: "tenant-a", RequestedBy: "compliance-1",
		Query:     domain.Query{From: base.Add(-48 * time.Hour), To: base.Add(24 * time.Hour), PageSize: 100},
		Status:    "running",
		CreatedAt: base.Add(-25 * time.Hour),
	}
	archiveStub := &flakyArchive{readyErr: errors.New("archive bucket not ready")}
	svc.Config.Archive = archiveStub
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	runEvaluatePass(logger, svc)

	out := buf.String()
	if !strings.Contains(out, "tenant=tenant-a stuck_exports_recovered=1") {
		t.Fatalf("log must report stuck_exports_recovered=1 despite the failed probe, got: %q", out)
	}
	if !strings.Contains(out, "archive_ready=failed") || !strings.Contains(out, "tenant=tenant-a archive_skipped=ready_probe_failed") {
		t.Fatalf("log must surface the probe failure and per-tenant skip, got: %q", out)
	}
	if len(archiveStub.puts) != 0 {
		t.Fatalf("puts=%v, want zero archive writes (recovery does no archive I/O)", archiveStub.puts)
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snap.Exports["export-1"].Status != "failed" {
		t.Fatalf("status=%s, want failed", snap.Exports["export-1"].Status)
	}
}

// TestRunEvaluatePassRecoveryNoopForCompleted is the AC-2 pass-level variant:
// a pass over a completed job only must recover nothing, log
// stuck_exports_recovered=0 and rewrite the snapshot zero times.
func TestRunEvaluatePassRecoveryNoopForCompleted(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := workerService(t, backend)
	base := time.Unix(1_700_000_000, 0).UTC()
	svc.Config.Now = func() time.Time { return base }
	svc.Config.StuckExportAge = 24 * time.Hour
	backend.data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}
	backend.data.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a", RetentionClass: "default", ArchiveDays: 365}
	past := base.Add(-48 * time.Hour)
	backend.data.Exports["export-done"] = domain.ExportJob{
		ID: "export-done", TenantID: "tenant-a", RequestedBy: "compliance-1",
		Query:      domain.Query{From: base.Add(-48 * time.Hour), To: base.Add(24 * time.Hour), PageSize: 100},
		Status:     "completed",
		CreatedAt:  base.Add(-25 * time.Hour),
		FinishedAt: &past,
		ObjectPath: "exports/export-done.jsonl",
		Digest:     "deadbeef",
		EventCount: 7,
	}
	savesBefore := backend.saves
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	runEvaluatePass(logger, svc)

	if !strings.Contains(buf.String(), "tenant=tenant-a stuck_exports_recovered=0") {
		t.Fatalf("log must report stuck_exports_recovered=0, got: %q", buf.String())
	}
	if backend.saves != savesBefore {
		t.Fatalf("saves=%d, want %d (idle recovery must not rewrite the snapshot)", backend.saves, savesBefore)
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	done := snap.Exports["export-done"]
	if done.Status != "completed" || done.ObjectPath != "exports/export-done.jsonl" || done.Digest != "deadbeef" || done.EventCount != 7 {
		t.Fatalf("completed job mutated: %+v", done)
	}
}

// seedPendingReceipts seeds tenant-a with n events whose receipts sit at
// StatusIndexed (direct snapshot seeding, mirroring the service package's
// seedPendingEvents), plus a retention policy whose class no seeded event
// carries, so EvaluateRetention's eligible count is pinned to zero
// regardless of wall-clock time (QA-6).
func seedPendingReceipts(t *testing.T, backend *scriptedConflictBackend, n int) {
	t.Helper()
	backend.data.Tenants["tenant-a"] = domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}
	backend.data.Policies["tenant-a"] = domain.RetentionPolicy{TenantID: "tenant-a", RetentionClass: "default", ArchiveDays: 365}
	base := time.Unix(1_700_000_000, 0).UTC()
	for i := 1; i <= n; i++ {
		key := store.EventKey("tenant-a", fmt.Sprintf("evt-%d", i))
		backend.data.Events[key] = domain.Event{
			EventID:        fmt.Sprintf("evt-%d", i),
			TenantID:       "tenant-a",
			StreamID:       "s",
			EventType:      "op-pending",
			SchemaID:       "audit.event",
			SchemaVersion:  1,
			Sequence:       int64(i),
			OccurredAt:     base.Add(time.Duration(i) * time.Second),
			RetentionClass: "other", // pinned: never matches the policy class
		}
		backend.data.Receipts[key] = domain.EventReceipt{EventID: fmt.Sprintf("evt-%d", i), TenantID: "tenant-a", Status: domain.StatusIndexed}
	}
}

// TestRunEvaluatePassGatesArchivingOnProbe is T3 (AC-3, REQ-3): when the
// probe fails, the pass must not mark any receipt StatusArchived, must not
// call Put, must not open any Store.Update window and must not reset the
// tenant's conflict counter; the other per-tenant steps proceed. When the
// probe heals, the next pass archives everything exactly as today — one
// atomic batch window and the counter reset ride the same Save.
func TestRunEvaluatePassGatesArchivingOnProbe(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := workerService(t, backend)
	seedPendingReceipts(t, backend, 3)
	archiveStub := &flakyArchive{readyErr: errors.New("archive bucket not ready")}
	svc.Config.Archive = archiveStub
	// Pre-set the conflict counter through the real service path (one
	// deliberate Save before the pass; the pass must not open any more).
	if err := svc.RecordArchivePassConflict("tenant-a"); err != nil {
		t.Fatal(err)
	}
	savesBefore := backend.saves
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	runEvaluatePass(logger, svc)

	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		receipt := snap.Receipts[store.EventKey("tenant-a", fmt.Sprintf("evt-%d", i))]
		if receipt.Status != domain.StatusIndexed {
			t.Fatalf("event %d status=%s, want untouched StatusIndexed on a failed probe", i, receipt.Status)
		}
	}
	if len(archiveStub.puts) != 0 {
		t.Fatalf("puts=%v, want zero Put calls on a failed probe", archiveStub.puts)
	}
	if backend.saves != savesBefore {
		t.Fatalf("saves=%d, want %d (no Store.Update window on a failed probe)", backend.saves, savesBefore)
	}
	if n, err := svc.ArchivePassConflictFailures("tenant-a"); err != nil || n != 1 {
		t.Fatalf("conflict counter=%d, %v; want 1 (not reset)", n, err)
	}
	if !strings.Contains(buf.String(), "tenant=tenant-a archive_skipped=ready_probe_failed") {
		t.Fatalf("log must surface the per-tenant skip, got: %q", buf.String())
	}

	// Heal and re-run: the pass behaves exactly as before the probe.
	archiveStub.readyErr = nil
	runEvaluatePass(logger, svc)

	snap, err = svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		receipt := snap.Receipts[store.EventKey("tenant-a", fmt.Sprintf("evt-%d", i))]
		if receipt.Status != domain.StatusArchived {
			t.Fatalf("event %d status=%s, want archived after healing", i, receipt.Status)
		}
	}
	if len(archiveStub.puts) != 3 {
		t.Fatalf("puts=%v, want 3 Put calls on the healed pass", archiveStub.puts)
	}
	if backend.saves != savesBefore+1 {
		t.Fatalf("saves=%d, want %d (one atomic batch window)", backend.saves, savesBefore+1)
	}
	if n, err := svc.ArchivePassConflictFailures("tenant-a"); err != nil || n != 0 {
		t.Fatalf("conflict counter=%d, %v; want 0 after the healed pass", n, err)
	}
}

// TestRunEvaluatePassSurfacesProbeFailurePerPass is T4 (AC-3): the failing
// pass surfaces the probe failure once per pass (identifying the destination
// and the error), skips archiving per tenant, and the other steps still emit
// their per-tenant pass line; the healed pass reports archive_ready=ok and no
// skip line. RetentionClass seeding pins eligible=0 (QA-6).
func TestRunEvaluatePassSurfacesProbeFailurePerPass(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := workerService(t, backend)
	seedPendingReceipts(t, backend, 1)
	archiveStub := &flakyArchive{readyErr: errors.New("archive bucket not ready")}
	svc.Config.Archive = archiveStub
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	runEvaluatePass(logger, svc)

	out := buf.String()
	for _, want := range []string{
		"archive_ready=failed store=*main.flakyArchive error=archive bucket not ready",
		"tenant=tenant-a archive_skipped=ready_probe_failed",
		"tenant=tenant-a eligible=0 protected=0 action=archive_only_immutable_ledger",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("log must contain %q, got: %q", want, out)
		}
	}

	// Healed pass: archive_ready=ok, no skip line, receipts archived.
	archiveStub.readyErr = nil
	buf.Reset()
	runEvaluatePass(logger, svc)
	out = buf.String()
	if strings.Contains(out, "archive_skipped=ready_probe_failed") {
		t.Fatalf("healed pass must not log skip lines, got: %q", out)
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if receipt := snap.Receipts[store.EventKey("tenant-a", "evt-1")]; receipt.Status != domain.StatusArchived {
		t.Fatalf("receipt status=%s, want archived after healing", receipt.Status)
	}
}

var (
	workerBinOnce sync.Once
	workerBinPath string
	workerBinErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if workerBinPath != "" {
		_ = os.RemoveAll(filepath.Dir(workerBinPath))
	}
	os.Exit(code)
}

// buildWorkerBinary compiles the real worker binary once per test process
// into a private temp directory (never the repo's bin/) so subprocess tests
// can exercise the -check-config and startup exit-code plumbing end-to-end.
func buildWorkerBinary() (string, error) {
	workerBinOnce.Do(func() {
		dir, err := os.MkdirTemp("", "audit-governance-worker-test-")
		if err != nil {
			workerBinErr = err
			return
		}
		workerBinPath = filepath.Join(dir, "audit-governance-worker")
		cmd := exec.Command("go", "build", "-o", workerBinPath, ".")
		if out, err := cmd.CombinedOutput(); err != nil {
			workerBinErr = fmt.Errorf("go build worker binary: %w: %s", err, out)
		}
	})
	return workerBinPath, workerBinErr
}

// TestCheckConfigSubprocessExitCodes is T1e (AC-1): the real binary's
// -check-config exits non-zero and prints archive_ready=failed (never
// check_config=ok) when the archive dir is occupied by a regular file, and
// exits 0 with archive_ready=ok + check_config=ok archive=file for a
// writable dir. The binary is built once per test process (buildWorkerBinary)
// so the flag plumbing is covered even though the unit-level runCheckConfig
// table is the primary coverage.
func TestCheckConfigSubprocessExitCodes(t *testing.T) {
	binary, err := buildWorkerBinary()
	if err != nil {
		t.Skipf("worker binary unavailable: %v", err)
	}
	t.Run("occupied archive dir fails", func(t *testing.T) {
		occupied := filepath.Join(t.TempDir(), "occupied")
		if err := os.WriteFile(occupied, []byte("occupied"), 0o640); err != nil {
			t.Fatal(err)
		}
		out, err := exec.Command(binary, "-check-config", "-archive", occupied, "-allow-dev-secrets").CombinedOutput()
		if err == nil {
			t.Fatalf("-check-config with an occupied archive dir must exit non-zero, output: %s", out)
		}
		if !strings.Contains(string(out), "archive_ready=failed") {
			t.Fatalf("output must surface archive_ready=failed, got: %s", out)
		}
		if strings.Contains(string(out), "check_config=ok") {
			t.Fatalf("output must not print check_config=ok, got: %s", out)
		}
	})
	t.Run("healthy file store passes", func(t *testing.T) {
		dir := t.TempDir()
		out, err := exec.Command(binary, "-check-config", "-archive", dir, "-allow-dev-secrets").CombinedOutput()
		if err != nil {
			t.Fatalf("-check-config with a writable archive dir must exit 0: %v\n%s", err, out)
		}
		for _, want := range []string{"archive_ready=ok", "check_config=ok", "archive=file"} {
			if !strings.Contains(string(out), want) {
				t.Fatalf("output must contain %q, got: %s", want, out)
			}
		}
	})
}

// TestStartupProbeSubprocessFatal is T2e (AC-2, REQ-1): the real binary
// refuses to start (-once mode, which also runs the startup probe) with a
// non-zero exit and an archive_ready=failed line when the archive dir is
// occupied. This covers the main wiring's logger.Fatalf glue end-to-end.
func TestStartupProbeSubprocessFatal(t *testing.T) {
	binary, err := buildWorkerBinary()
	if err != nil {
		t.Skipf("worker binary unavailable: %v", err)
	}
	dir := t.TempDir()
	occupied := filepath.Join(dir, "occupied")
	if err := os.WriteFile(occupied, []byte("occupied"), 0o640); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(binary, "-once", "-archive", occupied, "-state", filepath.Join(dir, "state.json"), "-allow-dev-secrets").CombinedOutput()
	if err == nil {
		t.Fatalf("worker must refuse to start with an occupied archive dir, output: %s", out)
	}
	if !strings.Contains(string(out), "archive_ready=failed") {
		t.Fatalf("startup output must surface archive_ready=failed, got: %s", out)
	}
}

// TestCheckConfigTransportLine is AC-4 (REQ-TLS-6): the worker's ok line
// carries per-leg transport labels for both legs, byte-identical in shape to
// the API's line (same fields, same order, same format string), so a mixed
// S3+Vault configuration never hides one leg's transport (F3). S3 rows swap
// the newArchiveStore seam so the destination probe passes network-free and
// archive=s3 matches the API line.
func TestCheckConfigTransportLine(t *testing.T) {
	cases := []struct {
		name      string
		external  runtimeconfig.SigningArchive
		wantS3    string
		wantVault string
		swapStore bool
	}{
		{"no external legs", runtimeconfig.SigningArchive{}, "local", "local", false},
		{"s3 tls", runtimeconfig.SigningArchive{S3Endpoint: "s3.example.com:9000", S3Bucket: "worm", S3AccessKey: "k", S3SecretKey: "s", S3UseSSL: true}, "tls", "local", true},
		{"s3 http", runtimeconfig.SigningArchive{S3Endpoint: "s3.example.com:9000", S3Bucket: "worm", S3AccessKey: "k", S3SecretKey: "s", S3UseSSL: false}, "http", "local", true},
		{"vault tls", runtimeconfig.SigningArchive{VaultAddr: "https://vault.example.com:8200", VaultToken: "t", VaultTransitKey: "audit-checkpoints"}, "local", "tls", false},
		{"mixed s3 tls + vault tls", runtimeconfig.SigningArchive{
			S3Endpoint: "https://s3.example.com", S3Bucket: "worm", S3AccessKey: "k", S3SecretKey: "s", S3UseSSL: true,
			VaultAddr: "https://vault.example.com:8200", VaultToken: "t", VaultTransitKey: "audit-checkpoints",
		}, "tls", "tls", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.swapStore {
				swapArchiveStore(t, func(runtimeconfig.SigningArchive) (archive.Store, error) {
					return archive.NewS3StoreWithClient(lockedScriptedS3(), "worm"), nil
				})
			}
			var buf bytes.Buffer
			logger := log.New(&buf, "", 0)
			exit := runCheckConfig(logger, service.Config{SigningSecret: "test-secret", EncryptionKey: "test-key", AllowDevSecrets: true}, tc.external)
			if exit != 0 {
				t.Fatalf("exit=%d, want 0; log: %q", exit, buf.String())
			}
			out := buf.String()
			if !strings.Contains(out, "check_config=ok") {
				t.Fatalf("missing check_config=ok, got: %q", out)
			}
			for _, want := range []string{"transport_s3=" + tc.wantS3, "transport_vault=" + tc.wantVault} {
				if !strings.Contains(out, want) {
					t.Fatalf("log must contain %q, got: %q", want, out)
				}
			}
		})
	}
}

// TestRunCheckConfigVaultFailFast covers REQ-TLS-4/5 through the worker's
// check-config path: plaintext non-loopback and schemeless Vault addrs fail
// with exit 1 and actionable text; https passes with transport_vault=tls.
func TestRunCheckConfigVaultFailFast(t *testing.T) {
	cases := []struct {
		name        string
		addr        string
		allow       bool
		wantExit    int
		wantErrText string
	}{
		{"http non-loopback", "http://vault.example.com:8200", false, 1, "non-loopback"},
		{"http non-loopback even with opt-in", "http://vault.example.com:8200", true, 1, "non-loopback"},
		{"schemeless", "vault.example.com:8200", false, 1, "scheme"},
		{"https passes", "https://vault.example.com:8200", false, 0, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := log.New(&buf, "", 0)
			external := runtimeconfig.SigningArchive{VaultAddr: tc.addr, VaultToken: "t", VaultTransitKey: "audit-checkpoints", AllowInsecureVaultLoopback: tc.allow}
			exit := runCheckConfig(logger, service.Config{SigningSecret: "test-secret", EncryptionKey: "test-key", AllowDevSecrets: true}, external)
			if exit != tc.wantExit {
				t.Fatalf("exit=%d, want %d; log: %q", exit, tc.wantExit, buf.String())
			}
			out := buf.String()
			if tc.wantExit == 1 {
				if tc.wantErrText != "" && !strings.Contains(out, tc.wantErrText) {
					t.Fatalf("log must contain %q, got: %q", tc.wantErrText, out)
				}
				if strings.Contains(out, "check_config=ok") {
					t.Fatalf("check_config=ok must not be printed, got: %q", out)
				}
			} else if !strings.Contains(out, "transport_vault=tls") {
				t.Fatalf("ok line must report transport_vault=tls, got: %q", out)
			}
		})
	}
}

// TestStrictBoolEnvSubprocessFailsClosed is FM-1 (REQ-TLS-7): a malformed
// AUDIT_S3_USE_SSL value makes the real binary exit 1 naming the variable
// before flag.Parse — there is no fail-open path to plaintext.
func TestStrictBoolEnvSubprocessFailsClosed(t *testing.T) {
	binary, err := buildWorkerBinary()
	if err != nil {
		t.Skipf("worker binary unavailable: %v", err)
	}
	cmd := exec.Command(binary, "-check-config", "-allow-dev-secrets")
	cmd.Env = append(os.Environ(), "AUDIT_S3_USE_SSL=tru")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("binary must exit non-zero on a malformed AUDIT_S3_USE_SSL, output: %s", out)
	}
	if !strings.Contains(string(out), runtimeconfig.EnvS3UseSSL) || !strings.Contains(string(out), "not a valid boolean") {
		t.Fatalf("output must name the variable and the parse failure, got: %s", out)
	}
}

// TestStrictBoolEnvSubprocessPositive is the FM-1/REQ-TLS-7 wiring control:
// well-formed strict env vars parse into the flags and drive the resolved
// transport end-to-end. Network-free by construction: the S3 case configures
// no S3 endpoint (the flag merely parses → transport_s3=local), and the
// Vault case is a loopback opt-in (no probe on the signer path).
func TestStrictBoolEnvSubprocessPositive(t *testing.T) {
	binary, err := buildWorkerBinary()
	if err != nil {
		t.Skipf("worker binary unavailable: %v", err)
	}
	archiveDir := t.TempDir()
	baseEnv := []string{
		"AUDIT_SIGNING_SECRET=test-secret",
		"AUDIT_ENCRYPTION_KEY=test-key",
	}
	run := func(env ...string) (string, error) {
		cmd := exec.Command(binary, "-check-config", "-archive", archiveDir)
		cmd.Env = append(os.Environ(), baseEnv...)
		cmd.Env = append(cmd.Env, env...)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	t.Run("s3 use ssl true parses with no s3 config", func(t *testing.T) {
		out, err := run("AUDIT_S3_USE_SSL=true")
		if err != nil {
			t.Fatalf("check-config must exit 0: %v\n%s", err, out)
		}
		if !strings.Contains(out, "check_config=ok") || !strings.Contains(out, "transport_s3=local") {
			t.Fatalf("output must report check_config=ok with transport_s3=local, got: %s", out)
		}
	})
	t.Run("vault loopback opt-in", func(t *testing.T) {
		out, err := run("AUDIT_ALLOW_INSECURE_VAULT_LOOPBACK=true", "AUDIT_VAULT_ADDR=http://127.0.0.1:8200",
			"AUDIT_VAULT_TOKEN=t", "AUDIT_VAULT_TRANSIT_KEY=audit-checkpoints")
		if err != nil {
			t.Fatalf("check-config must exit 0: %v\n%s", err, out)
		}
		if !strings.Contains(out, "check_config=ok") || !strings.Contains(out, "transport_vault=http") {
			t.Fatalf("output must report check_config=ok with transport_vault=http, got: %s", out)
		}
	})
}

// TestStartupVaultHTTPFatal is QA F5/REQ-TLS-5: the real worker refuses to
// start (-once mode) with a non-loopback plaintext Vault addr — non-zero
// exit and the actionable validation text naming the opt-in, matching the
// check-config preflight.
func TestStartupVaultHTTPFatal(t *testing.T) {
	binary, err := buildWorkerBinary()
	if err != nil {
		t.Skipf("worker binary unavailable: %v", err)
	}
	dir := t.TempDir()
	cmd := exec.Command(binary, "-once", "-state", filepath.Join(dir, "state.json"), "-archive", filepath.Join(dir, "archive"))
	cmd.Env = append(os.Environ(),
		"AUDIT_SIGNING_SECRET=test-secret",
		"AUDIT_ENCRYPTION_KEY=test-key",
		"AUDIT_VAULT_ADDR=http://vault.example.com:8200",
		"AUDIT_VAULT_TOKEN=t",
		"AUDIT_VAULT_TRANSIT_KEY=audit-checkpoints",
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("startup with a non-loopback plaintext Vault must exit non-zero, output: %s", out)
	}
	if !strings.Contains(string(out), runtimeconfig.EnvAllowInsecureVaultLoopback) {
		t.Fatalf("startup must surface the actionable validation text naming the opt-in, got: %s", out)
	}
	if strings.Contains(string(out), "check_config=ok") {
		t.Fatalf("startup output must not contain check_config=ok, got: %s", out)
	}
}
