package service

// The sealed-object read behind the export download route: tenant binding,
// completion shape, the archive-bounded read, and the classification the
// placement inside the store closure buys.
//
// The route used to call the archive port itself with the raw request context.
// These tests pin the three properties that move bought: the read is bounded
// by the store's own operation deadline, an exhausted bound is classified as
// store.ErrOperationDeadline (the transports render that as the retryable 503
// store_timeout), and every refusal happens BEFORE any remote call so the
// route still masks another tenant's job ids.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// exportReadArchive is a recording archive.Store for the export object read.
// Every Get appends its key to the log, so a test can assert WHICH namespaces
// were touched and HOW OFTEN — the cost oracle that keeps this read to one
// sealed object and no event objects.
//
// When block is set, a Get black-holes until its context ends and then returns
// the context error after settle. settle exists for the same reason as
// blackHoleSettle/countingBlockingArchive.settle: the read bound and the store
// operation bound carry the SAME duration in every shipping configuration, so
// without a short pause after the context ends, whether Store.end observes
// op.ctx as already expired — i.e. whether the retryable classification is
// applied at all — would be a coin flip between two timers due together. A
// real remote read returns slightly after its context ends.
type exportReadArchive struct {
	mu     sync.Mutex
	keys   []string
	sealed []byte
	block  bool
	settle time.Duration
}

func (a *exportReadArchive) Put(context.Context, string, []byte) error { return nil }

func (a *exportReadArchive) Get(ctx context.Context, key string) ([]byte, error) {
	a.mu.Lock()
	a.keys = append(a.keys, key)
	sealed := a.sealed
	a.mu.Unlock()
	if !a.block {
		return sealed, nil
	}
	<-ctx.Done()
	if a.settle > 0 {
		time.Sleep(a.settle)
	}
	return nil, fmt.Errorf("get %s: %w", key, ctx.Err())
}

func (a *exportReadArchive) Ready(context.Context) error { return nil }

func (a *exportReadArchive) log() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.keys...)
}

func (a *exportReadArchive) getsFor(prefix string) int {
	count := 0
	for _, key := range a.log() {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			count++
		}
	}
	return count
}

// seedExportJob writes one export record straight into the snapshot: this is a
// read-path contract, not an export pipeline, so the job state is the fixture.
func seedExportReadJob(t *testing.T, svc *Service, job domain.ExportJob) {
	t.Helper()
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
		data.Exports[job.ID] = job
		return nil
	}); err != nil {
		t.Fatalf("seed export job %s: %v", job.ID, err)
	}
}

func completedExportJob(id string) domain.ExportJob {
	return domain.ExportJob{ID: id, TenantID: "tenant-a", Status: "completed", ObjectPath: "exports/" + id + ".jsonl"}
}

// TestReadExportObjectReturnsSealedBytesVerbatim pins the happy contract: the
// bytes are the archived object, unmodified, from exactly one Get against the
// job's own object path, and no event object is read. The download route's
// cost is unchanged by moving the read into the service.
func TestReadExportObjectReturnsSealedBytesVerbatim(t *testing.T) {
	arch := &exportReadArchive{sealed: []byte("sealed export bytes\n")}
	svc := boundTestService(t, 5*time.Second, &countingBlockingArchive{})
	svc.Config.Archive = arch
	job := completedExportJob("export-read-happy")
	seedExportReadJob(t, svc, job)

	object, err := svc.ReadExportObject(testCtx, "tenant-a", job.ID)
	if err != nil {
		t.Fatalf("ReadExportObject: %v", err)
	}
	if string(object) != "sealed export bytes\n" {
		t.Fatalf("object=%q, want the archived bytes verbatim", object)
	}
	if log := arch.log(); len(log) != 1 || log[0] != job.ObjectPath {
		t.Fatalf("archive keys=%v, want exactly [%s]", log, job.ObjectPath)
	}
	if got := arch.getsFor("events/"); got != 0 {
		t.Fatalf("event object reads=%d, want 0", got)
	}

	// Read-only: the job record and the governance trail are untouched.
	snapshot, err := svc.Store.Snapshot(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshot.Exports[job.ID]; got != job {
		t.Fatalf("job record=%+v, want it unchanged by a read", got)
	}
	if len(snapshot.AdminActions) != 0 {
		t.Fatalf("ReadExportObject appended admin facts: %+v", snapshot.AdminActions)
	}
}

// TestReadExportObjectIsBoundedAndClassified is the headline property: with
// the shipped relation (read bound == store operation bound) a black-holed
// archive ends the read at the operation deadline and the failure arrives
// classified, not as a bare sentinel the transport would have to guess at.
func TestReadExportObjectIsBoundedAndClassified(t *testing.T) {
	// The shipped ceiling stays far above the operation bound, so
	// archiveReadBound() resolves to Store.OperationTimeout().
	withArchiveReadTimeout(t, 5*time.Second)
	arch := &exportReadArchive{block: true, settle: 20 * time.Millisecond}
	svc := boundTestService(t, 200*time.Millisecond, &countingBlockingArchive{})
	svc.Config.Archive = arch
	job := completedExportJob("export-read-bounded")
	seedExportReadJob(t, svc, job)

	started := time.Now()
	object, err := svc.ReadExportObject(testCtx, "tenant-a", job.ID)
	elapsed := time.Since(started)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v, want the bound to expire", err)
	}
	if !errors.Is(err, store.ErrOperationDeadline) {
		t.Fatalf("error=%v, want store.ErrOperationDeadline so HTTP can answer 503 store_timeout", err)
	}
	if object != nil {
		t.Fatalf("a bounded read must fail closed, got %d bytes", len(object))
	}
	if elapsed > 2*time.Second {
		t.Fatalf("read took %s, want the ~200ms operation bound", elapsed)
	}
	if got := arch.getsFor("exports/"); got != 1 {
		t.Fatalf("sealed object reads=%d, want exactly 1", got)
	}
	if got := svc.Store.OperationDeadlineExhaustions(); got != 1 {
		t.Fatalf("audit_store_operation_deadline_exceeded_total=%d, want 1", got)
	}
}

// TestReadExportObjectNotClassifiedWhenCeilingWins is the discriminating leg.
// A read bound tighter than the store's own bound (only reachable by shrinking
// the package ceiling, since every shipping configuration keeps the store
// deadline below the 30s ceiling) exhausts BEFORE op.ctx, so Store.end does
// NOT classify. That is what makes the retryable 503 depend on the shipped
// relation rather than on luck: pin it here so a future edit to the bound
// cannot quietly turn this route's 503 into a bare sentinel.
func TestReadExportObjectNotClassifiedWhenCeilingWins(t *testing.T) {
	withArchiveReadTimeout(t, 100*time.Millisecond)
	arch := &exportReadArchive{block: true}
	svc := boundTestService(t, 5*time.Second, &countingBlockingArchive{})
	svc.Config.Archive = arch
	job := completedExportJob("export-read-unclassified")
	seedExportReadJob(t, svc, job)

	object, err := svc.ReadExportObject(testCtx, "tenant-a", job.ID)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v, want the shrunken ceiling to expire", err)
	}
	if errors.Is(err, store.ErrOperationDeadline) {
		t.Fatalf("error=%v, want NO classification: the store bound was never exhausted", err)
	}
	if object != nil {
		t.Fatalf("a bounded read must fail closed, got %d bytes", len(object))
	}
	if got := svc.Store.OperationDeadlineExhaustions(); got != 0 {
		t.Fatalf("audit_store_operation_deadline_exceeded_total=%d, want 0", got)
	}
}

// TestArchiveReadBoundCoversShippedStoreOperationTimeout pins the production
// relation the retryable 503 rests on, over the operator knob's whole useful
// range: with the shipped 30s ceiling, archiveReadBound() IS the store's
// per-operation bound for every value below it. audit-api refuses to start
// with a store bound at or above its 30s HTTP write timeout
// (runtimeconfig.ValidateStoreOperationTimeoutBelow, called from openStore
// and prepareServer), so "below the ceiling" is not an assumption here — it is
// the shipped precondition, and this table fails if the bound arithmetic ever
// stops satisfying it.
func TestArchiveReadBoundCoversShippedStoreOperationTimeout(t *testing.T) {
	withArchiveReadTimeout(t, 30*time.Second)
	for _, operationTimeout := range []time.Duration{2 * time.Second, 10 * time.Second, 25 * time.Second} {
		svc := boundTestService(t, operationTimeout, &countingBlockingArchive{})
		bound := svc.archiveReadBound()
		if bound != operationTimeout {
			t.Fatalf("operationTimeout=%s archiveReadBound()=%s, want the store bound (a tighter read bound would end the read before op.ctx and drop the retryable 503)", operationTimeout, bound)
		}
		if operationTimeout >= 30*time.Second {
			t.Fatalf("fixture %s is not a value the shipped binary accepts", operationTimeout)
		}
	}
}

// TestReadExportObjectShortCircuitsExhaustedBound pins fail-closed: a caller
// whose context is already gone issues no remote call at all. A cancellation
// stays a bare context.Canceled (Store.end propagates it unchanged, so the
// transport keeps answering 499 rather than a 5xx); an already-expired
// deadline is classified like any other exhausted store bound.
func TestReadExportObjectShortCircuitsExhaustedBound(t *testing.T) {
	withArchiveReadTimeout(t, 5*time.Second)
	arch := &exportReadArchive{sealed: []byte("must not be served\n")}
	svc := boundTestService(t, 30*time.Second, &countingBlockingArchive{})
	svc.Config.Archive = arch
	job := completedExportJob("export-read-exhausted")
	seedExportReadJob(t, svc, job)

	cancelled, cancel := context.WithCancel(testCtx)
	cancel()
	object, err := svc.ReadExportObject(cancelled, "tenant-a", job.ID)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v, want context.Canceled", err)
	}
	if errors.Is(err, store.ErrOperationDeadline) {
		t.Fatalf("error=%v, want a cancellation NOT laundered into the retryable store class", err)
	}
	if object != nil {
		t.Fatalf("cancelled read returned %d bytes, want none", len(object))
	}
	if got := len(arch.log()); got != 0 {
		t.Fatalf("archive reads=%d, want 0 for an already-cancelled caller", got)
	}

	expired, cancelExpired := context.WithDeadline(testCtx, time.Now().Add(-time.Hour))
	defer cancelExpired()
	object, err = svc.ReadExportObject(expired, "tenant-a", job.ID)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v, want the exhausted deadline", err)
	}
	if object != nil {
		t.Fatalf("expired read returned %d bytes, want none", len(object))
	}
	if got := len(arch.log()); got != 0 {
		t.Fatalf("archive reads=%d, want 0 for an already-expired caller", got)
	}
}

// TestReadExportObjectRefusesUnbindableJobs pins tenant binding, existence
// masking and the completion shape — every refusal before any remote call, so
// the route cannot be used to discover another tenant's jobs and a stalled
// archive cannot turn a bad request into a wait.
func TestReadExportObjectRefusesUnbindableJobs(t *testing.T) {
	arch := &exportReadArchive{sealed: []byte("sealed\n")}
	svc := boundTestService(t, 5*time.Second, &countingBlockingArchive{})
	svc.Config.Archive = arch
	job := completedExportJob("export-read-binding")
	seedExportReadJob(t, svc, job)
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
		pending := completedExportJob("export-read-pending")
		pending.Status = "pending"
		pending.ObjectPath = ""
		data.Exports[pending.ID] = pending
		noPath := completedExportJob("export-read-nopath")
		noPath.ObjectPath = ""
		data.Exports[noPath.ID] = noPath
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name     string
		tenantID string
		jobID    string
		wantErr  error
		wantText string
	}{
		{"unknown job", "tenant-a", "export-read-absent", domain.ErrNotFound, ""},
		{"another tenant's job", "tenant-b", job.ID, domain.ErrNotFound, ""},
		{"tenant-less caller", "", job.ID, domain.ErrInvalid, "tenant_id is required"},
		{"malformed tenant", "../etc", job.ID, domain.ErrInvalid, "tenant id"},
		{"job not completed", "tenant-a", "export-read-pending", domain.ErrConflict, "export is not completed"},
		{"completed without an object path", "tenant-a", "export-read-nopath", domain.ErrConflict, "export is not completed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			object, err := svc.ReadExportObject(testCtx, tc.tenantID, tc.jobID)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error=%v, want %v", err, tc.wantErr)
			}
			if tc.wantText != "" && !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("error=%v, want it to mention %q", err, tc.wantText)
			}
			if object != nil {
				t.Fatalf("a refused read returned %d bytes, want none", len(object))
			}
			if got := len(arch.log()); got != 0 {
				t.Fatalf("archive reads=%d, want 0 before any refusal is answered", got)
			}
		})
	}
}

// TestReadExportObjectMissingObjectIsNotFound pins the 404 class end to end
// against a real local archive, and pins WHY the translation happens in the
// service: os.IsNotExist does not unwrap a fmt.Errorf("%w") wrapper, so a
// transport that kept matching the archive's raw error taxonomy would see the
// missing object as an opaque 500 the moment the service wrapped it.
func TestReadExportObjectMissingObjectIsNotFound(t *testing.T) {
	archiveDir := filepath.Join(t.TempDir(), "archive")
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	svc, err := New(st, Config{ArchiveDir: archiveDir, SigningSecret: "test-secret", Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	job := completedExportJob("export-read-missing")
	seedExportReadJob(t, svc, job)

	_, err = svc.ReadExportObject(testCtx, "tenant-a", job.ID)
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("error=%v, want domain.ErrNotFound so the route answers 404 not_found", err)
	}

	// The baseline that makes the translation load-bearing.
	_, raw := svc.Config.Archive.Get(testCtx, job.ObjectPath)
	if !errors.Is(raw, fs.ErrNotExist) {
		t.Fatalf("archive error=%v, want fs.ErrNotExist (the missing-object class the service translates)", raw)
	}
	if os.IsNotExist(fmt.Errorf("export %s: %w", job.ID, raw)) {
		t.Skip("os.IsNotExist unexpectedly unwraps %w; the transport-side predicate would no longer be the hazard this test documents")
	}
}

// TestReadExportObjectUnconfiguredArchiveFailsClosed pins the nil-port shape:
// an archive that is not configured is an error the transport redacts, never
// a nil-interface panic on the download path.
func TestReadExportObjectUnconfiguredArchiveFailsClosed(t *testing.T) {
	arch := &exportReadArchive{sealed: []byte("sealed\n")}
	svc := boundTestService(t, 5*time.Second, &countingBlockingArchive{})
	svc.Config.Archive = arch
	job := completedExportJob("export-read-nil-archive")
	seedExportReadJob(t, svc, job)
	svc.Config.Archive = nil

	object, err := svc.ReadExportObject(testCtx, "tenant-a", job.ID)
	if err == nil {
		t.Fatal("an unconfigured archive must fail closed, not serve bytes")
	}
	if object != nil {
		t.Fatalf("failed read returned %d bytes, want none", len(object))
	}
	if errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrConflict) {
		t.Fatalf("error=%v, want an opaque dependency failure, not a 404/409 class", err)
	}
}
