package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

type archivePutObservation struct {
	key      string
	data     []byte
	calledAt time.Time
	deadline time.Time
}

type archivePutRecorder struct {
	puts []archivePutObservation
}

func (r *archivePutRecorder) Put(ctx context.Context, key string, data []byte) error {
	calledAt := time.Now()
	deadline, _ := ctx.Deadline()
	r.puts = append(r.puts, archivePutObservation{
		key: key, data: append([]byte(nil), data...), calledAt: calledAt, deadline: deadline,
	})
	return nil
}

func (r *archivePutRecorder) Get(context.Context, string) ([]byte, error) { return nil, nil }

func (r *archivePutRecorder) Ready(context.Context) error { return nil }

type archivePutBlockingStore struct {
	observedErr error
	cancelOnPut context.CancelFunc
	calledAt    time.Time
	finishedAt  time.Time
	deadline    time.Time
	calls       int
}

func (s *archivePutBlockingStore) Put(ctx context.Context, _ string, _ []byte) error {
	s.calledAt = time.Now()
	s.deadline, _ = ctx.Deadline()
	s.calls++
	if s.cancelOnPut != nil {
		s.cancelOnPut()
	}
	<-ctx.Done()
	s.finishedAt = time.Now()
	s.observedErr = ctx.Err()
	return ctx.Err()
}

func (s *archivePutBlockingStore) Get(context.Context, string) ([]byte, error) { return nil, nil }

func (s *archivePutBlockingStore) Ready(context.Context) error { return nil }

func TestArchivePendingPutReceivesPerObjectDeadline(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	seedPendingEvents(t, svc, &recordingArchive{fail: true}, 2)
	recorder := &archivePutRecorder{}
	svc.Config.Archive = recorder

	count, err := svc.ArchivePending(testCtx, "tenant-a")
	if err != nil || count != 2 {
		t.Fatalf("ArchivePending count=%d err=%v, want 2,nil", count, err)
	}
	if len(recorder.puts) != 2 {
		t.Fatalf("archive puts=%d, want 2", len(recorder.puts))
	}
	for _, put := range recorder.puts {
		if put.deadline.IsZero() {
			t.Fatal("archive Put received a context without a deadline")
		}
		lower := put.calledAt.Add(archivePutTimeout - 250*time.Millisecond)
		upper := put.calledAt.Add(archivePutTimeout + 250*time.Millisecond)
		if put.deadline.Before(lower) || put.deadline.After(upper) {
			t.Fatalf("deadline=%v, want approximately call time + %v", put.deadline, archivePutTimeout)
		}
		var event domain.Event
		if err := json.Unmarshal(put.data, &event); err != nil {
			t.Fatalf("archive data is not an event: %v", err)
		}
		wantKey := eventArchiveKey(event.TenantID, event.StreamID, event.Sequence, event.EventID)
		wantData, err := domain.CanonicalJSON(event)
		if err != nil {
			t.Fatalf("canonical event: %v", err)
		}
		if put.key != wantKey || !bytes.Equal(put.data, wantData) {
			t.Fatalf("archive put key/data changed: key=%q want=%q data_equal=%v", put.key, wantKey, bytes.Equal(put.data, wantData))
		}
	}
	if recorder.puts[0].deadline.Equal(recorder.puts[1].deadline) {
		t.Fatal("archive objects shared one deadline; each Put needs a fresh context")
	}
}

func TestArchivePendingEarlierCallerDeadlineWins(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	seedPendingEvents(t, svc, &recordingArchive{fail: true}, 1)
	blocking := &archivePutBlockingStore{}
	svc.Config.Archive = blocking

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := svc.ArchivePending(ctx, "tenant-a")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ArchivePending err=%v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("ArchivePending took %v; caller deadline did not win", elapsed)
	}
	if !errors.Is(blocking.observedErr, context.DeadlineExceeded) {
		t.Fatalf("Put observed %v, want context deadline exceeded", blocking.observedErr)
	}
}

func TestArchivePendingCancelledCtxAbortsPut(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	seedPendingEvents(t, svc, &recordingArchive{fail: true}, 1)
	blocking := &archivePutBlockingStore{}
	svc.Config.Archive = blocking

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	_, err := svc.ArchivePending(ctx, "tenant-a")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ArchivePending err=%v, want context canceled", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("ArchivePending took %v for an already-cancelled context", elapsed)
	}
	if !errors.Is(blocking.observedErr, context.Canceled) {
		t.Fatalf("Put observed %v, want context canceled", blocking.observedErr)
	}
}

func TestIngestArchivePutUsesCallerContext(t *testing.T) {
	svc := testService(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cancel precisely when Put starts, not while preflight/fsync is still
	// running. A 50ms timer armed before Ingest could expire before Put under
	// race/load, testing early withdrawal rather than context propagation.
	blocking := &archivePutBlockingStore{cancelOnPut: cancel}
	svc.Config.Archive = blocking

	receipt, err := svc.Ingest(ctx, "tenant-a", crmPrincipal, testEvent("ingest-context", "op-context", time.Unix(1_700_000_010, 0).UTC()), domain.StatusArchived)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Ingest err=%v, want caller cancellation from archive Put", err)
	}
	if !errors.Is(blocking.observedErr, context.Canceled) {
		t.Fatalf("Put observed %v, want caller cancellation", blocking.observedErr)
	}
	if receipt.Status == domain.StatusArchived {
		t.Fatalf("receipt=%+v, must not claim archived after a failed Put", receipt)
	}
}

func TestIngestArchivePutPreservesEarlierCallerDeadline(t *testing.T) {
	st, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	svc := newServiceOn(t, st)
	seedTestDomain(t, svc)
	recorder := &archivePutRecorder{}
	svc.Config.Archive = recorder
	// Assert the exact inherited deadline rather than relying on how fast
	// preparation reaches a blocking Put. The per-object timeout stays 5s;
	// the existing worker and detached-ingest tests still exercise real expiry.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	want, _ := ctx.Deadline()
	if _, err := svc.Ingest(ctx, "tenant-a", crmPrincipal, testEvent("ingest-earlier-deadline", "op-context", time.Unix(1_700_000_010, 0).UTC()), domain.StatusArchived); err != nil {
		t.Fatal(err)
	}
	if len(recorder.puts) != 1 || !recorder.puts[0].deadline.Equal(want) {
		t.Fatalf("Put did not preserve the earlier caller deadline: puts=%d", len(recorder.puts))
	}
}

func TestIngestArchivePutUsesDetachedContextDeadline(t *testing.T) {
	svc := testService(t, true)
	recorder := &archivePutRecorder{}
	svc.Config.Archive = recorder

	base := time.Now()
	ctx := context.WithoutCancel(context.Background())
	receipt, err := svc.Ingest(ctx, "tenant-a", crmPrincipal, testEvent("ingest-detached-context", "op-detached", time.Unix(1_700_000_010, 0).UTC()), domain.StatusArchived)
	if err != nil || receipt.Status != domain.StatusArchived {
		t.Fatalf("Ingest receipt=%+v err=%v, want archived,nil", receipt, err)
	}
	if len(recorder.puts) != 1 || recorder.puts[0].deadline.Before(base.Add(archivePutTimeout-250*time.Millisecond)) {
		t.Fatalf("detached archive Put deadline=%v, want the per-object deadline", recorder.puts[0].deadline)
	}
}

func TestIngestArchivePutDetachedContextCancelsBlockingPut(t *testing.T) {
	svc := testService(t, true)
	blocking := &archivePutBlockingStore{}
	svc.Config.Archive = blocking
	assertDetachedPutCancellation(t, svc, blocking)
}

func assertDetachedPutCancellation(t *testing.T, svc *Service, blocking *archivePutBlockingStore) {
	t.Helper()
	started := time.Now()
	receipt, err := svc.Ingest(context.WithoutCancel(context.Background()), "tenant-a", crmPrincipal,
		testEvent("ingest-detached-blocking", "op-detached-blocking", time.Unix(1_700_000_010, 0).UTC()), domain.StatusArchived)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ingest err=%v, want context deadline exceeded", err)
	}
	t.Logf("archive_boundary ingest_ms=%d before_put_ms=%d put_ms=%d after_put_ms=%d", time.Since(started).Milliseconds(), blocking.calledAt.Sub(started).Milliseconds(), blocking.finishedAt.Sub(blocking.calledAt).Milliseconds(), time.Since(blocking.finishedAt).Milliseconds())
	if blocking.calls != 1 || blocking.calledAt.Before(started) || blocking.deadline.IsZero() {
		t.Fatal("detached ingest did not reach exactly one bounded archive Put")
	}
	// This is the per-object Put bound, not the whole-Ingest latency: the
	// original real file commits before and after Put remain mandatory.
	if elapsed := blocking.finishedAt.Sub(blocking.calledAt); elapsed < archivePutTimeout-500*time.Millisecond || elapsed > archivePutTimeout+time.Second {
		t.Fatalf("detached Put elapsed=%v, want approximately %v", elapsed, archivePutTimeout)
	}
	if budget := blocking.deadline.Sub(blocking.calledAt); budget < archivePutTimeout-250*time.Millisecond || budget > archivePutTimeout+250*time.Millisecond {
		t.Fatal("detached Put did not inherit the unchanged per-object deadline")
	}
	if !errors.Is(blocking.observedErr, context.DeadlineExceeded) {
		t.Fatalf("detached Put observed %v, want context deadline exceeded", blocking.observedErr)
	}
	// The repository's existing ingest contract advances a failed archive to
	// indexed so ArchivePending can retry it; it must never claim archived.
	if receipt.Status != domain.StatusIndexed || !receipt.ArchivedAt.IsZero() {
		t.Fatalf("receipt=%+v, want indexed and not archived", receipt)
	}
}

func TestArchivePendingSegmentPutReceivesPerObjectDeadline(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	segment := domain.Segment{
		TenantID: "tenant-a", StreamID: "stream-segment", FirstSequence: 1, LastSequence: 1,
		FirstPrevHash: "prev", LastHash: "last", EventCount: 1, MerkleRoot: "root",
		ManifestHash: "manifest", Signature: "signature", CreatedAt: time.Unix(1_700_000_000, 0).UTC(),
	}
	backend.data.Segments[store.StreamKey("tenant-a", segment.StreamID)] = []domain.Segment{segment}
	recorder := &archivePutRecorder{}
	svc.Config.Archive = recorder

	if count, err := svc.ArchivePending(testCtx, "tenant-a"); err != nil || count != 0 {
		t.Fatalf("ArchivePending count=%d err=%v, want 0,nil for a segment-only pass", count, err)
	}
	if len(recorder.puts) != 1 {
		t.Fatalf("segment archive puts=%d, want 1", len(recorder.puts))
	}
	put := recorder.puts[0]
	wantKey := "segments/tenant-a/stream-segment/00000000000000000001-00000000000000000001.manifest.json"
	wantData, err := domain.CanonicalJSON(segment)
	if err != nil {
		t.Fatal(err)
	}
	if put.key != wantKey || !bytes.Equal(put.data, wantData) {
		t.Fatalf("segment archive key/data changed: key=%q want=%q data_equal=%v", put.key, wantKey, bytes.Equal(put.data, wantData))
	}
	if put.deadline.IsZero() {
		t.Fatal("segment archive Put received a context without a deadline")
	}
	if put.deadline.Before(put.calledAt.Add(archivePutTimeout-250*time.Millisecond)) || put.deadline.After(put.calledAt.Add(archivePutTimeout+250*time.Millisecond)) {
		t.Fatalf("segment deadline=%v, want approximately call time + %v", put.deadline, archivePutTimeout)
	}
}
