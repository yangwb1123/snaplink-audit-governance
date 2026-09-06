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
}

func (s *archivePutBlockingStore) Put(ctx context.Context, _ string, _ []byte) error {
	<-ctx.Done()
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

	count, err := svc.ArchivePending(context.Background(), "tenant-a")
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
	blocking := &archivePutBlockingStore{}
	svc.Config.Archive = blocking

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	receipt, err := svc.Ingest(ctx, "tenant-a", crmPrincipal, testEvent("ingest-context", "op-context", time.Unix(1_700_000_010, 0).UTC()), domain.StatusArchived)
	if err == nil {
		t.Fatal("Ingest succeeded despite a cancelled archive Put")
	}
	if !errors.Is(blocking.observedErr, context.DeadlineExceeded) {
		t.Fatalf("Put observed %v, want caller deadline exceeded", blocking.observedErr)
	}
	if receipt.Status == domain.StatusArchived {
		t.Fatalf("receipt=%+v, must not claim archived after a failed Put", receipt)
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

	started := time.Now()
	receipt, err := svc.Ingest(context.WithoutCancel(context.Background()), "tenant-a", crmPrincipal,
		testEvent("ingest-detached-blocking", "op-detached-blocking", time.Unix(1_700_000_010, 0).UTC()), domain.StatusArchived)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ingest err=%v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed < archivePutTimeout-500*time.Millisecond || elapsed > archivePutTimeout+time.Second {
		t.Fatalf("detached Put elapsed=%v, want approximately %v", elapsed, archivePutTimeout)
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

	if count, err := svc.ArchivePending(context.Background(), "tenant-a"); err != nil || count != 0 {
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
