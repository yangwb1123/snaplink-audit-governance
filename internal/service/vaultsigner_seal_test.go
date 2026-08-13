package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// blockingSigner is the deterministic stand-in for an in-flight Vault call
// (REQ-3, AC-3 at the service boundary): Sign/Verify return the context error
// once the context is done, so a pre-cancelled context aborts the operation
// promptly instead of blocking on the client timeout. entered (optional)
// reports when a sign/verify call actually started.
type blockingSigner struct {
	Signer
	entered chan struct{}
}

func (b *blockingSigner) enteredOnce() {
	if b.entered != nil {
		select {
		case b.entered <- struct{}{}:
		default:
		}
	}
}

func (b *blockingSigner) Sign(ctx context.Context, _ []byte) (string, error) {
	b.enteredOnce()
	<-ctx.Done()
	return "", ctx.Err()
}

func (b *blockingSigner) Verify(ctx context.Context, _ []byte, _ string) (bool, error) {
	b.enteredOnce()
	<-ctx.Done()
	return false, ctx.Err()
}

// TestIngestSealSignerFailureCommitsNothingAndArchivesNothing pins REQ-2's
// fail-closed promise on the ingest-seal path (QA review High #1): the seal
// signs inside the atomic Store.Update closure, so a signer failure must
// abort the whole attempt — no event, no receipt, no segment, no checkpoint —
// and the archive step (which runs only after a successful update) must never
// fire.
func TestIngestSealSignerFailureCommitsNothingAndArchivesNothing(t *testing.T) {
	svc := testService(t, true) // SegmentSize=2: the second event seals
	archiveStub := &recordingArchive{}
	svc.Config.Archive = archiveStub
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("seal-a1", "op-a", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	svc.Config.Signer = erroringSigner{Signer: svc.Config.Signer}
	archiveStub.puts = nil
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("seal-a2", "op-a", at.Add(time.Second)), domain.StatusLedgered); err == nil {
		t.Fatal("Ingest must surface the seal signer failure")
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	key := store.EventKey("tenant-a", "seal-a2")
	if _, ok := snap.Events[key]; ok {
		t.Fatal("failed ingest must not commit the sealing event")
	}
	if _, ok := snap.Receipts[key]; ok {
		t.Fatal("failed ingest must not commit a receipt")
	}
	streamKey := store.StreamKey("tenant-a", testEvent("seal-a2", "op-a", at.Add(time.Second)).Stream())
	if got := len(snap.Segments[streamKey]); got != 0 {
		t.Fatalf("failed ingest must not commit a segment, got %d", got)
	}
	if got := len(snap.Checkpoints[streamKey]); got != 0 {
		t.Fatalf("failed ingest must not commit a checkpoint, got %d", got)
	}
	if len(archiveStub.puts) != 0 {
		t.Fatalf("failed ingest must archive nothing, got %d archive Puts", len(archiveStub.puts))
	}
}

// TestSealPendingSegmentsSignerFailureCommitsNothingAndArchivesNothing pins
// the same atomicity on the worker's SealPendingSegments path (QA review High
// #1): a signer failure aborts the UpdateChecked closure — segments and
// checkpoints stay absent, pending hashes stay untouched, and nothing is
// archived.
func TestSealPendingSegmentsSignerFailureCommitsNothingAndArchivesNothing(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	archiveStub := &recordingArchive{fail: true}
	seedPendingEvents(t, svc, archiveStub, 2)

	svc.Config.Signer = erroringSigner{Signer: svc.Config.Signer}
	archiveStub.puts = nil
	if err := svc.SealPendingSegments(testCtx, "tenant-a"); err == nil {
		t.Fatal("SealPendingSegments must surface the signer failure")
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	streams := 0
	for key, stream := range snap.Streams {
		if stream.TenantID != "tenant-a" {
			continue
		}
		streams++
		if got := len(snap.Segments[key]); got != 0 {
			t.Fatalf("failed seal must not commit a segment for %s, got %d", key, got)
		}
		if got := len(snap.Checkpoints[key]); got != 0 {
			t.Fatalf("failed seal must not commit a checkpoint for %s, got %d", key, got)
		}
		if got := len(stream.PendingHashes); got != 2 {
			t.Fatalf("pending hashes=%d, want 2 (unchanged after failed seal)", got)
		}
	}
	if streams != 1 {
		t.Fatalf("tenant-a streams=%d, want 1", streams)
	}
	if len(archiveStub.puts) != 0 {
		t.Fatalf("failed seal must archive nothing, got %d Puts", len(archiveStub.puts))
	}
}

// TestIngestCancelledContextAbortsPromptlyAndRecordsNothing pins REQ-3 at the
// service boundary (QA review High #2): a cancelled context aborts the ingest
// promptly — the seal's Vault call returns the context error instead of
// blocking on the client timeout — and the atomic Update commits nothing.
func TestIngestCancelledContextAbortsPromptlyAndRecordsNothing(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("ctx-c1", "op-ctx", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	svc.Config.Signer = &blockingSigner{Signer: svc.Config.Signer}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		_, err := svc.Ingest(ctx, "tenant-a", crmPrincipal, testEvent("ctx-c2", "op-ctx", at.Add(time.Second)), domain.StatusLedgered)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Ingest with cancelled context: err=%v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Ingest did not abort promptly on a cancelled context")
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	key := store.EventKey("tenant-a", "ctx-c2")
	if _, ok := snap.Events[key]; ok {
		t.Fatal("cancelled ingest must not commit the event")
	}
	if _, ok := snap.Receipts[key]; ok {
		t.Fatal("cancelled ingest must not commit a receipt")
	}
}

// TestVerifyIntegrityCancelledCtxReturnsValidFalse pins REQ-3's VerifyIntegrity
// semantics (QA review High #2 + async review F2): a cancelled verification is
// fail-closed Valid=false but must never be recorded as a false "signature
// mismatch" fact — it is classified as "verification interrupted".
func TestVerifyIntegrityCancelledCtxReturnsValidFalse(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("vi-c1", "op-vi", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("vi-c2", "op-vi", at.Add(time.Second)), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateAggregateCheckpoint(testCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	svc.Config.Signer = &blockingSigner{Signer: svc.Config.Signer}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := svc.VerifyIntegrity(ctx, "tenant-a", "compliance-1", testEvent("vi-c1", "op-vi", at).Stream())
	if err != nil {
		t.Fatalf("a cancelled verification must not fail the endpoint: %v", err)
	}
	if result.Valid {
		t.Fatal("a cancelled verification must be fail-closed Valid=false")
	}
	interrupted := 0
	for _, e := range result.Errors {
		if strings.Contains(e, "signature mismatch") {
			t.Fatalf("interrupted verification must not be recorded as a mismatch fact: %q", e)
		}
		if strings.Contains(e, "verification interrupted") {
			interrupted++
		}
	}
	if interrupted == 0 {
		t.Fatalf("expected 'verification interrupted' entries, got %q", result.Errors)
	}
}
