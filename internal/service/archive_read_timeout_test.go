package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

type blockingArchiveGet struct {
	started chan struct{}
	once    sync.Once
}

func (b *blockingArchiveGet) Put(context.Context, string, []byte) error { return nil }

func (b *blockingArchiveGet) Get(ctx context.Context, _ string) ([]byte, error) {
	b.once.Do(func() { close(b.started) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *blockingArchiveGet) Ready(context.Context) error { return nil }

// TestArchivedEventReadIsBounded pins the fallback's per-call timeout. A
// WORM client that honors context cancellation must release the caller even
// when the original request has no deadline (GetEvent's service API has no
// context parameter).
func TestArchivedEventReadIsBounded(t *testing.T) {
	original := archiveReadTimeout
	archiveReadTimeout = 25 * time.Millisecond
	t.Cleanup(func() { archiveReadTimeout = original })
	archive := &blockingArchiveGet{started: make(chan struct{})}
	svc := &Service{Config: Config{Archive: archive}}
	receipt := domain.EventReceipt{TenantID: "tenant-a", StreamID: "stream-a", Sequence: 1, EventID: "archive-timeout"}
	started := time.Now()
	_, err := svc.archivedEventContext(context.Background(), receipt)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("archive fallback took %s, want bounded read", elapsed)
	}
	select {
	case <-archive.started:
	default:
		t.Fatal("archive Get was not called")
	}
}

// TestArchivedEventReadHonorsCallerDeadline ensures request cancellation is
// still the tighter bound when a context already has an earlier deadline.
func TestArchivedEventReadHonorsCallerDeadline(t *testing.T) {
	original := archiveReadTimeout
	archiveReadTimeout = time.Second
	t.Cleanup(func() { archiveReadTimeout = original })
	archive := &blockingArchiveGet{started: make(chan struct{})}
	svc := &Service{Config: Config{Archive: archive}}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	receipt := domain.EventReceipt{TenantID: "tenant-a", StreamID: "stream-a", Sequence: 1, EventID: "archive-caller-deadline"}
	_, err := svc.archivedEventContext(ctx, receipt)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v, want caller deadline exceeded", err)
	}
}
