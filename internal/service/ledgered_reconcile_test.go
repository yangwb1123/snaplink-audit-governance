package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/fsutil"
	"github.com/snaplink/audit-governance/internal/store"
)

func dropPublicationIntents(t *testing.T, svc *Service) {
	t.Helper()
	if err := svc.Store.UpdateControl(testCtx, func(data *store.Snapshot) error {
		data.LedgeredOutbox = map[string]domain.Event{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReconcileRepairsInterruptedCommitWithoutClientRetry(t *testing.T) {
	publisher := &recordingLedgeredPublisher{}
	svc, arm, writes := faultedLedgeredService(t, publisher, false)
	event := testEvent("active-repair", "op-active", time.Unix(1_700_001_300, 0).UTC())
	arm(true)
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err == nil {
		t.Fatal("expected interrupted tenant commit")
	}
	arm(false)
	before := durableReceipt(t, svc, "tenant-a", event.EventID)
	if _, found := pendingIntent(t, svc, "tenant-a", event.EventID); found {
		t.Fatal("fault must leave no intent")
	}
	// Reconstruct the process, without another ingest or a client retry.
	fresh, err := New(svc.Store, svc.Config)
	if err != nil {
		t.Fatal(err)
	}
	result, err := fresh.ReconcileLedgeredPublications(testCtx, "tenant-a", store.LedgerPageCursor{}, 10)
	if err != nil || result.Repaired != 1 || result.Failed != 0 || !result.Done {
		t.Fatalf("reconciliation=%+v err=%v", result, err)
	}
	if after := durableReceipt(t, fresh, "tenant-a", event.EventID); after != before {
		t.Fatalf("reconciliation rewrote receipt: before=%+v after=%+v", before, after)
	}
	if len(publisher.events) != 0 {
		t.Fatal("reconciliation must only queue, not publish inline")
	}
	if n, err := fresh.FlushLedgeredOutbox(testCtx, 10); err != nil || n != 1 {
		t.Fatalf("flush=%d err=%v", n, err)
	}
	if len(publisher.events) != 1 || publisher.events[0].Hash != before.Hash {
		t.Fatalf("wrong canonical event delivered: %+v", publisher.events)
	}
	writes.Store(0)
	result, err = fresh.ReconcileLedgeredPublications(testCtx, "tenant-a", store.LedgerPageCursor{}, 10)
	if err != nil || result.Repaired != 0 || result.Failed != 0 || writes.Load() != 0 {
		t.Fatalf("confirmed idle sweep=%+v writes=%d err=%v", result, writes.Load(), err)
	}
	if fresh.LedgeredIntentRepairs() != 1 {
		t.Fatalf("repair count=%d", fresh.LedgeredIntentRepairs())
	}
}

func TestReconcileFailedControlSaveDoesNotConsumeProgress(t *testing.T) {
	publisher := &recordingLedgeredPublisher{}
	svc, arm, _ := faultedLedgeredService(t, publisher, false)
	arm(true)
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("repair-retry", "op-retry", time.Unix(1_700_001_301, 0).UTC()), domain.StatusLedgered); err == nil {
		t.Fatal("expected interrupted tenant commit")
	}
	result, err := svc.ReconcileLedgeredPublications(testCtx, "tenant-a", store.LedgerPageCursor{}, 10)
	if err == nil || result.Repaired != 0 || result.Next != (store.LedgerPageCursor{}) || svc.LedgeredIntentRepairs() != 0 {
		t.Fatalf("failed repair consumed progress: %+v repairs=%d err=%v", result, svc.LedgeredIntentRepairs(), err)
	}
	arm(false)
	result, err = svc.ReconcileLedgeredPublications(testCtx, "tenant-a", store.LedgerPageCursor{}, 10)
	if err != nil || result.Repaired != 1 || svc.LedgeredIntentRepairs() != 1 {
		t.Fatalf("repair retry=%+v repairs=%d err=%v", result, svc.LedgeredIntentRepairs(), err)
	}
}

type unlockedPublicationArchive struct {
	archive.Store
	state *store.Store
}

func (a unlockedPublicationArchive) Get(ctx context.Context, key string) ([]byte, error) {
	read := make(chan error, 1)
	go func() { read <- a.state.ReadControl(ctx, func(*store.Snapshot) error { return nil }) }()
	select {
	case err := <-read:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return a.Store.Get(ctx, key)
}

func TestReconcileArchivedUnconfirmedEventOutsideStoreLock(t *testing.T) {
	svc := testService(t, true)
	defer svc.Store.Close()
	publisher := &recordingLedgeredPublisher{err: errors.New("broker unavailable")}
	svc.Config.LedgeredPublisher = publisher
	event := testEvent("archived-unconfirmed", "op-archived", time.Unix(1_700_001_302, 0).UTC())
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived)
	if err != nil || receipt.Status != domain.StatusArchived {
		t.Fatalf("archival=%+v err=%v", receipt, err)
	}
	// Archived/IndexedAt is deliberately NOT a broker acknowledgement.
	dropPublicationIntents(t, svc)
	svc.Config.Archive = unlockedPublicationArchive{Store: svc.Config.Archive, state: svc.Store}
	ctx, cancel := context.WithTimeout(testCtx, time.Second)
	defer cancel()
	result, err := svc.ReconcileLedgeredPublications(ctx, "tenant-a", store.LedgerPageCursor{}, 10)
	if err != nil || result.Repaired != 1 || result.Failed != 0 {
		t.Fatalf("archive repair=%+v err=%v (archive IO may be under Store.mu)", result, err)
	}
	pending, found := pendingIntent(t, svc, "tenant-a", event.EventID)
	if !found || pending.Hash != receipt.Hash || pending.Sequence != receipt.Sequence {
		t.Fatalf("recovered wrong archive event: %+v found=%v", pending, found)
	}
	if after := durableReceipt(t, svc, "tenant-a", event.EventID); after != receipt {
		t.Fatalf("archive repair changed receipt: %+v", after)
	}
	publisher.err = nil
	if n, err := svc.FlushLedgeredOutbox(testCtx, 10); err != nil || n != 1 {
		t.Fatalf("archive flush=%d err=%v", n, err)
	}
	result, err = svc.ReconcileLedgeredPublications(testCtx, "tenant-a", store.LedgerPageCursor{}, 10)
	if err != nil || result.Repaired != 0 || result.Failed != 0 {
		t.Fatalf("confirmed archive queued again: %+v err=%v", result, err)
	}
}

func TestReconcileRejectsCorruptionAndContinuesBoundedPages(t *testing.T) {
	svc := testService(t, false)
	defer svc.Store.Close()
	svc.Config.LedgeredPublisher = &recordingLedgeredPublisher{err: errors.New("broker unavailable")}
	for _, id := range []string{"corrupt-original", "healthy-original"} {
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent(id, "op-bounded", time.Unix(1_700_001_303, 0).UTC()), domain.StatusLedgered); err != nil {
			t.Fatal(err)
		}
	}
	dropPublicationIntents(t, svc)
	if err := svc.Store.UpdateTenant(testCtx, "tenant-a", store.HotFirst, func(view *store.TenantView) error {
		key := store.EventKey("tenant-a", "corrupt-original")
		event := view.Hot.Events[key]
		event.Payload["value"] = "tampered"
		view.Hot.Events[key] = event
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	after := store.LedgerPageCursor{}
	var repairs, failures int
	for round := 0; round < 20; round++ {
		result, err := svc.ReconcileLedgeredPublications(testCtx, "tenant-a", after, 1)
		if err != nil || result.Scanned > 1 {
			t.Fatalf("bounded reconciliation=%+v err=%v", result, err)
		}
		repairs += result.Repaired
		failures += result.Failed
		if result.Done {
			break
		}
		after = result.Next
	}
	if repairs != 1 || failures == 0 {
		t.Fatalf("repairs=%d failures=%d; poison evidence blocked healthy candidate", repairs, failures)
	}
	if _, found := pendingIntent(t, svc, "tenant-a", "corrupt-original"); found {
		t.Fatal("tampered event entered publication queue")
	}
	if _, found := pendingIntent(t, svc, "tenant-a", "healthy-original"); !found {
		t.Fatal("healthy event was not repaired")
	}
}

func TestConfirmationFailureRetainsIntentAndAllowsRedelivery(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	svc := newServiceOn(t, st)
	seedTestDomain(t, svc)
	publisher := &recordingLedgeredPublisher{err: errors.New("broker unavailable")}
	svc.Config.LedgeredPublisher = publisher
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("confirmation-unavailable", "op-ack", time.Unix(1_700_001_304, 0).UTC()), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	// Reads still report missing; only confirmation writes fail AFTER a
	// successful broker call. Never delete the intent before this write.
	digest := sha256.Sum256([]byte("confirmation-unavailable"))
	name := hex.EncodeToString(digest[:])
	blocker := filepath.Join(dir, "publications", fsutil.EncodeKeyComponent("tenant-a"), name[:2], name+".json.tmp")
	if err := os.MkdirAll(blocker, 0o750); err != nil {
		t.Fatal(err)
	}
	publisher.err = nil
	if n, err := svc.FlushLedgeredOutbox(testCtx, 10); err == nil || n != 0 {
		t.Fatalf("confirmation failure flush=%d err=%v", n, err)
	}
	if len(publisher.events) != 1 {
		t.Fatal("confirmation fault must occur after a successful broker call")
	}
	if _, found := pendingIntent(t, svc, "tenant-a", "confirmation-unavailable"); !found {
		t.Fatal("unconfirmed intent was removed")
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	if n, err := svc.FlushLedgeredOutbox(testCtx, 10); err != nil || n != 1 {
		t.Fatalf("recovered confirmation flush=%d err=%v", n, err)
	}
	if len(publisher.events) != 2 {
		t.Fatal("unknown confirmation must permit at-least-once redelivery")
	}
}

func TestConfirmedButUncleanedIntentDoesNotRedeliver(t *testing.T) {
	publisher := &recordingLedgeredPublisher{err: errors.New("broker unavailable")}
	svc, arm, _ := faultedLedgeredService(t, publisher, false)
	event := testEvent("confirmation-before-cleanup", "op-ack", time.Unix(1_700_001_305, 0).UTC())
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	publisher.err = nil
	arm(true)
	if n, err := svc.FlushLedgeredOutbox(testCtx, 10); err == nil || n != 0 {
		t.Fatalf("failed cleanup flush=%d err=%v", n, err)
	}
	if len(publisher.events) != 1 {
		t.Fatalf("publish count=%d", len(publisher.events))
	}
	arm(false)
	// New service process loses all gates/counters, but keeps durable delivery
	// metadata. A surviving intent only owes cleanup, not another delivery.
	fresh, err := New(svc.Store, svc.Config)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := fresh.FlushLedgeredOutbox(testCtx, 10); err != nil || n != 0 {
		t.Fatalf("cleanup-only flush=%d err=%v", n, err)
	}
	if len(publisher.events) != 1 {
		t.Fatal("confirmed event was delivered again after cleanup failure")
	}
	if _, found := pendingIntent(t, fresh, "tenant-a", event.EventID); found {
		t.Fatal("confirmed intent was not cleaned up")
	}
}
