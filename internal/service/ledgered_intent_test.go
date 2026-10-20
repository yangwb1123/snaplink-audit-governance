package service

// Regression coverage for the post-ledger publication intent on the tenant
// (hot/cold) ingest path.
//
// Before this change the intent was written by a second store operation after
// the tenant commit, so an interrupted ingest left a committed event, a
// durable cold receipt and a WORM object with nothing owed to
// audit.events.ledgered.v1 — and the duplicate path returned before any repair
// could happen. These tests drive that window with a control-write fault and
// assert the whole recovery contract: the repair fires, the receipt is
// untouched, the stored chain (never the client-supplied one) is what gets
// published, and the repair is a one-shot signal rather than a retry
// amplifier.

import (
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// faultedLedgeredService returns a service whose store fails every durable
// control-plane write while armed, plus a hook to arm/disarm it and to count
// how many durable control writes an operation performed.
func faultedLedgeredService(t *testing.T, publisher LedgeredPublisher, archive bool) (*Service, func(armed bool), *atomic.Int64) {
	t.Helper()
	dir := t.TempDir()
	var armed atomic.Bool
	writes := &atomic.Int64{}
	st, err := store.OpenWithControlWriteFault(filepath.Join(dir, "state.json"), func() error {
		writes.Add(1)
		if armed.Load() {
			return errors.New("injected control write failure")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	archiveDir := ""
	if archive {
		archiveDir = filepath.Join(dir, "archive")
	}
	svc, err := New(st, Config{
		ArchiveDir: archiveDir, SegmentSize: 2, SigningSecret: "test-secret",
		LedgeredPublisher: publisher,
		Now:               func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
		AllowDevSecrets:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant(testCtx, "test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true, EventsPerSecond: 1000, Burst: 1000}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource(testCtx, "test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema(testCtx, "test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}}); err != nil {
		t.Fatal(err)
	}
	return svc, func(on bool) { armed.Store(on) }, writes
}

// durableReceipt reads the committed cold receipt, i.e. the state the store
// would hand to the next caller.
func durableReceipt(t *testing.T, svc *Service, tenantID, eventID string) domain.EventReceipt {
	t.Helper()
	key := store.EventKey(tenantID, eventID)
	var receipt domain.EventReceipt
	if err := svc.Store.ReadTenant(testCtx, tenantID, func(view *store.TenantView) error {
		found, ok := view.Ledger.Receipt(key)
		if !ok {
			return errors.New("no cold receipt for " + key)
		}
		receipt = found
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return receipt
}

func pendingIntent(t *testing.T, svc *Service, tenantID, eventID string) (domain.Event, bool) {
	t.Helper()
	key := store.EventKey(tenantID, eventID)
	var (
		pending domain.Event
		ok      bool
	)
	if err := svc.Store.ReadControl(testCtx, func(data *store.Snapshot) error {
		pending, ok = data.LedgeredOutbox[key]
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return pending, ok
}

// TestTenantIngestRepairsLostPublicationIntent is the core regression: an
// ingest interrupted between its durable cold append and its control-plane
// write used to be permanently unpublishable.
func TestTenantIngestRepairsLostPublicationIntent(t *testing.T) {
	publisher := &recordingLedgeredPublisher{}
	svc, arm, _ := faultedLedgeredService(t, publisher, false)
	at := time.Unix(1_700_000_900, 0).UTC()
	// A client-supplied chain must never reach the feed, on the repair path
	// either — the published envelope has to be the stored one.
	event := testEvent("intent-repair-1", "op-repair", at)
	event.StreamID, event.Sequence, event.Hash = "client:forged", 999, "forged-hash"

	arm(true)
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err == nil || !strings.Contains(err.Error(), "injected control write failure") {
		t.Fatalf("interrupted ingest receipt=%+v err=%v, want the injected control write failure", receipt, err)
	}
	arm(false)
	if _, ok := pendingIntent(t, svc, "tenant-a", event.EventID); ok {
		t.Fatal("a failed control write must not leave a durable intent")
	}
	if len(publisher.events) != 0 {
		t.Fatalf("interrupted ingest published %d events, want 0", len(publisher.events))
	}
	if got := svc.LedgeredIntentRepairs(); got != 0 {
		t.Fatalf("repair counter=%d after a failed commit, want 0", got)
	}

	interrupted := durableReceipt(t, svc, "tenant-a", event.EventID)
	if interrupted.Status != domain.StatusLedgered || !interrupted.IndexedAt.IsZero() || !interrupted.ArchivedAt.IsZero() {
		t.Fatalf("durable receipt=%+v, want an interrupted ledgered state", interrupted)
	}

	// The idempotent retry is what re-creates the lost intent.
	retry, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err != nil || !retry.Duplicate {
		t.Fatalf("retry receipt=%+v err=%v, want a clean duplicate", retry, err)
	}
	if got := svc.LedgeredIntentRepairs(); got != 1 {
		t.Fatalf("repair counter=%d, want exactly 1", got)
	}
	// AC-4: the repair must not mutate the receipt it reads.
	duplicateFlag := retry.Duplicate
	retry.Duplicate = false
	if retry != interrupted {
		t.Fatalf("duplicate receipt=%+v, want the untouched durable receipt %+v", retry, interrupted)
	}
	retry.Duplicate = duplicateFlag
	if len(publisher.events) != 0 {
		t.Fatalf("the duplicate path published %d events inline, want 0", len(publisher.events))
	}

	pending, ok := pendingIntent(t, svc, "tenant-a", event.EventID)
	if !ok {
		t.Fatal("the duplicate path must re-create the lost publication intent")
	}
	if pending.StreamID == "client:forged" || pending.Sequence == 999 || pending.Hash == "forged-hash" {
		t.Fatalf("repair queued the client-supplied chain state: %+v", pending)
	}
	if pending.StreamID != interrupted.StreamID || pending.Sequence != interrupted.Sequence || pending.Hash != interrupted.Hash {
		t.Fatalf("queued chain=%+v, durable receipt=%+v", pending, interrupted)
	}

	// The flush loop is still the only publisher.
	published, err := svc.FlushLedgeredOutbox(testCtx, 10)
	if err != nil || published != 1 {
		t.Fatalf("FlushLedgeredOutbox n=%d err=%v, want 1", published, err)
	}
	if len(publisher.events) != 1 || publisher.events[0].EventID != event.EventID {
		t.Fatalf("published %+v, want the repaired event %q", publisher.events, event.EventID)
	}
	if _, ok := pendingIntent(t, svc, "tenant-a", event.EventID); ok {
		t.Fatal("a published intent must be acknowledged out of the durable outbox")
	}
	if got := svc.LedgeredIntentRepairs(); got != 1 {
		t.Fatalf("repair counter=%d after a successful publish, want 1", got)
	}
}

// TestTenantIngestIntentIsOneCommitWithTheReceipt pins that the healthy path
// creates the intent in the same store operation as the receipt: exactly one
// durable control-plane write carries it. (The interrupted-commit shape that
// proves the write is the tenant commit's own lives in
// internal/store's TestUpdateTenantControlWriteFaultLosesOnlyControlDelta.)
func TestTenantIngestIntentIsOneCommitWithTheReceipt(t *testing.T) {
	// The broker stays down so the acknowledged intent is not removed before
	// it can be observed; the write that matters is the commit's own.
	publisher := &recordingLedgeredPublisher{err: errors.New("broker unavailable")}
	svc, _, writes := faultedLedgeredService(t, publisher, false)
	// Schema registration now uses the same real control-write seam. Retain
	// its preparation count; measure only this Ingest without resetting it.
	writesBeforeIngest := writes.Load()
	at := time.Unix(1_700_000_950, 0).UTC()
	event := testEvent("intent-single-commit", "op-single", at)

	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err != nil {
		t.Fatal(err)
	}
	if got := writes.Load() - writesBeforeIngest; got != 1 {
		t.Fatalf("durable control-plane writes=%d for one ingest, want 1 (the tenant commit)", got)
	}
	pending, ok := pendingIntent(t, svc, "tenant-a", event.EventID)
	if !ok {
		t.Fatal("the ingest commit must leave the publication intent durable")
	}
	if pending.StreamID != receipt.StreamID || pending.Sequence != receipt.Sequence || pending.Hash != receipt.Hash {
		t.Fatalf("queued chain=%+v, receipt=%+v", pending, receipt)
	}
	published, err := svc.FlushLedgeredOutbox(testCtx, 10)
	if err != nil || published != 0 {
		t.Fatalf("FlushLedgeredOutbox n=%d err=%v, want 0 while the broker is down", published, err)
	}
	if _, ok := pendingIntent(t, svc, "tenant-a", event.EventID); !ok {
		t.Fatal("a failed publish must retain the durable intent for the next flush")
	}
}

// TestTenantIngestRepairIsOneShotPerProcess pins the R-4 bound: once this
// process has re-created an intent, later retries must not keep re-queueing an
// event whose publication the flush loop already owes.
func TestTenantIngestRepairIsOneShotAfterConfirmation(t *testing.T) {
	publisher := &recordingLedgeredPublisher{}
	svc, arm, _ := faultedLedgeredService(t, publisher, false)
	at := time.Unix(1_700_001_000, 0).UTC()
	event := testEvent("intent-one-shot", "op-one-shot", at)

	arm(true)
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err == nil {
		t.Fatal("an ingest whose control write fails must report the failure")
	}
	arm(false)
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if got := svc.LedgeredIntentRepairs(); got != 1 {
		t.Fatalf("repair counter=%d, want 1", got)
	}

	// Actually deliver and durably confirm the repaired row, then retry.
	// The receipt is still StatusLedgered; the persistent confirmation, not
	// a process-local set or the absence of an outbox row, prevents requeue.
	if n, err := svc.FlushLedgeredOutbox(testCtx, 10); err != nil || n != 1 {
		t.Fatalf("flush=%d err=%v", n, err)
	}
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if _, ok := pendingIntent(t, svc, "tenant-a", event.EventID); ok {
		t.Fatal("a repaired key must not be re-queued on every later retry")
	}
	if got := svc.LedgeredIntentRepairs(); got != 1 {
		t.Fatalf("repair counter=%d after further retries, want 1", got)
	}
}

// TestTenantIngestRepairsOnlyInterruptedLedgeredIngests pins the
// StatusLedgered discriminator from both sides: an ingest that finished (and
// was published) and an event-id content conflict must never queue anything.
func TestTenantIngestRepairsOnlyInterruptedLedgeredIngests(t *testing.T) {
	t.Run("published ingest is never re-queued", func(t *testing.T) {
		publisher := &recordingLedgeredPublisher{}
		svc := testService(t, false)
		svc.Config.LedgeredPublisher = publisher
		at := time.Unix(1_700_001_050, 0).UTC()
		event := testEvent("intent-completed", "op-completed", at)
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatal(err)
		}
		duplicate, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
		if err != nil || !duplicate.Duplicate {
			t.Fatalf("duplicate receipt=%+v err=%v", duplicate, err)
		}
		if duplicate.Status != domain.StatusIndexed {
			t.Fatalf("completed ingest receipt status=%q, want indexed", duplicate.Status)
		}
		if got := svc.LedgeredIntentRepairs(); got != 0 {
			t.Fatalf("repair counter=%d for a published event, want 0", got)
		}
		if len(publisher.events) != 1 {
			t.Fatalf("publish count=%d, want 1", len(publisher.events))
		}
		if _, ok := pendingIntent(t, svc, "tenant-a", event.EventID); ok {
			t.Fatal("a completed ingest must not leave a durable intent")
		}
	})

	t.Run("archived ingest without hot payload is never re-queued", func(t *testing.T) {
		publisher := &recordingLedgeredPublisher{}
		svc := testService(t, true)
		svc.Config.LedgeredPublisher = publisher
		at := time.Unix(1_700_001_100, 0).UTC()
		event := testEvent("intent-archived", "op-archived", at)
		receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived)
		if err != nil {
			t.Fatal(err)
		}
		if receipt.Status != domain.StatusArchived {
			t.Fatalf("receipt status=%q, want archived", receipt.Status)
		}
		duplicate, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived)
		if err != nil || !duplicate.Duplicate {
			t.Fatalf("duplicate receipt=%+v err=%v", duplicate, err)
		}
		if got := svc.LedgeredIntentRepairs(); got != 0 {
			t.Fatalf("repair counter=%d for an archived event, want 0", got)
		}
		if _, ok := pendingIntent(t, svc, "tenant-a", event.EventID); ok {
			t.Fatal("an archived event must not leave a durable intent")
		}
	})

	t.Run("content conflict is never repaired", func(t *testing.T) {
		publisher := &recordingLedgeredPublisher{}
		svc, arm, _ := faultedLedgeredService(t, publisher, false)
		at := time.Unix(1_700_001_150, 0).UTC()
		original := testEvent("intent-conflict", "op-conflict", at)
		arm(true)
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, original, domain.StatusLedgered); err == nil {
			t.Fatal("an ingest whose control write fails must report the failure")
		}
		arm(false)
		// A different canonical content under the same event_id is a
		// conflict, and a conflicting caller must never queue a publication.
		conflicting := testEvent("intent-conflict", "op-conflict", at)
		conflicting.Payload = map[string]any{"resource": "invoice", "value": 99}
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, conflicting, domain.StatusLedgered); !errors.Is(err, domain.ErrConflict) {
			t.Fatalf("conflicting ingest err=%v, want a conflict", err)
		}
		if got := svc.LedgeredIntentRepairs(); got != 0 {
			t.Fatalf("repair counter=%d for a conflict, want 0", got)
		}
		if _, ok := pendingIntent(t, svc, "tenant-a", original.EventID); ok {
			t.Fatal("a content conflict must not queue a publication intent")
		}
	})

	t.Run("no publisher means no intent", func(t *testing.T) {
		svc, arm, writes := faultedLedgeredService(t, nil, false)
		// Do not count the real Schema initialization as this Ingest's work.
		writesBeforeIngest := writes.Load()
		at := time.Unix(1_700_001_200, 0).UTC()
		event := testEvent("intent-no-publisher", "op-no-pub", at)
		// With no publisher the closure writes no control delta, so the commit
		// attempts no control write and an armed fault must not fire.
		arm(true)
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatalf("unconfigured publisher err=%v, want a successful ingest", err)
		}
		if got := writes.Load() - writesBeforeIngest; got != 0 {
			t.Fatalf("durable control-plane writes=%d without a publisher, want 0", got)
		}
		arm(false)
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatal(err)
		}
		if got := svc.LedgeredIntentRepairs(); got != 0 {
			t.Fatalf("repair counter=%d without a publisher, want 0", got)
		}
		if _, ok := pendingIntent(t, svc, "tenant-a", event.EventID); ok {
			t.Fatal("an unconfigured publisher must not leave a durable intent")
		}
	})
}
