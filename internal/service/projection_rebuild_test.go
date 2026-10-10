package service

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

type rebuildRecorder struct {
	events []domain.Event
	err    error
	state  *store.Store
}

func (r *rebuildRecorder) Insert(ctx context.Context, event domain.Event) error {
	if r.state != nil {
		// A destination performing independent store access must not deadlock.
		read := make(chan error, 1)
		go func() { read <- r.state.ReadControl(ctx, func(*store.Snapshot) error { return nil }) }()
		select {
		case err := <-read:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if r.err != nil {
		return r.err
	}
	copy, err := domain.CloneEvent(event)
	if err != nil {
		return err
	}
	r.events = append(r.events, copy)
	return nil
}

func uniqueRebuilt(events []domain.Event) map[string]bool {
	unique := map[string]bool{}
	for _, event := range events {
		unique[event.EventID] = true
	}
	return unique
}

func rebuildAll(t *testing.T, svc *Service, recorder *rebuildRecorder) ProjectionRebuildPage {
	t.Helper()
	cursor := store.LedgerPageCursor{}
	total := ProjectionRebuildPage{}
	for i := 0; i < 100; i++ {
		result, err := svc.RebuildProjectionPage(testCtx, "tenant-a", cursor, 1, recorder)
		if err != nil {
			t.Fatal(err)
		}
		if result.Scanned > 1 {
			t.Fatal("page budget exceeded")
		}
		total.Scanned += result.Scanned
		total.Written += result.Written
		total.Failed += result.Failed
		if result.Done {
			total.Done = true
			return total
		}
		cursor = result.Next
	}
	t.Fatal("finite sweep never finished")
	return total
}

func TestProjectionRebuildDoesNotChangeTruthOrPublish(t *testing.T) {
	svc := testService(t, false)
	defer svc.Store.Close()
	publisher := &recordingLedgeredPublisher{err: errors.New("offline")}
	svc.Config.LedgeredPublisher = publisher
	for _, id := range []string{"rebuild-1", "rebuild-2"} {
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent(id, "op-rebuild", time.Unix(1_700_002_000, 0).UTC()), domain.StatusLedgered); err != nil {
			t.Fatal(err)
		}
	}
	var before *store.Snapshot
	if err := svc.Store.Read(testCtx, func(data *store.Snapshot) error { before = data; return nil }); err != nil {
		t.Fatal(err)
	}
	recorder := &rebuildRecorder{state: svc.Store}
	result := rebuildAll(t, svc, recorder)
	if result.Failed != 0 || result.Written != len(recorder.events) || len(uniqueRebuilt(recorder.events)) != 2 {
		t.Fatalf("rebuild=%+v", result)
	}
	if len(publisher.events) != 0 {
		t.Fatal("rebuild published to broker")
	}
	if err := svc.Store.Read(testCtx, func(data *store.Snapshot) error {
		if !reflect.DeepEqual(before, data) {
			t.Fatal("rebuild mutated ledger, receipts or control")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, event := range recorder.events {
		if event.TenantID != "tenant-a" || event.Hash == "" || event.Sequence <= 0 {
			t.Fatal("unbound projection")
		}
	}
}

func TestProjectionRebuildArchivedEvidenceOutsideLock(t *testing.T) {
	svc := testService(t, true)
	defer svc.Store.Close()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("rebuild-archived", "op-rebuild", time.Unix(1_700_002_001, 0).UTC()), domain.StatusArchived); err != nil {
		t.Fatal(err)
	}
	svc.Config.Archive = unlockedPublicationArchive{Store: svc.Config.Archive, state: svc.Store}
	recorder := &rebuildRecorder{state: svc.Store}
	result := rebuildAll(t, svc, recorder)
	if result.Failed != 0 || len(recorder.events) == 0 {
		t.Fatalf("rebuild=%+v", result)
	}
	if recorder.events[0].EventID != "rebuild-archived" {
		t.Fatal("wrong archived identity")
	}
}

func TestProjectionRebuildRejectsTamperingButContinues(t *testing.T) {
	svc := testService(t, false)
	defer svc.Store.Close()
	for _, id := range []string{"rebuild-bad", "rebuild-good"} {
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent(id, "op-rebuild", time.Unix(1_700_002_002, 0).UTC()), domain.StatusLedgered); err != nil {
			t.Fatal(err)
		}
	}
	if err := svc.Store.UpdateTenant(testCtx, "tenant-a", store.HotFirst, func(view *store.TenantView) error {
		event := view.Hot.Events[store.EventKey("tenant-a", "rebuild-bad")]
		event.Payload["value"] = "tampered with retained SourceDigest"
		view.Hot.Events[store.EventKey("tenant-a", "rebuild-bad")] = event
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	recorder := &rebuildRecorder{}
	result := rebuildAll(t, svc, recorder)
	if result.Failed == 0 || len(uniqueRebuilt(recorder.events)) != 1 || recorder.events[0].EventID != "rebuild-good" {
		t.Fatalf("rebuild=%+v events=%d", result, len(recorder.events))
	}
}

func TestProjectionRebuildDestinationFailureDoesNotAdvance(t *testing.T) {
	svc := testService(t, false)
	defer svc.Store.Close()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("rebuild-retry", "op-rebuild", time.Unix(1_700_002_003, 0).UTC()), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	writer := &rebuildRecorder{err: errors.New("unavailable")}
	result, err := svc.RebuildProjectionPage(testCtx, "tenant-a", store.LedgerPageCursor{}, 100, writer)
	if err == nil || result.Done || result.Next != (store.LedgerPageCursor{}) {
		t.Fatalf("failed destination consumed cursor: %+v err=%v", result, err)
	}
	writer.err = nil
	if result := rebuildAll(t, svc, writer); result.Failed != 0 || len(uniqueRebuilt(writer.events)) != 1 {
		t.Fatalf("retry=%+v", result)
	}
}

func TestProjectionRebuildScopeAndCancellation(t *testing.T) {
	svc := testService(t, false)
	defer svc.Store.Close()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("rebuild-scope", "op-rebuild", time.Unix(1_700_002_004, 0).UTC()), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	writer := &rebuildRecorder{}
	if _, err := svc.RebuildProjectionPage(testCtx, "", store.LedgerPageCursor{}, 100, writer); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
	if result, err := svc.RebuildProjectionPage(testCtx, "tenant-b", store.LedgerPageCursor{}, 100, writer); err != nil || result.Written != 0 {
		t.Fatalf("foreign scope=%+v err=%v", result, err)
	}
	ctx, cancel := context.WithCancel(testCtx)
	cancel()
	if _, err := svc.RebuildProjectionPage(ctx, "tenant-a", store.LedgerPageCursor{}, 100, writer); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(writer.events) != 0 {
		t.Fatal("out-of-scope or cancelled rebuild wrote evidence")
	}
}
