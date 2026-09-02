package store

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

func ownershipEvent(id string) domain.Event {
	return domain.Event{
		EventID:            id,
		TenantID:           "tenant-a",
		SourceSystem:       "crm",
		EventType:          "audit.event",
		SchemaID:           "audit.event",
		SchemaVersion:      1,
		OccurredAt:         time.Unix(1_700_000_000, 0).UTC(),
		ReceivedAt:         time.Unix(1_700_000_010, 0).UTC(),
		OperationID:        "op-1",
		Actor:              domain.Actor{ID: "user-1", Roles: []string{"operator"}},
		Targets:            []domain.Target{{Type: "invoice", ID: "inv-1"}},
		Action:             "update",
		Outcome:            "success",
		ChangedFields:      map[string]domain.FieldChange{"status": {Before: "open", After: []any{map[string]any{"state": "paid"}}}},
		Payload:            map[string]any{"resource": "invoice", "count": json.Number("7"), "nested": map[string]any{"state": "open"}},
		DataClassification: "internal",
		RetentionClass:     "standard",
		IdempotencyKey:     "idem-" + id,
		StreamID:           "tenant-a:source:crm",
		Sequence:           1,
		Hash:               "hash-" + id,
	}
}

func mutateOwnedEvent(event *domain.Event) {
	event.Payload["resource"] = "mutated"
	event.Payload["nested"].(map[string]any)["state"] = "closed"
	event.Actor.Roles[0] = "admin"
	event.Targets[0].ID = "inv-9"
	change := event.ChangedFields["status"]
	change.After.([]any)[0].(map[string]any)["state"] = "closed"
	event.ChangedFields["status"] = change
}

func assertEventEqual(t *testing.T, got, want domain.Event) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("event mismatch\n got: %+v\nwant: %+v", got, want)
	}
}

func TestFileBackendSaveCopiesInputAndLoadReturnsIndependentGraph(t *testing.T) {
	backend, err := openFileBackend("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	source := NewSnapshot()
	key := EventKey("tenant-a", "evt-backend")
	event := ownershipEvent("evt-backend")
	source.Events[key] = event
	source.LedgeredOutbox[key] = event
	if err := backend.Save(source); err != nil {
		t.Fatal(err)
	}

	stored := backend.data.Events[key]
	assertEventEqual(t, stored, ownershipEvent("evt-backend"))
	savedOutbox := backend.data.LedgeredOutbox[key]
	assertEventEqual(t, savedOutbox, ownershipEvent("evt-backend"))

	mutated := source.Events[key]
	mutateOwnedEvent(&mutated)
	source.Events[key] = mutated
	mutatedOutbox := source.LedgeredOutbox[key]
	mutateOwnedEvent(&mutatedOutbox)
	source.LedgeredOutbox[key] = mutatedOutbox
	assertEventEqual(t, backend.data.Events[key], ownershipEvent("evt-backend"))
	assertEventEqual(t, backend.data.LedgeredOutbox[key], ownershipEvent("evt-backend"))

	loaded, err := backend.Load()
	if err != nil {
		t.Fatal(err)
	}
	loadedEvent := loaded.Events[key]
	mutateOwnedEvent(&loadedEvent)
	loaded.Events[key] = loadedEvent
	fresh, err := backend.Load()
	if err != nil {
		t.Fatal(err)
	}
	assertEventEqual(t, fresh.Events[key], ownershipEvent("evt-backend"))
}

func TestFileBackendSaveClonesEventsAndOutboxIndependently(t *testing.T) {
	backend, err := openFileBackend("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	source := NewSnapshot()
	key := EventKey("tenant-a", "evt-independent")
	event := ownershipEvent("evt-independent")
	source.Events[key] = event
	source.LedgeredOutbox[key] = event
	if err := backend.Save(source); err != nil {
		t.Fatal(err)
	}

	hot := backend.data.Events[key]
	hot.Payload["nested"].(map[string]any)["state"] = "hot-mutated"
	backend.data.Events[key] = hot
	if got := backend.data.LedgeredOutbox[key].Payload["nested"].(map[string]any)["state"]; got != "open" {
		t.Fatalf("outbox event shared hot payload graph: %v", got)
	}
}

func TestSplitStoreUpdateTenantClonesRetainedEventsAndOutbox(t *testing.T) {
	st, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	key := EventKey("tenant-a", "evt-split")
	shared := ownershipEvent("evt-split")
	if err := st.UpdateTenant("tenant-a", HotFirst, func(view *TenantView) error {
		view.Hot.Events[key] = shared
		view.Global.LedgeredOutbox[key] = shared
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	mutateOwnedEvent(&shared)
	assertEventEqual(t, st.split.tenant["tenant-a"].hot.Events[key], ownershipEvent("evt-split"))
	assertEventEqual(t, st.split.control.LedgeredOutbox[key], ownershipEvent("evt-split"))

	hot := st.split.tenant["tenant-a"].hot.Events[key]
	hot.Payload["nested"].(map[string]any)["state"] = "hot-mutated"
	st.split.tenant["tenant-a"].hot.Events[key] = hot
	if got := st.split.control.LedgeredOutbox[key].Payload["nested"].(map[string]any)["state"]; got != "open" {
		t.Fatalf("control outbox shared tenant hot payload graph: %v", got)
	}
}

func TestSplitStoreCloneFailuresPreserveState(t *testing.T) {
	st, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	goodKey := EventKey("tenant-a", "evt-good")
	good := ownershipEvent("evt-good")
	if err := st.UpdateControl(func(data *Snapshot) error {
		data.LedgeredOutbox[goodKey] = good
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	bad := ownershipEvent("evt-bad")
	bad.Payload["bad"] = func() {}
	badKey := EventKey("tenant-a", "evt-bad")
	if err := st.UpdateControl(func(data *Snapshot) error {
		data.LedgeredOutbox[badKey] = bad
		return nil
	}); !errors.Is(err, domain.ErrEventClone) {
		t.Fatalf("UpdateControl error=%v, want ErrEventClone", err)
	}
	if _, err := controlSnapshot(&Snapshot{LedgeredOutbox: map[string]domain.Event{badKey: bad}}); !errors.Is(err, domain.ErrEventClone) {
		t.Fatalf("controlSnapshot error=%v, want ErrEventClone", err)
	}
	if _, err := cloneControl(&Snapshot{LedgeredOutbox: map[string]domain.Event{badKey: bad}}); !errors.Is(err, domain.ErrEventClone) {
		t.Fatalf("cloneControl error=%v, want ErrEventClone", err)
	}
	assertEventEqual(t, st.split.control.LedgeredOutbox[goodKey], ownershipEvent("evt-good"))
	if _, ok := st.split.control.LedgeredOutbox[badKey]; ok {
		t.Fatal("failed UpdateControl retained unsupported event")
	}
}

func TestLegacyUpdateTenantCloneFailurePreservesState(t *testing.T) {
	backend, err := openFileBackend("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	st := NewWithBackend(backend)
	goodKey := EventKey("tenant-a", "evt-legacy")
	good := ownershipEvent("evt-legacy")
	if err := st.Update(func(data *Snapshot) error {
		data.Events[goodKey] = good
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	bad := ownershipEvent("evt-legacy-bad")
	bad.Payload["bad"] = func() {}
	badKey := EventKey("tenant-a", "evt-legacy-bad")
	if err := st.UpdateTenant("tenant-a", HotFirst, func(view *TenantView) error {
		view.Hot.Events[badKey] = bad
		return nil
	}); !errors.Is(err, domain.ErrEventClone) {
		t.Fatalf("UpdateTenant error=%v, want ErrEventClone", err)
	}
	snap, err := st.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertEventEqual(t, snap.Events[goodKey], ownershipEvent("evt-legacy"))
	if _, ok := snap.Events[badKey]; ok {
		t.Fatal("failed legacy UpdateTenant retained unsupported event")
	}
}
