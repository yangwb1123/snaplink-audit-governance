package service

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

type mutatingLedgeredPublisher struct {
	err   error
	calls int
}

func (p *mutatingLedgeredPublisher) Publish(_ context.Context, event domain.Event) error {
	p.calls++
	mutateServiceEvent(&event)
	return p.err
}

func serviceOwnershipEvent(id string, at time.Time) domain.Event {
	event := testEvent(id, "op-ownership", at)
	event.Targets = []domain.Target{{Type: "invoice", ID: "inv-1"}}
	event.Payload["value"] = map[string]any{"state": "open"}
	event.ChangedFields["status"] = domain.FieldChange{Before: map[string]any{"state": "open"}, After: []any{map[string]any{"state": "paid"}}}
	return event
}

func mutateServiceEvent(event *domain.Event) {
	event.Payload["resource"] = "mutated"
	event.Payload["value"].(map[string]any)["state"] = "closed"
	event.Actor.Roles[0] = "admin"
	event.Targets[0].ID = "inv-9"
	change := event.ChangedFields["status"]
	change.Before.(map[string]any)["state"] = "closed"
	change.After.([]any)[0].(map[string]any)["state"] = "closed"
	event.ChangedFields["status"] = change
}

func assertStoredEventEqual(t *testing.T, svc *Service, eventID string, want domain.Event) {
	t.Helper()
	got, err := svc.GetEvent("tenant-a", "auditor", eventID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("event mismatch\n got: %+v\nwant: %+v", got, want)
	}
}

func readResultItems(event domain.Event, err error) ([]domain.Event, error) {
	if err != nil {
		return nil, err
	}
	return []domain.Event{event}, nil
}

func queryResultItems(result domain.QueryResult, err error) ([]domain.Event, error) {
	if err != nil {
		return nil, err
	}
	return result.Items, nil
}

func TestIngestClonesMutableEventGraph(t *testing.T) {
	svc := testService(t, false)
	event := serviceOwnershipEvent("evt-ingest-owned", time.Unix(1_700_000_200, 0).UTC())
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err != nil {
		t.Fatal(err)
	}
	mutateServiceEvent(&event)

	got, err := svc.GetEvent("tenant-a", "auditor", receipt.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Payload["resource"] != "invoice" || got.Payload["value"].(map[string]any)["state"] != "open" {
		t.Fatalf("stored payload mutated through caller alias: %+v", got.Payload)
	}
	if got.Actor.Roles[0] != "operator" || got.Targets[0].ID != "inv-1" {
		t.Fatalf("stored actor/targets mutated through caller alias: actor=%+v targets=%+v", got.Actor, got.Targets)
	}
	change := got.ChangedFields["status"]
	if change.Before.(map[string]any)["state"] != "open" || change.After.([]any)[0].(map[string]any)["state"] != "paid" {
		t.Fatalf("stored changed_fields mutated through caller alias: %+v", change)
	}
	verified, err := svc.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !verified.Valid || verified.EventCount != 1 {
		t.Fatalf("integrity result=%+v, want valid single event", verified)
	}
	if got.Hash != receipt.Hash || got.Sequence != receipt.Sequence {
		t.Fatalf("stored chain state changed: event=%+v receipt=%+v", got, receipt)
	}
}

func TestEventReadMethodsReturnOwnedCopies(t *testing.T) {
	modes := []struct {
		name    string
		archive bool
		waitFor string
	}{
		{name: "hot", archive: false, waitFor: domain.StatusLedgered},
		{name: "archived", archive: true, waitFor: domain.StatusArchived},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			svc := testService(t, mode.archive)
			at := time.Unix(1_700_000_210, 0).UTC()
			event := serviceOwnershipEvent("evt-read-owned", at)
			if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, mode.waitFor); err != nil {
				t.Fatal(err)
			}
			baseline, err := svc.GetEvent("tenant-a", "auditor", event.EventID)
			if err != nil {
				t.Fatal(err)
			}
			query := domain.Query{From: at.Add(-time.Minute), To: at.Add(time.Minute), PageSize: 10}
			cases := []struct {
				name string
				read func() ([]domain.Event, error)
			}{
				{name: "GetEvent", read: func() ([]domain.Event, error) {
					return readResultItems(svc.GetEvent("tenant-a", "auditor", event.EventID))
				}},
				{name: "GetEventAcrossTenants", read: func() ([]domain.Event, error) {
					return readResultItems(svc.GetEventAcrossTenants("auditor", event.EventID))
				}},
				{name: "QueryEvents", read: func() ([]domain.Event, error) { return queryResultItems(svc.QueryEvents("tenant-a", "auditor", query)) }},
				{name: "QueryConsoleEvents", read: func() ([]domain.Event, error) {
					return queryResultItems(svc.QueryConsoleEvents("tenant-a", "auditor", query))
				}},
				{name: "OperationTimeline", read: func() ([]domain.Event, error) { return svc.OperationTimeline("tenant-a", "auditor", event.OperationID) }},
				{name: "AggregateTimeline", read: func() ([]domain.Event, error) {
					return svc.AggregateTimeline("tenant-a", "auditor", event.AggregateType, event.AggregateID)
				}},
				{name: "OperationTimelinePage", read: func() ([]domain.Event, error) {
					return queryResultItems(svc.OperationTimelinePage("tenant-a", "auditor", event.OperationID, 10, ""))
				}},
				{name: "AggregateTimelinePage", read: func() ([]domain.Event, error) {
					return queryResultItems(svc.AggregateTimelinePage("tenant-a", "auditor", event.AggregateType, event.AggregateID, 10, ""))
				}},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					items, err := tc.read()
					if err != nil {
						t.Fatal(err)
					}
					if len(items) != 1 {
						t.Fatalf("items=%d, want 1", len(items))
					}
					mutateServiceEvent(&items[0])
					assertStoredEventEqual(t, svc, event.EventID, baseline)
				})
			}
		})
	}
}

func TestPublisherMutationDoesNotAffectLedgerOrOutbox(t *testing.T) {
	svc := testService(t, false)
	publisher := &mutatingLedgeredPublisher{err: errors.New("broker unavailable")}
	svc.Config.LedgeredPublisher = publisher
	event := serviceOwnershipEvent("evt-publish-owned", time.Unix(1_700_000_220, 0).UTC())
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if publisher.calls != 1 {
		t.Fatalf("publish calls=%d, want 1", publisher.calls)
	}
	baseline := mustGetEvent(t, svc, event.EventID)
	pending := pendingLedgeredEvents(t, svc)
	pendingEvent := pending[store.EventKey("tenant-a", event.EventID)]
	if pendingEvent.Payload["resource"] != "invoice" || pendingEvent.Payload["value"].(map[string]any)["state"] != "open" {
		t.Fatalf("outbox event mutated by publisher: %+v", pendingEvent.Payload)
	}
	assertStoredEventEqual(t, svc, event.EventID, baseline)
	verified, err := svc.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !verified.Valid {
		t.Fatalf("integrity result=%+v, want valid", verified)
	}

	publisher.err = nil
	flushed, err := svc.FlushLedgeredOutbox(context.Background(), 10)
	if err != nil || flushed != 1 {
		t.Fatalf("FlushLedgeredOutbox flushed=%d err=%v, want 1 nil", flushed, err)
	}
	if pending := pendingLedgeredEvents(t, svc); len(pending) != 0 {
		t.Fatalf("pending outbox=%v, want empty after successful retry", pending)
	}
	assertStoredEventEqual(t, svc, event.EventID, baseline)
}

func TestPublishLedgeredNowCloneFailureReturnsFalse(t *testing.T) {
	svc := testService(t, false)
	publisher := &mutatingLedgeredPublisher{}
	svc.Config.LedgeredPublisher = publisher
	bad := serviceOwnershipEvent("evt-publish-bad", time.Unix(1_700_000_230, 0).UTC())
	bad.Payload["bad"] = func() {}
	if ok := svc.publishLedgeredNow(context.Background(), bad); ok {
		t.Fatal("publishLedgeredNow succeeded on unsupported event graph")
	}
	if publisher.calls != 0 {
		t.Fatalf("publisher calls=%d, want 0 when clone fails", publisher.calls)
	}
}

func mustGetEvent(t *testing.T, svc *Service, eventID string) domain.Event {
	t.Helper()
	event, err := svc.GetEvent("tenant-a", "auditor", eventID)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func TestIngestRejectsUnsupportedEventGraphWithoutSideEffects(t *testing.T) {
	svc := testService(t, false)
	event := serviceOwnershipEvent("evt-ingest-bad", time.Unix(1_700_000_240, 0).UTC())
	event.Payload["bad"] = func() {}
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if !errors.Is(err, domain.ErrEventClone) {
		t.Fatalf("error=%v, want ErrEventClone", err)
	}
	if receipt != (domain.EventReceipt{}) {
		t.Fatalf("receipt=%+v, want zero receipt on clone failure", receipt)
	}
	snapshot, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Events) != 0 || len(snapshot.Receipts) != 0 || len(snapshot.Streams) != 0 || len(snapshot.LedgeredOutbox) != 0 {
		raw, _ := json.Marshal(snapshot)
		t.Fatalf("failed ingest mutated store: %s", raw)
	}
}
