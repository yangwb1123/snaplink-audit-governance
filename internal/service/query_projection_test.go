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

type queryProjectionFixture struct {
	refs  []store.EventReference
	err   error
	calls int
}

func (p *queryProjectionFixture) SelectQueryReferences(context.Context, string, domain.Query) ([]store.EventReference, error) {
	p.calls++
	return append([]store.EventReference(nil), p.refs...), p.err
}
func (p *queryProjectionFixture) CheckQueryStorage(context.Context) error { return p.err }

func TestQueryProjectionUsesCommittedEvidenceAndFixedCandidates(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1700000200, 0).UTC()
	seedChronologyFixture(t, svc, at)
	refs := []store.EventReference{}
	for _, id := range []string{"B2", "A1", "B1", "A2"} {
		event, err := svc.GetEvent(testCtx, "tenant-a", "", id)
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, eventReference(event))
	}
	projection := &queryProjectionFixture{refs: refs}
	svc.Config.QueryProjection = projection
	q := domain.Query{From: at.Add(-time.Second), To: at.Add(5 * time.Second), PageSize: 2}
	first, err := svc.QueryEvents(testCtx, "tenant-a", "reader", q)
	if err != nil || first.Count != 4 || !reflect.DeepEqual(eventIDs(first.Items), []string{"A1", "A2"}) || first.Snapshot.Source != "clickhouse" {
		t.Fatalf("page=%+v err=%v", first, err)
	}
	// A configured destination's failure does not silently change query source.
	projection.err = errors.New("projection unavailable")
	if page, err := svc.QueryEvents(testCtx, "tenant-a", "reader", q); err == nil || len(page.Items) != 0 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	// Existing membership is fixed and uses truth on resume, not a fresh index
	// query or Kafka checkpoint. Page 2 survives index outage without lying.
	q.Cursor = first.NextCursor
	second, err := svc.QueryEvents(testCtx, "tenant-a", "reader", q)
	if err != nil || second.Count != 4 || !reflect.DeepEqual(eventIDs(second.Items), []string{"B1", "B2"}) || projection.calls != 2 {
		t.Fatalf("page=%+v calls=%d err=%v", second, projection.calls, err)
	}
}

func TestQueryProjectionRejectsPhantomAndChangedCoordinates(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1700000200, 0).UTC()
	seedChronologyFixture(t, svc, at)
	event, err := svc.GetEvent(testCtx, "tenant-a", "", "A1")
	if err != nil {
		t.Fatal(err)
	}
	projection := &queryProjectionFixture{}
	svc.Config.QueryProjection = projection
	q := domain.Query{From: at.Add(-time.Second), To: at.Add(5 * time.Second), PageSize: 2}
	for _, change := range []func(*store.EventReference){
		func(r *store.EventReference) { r.EventID = "phantom" },
		func(r *store.EventReference) { r.Hash = "other-hash" },
		func(r *store.EventReference) { r.OccurredAt = r.OccurredAt.Add(time.Second) },
	} {
		reference := eventReference(event)
		change(&reference)
		projection.refs = []store.EventReference{reference}
		if page, err := svc.QueryEvents(testCtx, "tenant-a", "reader", q); !errors.Is(err, domain.ErrConflict) || len(page.Items) != 0 {
			t.Fatalf("page=%+v err=%v", page, err)
		}
	}
	projection.refs = []store.EventReference{eventReference(event)}
	q.ActorID = "other-reader"
	if page, err := svc.QueryEvents(testCtx, "tenant-a", "reader", q); !errors.Is(err, domain.ErrConflict) || len(page.Items) != 0 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	// Protected search digests are deliberately absent from ClickHouse; route
	// them to Ledger/WORM, not a broken cross-format or ciphertext fallback.
	q.ActorID = ""
	q.PayloadField = "email"
	q.PayloadDigest = "never-logged"
	before := projection.calls
	page, err := svc.QueryEvents(testCtx, "tenant-a", "reader", q)
	if err != nil || page.Count != 0 || projection.calls != before || page.Snapshot.Source != "ledger" {
		t.Fatalf("page=%+v calls=%d err=%v", page, projection.calls, err)
	}
}
