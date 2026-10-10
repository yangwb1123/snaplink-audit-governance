package service

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestQuerySnapshotExcludesLateOccurrenceAndBindsScope(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1700000200, 0).UTC()
	seedChronologyFixture(t, svc, at)
	q := domain.Query{From: at.Add(-time.Second), To: at.Add(5 * time.Second), PageSize: 2}
	first, err := svc.QueryEvents(testCtx, "tenant-a", "reader", q)
	if err != nil || first.Count != 4 || first.NextCursor == "" || first.Snapshot == nil {
		t.Fatalf("page=%+v err=%v", first, err)
	}
	late := testEvent("late-before-cursor", "late-op", at.Add(time.Millisecond))
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, late, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	next := q
	next.Cursor = first.NextCursor
	second, err := svc.QueryEvents(testCtx, "tenant-a", "reader", next)
	if err != nil || second.Count != 4 || second.NextCursor != "" || !reflect.DeepEqual(eventIDs(second.Items), []string{"B1", "B2"}) {
		t.Fatalf("page=%+v err=%v", second, err)
	}
	// Replaying a cursor is deterministic and separately audited.
	repeated, err := svc.QueryEvents(testCtx, "tenant-a", "reader", next)
	if err != nil || !reflect.DeepEqual(eventIDs(repeated.Items), eventIDs(second.Items)) {
		t.Fatal(err)
	}
	fresh, err := svc.QueryEvents(testCtx, "tenant-a", "reader", q)
	if err != nil || fresh.Count != 5 {
		t.Fatalf("count=%d err=%v", fresh.Count, err)
	}
	for _, change := range []struct {
		tenant, actor string
		query         domain.Query
	}{
		{"tenant-b", "reader", next}, {"tenant-a", "other-reader", next},
		{"tenant-a", "reader", func() domain.Query { n := next; n.Outcome = "success"; return n }()},
		{"tenant-a", "reader", func() domain.Query { n := next; n.PageSize = 3; return n }()},
		{"tenant-a", "reader", func() domain.Query { n := next; n.Cursor += "corrupt"; return n }()},
		{"tenant-a", "reader", func() domain.Query { n := next; n.PayloadField = "email"; n.PayloadDigest = "private-digest"; return n }()},
	} {
		result, err := svc.QueryEvents(testCtx, change.tenant, change.actor, change.query)
		if !errors.Is(err, domain.ErrInvalid) || len(result.Items) != 0 {
			t.Fatalf("change=%+v err=%v", change, err)
		}
	}
	if strings.Contains(first.NextCursor, "reader") || strings.Contains(first.NextCursor, "private-digest") {
		t.Fatal("cursor leaks scope/filter")
	}
}

func TestQuerySnapshotSurvivesServiceRestartAndExpires(t *testing.T) {
	svc := testService(t, false)
	now := time.Now().UTC()
	svc.Config.Now = func() time.Time { return now }
	at := time.Unix(1700000200, 0).UTC()
	seedChronologyFixture(t, svc, at)
	q := domain.Query{From: at.Add(-time.Second), To: at.Add(5 * time.Second), PageSize: 2}
	first, err := svc.QueryEvents(testCtx, "tenant-a", "reader", q)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(svc.Store, svc.Config)
	if err != nil {
		t.Fatal(err)
	}
	q.Cursor = first.NextCursor
	if page, err := reopened.QueryEvents(testCtx, "tenant-a", "reader", q); err != nil || page.Count != 4 {
		t.Fatalf("count=%d err=%v", page.Count, err)
	}
	now = now.Add(store.QuerySnapshotLifetime)
	if page, err := reopened.QueryEvents(testCtx, "tenant-a", "reader", q); !errors.Is(err, store.ErrQuerySnapshotUnavailable) || len(page.Items) != 0 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
}

func TestQuerySnapshotRejectsTamperedMembershipAndEvidence(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1700000200, 0).UTC()
	seedChronologyFixture(t, svc, at)
	q := domain.Query{From: at.Add(-time.Second), To: at.Add(5 * time.Second), PageSize: 2}
	first, err := svc.QueryEvents(testCtx, "tenant-a", "reader", q)
	if err != nil {
		t.Fatal(err)
	}
	q.Cursor = first.NextCursor
	if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
		key := store.EventKey("tenant-a", "B1")
		event := data.Events[key]
		event.Payload["value"] = "tampered"
		data.Events[key] = event
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if page, err := svc.QueryEvents(testCtx, "tenant-a", "reader", q); err == nil || len(page.Items) != 0 {
		t.Fatalf("tamper accepted page=%+v err=%v", page, err)
	}
}

type queryRecordingArchive struct {
	archive.Store
	forbiddenID string
	reads       int
}

func (a *queryRecordingArchive) Get(ctx context.Context, key string) ([]byte, error) {
	a.reads++
	if strings.HasSuffix(key, "-"+a.forbiddenID+".json") {
		return nil, errors.New("already served object unavailable")
	}
	return a.Store.Get(ctx, key)
}

func TestQuerySnapshotPageOnlyReadsRequestedArchiveEvidence(t *testing.T) {
	svc := testService(t, true)
	at := time.Unix(1700000200, 0).UTC()
	seedChronologyFixture(t, svc, at)
	q := domain.Query{From: at.Add(-time.Second), To: at.Add(5 * time.Second), PageSize: 2}
	first, err := svc.QueryEvents(testCtx, "tenant-a", "reader", q)
	if err != nil {
		t.Fatal(err)
	}
	// Remove an already-served archived object. A continuation must not scan it
	// again; missing selected evidence, however, must fail closed.
	archive := &queryRecordingArchive{Store: svc.Config.Archive, forbiddenID: first.Items[0].EventID}
	svc.Config.Archive = archive
	q.Cursor = first.NextCursor
	second, err := svc.QueryEvents(testCtx, "tenant-a", "reader", q)
	if err != nil || second.Count != 4 || archive.reads != 2 {
		t.Fatalf("reads=%d page=%+v err=%v", archive.reads, second, err)
	}
}
