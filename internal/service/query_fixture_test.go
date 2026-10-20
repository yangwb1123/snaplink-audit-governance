package service

import (
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// Query scope/membership and Store.mu exclusion are not filesystem throughput
// tests. Use the production in-memory backend for their control/trail writes;
// archive=true still uses real FileStore objects. File restart/expiry and
// coordinate-only archive continuation retain testService's file-backed state.
func queryMemoryService(t *testing.T, archive bool) *Service {
	t.Helper()
	st, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	archiveDir := ""
	if archive {
		archiveDir = t.TempDir()
	}
	svc, err := New(st, Config{ArchiveDir: archiveDir, SegmentSize: 2, SigningSecret: "test-secret", Now: func() time.Time { return pinnedNow }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	seedTestDomain(t, svc)
	return svc
}

func TestQueryMemoryFixturePreservesCommittedEvidenceAndReadFacts(t *testing.T) {
	file, memory := testService(t, false), queryMemoryService(t, false)
	t.Cleanup(func() { _ = file.Store.Close() })
	at := pinnedNow.Add(time.Minute)
	for _, svc := range []*Service{file, memory} {
		seedChronologyFixture(t, svc, at)
	}
	left, right := leaseSnapshot(t, file), leaseSnapshot(t, memory)
	if !reflect.DeepEqual(left.Events, right.Events) || !reflect.DeepEqual(left.Receipts, right.Receipts) || !reflect.DeepEqual(left.Schemas, right.Schemas) || !reflect.DeepEqual(left.Streams, right.Streams) || !reflect.DeepEqual(left.Segments, right.Segments) || !reflect.DeepEqual(left.Checkpoints, right.Checkpoints) {
		t.Fatal("query fixture changed real ingest evidence, exact Schema or chain")
	}
	query := domain.Query{From: at.Add(-time.Second), To: at.Add(5 * time.Second), PageSize: 2}
	firstFile, err := file.QueryEvents(testCtx, "tenant-a", "reader", query)
	if err != nil {
		t.Fatal("file-backed query fixture failed")
	}
	firstMemory, err := memory.QueryEvents(testCtx, "tenant-a", "reader", query)
	if err != nil || firstFile.Count != 4 || firstMemory.Count != 4 || firstFile.NextCursor == "" || firstMemory.NextCursor == "" || !reflect.DeepEqual(firstFile.Items, firstMemory.Items) || !reflect.DeepEqual(firstFile.Snapshot, firstMemory.Snapshot) {
		t.Fatal("query fixtures differ in first-page membership or observation")
	}
	for _, item := range []struct {
		svc    *Service
		cursor string
	}{{file, firstFile.NextCursor}, {memory, firstMemory.NextCursor}} {
		next := query
		next.Cursor = item.cursor
		page, err := item.svc.QueryEvents(testCtx, "tenant-a", "reader", next)
		if err != nil || page.Count != 4 || page.NextCursor != "" || !reflect.DeepEqual(eventIDs(page.Items), []string{"B1", "B2"}) {
			t.Fatal("query fixture did not execute authenticated continuation")
		}
		facts := readFacts(t, item.svc)
		if len(facts) != 2 {
			t.Fatal("query fixture lost or duplicated a real read append")
		}
		for _, fact := range facts {
			if fact.Action != domain.AdminActionEventRead || fact.TenantID != "tenant-a" || fact.Actor != "reader" || fact.TargetType != "query" {
				t.Fatal("query fixture changed read fact scope or identity")
			}
		}
	}
}
