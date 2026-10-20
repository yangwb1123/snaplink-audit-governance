package service

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
)

func TestTimelineFixtureMemoryObjectsPreserveFileEvidenceAndReadFacts(t *testing.T) {
	memory, _, _ := budgetTimelineFixture(t)
	file, _, objects := budgetTimelineFixture(t)
	actual := &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")}
	// Copy exact already-ingested object bytes into actual private FileStore
	// objects. This isolates the adapter comparison from ingest randomness and
	// still exercises real file Put/Get, canonical decoding and authentication.
	for _, receipt := range leaseSnapshot(t, file).Receipts {
		key := eventArchiveKey(receipt.TenantID, receipt.StreamID, receipt.Sequence, receipt.EventID)
		raw, err := objects.Store.Get(testCtx, key)
		if err != nil || actual.Put(testCtx, key, raw) != nil {
			t.Fatal("could not prepare exact file-backed timeline objects")
		}
	}
	objects.Store = actual
	left, right := leaseSnapshot(t, memory), leaseSnapshot(t, file)
	if !reflect.DeepEqual(left.Events, right.Events) || !reflect.DeepEqual(left.Receipts, right.Receipts) || !reflect.DeepEqual(left.Schemas, right.Schemas) || !reflect.DeepEqual(left.Streams, right.Streams) || !reflect.DeepEqual(left.Segments, right.Segments) || !reflect.DeepEqual(left.Checkpoints, right.Checkpoints) {
		t.Fatal("timeline fixture adapters changed original evidence, exact Schema or chain")
	}
	for _, kind := range []string{"operation", "aggregate"} {
		var previous domain.QueryResult
		for i, svc := range []*Service{memory, file} {
			first, err := readTimelinePage(testCtx, svc, kind, "tenant-a", "reader", 1, "")
			if err != nil || first.Count != 2 || len(first.Items) != 1 || first.Items[0].EventID != "timeline-a" || first.NextCursor == "" {
				t.Fatal("timeline fixture lost the authenticated first page")
			}
			second, err := readTimelinePage(testCtx, svc, kind, "tenant-a", "reader", 1, first.NextCursor)
			if err != nil || second.Count != 2 || len(second.Items) != 1 || second.Items[0].EventID != "timeline-b" || second.NextCursor != "" {
				t.Fatal("timeline fixture lost live continuation or total count")
			}
			if i == 0 {
				previous = first
			} else if !reflect.DeepEqual(first, previous) {
				t.Fatal("file and memory sources returned different authenticated timeline pages")
			}
		}
	}
	for _, svc := range []*Service{memory, file} {
		facts := readFacts(t, svc)
		if len(facts) != 4 {
			t.Fatal("timeline fixture did not append exactly one fact for each page")
		}
		targets := map[string]int{}
		for _, fact := range facts {
			if fact.Action != domain.AdminActionEventRead || fact.TenantID != "tenant-a" || fact.Actor != "reader" {
				t.Fatal("timeline fixture changed the read fact scope or actor")
			}
			targets[fact.TargetType+"|"+fact.TargetID+"|"+fact.Detail]++
		}
		if !reflect.DeepEqual(targets, map[string]int{"operation|timeline-operation|timeline": 2, "aggregate|inv-1|invoice": 2}) {
			t.Fatal("timeline fixture changed original read target encoding")
		}
	}
}
