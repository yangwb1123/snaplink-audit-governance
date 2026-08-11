package service

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// testTime matches testService's injected Now() so seeded fixtures carry the
// same timestamps the service would produce.
var testTime = time.Unix(1_700_000_000, 0).UTC()

// testCheckpoint builds a minimal Checkpoint for seeded snapshot fixtures.
func testCheckpoint(tenantID, streamID, root string) domain.Checkpoint {
	return domain.Checkpoint{
		ID: newID("ck"), TenantID: tenantID, StreamID: streamID, Sequence: 1,
		MerkleRoot: root, Signature: "sig", Algorithm: "hmac", CreatedAt: testTime,
	}
}

// testSegment builds a minimal Segment for seeded snapshot fixtures. The
// struct fields mirror what the map key claims (TenantID/StreamID), exactly
// like segments produced by the ingest path.
func testSegment(tenantID, streamID string, seq int64) domain.Segment {
	return domain.Segment{
		TenantID: tenantID, StreamID: streamID,
		FirstSequence: seq, LastSequence: seq, EventCount: 1,
		MerkleRoot: "root-" + tenantID + "-" + streamID,
		CreatedAt:  testTime,
	}
}

// TestCreateAggregateCheckpointExactTenantScoping is AC-2: the aggregate
// evidence trail for tenant "a" must include only keys that parse as
// exactly ("a", rest, ok). The colliding key "a\x1fb\x1fs" (tenant "a\x1fb"'s
// stream "s", byte-identical to tenant "a"'s stream "b\x1fs") must be
// excluded, as must tenant "b" and the plain-prefix "ab".
func TestCreateAggregateCheckpointExactTenantScoping(t *testing.T) {
	svc := testService(t, false)
	leakedRoot := "root-leaked"
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Checkpoints[store.StreamKey("a", "s1")] = []domain.Checkpoint{testCheckpoint("a", "s1", "root-s1")}
		data.Checkpoints[store.StreamKey("a", "s2")] = []domain.Checkpoint{testCheckpoint("a", "s2", "root-s2")}
		data.Checkpoints[store.StreamKey("a", "")] = []domain.Checkpoint{testCheckpoint("a", "", "root-empty")}
		data.Checkpoints[store.StreamKey("b", "s1")] = []domain.Checkpoint{testCheckpoint("b", "s1", "root-b")}
		data.Checkpoints["a\x1fb\x1fs"] = []domain.Checkpoint{testCheckpoint("a\x1fb", "s", leakedRoot)}
		data.Checkpoints[store.StreamKey("ab", "s1")] = []domain.Checkpoint{testCheckpoint("ab", "s1", "root-ab")}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateAggregateCheckpoint("a"); err != nil {
		t.Fatal(err)
	}
	var agg domain.AggregateCheckpoint
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		items := data.AggregateCheckpoints
		if len(items) == 0 {
			t.Fatal("no aggregate checkpoint was created")
		}
		agg = items[len(items)-1]
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if agg.StreamCount != 3 {
		t.Fatalf("StreamCount = %d, want 3 (s1, s2, empty-rest)", agg.StreamCount)
	}
	for _, root := range []string{leakedRoot, "root-b", "root-ab"} {
		for _, got := range agg.StreamRoots {
			if got == root {
				t.Fatalf("aggregate checkpoint absorbed foreign root %q", root)
			}
		}
	}
}

// TestVerifyIntegrityExactTenantAndStreamScoping is AC-3: segment selection
// must be exact-component equality. Fabricated seeds cannot pass the content
// checks, so assertions target selection counts, not result.Valid.
func TestVerifyIntegrityExactTenantAndStreamScoping(t *testing.T) {
	svc := testService(t, false)
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Segments[store.StreamKey("a", "s1")] = []domain.Segment{testSegment("a", "s1", 1)}
		data.Segments["a\x1fb\x1fs"] = []domain.Segment{testSegment("a\x1fb", "s", 2)}
		data.Segments[store.StreamKey("b", "s1")] = []domain.Segment{testSegment("b", "s1", 3)}
		data.Segments[store.StreamKey("a", "")] = []domain.Segment{testSegment("a", "", 4)}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	full, err := svc.VerifyIntegrity("a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if full.SegmentCount != 2 {
		t.Fatalf("VerifyIntegrity(\"a\",\"\").SegmentCount = %d, want 2 (s1 + empty-rest; leak and b excluded)", full.SegmentCount)
	}

	scoped, err := svc.VerifyIntegrity("a", "", "s1")
	if err != nil {
		t.Fatal(err)
	}
	if scoped.SegmentCount != 1 {
		t.Fatalf("VerifyIntegrity(\"a\",\"s1\").SegmentCount = %d, want 1", scoped.SegmentCount)
	}

	foreign, err := svc.VerifyIntegrity("b", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if foreign.SegmentCount != 1 {
		t.Fatalf("VerifyIntegrity(\"b\",\"\").SegmentCount = %d, want 1 (only b's own segment)", foreign.SegmentCount)
	}
}

// TestArchivePendingExactTenantScoping is AC-3 for the archive path: only
// segments whose key parses as exactly ("a", rest, ok) are archived under
// tenant "a"'s run. The malicious tenant "a\x1fb"'s segment (struct fields
// TenantID "a\x1fb", StreamID "s") must not be archived at all.
func TestArchivePendingExactTenantScoping(t *testing.T) {
	svc := testService(t, true)
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Segments[store.StreamKey("a", "s1")] = []domain.Segment{testSegment("a", "s1", 1)}
		data.Segments[store.StreamKey("b", "s1")] = []domain.Segment{testSegment("b", "s1", 2)}
		data.Segments["a\x1fb\x1fs"] = []domain.Segment{testSegment("a\x1fb", "s", 3)}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ArchivePending("a"); err != nil {
		t.Fatal(err)
	}
	archiveRoot := svc.Config.ArchiveDir

	assertManifests := func(tenantDir string) int {
		t.Helper()
		dir := filepath.Join(archiveRoot, "segments", tenantDir)
		count := 0
		err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			if filepath.Ext(entry.Name()) != ".json" {
				t.Fatalf("unexpected file %s in %s", entry.Name(), path)
			}
			count++
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
		return count
	}

	if n := assertManifests("a"); n != 1 {
		t.Fatalf("tenant a archive has %d manifests, want 1 (only stream s1)", n)
	}
	// Other tenants' segments must not be archived under a's run at all.
	for _, foreign := range []string{"b", "a_b"} { // safeName renders 0x1F as '_'
		if _, err := os.Stat(filepath.Join(archiveRoot, "segments", foreign)); !os.IsNotExist(err) {
			t.Fatalf("foreign tenant dir %q exists after a's archive run: %v", foreign, err)
		}
	}
}
