package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// countingSigner wraps a Signer and counts Verify calls so tests can bound
// the verification work VerifyIntegrity performs on retained aggregate
// records (AC-2).
type countingSigner struct {
	Signer
	verifyCalls int
}

func (c *countingSigner) Verify(ctx context.Context, data []byte, signature string) (bool, error) {
	c.verifyCalls++
	return c.Signer.Verify(ctx, data, signature)
}

// erroringSigner fails Sign on demand so the signer-failure path of
// CreateAggregateCheckpoint can be exercised (FR-6).
type erroringSigner struct {
	Signer
}

func (e erroringSigner) Sign(context.Context, []byte) (string, error) {
	return "", fmt.Errorf("signer unavailable")
}

// tenantAggregateCount returns how many aggregate checkpoints the snapshot
// holds for the tenant.
func tenantAggregateCount(t *testing.T, svc *Service, tenantID string) int {
	t.Helper()
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, item := range snap.AggregateCheckpoints {
		if item.TenantID == tenantID {
			count++
		}
	}
	return count
}

// TestCreateAggregateCheckpointDedupNoLedgerChange is AC-1: two consecutive
// calls with no ledger change append exactly one record; a changed ledger
// appends a new record with a different root; an unchanged tenant never
// suppresses another tenant's append.
func TestCreateAggregateCheckpointDedupNoLedgerChange(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	first := testEvent("ac1-a", "", at)
	first.AggregateType = "invoice"
	first.AggregateID = "inv-a"
	first.OperationID = ""
	first.IdempotencyKey = "ac1-a"
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, first, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	second := testEvent("ac1-b", "", at.Add(time.Second))
	second.AggregateType = "invoice"
	second.AggregateID = "inv-b"
	second.OperationID = ""
	second.IdempotencyKey = "ac1-b"
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, second, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if err := svc.SealPendingSegments(testCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}

	// First call appends exactly one record.
	if err := svc.CreateAggregateCheckpoint(testCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	firstRecord := snap.AggregateCheckpoints[len(snap.AggregateCheckpoints)-1]
	if firstRecord.TenantID != "tenant-a" || firstRecord.Root == "" || firstRecord.Signature == "" {
		t.Fatalf("first record malformed: %+v", firstRecord)
	}
	var roots []string
	for key, checkpoints := range snap.Checkpoints {
		if tenant, _, ok := store.SplitTenantKey(key); ok && tenant == "tenant-a" {
			roots = append(roots, checkpoints[len(checkpoints)-1].MerkleRoot)
		}
	}
	sort.Strings(roots)
	if want := merkleRoot(roots); firstRecord.Root != want {
		t.Fatalf("root=%q, want merkle root %q of %v", firstRecord.Root, want, roots)
	}

	// Second call with no ledger change: dedup skip, success, still one record.
	if err := svc.CreateAggregateCheckpoint(testCtx, "tenant-a"); err != nil {
		t.Fatalf("dedup skip must return success, got %v", err)
	}
	if got := tenantAggregateCount(t, svc, "tenant-a"); got != 1 {
		t.Fatalf("records after dedup call=%d, want 1 (exactly one across both calls)", got)
	}

	// Control leg: a changed ledger appends a record with a different root.
	third := testEvent("ac1-c", "", at.Add(2*time.Second))
	third.AggregateType = "invoice"
	third.AggregateID = "inv-c"
	third.OperationID = ""
	third.IdempotencyKey = "ac1-c"
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, third, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if err := svc.SealPendingSegments(testCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateAggregateCheckpoint(testCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	snap, err = svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	control := snap.AggregateCheckpoints[len(snap.AggregateCheckpoints)-1]
	if got := tenantAggregateCount(t, svc, "tenant-a"); got != 2 {
		t.Fatalf("records after control leg=%d, want 2", got)
	}
	if control.Root == firstRecord.Root {
		t.Fatal("control leg must append a record with a different root")
	}

	// Isolation leg: tenant-b appends while tenant-a is unchanged.
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Checkpoints[store.StreamKey("tenant-b", "s1")] = []domain.Checkpoint{testCheckpoint("tenant-b", "s1", "root-b1")}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	aBefore := tenantAggregateCount(t, svc, "tenant-a")
	if err := svc.CreateAggregateCheckpoint(testCtx, "tenant-b"); err != nil {
		t.Fatal(err)
	}
	if got := tenantAggregateCount(t, svc, "tenant-b"); got != 1 {
		t.Fatalf("tenant-b records=%d, want 1 (unchanged tenant-a must not suppress b's append)", got)
	}
	if got := tenantAggregateCount(t, svc, "tenant-a"); got != aBefore {
		t.Fatalf("tenant-a records changed by tenant-b's call: %d -> %d", aBefore, got)
	}
}

// TestCreateAggregateCheckpointNoSaveWhenIdle pins FR-2 at the service
// level: a dedup skip and a zero-checkpoint tenant must not reach the
// backend Save at all, while an append writes exactly once.
func TestCreateAggregateCheckpointNoSaveWhenIdle(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Checkpoints[store.StreamKey("tenant-a", "s1")] = []domain.Checkpoint{testCheckpoint("tenant-a", "s1", "root-s1")}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Zero-checkpoint tenant: no record, no Save.
	backend.saves = 0
	if err := svc.CreateAggregateCheckpoint(testCtx, "tenant-c"); err != nil {
		t.Fatal(err)
	}
	if backend.saves != 0 {
		t.Fatalf("zero-checkpoint tenant issued %d Saves, want 0", backend.saves)
	}

	// First append writes exactly once.
	backend.saves = 0
	if err := svc.CreateAggregateCheckpoint(testCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if backend.saves != 1 {
		t.Fatalf("append issued %d Saves, want 1", backend.saves)
	}

	// Dedup skip: no Save.
	backend.saves = 0
	if err := svc.CreateAggregateCheckpoint(testCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if backend.saves != 0 {
		t.Fatalf("dedup skip issued %d Saves, want 0", backend.saves)
	}
}

// TestSealPendingSegmentsNoWriteWhenQuiet pins FR-2 for the seal path: a
// quiet tenant (no pending hashes) issues no Save; a tenant with pending
// hashes writes exactly once and seals its checkpoints.
func TestSealPendingSegmentsNoWriteWhenQuiet(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("seal-q1", "op-q", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("seal-q2", "op-q", at.Add(time.Second)), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	// Pending hashes exist now; a first seal pass must write once.
	backend.saves = 0
	if err := svc.SealPendingSegments(testCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if backend.saves != 1 {
		t.Fatalf("seal pass with pending hashes issued %d Saves, want 1", backend.saves)
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sealed := 0
	for key, checkpoints := range snap.Checkpoints {
		if tenant, _, ok := store.SplitTenantKey(key); ok && tenant == "tenant-a" && len(checkpoints) > 0 {
			sealed++
		}
	}
	if sealed == 0 {
		t.Fatal("seal pass must create stream checkpoints")
	}

	// Quiet pass (nothing pending): zero Saves.
	backend.saves = 0
	if err := svc.SealPendingSegments(testCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if backend.saves != 0 {
		t.Fatalf("quiet seal pass issued %d Saves, want 0", backend.saves)
	}
}

// TestCreateAggregateCheckpointSignerFailureSurfaces pins FR-6: a signer
// failure on the append path still surfaces as an error (worker logs
// aggregate_checkpoint_error) and commits nothing.
func TestCreateAggregateCheckpointSignerFailureSurfaces(t *testing.T) {
	svc := testService(t, false)
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Checkpoints[store.StreamKey("tenant-a", "s1")] = []domain.Checkpoint{testCheckpoint("tenant-a", "s1", "root-s1")}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	svc.Config.Signer = erroringSigner{Signer: svc.Config.Signer}
	err := svc.CreateAggregateCheckpoint(testCtx, "tenant-a")
	if err == nil {
		t.Fatal("CreateAggregateCheckpoint must surface the signer failure")
	}
	if got := tenantAggregateCount(t, svc, "tenant-a"); got != 0 {
		t.Fatalf("records=%d after signer failure, want 0 (nothing committed)", got)
	}
}

// TestCreateAggregateCheckpointDedupSurvivesConflictRetry is FR-3 leg A: a
// Save conflict on the append path re-runs the closure on the fresh
// snapshot and the retried closure appends exactly one record (no duplicate
// append under race).
func TestCreateAggregateCheckpointDedupSurvivesConflictRetry(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Checkpoints[store.StreamKey("tenant-a", "s1")] = []domain.Checkpoint{testCheckpoint("tenant-a", "s1", "root-s1")}
		data.Checkpoints[store.StreamKey("tenant-a", "s2")] = []domain.Checkpoint{testCheckpoint("tenant-a", "s2", "root-s2")}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateAggregateCheckpoint(testCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	// Change one stream's root: the next candidate differs from the last
	// record, so the append must survive one forced conflict retry.
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Checkpoints[store.StreamKey("tenant-a", "s2")] = []domain.Checkpoint{testCheckpoint("tenant-a", "s2", "root-s2-new")}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	backend.conflicts = 1
	backend.saves = 0
	if err := svc.CreateAggregateCheckpoint(testCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if backend.saves != 2 {
		t.Fatalf("saves=%d, want 2 (1 failed + 1 committed retry)", backend.saves)
	}
	if got := tenantAggregateCount(t, svc, "tenant-a"); got != 2 {
		t.Fatalf("records=%d, want 2 (exactly one record from the retried closure)", got)
	}
}

// stagedConflictBackend serves a scripted snapshot per LoadForUpdate attempt
// and always conflicts on Save. It simulates a concurrent writer that wins
// the race between our LoadForUpdate and Save (FR-3 leg B).
type stagedConflictBackend struct {
	loads []*store.Snapshot // snapshot served per attempt, last one repeated
	saves int
}

func (b *stagedConflictBackend) Load() (*store.Snapshot, error) { return b.loads[0], nil }

func (b *stagedConflictBackend) LoadForUpdate() (*store.Snapshot, error) {
	if len(b.loads) > 1 {
		data := b.loads[0]
		b.loads = b.loads[1:]
		return cloneTestSnapshot(data)
	}
	return cloneTestSnapshot(b.loads[0])
}

func (b *stagedConflictBackend) Save(*store.Snapshot) error {
	b.saves++
	return store.ErrSnapshotConflict
}

// TestCreateAggregateCheckpointDedupAfterConcurrentWinner is FR-3 leg B: the
// concurrent writer committed the identical candidate between our load and
// save; the retried closure must dedup against the winner's record instead
// of appending a duplicate. A skipped interval under race is acceptable
// (periodic evidence); a duplicate append is not.
func TestCreateAggregateCheckpointDedupAfterConcurrentWinner(t *testing.T) {
	// The winner's committed snapshot: one checkpoint and the candidate
	// record the loser will compute (same root, same deterministic HMAC).
	winner := store.NewSnapshot()
	winner.Checkpoints[store.StreamKey("tenant-a", "s1")] = []domain.Checkpoint{testCheckpoint("tenant-a", "s1", "root-s1")}
	signer := hmacSigner{secret: "test-secret"}
	cp := winner.Checkpoints[store.StreamKey("tenant-a", "s1")][0]
	refs := []domain.CheckpointRef{{StreamID: cp.StreamID, CheckpointID: cp.ID, Sequence: cp.Sequence, MerkleRoot: cp.MerkleRoot}}
	roots := aggregateRoots(refs)
	candidate := domain.AggregateCheckpoint{
		ID: "aggregate-winner", TenantID: "tenant-a", StreamCount: 1, Root: merkleRoot(roots),
		Algorithm: signer.Algorithm(), CreatedAt: testTime, StreamRoots: roots,
		AttestationVersion: aggregateAttestationVersion, CheckpointRefs: refs,
	}
	signature, err := signer.Sign(testCtx, aggregateAttestationPayload(candidate))
	if err != nil {
		t.Fatal(err)
	}
	candidate.Signature = signature
	winner.AggregateCheckpoints = []domain.AggregateCheckpoint{candidate}

	// The loser's first load: same checkpoint, no aggregate record yet.
	first := store.NewSnapshot()
	first.Checkpoints[store.StreamKey("tenant-a", "s1")] = []domain.Checkpoint{testCheckpoint("tenant-a", "s1", "root-s1")}

	backend := &stagedConflictBackend{loads: []*store.Snapshot{first, winner}}
	svc := newServiceOn(t, store.NewWithBackend(backend))
	if err := svc.CreateAggregateCheckpoint(testCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if backend.saves != 1 {
		t.Fatalf("saves=%d, want 1 (only the failed attempt; the retried closure must skip)", backend.saves)
	}
	// The loser never committed: the winner's record is the only one.
	if got := tenantAggregateCount(t, svc, "tenant-a"); got != 1 {
		t.Fatalf("records=%d, want 1 (no duplicate append under race)", got)
	}
}

// TestTrimAggregateCheckpoints pins the trim contract: order-preserving
// rebuild, per-tenant isolation, newest-n retention, no-copy early return.
func TestTrimAggregateCheckpoints(t *testing.T) {
	mk := func(tenant string, n int) domain.AggregateCheckpoint {
		return domain.AggregateCheckpoint{ID: fmt.Sprintf("%s-%d", tenant, n), TenantID: tenant, Root: fmt.Sprintf("root-%s-%d", tenant, n), CreatedAt: testTime}
	}
	items := []domain.AggregateCheckpoint{mk("a", 1), mk("b", 1), mk("a", 2), mk("b", 2), mk("a", 3), mk("a", 4)}

	// Under the cap: unchanged, no copy.
	same, changed := trimAggregateCheckpoints(items, "a", 4)
	if changed || len(same) != len(items) {
		t.Fatalf("under-cap trim changed=%v len=%d, want unchanged", changed, len(same))
	}

	// Over the cap: newest 2 of tenant a kept, order preserved, b untouched.
	trimmed, changed := trimAggregateCheckpoints(items, "a", 2)
	if !changed {
		t.Fatal("over-cap trim must report changed")
	}
	want := []string{"b-1", "b-2", "a-3", "a-4"}
	if len(trimmed) != len(want) {
		t.Fatalf("trimmed len=%d, want %d", len(trimmed), len(want))
	}
	for i := range want {
		if trimmed[i].ID != want[i] {
			t.Fatalf("trimmed[%d]=%s, want %s (order or membership wrong: %v)", i, trimmed[i].ID, want[i], ids(trimmed))
		}
	}

	// Cap 1: only the newest survives; the floor keeps the anchor record.
	floor, changed := trimAggregateCheckpoints(items, "a", 1)
	if !changed || len(floor) != 3 {
		t.Fatalf("floor trim changed=%v len=%d, want 3", changed, len(floor))
	}
	if floor[len(floor)-1].ID != "a-4" {
		t.Fatalf("newest record dropped: %v", ids(floor))
	}

	// Tenant with no records: untouched.
	none, changed := trimAggregateCheckpoints(items, "z", 1)
	if changed || len(none) != len(items) {
		t.Fatalf("absent-tenant trim changed=%v, want untouched", changed)
	}
}

func ids(items []domain.AggregateCheckpoint) []string {
	out := make([]string, len(items))
	for i, item := range items {
		out[i] = item.ID
	}
	return out
}

func boundAggregateFixture(t *testing.T) (*Service, domain.AggregateCheckpoint) {
	t.Helper()
	svc := testService(t, false)
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Checkpoints[store.StreamKey("tenant-a", "stream-a")] = []domain.Checkpoint{testCheckpoint("tenant-a", "stream-a", "root-a")}
		data.Checkpoints[store.StreamKey("tenant-a", "stream-b")] = []domain.Checkpoint{testCheckpoint("tenant-a", "stream-b", "root-b")}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateAggregateCheckpoint(testCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return svc, snapshot.AggregateCheckpoints[len(snapshot.AggregateCheckpoints)-1]
}

// TestAggregateAttestationBoundFields exercises the complete mutation matrix
// at the service boundary. Every mutable aggregate field must either fail
// lineage validation or invalidate the signed canonical payload.
func TestAggregateAttestationBoundFields(t *testing.T) {
	_, aggregate := boundAggregateFixture(t)
	if aggregate.AttestationVersion != aggregateAttestationVersion || len(aggregate.CheckpointRefs) != 2 {
		t.Fatalf("aggregate lacks bound lineage: %+v", aggregate)
	}
	if aggregate.StreamRoots[0] != aggregate.CheckpointRefs[0].MerkleRoot || aggregate.StreamRoots[1] != aggregate.CheckpointRefs[1].MerkleRoot {
		t.Fatalf("roots and references are not aligned: %+v", aggregate)
	}

	mutations := []struct {
		name   string
		mutate func(*domain.AggregateCheckpoint)
	}{
		{"tenant", func(a *domain.AggregateCheckpoint) { a.TenantID = "tenant-b" }},
		{"stream count", func(a *domain.AggregateCheckpoint) { a.StreamCount++ }},
		{"algorithm", func(a *domain.AggregateCheckpoint) { a.Algorithm = "other" }},
		{"root", func(a *domain.AggregateCheckpoint) { a.Root = "tampered-root" }},
		{"stream root", func(a *domain.AggregateCheckpoint) { a.StreamRoots[0] = "tampered-root" }},
		{"checkpoint id", func(a *domain.AggregateCheckpoint) { a.CheckpointRefs[0].CheckpointID = "other" }},
		{"stream id", func(a *domain.AggregateCheckpoint) { a.CheckpointRefs[0].StreamID = "other" }},
		{"sequence", func(a *domain.AggregateCheckpoint) { a.CheckpointRefs[0].Sequence++ }},
		{"reference root", func(a *domain.AggregateCheckpoint) { a.CheckpointRefs[0].MerkleRoot = "tampered-root" }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			svc, _ := boundAggregateFixture(t)
			if err := svc.Store.Update(func(data *store.Snapshot) error {
				mutation.mutate(&data.AggregateCheckpoints[0])
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "tester", "")
			if err != nil {
				t.Fatal(err)
			}
			if result.Valid || len(result.Errors) == 0 {
				t.Fatalf("tampered aggregate passed: %+v", result)
			}
		})
	}
}

func TestAggregateAttestationRejectsChangedCheckpoint(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*domain.Checkpoint)
	}{
		{"id", func(c *domain.Checkpoint) { c.ID = "changed" }},
		{"tenant", func(c *domain.Checkpoint) { c.TenantID = "tenant-b" }},
		{"stream", func(c *domain.Checkpoint) { c.StreamID = "changed" }},
		{"sequence", func(c *domain.Checkpoint) { c.Sequence++ }},
		{"root", func(c *domain.Checkpoint) { c.MerkleRoot = "changed" }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			svc, aggregate := boundAggregateFixture(t)
			key := store.StreamKey("tenant-a", aggregate.CheckpointRefs[0].StreamID)
			if err := svc.Store.Update(func(data *store.Snapshot) error {
				mutation.mutate(&data.Checkpoints[key][0])
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "tester", "")
			if err != nil {
				t.Fatal(err)
			}
			if result.Valid || len(result.Errors) == 0 {
				t.Fatalf("changed checkpoint passed: %+v", result)
			}
		})
	}
}

func TestAggregateAttestationRejectsCrossTenantRelabel(t *testing.T) {
	svc, aggregate := boundAggregateFixture(t)
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		// Give the target tenant the same roots and checkpoint identities. A
		// root-only verifier would accept the copied record after relabeling.
		for _, ref := range aggregate.CheckpointRefs {
			data.Checkpoints[store.StreamKey("tenant-b", ref.StreamID)] = []domain.Checkpoint{{
				ID: ref.CheckpointID, TenantID: "tenant-b", StreamID: ref.StreamID,
				Sequence: ref.Sequence, MerkleRoot: ref.MerkleRoot,
			}}
		}
		copied := aggregate
		copied.TenantID = "tenant-b"
		data.AggregateCheckpoints = []domain.AggregateCheckpoint{copied}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err := svc.VerifyIntegrity(testCtx, "tenant-b", "tester", "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid || !strings.Contains(strings.Join(result.Errors, " "), "root/signature") {
		t.Fatalf("cross-tenant relabel passed or lacked signature diagnostic: %+v", result)
	}
}

func TestLegacyAggregateIsInvalidCompatibilityEvidence(t *testing.T) {
	svc := testService(t, false)
	root := merkleRoot([]string{"legacy-root"})
	signature, err := svc.Config.Signer.Sign(testCtx, []byte(root))
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.AggregateCheckpoints = []domain.AggregateCheckpoint{{
			ID: "legacy", TenantID: "tenant-a", StreamCount: 1, Root: root,
			Signature: signature, Algorithm: svc.Config.Signer.Algorithm(),
			StreamRoots: []string{"legacy-root"},
		}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "tester", "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid || !strings.Contains(strings.Join(result.Errors, " "), "legacy") {
		t.Fatalf("legacy evidence was accepted without compatibility diagnostic: %+v", result)
	}
}

func TestMalformedBoundAggregateWithoutReferencesIsInvalid(t *testing.T) {
	svc := testService(t, false)
	aggregate := domain.AggregateCheckpoint{
		ID: "malformed-bound", TenantID: "tenant-a", StreamCount: 0,
		Root: merkleRoot(nil), Algorithm: svc.Config.Signer.Algorithm(),
		AttestationVersion: aggregateAttestationVersion,
	}
	signature, err := svc.Config.Signer.Sign(testCtx, aggregateAttestationPayload(aggregate))
	if err != nil {
		t.Fatal(err)
	}
	aggregate.Signature = signature
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.AggregateCheckpoints = []domain.AggregateCheckpoint{aggregate}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "tester", "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid || !strings.Contains(strings.Join(result.Errors, " "), "reference mismatch") {
		t.Fatalf("malformed bound evidence was accepted without diagnostic: %+v", result)
	}
}

func TestAggregateAttestationRetainsHistoricalCheckpoint(t *testing.T) {
	svc := testService(t, false)
	old := testCheckpoint("tenant-a", "stream-a", "root-old")
	newer := old
	newer.ID = "checkpoint-new"
	newer.Sequence = old.Sequence + 1
	newer.MerkleRoot = "root-new"
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Checkpoints[store.StreamKey("tenant-a", "stream-a")] = []domain.Checkpoint{old}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateAggregateCheckpoint(testCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		key := store.StreamKey("tenant-a", "stream-a")
		data.Checkpoints[key] = []domain.Checkpoint{old, newer}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateAggregateCheckpoint(testCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}

	snapshot, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.AggregateCheckpoints) != 2 {
		t.Fatalf("aggregate records=%d, want 2", len(snapshot.AggregateCheckpoints))
	}
	if got := snapshot.AggregateCheckpoints[0].CheckpointRefs[0].CheckpointID; got != old.ID {
		t.Fatalf("historical aggregate reference=%q, want %q", got, old.ID)
	}
	result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "tester", "")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Valid {
		t.Fatalf("historical checkpoint must remain verifiable: %+v", result.Errors)
	}
}

// TestVerifyIntegrityBoundedByRetentionCap is AC-2: with a small configured
// cap N, an over-cap bound history is trimmed to N on the next append
// (oldest dropped, newest retained), VerifyIntegrity verifies only the
// retained records (counting signer bound), and a tampered retained record
// still fails verification (evidence semantics preserved).
func TestVerifyIntegrityBoundedByRetentionCap(t *testing.T) {
	svc := testService(t, false)
	svc.Config.AggregateCheckpointRetention = 5
	// One live checkpoint drives the next candidate record.
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		data.Checkpoints[store.StreamKey("tenant-a", "s1")] = []domain.Checkpoint{testCheckpoint("tenant-a", "s1", "live-root-1")}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Pre-population (NFR-4): 5+K=55 self-consistent bound records. They
	// all reference the retained live checkpoint, so VerifyIntegrity can
	// prove each exact lineage record after trimming.
	const legacy = 55
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		checkpoint := data.Checkpoints[store.StreamKey("tenant-a", "s1")][0]
		refs := []domain.CheckpointRef{{StreamID: checkpoint.StreamID, CheckpointID: checkpoint.ID, Sequence: checkpoint.Sequence, MerkleRoot: checkpoint.MerkleRoot}}
		roots := aggregateRoots(refs)
		for i := 0; i < legacy; i++ {
			aggregate := domain.AggregateCheckpoint{
				ID: fmt.Sprintf("aggregate-bound-%d", i), TenantID: "tenant-a", StreamCount: 1,
				Root: merkleRoot(roots), Algorithm: svc.Config.Signer.Algorithm(),
				CreatedAt: testTime, StreamRoots: roots, AttestationVersion: aggregateAttestationVersion,
				CheckpointRefs: refs,
			}
			signature, err := svc.Config.Signer.Sign(testCtx, aggregateAttestationPayload(aggregate))
			if err != nil {
				return err
			}
			aggregate.Signature = signature
			data.AggregateCheckpoints = append(data.AggregateCheckpoints, aggregate)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// The next pass deduplicates the unchanged bound content and trims:
	// 55 records -> 5 retained, oldest dropped.
	if err := svc.CreateAggregateCheckpoint(testCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if got := tenantAggregateCount(t, svc, "tenant-a"); got != 5 {
		t.Fatalf("records after trim=%d, want 5 (cap)", got)
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	newest := snap.AggregateCheckpoints[len(snap.AggregateCheckpoints)-1]
	if newest.Root != merkleRoot([]string{"live-root-1"}) {
		t.Fatalf("newest retained record root=%q, want the live root (most recent record always retained)", newest.Root)
	}

	// Counting signer: verify work is bounded by the cap, not by the 55-record
	// history (FR-5). The wrap happens after the append so Sign calls of the
	// append path are not counted.
	counting := &countingSigner{Signer: svc.Config.Signer}
	svc.Config.Signer = counting
	result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Valid {
		t.Fatalf("VerifyIntegrity with retained records must be valid: %+v", result)
	}
	if counting.verifyCalls > 5 {
		t.Fatalf("aggregate Verify calls=%d, want <= 5 (work independent of history length)", counting.verifyCalls)
	}

	// Tamper a retained record: verification must fail (C4/FR-5 — no
	// weakening of tamper evidence for retained records).
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		for i := range data.AggregateCheckpoints {
			if data.AggregateCheckpoints[i].TenantID == "tenant-a" {
				data.AggregateCheckpoints[i].Root = "0000000000000000000000000000000000000000000000000000000000000000"
				break
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err = svc.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid {
		t.Fatal("tampered retained aggregate checkpoint must fail verification")
	}
}
