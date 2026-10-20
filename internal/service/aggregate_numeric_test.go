package service

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func mustAggregateAttestationPayload(t *testing.T, aggregate domain.AggregateCheckpoint) []byte {
	t.Helper()
	payload, err := aggregateAttestationPayload(aggregate)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func numericAggregate() domain.AggregateCheckpoint {
	return domain.AggregateCheckpoint{
		TenantID: "tenant-a", Algorithm: "HMAC-SHA256", AttestationVersion: 2,
		StreamCount: 1, StreamRoots: []string{"root"},
		CheckpointRefs: []domain.CheckpointRef{{
			StreamID: "stream-a", CheckpointID: "checkpoint-a", Sequence: math.MaxInt64, MerkleRoot: "root",
		}},
	}
}

func TestAggregateAttestationNumericBoundsAndHistoricalEncoding(t *testing.T) {
	payload := mustAggregateAttestationPayload(t, numericAggregate())
	digest := sha256.Sum256(payload)
	// Pre-hardening v2 length-prefixed bytes, including MaxInt64 sequence.
	const historical = "eae0607b41a3d738decca16df1a35823b72415e84566fd0a1b93a2188adeef99"
	if len(payload) != 172 || hex.EncodeToString(digest[:]) != historical {
		t.Fatalf("valid historical encoding changed: bytes=%d digest=%x", len(payload), digest)
	}
	for _, test := range []struct {
		name   string
		mutate func(*domain.AggregateCheckpoint)
	}{
		{"negative version", func(a *domain.AggregateCheckpoint) { a.AttestationVersion = -1 }},
		{"negative count", func(a *domain.AggregateCheckpoint) { a.StreamCount = -1 }},
		{"zero sequence", func(a *domain.AggregateCheckpoint) { a.CheckpointRefs[0].Sequence = 0 }},
		{"negative sequence", func(a *domain.AggregateCheckpoint) { a.CheckpointRefs[0].Sequence = math.MinInt64 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			aggregate := numericAggregate()
			test.mutate(&aggregate)
			payload, err := aggregateAttestationPayload(aggregate)
			if !errors.Is(err, domain.ErrInvalid) || payload != nil {
				t.Fatalf("invalid numeric field encoded: bytes=%x err=%v", payload, err)
			}
		})
	}
}

func TestAggregateCheckpointRejectsNonpositiveSequenceBeforeSigning(t *testing.T) {
	for _, sequence := range []int64{math.MinInt64, -1, 0} {
		svc := testService(t, false)
		checkpoint := testCheckpoint("tenant-a", "stream-a", "root")
		checkpoint.Sequence = sequence
		if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
			data.Checkpoints[store.StreamKey("tenant-a", "stream-a")] = []domain.Checkpoint{checkpoint}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		svc.Config.Signer = erroringSigner{Signer: svc.Config.Signer}
		if err := svc.CreateAggregateCheckpoint(testCtx, "tenant-a"); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("sequence=%d create=%v", sequence, err)
		}
		if count := tenantAggregateCount(t, svc, "tenant-a"); count != 0 {
			t.Fatalf("sequence=%d created %d aggregate records", sequence, count)
		}
	}
}

func TestAggregateNegativeFieldsRemainAuditedInvalidDiagnostics(t *testing.T) {
	for _, mutate := range []func(*domain.AggregateCheckpoint){
		func(a *domain.AggregateCheckpoint) { a.AttestationVersion = -1 },
		func(a *domain.AggregateCheckpoint) { a.StreamCount = -1 },
		func(a *domain.AggregateCheckpoint) { a.CheckpointRefs[0].Sequence = -1 },
	} {
		svc, _ := boundAggregateFixture(t)
		if err := svc.Store.Update(testCtx, func(data *store.Snapshot) error {
			mutate(&data.AggregateCheckpoints[0])
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "numeric-tester", "")
		if err != nil || result.Valid || len(result.Errors) == 0 {
			t.Fatalf("bad committed evidence must be an invalid diagnostic: result=%+v err=%v", result, err)
		}
		facts, err := svc.ListAdminActions(testCtx, "tenant-a", false, 100)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, fact := range facts {
			if fact.Actor == "numeric-tester" && fact.Action == domain.AdminActionEventRead && fact.TargetType == "integrity" {
				found = true
			}
		}
		if !found {
			t.Fatal("invalid diagnostic did not self-audit")
		}
	}
}
