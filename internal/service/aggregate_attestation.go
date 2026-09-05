package service

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

const aggregateAttestationVersion = 2

// aggregateCheckpointRefs selects one exact, tenant-owned checkpoint per
// stream. The checkpoint ID and root are retained together so an aggregate
// cannot be built from a root that has no durable lineage.
func aggregateCheckpointRefs(data *store.Snapshot, tenantID string) ([]domain.CheckpointRef, error) {
	refs := make([]domain.CheckpointRef, 0, len(data.Checkpoints))
	for key, checkpoints := range data.Checkpoints {
		if len(checkpoints) == 0 {
			continue
		}
		owner, streamID, ok := store.SplitTenantKey(key)
		if !ok || owner != tenantID {
			continue
		}
		checkpoint := checkpoints[len(checkpoints)-1]
		if checkpoint.TenantID != tenantID || checkpoint.StreamID != streamID || checkpoint.ID == "" || checkpoint.MerkleRoot == "" {
			return nil, fmt.Errorf("%w: malformed checkpoint for stream %s", domain.ErrInvalid, streamID)
		}
		refs = append(refs, domain.CheckpointRef{
			StreamID: checkpoint.StreamID, CheckpointID: checkpoint.ID,
			Sequence: checkpoint.Sequence, MerkleRoot: checkpoint.MerkleRoot,
		})
	}
	sortCheckpointRefs(refs)
	return refs, nil
}

func sortCheckpointRefs(refs []domain.CheckpointRef) {
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].StreamID != refs[j].StreamID {
			return refs[i].StreamID < refs[j].StreamID
		}
		if refs[i].CheckpointID != refs[j].CheckpointID {
			return refs[i].CheckpointID < refs[j].CheckpointID
		}
		if refs[i].Sequence != refs[j].Sequence {
			return refs[i].Sequence < refs[j].Sequence
		}
		return refs[i].MerkleRoot < refs[j].MerkleRoot
	})
}

func aggregateRoots(refs []domain.CheckpointRef) []string {
	roots := make([]string, len(refs))
	for i, ref := range refs {
		roots[i] = ref.MerkleRoot
	}
	return roots
}

// aggregateAttestationPayload is a deterministic, length-prefixed binary
// encoding. It deliberately includes both the roots compatibility field and
// the references field: neither duplicated representation can be changed
// without invalidating the signature.
func aggregateAttestationPayload(aggregate domain.AggregateCheckpoint) []byte {
	var payload bytes.Buffer
	writeAggregateField(&payload, "snaplink.aggregate-checkpoint")
	writeAggregateUint(&payload, uint64(aggregate.AttestationVersion))
	writeAggregateField(&payload, aggregate.TenantID)
	writeAggregateField(&payload, aggregate.Algorithm)
	writeAggregateUint(&payload, uint64(aggregate.StreamCount))
	writeAggregateUint(&payload, uint64(len(aggregate.StreamRoots)))
	for _, root := range aggregate.StreamRoots {
		writeAggregateField(&payload, root)
	}
	writeAggregateUint(&payload, uint64(len(aggregate.CheckpointRefs)))
	for _, ref := range aggregate.CheckpointRefs {
		writeAggregateField(&payload, ref.StreamID)
		writeAggregateField(&payload, ref.CheckpointID)
		writeAggregateUint(&payload, uint64(ref.Sequence))
		writeAggregateField(&payload, ref.MerkleRoot)
	}
	return payload.Bytes()
}

func writeAggregateField(payload *bytes.Buffer, value string) {
	writeAggregateUint(payload, uint64(len(value)))
	_, _ = payload.WriteString(value)
}

func writeAggregateUint(payload *bytes.Buffer, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = payload.Write(encoded[:])
}

func sameAggregateAttestation(left, right domain.AggregateCheckpoint) bool {
	if left.TenantID != right.TenantID || left.AttestationVersion != right.AttestationVersion ||
		left.StreamCount != right.StreamCount || left.Root != right.Root ||
		left.Signature != right.Signature || left.Algorithm != right.Algorithm ||
		len(left.StreamRoots) != len(right.StreamRoots) || len(left.CheckpointRefs) != len(right.CheckpointRefs) {
		return false
	}
	for i := range left.StreamRoots {
		if left.StreamRoots[i] != right.StreamRoots[i] {
			return false
		}
	}
	for i := range left.CheckpointRefs {
		if left.CheckpointRefs[i] != right.CheckpointRefs[i] {
			return false
		}
	}
	return true
}

func aggregateDiagnostic(id, reason string) string {
	if len(id) > 64 {
		id = id[:64]
	}
	return fmt.Sprintf("aggregate checkpoint %s %s", id, reason)
}

// verifyAggregateCheckpoint validates a bound aggregate against the exact
// checkpoint history in the same snapshot used by VerifyIntegrity. Legacy
// records are intentionally invalid: root-only signatures cannot provide the
// tenant and lineage guarantee, even when their historical signature checks.
func (s *Service) verifyAggregateCheckpoint(ctx context.Context, tenantID string, aggregate domain.AggregateCheckpoint, checkpoints map[string][]domain.Checkpoint) (bool, string) {
	if aggregate.AttestationVersion == 0 && len(aggregate.CheckpointRefs) == 0 {
		return false, aggregateDiagnostic(aggregate.ID, "is legacy root-only evidence")
	}
	if aggregate.AttestationVersion != aggregateAttestationVersion {
		return false, aggregateDiagnostic(aggregate.ID, "uses an unsupported attestation version")
	}
	if aggregate.TenantID != tenantID {
		return false, aggregateDiagnostic(aggregate.ID, "has a tenant mismatch")
	}
	if aggregate.Algorithm != s.Config.Signer.Algorithm() {
		return false, aggregateDiagnostic(aggregate.ID, "has an algorithm mismatch")
	}
	if aggregate.StreamCount <= 0 || aggregate.StreamCount != len(aggregate.CheckpointRefs) || len(aggregate.StreamRoots) != len(aggregate.CheckpointRefs) {
		return false, aggregateDiagnostic(aggregate.ID, "has a checkpoint reference mismatch")
	}
	ordered := append([]domain.CheckpointRef(nil), aggregate.CheckpointRefs...)
	sortCheckpointRefs(ordered)
	seenStreams := make(map[string]struct{}, len(ordered))
	seenCheckpoints := make(map[string]struct{}, len(ordered))
	for i, ref := range aggregate.CheckpointRefs {
		if ref != ordered[i] {
			return false, aggregateDiagnostic(aggregate.ID, "has a checkpoint reference mismatch")
		}
		if _, exists := seenStreams[ref.StreamID]; exists {
			return false, aggregateDiagnostic(aggregate.ID, "has duplicate stream references")
		}
		if _, exists := seenCheckpoints[ref.CheckpointID]; exists {
			return false, aggregateDiagnostic(aggregate.ID, "has duplicate checkpoint references")
		}
		seenStreams[ref.StreamID] = struct{}{}
		seenCheckpoints[ref.CheckpointID] = struct{}{}
		if ref.CheckpointID == "" || ref.MerkleRoot == "" || ref.MerkleRoot != aggregate.StreamRoots[i] {
			return false, aggregateDiagnostic(aggregate.ID, "has a checkpoint reference mismatch")
		}
		if !exactCheckpointReference(checkpoints, tenantID, ref) {
			return false, aggregateDiagnostic(aggregate.ID, "has a checkpoint reference mismatch")
		}
	}
	if merkleRoot(aggregate.StreamRoots) != aggregate.Root {
		return false, aggregateDiagnostic(aggregate.ID, "failed root/signature verification")
	}
	valid, verifyErr := s.Config.Signer.Verify(ctx, aggregateAttestationPayload(aggregate), aggregate.Signature)
	if verifyErr != nil {
		if isInterrupted(verifyErr) {
			return false, aggregateDiagnostic(aggregate.ID, "verification interrupted")
		}
		return false, aggregateDiagnostic(aggregate.ID, "failed root/signature verification")
	}
	if !valid {
		return false, aggregateDiagnostic(aggregate.ID, "failed root/signature verification")
	}
	return true, ""
}

func aggregateReferencesTenant(checkpoints map[string][]domain.Checkpoint, tenantID string, aggregate domain.AggregateCheckpoint) bool {
	for _, ref := range aggregate.CheckpointRefs {
		if exactCheckpointReference(checkpoints, tenantID, ref) {
			return true
		}
	}
	return false
}

func exactCheckpointReference(checkpoints map[string][]domain.Checkpoint, tenantID string, ref domain.CheckpointRef) bool {
	key := store.StreamKey(tenantID, ref.StreamID)
	values, ok := checkpoints[key]
	if !ok {
		return false
	}
	matches := 0
	for _, checkpoint := range values {
		if checkpoint.ID != ref.CheckpointID {
			continue
		}
		matches++
		if checkpoint.TenantID != tenantID || checkpoint.StreamID != ref.StreamID || checkpoint.Sequence != ref.Sequence || checkpoint.MerkleRoot != ref.MerkleRoot {
			return false
		}
	}
	return matches == 1
}
