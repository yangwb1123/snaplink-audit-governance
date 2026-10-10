package service

// Evidence-chain verification: the periodic cross-segment aggregate
// checkpoint and the three-layer verification it participates in (per-event
// prev_hash chain, per-segment Merkle root with a signed checkpoint, then the
// aggregate Merkle over segments). A signer or verifier error that means
// cancellation is never reported as a verdict.

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

func (s *Service) CreateAggregateCheckpoint(ctx context.Context, tenantID string) error {
	if err := requireTenantScope(tenantID); err != nil {
		return err
	}
	now := s.Now()
	return s.Store.UpdateChecked(ctx, func(data *store.Snapshot) (bool, error) {
		refs, err := aggregateCheckpointRefs(data, tenantID)
		if err != nil {
			return false, err
		}
		if len(refs) == 0 {
			// Tenant has no stream checkpoints: no record, no Save (FR-2).
			return false, nil
		}
		roots := aggregateRoots(refs)
		candidate := domain.AggregateCheckpoint{
			ID: newID("aggregate"), TenantID: tenantID, StreamCount: len(refs),
			Root: merkleRoot(roots), Algorithm: s.Config.Signer.Algorithm(),
			CreatedAt: now, StreamRoots: roots, AttestationVersion: aggregateAttestationVersion,
			CheckpointRefs: refs,
		}
		signature, err := s.Config.Signer.Sign(ctx, aggregateAttestationPayload(candidate))
		if err != nil {
			return false, fmt.Errorf("sign aggregate checkpoint: %w", err)
		}
		candidate.Signature = signature
		// Compare the complete bound attestation. A root-only or differently
		// lined record must not suppress new tenant-bound evidence.
		if last := lastAggregateCheckpoint(data.AggregateCheckpoints, tenantID); last != nil &&
			sameAggregateAttestation(*last, candidate) {
			trimmed, changed := trimAggregateCheckpoints(data.AggregateCheckpoints, tenantID, s.retentionCap())
			data.AggregateCheckpoints = trimmed
			return changed, nil
		}
		data.AggregateCheckpoints = append(data.AggregateCheckpoints, candidate)
		// FR-4 cap applied in the same atomic write as the append.
		data.AggregateCheckpoints, _ = trimAggregateCheckpoints(data.AggregateCheckpoints, tenantID, s.retentionCap())
		return true, nil
	})
}

// maxVerifyStreamIDLength bounds the stream_id accepted by VerifyIntegrity
// (security review F2). The fact append records stream_id verbatim as
// target_id in the unbounded admin trail; the cap keeps one verify call's
// trail growth bounded. 256 bytes is far above any real stream id (the
// longest derived stream key today is ~60 chars) and below any plausible
// audit-format concern.
const maxVerifyStreamIDLength = 256

// isInterrupted reports whether a signer/verifier error means the operation
// was cancelled or timed out rather than producing a verdict. Such errors
// must not be recorded as a false "signature mismatch" fact in the audit
// trail (F-2).
func isInterrupted(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// verificationInterrupted narrows isInterrupted to "the verification never
// reached a verdict" for the two store-backed steps of VerifyIntegrity.
//
// A store operation that exhausted its OWN deadline (store.ErrOperationDeadline,
// which wraps context.DeadlineExceeded) is deliberately excluded: that is the
// retryable store_timeout class, and turning it into a quiet Valid=false
// report would hide the very saturation this direction makes observable. Only
// a caller-side cancellation — the caller withdrew, so there is no result to
// report — is reported as an interrupted verification, preserving REQ-3:
// fail-closed Valid=false, no signature-mismatch fact, and no endpoint error.
func verificationInterrupted(err error) bool {
	if errors.Is(err, store.ErrOperationDeadline) {
		return false
	}
	return isInterrupted(err)
}

func (s *Service) VerifyIntegrity(ctx context.Context, tenantID, actor, streamID string) (IntegrityResult, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return IntegrityResult{}, err
	}
	result := IntegrityResult{Valid: true, TenantID: tenantID, StreamID: streamID, CheckedAt: s.Now()}
	// Self-audit trail bound (security review F2): the fact records stream_id
	// verbatim as target_id into the unbounded single-row admin trail, so an
	// integrity-verify caller must not be able to grow it without bound.
	// Reject non-key-framing characters via the shared charset rule and cap
	// the length at the service layer (transport-independent); rejected
	// calls append nothing.
	if streamID != "" {
		if err := domain.ValidKeyComponent("stream_id", streamID); err != nil {
			return result, err
		}
		if len(streamID) > maxVerifyStreamIDLength {
			return result, fmt.Errorf("%w: stream_id exceeds %d bytes", domain.ErrInvalid, maxVerifyStreamIDLength)
		}
	}
	// One snapshot read: events, segments, aggregate checkpoints and schemas
	// must come from the same ledger version so per-event content checks can
	// never combine events with schema definitions from another snapshot.
	var events []domain.Event
	var segments []domain.Segment
	var aggregateCheckpoints []domain.AggregateCheckpoint
	var checkpoints map[string][]domain.Checkpoint
	schemas := map[string]domain.EventSchema{}
	err := s.Store.Read(ctx, func(data *store.Snapshot) error {
		var resolveErr error
		events, resolveErr = s.resolveSnapshotEvents(ctx, data, tenantID, func(event domain.Event) bool {
			return streamID == "" || event.StreamID == streamID
		}, false)
		if resolveErr != nil {
			return resolveErr
		}
		for key, values := range data.Segments {
			tid, sid, ok := store.SplitTenantKey(key)
			if !ok || tid != tenantID || (streamID != "" && sid != streamID) {
				continue
			}
			segments = append(segments, values...)
		}
		checkpoints = data.Checkpoints
		for _, aggregate := range data.AggregateCheckpoints {
			// The aggregate slice has no separate map key. In addition to the
			// claimed tenant, retain a bound record whose exact references still
			// point into this tenant so a tampered TenantID cannot disappear
			// silently from the verification result.
			if aggregate.TenantID == tenantID || aggregateReferencesTenant(data.Checkpoints, tenantID, aggregate) {
				aggregateCheckpoints = append(aggregateCheckpoints, aggregate)
			}
		}
		for key, schema := range data.Schemas {
			schemas[key] = schema
		}
		return nil
	})
	if err != nil {
		if verificationInterrupted(err) {
			result.Valid = false
			result.Errors = append(result.Errors, "verification interrupted")
			return result, nil
		}
		return result, err
	}
	result.EventCount = len(events)
	result.SegmentCount = len(segments)
	// Aggregate attestations are checked against the exact checkpoint history
	// captured above, never against roots reconstructed from another read.
	for _, aggregate := range aggregateCheckpoints {
		valid, diagnostic := s.verifyAggregateCheckpoint(ctx, tenantID, aggregate, checkpoints)
		if !valid {
			result.Valid = false
			result.Errors = append(result.Errors, diagnostic)
		}
	}
	// Events from different streams have independent sequence spaces.  They
	// must be checked stream-by-stream; ordering the combined result by event
	// time can otherwise make a valid chain look corrupted.
	byStream := map[string][]domain.Event{}
	for _, event := range events {
		byStream[event.StreamID] = append(byStream[event.StreamID], event)
	}
	streamKeys := make([]string, 0, len(byStream))
	for stream := range byStream {
		streamKeys = append(streamKeys, stream)
	}
	sort.Strings(streamKeys)
	for _, currentStream := range streamKeys {
		streamEvents := byStream[currentStream]
		sort.Slice(streamEvents, func(i, j int) bool {
			if streamEvents[i].Sequence == streamEvents[j].Sequence {
				return streamEvents[i].EventID < streamEvents[j].EventID
			}
			return streamEvents[i].Sequence < streamEvents[j].Sequence
		})
		head := ""
		for _, event := range streamEvents {
			if event.PrevHash != head {
				result.Valid = false
				result.Errors = append(result.Errors, fmt.Sprintf("stream %s sequence %d prev_hash mismatch", currentStream, event.Sequence))
			}
			expected, hashErr := s.eventHash(event)
			if hashErr != nil || expected != event.Hash {
				result.Valid = false
				result.Errors = append(result.Errors, fmt.Sprintf("stream %s sequence %d hash mismatch", currentStream, event.Sequence))
			}
			if err := s.verifyContentDigest(event, schemas); err != nil {
				result.Valid = false
				result.Errors = append(result.Errors, err.Error())
			}
			head = event.Hash
		}
	}
	for _, segment := range segments {
		segmentHashes := make([]string, 0, segment.EventCount)
		streamEvents := byStream[segment.StreamID]
		for _, event := range streamEvents {
			if event.StreamID == segment.StreamID && event.Sequence >= segment.FirstSequence && event.Sequence <= segment.LastSequence {
				segmentHashes = append(segmentHashes, event.Hash)
			}
		}
		root := merkleRoot(segmentHashes)
		firstPrevHash := ""
		lastHash := ""
		if len(streamEvents) > 0 {
			for _, event := range streamEvents {
				if event.Sequence == segment.FirstSequence {
					firstPrevHash = event.PrevHash
				}
				if event.Sequence == segment.LastSequence {
					lastHash = event.Hash
				}
			}
		}
		contiguous := segment.LastSequence >= segment.FirstSequence && segment.EventCount == int(segment.LastSequence-segment.FirstSequence+1)
		if root != segment.MerkleRoot || len(segmentHashes) != segment.EventCount || firstPrevHash != segment.FirstPrevHash || lastHash != segment.LastHash || !contiguous {
			result.Valid = false
			result.Errors = append(result.Errors, fmt.Sprintf("stream %s segment %d-%d merkle root mismatch", segment.StreamID, segment.FirstSequence, segment.LastSequence))
		}
		manifest := fmt.Sprintf("%s:%d:%d:%s:%s", segment.TenantID, segment.FirstSequence, segment.LastSequence, segment.LastHash, segment.MerkleRoot)
		manifestHash := domain.HashBytes([]byte(manifest))
		valid, verifyErr := s.Config.Signer.Verify(ctx, []byte(manifestHash), segment.Signature)
		if verifyErr != nil || manifestHash != segment.ManifestHash || !valid {
			result.Valid = false
			if isInterrupted(verifyErr) {
				result.Errors = append(result.Errors, fmt.Sprintf("stream %s segment %d-%d verification interrupted", segment.StreamID, segment.FirstSequence, segment.LastSequence))
			} else {
				result.Errors = append(result.Errors, fmt.Sprintf("stream %s segment %d-%d signature mismatch", segment.StreamID, segment.FirstSequence, segment.LastSequence))
			}
		}
	}
	// Read self-audit (F-06): the verification itself is a governance fact.
	// It is appended regardless of result.Valid — the fact documents the
	// read, not the verdict (FM-6). A failed append fails the verify closed.
	if err := s.recordReadAction(ctx, tenantID, actor, domain.AdminActionEventRead, "integrity", streamID, "verify"); err != nil {
		// A caller that withdrew mid-verification cannot be reported to, so
		// the read fact is not lost silently: the interruption is recorded as
		// such. A store deadline is NOT swallowed (see verificationInterrupted).
		if verificationInterrupted(err) {
			result.Valid = false
			result.Errors = append(result.Errors, "verification interrupted")
			return result, nil
		}
		return result, err
	}
	return result, nil
}
