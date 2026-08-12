package service

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

func (s *Service) CreateExport(tenantID, requestedBy string, query domain.Query) (domain.ExportJob, error) {
	// The export reads events: run the query through the audited read path so
	// the same actor's audit.event.read fact is recorded, then create the job.
	if _, err := s.QueryEvents(tenantID, requestedBy, query); err != nil {
		return domain.ExportJob{}, err
	}
	job := domain.ExportJob{ID: newID("export"), TenantID: tenantID, RequestedBy: requestedBy, Query: query, Status: "pending", CreatedAt: s.Now()}
	if err := s.Store.Update(func(data *store.Snapshot) error {
		data.Exports[job.ID] = job
		data.AdminActions = append(data.AdminActions, s.adminAction(tenantID, requestedBy, domain.AdminActionExportCreated, "export", job.ID, fmt.Sprintf("from=%s to=%s", query.From.Format(time.RFC3339), query.To.Format(time.RFC3339))))
		return nil
	}); err != nil {
		return domain.ExportJob{}, err
	}
	go s.runExport(job.ID)
	return job, nil
}

func (s *Service) GetExport(tenantID, jobID string) (domain.ExportJob, error) {
	var job domain.ExportJob
	err := s.Store.Read(func(data *store.Snapshot) error {
		value, ok := data.Exports[jobID]
		if !ok || value.TenantID != tenantID {
			return domain.ErrNotFound
		}
		job = value
		return nil
	})
	return job, err
}

// RecordExportDownload appends the audit.event.export self-audit fact for a
// completed export being downloaded. Called by the transport after the job
// is verified completed and before the sealed object is streamed; a failed
// append aborts the download (fail-closed).
func (s *Service) RecordExportDownload(tenantID, actor, jobID string) error {
	if jobID == "" {
		return fmt.Errorf("%w: job id is required", domain.ErrInvalid)
	}
	return s.recordReadAction(tenantID, actor, domain.AdminActionEventExport, "export", jobID, "download")
}

func (s *Service) CreateLegalHold(hold domain.LegalHold) (domain.LegalHold, error) {
	if hold.TenantID == "" || hold.Name == "" || hold.Reason == "" {
		return domain.LegalHold{}, fmt.Errorf("%w: tenant_id, name and reason are required", domain.ErrInvalid)
	}
	if !hold.Filter.From.IsZero() || !hold.Filter.To.IsZero() {
		if hold.Filter.From.IsZero() || hold.Filter.To.IsZero() || !hold.Filter.From.Before(hold.Filter.To) {
			return domain.LegalHold{}, fmt.Errorf("%w: legal hold filter requires from before to", domain.ErrInvalid)
		}
	}
	if (hold.Filter.PayloadField == "") != (hold.Filter.PayloadDigest == "") {
		return domain.LegalHold{}, fmt.Errorf("%w: payload_field and payload_digest must be provided together", domain.ErrInvalid)
	}
	if hold.ID == "" {
		hold.ID = newID("hold")
	}
	if hold.CreatedAt.IsZero() {
		hold.CreatedAt = s.Now()
	}
	if err := s.Store.Update(func(data *store.Snapshot) error {
		if _, ok := data.Tenants[hold.TenantID]; !ok {
			return domain.ErrNotFound
		}
		if _, exists := data.LegalHolds[hold.ID]; exists {
			return fmt.Errorf("%w: legal hold already exists", domain.ErrConflict)
		}
		data.LegalHolds[hold.ID] = hold
		data.AdminActions = append(data.AdminActions, s.adminAction(hold.TenantID, hold.CreatedBy, domain.AdminActionLegalHoldCreated, "legal_hold", hold.ID, hold.Reason))
		return nil
	}); err != nil {
		return domain.LegalHold{}, err
	}
	return hold, nil
}

func (s *Service) ListLegalHolds(tenantID string) ([]domain.LegalHold, error) {
	var result []domain.LegalHold
	err := s.Store.Read(func(data *store.Snapshot) error {
		for _, hold := range data.LegalHolds {
			if hold.TenantID == tenantID {
				result = append(result, hold)
			}
		}
		return nil
	})
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result, err
}

func (s *Service) ReleaseLegalHold(tenantID, holdID, releasedBy string) (domain.LegalHold, error) {
	var hold domain.LegalHold
	if err := s.Store.Update(func(data *store.Snapshot) error {
		value, ok := data.LegalHolds[holdID]
		if !ok || value.TenantID != tenantID {
			return domain.ErrNotFound
		}
		if value.ReleasedAt != nil {
			hold = value
			return nil
		}
		now := s.Now()
		value.ReleasedAt = &now
		value.ReleasedBy = releasedBy
		data.LegalHolds[holdID] = value
		hold = value
		data.AdminActions = append(data.AdminActions, s.adminAction(value.TenantID, releasedBy, domain.AdminActionLegalHoldReleased, "legal_hold", holdID, ""))
		return nil
	}); err != nil {
		return domain.LegalHold{}, err
	}
	return hold, nil
}

// CreateAggregateCheckpoint builds a signed Merkle root over the latest
// checkpoint of every stream of a tenant and appends it to the evidence
// trail (architecture plan section 10). A tenant without any checkpoint
// yields no record.
//
// Change dedup (FR-1): when the candidate record (root AND signature) is
// identical to the tenant's most recent record, nothing is appended and the
// pass persists nothing (Store.UpdateChecked skips the Save). The
// comparison runs inside the optimistic-lock closure, so it always sees the
// snapshot that will be written: a concurrent writer's append is observed
// on the retried run instead of producing a duplicate (FR-3).
//
// Retention (FR-4): per-tenant history is capped (drop-oldest,
// Config.AggregateCheckpointRetention, default
// DefaultAggregateCheckpointRetention). The cap is applied in the same
// atomic write as the append, and legacy over-cap histories are trimmed
// even on a dedup skip (one-time write after deploy), so VerifyIntegrity's
// aggregate work is bounded by the cap (FR-5). Retained records keep
// today's layout and stay individually verifiable (C4).
func (s *Service) CreateAggregateCheckpoint(tenantID string) error {
	now := s.Now()
	return s.Store.UpdateChecked(func(data *store.Snapshot) (bool, error) {
		roots := make([]string, 0, len(data.Checkpoints))
		for key, checkpoints := range data.Checkpoints {
			if len(checkpoints) == 0 {
				continue
			}
			// Exact-component membership: a key belongs to the tenant only if
			// it parses as exactly tenantID + separator + one more component.
			// Prefix matching would absorb foreign keys for tenant IDs that
			// embed the separator (StreamKey("a\x1fb","s") has prefix
			// "a\x1f"); multi-separator keys are excluded from every tenant.
			if tenant, _, ok := store.SplitTenantKey(key); !ok || tenant != tenantID {
				continue
			}
			roots = append(roots, checkpoints[len(checkpoints)-1].MerkleRoot)
		}
		if len(roots) == 0 {
			// Tenant has no stream checkpoints: no record, no Save (FR-2).
			return false, nil
		}
		sort.Strings(roots)
		root := merkleRoot(roots)
		signature, err := s.Config.Signer.Sign([]byte(root))
		if err != nil {
			return false, fmt.Errorf("sign aggregate checkpoint: %w", err)
		}
		candidate := domain.AggregateCheckpoint{
			ID: newID("aggregate"), TenantID: tenantID, StreamCount: len(roots),
			Root: root, Signature: signature, Algorithm: s.Config.Signer.Algorithm(),
			CreatedAt: now, StreamRoots: roots,
		}
		// FR-1 dedup identity: root AND signature vs the tenant's most recent
		// record. CreatedAt/ID/Algorithm are not compared — a changed
		// signature (key rotation, non-deterministic signer) appends a new
		// record, which is the correct new evidence; identical root+signature
		// means identical evidence content, so skipping is sound.
		if last := lastAggregateCheckpoint(data.AggregateCheckpoints, tenantID); last != nil &&
			last.Root == candidate.Root && last.Signature == candidate.Signature {
			// Trim-on-skip: legacy over-cap histories converge on the first
			// pass even when the root never changes again (at most one Save
			// per tenant ever; afterwards this path is write-free).
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

func (s *Service) VerifyIntegrity(tenantID, actor, streamID string) (IntegrityResult, error) {
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
	schemas := map[string]domain.EventSchema{}
	err := s.Store.Read(func(data *store.Snapshot) error {
		for _, event := range data.Events {
			if event.TenantID == tenantID && (streamID == "" || event.StreamID == streamID) {
				events = append(events, event)
			}
		}
		for key, values := range data.Segments {
			tid, sid, ok := store.SplitTenantKey(key)
			if !ok || tid != tenantID || (streamID != "" && sid != streamID) {
				continue
			}
			segments = append(segments, values...)
		}
		for _, aggregate := range data.AggregateCheckpoints {
			if aggregate.TenantID == tenantID {
				aggregateCheckpoints = append(aggregateCheckpoints, aggregate)
			}
		}
		for key, schema := range data.Schemas {
			schemas[key] = schema
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	result.EventCount = len(events)
	result.SegmentCount = len(segments)
	// 聚合 checkpoint：重算租户各流最后段 root 的 Merkle，与签名记录比对。
	for _, aggregate := range aggregateCheckpoints {
		roots := append([]string(nil), aggregate.StreamRoots...)
		sort.Strings(roots)
		expected := merkleRoot(roots)
		valid, verifyErr := s.Config.Signer.Verify([]byte(expected), aggregate.Signature)
		if verifyErr != nil || expected != aggregate.Root || !valid {
			result.Valid = false
			result.Errors = append(result.Errors, fmt.Sprintf("aggregate checkpoint %s root/signature mismatch", aggregate.ID))
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
		valid, verifyErr := s.Config.Signer.Verify([]byte(manifestHash), segment.Signature)
		if verifyErr != nil || manifestHash != segment.ManifestHash || !valid {
			result.Valid = false
			result.Errors = append(result.Errors, fmt.Sprintf("stream %s segment %d-%d signature mismatch", segment.StreamID, segment.FirstSequence, segment.LastSequence))
		}
	}
	// Read self-audit (F-06): the verification itself is a governance fact.
	// It is appended regardless of result.Valid — the fact documents the
	// read, not the verdict (FM-6). A failed append fails the verify closed.
	if err := s.recordReadAction(tenantID, actor, domain.AdminActionEventRead, "integrity", streamID, "verify"); err != nil {
		return result, err
	}
	return result, nil
}

// SealPendingSegments creates a checkpoint for each non-empty partial stream.
// The ingest path seals full segments; the governance worker calls this method
// periodically so low-volume streams also receive a durable checkpoint.
//
// A pass that seals nothing (no pending hashes for the tenant) persists
// nothing: Store.UpdateChecked skips the Save on an idle tick (FR-2).
func (s *Service) SealPendingSegments(tenantID string) error {
	now := s.Now()
	return s.Store.UpdateChecked(func(data *store.Snapshot) (bool, error) {
		mutated := false
		for key, stream := range data.Streams {
			if stream.TenantID != tenantID || len(stream.PendingHashes) == 0 {
				continue
			}
			segment, checkpoint, err := s.sealSegment(stream, now)
			if err != nil {
				return false, err
			}
			data.Segments[key] = append(data.Segments[key], segment)
			data.Checkpoints[key] = append(data.Checkpoints[key], checkpoint)
			stream.PendingHashes = nil
			stream.PendingEvents = nil
			stream.PendingPrevHash = ""
			data.Streams[key] = stream
			mutated = true
		}
		return mutated, nil
	})
}

// ArchivePending retries local WORM-compatible archive writes for events that
// were ledgered/indexed before the archive destination became available.
// It is intentionally idempotent: a byte-identical existing object is treated
// as already archived, while a mismatched or unverifiable object at the key
// surfaces as an error instead of being silently accepted.
//
// All receipts for events whose Put succeeded in this pass are committed in
// exactly one Store.Update (one optimistic-lock window instead of one per
// event), so a pass fails atomically — either every receipt is marked
// StatusArchived or none is. The same atomic write resets the tenant's
// archive-conflict counter, and the successful pass's receipts share one
// timestamp.

func (s *Service) ArchivePending(tenantID string) (int, error) {
	if !archive.Configured(s.Config.Archive) {
		return 0, fmt.Errorf("%w: archive directory is not configured", domain.ErrInvalid)
	}
	var events []domain.Event
	var segments []domain.Segment
	if err := s.Store.Read(func(data *store.Snapshot) error {
		for key, event := range data.Events {
			if event.TenantID != tenantID {
				continue
			}
			receipt := data.Receipts[key]
			if receipt.Status != domain.StatusArchived {
				events = append(events, event)
			}
		}
		for key, values := range data.Segments {
			// Exact-component membership (see CreateAggregateCheckpoint); the
			// former StreamKey(tenantID, "") shortcut is subsumed: that key
			// parses as (tenantID, "", true).
			tid, _, ok := store.SplitTenantKey(key)
			if !ok || tid != tenantID {
				continue
			}
			segments = append(segments, values...)
		}
		return nil
	}); err != nil {
		return 0, err
	}
	for _, segment := range segments {
		if err := s.archiveSegment(segment); err != nil {
			return 0, err
		}
	}
	archivedEvents := []domain.Event{}
	for _, event := range events {
		if err := s.archiveEvent(event); err != nil {
			return 0, err
		}
		archivedEvents = append(archivedEvents, event)
	}
	if len(archivedEvents) == 0 {
		// Nothing to mark; the conflict counter is deliberately not reset on
		// an empty pass (it counts consecutive passes aborted by exhaustion,
		// and an empty pass cannot have been aborted).
		return 0, nil
	}
	// One batch Update for the whole pass: the captured event list (not a
	// re-derivation from the fresh snapshot) keeps the closure re-runnable
	// — Store.Update re-runs it on a fresh snapshot after each conflict and
	// a failed Save commits nothing, so re-runs apply the same mutations.
	// The reset of the tenant's conflict counter rides in the same atomic
	// write as the receipt marking (no extra window).
	now := s.Now()
	if err := s.Store.Update(func(data *store.Snapshot) error {
		for _, event := range archivedEvents {
			key := store.EventKey(tenantID, event.EventID)
			receipt, ok := data.Receipts[key]
			if !ok {
				return domain.ErrNotFound
			}
			receipt.Status = domain.StatusArchived
			receipt.IndexedAt = now
			receipt.ArchivedAt = now
			data.Receipts[key] = receipt
		}
		data.ArchiveConflictFailures[tenantID] = 0
		return nil
	}); err != nil {
		return 0, err
	}
	return len(archivedEvents), nil
}

// RecordArchivePassConflict increments the tenant's persisted archive-pass
// conflict counter. Best-effort: the worker invokes it after ArchivePending
// returned an exhausted ErrSnapshotConflict; if this write itself exhausts
// under sustained load it fails without masking the original error. The
// closure is conflict-safe by construction: Store.Update re-runs it on the
// fresh snapshot, and a failed Save commits nothing, so each committed
// attempt applies exactly one increment.
func (s *Service) RecordArchivePassConflict(tenantID string) error {
	return s.Store.Update(func(data *store.Snapshot) error {
		data.ArchiveConflictFailures[tenantID]++
		return nil
	})
}

// ArchivePassConflictFailures returns the tenant's persisted archive-pass
// conflict counter, for the worker's log line and for tests.
func (s *Service) ArchivePassConflictFailures(tenantID string) (int, error) {
	var count int
	err := s.Store.Read(func(data *store.Snapshot) error {
		count = data.ArchiveConflictFailures[tenantID]
		return nil
	})
	return count, err
}

func (s *Service) PreviewRestore(tenantID string, request domain.RestoreRequest) (domain.RestorePreview, error) {
	// F1 (security review): restore preview replays event-derived state with
	// the same read permission as the audited timeline/replay endpoints, but
	// the design gate records an explicit rejection of audit here: the actor
	// stays empty, so recordReadAction appends nothing (compat constraint 3,
	// pinned by TestReadSelfAuditRestoreFlowRecordsNothing). A restore
	// preview is a proposal for a compensating action, not a content read;
	// changing this requires a product decision, not a code change alone.
	result, err := s.ReplayOperation(tenantID, "", request.OperationID)
	if err != nil {
		return domain.RestorePreview{}, err
	}
	return domain.RestorePreview{ID: newID("restore-preview"), TenantID: tenantID, OperationID: request.OperationID, ProposedState: result.State, ExternalCalls: []string{}, RequiresApproval: true}, nil
}

func (s *Service) CreateRestore(tenantID string, request domain.RestoreRequest, requestedBy string) (domain.RestoreRun, error) {
	if request.OperationID == "" || request.Reason == "" {
		return domain.RestoreRun{}, fmt.Errorf("%w: operation_id and reason are required", domain.ErrInvalid)
	}
	if _, err := s.ReplayOperation(tenantID, "", request.OperationID); err != nil {
		return domain.RestoreRun{}, err
	}
	run := domain.RestoreRun{ID: newID("restore"), TenantID: tenantID, OperationID: request.OperationID, Status: domain.RestoreStatusPendingApproval, Reason: request.Reason, CreatedBy: requestedBy, CreatedAt: s.Now()}
	if err := s.Store.Update(func(data *store.Snapshot) error {
		data.RestoreRuns[run.ID] = run
		data.AdminActions = append(data.AdminActions, s.adminAction(tenantID, requestedBy, domain.AdminActionRestoreCreated, "restore", run.ID, request.OperationID))
		return nil
	}); err != nil {
		return domain.RestoreRun{}, err
	}
	return run, nil
}

// ApproveRestore moves a pending restore run to approved. Approval only
// records the decision fact; executing the compensating business calls is a
// separate, later stage and is never triggered here.

func (s *Service) ApproveRestore(tenantID, runID, approvedBy string) (domain.RestoreRun, error) {
	return s.transitionRestore(tenantID, runID, approvedBy, domain.RestoreStatusApproved)
}

// RejectRestore moves a pending restore run to rejected.

func (s *Service) RejectRestore(tenantID, runID, rejectedBy string) (domain.RestoreRun, error) {
	return s.transitionRestore(tenantID, runID, rejectedBy, domain.RestoreStatusRejected)
}

func (s *Service) transitionRestore(tenantID, runID, actor, target string) (domain.RestoreRun, error) {
	var run domain.RestoreRun
	err := s.Store.Update(func(data *store.Snapshot) error {
		value, ok := data.RestoreRuns[runID]
		if !ok || value.TenantID != tenantID {
			return domain.ErrNotFound
		}
		if value.Status != domain.RestoreStatusPendingApproval {
			return fmt.Errorf("%w: restore run is not pending approval", domain.ErrConflict)
		}
		// Separation of duties: the deciding actor must differ from the
		// requesting actor. Inside the Update closure, so a refusal commits
		// nothing (no version bump, no admin action). Precedence is pinned
		// NotFound → Conflict → Forbidden: 404 masks cross-tenant existence,
		// 409 dominates for decided runs.
		if value.CreatedBy == actor {
			return domain.ErrForbidden
		}
		now := s.Now()
		if target == domain.RestoreStatusApproved {
			value.Status = domain.RestoreStatusApproved
			value.ApprovedBy = actor
			value.ApprovedAt = &now
			data.AdminActions = append(data.AdminActions, s.adminAction(value.TenantID, actor, domain.AdminActionRestoreApproved, "restore", runID, value.OperationID))
		} else {
			value.Status = domain.RestoreStatusRejected
			value.RejectedBy = actor
			value.RejectedAt = &now
			data.AdminActions = append(data.AdminActions, s.adminAction(value.TenantID, actor, domain.AdminActionRestoreRejected, "restore", runID, value.OperationID))
		}
		data.RestoreRuns[runID] = value
		run = value
		return nil
	})
	return run, err
}

func (s *Service) GetRestore(tenantID, runID string) (domain.RestoreRun, error) {
	var run domain.RestoreRun
	err := s.Store.Read(func(data *store.Snapshot) error {
		value, ok := data.RestoreRuns[runID]
		if !ok || value.TenantID != tenantID {
			return domain.ErrNotFound
		}
		run = value
		return nil
	})
	return run, err
}

// DefaultStuckExportAge is the default age past which an export job stuck
// in "running" is failed by the governance worker (measured from CreatedAt,
// the only pre-terminal timestamp). Deliberately generous so a legitimately
// long export is never failed by a slow pass cycle; operators can tighten it
// per deployment with AUDIT_GOVERNANCE_STUCK_EXPORT_AGE; values <= 0 select
// this default.
const DefaultStuckExportAge = 24 * time.Hour

// RecoverStuckExports fails one tenant's export jobs stuck in "running" past
// Config.StuckExportAge (jobs left non-terminal by a crashed or restarted
// audit-api — runExport is a fire-and-forget goroutine, so nothing else ever
// re-examines them). The status change and the export.recovered self-audit
// fact ride one Store.UpdateChecked Save: the closure re-checks
// status/finished/age against the fresh snapshot and retries on
// ErrSnapshotConflict, so a terminal job is never regressed and two worker
// replicas cannot double-fail the same job. No archive I/O. Returns the
// number of transitions committed by the winning attempt.
func (s *Service) RecoverStuckExports(tenantID string) (int, error) {
	recovered := 0
	err := s.Store.UpdateChecked(func(data *store.Snapshot) (bool, error) {
		// Store.UpdateChecked may re-invoke the closure on a CAS conflict; a
		// failed attempt's mutations are discarded, so the counter is reset at
		// the start of every invocation to count only the committed attempt
		// (mirrors Ingest's sealedSegments truncation precedent).
		recovered = 0
		cutoff := s.Now().Add(-s.Config.StuckExportAge)
		mutated := false
		for jobID, job := range data.Exports {
			// Strict "older than": a job created exactly at the cutoff waits
			// for the next pass (deterministic boundary). A job with a zero
			// CreatedAt (corrupt/hand-edited row) fails the Before test and is
			// recovered, which is safe: CreateExport always stamps CreatedAt.
			if job.TenantID != tenantID || job.Status != "running" || job.FinishedAt != nil || !job.CreatedAt.Before(cutoff) {
				continue
			}
			now := s.Now()
			job.Status = "failed"
			job.FinishedAt = &now
			job.ObjectPath = ""
			job.Digest = ""
			job.EventCount = 0
			job.Error = fmt.Sprintf("export interrupted: stuck in running since %s; re-request the export", job.CreatedAt.Format(time.RFC3339))
			data.Exports[jobID] = job
			data.AdminActions = append(data.AdminActions, s.adminAction(tenantID, "governance-worker", domain.AdminActionExportRecovered, "export", jobID, fmt.Sprintf("stuck_running_since=%s status=failed", job.CreatedAt.Format(time.RFC3339))))
			recovered++
			mutated = true
		}
		return mutated, nil
	})
	return recovered, err
}

func (s *Service) runExport(jobID string) {
	var job domain.ExportJob
	if err := s.Store.Read(func(data *store.Snapshot) error {
		var ok bool
		job, ok = data.Exports[jobID]
		if !ok {
			return domain.ErrNotFound
		}
		return nil
	}); err != nil {
		return
	}
	_ = s.Store.Update(func(data *store.Snapshot) error {
		value := data.Exports[jobID]
		value.Status = "running"
		data.Exports[jobID] = value
		return nil
	})
	var events []domain.Event
	schemas := map[string]domain.EventSchema{}
	err := s.Store.Read(func(data *store.Snapshot) error {
		for key, schema := range data.Schemas {
			schemas[key] = schema
		}
		for _, event := range data.Events {
			if event.TenantID == job.TenantID && s.matches(event, job.Query, schemas) {
				events = append(events, event)
			}
		}
		return nil
	})
	if err != nil {
		s.finishExport(jobID, "failed", "", "", 0, err.Error())
		return
	}
	sortEvents(events)
	var bytesWritten []byte
	for _, event := range events {
		// 导出内容剥离搜索摘要（深拷贝，不触碰存储共享的 payload）：解密
		// 后的 JSONL 不得携带可跨租户关联的 digest 值。v2 bound 摘要本身
		// 已租户隔离，但 v1 遗留摘要对同明文跨租户相等，且摘要对消费者无
		// 业务价值，因此一律剥离。
		stripped, stripErr := security.StripSearchDigests(event.Payload)
		if stripErr != nil {
			s.finishExport(jobID, "failed", "", "", 0, stripErr.Error())
			return
		}
		event.Payload = stripped
		line, marshalErr := domain.CanonicalJSON(event)
		if marshalErr != nil {
			s.finishExport(jobID, "failed", "", "", 0, marshalErr.Error())
			return
		}
		bytesWritten = append(bytesWritten, line...)
		bytesWritten = append(bytesWritten, '\n')
	}
	// 独立加密：导出文件整体以 AES-GCM 密封后写入归档（§14）。
	sealed, sealErr := security.EncryptBytes(bytesWritten, s.Config.EncryptionKey)
	if sealErr != nil {
		s.finishExport(jobID, "failed", "", "", 0, sealErr.Error())
		return
	}
	key := fmt.Sprintf("exports/%s.jsonl", safeName(jobID))
	if err := s.Config.Archive.Put(context.Background(), key, sealed); err != nil {
		s.finishExport(jobID, "failed", "", "", 0, err.Error())
		return
	}
	s.finishExport(jobID, "completed", key, domain.HashBytes(bytesWritten), len(events), "")
}

func (s *Service) finishExport(jobID, status, path, digest string, count int, failure string) {
	_ = s.Store.Update(func(data *store.Snapshot) error {
		job := data.Exports[jobID]
		now := s.Now()
		job.Status, job.FinishedAt, job.ObjectPath, job.Digest, job.EventCount, job.Error = status, &now, path, digest, count, failure
		data.Exports[jobID] = job
		return nil
	})
}

func (s *Service) ListAdminActions(tenantID string, platform bool, limit int) ([]domain.AdminAction, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	var result []domain.AdminAction
	err := s.Store.Read(func(data *store.Snapshot) error {
		for i := len(data.AdminActions) - 1; i >= 0 && len(result) < limit; i-- {
			action := data.AdminActions[i]
			if platform || action.TenantID == tenantID {
				result = append(result, action)
			}
		}
		return nil
	})
	return result, err
}

// Marshal is kept here as a small compile-time guard that the domain model
// remains JSON serializable for the file store and export path.
