package service

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

func (s *Service) CreateExport(tenantID, requestedBy string, query domain.Query) (domain.ExportJob, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.ExportJob{}, err
	}
	// Fail closed before any read fact, job, self-audit record, or goroutine
	// exists (same ordering discipline as the R2 legal-hold gate).
	// The export reads events: run the query through the audited read path so
	// the same actor's audit.event.read fact is recorded, then create the job.
	if _, err := s.QueryEvents(tenantID, requestedBy, query); err != nil {
		return domain.ExportJob{}, err
	}
	// R2 (legal-hold gate): fail closed before any job exists. The check is a
	// read; the block fact commits in its own Update (a closure returning an
	// error discards its mutations, so check-and-abort cannot share the
	// job-creating closure). No job, no export.created, no goroutine on the
	// block path.
	var hold domain.LegalHold
	blocked := false
	if err := s.Store.Read(func(data *store.Snapshot) error {
		var holdErr error
		hold, blocked, holdErr = s.holdBlockingExport(data, tenantID, query)
		return holdErr
	}); err != nil {
		return domain.ExportJob{}, err
	}
	if blocked {
		if err := s.Store.Update(func(data *store.Snapshot) error {
			s.appendAdminAction(data, s.adminAction(tenantID, requestedBy, domain.AdminActionExportBlocked, "legal_hold", hold.ID, fmt.Sprintf("export_blocked reason=%s", hold.Reason)))
			return nil
		}); err != nil {
			return domain.ExportJob{}, err
		}
		return domain.ExportJob{}, fmt.Errorf("%w", exportBlockedError{holdID: hold.ID})
	}
	job := domain.ExportJob{ID: newID("export"), TenantID: tenantID, RequestedBy: requestedBy, Query: query, Status: "pending", CreatedAt: s.Now()}
	if err := s.Store.Update(func(data *store.Snapshot) error {
		data.Exports[job.ID] = job
		s.appendAdminAction(data, s.adminAction(tenantID, requestedBy, domain.AdminActionExportCreated, "export", job.ID, fmt.Sprintf("from=%s to=%s", query.From.Format(time.RFC3339), query.To.Format(time.RFC3339))))
		return nil
	}); err != nil {
		return domain.ExportJob{}, err
	}
	go s.runExport(job.ID)
	return job, nil
}

func (s *Service) GetExport(tenantID, jobID string) (domain.ExportJob, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.ExportJob{}, err
	}
	var job domain.ExportJob
	err := s.Store.Read(func(data *store.Snapshot) error {
		value, ok := data.Exports[jobID]
		if !ok || value.TenantID != tenantID {
			return domain.ErrNotFound
		}
		// R4 (legal-hold gate): fail closed while any active hold covers the
		// job's query, regardless of job status (covers exports completed
		// before the hold). Read-only denial: no block fact is appended.
		hold, blocked, holdErr := s.holdBlockingExport(data, tenantID, value.Query)
		if holdErr != nil {
			return holdErr
		}
		if blocked {
			return fmt.Errorf("%w", exportBlockedError{holdID: hold.ID})
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
	if err := requireTenantScope(tenantID); err != nil {
		return err
	}
	if jobID == "" {
		return fmt.Errorf("%w: job id is required", domain.ErrInvalid)
	}
	// R4 (legal-hold gate): defense in depth — never stream a sealed object
	// while any matching hold is active, even if a caller bypassed GetExport.
	// Tenant-scoped job lookup + gate run in one read; on block the
	// export.blocked fact replaces the audit.event.export fact (empty actor
	// keeps the recordReadAction no-fact convention).
	var hold domain.LegalHold
	blocked := false
	if err := s.Store.Read(func(data *store.Snapshot) error {
		value, ok := data.Exports[jobID]
		if !ok || value.TenantID != tenantID {
			return domain.ErrNotFound
		}
		var holdErr error
		hold, blocked, holdErr = s.holdBlockingExport(data, tenantID, value.Query)
		return holdErr
	}); err != nil {
		return err
	}
	if blocked {
		if actor != "" {
			if err := s.Store.AppendAdminFact(s.adminAction(tenantID, actor, domain.AdminActionExportBlocked, "legal_hold", hold.ID, fmt.Sprintf("export_blocked reason=%s", hold.Reason)), s.Config.MaxAdminActions, s.Config.MaxAdminTrailActions); err != nil {
				return err
			}
		}
		return fmt.Errorf("%w", exportBlockedError{holdID: hold.ID})
	}
	return s.recordReadAction(tenantID, actor, domain.AdminActionEventExport, "export", jobID, "download")
}

// exportDigestPattern is the exact shape of domain.HashBytes output
// (lowercase hex-encoded SHA-256). An empty or malformed stored digest
// fails closed: the download choke point must not serve bytes whose
// integrity cannot be established, mirroring verifyContentDigest's
// empty-digest stance (service.go).
var exportDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// VerifyExportDownload is the single service-layer choke point for export
// downloads: it re-looks up the job tenant-scoped, opens the sealed blob
// with the job-derived tenant/job binding, and verifies the decrypted bytes
// against the stored digest. It returns the plaintext JSONL only when every
// check passes; any failure returns an error and no bytes, and appends an
// export.download_rejected self-audit fact so the governance trail is never
// silent about integrity failures (security review F-2).
//
// The binding and digest are derived inside the service from the store
// record, never from transport-supplied strings, so a caller cannot nominate
// a (tenant, job) pair that differs from the authenticated job. Pairing
// invariant: completed jobs are immutable (finishExport runs at most once
// per job; RecoverStuckExports only transitions "running" jobs), so the
// blob the transport fetched at job.ObjectPath and this record's
// binding/digest always describe the same export.
func (s *Service) VerifyExportDownload(tenantID, jobID string, sealed []byte) ([]byte, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return nil, err
	}
	var job domain.ExportJob
	if err := s.Store.Read(func(data *store.Snapshot) error {
		value, ok := data.Exports[jobID]
		if !ok || value.TenantID != tenantID {
			return domain.ErrNotFound
		}
		job = value
		return nil
	}); err != nil {
		return nil, err
	}
	// Defense in depth: the transport already 409s non-completed jobs, but
	// the choke point must not trust transport-supplied identity or status.
	if job.Status != "completed" || job.ObjectPath == "" {
		return nil, fmt.Errorf("%w: export is not completed", domain.ErrConflict)
	}
	plain, err := security.DecryptExport(sealed, s.Config.EncryptionKey, security.ExportBinding(job.TenantID, job.ID))
	if err != nil {
		s.recordExportRejected(job.TenantID, job.ID)
		return nil, err
	}
	if !exportDigestPattern.MatchString(job.Digest) {
		s.recordExportRejected(job.TenantID, job.ID)
		return nil, fmt.Errorf("export %s: stored digest is empty or malformed", job.ID)
	}
	if domain.HashBytes(plain) != job.Digest {
		s.recordExportRejected(job.TenantID, job.ID)
		return nil, fmt.Errorf("export %s: decrypted content digest mismatch", job.ID)
	}
	return plain, nil
}

// recordExportRejected appends the export.download_rejected self-audit fact
// for an integrity-verification failure in VerifyExportDownload. Best-effort:
// a failed append must not mask the verification error it accompanies. The
// fact carries only the job ID — never plaintext, ciphertext or error
// internals. Actor stays empty because VerifyExportDownload does not receive
// transport identity; the fact's purpose is exposing tamper attempts, and
// tenant + job identify the target.
func (s *Service) recordExportRejected(tenantID, jobID string) {
	_ = s.Store.AppendAdminFact(s.adminAction(tenantID, "", domain.AdminActionExportRejected, "export", jobID, "download_verification_failed"), s.Config.MaxAdminActions, s.Config.MaxAdminTrailActions)
}

func (s *Service) CreateLegalHold(hold domain.LegalHold) (domain.LegalHold, error) {
	if err := requireTenantScope(hold.TenantID); err != nil {
		return domain.LegalHold{}, err
	}
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
		s.appendAdminAction(data, s.adminAction(hold.TenantID, hold.CreatedBy, domain.AdminActionLegalHoldCreated, "legal_hold", hold.ID, hold.Reason))
		return nil
	}); err != nil {
		return domain.LegalHold{}, err
	}
	return hold, nil
}

func (s *Service) ListLegalHolds(tenantID string) ([]domain.LegalHold, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return nil, err
	}
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
	if err := requireTenantScope(tenantID); err != nil {
		return domain.LegalHold{}, err
	}
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
		s.appendAdminAction(data, s.adminAction(value.TenantID, releasedBy, domain.AdminActionLegalHoldReleased, "legal_hold", holdID, ""))
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
func (s *Service) CreateAggregateCheckpoint(ctx context.Context, tenantID string) error {
	if err := requireTenantScope(tenantID); err != nil {
		return err
	}
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
		signature, err := s.Config.Signer.Sign(ctx, []byte(root))
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

// isInterrupted reports whether a signer/verifier error means the operation
// was cancelled or timed out rather than producing a verdict. Such errors
// must not be recorded as a false "signature mismatch" fact in the audit
// trail (F-2).
func isInterrupted(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
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
	schemas := map[string]domain.EventSchema{}
	err := s.Store.Read(func(data *store.Snapshot) error {
		var resolveErr error
		events, resolveErr = s.eventsFromSnapshot(ctx, data, tenantID, func(event domain.Event) bool {
			return streamID == "" || event.StreamID == streamID
		})
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
		valid, verifyErr := s.Config.Signer.Verify(ctx, []byte(expected), aggregate.Signature)
		if verifyErr != nil || expected != aggregate.Root || !valid {
			result.Valid = false
			if isInterrupted(verifyErr) {
				// Cancelled/deadline-exceeded verification is not a mismatch:
				// the audit trail must not record a false mismatch fact.
				result.Errors = append(result.Errors, fmt.Sprintf("aggregate checkpoint %s verification interrupted", aggregate.ID))
			} else {
				result.Errors = append(result.Errors, fmt.Sprintf("aggregate checkpoint %s root/signature mismatch", aggregate.ID))
			}
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
func (s *Service) SealPendingSegments(ctx context.Context, tenantID string) error {
	if err := requireTenantScope(tenantID); err != nil {
		return err
	}
	if s.Store.HotCold() {
		return s.sealPendingTenant(ctx, tenantID)
	}
	now := s.Now()
	return s.Store.UpdateChecked(func(data *store.Snapshot) (bool, error) {
		mutated := false
		for key, stream := range data.Streams {
			if stream.TenantID != tenantID || len(stream.PendingHashes) == 0 {
				continue
			}
			segment, checkpoint, err := s.sealSegment(ctx, stream, now)
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

// archivePutTimeout bounds one fire-and-forget export write. runExport has no
// pass context (it outlives the request that created it), so a black-holed
// endpoint must not be able to keep the goroutine (and its finishExport
// write) alive for minutes per object. Mirrors vaultCallBudget's
// detached-cancellation bound. Package-level var (not const) as a test seam:
// export-timeout tests shrink the ceiling instead of waiting on the real 30s
// bound (newArchiveStore precedent).
var archivePutTimeout = 30 * time.Second

// exportTerminalRetryLimit bounds the terminal-persistence attempts in
// finishExport/failExportBlocked. Var (not const) as a test seam: failure
// tests shrink it. The store's own snapshotConflictRetries (3) applies inside
// each UpdateChecked call, so worst-case total Saves =
// (exportTerminalRetryLimit + 1) * (1 + snapshotConflictRetries). Bounded,
// finite, no goroutine spin. Mirrors archivePutTimeout precedent
// (governance.go:573).
var exportTerminalRetryLimit = 3

// ArchivePending retries local WORM-compatible archive writes for events that
// were ledgered/indexed before the archive destination became available.
// It is intentionally idempotent: a byte-identical existing object is treated
// as already archived, while a mismatched or unverifiable object at the key
// surfaces as an error instead of being silently accepted.
//
// Permanent failures are dead-lettered instead of aborting the pass (FM-1 /
// F-2): an archive key that exceeds the backend limits
// (archive.ErrArchiveKeyTooLong) or collides with a byte-different existing
// object (archive.ErrObjectConflict) can never succeed on retry, so the
// event (or sealed segment) is recorded in the tenant's DeadLetters set, its
// receipt carries ErrorCode "archive_dead_letter", and the pass continues
// with the remaining events — one doomed object never wedges the backlog.
// Dead-lettered entries are excluded from later passes. Transient failures
// (store exceptions, unverifiable objects) still abort the pass so the
// worker retries them next pass.
//
// All receipts for events whose Put succeeded in this pass are committed in
// exactly one Store.Update (one optimistic-lock window instead of one per
// event), so a pass fails atomically — either every receipt is marked
// StatusArchived (and every dead letter recorded) or none is. The same
// atomic write resets the tenant's archive-conflict counter, and the
// successful pass's receipts share one timestamp.
func (s *Service) ArchivePending(ctx context.Context, tenantID string) (int, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return 0, err
	}
	if !archive.Configured(s.Config.Archive) {
		return 0, fmt.Errorf("%w: archive directory is not configured", domain.ErrInvalid)
	}
	if s.Store.HotCold() {
		return s.archivePendingTenant(ctx, tenantID)
	}
	var events []domain.Event
	var evictedKeys []string
	var segments []domain.Segment
	if err := s.Store.Read(func(data *store.Snapshot) error {
		for key, event := range data.Events {
			if event.TenantID != tenantID {
				continue
			}
			if _, dead := data.DeadLetters[key]; dead {
				continue // already dead-lettered: never retried
			}
			receipt := data.Receipts[key]
			if receipt.Status == domain.StatusArchived {
				// A legacy snapshot or a recovered two-phase deployment can
				// contain the archived receipt and hot body together. The
				// archive is already verified, so only schedule the safe,
				// idempotent hot-body eviction; never rewrite the WORM object.
				evictedKeys = append(evictedKeys, key)
				continue
			}
			events = append(events, event)
		}
		for key, values := range data.Segments {
			// Exact-component membership (see CreateAggregateCheckpoint); the
			// former StreamKey(tenantID, "") shortcut is subsumed: that key
			// parses as (tenantID, "", true).
			tid, _, ok := store.SplitTenantKey(key)
			if !ok || tid != tenantID {
				continue
			}
			for _, segment := range values {
				// Same dead-letter exclusion as events: a dead-lettered
				// segment (over-limit key, or a byte-different object at its
				// manifest key) can never succeed and is never re-attempted,
				// so one doomed segment cannot churn a Stat/verify (or abort
				// the pass) on every run.
				dlKey, _ := segmentDeadLetterKey(segment)
				if _, dead := data.DeadLetters[dlKey]; dead {
					continue
				}
				segments = append(segments, segment)
			}
		}
		return nil
	}); err != nil {
		return 0, err
	}
	now := s.Now()
	deadLetters := map[string]domain.DeadLetter{}
	for _, segment := range segments {
		if err := s.archiveSegment(ctx, segment); err != nil {
			if !isPermanentArchiveError(err) {
				return 0, err
			}
			key, eventID := segmentDeadLetterKey(segment)
			deadLetters[key] = domain.DeadLetter{TenantID: tenantID, EventID: eventID, StreamID: segment.StreamID, Sequence: segment.FirstSequence, Reason: archiveErrorReason(err), ErrorMessage: boundedErrorMessage(err), At: now}
		}
	}
	archivedEvents := []domain.Event{}
	for _, event := range events {
		if err := s.archiveEvent(ctx, event); err != nil {
			if !isPermanentArchiveError(err) {
				return 0, err
			}
			key := store.EventKey(tenantID, event.EventID)
			deadLetters[key] = domain.DeadLetter{TenantID: tenantID, EventID: event.EventID, StreamID: event.StreamID, Sequence: event.Sequence, Reason: archiveErrorReason(err), ErrorMessage: boundedErrorMessage(err), At: now}
			continue
		}
		archivedEvents = append(archivedEvents, event)
	}
	if len(archivedEvents) == 0 && len(deadLetters) == 0 && len(evictedKeys) == 0 {
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
	if err := s.Store.Update(func(data *store.Snapshot) error {
		for _, key := range evictedKeys {
			if receipt, ok := data.Receipts[key]; ok && receipt.Status == domain.StatusArchived {
				delete(data.Events, key)
			}
		}
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
			// The verified WORM object is now the payload source of truth. Keep
			// the receipt/hash linkage, but evict the immutable event body from
			// the rewritten control-plane snapshot in the same atomic Save.
			delete(data.Events, key)
		}
		for key, dead := range deadLetters {
			data.DeadLetters[key] = dead
			receipt, ok := data.Receipts[key]
			if !ok {
				if isSegmentDeadLetter(dead) {
					continue // segments have no receipt
				}
				return domain.ErrNotFound // event receipt vanished: fail atomically
			}
			receipt.ErrorCode = "archive_dead_letter"
			receipt.ErrorMessage = dead.ErrorMessage
			data.Receipts[key] = receipt
		}
		data.ArchiveConflictFailures[tenantID] = 0
		return nil
	}); err != nil {
		return 0, err
	}
	return len(archivedEvents), nil
}

// isPermanentArchiveError reports whether an archive failure can never
// succeed on retry: the key exceeds the backend limits (FM-1) or a
// byte-different object already occupies the key (F-2). Everything else is
// treated as transient and aborts the pass so the worker retries it.
func isPermanentArchiveError(err error) bool {
	return errors.Is(err, archive.ErrArchiveKeyTooLong) || errors.Is(err, archive.ErrObjectConflict)
}

// archiveErrorReason is the stable dead-letter reason code for a permanent
// archive failure; it is what operators see in ListDeadLetters and the
// receipt record.
func archiveErrorReason(err error) string {
	switch {
	case errors.Is(err, archive.ErrArchiveKeyTooLong):
		return "archive_key_too_long"
	case errors.Is(err, archive.ErrObjectConflict):
		return "archive_object_conflict"
	}
	return "archive_permanent_failure"
}

// boundedErrorMessage caps a dead-letter error message at 512 bytes so a
// stored message never embeds an unbounded attacker-controlled value (the
// FileStore conflict error embeds the archive path, which can be long).
func boundedErrorMessage(err error) string {
	message := err.Error()
	if len(message) > 512 {
		message = message[:512]
	}
	return message
}

// segmentDeadLetterKey returns the DeadLetters map key for a sealed segment
// (segments have no receipt). The synthetic event id embeds store.KeySeparator
// (0x1F), which ValidKeyComponent forbids in real event ids, so a segment
// dead-letter can never alias an event dead-letter in the same map.
func segmentDeadLetterKey(segment domain.Segment) (key, eventID string) {
	eventID = "seg" + store.KeySeparator + segment.StreamID + ":" + fmt.Sprintf("%d-%d", segment.FirstSequence, segment.LastSequence)
	return store.EventKey(segment.TenantID, eventID), eventID
}

// isSegmentDeadLetter reports whether a DeadLetter entry is the synthetic
// segment record (no receipt exists for it).
func isSegmentDeadLetter(dead domain.DeadLetter) bool {
	return strings.HasPrefix(dead.EventID, "seg"+store.KeySeparator)
}

// ListDeadLetters returns the tenant's persisted dead-letter entries (FM-1
// over-limit keys, F-2 byte-conflicting objects) ordered by time then event
// id. This is the operator detection surface: entries whose receipt carries
// ErrorCode "archive_dead_letter" exactly flag the state that will never
// converge without operator action.
func (s *Service) ListDeadLetters(tenantID string) ([]domain.DeadLetter, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return nil, err
	}
	var out []domain.DeadLetter
	err := s.Store.Read(func(data *store.Snapshot) error {
		for _, dead := range data.DeadLetters {
			if dead.TenantID == tenantID {
				out = append(out, dead)
			}
		}
		sort.Slice(out, func(i, j int) bool {
			if !out[i].At.Equal(out[j].At) {
				return out[i].At.Before(out[j].At)
			}
			return out[i].EventID < out[j].EventID
		})
		return nil
	})
	return out, err
}

// ClearDeadLetter removes a dead-letter entry (and the receipt's typed
// error) so the next ArchivePending pass retries the event. Intended for
// operator remediation after the root cause is resolved; the retry
// re-dead-letters the event if the conflict persists, keeping the loop
// bounded.
func (s *Service) ClearDeadLetter(tenantID, eventID string) error {
	if err := requireTenantScope(tenantID); err != nil {
		return err
	}
	return s.Store.Update(func(data *store.Snapshot) error {
		key := store.EventKey(tenantID, eventID)
		delete(data.DeadLetters, key)
		receipt, ok := data.Receipts[key]
		if ok {
			receipt.ErrorCode = ""
			receipt.ErrorMessage = ""
			data.Receipts[key] = receipt
		}
		return nil
	})
}

// RecordArchivePassConflict increments the tenant's persisted archive-pass
// conflict counter. Best-effort: the worker invokes it after ArchivePending
// returned an exhausted ErrSnapshotConflict; if this write itself exhausts
// under sustained load it fails without masking the original error. The
// closure is conflict-safe by construction: Store.Update re-runs it on the
// fresh snapshot, and a failed Save commits nothing, so each committed
// attempt applies exactly one increment.
func (s *Service) RecordArchivePassConflict(tenantID string) error {
	if err := requireTenantScope(tenantID); err != nil {
		return err
	}
	return s.Store.Update(func(data *store.Snapshot) error {
		data.ArchiveConflictFailures[tenantID]++
		return nil
	})
}

// ArchivePassConflictFailures returns the tenant's persisted archive-pass
// conflict counter, for the worker's log line and for tests.
func (s *Service) ArchivePassConflictFailures(tenantID string) (int, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return 0, err
	}
	var count int
	err := s.Store.Read(func(data *store.Snapshot) error {
		count = data.ArchiveConflictFailures[tenantID]
		return nil
	})
	return count, err
}

func (s *Service) PreviewRestore(tenantID string, request domain.RestoreRequest) (domain.RestorePreview, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.RestorePreview{}, err
	}
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
	if err := requireTenantScope(tenantID); err != nil {
		return domain.RestoreRun{}, err
	}
	if request.OperationID == "" || request.Reason == "" {
		return domain.RestoreRun{}, fmt.Errorf("%w: operation_id and reason are required", domain.ErrInvalid)
	}
	if _, err := s.ReplayOperation(tenantID, "", request.OperationID); err != nil {
		return domain.RestoreRun{}, err
	}
	run := domain.RestoreRun{ID: newID("restore"), TenantID: tenantID, OperationID: request.OperationID, Status: domain.RestoreStatusPendingApproval, Reason: request.Reason, CreatedBy: requestedBy, CreatedAt: s.Now()}
	if err := s.Store.Update(func(data *store.Snapshot) error {
		data.RestoreRuns[run.ID] = run
		s.appendAdminAction(data, s.adminAction(tenantID, requestedBy, domain.AdminActionRestoreCreated, "restore", run.ID, request.OperationID))
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
	if err := requireTenantScope(tenantID); err != nil {
		return domain.RestoreRun{}, err
	}
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
			s.appendAdminAction(data, s.adminAction(value.TenantID, actor, domain.AdminActionRestoreApproved, "restore", runID, value.OperationID))
		} else {
			value.Status = domain.RestoreStatusRejected
			value.RejectedBy = actor
			value.RejectedAt = &now
			s.appendAdminAction(data, s.adminAction(value.TenantID, actor, domain.AdminActionRestoreRejected, "restore", runID, value.OperationID))
		}
		data.RestoreRuns[runID] = value
		run = value
		return nil
	})
	return run, err
}

func (s *Service) GetRestore(tenantID, runID string) (domain.RestoreRun, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.RestoreRun{}, err
	}
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
	if err := requireTenantScope(tenantID); err != nil {
		return 0, err
	}
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
			s.appendAdminAction(data, s.adminAction(tenantID, "governance-worker", domain.AdminActionExportRecovered, "export", jobID, fmt.Sprintf("stuck_running_since=%s status=failed", job.CreatedAt.Format(time.RFC3339))))
			recovered++
			mutated = true
		}
		return mutated, nil
	})
	return recovered, err
}

// exportBlockedError is the legal-hold gate denial error. Its text is the
// stable operator-visible message ("export blocked by active legal hold
// <id>") — the exact bytes surfaced in 403 bodies and job.Error — while
// Unwrap reports ErrForbidden so statusForError/errorBody map it to 403
// "forbidden" (AC-1, R3, R4: the error carries only the hold ID, never
// tenant or event details).
type exportBlockedError struct {
	holdID string
}

func (e exportBlockedError) Error() string {
	return "export blocked by active legal hold " + e.holdID
}

func (e exportBlockedError) Unwrap() error { return domain.ErrForbidden }

// holdBlockingExport reports whether an export of tenantID matching query is
// blocked by an active legal hold (ReleasedAt == nil), returning the blocking
// hold with the lowest ID (deterministic over the map-ordered snapshot). A
// hold blocks iff at least one tenant event matches the export query AND is
// covered by the hold under the existing holdMatchesEvent semantics. An
// export selecting zero events is never blocked.
func (s *Service) holdBlockingExport(data *store.Snapshot, tenantID string, query domain.Query) (domain.LegalHold, bool, error) {
	schemas := map[string]domain.EventSchema{}
	for key, schema := range data.Schemas {
		schemas[key] = schema
	}
	events, err := s.eventsFromSnapshot(context.Background(), data, tenantID, func(event domain.Event) bool {
		return s.matches(event, query, schemas)
	})
	if err != nil {
		return domain.LegalHold{}, false, err
	}
	var holds []domain.LegalHold
	for _, hold := range data.LegalHolds {
		if hold.TenantID == tenantID && hold.ReleasedAt == nil {
			holds = append(holds, hold)
		}
	}
	sort.Slice(holds, func(i, j int) bool { return holds[i].ID < holds[j].ID })
	for _, hold := range holds {
		for _, event := range events {
			if s.holdMatchesEvent(hold, event, schemas) {
				return hold, true, nil
			}
		}
	}
	return domain.LegalHold{}, false, nil
}

// failExportBlocked transitions a job to failed because an active legal hold
// covers its query, and appends the export.blocked fact (actor
// "governance-worker"). No archive object is written on this path. The
// transition and fact ride the same atomic Save via persistTerminalExport, so
// the fact is never committed on a no-op (e.g. a concurrent recovery already
// failed the job) and a transient Store error is retried rather than silently
// discarded (FR-1). The legal-hold access-time gate R4 remains authoritative.
func (s *Service) failExportBlocked(jobID string, hold domain.LegalHold) {
	s.persistTerminalExport(jobID, "failed", "", "", 0,
		fmt.Sprintf("export blocked by active legal hold %s", hold.ID),
		func(data *store.Snapshot, job *domain.ExportJob) {
			s.appendAdminAction(data, s.adminAction(job.TenantID, "governance-worker",
				domain.AdminActionExportBlocked, "legal_hold", hold.ID,
				fmt.Sprintf("export_blocked reason=%s", hold.Reason)))
		})
}

func (s *Service) runExport(jobID string) {
	// Preserve the lookup-before-claim ordering: besides avoiding work for a
	// deleted job, this keeps the selection read as a distinct failure boundary
	// for the legal-hold and storage fault-injection seams.
	if err := s.Store.Read(func(data *store.Snapshot) error {
		var ok bool
		_, ok = data.Exports[jobID]
		if !ok {
			return domain.ErrNotFound
		}
		return nil
	}); err != nil {
		return
	}
	job, claimed, err := s.claimPendingExport(jobID)
	if err != nil || !claimed {
		return
	}
	var events []domain.Event
	schemas := map[string]domain.EventSchema{}
	var blockedHold domain.LegalHold
	blocked := false
	err = s.Store.Read(func(data *store.Snapshot) error {
		for key, schema := range data.Schemas {
			schemas[key] = schema
		}
		// R3 (legal-hold gate): re-check at the start of the selection pass; a
		// hold created after CreateExport's gate blocks the seal (no archive
		// write). The block is reported after the read returns — an Update
		// inside a Read closure would self-deadlock on the store RWMutex.
		hold, ok, holdErr := s.holdBlockingExport(data, job.TenantID, job.Query)
		if holdErr != nil {
			return holdErr
		}
		if ok {
			blockedHold, blocked = hold, true
			return nil
		}
		var resolveErr error
		events, resolveErr = s.eventsFromSnapshot(context.Background(), data, job.TenantID, func(event domain.Event) bool {
			return s.matches(event, job.Query, schemas)
		})
		return resolveErr
	})
	if err != nil {
		s.finishExport(jobID, "failed", "", "", 0, err.Error())
		return
	}
	if blocked {
		s.failExportBlocked(jobID, blockedHold)
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
	// 独立加密：导出文件整体以 AES-GCM 密封后写入归档（§14）。v2 密封把
	// 租户/任务派生绑定（ExportBinding）作为 AEAD 关联数据，下载时同一
	// 绑定才能解开 —— 跨任务/跨租户的 blob 交换在 GCM 认证处失败（F8）。
	sealed, sealErr := security.EncryptBytesBound(bytesWritten, s.Config.EncryptionKey, security.ExportBinding(job.TenantID, job.ID))
	if sealErr != nil {
		s.finishExport(jobID, "failed", "", "", 0, sealErr.Error())
		return
	}
	key := fmt.Sprintf("exports/%s.jsonl", safeName(jobID))
	// Per-write bound (REQ-3): runExport has no pass context (it outlives the
	// request that created it), so a black-holed endpoint must not be able to
	// keep the goroutine — and its finishExport write — alive for minutes per
	// object. Mirrors vaultCallBudget's detached-cancellation bound. Failure
	// semantics are unchanged: a timed-out Put fails the job, and a re-request
	// is a fresh CreateExport.
	putCtx, cancel := context.WithTimeout(context.Background(), archivePutTimeout)
	defer cancel()
	if err := s.Config.Archive.Put(putCtx, key, sealed); err != nil {
		s.finishExport(jobID, "failed", "", "", 0, err.Error())
		return
	}
	s.finishExport(jobID, "completed", key, domain.HashBytes(bytesWritten), len(events), "")
}

// persistTerminalExport writes the terminal outcome for jobID, but ONLY when the
// durable record is still terminal-eligible: status == "running" and
// FinishedAt == nil. It is idempotent and safe:
//   - missing job        -> no write, no Save, never created (FR-2)
//   - already terminal    -> no write, no Save (no late overwrite; T3)
//   - worker-recovered     -> no write, no Save (no duplicate export.recovered)
//
// onWrite (may be nil) runs INSIDE the same atomic Save as the state change so
// any admin fact commits atomically (mirrors RecoverStuckExports). On a
// persistent or conflict-exhausted Store.Update error it retries up to
// exportTerminalRetryLimit times; if every attempt fails the durable "running"
// record is deliberately left in place so RecoverStuckExports converges it
// (FR-1 / AC-1). Failures are observed only via bounded, non-sensitive logs
// (FR-7) — never raw error text, payload, path, or credential.
func (s *Service) persistTerminalExport(
	jobID, status, path, digest string, count int, failure string,
	onWrite func(data *store.Snapshot, job *domain.ExportJob),
) {
	for attempt := 0; attempt <= exportTerminalRetryLimit; attempt++ {
		err := s.Store.UpdateChecked(func(data *store.Snapshot) (bool, error) {
			value, ok := data.Exports[jobID]
			if !ok || value.Status != "running" || value.FinishedAt != nil {
				return false, nil // no-op: missing or already terminal (FR-2)
			}
			now := s.Now()
			value.Status = status
			value.FinishedAt = &now
			value.ObjectPath = path
			value.Digest = digest
			value.EventCount = count
			value.Error = failure
			data.Exports[jobID] = value
			if onWrite != nil {
				onWrite(data, &value)
			}
			return true, nil
		})
		if err == nil {
			return // committed, or already-converged no-op
		}
		category := "write"
		if errors.Is(err, store.ErrSnapshotConflict) {
			category = "conflict"
		}
		s.logf("export terminal persistence failed job_id=%s state=%s attempt=%d category=%s",
			jobID, status, attempt+1, category)
	}
	s.logf("export terminal persistence exhausted job_id=%s state=%s attempts=%d category=write",
		jobID, status, exportTerminalRetryLimit+1)
}

func (s *Service) finishExport(jobID, status, path, digest string, count int, failure string) {
	s.persistTerminalExport(jobID, status, path, digest, count, failure, nil)
}

func (s *Service) ListAdminActions(tenantID string, platform bool, limit int) ([]domain.AdminAction, error) {
	if tenantID == "" && !platform {
		return nil, fmt.Errorf("%w: tenant_id is required", domain.ErrInvalid)
	}
	if err := requireOptionalTenantScope(tenantID); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	// Read the trail BEFORE the snapshot so a corrupt trail fails closed
	// without masking the snapshot read, and so the fast path never opens an
	// RLock inside Store.Read (lock-order safety with Store.mu writers).
	trail, err := s.Store.ReadAdminTrail()
	if err != nil {
		return nil, err
	}
	var result []domain.AdminAction
	if len(trail) == 0 {
		// Fast path — no read facts: the verbatim pre-trail loop.
		err = s.Store.Read(func(data *store.Snapshot) error {
			for i := len(data.AdminActions) - 1; i >= 0 && len(result) < limit; i-- {
				action := data.AdminActions[i]
				if (platform && tenantID == "") || action.TenantID == tenantID {
					result = append(result, action)
				}
			}
			return nil
		})
		return result, err
	}
	// Merge path: snapshot mutation facts ∪ read trail, newest-first
	// (equal CreatedAt ⇒ trail first, then reverse-append within source).
	var snapshot []domain.AdminAction
	err = s.Store.Read(func(data *store.Snapshot) error {
		snapshot = data.AdminActions
		return nil
	})
	if err != nil {
		return nil, err
	}
	return mergeAdminActions(snapshot, trail, tenantID, platform, limit), nil
}

// Marshal is kept here as a small compile-time guard that the domain model
// remains JSON serializable for the file store and export path.
