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
	if _, err := s.QueryEvents(tenantID, query); err != nil {
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

func (s *Service) CreateAggregateCheckpoint(tenantID string) error {
	now := s.Now()
	return s.Store.Update(func(data *store.Snapshot) error {
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
			return nil
		}
		sort.Strings(roots)
		root := merkleRoot(roots)
		signature, err := s.Config.Signer.Sign([]byte(root))
		if err != nil {
			return fmt.Errorf("sign aggregate checkpoint: %w", err)
		}
		data.AggregateCheckpoints = append(data.AggregateCheckpoints, domain.AggregateCheckpoint{
			ID: newID("aggregate"), TenantID: tenantID, StreamCount: len(roots),
			Root: root, Signature: signature, Algorithm: s.Config.Signer.Algorithm(),
			CreatedAt: now, StreamRoots: roots,
		})
		return nil
	})
}

func (s *Service) VerifyIntegrity(tenantID, streamID string) (IntegrityResult, error) {
	result := IntegrityResult{Valid: true, TenantID: tenantID, StreamID: streamID, CheckedAt: s.Now()}
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
	return result, nil
}

// SealPendingSegments creates a checkpoint for each non-empty partial stream.
// The ingest path seals full segments; the governance worker calls this method
// periodically so low-volume streams also receive a durable checkpoint.

func (s *Service) SealPendingSegments(tenantID string) error {
	now := s.Now()
	return s.Store.Update(func(data *store.Snapshot) error {
		for key, stream := range data.Streams {
			if stream.TenantID != tenantID || len(stream.PendingHashes) == 0 {
				continue
			}
			segment, checkpoint, err := s.sealSegment(stream, now)
			if err != nil {
				return err
			}
			data.Segments[key] = append(data.Segments[key], segment)
			data.Checkpoints[key] = append(data.Checkpoints[key], checkpoint)
			stream.PendingHashes = nil
			stream.PendingEvents = nil
			stream.PendingPrevHash = ""
			data.Streams[key] = stream
		}
		return nil
	})
}

// ArchivePending retries local WORM-compatible archive writes for events that
// were ledgered/indexed before the archive destination became available.
// It is intentionally idempotent: a byte-identical existing object is treated
// as already archived, while a mismatched or unverifiable object at the key
// surfaces as an error instead of being silently accepted.

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
	archived := 0
	for _, event := range events {
		if err := s.archiveEvent(event); err != nil {
			return archived, err
		}
		if err := s.Store.Update(func(data *store.Snapshot) error {
			key := store.EventKey(tenantID, event.EventID)
			receipt, ok := data.Receipts[key]
			if !ok {
				return domain.ErrNotFound
			}
			now := s.Now()
			receipt.Status = domain.StatusArchived
			receipt.IndexedAt = now
			receipt.ArchivedAt = now
			data.Receipts[key] = receipt
			return nil
		}); err != nil {
			return archived, err
		}
		archived++
	}
	return archived, nil
}

func (s *Service) PreviewRestore(tenantID string, request domain.RestoreRequest) (domain.RestorePreview, error) {
	result, err := s.ReplayOperation(tenantID, request.OperationID)
	if err != nil {
		return domain.RestorePreview{}, err
	}
	return domain.RestorePreview{ID: newID("restore-preview"), TenantID: tenantID, OperationID: request.OperationID, ProposedState: result.State, ExternalCalls: []string{}, RequiresApproval: true}, nil
}

func (s *Service) CreateRestore(tenantID string, request domain.RestoreRequest, requestedBy string) (domain.RestoreRun, error) {
	if request.OperationID == "" || request.Reason == "" {
		return domain.RestoreRun{}, fmt.Errorf("%w: operation_id and reason are required", domain.ErrInvalid)
	}
	if _, err := s.ReplayOperation(tenantID, request.OperationID); err != nil {
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
	events, err := s.eventsFor(job.TenantID, func(event domain.Event) bool { return matches(event, job.Query) })
	if err != nil {
		s.finishExport(jobID, "failed", "", "", 0, err.Error())
		return
	}
	sortEvents(events)
	var bytesWritten []byte
	for _, event := range events {
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
