package service

// Compliance export: creating a job, sealing the evidence set, the sealed
// download (tenant- and job-bound AES-GCM plus a digest over the decrypted
// bytes), running the job, and recovering jobs a crashed API left non-terminal.
// A Legal Hold that overlaps the job's query blocks the export rather than
// letting a hold be exported around.

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

func (s *Service) CreateExport(ctx context.Context, tenantID, requestedBy string, query domain.Query) (domain.ExportJob, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.ExportJob{}, err
	}
	var job domain.ExportJob
	if err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		holdBinding, err := s.prepareExport(opCtx, tenantID, requestedBy, query)
		if err != nil {
			return err
		}
		job = domain.ExportJob{ID: newID("export"), TenantID: tenantID, RequestedBy: requestedBy, Query: query, Status: "pending", CreatedAt: s.Now()}
		// Admission owns only control metadata. Run its checks on every fresh
		// control CAS attempt; a materialized compatibility Update can replace
		// evidence files and reuse a stale control copy after a PG conflict.
		return s.Store.UpdateControl(opCtx, func(data *store.Snapshot) error {
			if err := s.checkExportPreflight(data, tenantID, holdBinding); err != nil {
				return err
			}
			if !exportQueueHasCapacity(data.Exports, tenantID) {
				return fmt.Errorf("%w: export queue capacity exhausted", domain.ErrQuotaExceeded)
			}
			data.Exports[job.ID] = job
			s.appendAdminAction(data, s.adminAction(tenantID, requestedBy, domain.AdminActionExportCreated, "export", job.ID, fmt.Sprintf("from=%s to=%s", query.From.Format(time.RFC3339), query.To.Format(time.RFC3339))))
			return nil
		})
	}); err != nil {
		return domain.ExportJob{}, err
	}
	// The export runs to completion in the background, so it must outlive the
	// request that created it: withoutCancel keeps the job's store operations
	// bounded by the store's own operation deadline (and by the archive
	// client's timeout) instead of being killed the moment the HTTP response
	// is written. Cancellation of the caller's context legitimately abandons
	// the create; it must not abandon a durable export job.
	s.wakeExports(context.WithoutCancel(ctx))
	return job, nil
}

func (s *Service) GetExport(ctx context.Context, tenantID, jobID string) (domain.ExportJob, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return domain.ExportJob{}, err
	}
	var job domain.ExportJob
	err := s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		access, err := s.captureExportAccess(opCtx, tenantID, jobID)
		if err != nil {
			return err
		}
		// Read-only denial, including exports completed before the Hold.
		if access.Blocked {
			return exportBlockedError{holdID: access.Hold.ID}
		}
		job, err = s.revalidateExportAccess(opCtx, access, false)
		return err
	})
	if err != nil {
		return domain.ExportJob{}, err
	}
	return job, nil
}

// RecordExportDownload appends the audit.event.export self-audit fact for a
// completed export being downloaded. Called by the transport after the job
// is verified completed and before the sealed object is streamed; a failed
// append aborts the download (fail-closed).
func (s *Service) RecordExportDownload(ctx context.Context, tenantID, actor, jobID string) error {
	if err := requireTenantScope(tenantID); err != nil {
		return err
	}
	if jobID == "" {
		return fmt.Errorf("%w: job id is required", domain.ErrInvalid)
	}
	// Defense in depth for callers bypassing GetExport. Capture only owned
	// structures under the lock; current evidence, self-audit and the final
	// control recheck share one deadline without holding Store.mu across Get.
	return s.Store.WithOperation(ctx, func(opCtx context.Context) error {
		access, err := s.captureExportAccess(opCtx, tenantID, jobID)
		if err != nil {
			return err
		}
		if access.Blocked {
			if actor != "" {
				hold := access.Hold
				if err := s.Store.AppendAdminFact(opCtx, s.adminAction(tenantID, actor, domain.AdminActionExportBlocked, "legal_hold", hold.ID, fmt.Sprintf("export_blocked reason=%s", hold.Reason)), s.Config.MaxAdminActions, s.Config.MaxAdminTrailActions); err != nil {
					return err
				}
			}
			return exportBlockedError{holdID: access.Hold.ID}
		}
		if _, err := s.revalidateExportAccess(opCtx, access, true); err != nil {
			return err
		}
		if err := s.recordReadAction(opCtx, tenantID, actor, domain.AdminActionEventExport, "export", jobID, "download"); err != nil {
			return err
		}
		// The independent append can block while another replica changes a
		// Hold. A recorded attempt is not proof that any bytes were served.
		_, err = s.revalidateExportAccess(opCtx, access, true)
		return err
	})
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
// blob the service read at job.ObjectPath (ReadExportObject, the only
// archive read on this route) and this record's binding/digest always
// describe the same export.
func (s *Service) VerifyExportDownload(ctx context.Context, tenantID, jobID string, sealed []byte) ([]byte, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return nil, err
	}
	var job domain.ExportJob
	if err := s.Store.Read(ctx, func(data *store.Snapshot) error {
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
		s.recordExportRejected(ctx, job.TenantID, job.ID)
		return nil, err
	}
	if !exportDigestPattern.MatchString(job.Digest) {
		s.recordExportRejected(ctx, job.TenantID, job.ID)
		return nil, fmt.Errorf("export %s: stored digest is empty or malformed", job.ID)
	}
	if domain.HashBytes(plain) != job.Digest {
		s.recordExportRejected(ctx, job.TenantID, job.ID)
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
func (s *Service) recordExportRejected(ctx context.Context, tenantID, jobID string) {
	_ = s.Store.AppendAdminFact(ctx, s.adminAction(tenantID, "", domain.AdminActionExportRejected, "export", jobID, "download_verification_failed"), s.Config.MaxAdminActions, s.Config.MaxAdminTrailActions)
}

// detached-cancellation bound. Package-level var (not const) as a test seam:
// export-timeout tests shrink the ceiling instead of waiting on the real 30s
// bound (newArchiveStore precedent).
var exportPutTimeout = 30 * time.Second

// exportTerminalRetryLimit bounds the terminal-persistence attempts in
// finishExport/failExportBlocked. Var (not const) as a test seam: failure
// tests shrink it. The store's own snapshotConflictRetries (3) applies inside
// each UpdateChecked call, so worst-case total Saves =
// (exportTerminalRetryLimit + 1) * (1 + snapshotConflictRetries). Bounded,
// finite, no goroutine spin. Mirrors archivePutTimeout precedent
// (governance.go:573).
var exportTerminalRetryLimit = 3

// ArchivePending retries local WORM-compatible archive writes for events that
// were ledgered/indexed before the archive destination became available. Each
// object Put is bounded by archivePutTimeout in service.go, while the caller
// context can cancel it sooner.
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

// RecoverStuckExports atomically fails expired uncheckpointed leased jobs,
// or legacy jobs past StuckExportAge (StartedAt, otherwise CreatedAt). Valid
// selected/sealed checkpoints remain running for fenced dispatcher continuation, not
// reported as completed recovery. Never requeues a terminal job. Transitions
// and export.recovered share the control CAS; no archive I/O or partial count.
func (s *Service) RecoverStuckExports(ctx context.Context, tenantID string) (int, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return 0, err
	}
	return s.recoverExportJobs(ctx, tenantID, true)
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

// LegalHoldID exposes a stable machine-readable denial reason to transports
// without making them parse the diagnostic text or import a service error type.
func (e exportBlockedError) LegalHoldID() string { return e.holdID }

// holdBlockingExport reports whether an export of tenantID matching query is
// blocked by an active legal hold (ReleasedAt == nil), returning the blocking
// hold with the lowest ID (deterministic over the map-ordered snapshot). A
// hold blocks iff at least one tenant event matches the export query AND is
// covered by the hold under the existing holdMatchesEvent semantics. An
// export selecting zero events is never blocked.
//
// The candidate set is proven before any event is touched: it reads only the
// in-memory control-plane hold map, so an empty result is a complete and
// cost-free proof that the export is not blocked and no schema map is copied,
// no event is cloned and no WORM object is read. Every surviving hold is then
// evaluated exactly as before, so the returned (hold, blocked) pair, the
// denial text and the reported hold ID are unchanged.
//
// ctx is the caller's store-operation context, threaded so the archived-event
// pass below inherits the store bound; it only reaches eventsFromSnapshot.
func (s *Service) holdBlockingExport(ctx context.Context, data *store.Snapshot, tenantID string, query domain.Query) (domain.LegalHold, bool, error) {
	holds := s.holdBlockingCandidates(data, tenantID, query)
	if len(holds) == 0 {
		return domain.LegalHold{}, false, nil
	}
	schemas := map[string]domain.EventSchema{}
	for key, schema := range data.Schemas {
		schemas[key] = schema
	}
	events, err := s.eventsFromSnapshot(ctx, data, tenantID, func(event domain.Event) bool {
		return s.matches(event, query, schemas)
	})
	if err != nil {
		return domain.LegalHold{}, false, err
	}
	for _, hold := range holds {
		for _, event := range events {
			if s.holdMatchesEvent(hold, event, schemas) {
				return hold, true, nil
			}
		}
	}
	return domain.LegalHold{}, false, nil
}

// holdBlockingCandidates returns the tenant's active holds (ReleasedAt == nil)
// that can still block an export of query, ordered by hold ID. It reads only
// the in-memory control-plane hold map: no event, receipt, archive object or
// store call, so an empty result is a complete and cost-free proof that the
// export is not blocked. A dropped hold provably cannot match any event the
// query selects, so the candidate list is a subsequence of the sorted active
// list and the first matching hold is the same hold as before (lowest-ID
// determinism, TestMultipleHoldsReportLowestID).
func (s *Service) holdBlockingCandidates(data *store.Snapshot, tenantID string, query domain.Query) []domain.LegalHold {
	holds := make([]domain.LegalHold, 0, len(data.LegalHolds))
	for _, hold := range data.LegalHolds {
		if hold.TenantID == tenantID && hold.ReleasedAt == nil && holdCanSelectAnySelected(hold, query) {
			holds = append(holds, hold)
		}
	}
	sort.Slice(holds, func(i, j int) bool { return holds[i].ID < holds[j].ID })
	return holds
}

// failExportBlocked transitions a job to failed because an active legal hold
// covers its query, and appends the export.blocked fact (actor
// "governance-worker"). No archive object is written on this path. The
// transition and fact ride the same atomic Save via persistTerminalExport, so
// the fact is never committed on a no-op (e.g. a concurrent recovery already
// failed the job) and a transient Store error is retried rather than silently
// discarded (FR-1). The legal-hold access-time gate R4 remains authoritative.
func (s *Service) failExportBlocked(ctx context.Context, jobID string, hold domain.LegalHold) {
	s.persistTerminalExport(ctx, jobID, "failed", "", "", 0,
		fmt.Sprintf("export blocked by active legal hold %s", hold.ID),
		func(data *store.Snapshot, job *domain.ExportJob) {
			job.FailureCode = domain.ExportFailureLegalHold
			s.appendAdminAction(data, s.adminAction(job.TenantID, "governance-worker",
				domain.AdminActionExportBlocked, "legal_hold", hold.ID,
				fmt.Sprintf("export_blocked reason=%s", hold.Reason)))
		})
}

func (s *Service) runExport(ctx context.Context, jobID string) {
	// Preserve the lookup-before-claim ordering: besides avoiding work for a
	// deleted job, this keeps the selection read as a distinct failure boundary
	// for the legal-hold and storage fault-injection seams.
	if err := s.Store.Read(ctx, func(data *store.Snapshot) error {
		var ok bool
		_, ok = data.Exports[jobID]
		if !ok {
			return domain.ErrNotFound
		}
		return nil
	}); err != nil {
		return
	}
	job, claimed, err := s.claimExport(ctx, jobID, true)
	if err != nil || !claimed {
		return
	}
	ctx, stopHeartbeat := s.startExportHeartbeat(ctx, job)
	defer stopHeartbeat()
	if job.Checkpoint.Version != 0 {
		s.finalizeExportCheckpoint(ctx, job, nil)
		return
	}
	job, events, ready := s.exportExecutionEvidence(ctx, job)
	if !ready {
		return
	}
	ctx = exportClaimContext(ctx, job)
	var bytesWritten []byte
	if job.Plan.Version == 2 {
		job, bytesWritten, ready = s.generateExportPages(ctx, job)
		if !ready {
			return
		}
		ctx = exportClaimContext(ctx, job)
	} else {
		var err error
		bytesWritten, err = encodeExportEvents(ctx, events)
		if err != nil {
			s.finishExport(ctx, jobID, "failed", "", "", 0, err.Error())
			return
		}
	}
	// 独立加密：导出文件整体以 AES-GCM 密封后写入归档（§14）。v2 密封把
	// 租户/任务派生绑定（ExportBinding）作为 AEAD 关联数据，下载时同一
	// 绑定才能解开 —— 跨任务/跨租户的 blob 交换在 GCM 认证处失败（F8）。
	sealed, sealErr := security.EncryptBytesBound(bytesWritten, s.Config.EncryptionKey, security.ExportBinding(job.TenantID, job.ID))
	if sealErr != nil {
		s.finishExport(ctx, jobID, "failed", "", "", 0, sealErr.Error())
		return
	}
	if err := s.checkExportPlan(ctx, job); err != nil {
		return
	}
	// Attempt-specific WORM identity fences stale writers from another
	// attempt's object. A late external Put may still leave an unreferenced
	// sealed object; only a valid current control lease can publish its path.
	key := exportObjectKey(job)
	// Per-write bound (REQ-3): runExport has no pass context (it outlives the
	// request that created it), so a black-holed endpoint must not be able to
	// keep the goroutine — and its finishExport write — alive for minutes per
	// object. Mirrors vaultCallBudget's detached-cancellation bound. Failure
	// semantics are unchanged: a timed-out Put fails the job, and a re-request
	// is a fresh CreateExport.
	putCtx, cancel := context.WithTimeout(ctx, exportPutTimeout)
	defer cancel()
	if err := s.Config.Archive.Put(putCtx, key, sealed); err != nil {
		s.finishExport(ctx, jobID, "failed", "", "", 0, err.Error())
		return
	}
	checkpoint, err := s.saveExportCheckpoint(ctx, job, sealed, bytesWritten, len(job.Plan.Members))
	if err != nil {
		s.logf("export checkpoint persistence deferred job_id=%s category=control", jobID)
		return // an unknown Save may have committed; do not guess or reseal
	}
	job.Checkpoint = checkpoint
	job.Plan = store.ExportPlan{} // consumed atomically by the sealed checkpoint
	s.finalizeExportCheckpoint(exportClaimContext(ctx, job), job, sealed)
}

// persistTerminalExport writes the terminal outcome for jobID, but ONLY when the
// durable record is still terminal-eligible: running, unfinished and owned
// by this exact unexpired claim from ctx. It is idempotent and safe:
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
func (s *Service) persistTerminalExport(ctx context.Context,
	jobID, status, path, digest string, count int, failure string,
	onWrite func(data *store.Snapshot, job *domain.ExportJob),
) {
	claim, owned := exportClaimFromContext(ctx)
	if !owned || claim.ID != jobID {
		return // never infer ownership by looking up another executor's lease
	}
	for attempt := 0; attempt <= exportTerminalRetryLimit; attempt++ {
		err := s.Store.UpdateControlChecked(ctx, func(data *store.Snapshot) (bool, error) {
			value, ok := data.Exports[jobID]
			now := s.Now()
			if !ok || !ownsExportLease(data, value, claim, now) {
				return false, nil // missing/terminal/expired/wrong-owner is a no-op
			}
			_, planned := data.ExportPlans[jobID]
			if !s.exportPlanMatches(data, value, claim) || (planned && status == "completed") {
				return false, errExportPlanInvalid
			}
			if !s.terminalExportCheckpointMatches(data, value, claim, status, path, digest, count) {
				return false, errExportCheckpointInvalid
			}
			_, checkpointed := data.ExportCheckpoints[jobID]
			if checkpointed && status == "completed" {
				s.appendAdminAction(data, s.adminAction(value.TenantID, "governance-worker", domain.AdminActionExportCompleted, "export", jobID, "stage=sealed status=completed"))
			}
			value.Status = status
			value.FinishedAt = &now
			value.ObjectPath = path
			value.Digest = digest
			value.EventCount = count
			value.Error = failure
			value.FailureCode = ""
			if status == "failed" {
				value.FailureCode = domain.ExportFailureExecution
			}
			delete(data.ExportLeases, jobID)
			delete(data.ExportCheckpoints, jobID)
			delete(data.ExportPlans, jobID)
			if onWrite != nil {
				onWrite(data, &value)
			}
			data.Exports[jobID] = value
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

func (s *Service) finishExport(ctx context.Context, jobID, status, path, digest string, count int, failure string) {
	s.persistTerminalExport(ctx, jobID, status, path, digest, count, failure, nil)
}
