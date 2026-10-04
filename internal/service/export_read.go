package service

// The sealed-object read behind GET /api/v1/exports/{jobID}/download.
//
// The download route used to call the WORM archive port itself, with the raw
// request context. That was the only Archive.Get in the tree outside the
// bounded service path: nothing capped the wait, and nothing classified the
// failure — a black-holed endpoint pinned the handler until the HTTP write
// timeout, and a deadline consumed by that read arrived at the transport as a
// bare sentinel instead of the retryable class every other bounded read
// produces. This method is the archive read for that route: the port is
// reached here, inside one store operation, through the shared bound.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// ReadExportObject returns the sealed bytes of a completed export job for
// tenantID. It is read-only: it resolves the job tenant-scoped, reads the WORM
// object once and appends no audit fact, no admin action and no job state.
// Decryption and digest verification stay in VerifyExportDownload, which is
// still the choke point that decides whether any plaintext is served.
//
// Tenant binding and existence masking: an unknown job, a job owned by another
// tenant and a tenant-less caller are all refused before any remote call, so
// the route cannot be used to probe another tenant's job ids.
//
// Classification: the read runs INSIDE the store closure, which is what makes
// the failure class correct. archiveReadBound is min(archiveReadTimeout,
// Store.OperationTimeout()) and audit-api validates Store.OperationTimeout
// strictly below its 30s HTTP write timeout
// (runtimeconfig.ValidateStoreOperationTimeoutBelow), so in every shipping
// configuration the derived read deadline can only be equal to or later than
// the store operation's own deadline — the operation bound is exhausted first
// and Store.end, the single classification site, wraps the result as
// store.ErrOperationDeadline. The transports render that as the retryable
// 503 store_timeout this route already declares. Deriving the bound before the
// store call would invert that order and turn the same stall into an opaque
// 500, which is the exact defect this method exists to remove.
//
// A missing object is translated here rather than in the transport: the
// missing-object class belongs to the archive adapter (fs.ErrNotExist for the
// local FileStore, a raw client error for S3), so httpapi needs no filesystem
// taxonomy and derives 404 from the one error mapping table.
func (s *Service) ReadExportObject(ctx context.Context, tenantID, jobID string) ([]byte, error) {
	if err := requireTenantScope(tenantID); err != nil {
		return nil, err
	}
	// A nil or unconfigured archive port must fail closed with an error the
	// transport redacts, never with a nil-interface panic on the read path.
	if !archive.Configured(s.Config.Archive) {
		return nil, fmt.Errorf("export %s: archive is not configured", jobID)
	}
	var object []byte
	err := s.Store.Read(ctx, func(data *store.Snapshot) error {
		job, ok := data.Exports[jobID]
		if !ok || job.TenantID != tenantID {
			return domain.ErrNotFound
		}
		// Same shape as VerifyExportDownload: a job that cannot serve bytes
		// is a conflict, never a silent empty download.
		if job.Status != "completed" || job.ObjectPath == "" {
			return fmt.Errorf("%w: export is not completed", domain.ErrConflict)
		}
		// PLACEMENT IS LOAD-BEARING (see the ReadExportObject doc comment and
		// archiveFallbackContext): the bound is derived INSIDE the closure so
		// the store operation deadline is the one that fires.
		readCtx, cancel := s.archiveFallbackContext(ctx)
		defer cancel()
		// Short-circuit an already-exhausted bound before any remote work: a
		// doomed read must not issue another Get.
		if err := readCtx.Err(); err != nil {
			return fmt.Errorf("export %s: %w", jobID, err)
		}
		sealed, err := s.Config.Archive.Get(readCtx, job.ObjectPath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("%w: export object is missing", domain.ErrNotFound)
			}
			return fmt.Errorf("export %s: %w", jobID, err)
		}
		object = sealed
		return nil
	})
	if err != nil {
		return nil, err
	}
	return object, nil
}
