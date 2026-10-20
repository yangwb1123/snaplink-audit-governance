package service

import (
	"context"
	"fmt"
	"sort"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

// A sealed artifact authenticates the original output, not the current source
// bodies used by the Hold gate. Creation/retry preflight, checkpoint
// finalization and detail/download access authenticate committed receipts BEFORE either
// predicate, or corrupted fields could hide an event from its query/Hold.
// data is owned; all remote work stays outside Store.mu and inherits the
// caller's one operation deadline. The original output is never rebuilt.
func (s *Service) holdBlockingExportEvidence(ctx context.Context, data *store.Snapshot, job domain.ExportJob) (domain.LegalHold, bool, error) {
	holds := s.holdBlockingCandidates(data, job.TenantID, job.Query)
	if len(holds) == 0 {
		return domain.LegalHold{}, false, nil
	}
	keys := make([]string, 0)
	for key, receipt := range data.Receipts {
		if err := ctx.Err(); err != nil {
			return domain.LegalHold{}, false, err
		}
		keyTenant, _, validKey := store.SplitTenantKey(key)
		if receipt.TenantID != job.TenantID && (!validKey || keyTenant != job.TenantID) {
			continue
		}
		if receipt.TenantID != job.TenantID || key != store.EventKey(job.TenantID, receipt.EventID) {
			return domain.LegalHold{}, false, fmt.Errorf("%w: current export Hold evidence identity mismatch", domain.ErrConflict)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	blocker := len(holds)
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return domain.LegalHold{}, false, err
		}
		receipt := data.Receipts[key]
		candidate := queryEvidence{receipt: receipt, schemas: data.Schemas}
		if event, hot := data.Events[key]; hot {
			candidate.event = &event
		} else if receipt.Status != domain.StatusArchived {
			return domain.LegalHold{}, false, domain.ErrNotFound
		}
		event, err := s.resolveQueryEvidence(ctx, candidate)
		if err != nil {
			return domain.LegalHold{}, false, err
		}
		schema := data.Schemas[store.SchemaKey(job.TenantID, event.SchemaID, event.SchemaVersion)]
		if schema.TenantID != job.TenantID || schema.SchemaID != event.SchemaID || schema.Version != event.SchemaVersion {
			return domain.LegalHold{}, false, fmt.Errorf("%w: current export Hold schema identity mismatch", domain.ErrConflict)
		}
		// Search indexes are derived metadata excluded from SourceDigest. Use
		// authenticated, exactly reconstructed source values, never a saved
		// digest's value/presence/format to decide that a Hold does not match.
		source, err := s.reconstructEventForDigest(event, data.Schemas)
		if err != nil {
			return domain.LegalHold{}, false, err
		}
		selected, err := s.matchesExportHoldEvidence(source, schema, job.Query)
		if err != nil {
			return domain.LegalHold{}, false, err
		}
		if !selected {
			continue
		}
		for index, hold := range holds[:blocker] {
			matches := hold.Filter.From.IsZero() && hold.Filter.To.IsZero()
			if !matches && !hold.Filter.From.IsZero() && !hold.Filter.To.IsZero() {
				matches, err = s.matchesExportHoldEvidence(source, schema, hold.Filter)
				if err != nil {
					return domain.LegalHold{}, false, err
				}
			}
			if matches {
				blocker = index
				break // retain the lowest-ID matching Hold across all events
			}
		}
	}
	if blocker < len(holds) {
		return holds[blocker], true, nil
	}
	return domain.LegalHold{}, false, nil
}

func (s *Service) matchesExportHoldEvidence(source domain.Event, schema domain.EventSchema, query domain.Query) (bool, error) {
	field, digest := query.PayloadField, query.PayloadDigest
	query.PayloadField, query.PayloadDigest = "", ""
	if !s.matches(source, query, nil) {
		return false, nil
	}
	if field == "" {
		return true, nil
	}
	value, exists := source.Payload[field]
	if !exists || !containsString(schema.SearchableFields, field) {
		return false, nil
	}
	var derived string
	var err error
	if security.IsBoundSearchDigest(digest) {
		derived, err = security.SearchDigestBound(value, s.Config.EncryptionKey, source.TenantID, field)
	} else {
		derived, err = security.SearchDigest(value, s.Config.EncryptionKey)
	}
	return err == nil && derived == digest, err
}
