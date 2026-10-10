package store

import (
	"context"
	"fmt"

	"github.com/snaplink/audit-governance/internal/domain"
)

const MaxLedgerPageSize = 100

// LedgerPageCursor carries backend-local scan coordinates, NOT evidence
// snapshot tokens or commit watermarks. Through is captured on the first page
// to make every sweep finite even while new records arrive faster than scans.
// Writers on the legacy/file replacement path can reorder records, and SQL
// transactions can commit out of id order. Callers must wrap to a zero cursor
// after Done (and restart); confirmations, not positions, determine delivery.
type LedgerPageCursor struct {
	After   int64
	Through int64
}

// Every cold family counts against the page budget, including receipt versions.
type LedgerPage struct {
	Records []LedgerRecord
	Next    LedgerPageCursor
	Done    bool
}

func (s *Store) ReadLedgerPage(ctx context.Context, tenantID string, cursor LedgerPageCursor, limit int) (page LedgerPage, err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	if tenantID == "" || cursor.After < 0 || cursor.Through < 0 ||
		(cursor.Through > 0 && cursor.After > cursor.Through) || limit < 1 || limit > MaxLedgerPageSize {
		return page, fmt.Errorf("%w: invalid ledger page scope or bounds", domain.ErrInvalid)
	}
	if err := ValidTenantID(tenantID); err != nil {
		return page, err
	}
	if s.split != nil {
		s.mu.RLock()
		defer s.mu.RUnlock()
		if err := op.ctx.Err(); err != nil {
			return page, err
		}
		state := s.split.tenant[tenantID]
		if state == nil {
			return LedgerPage{Next: cursor, Done: true}, nil
		}
		return ledgerRecordsPage(state.ledger.records, cursor, limit), nil
	}
	if s.pgSplit != nil {
		enabled, err := s.pgSplit.enabled(op.ctx)
		if err != nil {
			return page, err
		}
		if enabled {
			s.mu.RLock()
			defer s.mu.RUnlock()
			return s.pgSplit.readLedgerPage(op.ctx, tenantID, cursor, limit)
		}
	}
	// Compatibility for the pre-cutover single snapshot only. Production
	// deployments needing bounded historical IO must use the v2 layout.
	err = s.read(op.ctx, func(data *Snapshot) error {
		state, err := splitTenantSnapshot(data, tenantID)
		if err != nil {
			return err
		}
		page = ledgerRecordsPage(state.ledger.records, cursor, limit)
		return nil
	})
	return page, err
}

func ledgerRecordsPage(records []LedgerRecord, cursor LedgerPageCursor, limit int) LedgerPage {
	if cursor.Through == 0 {
		cursor.Through = int64(len(records))
	}
	page := LedgerPage{Next: cursor, Done: true, Records: []LedgerRecord{}}
	endOfSweep := min(cursor.Through, int64(len(records)))
	if cursor.After >= endOfSweep {
		page.Next.After = endOfSweep
		return page
	}
	// Subtraction first avoids overflow even for a caller-supplied cursor
	// near MaxInt64. Returned cursors always point into a finite snapshot.
	end := cursor.After + min(int64(limit), endOfSweep-cursor.After)
	for _, record := range records[cursor.After:end] {
		page.Records = append(page.Records, cloneLedgerRecord(record))
	}
	page.Next.After, page.Done = end, end == endOfSweep
	return page
}

func (p *postgresSplitStore) readLedgerPage(ctx context.Context, tenantID string, cursor LedgerPageCursor, limit int) (LedgerPage, error) {
	page := LedgerPage{Next: cursor, Records: []LedgerRecord{}}
	if page.Next.Through == 0 {
		// audit_ledger_tenant_id_idx supports the tenant-scoped maximum. This
		// is a SCAN horizon, not a commit watermark: a lower id can commit late.
		if err := p.backend.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(id), 0) FROM audit_ledger WHERE tenant_id = $1`, tenantID).Scan(&page.Next.Through); err != nil {
			return page, err
		}
	}
	if page.Next.After >= page.Next.Through {
		page.Next.After = page.Next.Through
		page.Done = true
		return page, nil
	}
	rows, err := p.backend.db.QueryContext(ctx, `SELECT id, tenant_id, record_type, key, version, record
FROM audit_ledger WHERE tenant_id = $1 AND id > $2 AND id <= $3 ORDER BY id LIMIT $4`, tenantID, cursor.After, page.Next.Through, limit)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var sqlTenant, sqlType, sqlKey string
		var version int
		var encoded []byte
		if err := rows.Scan(&id, &sqlTenant, &sqlType, &sqlKey, &version, &encoded); err != nil {
			return page, err
		}
		record, err := decodeLedgerRecord(encoded, tenantID)
		if err != nil {
			return page, err
		}
		if err := validatePostgresLedgerIdentity(record, sqlTenant, LedgerRecordType(sqlType), sqlKey, version); err != nil {
			return page, err
		}
		if id <= page.Next.After || id > page.Next.Through {
			return page, fmt.Errorf("invalid ledger page ordering")
		}
		page.Records = append(page.Records, record)
		page.Next.After = id
	}
	page.Done = len(page.Records) < limit || page.Next.After == page.Next.Through
	return page, rows.Err()
}
