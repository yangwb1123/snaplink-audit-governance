package store

// The store's tenant-scoped and control-plane API on top of whichever backend
// is configured: tenant read/update, ledger scans, and control-plane
// read/update. On a single-document (v1) store each of these serves the same
// snapshot without a tenant split, which is why the legacy paths live beside
// the split ones instead of behind a separate type.

import (
	"context"

	"github.com/snaplink/audit-governance/internal/domain"
)

// HotCold reports whether this Store uses the v2 tenant/cold-ledger layout.
// It is intentionally read-only; callers use it to select a mutation path
// without inspecting backend implementation details.
func (s *Store) HotCold(ctx context.Context) bool {
	if s.split != nil {
		return true
	}
	if s.pgSplit == nil {
		return false
	}
	enabled, _ := s.pgSplit.enabled(ctx)
	return enabled
}

// ReadTenant executes fn against one tenant without materializing other
// tenants or the cold ledger into a global snapshot.
func (s *Store) ReadTenant(ctx context.Context, tenantID string, fn func(*TenantView) error) (err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	return s.readTenant(op.ctx, tenantID, fn)
}

// readTenant is ReadTenant's body; see Store.read for the nesting rule.
func (s *Store) readTenant(ctx context.Context, tenantID string, fn func(*TenantView) error) error {
	if s.split == nil {
		if s.pgSplit != nil {
			enabled, err := s.pgSplit.enabled(ctx)
			if err != nil {
				return err
			}
			if enabled {
				s.mu.RLock()
				defer s.mu.RUnlock()
				return s.pgSplit.readTenant(ctx, tenantID, fn)
			}
		}
		return s.readLegacyTenant(ctx, tenantID, fn)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.split.readTenant(ctx, tenantID, fn)
}

// UpdateTenant executes a per-tenant hot/cold mutation. The private closure
// is retried by callers when a commit fails; cold appends and hot writes are
// idempotent so a half-completed two-phase pass converges safely.
func (s *Store) UpdateTenant(ctx context.Context, tenantID string, order CommitOrder, fn func(*TenantView) error) (err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	return s.updateTenant(op.ctx, tenantID, order, fn)
}

// updateTenant is UpdateTenant's body; see Store.read for the nesting rule.
func (s *Store) updateTenant(ctx context.Context, tenantID string, order CommitOrder, fn func(*TenantView) error) error {
	if s.split == nil {
		if s.pgSplit != nil {
			enabled, err := s.pgSplit.enabled(ctx)
			if err != nil {
				return err
			}
			if enabled {
				s.mu.Lock()
				defer s.mu.Unlock()
				return s.pgSplit.updateTenant(ctx, tenantID, order, fn)
			}
		}
		return s.updateLegacyTenant(ctx, tenantID, fn)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.split.updateTenant(ctx, tenantID, order, fn)
}

// LedgerScan invokes fn for every committed cold record belonging to a
// tenant. Records are copied and sorted by family/key/version so callers get
// deterministic migration, integrity and export scans.
func (s *Store) LedgerScan(ctx context.Context, tenantID string, fn func(LedgerRecord) error) (err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	return s.ledgerScan(op.ctx, tenantID, fn)
}

// ledgerScan is LedgerScan's body; see Store.read for the nesting rule.
func (s *Store) ledgerScan(ctx context.Context, tenantID string, fn func(LedgerRecord) error) error {
	if s.split == nil {
		if s.pgSplit != nil {
			enabled, err := s.pgSplit.enabled(ctx)
			if err != nil {
				return err
			}
			if enabled {
				s.mu.RLock()
				defer s.mu.RUnlock()
				return s.pgSplit.ledgerScan(ctx, tenantID, fn)
			}
		}
		return s.scanLegacyLedger(ctx, tenantID, fn)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	state := s.split.tenant[tenantID]
	if state == nil {
		return nil
	}
	records := state.ledger.committedRecords()
	sortLedgerRecords(records)
	for _, record := range records {
		if err := fn(record); err != nil {
			return err
		}
	}
	return nil
}

// ReadControl executes fn against a clone of the global hot control document
// only. It never materializes tenant hot documents or cold receipts, segments,
// checkpoints or archived events, so a control-plane reader cannot widen its
// own working set by accident.
func (s *Store) ReadControl(ctx context.Context, fn func(*Snapshot) error) (err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	return s.readControl(op.ctx, fn)
}

// readControl is ReadControl's body; see Store.read for the nesting rule.
func (s *Store) readControl(ctx context.Context, fn func(*Snapshot) error) error {
	if s.split == nil {
		if s.pgSplit != nil {
			enabled, err := s.pgSplit.enabled(ctx)
			if err != nil {
				return err
			}
			if enabled {
				s.mu.RLock()
				defer s.mu.RUnlock()
				return s.pgSplit.readControl(ctx, fn)
			}
		}
		return s.read(ctx, fn)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	control, err := cloneControl(s.split.control)
	if err != nil {
		return err
	}
	return fn(control)
}

// UpdateControl mutates only the global hot control document and never
// materializes cold receipts, segments, checkpoints or archived events.
//
// It takes its own Store.mu hold, so it is a separate store operation from a
// tenant commit and therefore a separate durability boundary. A writer that
// owns control-plane state as part of a tenant commit — the ingest publication
// intent, for example — must set that state on view.Global inside the
// UpdateTenant closure instead, so one store operation carries both.
func (s *Store) UpdateControl(ctx context.Context, fn func(*Snapshot) error) (err error) {
	op := s.begin(ctx)
	defer func() { op.cancel(); err = s.end(op, err) }()
	return s.updateControl(op.ctx, fn)
}

// updateControl is UpdateControl's body; see Store.read for the nesting rule.
func (s *Store) updateControl(ctx context.Context, fn func(*Snapshot) error) error {
	if s.split == nil {
		if s.pgSplit != nil {
			enabled, err := s.pgSplit.enabled(ctx)
			if err != nil {
				return err
			}
			if enabled {
				s.mu.Lock()
				defer s.mu.Unlock()
				return s.pgSplit.updateControl(ctx, fn)
			}
		}
		return s.update(ctx, fn)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	control, err := cloneControl(s.split.control)
	if err != nil {
		return err
	}
	if err := fn(control); err != nil {
		return err
	}
	retained, err := cloneControl(control)
	if err != nil {
		return err
	}
	if s.split.path != "" {
		if s.split.controlWriteFault != nil {
			if err := s.split.controlWriteFault(); err != nil {
				return err
			}
		}
		if err := writeControlFile(s.split.path, retained); err != nil {
			return err
		}
	}
	s.split.control = retained
	return nil
}

func (s *Store) readLegacyTenant(ctx context.Context, tenantID string, fn func(*TenantView) error) error {
	return s.read(ctx, func(data *Snapshot) error {
		view, err := legacyTenantView(data, tenantID)
		if err != nil {
			return err
		}
		return fn(view)
	})
}

func (s *Store) updateLegacyTenant(ctx context.Context, tenantID string, fn func(*TenantView) error) error {
	return s.update(ctx, func(data *Snapshot) error {
		view, err := legacyTenantView(data, tenantID)
		if err != nil {
			return err
		}
		if err := fn(view); err != nil {
			return err
		}
		view.Ledger.commitPending(view.Ledger.pendingRecords())
		return copyTenantView(data, tenantID, view)
	})
}

func (s *Store) scanLegacyLedger(ctx context.Context, tenantID string, fn func(LedgerRecord) error) error {
	return s.read(ctx, func(data *Snapshot) error {
		state, err := splitTenantSnapshot(data, tenantID)
		if err != nil {
			return err
		}
		records := state.ledger.committedRecords()
		sortLedgerRecords(records)
		for _, record := range records {
			if err := fn(record); err != nil {
				return err
			}
		}
		return nil
	})
}

func legacyTenantView(data *Snapshot, tenantID string) (*TenantView, error) {
	hot := emptyTenantHot(tenantID)
	ledger, err := newTenantLedgerFromSnapshot(data, tenantID)
	if err != nil {
		return nil, err
	}
	for key, event := range data.Events {
		if keyBelongsToTenant(key, tenantID) || event.TenantID == tenantID {
			cloned, err := domain.CloneEvent(event)
			if err != nil {
				return nil, err
			}
			hot.Events[key] = cloned
		}
	}
	for key, stream := range data.Streams {
		if keyBelongsToTenant(key, tenantID) || stream.TenantID == tenantID {
			stream.PendingHashes = append([]string(nil), stream.PendingHashes...)
			stream.PendingEvents = append([]string(nil), stream.PendingEvents...)
			hot.Streams[key] = stream
		}
	}
	global, err := cloneControl(data)
	if err != nil {
		return nil, err
	}
	return &TenantView{Hot: hot, Ledger: ledger, Global: global}, nil
}

func newTenantLedgerFromSnapshot(data *Snapshot, tenantID string) (*TenantLedger, error) {
	state, err := splitTenantSnapshot(data, tenantID)
	if err != nil {
		return nil, err
	}
	return state.ledger, nil
}

func copyTenantView(data *Snapshot, tenantID string, view *TenantView) error {
	events, err := cloneEventMap(view.Hot.Events)
	if err != nil {
		return err
	}
	streams := make(map[string]StreamState, len(view.Hot.Streams))
	for key, stream := range view.Hot.Streams {
		stream.PendingHashes = append([]string(nil), stream.PendingHashes...)
		stream.PendingEvents = append([]string(nil), stream.PendingEvents...)
		streams[key] = stream
	}
	for key := range data.Events {
		if keyBelongsToTenant(key, tenantID) {
			delete(data.Events, key)
		}
	}
	for key := range data.Streams {
		if keyBelongsToTenant(key, tenantID) {
			delete(data.Streams, key)
		}
	}
	for key, event := range events {
		data.Events[key] = event
	}
	for key, stream := range streams {
		data.Streams[key] = stream
	}
	for key, receipt := range view.Ledger.receipts {
		data.Receipts[key] = receipt
	}
	for key, segments := range view.Ledger.segments {
		data.Segments[key] = cloneSegments(segments)
	}
	for key, checkpoints := range view.Ledger.checkpoints {
		data.Checkpoints[key] = cloneCheckpoints(checkpoints)
	}
	// A tenant closure may add control-plane delivery state (the ingest
	// publication intent). Commit that delta here too, so the v1 tenant API
	// gives the same single-commit durability as the split backends. Keys are
	// overlaid, never replacing the whole map, so a row another writer added
	// survives; this also matches mergeControlDelta's rule on PostgreSQL. A
	// control key DELETED inside a tenant closure is not propagated: this path
	// has no baseline to tell a deliberate removal from an untouched field,
	// and no closure removes control keys.
	if data.LedgeredOutbox == nil {
		data.LedgeredOutbox = map[string]domain.Event{}
	}
	for key, event := range view.Global.LedgeredOutbox {
		data.LedgeredOutbox[key] = event
	}
	return nil
}
