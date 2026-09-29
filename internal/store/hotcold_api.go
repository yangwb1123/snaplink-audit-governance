package store

// The store's tenant-scoped and control-plane API on top of whichever backend
// is configured: tenant read/update, ledger scans, and control-plane
// read/update. On a single-document (v1) store each of these serves the same
// snapshot without a tenant split, which is why the legacy paths live beside
// the split ones instead of behind a separate type.

import (
	"github.com/snaplink/audit-governance/internal/domain"
)

// HotCold reports whether this Store uses the v2 tenant/cold-ledger layout.
// It is intentionally read-only; callers use it to select a mutation path
// without inspecting backend implementation details.
func (s *Store) HotCold() bool {
	if s.split != nil {
		return true
	}
	if s.pgSplit == nil {
		return false
	}
	enabled, _ := s.pgSplit.enabled()
	return enabled
}

// ReadTenant executes fn against one tenant without materializing other
// tenants or the cold ledger into a global snapshot.
func (s *Store) ReadTenant(tenantID string, fn func(*TenantView) error) error {
	if s.split == nil {
		if s.pgSplit != nil {
			enabled, err := s.pgSplit.enabled()
			if err != nil {
				return err
			}
			if enabled {
				s.mu.RLock()
				defer s.mu.RUnlock()
				return s.pgSplit.readTenant(tenantID, fn)
			}
		}
		return s.readLegacyTenant(tenantID, fn)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.split.readTenant(tenantID, fn)
}

// UpdateTenant executes a per-tenant hot/cold mutation. The private closure
// is retried by callers when a commit fails; cold appends and hot writes are
// idempotent so a half-completed two-phase pass converges safely.
func (s *Store) UpdateTenant(tenantID string, order CommitOrder, fn func(*TenantView) error) error {
	if s.split == nil {
		if s.pgSplit != nil {
			enabled, err := s.pgSplit.enabled()
			if err != nil {
				return err
			}
			if enabled {
				s.mu.Lock()
				defer s.mu.Unlock()
				return s.pgSplit.updateTenant(tenantID, order, fn)
			}
		}
		return s.updateLegacyTenant(tenantID, fn)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.split.updateTenant(tenantID, order, fn)
}

// LedgerScan invokes fn for every committed cold record belonging to a
// tenant. Records are copied and sorted by family/key/version so callers get
// deterministic migration, integrity and export scans.
func (s *Store) LedgerScan(tenantID string, fn func(LedgerRecord) error) error {
	if s.split == nil {
		if s.pgSplit != nil {
			enabled, err := s.pgSplit.enabled()
			if err != nil {
				return err
			}
			if enabled {
				s.mu.RLock()
				defer s.mu.RUnlock()
				return s.pgSplit.ledgerScan(tenantID, fn)
			}
		}
		return s.scanLegacyLedger(tenantID, fn)
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

// UpdateControl mutates only the global hot control document. It is used for
// delivery aids such as LedgeredOutbox after a tenant ledger commit and never
// materializes cold receipts, segments, checkpoints or archived events.
func (s *Store) ReadControl(fn func(*Snapshot) error) error {
	if s.split == nil {
		if s.pgSplit != nil {
			enabled, err := s.pgSplit.enabled()
			if err != nil {
				return err
			}
			if enabled {
				s.mu.RLock()
				defer s.mu.RUnlock()
				return s.pgSplit.readControl(fn)
			}
		}
		return s.Read(fn)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	control, err := cloneControl(s.split.control)
	if err != nil {
		return err
	}
	return fn(control)
}

// UpdateControl mutates only the global hot control document. It is used for
// delivery aids such as LedgeredOutbox after a tenant ledger commit and never
// materializes cold receipts, segments, checkpoints or archived events.
func (s *Store) UpdateControl(fn func(*Snapshot) error) error {
	if s.split == nil {
		if s.pgSplit != nil {
			enabled, err := s.pgSplit.enabled()
			if err != nil {
				return err
			}
			if enabled {
				s.mu.Lock()
				defer s.mu.Unlock()
				return s.pgSplit.updateControl(fn)
			}
		}
		return s.Update(fn)
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
		if err := writeControlFile(s.split.path, retained); err != nil {
			return err
		}
	}
	s.split.control = retained
	return nil
}

func (s *Store) readLegacyTenant(tenantID string, fn func(*TenantView) error) error {
	return s.Read(func(data *Snapshot) error {
		view, err := legacyTenantView(data, tenantID)
		if err != nil {
			return err
		}
		return fn(view)
	})
}

func (s *Store) updateLegacyTenant(tenantID string, fn func(*TenantView) error) error {
	return s.Update(func(data *Snapshot) error {
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

func (s *Store) scanLegacyLedger(tenantID string, fn func(LedgerRecord) error) error {
	return s.Read(func(data *Snapshot) error {
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
	return nil
}
