package store

// Committing one tenant mutation under an explicit commit order: the hot
// document, the ledger records the order makes durable, and the control-plane
// delta the tenant owns. A tenant commit never rewrites another tenant's file
// and never rewrites the control plane wholesale.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"

	"github.com/snaplink/audit-governance/internal/domain"
)

func keyBelongsToTenant(key, tenantID string) bool {
	owner, _, ok := SplitTenantKey(key)
	return ok && owner == tenantID
}

func (s *splitStore) readTenant(ctx context.Context, tenantID string, fn func(*TenantView) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	state := s.tenant[tenantID]
	if state == nil {
		state = &tenantState{hot: emptyTenantHot(tenantID), ledger: NewTenantLedger(tenantID), version: 1}
	}
	hot, err := cloneTenantHot(state.hot)
	if err != nil {
		return err
	}
	global, err := cloneControl(s.control)
	if err != nil {
		return err
	}
	view := &TenantView{Hot: hot, Ledger: newTenantLedgerView(state.ledger), Global: global}
	if err := fn(view); err != nil {
		return err
	}
	return view.Ledger.Err()
}

func (s *splitStore) updateTenant(ctx context.Context, tenantID string, order CommitOrder, fn func(*TenantView) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	state := s.tenantState(tenantID)
	hot, err := cloneTenantHot(state.hot)
	if err != nil {
		return err
	}
	ledger := newTenantLedgerView(state.ledger)
	global, err := cloneControl(s.control)
	if err != nil {
		return err
	}
	view := &TenantView{Hot: hot, Ledger: ledger, Global: global}
	if err := fn(view); err != nil {
		ledger.clearPending()
		return err
	}
	if err := ledger.Err(); err != nil {
		ledger.clearPending()
		return err
	}
	retainedHot, err := cloneTenantHot(view.Hot)
	if err != nil {
		ledger.clearPending()
		return err
	}
	retainedGlobal, err := cloneControl(view.Global)
	if err != nil {
		ledger.clearPending()
		return err
	}
	pending := ledger.pendingRecords()
	globalChanged := !reflect.DeepEqual(retainedGlobal, s.control)
	if err := s.commitTenant(ctx, tenantID, order, retainedHot, ledger, pending, retainedGlobal, globalChanged); err != nil {
		return err
	}
	return nil
}

func (s *splitStore) commitTenant(ctx context.Context, tenantID string, order CommitOrder, hot *Snapshot, ledger *TenantLedger, pending []LedgerRecord, global *Snapshot, globalChanged bool) error {
	if err := checkControlTempPath(s.path); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	state := s.tenantState(tenantID)
	if order == ColdFirst {
		if err := s.appendLedger(ctx, tenantID, state.ledger, pending); err != nil {
			return err
		}
		if err := s.saveTenantHot(tenantID, hot, state.version+1); err != nil {
			return err
		}
		// The cold append is already durable when the hot save succeeds. Keep
		// the in-memory state in the same post-commit shape before any later
		// control-plane write can fail.
		state.hot = hot
		state.version++
	} else {
		if err := s.saveTenantHot(tenantID, hot, state.version+1); err != nil {
			return err
		}
		// HotFirst deliberately exposes the hot commit before the cold append.
		// If the append then fails, the persisted hot file must still be visible
		// to the next retry; otherwise this process would retry from a stale
		// in-memory stream and could derive the wrong next sequence.
		state.hot = hot
		state.version++
		if err := s.appendLedger(ctx, tenantID, state.ledger, pending); err != nil {
			return err
		}
	}
	if globalChanged {
		if s.path != "" {
			// The hot document and the cold ledger are already durable when
			// this write runs. A failure here therefore drops only the
			// control-plane delta — the residual crash window that the
			// service's lost-intent repair compensates for.
			if s.controlWriteFault != nil {
				if err := s.controlWriteFault(); err != nil {
					return err
				}
			}
			if err := writeControlFile(s.path, global); err != nil {
				return err
			}
		}
		s.control = global
	}
	state.ledger.commitPending(pending)
	return nil
}

func checkControlTempPath(path string) error {
	if path == "" {
		return nil
	}
	info, err := os.Stat(path + ".tmp")
	if err == nil && info.IsDir() {
		return fmt.Errorf("state temporary path %s is a directory", path+".tmp")
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s *splitStore) appendLedger(ctx context.Context, tenantID string, committed *TenantLedger, pending []LedgerRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}
	toAppend, err := committed.recordsToAppend(pending)
	if err != nil {
		return err
	}
	if s.path != "" && len(toAppend) > 0 {
		if err := appendLedgerFile(s.root, tenantID, toAppend); err != nil {
			return err
		}
	}
	committed.commitPending(pending)
	return nil
}

func (s *splitStore) saveTenantHot(tenantID string, hot *Snapshot, version int64) error {
	hot.LayoutVersion = hotColdLayoutVersion
	if s.path != "" {
		return writeTenantFile(s.root, tenantID, tenantHotSnapshot{TenantID: tenantID, Version: version, Events: hot.Events, Streams: hot.Streams})
	}
	return nil
}

func (s *splitStore) persistPrepared(prepared *preparedSplit) error {
	if s.path != "" {
		ids := make([]string, 0, len(prepared.tenant))
		for tenantID := range prepared.tenant {
			ids = append(ids, tenantID)
		}
		sort.Strings(ids)
		for _, tenantID := range ids {
			state := prepared.tenant[tenantID]
			if err := writeTenantFile(s.root, tenantID, tenantHotSnapshot{TenantID: tenantID, Version: state.version, Events: state.hot.Events, Streams: state.hot.Streams}); err != nil {
				return err
			}
			if err := replaceLedgerFile(s.root, tenantID, state.ledger.committedRecords()); err != nil {
				return err
			}
		}
		if err := writeControlFile(s.path, prepared.control); err != nil {
			return err
		}
	}
	return nil
}

func cloneControl(control *Snapshot) (*Snapshot, error) {
	clone, err := cloneSnapshot(control)
	if err != nil {
		return nil, err
	}
	clone.Events = map[string]domain.Event{}
	clone.Receipts = map[string]domain.EventReceipt{}
	clone.Streams = map[string]StreamState{}
	clone.Segments = map[string][]domain.Segment{}
	clone.Checkpoints = map[string][]domain.Checkpoint{}
	return clone, nil
}

func cloneTenantHot(hot *Snapshot) (*Snapshot, error) {
	clone := emptyTenantHot("")
	var err error
	clone.Events, err = cloneEventMap(hot.Events)
	if err != nil {
		return nil, err
	}
	for key, stream := range hot.Streams {
		stream.PendingHashes = append([]string(nil), stream.PendingHashes...)
		stream.PendingEvents = append([]string(nil), stream.PendingEvents...)
		clone.Streams[key] = stream
	}
	return clone, nil
}
