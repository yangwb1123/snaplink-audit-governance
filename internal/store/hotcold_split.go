package store

// The v2 layout: what the split between the control plane, per-tenant hot
// documents and the append-only tenant ledgers means on disk. newSplitStore
// validates that the layout on disk matches the control snapshot it is opened
// with (a missing hot file, cold file or temporary file refuses to start), and
// splitSnapshot turns one control snapshot into the per-tenant hot and ledger
// halves a commit writes.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/fsutil"
)

const hotColdLayoutVersion = 2

type tenantHotSnapshot struct {
	TenantID      string                  `json:"tenant_id"`
	Version       int64                   `json:"version"`
	Events        map[string]domain.Event `json:"events"`
	Streams       map[string]StreamState  `json:"streams"`
	PendingLedger []LedgerRecord          `json:"pending_ledger,omitempty"`
}

type tenantState struct {
	hot     *Snapshot
	ledger  *TenantLedger
	version int64
	pending []LedgerRecord
}

type splitStore struct {
	mu      sync.RWMutex
	path    string
	root    string
	control *Snapshot
	tenant  map[string]*tenantState
	// controlWriteFault is nil in production. When set (OpenWithControlWriteFault)
	// it is consulted before every durable control-plane write of this store.
	controlWriteFault ControlWriteFault
	ledgerWriteFault  func() error
}

// newSplitStore opens the v2 layout. A v1 file is migrated before the
// returned store is exposed; in-memory stores begin directly in v2 shape.
func newSplitStore(data *Snapshot, path string) (*splitStore, error) {
	if data == nil {
		data = NewSnapshot()
	}
	stateExists := false
	if path != "" {
		var err error
		stateExists, err = fileHasContent(path)
		if err != nil {
			return nil, err
		}
	}
	if stateExists && data.LayoutVersion < hotColdLayoutVersion {
		if err := migrateV1File(path, data); err != nil {
			return nil, err
		}
	}
	if !stateExists && data.LayoutVersion < hotColdLayoutVersion {
		data.LayoutVersion = hotColdLayoutVersion
	}
	control, err := controlSnapshot(data)
	if err != nil {
		return nil, err
	}
	s := &splitStore{path: path, root: filepath.Dir(path), control: control, tenant: map[string]*tenantState{}}
	if path == "" {
		s.root = ""
	}
	if path == "" {
		return s, nil
	}
	if data.LayoutVersion >= hotColdLayoutVersion {
		if err := validateV2Layout(path, data); err != nil {
			return nil, err
		}
	}
	if err := s.loadFiles(); err != nil {
		return nil, err
	}
	return s, nil
}

func validateV2Layout(path string, control *Snapshot) error {
	root := filepath.Dir(path)
	for tenantID := range control.Tenants {
		if _, err := fsutil.StatDataFile(tenantFilePath(root, tenantID)); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("v2 layout is incomplete: tenant %q hot file is missing", tenantID)
			}
			return err
		}
		if _, err := fsutil.StatDataFile(ledgerFilePath(root, tenantID)); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("v2 layout is incomplete: tenant %q ledger file is missing", tenantID)
			}
			return err
		}
	}
	for _, directory := range []string{filepath.Join(root, "tenants"), filepath.Join(root, "ledger")} {
		entries, err := fsutil.ReadDataDir(directory)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".tmp" {
				continue
			}
			return fmt.Errorf("v2 layout is incomplete: temporary sibling %s remains", filepath.Join(directory, entry.Name()))
		}
	}
	return nil
}

func fileHasContent(path string) (bool, error) {
	info, err := fsutil.StatDataFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return info.Size() > 0, nil
}

// splitFileEvidence inspects sibling v2 files without opening the store. It
// is used by the file-to-Postgres cutover guard because a v2 control document
// intentionally contains neither hot events nor cold receipts.
func splitFileEvidence(root string) (int, int, error) {
	hotEvents, ledgerRecords := 0, 0
	for _, kind := range []string{"tenants", "ledger"} {
		entries, err := fsutil.ReadDataDir(filepath.Join(root, kind))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, 0, err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			path := filepath.Join(root, kind, entry.Name())
			raw, err := fsutil.ReadDataFile(path)
			if err != nil {
				return 0, 0, err
			}
			if len(bytes.TrimSpace(raw)) == 0 {
				continue
			}
			if kind == "ledger" {
				count, err := countLedgerRecords(raw)
				if err != nil {
					return 0, 0, fmt.Errorf("decode %s: %w", path, err)
				}
				ledgerRecords += count
				continue
			}
			var hot tenantHotSnapshot
			if err := decodeJSON(raw, &hot); err != nil {
				return 0, 0, fmt.Errorf("decode %s: %w", path, err)
			}
			hotEvents += len(hot.Events)
		}
	}
	return hotEvents, ledgerRecords, nil
}

func countLedgerRecords(raw []byte) (int, error) {
	count := 0
	for _, line := range bytes.Split(raw, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var record LedgerRecord
		if err := decodeJSON(line, &record); err != nil {
			return 0, err
		}
		if err := validateLedgerRecord(record); err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

func controlSnapshot(data *Snapshot) (*Snapshot, error) {
	control, err := cloneSnapshot(data)
	if err != nil {
		return nil, err
	}
	control.LayoutVersion = hotColdLayoutVersion
	control.Events = map[string]domain.Event{}
	control.Receipts = map[string]domain.EventReceipt{}
	control.Streams = map[string]StreamState{}
	control.Segments = map[string][]domain.Segment{}
	control.Checkpoints = map[string][]domain.Checkpoint{}
	return control, nil
}

func emptyTenantHot(tenantID string) *Snapshot {
	data := NewSnapshot()
	data.LayoutVersion = hotColdLayoutVersion
	data.Tenants = map[string]domain.Tenant{}
	data.Sources = map[string]domain.SourceSystem{}
	data.Schemas = map[string]domain.EventSchema{}
	data.Policies = map[string]domain.RetentionPolicy{}
	data.LegalHolds = map[string]domain.LegalHold{}
	data.Exports = map[string]domain.ExportJob{}
	data.ExportLeases = nil
	data.ExportCheckpoints = nil
	data.ExportPlans = nil
	data.RestoreRuns = map[string]domain.RestoreRun{}
	data.LedgeredOutbox = map[string]domain.Event{}
	data.AdminActions = nil
	data.AggregateCheckpoints = nil
	data.ArchiveConflictFailures = nil
	data.DeadLetters = nil
	_ = tenantID
	return data
}

func (s *splitStore) tenantState(tenantID string) *tenantState {
	state, ok := s.tenant[tenantID]
	if ok {
		return state
	}
	state = &tenantState{hot: emptyTenantHot(tenantID), ledger: NewTenantLedger(tenantID), version: 1}
	s.tenant[tenantID] = state
	return state
}

func (s *splitStore) loadFiles() error {
	ids := s.tenantIDs()
	fileIDs, err := tenantIDsFromFiles(s.root)
	if err != nil {
		return err
	}
	for _, tenantID := range fileIDs {
		ids = appendUniqueString(ids, tenantID)
	}
	sort.Strings(ids)
	for _, tenantID := range ids {
		state, err := loadTenantState(s.root, tenantID)
		if err != nil {
			return err
		}
		s.tenant[tenantID] = state
	}
	return nil
}

func tenantIDsFromFiles(root string) ([]string, error) {
	seen := map[string]struct{}{}
	for _, directory := range []string{filepath.Join(root, "tenants"), filepath.Join(root, "ledger")} {
		entries, err := fsutil.ReadDataDir(directory)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			tenantID, err := tenantIDFromLayoutFile(filepath.Join(directory, entry.Name()))
			if err != nil {
				return nil, fmt.Errorf("decode tenant file %s: %w", entry.Name(), err)
			}
			if tenantID != "" {
				seen[tenantID] = struct{}{}
			}
		}
	}
	ids := make([]string, 0, len(seen))
	for tenantID := range seen {
		ids = append(ids, tenantID)
	}
	return ids, nil
}

func tenantIDFromLayoutFile(path string) (string, error) {
	raw, err := fsutil.ReadDataFile(path)
	if err != nil {
		return "", err
	}
	if filepath.Ext(path) == ".json" {
		var disk tenantHotSnapshot
		if err := decodeJSON(raw, &disk); err != nil {
			return "", err
		}
		return disk.TenantID, nil
	}
	for _, line := range bytes.Split(raw, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var record LedgerRecord
		if err := decodeJSON(line, &record); err != nil {
			return "", err
		}
		return record.TenantID, nil
	}
	return "", nil
}

func appendUniqueString(values []string, value string) []string {
	for _, current := range values {
		if current == value {
			return values
		}
	}
	return append(values, value)
}

func (s *splitStore) tenantIDs() []string {
	seen := map[string]struct{}{}
	for tenantID := range s.control.Tenants {
		seen[tenantID] = struct{}{}
	}
	for key := range s.control.LedgeredOutbox {
		if tenantID, _, ok := SplitTenantKey(key); ok {
			seen[tenantID] = struct{}{}
		}
	}
	ids := make([]string, 0, len(seen))
	for tenantID := range seen {
		ids = append(ids, tenantID)
	}
	sort.Strings(ids)
	return ids
}

func (s *splitStore) materialize(ctx context.Context) (*Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result, err := cloneSnapshot(s.control)
	if err != nil {
		return nil, err
	}
	for tenantID, state := range s.tenant {
		hot, err := cloneTenantHot(state.hot)
		if err != nil {
			return nil, err
		}
		for key, event := range hot.Events {
			result.Events[key] = event
		}
		for key, stream := range hot.Streams {
			result.Streams[key] = stream
		}
		for key, receipt := range state.ledger.receipts {
			result.Receipts[key] = receipt
		}
		for key, segments := range state.ledger.segments {
			result.Segments[key] = cloneSegments(segments)
		}
		for key, checkpoints := range state.ledger.checkpoints {
			result.Checkpoints[key] = cloneCheckpoints(checkpoints)
		}
		if _, ok := result.Tenants[tenantID]; !ok && tenantID != "" {
			result.Tenants[tenantID] = domain.Tenant{ID: tenantID}
		}
	}
	result.LayoutVersion = hotColdLayoutVersion
	result.normalize()
	return result, nil
}

func (s *splitStore) read(ctx context.Context, fn func(*Snapshot) error) error {
	data, err := s.materialize(ctx)
	if err != nil {
		return err
	}
	return fn(data)
}

func (s *splitStore) update(ctx context.Context, fn func(*Snapshot) error) error {
	before, err := s.materialize(ctx)
	if err != nil {
		return err
	}
	working, err := cloneSnapshot(before)
	if err != nil {
		return err
	}
	if err := fn(working); err != nil {
		return err
	}
	return s.replaceFromSnapshot(working)
}

func (s *splitStore) updateChecked(ctx context.Context, fn func(*Snapshot) (bool, error)) error {
	before, err := s.materialize(ctx)
	if err != nil {
		return err
	}
	working, err := cloneSnapshot(before)
	if err != nil {
		return err
	}
	mutated, err := fn(working)
	if err != nil || !mutated {
		return err
	}
	return s.replaceFromSnapshot(working)
}

func (s *splitStore) replaceFromSnapshot(data *Snapshot) error {
	prepared, err := splitSnapshot(data)
	if err != nil {
		return err
	}
	// Snapshot/control compatibility writers must not erase a lost append.
	// Preserve an unchanged partial tenant; reject mutations of its evidence.
	for id, current := range s.tenant {
		if len(current.pending) == 0 {
			continue
		}
		next := prepared.tenant[id]
		if next == nil || !reflect.DeepEqual(current.hot.Events, next.hot.Events) || !reflect.DeepEqual(current.hot.Streams, next.hot.Streams) || !reflect.DeepEqual(current.ledger.receipts, next.ledger.receipts) || !reflect.DeepEqual(current.ledger.segments, next.ledger.segments) || !reflect.DeepEqual(current.ledger.checkpoints, next.ledger.checkpoints) {
			return ErrTenantCommitPending
		}
		prepared.tenant[id] = current
	}
	if err := s.persistPrepared(prepared); err != nil {
		return err
	}
	s.control = prepared.control
	s.tenant = prepared.tenant
	return nil
}

type preparedSplit struct {
	control *Snapshot
	tenant  map[string]*tenantState
}

func splitSnapshot(data *Snapshot) (*preparedSplit, error) {
	control, err := controlSnapshot(data)
	if err != nil {
		return nil, err
	}
	ids := tenantIDsFromSnapshot(data)
	prepared := &preparedSplit{control: control, tenant: map[string]*tenantState{}}
	for _, tenantID := range ids {
		state, err := splitTenantSnapshot(data, tenantID)
		if err != nil {
			return nil, err
		}
		prepared.tenant[tenantID] = state
	}
	return prepared, nil
}

func tenantIDsFromSnapshot(data *Snapshot) []string {
	seen := map[string]struct{}{}
	for tenantID := range data.Tenants {
		seen[tenantID] = struct{}{}
	}
	for key := range data.Events {
		if tenantID, _, ok := SplitTenantKey(key); ok {
			seen[tenantID] = struct{}{}
		}
	}
	for key := range data.Receipts {
		if tenantID, _, ok := SplitTenantKey(key); ok {
			seen[tenantID] = struct{}{}
		}
	}
	for key := range data.Streams {
		if tenantID, _, ok := SplitTenantKey(key); ok {
			seen[tenantID] = struct{}{}
		}
	}
	for key := range data.Segments {
		if tenantID, _, ok := SplitTenantKey(key); ok {
			seen[tenantID] = struct{}{}
		}
	}
	for key := range data.Checkpoints {
		if tenantID, _, ok := SplitTenantKey(key); ok {
			seen[tenantID] = struct{}{}
		}
	}
	ids := make([]string, 0, len(seen))
	for tenantID := range seen {
		if tenantID != "" {
			ids = append(ids, tenantID)
		}
	}
	sort.Strings(ids)
	return ids
}

func splitTenantSnapshot(data *Snapshot, tenantID string) (*tenantState, error) {
	hot := emptyTenantHot(tenantID)
	for key, event := range data.Events {
		if event.TenantID == tenantID || keyBelongsToTenant(key, tenantID) {
			receipt, hasReceipt := data.Receipts[key]
			if !hasReceipt || receipt.Status != domain.StatusArchived {
				cloned, err := domain.CloneEvent(event)
				if err != nil {
					return nil, err
				}
				hot.Events[key] = cloned
			}
		}
	}
	for key, stream := range data.Streams {
		if stream.TenantID == tenantID || keyBelongsToTenant(key, tenantID) {
			hot.Streams[key] = stream
		}
	}
	records := make([]LedgerRecord, 0)
	for key, receipt := range data.Receipts {
		if receipt.TenantID != tenantID && !keyBelongsToTenant(key, tenantID) {
			continue
		}
		records = append(records, LedgerRecord{TenantID: tenantID, RecordType: LedgerReceipt, Key: key, Version: 1, Receipt: cloneReceiptPtr(&receipt)})
	}
	for key, values := range data.Segments {
		if !keyBelongsToTenant(key, tenantID) {
			continue
		}
		for _, segment := range values {
			records = append(records, LedgerRecord{TenantID: tenantID, RecordType: LedgerSegment, Key: segmentLedgerKey(segment), Version: 1, Segment: cloneSegmentPtr(&segment)})
		}
	}
	for key, values := range data.Checkpoints {
		if !keyBelongsToTenant(key, tenantID) {
			continue
		}
		for _, checkpoint := range values {
			records = append(records, LedgerRecord{TenantID: tenantID, RecordType: LedgerCheckpoint, Key: checkpoint.ID, Version: 1, Checkpoint: cloneCheckpointPtr(&checkpoint)})
		}
	}
	sortLedgerRecords(records)
	return &tenantState{hot: hot, ledger: newTenantLedger(tenantID, records), version: 1}, nil
}
