package store

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	TenantID string                  `json:"tenant_id"`
	Version  int64                   `json:"version"`
	Events   map[string]domain.Event `json:"events"`
	Streams  map[string]StreamState  `json:"streams"`
}

type tenantState struct {
	hot     *Snapshot
	ledger  *TenantLedger
	version int64
}

type splitStore struct {
	mu      sync.RWMutex
	path    string
	root    string
	control *Snapshot
	tenant  map[string]*tenantState
}

// newSplitStore opens the v2 layout. A v1 file is migrated before the
// returned store is exposed; in-memory stores begin directly in v2 shape.
func newSplitStore(data *Snapshot, path string) (*splitStore, error) {
	if data == nil {
		data = NewSnapshot()
	}
	stateExists := path != "" && fileHasContent(path)
	if stateExists && data.LayoutVersion < hotColdLayoutVersion {
		if err := migrateV1File(path, data); err != nil {
			return nil, err
		}
	}
	if !stateExists && data.LayoutVersion < hotColdLayoutVersion {
		data.LayoutVersion = hotColdLayoutVersion
	}
	control := controlSnapshot(data)
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
		if _, err := os.Stat(tenantFilePath(root, tenantID)); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("v2 layout is incomplete: tenant %q hot file is missing", tenantID)
			}
			return err
		}
		if _, err := os.Stat(ledgerFilePath(root, tenantID)); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("v2 layout is incomplete: tenant %q ledger file is missing", tenantID)
			}
			return err
		}
	}
	for _, directory := range []string{filepath.Join(root, "tenants"), filepath.Join(root, "ledger")} {
		entries, err := os.ReadDir(directory)
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

func fileHasContent(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Size() > 0
}

// splitFileEvidence inspects sibling v2 files without opening the store. It
// is used by the file-to-Postgres cutover guard because a v2 control document
// intentionally contains neither hot events nor cold receipts.
func splitFileEvidence(root string) (int, int, error) {
	hotEvents, ledgerRecords := 0, 0
	for _, kind := range []string{"tenants", "ledger"} {
		entries, err := os.ReadDir(filepath.Join(root, kind))
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
			raw, err := os.ReadFile(path)
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

func controlSnapshot(data *Snapshot) *Snapshot {
	control, err := cloneSnapshot(data)
	if err != nil {
		control = NewSnapshot()
	}
	control.LayoutVersion = hotColdLayoutVersion
	control.Events = map[string]domain.Event{}
	control.Receipts = map[string]domain.EventReceipt{}
	control.Streams = map[string]StreamState{}
	control.Segments = map[string][]domain.Segment{}
	control.Checkpoints = map[string][]domain.Checkpoint{}
	return control
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
		entries, err := os.ReadDir(directory)
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
	raw, err := os.ReadFile(path)
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

func (s *splitStore) materialize() (*Snapshot, error) {
	result, err := cloneSnapshot(s.control)
	if err != nil {
		return nil, err
	}
	for tenantID, state := range s.tenant {
		for key, event := range state.hot.Events {
			result.Events[key] = event
		}
		for key, stream := range state.hot.Streams {
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

func (s *splitStore) read(fn func(*Snapshot) error) error {
	data, err := s.materialize()
	if err != nil {
		return err
	}
	return fn(data)
}

func (s *splitStore) update(fn func(*Snapshot) error) error {
	before, err := s.materialize()
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

func (s *splitStore) updateChecked(fn func(*Snapshot) (bool, error)) error {
	before, err := s.materialize()
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
	control := controlSnapshot(data)
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
				hot.Events[key] = event
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

func keyBelongsToTenant(key, tenantID string) bool {
	owner, _, ok := SplitTenantKey(key)
	return ok && owner == tenantID
}

func (s *splitStore) readTenant(tenantID string, fn func(*TenantView) error) error {
	state := s.tenant[tenantID]
	if state == nil {
		state = &tenantState{hot: emptyTenantHot(tenantID), ledger: NewTenantLedger(tenantID), version: 1}
	}
	view := &TenantView{Hot: cloneTenantHot(state.hot), Ledger: newTenantLedgerView(state.ledger), Global: cloneControl(s.control)}
	if err := fn(view); err != nil {
		return err
	}
	return view.Ledger.Err()
}

func (s *splitStore) updateTenant(tenantID string, order CommitOrder, fn func(*TenantView) error) error {
	state := s.tenantState(tenantID)
	hot := cloneTenantHot(state.hot)
	ledger := newTenantLedgerView(state.ledger)
	global := cloneControl(s.control)
	view := &TenantView{Hot: hot, Ledger: ledger, Global: global}
	if err := fn(view); err != nil {
		ledger.clearPending()
		return err
	}
	if err := ledger.Err(); err != nil {
		ledger.clearPending()
		return err
	}
	pending := ledger.pendingRecords()
	globalChanged := !reflect.DeepEqual(global, s.control)
	if err := s.commitTenant(tenantID, order, hot, ledger, pending, global, globalChanged); err != nil {
		return err
	}
	return nil
}

func (s *splitStore) commitTenant(tenantID string, order CommitOrder, hot *Snapshot, ledger *TenantLedger, pending []LedgerRecord, global *Snapshot, globalChanged bool) error {
	if err := checkControlTempPath(s.path); err != nil {
		return err
	}
	state := s.tenantState(tenantID)
	if order == ColdFirst {
		if err := s.appendLedger(tenantID, state.ledger, pending); err != nil {
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
		if err := s.appendLedger(tenantID, state.ledger, pending); err != nil {
			return err
		}
	}
	if globalChanged {
		if s.path != "" {
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

func (s *splitStore) appendLedger(tenantID string, committed *TenantLedger, pending []LedgerRecord) error {
	if len(pending) == 0 {
		return nil
	}
	for _, record := range pending {
		if err := validateLedgerRecord(record); err != nil {
			return err
		}
	}
	if s.path != "" {
		if err := appendLedgerFile(s.root, tenantID, pending); err != nil {
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

func cloneControl(control *Snapshot) *Snapshot {
	clone, err := cloneSnapshot(control)
	if err != nil {
		return NewSnapshot()
	}
	clone.Events = map[string]domain.Event{}
	clone.Receipts = map[string]domain.EventReceipt{}
	clone.Streams = map[string]StreamState{}
	clone.Segments = map[string][]domain.Segment{}
	clone.Checkpoints = map[string][]domain.Checkpoint{}
	return clone
}

func cloneTenantHot(hot *Snapshot) *Snapshot {
	clone := emptyTenantHot("")
	for key, event := range hot.Events {
		clone.Events[key] = event
	}
	for key, stream := range hot.Streams {
		stream.PendingHashes = append([]string(nil), stream.PendingHashes...)
		stream.PendingEvents = append([]string(nil), stream.PendingEvents...)
		clone.Streams[key] = stream
	}
	return clone
}

func loadTenantState(root, tenantID string) (*tenantState, error) {
	hot := emptyTenantHot(tenantID)
	hotPath := tenantFilePath(root, tenantID)
	if raw, err := os.ReadFile(hotPath); err == nil {
		var disk tenantHotSnapshot
		if err := decodeJSON(raw, &disk); err != nil {
			return nil, fmt.Errorf("decode tenant hot file %s: %w", hotPath, err)
		}
		hot.Events = disk.Events
		hot.Streams = disk.Streams
		if hot.Events == nil {
			hot.Events = map[string]domain.Event{}
		}
		if hot.Streams == nil {
			hot.Streams = map[string]StreamState{}
		}
		return loadTenantLedger(root, tenantID, hot, disk.Version)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return loadTenantLedger(root, tenantID, hot, 1)
}

func loadTenantLedger(root, tenantID string, hot *Snapshot, version int64) (*tenantState, error) {
	records, err := readLedgerFile(root, tenantID)
	if err != nil {
		return nil, err
	}
	return &tenantState{hot: hot, ledger: newTenantLedger(tenantID, records), version: version}, nil
}

func migrateV1File(path string, data *Snapshot) error {
	backup := path + ".v1"
	if _, err := os.Stat(backup); errors.Is(err, os.ErrNotExist) {
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return fmt.Errorf("backup v1 state: %w", readErr)
		}
		if err := writeDurableFile(backup, raw, 0o640); err != nil {
			return fmt.Errorf("backup v1 state: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("inspect v1 backup: %w", err)
	}
	prepared, err := splitSnapshot(data)
	if err != nil {
		return err
	}
	root := filepath.Dir(path)
	ids := make([]string, 0, len(prepared.tenant))
	for tenantID := range prepared.tenant {
		ids = append(ids, tenantID)
	}
	sort.Strings(ids)
	for _, tenantID := range ids {
		state := prepared.tenant[tenantID]
		if err := writeTenantFile(root, tenantID, tenantHotSnapshot{TenantID: tenantID, Version: state.version, Events: state.hot.Events, Streams: state.hot.Streams}); err != nil {
			return err
		}
		if err := replaceLedgerFile(root, tenantID, state.ledger.committedRecords()); err != nil {
			return err
		}
	}
	if err := writeControlFile(path, prepared.control); err != nil {
		return err
	}
	*data = *prepared.control
	return nil
}

func tenantFilePath(root, tenantID string) string {
	return filepath.Join(root, "tenants", tenantFileName(tenantID)+".json")
}

func ledgerFilePath(root, tenantID string) string {
	return filepath.Join(root, "ledger", tenantFileName(tenantID)+".jsonl")
}

func tenantFileName(tenantID string) string {
	encoded := fsutil.EncodeKeyComponent(tenantID)
	if len(encoded)+len(".json.tmp") <= 255 {
		return encoded
	}
	digest := sha256.Sum256([]byte(tenantID))
	return "sha256-" + hex.EncodeToString(digest[:])
}

func writeTenantFile(root, tenantID string, disk tenantHotSnapshot) error {
	encoded, err := json.MarshalIndent(disk, "", "  ")
	if err != nil {
		return err
	}
	return writeDurableFile(tenantFilePath(root, tenantID), encoded, 0o640)
}

func writeControlFile(path string, data *Snapshot) error {
	encoded, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return writeDurableFile(path, encoded, 0o640)
}

var writeDurableFile = writeDurableFileImpl

func writeDurableFileImpl(path string, data []byte, mode os.FileMode) error {
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return err
	}
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return fsutil.SyncDir(parent)
}

var appendLedgerFile = appendLedgerFileImpl

func appendLedgerFileImpl(root, tenantID string, records []LedgerRecord) error {
	path := ledgerFilePath(root, tenantID)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			_ = file.Close()
			return err
		}
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return fsutil.SyncDir(filepath.Dir(path))
}

func replaceLedgerFile(root, tenantID string, records []LedgerRecord) error {
	if len(records) == 0 {
		// Keep an explicit empty ledger alongside every tenant hot file. The
		// marker is part of the v2 layout integrity check: a missing ledger
		// file is indistinguishable from lost cold evidence after restart.
		return writeDurableFile(ledgerFilePath(root, tenantID), nil, 0o640)
	}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			return err
		}
	}
	return writeDurableFile(ledgerFilePath(root, tenantID), buffer.Bytes(), 0o640)
}

func readLedgerFile(root, tenantID string) ([]LedgerRecord, error) {
	path := ledgerFilePath(root, tenantID)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var records []LedgerRecord
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var record LedgerRecord
		if err := decodeJSON(line, &record); err != nil {
			return nil, fmt.Errorf("decode ledger %s: %w", path, err)
		}
		if record.TenantID != tenantID {
			return nil, fmt.Errorf("ledger %s contains tenant %q", path, record.TenantID)
		}
		if err := validateLedgerRecord(record); err != nil {
			return nil, fmt.Errorf("validate ledger %s: %w", path, err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func decodeJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	return decoder.Decode(target)
}

// TenantView is the closure-facing view of one tenant. Hot contains only
// Streams and hot Events; global control-plane maps are intentionally absent
// from Hot. Global is a private control snapshot for authorization checks and
// tenant-scoped worker metadata mutations in the same store critical section.
type TenantView struct {
	Hot    *Snapshot
	Ledger *TenantLedger
	Global *Snapshot
}

// HotCold reports whether this Store uses the v2 tenant/cold-ledger layout.
// It is intentionally read-only; callers use it to select a mutation path
// without inspecting backend implementation details.
func (s *Store) HotCold() bool {
	return s.split != nil || s.pgSplit != nil && s.pgSplit.enabled()
}

// ReadTenant executes fn against one tenant without materializing other
// tenants or the cold ledger into a global snapshot.
func (s *Store) ReadTenant(tenantID string, fn func(*TenantView) error) error {
	if s.split == nil {
		if s.pgSplit != nil && s.pgSplit.enabled() {
			s.mu.RLock()
			defer s.mu.RUnlock()
			return s.pgSplit.readTenant(tenantID, fn)
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
		if s.pgSplit != nil && s.pgSplit.enabled() {
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.pgSplit.updateTenant(tenantID, order, fn)
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
		if s.pgSplit != nil && s.pgSplit.enabled() {
			s.mu.RLock()
			defer s.mu.RUnlock()
			return s.pgSplit.ledgerScan(tenantID, fn)
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
		if s.pgSplit != nil && s.pgSplit.enabled() {
			s.mu.RLock()
			defer s.mu.RUnlock()
			return s.pgSplit.readControl(fn)
		}
		return s.Read(fn)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fn(cloneControl(s.split.control))
}

// UpdateControl mutates only the global hot control document. It is used for
// delivery aids such as LedgeredOutbox after a tenant ledger commit and never
// materializes cold receipts, segments, checkpoints or archived events.
func (s *Store) UpdateControl(fn func(*Snapshot) error) error {
	if s.split == nil {
		if s.pgSplit != nil && s.pgSplit.enabled() {
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.pgSplit.updateControl(fn)
		}
		return s.Update(fn)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	control := cloneControl(s.split.control)
	if err := fn(control); err != nil {
		return err
	}
	if s.split.path != "" {
		if err := writeControlFile(s.split.path, control); err != nil {
			return err
		}
	}
	s.split.control = control
	return nil
}

func (s *Store) readLegacyTenant(tenantID string, fn func(*TenantView) error) error {
	return s.Read(func(data *Snapshot) error {
		view := legacyTenantView(data, tenantID)
		return fn(view)
	})
}

func (s *Store) updateLegacyTenant(tenantID string, fn func(*TenantView) error) error {
	return s.Update(func(data *Snapshot) error {
		view := legacyTenantView(data, tenantID)
		if err := fn(view); err != nil {
			return err
		}
		view.Ledger.commitPending(view.Ledger.pendingRecords())
		copyTenantView(data, tenantID, view)
		return nil
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

func legacyTenantView(data *Snapshot, tenantID string) *TenantView {
	hot := emptyTenantHot(tenantID)
	ledger, _ := newTenantLedgerFromSnapshot(data, tenantID)
	for key, event := range data.Events {
		if keyBelongsToTenant(key, tenantID) || event.TenantID == tenantID {
			hot.Events[key] = event
		}
	}
	for key, stream := range data.Streams {
		if keyBelongsToTenant(key, tenantID) || stream.TenantID == tenantID {
			hot.Streams[key] = stream
		}
	}
	return &TenantView{Hot: hot, Ledger: ledger, Global: cloneControl(data)}
}

func newTenantLedgerFromSnapshot(data *Snapshot, tenantID string) (*TenantLedger, error) {
	state, err := splitTenantSnapshot(data, tenantID)
	if err != nil {
		return nil, err
	}
	return state.ledger, nil
}

func copyTenantView(data *Snapshot, tenantID string, view *TenantView) {
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
	for key, event := range view.Hot.Events {
		data.Events[key] = event
	}
	for key, stream := range view.Hot.Streams {
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
}
