package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

const postgresHotColdCatalogQuery = `SELECT to_regclass('audit_tenant') IS NOT NULL AND to_regclass('audit_ledger') IS NOT NULL`

const postgresTenantLoadQuery = `SELECT snapshot, version FROM audit_tenant WHERE tenant_id = $1`

const postgresTenantSaveQuery = `INSERT INTO audit_tenant (tenant_id, snapshot, version)
VALUES ($1, $2::jsonb, 1)
ON CONFLICT (tenant_id) DO UPDATE
SET snapshot = EXCLUDED.snapshot, version = audit_tenant.version + 1, updated_at = now()
WHERE audit_tenant.version = $3`

const postgresLedgerAppendQuery = `INSERT INTO audit_ledger (tenant_id, record_type, key, version, record, written_at)
VALUES ($1, $2, $3, $4, $5::jsonb, $6)
ON CONFLICT (tenant_id, record_type, key, version) DO NOTHING`

// postgresSplitStore is selected only when migration 006's two tables exist.
// The legacy postgresBackend remains the fallback for old deployments, which
// lets operators apply the expand migration before switching binaries.
type postgresSplitStore struct {
	backend *postgresBackend
	once    sync.Once
	active  bool

	// saveControlHook, when non-nil, is consulted before the real save and
	// may inject a transient optimistic-lock conflict. It is nil in
	// production; tests use it to force the conflict+retry path of
	// updateControl deterministically (no flaky timing). See IT-CTRL-04.
	saveControlHook func() error
}

func newPostgresSplitStore(backend *postgresBackend) *postgresSplitStore {
	return &postgresSplitStore{backend: backend}
}

// MigratePostgresSnapshot performs the v1 single-row to v2 hot/cold cutover
// while holding the snapshot row lock. It is intentionally an explicit
// operator action: production startup refuses to reinterpret a legacy row
// when migration 006 tables are present but the cutover has not run.
func MigratePostgresSnapshot(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	var encoded []byte
	var version int64
	err = tx.QueryRow(`SELECT snapshot, version FROM audit_state_snapshot WHERE id = 1 FOR UPDATE`).Scan(&encoded, &version)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.Exec(`INSERT INTO audit_state_snapshot (id, snapshot, version) VALUES (1, $1::jsonb, 1)`, `{"layout_version":2}`); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
		return nil
	}
	if err != nil {
		return err
	}
	var data Snapshot
	if err := decodeSnapshot(encoded, &data); err != nil {
		return err
	}
	if data.LayoutVersion >= hotColdLayoutVersion {
		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
		return nil
	}
	var tenantCount, ledgerCount int
	if err := tx.QueryRow(`SELECT count(*) FROM audit_tenant`).Scan(&tenantCount); err != nil {
		return err
	}
	if err := tx.QueryRow(`SELECT count(*) FROM audit_ledger`).Scan(&ledgerCount); err != nil {
		return err
	}
	if tenantCount != 0 || ledgerCount != 0 {
		return fmt.Errorf("hot/cold target tables are partially populated; restore the cutover backup before retrying")
	}
	prepared, err := splitSnapshot(&data)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS audit_state_snapshot_v1_backup (id INTEGER PRIMARY KEY, snapshot JSONB NOT NULL, version BIGINT NOT NULL, updated_at TIMESTAMPTZ NOT NULL)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM audit_state_snapshot_v1_backup WHERE id = 1`); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO audit_state_snapshot_v1_backup (id, snapshot, version, updated_at) SELECT id, snapshot, version, updated_at FROM audit_state_snapshot WHERE id = 1`); err != nil {
		return err
	}
	for tenantID, state := range prepared.tenant {
		encodedHot, err := json.Marshal(tenantHotSnapshot{TenantID: tenantID, Version: state.version, Events: state.hot.Events, Streams: state.hot.Streams})
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO audit_tenant (tenant_id, snapshot, version) VALUES ($1, $2::jsonb, $3)`, tenantID, string(encodedHot), state.version); err != nil {
			return err
		}
		for _, record := range state.ledger.committedRecords() {
			encodedRecord, err := json.Marshal(record)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(`INSERT INTO audit_ledger (tenant_id, record_type, key, version, record, written_at) VALUES ($1, $2, $3, $4, $5::jsonb, $6)`, record.TenantID, record.RecordType, record.Key, record.Version, string(encodedRecord), record.WrittenAt); err != nil {
				return err
			}
		}
	}
	encodedControl, err := json.Marshal(prepared.control)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE audit_state_snapshot SET snapshot = $1::jsonb, version = version + 1, updated_at = now() WHERE id = 1 AND version = $2`, string(encodedControl), version); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func (p *postgresSplitStore) enabled() bool {
	p.once.Do(func() {
		if err := p.backend.db.QueryRow(postgresHotColdCatalogQuery).Scan(&p.active); err != nil {
			p.active = false
		}
	})
	return p.active
}

func (p *postgresSplitStore) loadControl() (*Snapshot, int64, error) {
	data, version, err := p.backend.load()
	if errors.Is(err, sql.ErrNoRows) {
		data.LayoutVersion = hotColdLayoutVersion
		return controlSnapshot(data), 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	if data.LayoutVersion < hotColdLayoutVersion && hasLedgerData(data) {
		return nil, 0, fmt.Errorf("postgres snapshot contains legacy ledger data; run the hot/cold cutover before enabling migration 006")
	}
	data.LayoutVersion = hotColdLayoutVersion
	return controlSnapshot(data), version, nil
}

func hasLedgerData(data *Snapshot) bool {
	return len(data.Events) > 0 || len(data.Receipts) > 0 || len(data.Streams) > 0 || len(data.Segments) > 0 || len(data.Checkpoints) > 0
}

func (p *postgresSplitStore) readControl(fn func(*Snapshot) error) error {
	control, _, err := p.loadControl()
	if err != nil {
		return err
	}
	return fn(control)
}

func (p *postgresSplitStore) updateControl(fn func(*Snapshot) error) error {
	for attempt := 0; attempt <= snapshotConflictRetries; attempt++ {
		control, version, err := p.loadControl()
		if err != nil {
			return err
		}
		if err := fn(control); err != nil {
			return err
		}
		if err := p.saveControl(control, version); err != nil {
			if errors.Is(err, ErrSnapshotConflict) && attempt < snapshotConflictRetries {
				time.Sleep(snapshotConflictBackoff(attempt))
				continue
			}
			return err
		}
		return nil
	}
	return ErrSnapshotConflict
}

func (p *postgresSplitStore) saveControl(control *Snapshot, version int64) error {
	if p.saveControlHook != nil {
		if err := p.saveControlHook(); err != nil {
			return err
		}
	}
	control.LayoutVersion = hotColdLayoutVersion
	p.backend.lastVersion = version
	return p.backend.Save(control)
}

func (p *postgresSplitStore) tenantIDs() ([]string, error) {
	rows, err := p.backend.db.Query(`SELECT tenant_id FROM audit_tenant UNION SELECT tenant_id FROM audit_ledger ORDER BY tenant_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (p *postgresSplitStore) loadTenantHot(tenantID string) (*Snapshot, int64, error) {
	hot := emptyTenantHot(tenantID)
	var encoded []byte
	var version int64
	err := p.backend.db.QueryRow(postgresTenantLoadQuery, tenantID).Scan(&encoded, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return hot, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("load tenant %s: %w", tenantID, err)
	}
	var disk tenantHotSnapshot
	if err := decodeJSON(encoded, &disk); err != nil {
		return nil, 0, fmt.Errorf("decode tenant %s: %w", tenantID, err)
	}
	if disk.Events != nil {
		hot.Events = disk.Events
	}
	if disk.Streams != nil {
		hot.Streams = disk.Streams
	}
	return hot, version, nil
}

func (p *postgresSplitStore) saveTenantHot(tenantID string, hot *Snapshot, version int64) error {
	disk := tenantHotSnapshot{TenantID: tenantID, Version: version + 1, Events: hot.Events, Streams: hot.Streams}
	encoded, err := json.Marshal(disk)
	if err != nil {
		return err
	}
	result, err := p.backend.db.Exec(postgresTenantSaveQuery, tenantID, string(encoded), version)
	if err != nil {
		return fmt.Errorf("save tenant %s: %w", tenantID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrSnapshotConflict
	}
	return nil
}

func (p *postgresSplitStore) readTenant(tenantID string, fn func(*TenantView) error) error {
	hot, _, err := p.loadTenantHot(tenantID)
	if err != nil {
		return err
	}
	control, _, err := p.loadControl()
	if err != nil {
		return err
	}
	ledger := newTenantLedgerWithReader(tenantID, &postgresLedgerReader{db: p.backend.db, tenantID: tenantID})
	view := &TenantView{Hot: hot, Ledger: ledger, Global: control}
	if err := fn(view); err != nil {
		return err
	}
	return ledger.Err()
}

func (p *postgresSplitStore) updateTenant(tenantID string, order CommitOrder, fn func(*TenantView) error) error {
	for attempt := 0; attempt <= snapshotConflictRetries; attempt++ {
		err := p.updateTenantOnce(tenantID, order, fn)
		if !errors.Is(err, ErrSnapshotConflict) || attempt == snapshotConflictRetries {
			return err
		}
		time.Sleep(snapshotConflictBackoff(attempt))
	}
	return ErrSnapshotConflict
}

func (p *postgresSplitStore) updateTenantOnce(tenantID string, order CommitOrder, fn func(*TenantView) error) error {
	hot, version, err := p.loadTenantHot(tenantID)
	if err != nil {
		return err
	}
	control, _, err := p.loadControl()
	if err != nil {
		return err
	}
	baselineControl, err := cloneSnapshot(control)
	if err != nil {
		return err
	}
	ledger := newTenantLedgerWithReader(tenantID, &postgresLedgerReader{db: p.backend.db, tenantID: tenantID})
	view := &TenantView{Hot: hot, Ledger: ledger, Global: control}
	if err := fn(view); err != nil {
		return err
	}
	if err := ledger.Err(); err != nil {
		return err
	}
	pending := ledger.pendingRecords()
	globalChanged := !reflect.DeepEqual(control, baselineControl)
	if order == ColdFirst {
		if err := p.appendLedger(pending); err != nil {
			return err
		}
		if err := p.saveTenantHot(tenantID, hot, version); err != nil {
			return err
		}
	} else {
		if err := p.saveTenantHot(tenantID, hot, version); err != nil {
			return err
		}
		if err := p.appendLedger(pending); err != nil {
			return err
		}
	}
	if globalChanged {
		if err := p.updateControl(func(current *Snapshot) error {
			mergeControlDelta(current, baselineControl, control)
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// mergeControlDelta applies the writer's intended control-plane change onto the
// freshly reloaded current snapshot without clobbering entries a concurrent
// writer committed in the meantime, and without re-applying a stale whole-field
// replacement on a conflict+retry.
//
// The previous body did current.F = mutated.F for every field whose whole value
// differed from baseline. That discarded every key another writer had added to F
// after this writer's baseline was captured, and — on ErrSnapshotConflict — the
// retry reloaded current but re-applied the same stale replacement, dropping the
// winner's committed entries (archive DeadLetters / ArchiveConflictFailures).
//
// The fix overlays the writer's delta instead. For map fields every key the
// writer set (mutated.F[k]) is written onto current; keys the writer left
// untouched (mutated.F == baseline.F for the whole field) are skipped so
// current.F is preserved verbatim. For the two slice fields the writer's
// elements are appended (value-deduped) so concurrent appends survive. On the
// no-conflict single-writer path current starts equal to baseline and the result
// reduces byte-for-byte to mutated (Go json.Marshal sorts map keys
// deterministically, slices are already ordered), so observable state is
// unchanged. On conflict+retry the same stable (baseline, mutated) pair is
// re-applied to the reloaded current: that is idempotent and non-lossy and
// converges exactly like UpdateControl's re-run-on-retry semantics.
func mergeControlDelta(current, baseline, mutated *Snapshot) {
	current.Tenants = overlayControlMap(current.Tenants, baseline.Tenants, mutated.Tenants)
	current.Sources = overlayControlMap(current.Sources, baseline.Sources, mutated.Sources)
	current.Schemas = overlayControlMap(current.Schemas, baseline.Schemas, mutated.Schemas)
	current.Policies = overlayControlMap(current.Policies, baseline.Policies, mutated.Policies)
	current.LegalHolds = overlayControlMap(current.LegalHolds, baseline.LegalHolds, mutated.LegalHolds)
	current.Exports = overlayControlMap(current.Exports, baseline.Exports, mutated.Exports)
	current.RestoreRuns = overlayControlMap(current.RestoreRuns, baseline.RestoreRuns, mutated.RestoreRuns)
	current.LedgeredOutbox = overlayControlMap(current.LedgeredOutbox, baseline.LedgeredOutbox, mutated.LedgeredOutbox)
	current.ArchiveConflictFailures = overlayControlMap(current.ArchiveConflictFailures, baseline.ArchiveConflictFailures, mutated.ArchiveConflictFailures)
	current.DeadLetters = overlayControlMap(current.DeadLetters, baseline.DeadLetters, mutated.DeadLetters)
	if !reflect.DeepEqual(mutated.AdminActions, baseline.AdminActions) {
		current.AdminActions = unionControlSlice(current.AdminActions, mutated.AdminActions)
	}
	if !reflect.DeepEqual(mutated.AggregateCheckpoints, baseline.AggregateCheckpoints) {
		current.AggregateCheckpoints = unionControlSlice(current.AggregateCheckpoints, mutated.AggregateCheckpoints)
	}
}

// overlayControlMap returns current with the writer's keys from mutated applied
// on top. If the writer left the whole field untouched (mutated == baseline),
// current is returned unchanged so a concurrent writer's additions survive. Keys
// not present in mutated are preserved.
func overlayControlMap[K comparable, V any](current, baseline, mutated map[K]V) map[K]V {
	if reflect.DeepEqual(mutated, baseline) {
		return current
	}
	if current == nil {
		current = make(map[K]V, len(mutated))
	}
	for k, v := range mutated {
		current[k] = v
	}
	return current
}

// unionControlSlice returns current with every element of mutated that is not
// already present (by value) appended. It preserves current's existing order and
// dedups against it, so re-applying a stable delta on retry converges instead of
// growing without bound.
func unionControlSlice[T any](current, mutated []T) []T {
	union := make([]T, 0, len(current)+len(mutated))
	union = append(union, current...)
	for _, m := range mutated {
		found := false
		for _, c := range current {
			if reflect.DeepEqual(c, m) {
				found = true
				break
			}
		}
		if !found {
			union = append(union, m)
		}
	}
	return union
}

func (p *postgresSplitStore) appendLedger(records []LedgerRecord) error {
	for _, record := range records {
		if err := validateLedgerRecord(record); err != nil {
			return err
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if _, err := p.backend.db.Exec(postgresLedgerAppendQuery, record.TenantID, record.RecordType, record.Key, record.Version, string(encoded), record.WrittenAt); err != nil {
			return fmt.Errorf("append tenant ledger %s/%s: %w", record.TenantID, record.Key, err)
		}
	}
	return nil
}

func (p *postgresSplitStore) ledgerScan(tenantID string, fn func(LedgerRecord) error) error {
	reader := &postgresLedgerReader{db: p.backend.db, tenantID: tenantID}
	records, err := reader.records()
	if err != nil {
		return err
	}
	sortLedgerRecords(records)
	for _, record := range records {
		if err := fn(record); err != nil {
			return err
		}
	}
	return nil
}

func (p *postgresSplitStore) materialize() (*Snapshot, error) {
	control, _, err := p.loadControl()
	if err != nil {
		return nil, err
	}
	ids, err := p.tenantIDs()
	if err != nil {
		return nil, err
	}
	result, err := cloneSnapshot(control)
	if err != nil {
		return nil, err
	}
	for _, tenantID := range ids {
		hot, _, err := p.loadTenantHot(tenantID)
		if err != nil {
			return nil, err
		}
		for key, event := range hot.Events {
			result.Events[key] = event
		}
		for key, stream := range hot.Streams {
			result.Streams[key] = stream
		}
		records, err := (&postgresLedgerReader{db: p.backend.db, tenantID: tenantID}).records()
		if err != nil {
			return nil, err
		}
		ledger := newTenantLedger(tenantID, records)
		for key, receipt := range ledger.receipts {
			result.Receipts[key] = receipt
		}
		for key, segments := range ledger.segments {
			result.Segments[key] = cloneSegments(segments)
		}
		for key, checkpoints := range ledger.checkpoints {
			result.Checkpoints[key] = cloneCheckpoints(checkpoints)
		}
	}
	result.LayoutVersion = hotColdLayoutVersion
	result.normalize()
	return result, nil
}

func (p *postgresSplitStore) read(fn func(*Snapshot) error) error {
	data, err := p.materialize()
	if err != nil {
		return err
	}
	return fn(data)
}

func (p *postgresSplitStore) update(fn func(*Snapshot) error) error {
	return p.updateChecked(func(data *Snapshot) (bool, error) {
		if err := fn(data); err != nil {
			return false, err
		}
		return true, nil
	})
}

func (p *postgresSplitStore) updateChecked(fn func(*Snapshot) (bool, error)) error {
	before, err := p.materialize()
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
	if tenantDataEqual(before, working) {
		return p.updateControl(func(control *Snapshot) error {
			*control = *controlSnapshot(working)
			return nil
		})
	}
	return p.replaceSnapshot(working)
}

func tenantDataEqual(left, right *Snapshot) bool {
	return reflect.DeepEqual(left.Events, right.Events) && reflect.DeepEqual(left.Receipts, right.Receipts) && reflect.DeepEqual(left.Streams, right.Streams) && reflect.DeepEqual(left.Segments, right.Segments) && reflect.DeepEqual(left.Checkpoints, right.Checkpoints)
}

func (p *postgresSplitStore) replaceSnapshot(data *Snapshot) error {
	prepared, err := splitSnapshot(data)
	if err != nil {
		return err
	}
	ids, err := p.tenantIDs()
	if err != nil {
		return err
	}
	known := map[string]struct{}{}
	for _, id := range ids {
		known[id] = struct{}{}
	}
	for id, state := range prepared.tenant {
		version := int64(0)
		if _, ok := known[id]; ok {
			_, version, err = p.loadTenantHot(id)
			if err != nil {
				return err
			}
		}
		if err := p.saveTenantHot(id, state.hot, version); err != nil {
			return err
		}
		if err := p.appendLedger(state.ledger.committedRecords()); err != nil {
			return err
		}
	}
	return p.updateControl(func(control *Snapshot) error {
		*control = *prepared.control
		return nil
	})
}

type postgresLedgerReader struct {
	db       *sql.DB
	tenantID string
}

func (r *postgresLedgerReader) receipt(key string) (domain.EventReceipt, int, bool, error) {
	return r.oneReceipt(`AND key = $2`, key)
}

func (r *postgresLedgerReader) idempotency(key string) (domain.EventReceipt, int, bool, error) {
	return r.oneReceipt(`AND record->'receipt'->>'idempotency_key' = $2`, key)
}

func (r *postgresLedgerReader) oneReceipt(filter, key string) (domain.EventReceipt, int, bool, error) {
	query := `SELECT version, record FROM audit_ledger WHERE tenant_id = $1 AND record_type = 'receipt' ` + filter + ` ORDER BY version DESC LIMIT 1`
	var version int
	var encoded []byte
	err := r.db.QueryRow(query, r.tenantID, key).Scan(&version, &encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.EventReceipt{}, 0, false, nil
	}
	if err != nil {
		return domain.EventReceipt{}, 0, false, err
	}
	record, err := decodeLedgerRecord(encoded, r.tenantID)
	if err != nil || record.Receipt == nil {
		return domain.EventReceipt{}, 0, false, errOrLedgerPayload(err, record)
	}
	return *record.Receipt, version, true, nil
}

func errOrLedgerPayload(err error, record LedgerRecord) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("ledger record %s has no receipt payload", record.Key)
}

func (r *postgresLedgerReader) segments(streamKey string) ([]domain.Segment, error) {
	records, err := r.recordsByType(LedgerSegment)
	if err != nil {
		return nil, err
	}
	segments := make([]domain.Segment, 0)
	for _, record := range records {
		if record.Segment != nil && StreamKey(record.Segment.TenantID, record.Segment.StreamID) == streamKey {
			segments = append(segments, *record.Segment)
		}
	}
	return segments, nil
}

func (r *postgresLedgerReader) checkpoints(streamKey string) ([]domain.Checkpoint, error) {
	records, err := r.recordsByType(LedgerCheckpoint)
	if err != nil {
		return nil, err
	}
	checkpoints := make([]domain.Checkpoint, 0)
	for _, record := range records {
		if record.Checkpoint != nil && StreamKey(record.Checkpoint.TenantID, record.Checkpoint.StreamID) == streamKey {
			checkpoints = append(checkpoints, *record.Checkpoint)
		}
	}
	return checkpoints, nil
}

func (r *postgresLedgerReader) recordsByType(kind LedgerRecordType) ([]LedgerRecord, error) {
	rows, err := r.db.Query(`SELECT record FROM audit_ledger WHERE tenant_id = $1 AND record_type = $2 ORDER BY version, key`, r.tenantID, kind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []LedgerRecord
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			return nil, err
		}
		record, err := decodeLedgerRecord(encoded, r.tenantID)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (r *postgresLedgerReader) records() ([]LedgerRecord, error) {
	rows, err := r.db.Query(`SELECT record FROM audit_ledger WHERE tenant_id = $1 ORDER BY id`, r.tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []LedgerRecord
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			return nil, err
		}
		record, err := decodeLedgerRecord(encoded, r.tenantID)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func decodeLedgerRecord(encoded []byte, tenantID string) (LedgerRecord, error) {
	var record LedgerRecord
	if err := decodeJSON(encoded, &record); err != nil {
		return LedgerRecord{}, err
	}
	if record.TenantID != tenantID {
		return LedgerRecord{}, fmt.Errorf("ledger record belongs to tenant %q", record.TenantID)
	}
	if err := validateLedgerRecord(record); err != nil {
		return LedgerRecord{}, err
	}
	return record, nil
}

var _ ledgerReader = (*postgresLedgerReader)(nil)
