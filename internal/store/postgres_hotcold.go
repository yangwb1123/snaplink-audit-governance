package store

// The split store on PostgreSQL: several replicas share the control plane, the
// per-tenant hot documents and the append-only tenant ledgers behind optimistic
// version locks. A tenant update applies its hot document, the ledger records
// its commit order makes durable, and a control-plane delta limited to what
// this tenant owns, so concurrent tenants never rewrite each other's rows.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/snaplink/audit-governance/internal/domain"
)

type postgresSplitStore struct {
	backend *postgresBackend

	// probeMu guards the memoized hot/cold catalog verdict. It replaces the
	// former sync.Once because enabled() now runs under a caller-bounded
	// context: a probe aborted by that context must NOT be memoized, or one
	// transient deadline on the very first call would permanently fail every
	// store operation in the process. Schema verdicts (absent/incomplete/
	// incompatible/complete) and genuine errors stay sticky, exactly as before.
	probeMu      sync.Mutex
	probed       bool
	catalogErr   error
	catalogState string

	// saveControlHook, when non-nil, is consulted before the real save and
	// may inject a transient optimistic-lock conflict. It is nil in
	// production; tests use it to force the conflict+retry path of
	// updateControl deterministically (no flaky timing). See IT-CTRL-04.
	saveControlHook func() error
}

func newPostgresSplitStore(backend *postgresBackend) *postgresSplitStore {
	return &postgresSplitStore{backend: backend}
}

func (p *postgresSplitStore) loadControl(ctx context.Context) (*Snapshot, int64, error) {
	data, version, err := p.backend.load(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		data.LayoutVersion = hotColdLayoutVersion
		control, cloneErr := controlSnapshot(data)
		if cloneErr != nil {
			return nil, 0, cloneErr
		}
		return control, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	if data.LayoutVersion < hotColdLayoutVersion {
		if hasLedgerData(data) {
			return nil, 0, fmt.Errorf("postgres snapshot contains legacy ledger data; run the hot/cold cutover before enabling migration 006")
		}
		return nil, 0, fmt.Errorf("postgres snapshot is pre-cutover; run audit-pg-migrate before enabling migration 006")
	}
	data.LayoutVersion = hotColdLayoutVersion
	control, err := controlSnapshot(data)
	if err != nil {
		return nil, 0, err
	}
	return control, version, nil
}

func hasLedgerData(data *Snapshot) bool {
	return len(data.Events) > 0 || len(data.Receipts) > 0 || len(data.Streams) > 0 || len(data.Segments) > 0 || len(data.Checkpoints) > 0
}

func (p *postgresSplitStore) readControl(ctx context.Context, fn func(*Snapshot) error) error {
	control, _, err := p.loadControl(ctx)
	if err != nil {
		return err
	}
	return fn(control)
}

func (p *postgresSplitStore) updateControl(ctx context.Context, fn func(*Snapshot) error) error {
	for attempt := 0; attempt <= snapshotConflictRetries; attempt++ {
		control, version, err := p.loadControl(ctx)
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
		if err := p.saveControl(ctx, retained, version); err != nil {
			if errors.Is(err, ErrSnapshotConflict) && attempt < snapshotConflictRetries {
				if err := sleepBackoff(ctx, attempt); err != nil {
					return err
				}
				continue
			}
			return err
		}
		return nil
	}
	return ErrSnapshotConflict
}

func (p *postgresSplitStore) saveControl(ctx context.Context, control *Snapshot, version int64) error {
	if p.saveControlHook != nil {
		if err := p.saveControlHook(); err != nil {
			return err
		}
	}
	control.LayoutVersion = hotColdLayoutVersion
	if version == 0 {
		return p.saveInitialControl(ctx, control)
	}
	p.backend.lastVersion = version
	return p.backend.Save(ctx, control)
}

// saveInitialControl establishes the zero-data baseline in the same
// transaction as the first v2 control row. A complete migration-006 catalog
// may exist before either row does; allowing the normal write path to create
// only the marker would make the next readiness check unverifiable.
func (p *postgresSplitStore) saveInitialControl(ctx context.Context, control *Snapshot) error {
	tx, err := p.backend.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, postgresHotColdMigrationLockKey); err != nil {
		return fmt.Errorf("acquire hot/cold migration lock: %w", err)
	}
	var encodedExisting []byte
	var existingVersion int64
	err = tx.QueryRowContext(ctx, `SELECT snapshot, version FROM audit_state_snapshot WHERE id = 1 FOR UPDATE`).Scan(&encodedExisting, &existingVersion)
	if err == nil {
		return ErrSnapshotConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err := verifyPostgresHotColdTargetsEmpty(ctx, tx); err != nil {
		return err
	}
	copyControl, err := cloneSnapshot(control)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(copyControl)
	if err != nil {
		return err
	}
	if err := writePostgresHotColdZeroBaseline(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO audit_state_snapshot (id, snapshot, version) VALUES (1, $1::jsonb, 1)`, string(encoded)); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	p.backend.lastVersion = 1
	return nil
}

func (p *postgresSplitStore) tenantIDs(ctx context.Context) ([]string, error) {
	rows, err := p.backend.db.QueryContext(ctx, `SELECT tenant_id FROM audit_tenant UNION SELECT tenant_id FROM audit_ledger ORDER BY tenant_id`)
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

func (p *postgresSplitStore) loadTenantHot(ctx context.Context, tenantID string) (*Snapshot, int64, error) {
	hot := emptyTenantHot(tenantID)
	var encoded []byte
	var version int64
	err := p.backend.db.QueryRowContext(ctx, postgresTenantLoadQuery, tenantID).Scan(&encoded, &version)
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
	if disk.TenantID != "" && disk.TenantID != tenantID {
		return nil, 0, fmt.Errorf("%w: tenant hot identity mismatch", domain.ErrInvalid)
	}
	if len(disk.PendingLedger) > 0 {
		return nil, 0, ErrTenantCommitPending
	}
	if disk.Events != nil {
		hot.Events = disk.Events
	}
	if disk.Streams != nil {
		hot.Streams = disk.Streams
	}
	return hot, version, nil
}

type tenantCommitSQL interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (p *postgresSplitStore) saveTenantHot(ctx context.Context, tenantID string, hot *Snapshot, version int64) error {
	return p.saveTenantHotOn(ctx, p.backend.db, tenantID, hot, version)
}

func (p *postgresSplitStore) saveTenantHotOn(ctx context.Context, db tenantCommitSQL, tenantID string, hot *Snapshot, version int64) error {
	disk := tenantHotSnapshot{TenantID: tenantID, Version: version + 1, Events: hot.Events, Streams: hot.Streams}
	encoded, err := json.Marshal(disk)
	if err != nil {
		return err
	}
	result, err := db.ExecContext(ctx, postgresTenantSaveQuery, tenantID, string(encoded), version)
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

func (p *postgresSplitStore) readTenant(ctx context.Context, tenantID string, fn func(*TenantView) error) error {
	hot, _, err := p.loadTenantHot(ctx, tenantID)
	if err != nil {
		return err
	}
	control, _, err := p.loadControl(ctx)
	if err != nil {
		return err
	}
	viewHot, err := cloneTenantHot(hot)
	if err != nil {
		return err
	}
	viewControl, err := cloneControl(control)
	if err != nil {
		return err
	}
	ledger := newTenantLedgerWithReader(tenantID, &postgresLedgerReader{ctx: ctx, db: p.backend.db, tenantID: tenantID})
	view := &TenantView{Hot: viewHot, Ledger: ledger, Global: viewControl}
	if err := fn(view); err != nil {
		return err
	}
	return ledger.Err()
}

func (p *postgresSplitStore) updateTenant(ctx context.Context, tenantID string, order CommitOrder, fn func(*TenantView) error) error {
	for attempt := 0; attempt <= snapshotConflictRetries; attempt++ {
		err := p.updateTenantOnce(ctx, tenantID, order, fn)
		if !errors.Is(err, ErrSnapshotConflict) || attempt == snapshotConflictRetries {
			return err
		}
		if err := sleepBackoff(ctx, attempt); err != nil {
			return err
		}
	}
	return ErrSnapshotConflict
}

func (p *postgresSplitStore) updateTenantOnce(ctx context.Context, tenantID string, order CommitOrder, fn func(*TenantView) error) error {
	hot, version, err := p.loadTenantHot(ctx, tenantID)
	if err != nil {
		return err
	}
	control, _, err := p.loadControl(ctx)
	if err != nil {
		return err
	}
	baselineControl, err := cloneSnapshot(control)
	if err != nil {
		return err
	}
	ledger := newTenantLedgerWithReader(tenantID, &postgresLedgerReader{ctx: ctx, db: p.backend.db, tenantID: tenantID})
	view := &TenantView{Hot: hot, Ledger: ledger, Global: control}
	if err := fn(view); err != nil {
		return err
	}
	if err := ledger.Err(); err != nil {
		return err
	}
	retainedHot, err := cloneTenantHot(view.Hot)
	if err != nil {
		return err
	}
	retainedControl, err := cloneControl(view.Global)
	if err != nil {
		return err
	}
	pending := ledger.pendingRecords()
	globalChanged := !reflect.DeepEqual(retainedControl, baselineControl)
	if order == ColdFirst {
		if err := p.appendLedger(ctx, pending); err != nil {
			return err
		}
		if err := p.saveTenantHot(ctx, tenantID, retainedHot, version); err != nil {
			return err
		}
	} else {
		// The logical HotFirst order stays hot then ledger, but PostgreSQL
		// makes both visible atomically. A cold error rolls back the hot CAS;
		// other replicas can never continue from an uncommitted stream head.
		if err := p.commitHotFirst(ctx, tenantID, retainedHot, version, pending); err != nil {
			return err
		}
	}
	if globalChanged {
		if err := p.updateControl(ctx, func(current *Snapshot) error {
			mergeControlDelta(current, baselineControl, retainedControl)
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
	current.ExportLeases = overlayControlMap(current.ExportLeases, baseline.ExportLeases, mutated.ExportLeases)
	current.ExportCheckpoints = overlayControlMap(current.ExportCheckpoints, baseline.ExportCheckpoints, mutated.ExportCheckpoints)
	current.ExportPlans = overlayControlMap(current.ExportPlans, baseline.ExportPlans, mutated.ExportPlans)
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

func (p *postgresSplitStore) commitHotFirst(ctx context.Context, tenantID string, hot *Snapshot, version int64, records []LedgerRecord) error {
	tx, err := p.backend.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := p.saveTenantHotOn(ctx, tx, tenantID, hot, version); err != nil {
		return err
	}
	if err := p.appendLedgerOn(ctx, tx, records); err != nil {
		return err
	}
	return tx.Commit()
}

func (p *postgresSplitStore) appendLedger(ctx context.Context, records []LedgerRecord) error {
	return p.appendLedgerOn(ctx, p.backend.db, records)
}

func (p *postgresSplitStore) appendLedgerOn(ctx context.Context, db tenantCommitSQL, records []LedgerRecord) error {
	for _, record := range records {
		if err := validateLedgerRecord(record); err != nil {
			return err
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		result, err := db.ExecContext(ctx, postgresLedgerAppendQuery, record.TenantID, record.RecordType, record.Key, record.Version, string(encoded), record.WrittenAt)
		if err != nil {
			return fmt.Errorf("append tenant ledger %s/%s: %w", record.TenantID, record.Key, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected != 0 {
			continue
		}
		var sqlTenant, sqlType, sqlKey string
		var sqlVersion int
		var existingEncoded []byte
		err = db.QueryRowContext(ctx, `SELECT tenant_id, record_type, key, version, record FROM audit_ledger WHERE tenant_id = $1 AND record_type = $2 AND key = $3 AND version = $4`, record.TenantID, record.RecordType, record.Key, record.Version).Scan(&sqlTenant, &sqlType, &sqlKey, &sqlVersion, &existingEncoded)
		if err != nil {
			return fmt.Errorf("inspect existing tenant ledger %s/%s: %w", record.TenantID, record.Key, err)
		}
		existing, err := decodeLedgerRecord(existingEncoded, record.TenantID)
		if err != nil {
			return err
		}
		if err := validatePostgresLedgerIdentity(existing, sqlTenant, LedgerRecordType(sqlType), sqlKey, sqlVersion); err != nil {
			return err
		}
		if !immutableLedgerRecordEqual(existing, record) {
			return ledgerConflict(record)
		}
	}
	return nil
}

func (p *postgresSplitStore) ledgerScan(ctx context.Context, tenantID string, fn func(LedgerRecord) error) error {
	reader := &postgresLedgerReader{ctx: ctx, db: p.backend.db, tenantID: tenantID}
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

func (p *postgresSplitStore) materialize(ctx context.Context) (*Snapshot, error) {
	control, _, err := p.loadControl(ctx)
	if err != nil {
		return nil, err
	}
	ids, err := p.tenantIDs(ctx)
	if err != nil {
		return nil, err
	}
	result, err := cloneSnapshot(control)
	if err != nil {
		return nil, err
	}
	for _, tenantID := range ids {
		hot, _, err := p.loadTenantHot(ctx, tenantID)
		if err != nil {
			return nil, err
		}
		copyHot, err := cloneTenantHot(hot)
		if err != nil {
			return nil, err
		}
		for key, event := range copyHot.Events {
			result.Events[key] = event
		}
		for key, stream := range copyHot.Streams {
			result.Streams[key] = stream
		}
		records, err := (&postgresLedgerReader{ctx: ctx, db: p.backend.db, tenantID: tenantID}).records()
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

func (p *postgresSplitStore) read(ctx context.Context, fn func(*Snapshot) error) error {
	data, err := p.materialize(ctx)
	if err != nil {
		return err
	}
	return fn(data)
}

func (p *postgresSplitStore) update(ctx context.Context, fn func(*Snapshot) error) error {
	return p.updateChecked(ctx, func(data *Snapshot) (bool, error) {
		if err := fn(data); err != nil {
			return false, err
		}
		return true, nil
	})
}

func (p *postgresSplitStore) updateChecked(ctx context.Context, fn func(*Snapshot) (bool, error)) error {
	before, err := p.materialize(ctx)
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
		return p.updateControl(ctx, func(control *Snapshot) error {
			copyControl, err := controlSnapshot(working)
			if err != nil {
				return err
			}
			*control = *copyControl
			return nil
		})
	}
	return p.replaceSnapshot(ctx, working)
}

func tenantDataEqual(left, right *Snapshot) bool {
	return reflect.DeepEqual(left.Events, right.Events) && reflect.DeepEqual(left.Receipts, right.Receipts) && reflect.DeepEqual(left.Streams, right.Streams) && reflect.DeepEqual(left.Segments, right.Segments) && reflect.DeepEqual(left.Checkpoints, right.Checkpoints)
}

func (p *postgresSplitStore) replaceSnapshot(ctx context.Context, data *Snapshot) error {
	prepared, err := splitSnapshot(data)
	if err != nil {
		return err
	}
	ids, err := p.tenantIDs(ctx)
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
			_, version, err = p.loadTenantHot(ctx, id)
			if err != nil {
				return err
			}
		}
		if err := p.saveTenantHot(ctx, id, state.hot, version); err != nil {
			return err
		}
		if err := p.appendLedger(ctx, state.ledger.committedRecords()); err != nil {
			return err
		}
	}
	return p.updateControl(ctx, func(control *Snapshot) error {
		*control = *prepared.control
		return nil
	})
}

type postgresLedgerReader struct {
	// ctx is the store operation context the reader was created under; every
	// statement it issues is bounded by it.
	ctx      context.Context
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
	query := `SELECT tenant_id, record_type, key, version, record FROM audit_ledger WHERE tenant_id = $1 AND record_type = 'receipt' ` + filter + ` ORDER BY version DESC LIMIT 1`
	var sqlTenant, sqlType, sqlKey string
	var version int
	var encoded []byte
	err := r.db.QueryRowContext(r.ctx, query, r.tenantID, key).Scan(&sqlTenant, &sqlType, &sqlKey, &version, &encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.EventReceipt{}, 0, false, nil
	}
	if err != nil {
		return domain.EventReceipt{}, 0, false, err
	}
	record, err := decodeLedgerRecord(encoded, r.tenantID)
	if err != nil {
		return domain.EventReceipt{}, 0, false, err
	}
	if err := validatePostgresLedgerIdentity(record, sqlTenant, LedgerRecordType(sqlType), sqlKey, version); err != nil {
		return domain.EventReceipt{}, 0, false, err
	}
	if record.Receipt == nil {
		return domain.EventReceipt{}, 0, false, errOrLedgerPayload(nil, record)
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
	rows, err := r.db.QueryContext(r.ctx, `SELECT tenant_id, record_type, key, version, record FROM audit_ledger WHERE tenant_id = $1 AND record_type = $2 ORDER BY version, key`, r.tenantID, kind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []LedgerRecord
	for rows.Next() {
		var sqlTenant, sqlType, sqlKey string
		var sqlVersion int
		var encoded []byte
		if err := rows.Scan(&sqlTenant, &sqlType, &sqlKey, &sqlVersion, &encoded); err != nil {
			return nil, err
		}
		record, err := decodeLedgerRecord(encoded, r.tenantID)
		if err != nil {
			return nil, err
		}
		if err := validatePostgresLedgerIdentity(record, sqlTenant, LedgerRecordType(sqlType), sqlKey, sqlVersion); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (r *postgresLedgerReader) records() ([]LedgerRecord, error) {
	rows, err := r.db.QueryContext(r.ctx, `SELECT tenant_id, record_type, key, version, record FROM audit_ledger WHERE tenant_id = $1 ORDER BY id`, r.tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []LedgerRecord
	for rows.Next() {
		var sqlTenant, sqlType, sqlKey string
		var sqlVersion int
		var encoded []byte
		if err := rows.Scan(&sqlTenant, &sqlType, &sqlKey, &sqlVersion, &encoded); err != nil {
			return nil, err
		}
		record, err := decodeLedgerRecord(encoded, r.tenantID)
		if err != nil {
			return nil, err
		}
		if err := validatePostgresLedgerIdentity(record, sqlTenant, LedgerRecordType(sqlType), sqlKey, sqlVersion); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func validatePostgresLedgerIdentity(record LedgerRecord, sqlTenant string, sqlType LedgerRecordType, sqlKey string, sqlVersion int) error {
	if record.TenantID != sqlTenant || record.RecordType != LedgerRecordType(sqlType) || record.Key != sqlKey || record.Version != sqlVersion {
		return fmt.Errorf("postgres ledger identity mismatch: sql=(%q,%q,%q,%d) payload=(%q,%q,%q,%d)", sqlTenant, sqlType, sqlKey, sqlVersion, record.TenantID, record.RecordType, record.Key, record.Version)
	}
	return nil
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
