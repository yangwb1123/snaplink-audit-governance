package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
)

// ErrHotColdDataInconsistency is returned when a v2 marker cannot be
// substantiated by its durable migration baseline and target rows. Callers
// should fail closed; this error never triggers repair or a fresh migration.
var ErrHotColdDataInconsistency = errors.New("hot/cold data inconsistency")

const postgresHotColdZeroBaselineVersion int64 = 0

type postgresHotColdQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func hotColdInconsistency(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrHotColdDataInconsistency, fmt.Sprintf(format, args...))
}

// postgresHotColdBaseline is derived from the immutable source snapshot. The
// zero baseline is represented by a backup row with version 0 and an otherwise
// empty v2 snapshot. Legacy backup rows retain the source row's original
// version and are split with the same strict converter used by cutover.
func loadPostgresHotColdBaseline(ctx context.Context, queryer postgresHotColdQueryer) (*preparedSplit, error) {
	var encoded []byte
	var version int64
	err := queryer.QueryRowContext(ctx, `
SELECT snapshot, version
FROM audit_state_snapshot_v1_backup
WHERE id = 1`).Scan(&encoded, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, hotColdInconsistency("baseline audit_state_snapshot_v1_backup row is missing")
	}
	if err != nil {
		return nil, hotColdInconsistency("baseline audit_state_snapshot_v1_backup is unavailable: %v", err)
	}
	var source Snapshot
	if err := decodeSnapshot(encoded, &source); err != nil {
		return nil, hotColdInconsistency("baseline audit_state_snapshot_v1_backup cannot be decoded: %v", err)
	}
	if version == postgresHotColdZeroBaselineVersion && isPostgresHotColdZeroBaseline(&source) {
		return &preparedSplit{control: &source, tenant: map[string]*tenantState{}}, nil
	}
	if version <= postgresHotColdZeroBaselineVersion {
		return nil, hotColdInconsistency("baseline audit_state_snapshot_v1_backup has invalid version %d", version)
	}
	if source.LayoutVersion >= hotColdLayoutVersion {
		return nil, hotColdInconsistency("baseline audit_state_snapshot_v1_backup contains a v2 marker without a v1 source snapshot")
	}
	prepared, err := splitLegacySnapshot(&source)
	if err != nil {
		return nil, hotColdInconsistency("baseline audit_state_snapshot_v1_backup is invalid: %v", err)
	}
	return prepared, nil
}

func isPostgresHotColdZeroBaseline(source *Snapshot) bool {
	return source != nil && source.LayoutVersion == hotColdLayoutVersion &&
		len(source.Tenants) == 0 && len(source.Sources) == 0 && len(source.Schemas) == 0 &&
		len(source.Policies) == 0 && len(source.Events) == 0 && len(source.Receipts) == 0 &&
		len(source.Streams) == 0 && len(source.Segments) == 0 && len(source.Checkpoints) == 0 &&
		len(source.LegalHolds) == 0 && len(source.Exports) == 0 && len(source.RestoreRuns) == 0 &&
		len(source.LedgeredOutbox) == 0 && len(source.AdminActions) == 0 &&
		len(source.AggregateCheckpoints) == 0 && len(source.ArchiveConflictFailures) == 0 &&
		len(source.DeadLetters) == 0
}

func verifyPostgresHotColdTargetsEmpty(ctx context.Context, queryer postgresHotColdQueryer) error {
	var actualTenants, actualRecords int64
	if err := queryer.QueryRowContext(ctx, `SELECT count(*) FROM audit_tenant`).Scan(&actualTenants); err != nil {
		return hotColdInconsistency("audit_tenant count query failed while validating fresh migration: %v", err)
	}
	if err := queryer.QueryRowContext(ctx, `SELECT count(*) FROM audit_ledger`).Scan(&actualRecords); err != nil {
		return hotColdInconsistency("audit_ledger count query failed while validating fresh migration: %v", err)
	}
	if actualTenants != 0 || actualRecords != 0 {
		return hotColdInconsistency("fresh migration requires empty targets, actual audit_tenant=%d audit_ledger=%d", actualTenants, actualRecords)
	}
	return nil
}

func verifyPostgresHotColdBaseline(ctx context.Context, queryer postgresHotColdQueryer, baseline *preparedSplit, exactCounts bool) error {
	expectedTenants := len(baseline.tenant)
	var actualTenants int64
	if err := queryer.QueryRowContext(ctx, `SELECT count(*) FROM audit_tenant`).Scan(&actualTenants); err != nil {
		return hotColdInconsistency("audit_tenant count query failed: %v", err)
	}
	if (exactCounts && actualTenants != int64(expectedTenants)) || (!exactCounts && actualTenants < int64(expectedTenants)) {
		return hotColdInconsistency("audit_tenant count expected %s%d, actual %d", countComparator(exactCounts), expectedTenants, actualTenants)
	}

	tenantIDs := make([]string, 0, expectedTenants)
	for tenantID := range baseline.tenant {
		tenantIDs = append(tenantIDs, tenantID)
	}
	sort.Strings(tenantIDs)
	for _, tenantID := range tenantIDs {
		if err := verifyPostgresTenant(ctx, queryer, tenantID, baseline.tenant[tenantID]); err != nil {
			return err
		}
	}

	expectedRecords := expectedPostgresLedgerRecords(baseline)
	var actualRecords int64
	if err := queryer.QueryRowContext(ctx, `SELECT count(*) FROM audit_ledger`).Scan(&actualRecords); err != nil {
		return hotColdInconsistency("audit_ledger count query failed: %v", err)
	}
	if (exactCounts && actualRecords != int64(len(expectedRecords))) || (!exactCounts && actualRecords < int64(len(expectedRecords))) {
		return hotColdInconsistency("audit_ledger count expected %s%d, actual %d", countComparator(exactCounts), len(expectedRecords), actualRecords)
	}
	for _, expected := range expectedRecords {
		if err := verifyPostgresLedgerRecord(ctx, queryer, expected); err != nil {
			return err
		}
	}
	return nil
}

func countComparator(exact bool) string {
	if exact {
		return "exactly "
	}
	return "at least "
}

func verifyPostgresTenant(ctx context.Context, queryer postgresHotColdQueryer, tenantID string, state *tenantState) error {
	var encoded []byte
	var sqlVersion int64
	err := queryer.QueryRowContext(ctx, `SELECT snapshot, version FROM audit_tenant WHERE tenant_id = $1`, tenantID).Scan(&encoded, &sqlVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return hotColdInconsistency("audit_tenant missing identity %q", tenantID)
	}
	if err != nil {
		return hotColdInconsistency("audit_tenant identity %q query failed: %v", tenantID, err)
	}
	var actual tenantHotSnapshot
	if err := decodeJSON(encoded, &actual); err != nil {
		return hotColdInconsistency("audit_tenant identity %q snapshot cannot be decoded: %v", tenantID, err)
	}
	if actual.TenantID != tenantID {
		return hotColdInconsistency("audit_tenant identity %q contains tenant_id %q", tenantID, actual.TenantID)
	}
	if sqlVersion < state.version || actual.Version < state.version || sqlVersion != actual.Version {
		return hotColdInconsistency("audit_tenant identity %q version expected at least %d and matching JSON version, actual column=%d JSON=%d", tenantID, state.version, sqlVersion, actual.Version)
	}
	expected := &tenantHotSnapshot{
		TenantID: tenantID,
		Version:  state.version,
		Events:   state.hot.Events,
		Streams:  state.hot.Streams,
	}
	if !tenantHotContains(&actual, expected) {
		return hotColdInconsistency("audit_tenant identity %q hot snapshot does not contain the migrated data", tenantID)
	}
	return nil
}

// tenantHotContains permits normal post-cutover hot writes while ensuring that
// every event and stream produced by the migration is still present.
func tenantHotContains(actual, expected *tenantHotSnapshot) bool {
	for key, event := range expected.Events {
		current, ok := actual.Events[key]
		if !ok || !reflect.DeepEqual(current, event) {
			return false
		}
	}
	for key, stream := range expected.Streams {
		current, ok := actual.Streams[key]
		if !ok || !reflect.DeepEqual(current, stream) {
			return false
		}
	}
	return true
}

func expectedPostgresLedgerRecords(baseline *preparedSplit) []LedgerRecord {
	records := make([]LedgerRecord, 0)
	for _, state := range baseline.tenant {
		records = append(records, state.ledger.committedRecords()...)
	}
	sortLedgerRecords(records)
	return records
}

func verifyPostgresLedgerRecord(ctx context.Context, queryer postgresHotColdQueryer, expected LedgerRecord) error {
	identity := postgresLedgerIdentity(expected)
	rows, err := queryer.QueryContext(ctx, `
SELECT record
FROM audit_ledger
WHERE tenant_id = $1 AND record_type = $2 AND key = $3 AND version = $4
ORDER BY id`, expected.TenantID, expected.RecordType, expected.Key, expected.Version)
	if err != nil {
		return hotColdInconsistency("audit_ledger identity %s query failed: %v", identity, err)
	}
	defer rows.Close()
	var payloads [][]byte
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			return hotColdInconsistency("audit_ledger identity %s scan failed: %v", identity, err)
		}
		payloads = append(payloads, encoded)
	}
	if err := rows.Err(); err != nil {
		return hotColdInconsistency("audit_ledger identity %s iteration failed: %v", identity, err)
	}
	if len(payloads) == 0 {
		return hotColdInconsistency("audit_ledger missing identity %s", identity)
	}
	if len(payloads) != 1 {
		return hotColdInconsistency("audit_ledger identity %s occurs %d times, expected exactly once", identity, len(payloads))
	}
	var actual LedgerRecord
	actual, err = decodeLedgerRecord(payloads[0], expected.TenantID)
	if err != nil {
		return hotColdInconsistency("audit_ledger identity %s payload is invalid: %v", identity, err)
	}
	if err := jsonPayloadEqual(actual, expected); err != nil {
		return hotColdInconsistency("audit_ledger identity %s payload mismatch: %v", identity, err)
	}
	return nil
}

func jsonPayloadEqual(actual, expected LedgerRecord) error {
	actualJSON, err := json.Marshal(actual)
	if err != nil {
		return err
	}
	expectedJSON, err := json.Marshal(expected)
	if err != nil {
		return err
	}
	if string(actualJSON) != string(expectedJSON) {
		return fmt.Errorf("expected %s, actual %s", string(expectedJSON), string(actualJSON))
	}
	return nil
}

func postgresLedgerIdentity(record LedgerRecord) string {
	return fmt.Sprintf("%q/%q/%q/%d", record.TenantID, record.RecordType, record.Key, record.Version)
}

func createPostgresHotColdBaselineTable(tx *sql.Tx) error {
	_, err := tx.Exec(`CREATE TABLE IF NOT EXISTS audit_state_snapshot_v1_backup (id INTEGER PRIMARY KEY, snapshot JSONB NOT NULL, version BIGINT NOT NULL, updated_at TIMESTAMPTZ NOT NULL)`)
	return err
}

func writePostgresHotColdZeroBaseline(tx *sql.Tx) error {
	baseline := NewSnapshot()
	baseline.LayoutVersion = hotColdLayoutVersion
	encoded, err := json.Marshal(baseline)
	if err != nil {
		return err
	}
	if err := createPostgresHotColdBaselineTable(tx); err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO audit_state_snapshot_v1_backup (id, snapshot, version, updated_at) VALUES (1, $1::jsonb, $2, now())`, string(encoded), postgresHotColdZeroBaselineVersion)
	return err
}
