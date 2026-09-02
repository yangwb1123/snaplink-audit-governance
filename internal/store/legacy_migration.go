package store

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/snaplink/audit-governance/internal/domain"
)

// ErrLegacySnapshotOwnership identifies a legacy snapshot that cannot be
// safely assigned to v2 tenant stores. It is kept separate from the generic
// snapshot and ledger validators because those also validate existing v2 data.
var ErrLegacySnapshotOwnership = errors.New("legacy snapshot ownership validation failed")

type legacySnapshotOwnershipError struct {
	family   string
	key      string
	index    int
	hasIndex bool
	reason   string
}

func (e *legacySnapshotOwnershipError) Error() string {
	index := ""
	if e.hasIndex {
		index = fmt.Sprintf("\nindex=%d", e.index)
	}
	return fmt.Sprintf("%s:\nfamily=%s\nkey=%s%s\nreason=%s\nrepair or restore the legacy snapshot before retrying", ErrLegacySnapshotOwnership, e.family, e.key, index, e.reason)
}

func (e *legacySnapshotOwnershipError) Unwrap() error { return ErrLegacySnapshotOwnership }

func legacyOwnershipFailure(family, key, reason string, index int, hasIndex bool) error {
	return &legacySnapshotOwnershipError{family: family, key: key, reason: reason, index: index, hasIndex: hasIndex}
}

func legacyRegisteredTenants(data *Snapshot) map[string]struct{} {
	registered := make(map[string]struct{}, len(data.Tenants))
	for tenantID := range data.Tenants {
		if tenantID != "" {
			registered[tenantID] = struct{}{}
		}
	}
	return registered
}

func legacyTenantIDs(registered map[string]struct{}) []string {
	ids := make([]string, 0, len(registered))
	for tenantID := range registered {
		ids = append(ids, tenantID)
	}
	sort.Strings(ids)
	return ids
}

func legacySourceTenant(family, key string, registered map[string]struct{}, index int, hasIndex bool) (string, error) {
	tenantID, _, ok := SplitTenantKey(key)
	if !ok {
		return "", legacyOwnershipFailure(family, key, "malformed key", index, hasIndex)
	}
	if _, exists := registered[tenantID]; !exists {
		return "", legacyOwnershipFailure(family, key, "unregistered key tenant", index, hasIndex)
	}
	return tenantID, nil
}

func validateLegacyPayloadTenant(family, key, keyTenant, payloadTenant string, registered map[string]struct{}, index int, hasIndex bool) error {
	if payloadTenant == "" {
		return legacyOwnershipFailure(family, key, "empty payload tenant", index, hasIndex)
	}
	if _, exists := registered[payloadTenant]; !exists {
		return legacyOwnershipFailure(family, key, "unregistered payload tenant", index, hasIndex)
	}
	if payloadTenant != keyTenant {
		return legacyOwnershipFailure(family, key, "tenant mismatch", index, hasIndex)
	}
	return nil
}

func sortedLegacyKeys[T any](records map[string]T) []string {
	keys := make([]string, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// validateLegacySnapshotOwnership validates every tenant-bearing map entry.
// It deliberately uses only Snapshot.Tenants as the registration source; no
// record can create an implicit destination tenant during cutover.
func validateLegacySnapshotOwnership(data *Snapshot) error {
	if data == nil {
		return fmt.Errorf("%w: nil snapshot", ErrLegacySnapshotOwnership)
	}
	registered := legacyRegisteredTenants(data)
	if err := validateLegacyEvents(data.Events, registered); err != nil {
		return err
	}
	if err := validateLegacyReceipts(data.Receipts, registered); err != nil {
		return err
	}
	if err := validateLegacyStreams(data.Streams, registered); err != nil {
		return err
	}
	if err := validateLegacySegments(data.Segments, registered); err != nil {
		return err
	}
	return validateLegacyCheckpoints(data.Checkpoints, registered)
}

func validateLegacyEvents(events map[string]domain.Event, registered map[string]struct{}) error {
	for _, key := range sortedLegacyKeys(events) {
		tenantID, err := legacySourceTenant("events", key, registered, 0, false)
		if err != nil {
			return err
		}
		if err := validateLegacyPayloadTenant("events", key, tenantID, events[key].TenantID, registered, 0, false); err != nil {
			return err
		}
	}
	return nil
}

func validateLegacyReceipts(receipts map[string]domain.EventReceipt, registered map[string]struct{}) error {
	for _, key := range sortedLegacyKeys(receipts) {
		tenantID, err := legacySourceTenant("receipts", key, registered, 0, false)
		if err != nil {
			return err
		}
		if err := validateLegacyPayloadTenant("receipts", key, tenantID, receipts[key].TenantID, registered, 0, false); err != nil {
			return err
		}
	}
	return nil
}

func validateLegacyStreams(streams map[string]StreamState, registered map[string]struct{}) error {
	for _, key := range sortedLegacyKeys(streams) {
		tenantID, err := legacySourceTenant("streams", key, registered, 0, false)
		if err != nil {
			return err
		}
		if err := validateLegacyPayloadTenant("streams", key, tenantID, streams[key].TenantID, registered, 0, false); err != nil {
			return err
		}
	}
	return nil
}

func validateLegacySegments(segments map[string][]domain.Segment, registered map[string]struct{}) error {
	for _, key := range sortedLegacyKeys(segments) {
		values := segments[key]
		if len(values) == 0 {
			if _, err := legacySourceTenant("segments", key, registered, 0, false); err != nil {
				return err
			}
		}
		for index, segment := range values {
			tenantID, err := legacySourceTenant("segments", key, registered, index, true)
			if err != nil {
				return err
			}
			if err := validateLegacyPayloadTenant("segments", key, tenantID, segment.TenantID, registered, index, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateLegacyCheckpoints(checkpoints map[string][]domain.Checkpoint, registered map[string]struct{}) error {
	for _, key := range sortedLegacyKeys(checkpoints) {
		values := checkpoints[key]
		if len(values) == 0 {
			if _, err := legacySourceTenant("checkpoints", key, registered, 0, false); err != nil {
				return err
			}
		}
		for index, checkpoint := range values {
			tenantID, err := legacySourceTenant("checkpoints", key, registered, index, true)
			if err != nil {
				return err
			}
			if err := validateLegacyPayloadTenant("checkpoints", key, tenantID, checkpoint.TenantID, registered, index, true); err != nil {
				return err
			}
		}
	}
	return nil
}

// splitLegacySnapshot is the strict splitter used only by PostgreSQL legacy
// cutover. Generic file/v2 snapshot handling intentionally keeps its existing
// compatibility behavior in splitSnapshot.
func splitLegacySnapshot(data *Snapshot) (*preparedSplit, error) {
	if err := validateLegacySnapshotOwnership(data); err != nil {
		return nil, err
	}
	registered := legacyRegisteredTenants(data)
	control, err := controlSnapshot(data)
	if err != nil {
		return nil, err
	}
	prepared := &preparedSplit{control: control, tenant: map[string]*tenantState{}}
	for _, tenantID := range legacyTenantIDs(registered) {
		state, err := splitLegacyTenantSnapshot(data, tenantID)
		if err != nil {
			return nil, err
		}
		prepared.tenant[tenantID] = state
	}
	return prepared, nil
}

func splitLegacyTenantSnapshot(data *Snapshot, tenantID string) (*tenantState, error) {
	hot := emptyTenantHot(tenantID)
	for _, key := range sortedLegacyKeys(data.Events) {
		event := data.Events[key]
		if keyBelongsToTenant(key, tenantID) {
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
	for _, key := range sortedLegacyKeys(data.Streams) {
		stream := data.Streams[key]
		if keyBelongsToTenant(key, tenantID) {
			hot.Streams[key] = stream
		}
	}
	records := make([]LedgerRecord, 0)
	for _, key := range sortedLegacyKeys(data.Receipts) {
		receipt := data.Receipts[key]
		if !keyBelongsToTenant(key, tenantID) {
			continue
		}
		record := LedgerRecord{TenantID: tenantID, RecordType: LedgerReceipt, Key: key, Version: 1, Receipt: cloneReceiptPtr(&receipt)}
		if err := appendLegacyLedgerRecord(&records, key, record); err != nil {
			return nil, err
		}
	}
	for _, key := range sortedLegacyKeys(data.Segments) {
		values := data.Segments[key]
		if !keyBelongsToTenant(key, tenantID) {
			continue
		}
		for _, segment := range values {
			record := LedgerRecord{TenantID: tenantID, RecordType: LedgerSegment, Key: segmentLedgerKey(segment), Version: 1, Segment: cloneSegmentPtr(&segment)}
			if err := appendLegacyLedgerRecord(&records, key, record); err != nil {
				return nil, err
			}
		}
	}
	for _, key := range sortedLegacyKeys(data.Checkpoints) {
		values := data.Checkpoints[key]
		if !keyBelongsToTenant(key, tenantID) {
			continue
		}
		for _, checkpoint := range values {
			record := LedgerRecord{TenantID: tenantID, RecordType: LedgerCheckpoint, Key: checkpoint.ID, Version: 1, Checkpoint: cloneCheckpointPtr(&checkpoint)}
			if err := appendLegacyLedgerRecord(&records, key, record); err != nil {
				return nil, err
			}
		}
	}
	sortLedgerRecords(records)
	return &tenantState{hot: hot, ledger: newTenantLedger(tenantID, records), version: 1}, nil
}

func appendLegacyLedgerRecord(records *[]LedgerRecord, sourceKey string, record LedgerRecord) error {
	if err := validateLegacyLedgerRecord(sourceKey, record); err != nil {
		return err
	}
	*records = append(*records, record)
	return nil
}

// validateLegacyLedgerRecord extends the structural validator for records
// generated during legacy cutover with source-key and payload ownership checks.
func validateLegacyLedgerRecord(sourceKey string, record LedgerRecord) error {
	if err := validateLedgerRecord(record); err != nil {
		return err
	}
	switch record.RecordType {
	case LedgerReceipt:
		if err := validateLegacyRecordSource("receipts", sourceKey, record.TenantID); err != nil {
			return err
		}
		if record.Receipt.TenantID != record.TenantID {
			return legacyOwnershipFailure("receipts", sourceKey, "tenant mismatch", 0, false)
		}
	case LedgerSegment:
		if err := validateLegacyRecordSource("segments", sourceKey, record.TenantID); err != nil {
			return err
		}
		if record.Segment.TenantID != record.TenantID {
			return legacyOwnershipFailure("segments", sourceKey, "tenant mismatch", 0, false)
		}
		prefix, _, found := strings.Cut(record.Key, KeySeparator)
		if !found || prefix != record.TenantID || record.Key != segmentLedgerKey(*record.Segment) {
			return legacyOwnershipFailure("segments", sourceKey, "tenant mismatch", 0, false)
		}
	case LedgerCheckpoint:
		if err := validateLegacyRecordSource("checkpoints", sourceKey, record.TenantID); err != nil {
			return err
		}
		if record.Checkpoint.TenantID != record.TenantID || record.Key != record.Checkpoint.ID {
			return legacyOwnershipFailure("checkpoints", sourceKey, "tenant mismatch", 0, false)
		}
	}
	return nil
}

func validateLegacyRecordSource(family, sourceKey, tenantID string) error {
	owner, _, ok := SplitTenantKey(sourceKey)
	if !ok {
		return legacyOwnershipFailure(family, sourceKey, "malformed key", 0, false)
	}
	if owner != tenantID {
		return legacyOwnershipFailure(family, sourceKey, "tenant mismatch", 0, false)
	}
	return nil
}
