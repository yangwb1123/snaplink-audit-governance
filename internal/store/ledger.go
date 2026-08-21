package store

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

// CommitOrder selects the safe ordering for a tenant mutation. HotFirst is
// used by ingest: the event and stream linkage become visible before their
// receipt records. ColdFirst is used by eviction: the archived receipt is
// durable before the hot event can be removed.
type CommitOrder int

const (
	HotFirst CommitOrder = iota
	ColdFirst
)

// LedgerRecordType names the immutable record families stored in the cold
// tier. The strings are part of the JSONL/SQL compatibility contract.
type LedgerRecordType string

const (
	LedgerReceipt    LedgerRecordType = "receipt"
	LedgerSegment    LedgerRecordType = "segment"
	LedgerCheckpoint LedgerRecordType = "checkpoint"
)

// LedgerRecord is one append-only cold-ledger entry. Exactly one payload
// pointer is populated according to RecordType. Version is monotonic per
// (tenant, RecordType, Key); retries are idempotent at that tuple.
type LedgerRecord struct {
	TenantID   string               `json:"tenant_id"`
	RecordType LedgerRecordType     `json:"record_type"`
	Key        string               `json:"key"`
	Version    int                  `json:"version"`
	Receipt    *domain.EventReceipt `json:"receipt,omitempty"`
	Segment    *domain.Segment      `json:"segment,omitempty"`
	Checkpoint *domain.Checkpoint   `json:"checkpoint,omitempty"`
	WrittenAt  time.Time            `json:"written_at"`
}

// TenantLedger is the committed read index plus a private append buffer for
// one UpdateTenant pass. Read methods intentionally ignore pending records:
// a closure sees the last committed cold state, and the commit boundary is
// the only place where buffered records become visible.
type TenantLedger struct {
	tenantID      string
	records       []LedgerRecord
	reader        ledgerReader
	readerErr     error
	recordsLoaded bool
	parent        *TenantLedger

	receipts           map[string]domain.EventReceipt
	receiptVersions    map[string]int
	idempotency        map[string]string
	segments           map[string][]domain.Segment
	checkpoints        map[string][]domain.Checkpoint
	committedRecordKey map[string]struct{}

	pending          []LedgerRecord
	pendingReceipts  map[string]domain.EventReceipt
	pendingRecordKey map[string]struct{}
}

type ledgerReader interface {
	receipt(key string) (domain.EventReceipt, int, bool, error)
	idempotency(key string) (domain.EventReceipt, int, bool, error)
	segments(streamKey string) ([]domain.Segment, error)
	checkpoints(streamKey string) ([]domain.Checkpoint, error)
	records() ([]LedgerRecord, error)
}

// NewTenantLedger creates an empty in-memory ledger. Store implementations
// normally construct one from durable records; the constructor is exported
// for deterministic store-level tests and migration tooling.
func NewTenantLedger(tenantID string) *TenantLedger {
	return newTenantLedger(tenantID, nil)
}

func newTenantLedger(tenantID string, records []LedgerRecord) *TenantLedger {
	l := &TenantLedger{
		tenantID:           tenantID,
		recordsLoaded:      true,
		receipts:           map[string]domain.EventReceipt{},
		receiptVersions:    map[string]int{},
		idempotency:        map[string]string{},
		segments:           map[string][]domain.Segment{},
		checkpoints:        map[string][]domain.Checkpoint{},
		committedRecordKey: map[string]struct{}{},
		pendingReceipts:    map[string]domain.EventReceipt{},
		pendingRecordKey:   map[string]struct{}{},
	}
	for _, record := range records {
		l.applyRecord(record)
	}
	return l
}

func newTenantLedgerWithReader(tenantID string, reader ledgerReader) *TenantLedger {
	l := newTenantLedger(tenantID, nil)
	l.reader = reader
	l.recordsLoaded = false
	return l
}

func newTenantLedgerView(parent *TenantLedger) *TenantLedger {
	return &TenantLedger{
		tenantID:           parent.tenantID,
		parent:             parent,
		receipts:           map[string]domain.EventReceipt{},
		receiptVersions:    map[string]int{},
		idempotency:        map[string]string{},
		segments:           map[string][]domain.Segment{},
		checkpoints:        map[string][]domain.Checkpoint{},
		committedRecordKey: map[string]struct{}{},
		pendingReceipts:    map[string]domain.EventReceipt{},
		pendingRecordKey:   map[string]struct{}{},
		recordsLoaded:      true,
	}
}

// Receipt returns the latest committed receipt for key. The key is normally
// EventKey(tenantID, eventID), but accepting an explicit key keeps the API
// useful to migration and inspection tools.
func (l *TenantLedger) Receipt(key string) (domain.EventReceipt, bool) {
	receipt, ok := l.receipts[key]
	if !ok && l.reader != nil && l.readerErr == nil {
		var version int
		receipt, version, ok, l.readerErr = l.reader.receipt(key)
		if l.readerErr == nil && ok {
			l.receipts[key] = receipt
			l.receiptVersions[key] = version
			if receipt.IdempotencyKey != "" {
				l.idempotency[receipt.IdempotencyKey] = key
			}
		}
	}
	if !ok && l.parent != nil {
		receipt, ok = l.parent.Receipt(key)
		if ok {
			l.receipts[key] = receipt
			l.receiptVersions[key] = l.parent.receiptVersions[key]
		}
	}
	return receipt, ok
}

// FindReceiptByIdempotencyKey is the O(1) idempotency index used by ingest.
// Empty keys are deliberately not indexed because legacy receipts may omit
// the additive field and an empty value is not a usable idempotency key.
func (l *TenantLedger) FindReceiptByIdempotencyKey(key string) (domain.EventReceipt, bool) {
	if key == "" {
		return domain.EventReceipt{}, false
	}
	eventKey, ok := l.idempotency[key]
	var receipt domain.EventReceipt
	if !ok && l.reader != nil && l.readerErr == nil {
		var version int
		receipt, version, ok, l.readerErr = l.reader.idempotency(key)
		if l.readerErr == nil && ok {
			eventKey = EventKey(receipt.TenantID, receipt.EventID)
			l.receipts[eventKey] = receipt
			l.receiptVersions[eventKey] = version
			l.idempotency[key] = eventKey
		}
	}
	if !ok && l.parent != nil {
		receipt, ok = l.parent.FindReceiptByIdempotencyKey(key)
		if ok {
			eventKey = EventKey(receipt.TenantID, receipt.EventID)
			l.receipts[eventKey] = receipt
			l.idempotency[key] = eventKey
			l.receiptVersions[eventKey] = l.parent.receiptVersions[eventKey]
		}
	}
	if !ok {
		return domain.EventReceipt{}, false
	}
	receipt, ok = l.receipts[eventKey]
	return receipt, ok
}

// SetReceipt buffers the next changed version of a receipt. Repeating the
// same value is a no-op, which makes closure retries and two-phase recovery
// safe without producing duplicate cold records.
func (l *TenantLedger) SetReceipt(receipt domain.EventReceipt) {
	if receipt.TenantID == "" {
		receipt.TenantID = l.tenantID
	}
	key := EventKey(receipt.TenantID, receipt.EventID)
	// An overlay may be asked to write a receipt without a preceding read in
	// the closure. Load the parent/latest reader first so the next immutable
	// version remains monotonic instead of restarting at v1.
	if l.reader != nil || l.parent != nil {
		_, _ = l.Receipt(key)
	}
	if previous, ok := l.pendingReceipts[key]; ok && reflect.DeepEqual(previous, receipt) {
		return
	}
	if previous, ok := l.receipts[key]; ok && reflect.DeepEqual(previous, receipt) {
		if _, pending := l.pendingReceipts[key]; !pending {
			return
		}
	}
	version := l.receiptVersions[key] + 1
	record := LedgerRecord{TenantID: l.tenantID, RecordType: LedgerReceipt, Key: key, Version: version, Receipt: cloneReceiptPtr(&receipt), WrittenAt: time.Now().UTC()}
	if _, ok := l.pendingReceipts[key]; ok {
		for index, pending := range l.pending {
			if pending.RecordType == LedgerReceipt && pending.Key == key {
				delete(l.pendingRecordKey, ledgerRecordIdentity(pending))
				l.pending[index] = record
				l.pendingRecordKey[ledgerRecordIdentity(record)] = struct{}{}
				l.pendingReceipts[key] = receipt
				return
			}
		}
	}
	l.pending = append(l.pending, record)
	l.pendingReceipts[key] = receipt
	l.pendingRecordKey[ledgerRecordIdentity(record)] = struct{}{}
}

// Segments returns a copy of committed segments for a stream.
func (l *TenantLedger) Segments(streamKey string) []domain.Segment {
	values, ok := l.segments[streamKey]
	if !ok && l.reader != nil && l.readerErr == nil {
		values, l.readerErr = l.reader.segments(streamKey)
		if l.readerErr == nil {
			l.segments[streamKey] = values
		}
	}
	if !ok && l.parent != nil {
		return l.parent.Segments(streamKey)
	}
	return cloneSegments(values)
}

// AppendSegment buffers an immutable segment. A segment's stream and first
// sequence identify it, so a retry never appends the same segment twice.
func (l *TenantLedger) AppendSegment(segment domain.Segment) {
	key := segmentLedgerKey(segment)
	if l.hasRecord(LedgerSegment, key, 1) {
		return
	}
	record := LedgerRecord{TenantID: l.tenantID, RecordType: LedgerSegment, Key: key, Version: 1, Segment: cloneSegmentPtr(&segment), WrittenAt: time.Now().UTC()}
	l.pending = append(l.pending, record)
	l.pendingRecordKey[ledgerRecordIdentity(record)] = struct{}{}
}

// Checkpoints returns a copy of committed checkpoints for a stream.
func (l *TenantLedger) Checkpoints(streamKey string) []domain.Checkpoint {
	values, ok := l.checkpoints[streamKey]
	if !ok && l.reader != nil && l.readerErr == nil {
		values, l.readerErr = l.reader.checkpoints(streamKey)
		if l.readerErr == nil {
			l.checkpoints[streamKey] = values
		}
	}
	if !ok && l.parent != nil {
		return l.parent.Checkpoints(streamKey)
	}
	return cloneCheckpoints(values)
}

// Records returns committed cold records in a deterministic copy. It is
// intended for bounded tenant-scoped worker passes; pending records remain
// private until UpdateTenant reaches its commit boundary.
func (l *TenantLedger) Records() []LedgerRecord {
	if l.reader != nil && !l.recordsLoaded && l.readerErr == nil {
		l.records, l.readerErr = l.reader.records()
		l.recordsLoaded = l.readerErr == nil
		if l.readerErr == nil {
			loaded := newTenantLedger(l.tenantID, l.records)
			l.receipts = loaded.receipts
			l.receiptVersions = loaded.receiptVersions
			l.idempotency = loaded.idempotency
			l.segments = loaded.segments
			l.checkpoints = loaded.checkpoints
			l.committedRecordKey = loaded.committedRecordKey
		}
	}
	if l.parent != nil {
		return l.parent.Records()
	}
	result := l.committedRecords()
	sortLedgerRecords(result)
	return result
}

// Err reports a lazy cold-ledger query failure captured by the API-compatible
// read methods. UpdateTenant checks it before committing; read paths surface
// it after their closure so a database outage cannot look like a new event.
func (l *TenantLedger) Err() error { return l.readerErr }

// AppendCheckpoint buffers an immutable checkpoint. Checkpoint IDs are
// globally unique in the domain and therefore provide the idempotency key.
func (l *TenantLedger) AppendCheckpoint(checkpoint domain.Checkpoint) {
	key := checkpoint.ID
	if key == "" {
		key = checkpointLedgerKey(checkpoint)
	}
	if l.hasRecord(LedgerCheckpoint, key, 1) {
		return
	}
	record := LedgerRecord{TenantID: l.tenantID, RecordType: LedgerCheckpoint, Key: key, Version: 1, Checkpoint: cloneCheckpointPtr(&checkpoint), WrittenAt: time.Now().UTC()}
	l.pending = append(l.pending, record)
	l.pendingRecordKey[ledgerRecordIdentity(record)] = struct{}{}
}

func (l *TenantLedger) hasRecord(recordType LedgerRecordType, key string, version int) bool {
	identity := ledgerRecordIdentity(LedgerRecord{TenantID: l.tenantID, RecordType: recordType, Key: key, Version: version})
	if _, ok := l.pendingRecordKey[identity]; ok {
		return true
	}
	for _, record := range l.records {
		if record.RecordType == recordType && record.Key == key && record.Version == version {
			return true
		}
	}
	if l.parent != nil && l.parent.hasRecord(recordType, key, version) {
		return true
	}
	return false
}

func (l *TenantLedger) applyRecord(record LedgerRecord) {
	if record.TenantID == "" {
		record.TenantID = l.tenantID
	}
	if record.TenantID != l.tenantID || record.Version <= 0 {
		return
	}
	if l.hasCommittedRecord(record.RecordType, record.Key, record.Version) {
		return
	}
	switch record.RecordType {
	case LedgerReceipt:
		if record.Receipt == nil {
			return
		}
		if current, ok := l.receiptVersions[record.Key]; ok && current >= record.Version {
			return
		}
		receipt := *record.Receipt
		l.receipts[record.Key] = receipt
		l.receiptVersions[record.Key] = record.Version
		if receipt.IdempotencyKey != "" {
			l.idempotency[receipt.IdempotencyKey] = record.Key
		}
	case LedgerSegment:
		if record.Segment == nil {
			return
		}
		streamKey := StreamKey(record.Segment.TenantID, record.Segment.StreamID)
		l.segments[streamKey] = append(l.segments[streamKey], *record.Segment)
	case LedgerCheckpoint:
		if record.Checkpoint == nil {
			return
		}
		streamKey := StreamKey(record.Checkpoint.TenantID, record.Checkpoint.StreamID)
		l.checkpoints[streamKey] = append(l.checkpoints[streamKey], *record.Checkpoint)
	}
	l.records = append(l.records, cloneLedgerRecord(record))
	l.committedRecordKey[ledgerRecordIdentity(record)] = struct{}{}
}

func (l *TenantLedger) hasCommittedRecord(recordType LedgerRecordType, key string, version int) bool {
	identity := ledgerRecordIdentity(LedgerRecord{TenantID: l.tenantID, RecordType: recordType, Key: key, Version: version})
	if _, ok := l.committedRecordKey[identity]; ok {
		return true
	}
	if l.parent != nil && l.parent.hasCommittedRecord(recordType, key, version) {
		return true
	}
	return false
}

func (l *TenantLedger) committedRecords() []LedgerRecord {
	result := make([]LedgerRecord, len(l.records))
	for i, record := range l.records {
		result[i] = cloneLedgerRecord(record)
	}
	return result
}

func (l *TenantLedger) pendingRecords() []LedgerRecord {
	result := make([]LedgerRecord, len(l.pending))
	for i, record := range l.pending {
		result[i] = cloneLedgerRecord(record)
	}
	return result
}

func (l *TenantLedger) commitPending(records []LedgerRecord) {
	for _, record := range records {
		if l.hasCommittedRecord(record.RecordType, record.Key, record.Version) {
			continue
		}
		l.applyRecord(record)
	}
	l.pending = nil
	l.pendingReceipts = map[string]domain.EventReceipt{}
	l.pendingRecordKey = map[string]struct{}{}
}

func (l *TenantLedger) clearPending() {
	l.pending = nil
	l.pendingReceipts = map[string]domain.EventReceipt{}
	l.pendingRecordKey = map[string]struct{}{}
	for key := range l.receipts {
		l.receiptVersions[key] = receiptVersionFor(l.records, key)
	}
}

func receiptVersionFor(records []LedgerRecord, key string) int {
	version := 0
	for _, record := range records {
		if record.RecordType == LedgerReceipt && record.Key == key && record.Version > version {
			version = record.Version
		}
	}
	return version
}

func ledgerRecordIdentity(record LedgerRecord) string {
	return string(record.RecordType) + KeySeparator + record.Key + KeySeparator + strconv.Itoa(record.Version)
}

func segmentLedgerKey(segment domain.Segment) string {
	return StreamKey(segment.TenantID, segment.StreamID) + KeySeparator + strconv.FormatInt(segment.FirstSequence, 10)
}

func checkpointLedgerKey(checkpoint domain.Checkpoint) string {
	return StreamKey(checkpoint.TenantID, checkpoint.StreamID) + KeySeparator + strconv.FormatInt(checkpoint.Sequence, 10)
}

func cloneLedgerRecord(record LedgerRecord) LedgerRecord {
	clone := record
	clone.Receipt = cloneReceiptPtr(record.Receipt)
	clone.Segment = cloneSegmentPtr(record.Segment)
	clone.Checkpoint = cloneCheckpointPtr(record.Checkpoint)
	return clone
}

func cloneReceiptPtr(value *domain.EventReceipt) *domain.EventReceipt {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneSegmentPtr(value *domain.Segment) *domain.Segment {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneCheckpointPtr(value *domain.Checkpoint) *domain.Checkpoint {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneSegments(values []domain.Segment) []domain.Segment {
	if len(values) == 0 {
		return nil
	}
	return append([]domain.Segment(nil), values...)
}

func cloneCheckpoints(values []domain.Checkpoint) []domain.Checkpoint {
	if len(values) == 0 {
		return nil
	}
	result := make([]domain.Checkpoint, len(values))
	for i, value := range values {
		result[i] = value
	}
	return result
}

func sortLedgerRecords(records []LedgerRecord) {
	sort.SliceStable(records, func(i, j int) bool {
		left, right := records[i], records[j]
		if left.RecordType != right.RecordType {
			return left.RecordType < right.RecordType
		}
		if left.Key != right.Key {
			return left.Key < right.Key
		}
		return left.Version < right.Version
	})
}

func validateLedgerRecord(record LedgerRecord) error {
	if record.TenantID == "" || record.RecordType == "" || record.Key == "" || record.Version <= 0 {
		return fmt.Errorf("invalid ledger record identity")
	}
	switch record.RecordType {
	case LedgerReceipt:
		if record.Receipt == nil {
			return fmt.Errorf("receipt ledger record %s has no receipt", record.Key)
		}
	case LedgerSegment:
		if record.Segment == nil {
			return fmt.Errorf("segment ledger record %s has no segment", record.Key)
		}
	case LedgerCheckpoint:
		if record.Checkpoint == nil {
			return fmt.Errorf("checkpoint ledger record %s has no checkpoint", record.Key)
		}
	default:
		return fmt.Errorf("unknown ledger record type %q", record.RecordType)
	}
	return nil
}
