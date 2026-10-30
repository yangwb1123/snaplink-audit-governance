package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

// Every Snapshot field must be explicitly classified. The recursive fixture
// populates every nested field as well, including future scalar metadata.
func TestControlCloneFieldCoverageAndBidirectionalOwnership(t *testing.T) {
	fields := []string{"LayoutVersion", "Tenants", "Sources", "Schemas", "Policies", "Events", "Receipts", "Streams", "Segments", "Checkpoints", "LegalHolds", "Exports", "ExportLeases", "ExportCheckpoints", "ExportPlans", "RestoreRuns", "LedgeredOutbox", "AdminActions", "AggregateCheckpoints", "ArchiveConflictFailures", "DeadLetters"}
	typ := reflect.TypeOf(Snapshot{})
	if len(fields) != typ.NumField() {
		t.Fatal("classify new Snapshot fields before changing its control clone")
	}
	for i, name := range fields {
		if typ.Field(i).Name != name {
			t.Fatalf("unclassified field %s at %d", typ.Field(i).Name, i)
		}
	}
	for _, mutateSource := range []bool{false, true} {
		t.Run(fmt.Sprintf("mutate-source=%t", mutateSource), func(t *testing.T) {
			source := controlCloneFixture()
			want := controlCloneFixture()
			controlCloneClearEvidence(want)
			got, err := cloneControl(source)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("full control graph changed: err=%v", err)
			}
			if mutateSource {
				controlCloneMutate(reflect.ValueOf(source).Elem())
				if !reflect.DeepEqual(got, want) {
					t.Fatal("source mutation reached retained control")
				}
			} else {
				controlCloneMutate(reflect.ValueOf(got).Elem())
				if !reflect.DeepEqual(source, controlCloneFixture()) {
					t.Fatal("control mutation reached source graph")
				}
			}
		})
	}
}

func controlCloneFixture() *Snapshot {
	data := &Snapshot{}
	controlCloneFill(reflect.ValueOf(data).Elem())
	data.LayoutVersion = hotColdLayoutVersion
	data.LedgeredOutbox["k"] = controlCostEvent(3)
	return data
}

var controlCloneTime = time.Unix(1700000000, 123).UTC()

func controlCloneFill(v reflect.Value) {
	if v.Type() == reflect.TypeOf(time.Time{}) {
		v.Set(reflect.ValueOf(controlCloneTime))
		return
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			controlCloneFill(v.Field(i))
		}
	case reflect.Map:
		v.Set(reflect.MakeMap(v.Type()))
		item := reflect.New(v.Type().Elem()).Elem()
		controlCloneFill(item)
		v.SetMapIndex(reflect.ValueOf("k"), item)
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		controlCloneFill(v.Index(0))
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		controlCloneFill(v.Elem())
	case reflect.Interface:
		v.Set(reflect.ValueOf(map[string]any{"number": json.Number("90071992547409931234567890.123456789"), "list": []any{"original"}}))
	case reflect.String:
		v.SetString("original")
	case reflect.Int, reflect.Int64:
		v.SetInt(7)
	case reflect.Bool:
		v.SetBool(true)
	default:
		panic("extend control clone fixture for " + v.Type().String())
	}
}

// Mutate through every mutable edge, not merely map entries. time.Time and
// its Location are immutable values; time pointers themselves remain owned.
func controlCloneMutate(v reflect.Value) {
	if v.Type() == reflect.TypeOf(time.Time{}) {
		if v.CanSet() {
			v.Set(reflect.ValueOf(controlCloneTime.Add(time.Hour)))
		}
		return
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			controlCloneMutate(v.Field(i))
		}
	case reflect.Map:
		iter := v.MapRange()
		for iter.Next() {
			item := reflect.New(v.Type().Elem()).Elem()
			item.Set(iter.Value())
			controlCloneMutate(item)
			v.SetMapIndex(iter.Key(), item)
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			controlCloneMutate(v.Index(i))
		}
	case reflect.Pointer, reflect.Interface:
		if !v.IsNil() {
			controlCloneMutate(v.Elem())
		}
	case reflect.String:
		if v.CanSet() {
			v.SetString("changed")
		}
	case reflect.Int, reflect.Int64:
		if v.CanSet() {
			v.SetInt(99)
		}
	case reflect.Bool:
		if v.CanSet() {
			v.SetBool(false)
		}
	}
}

func controlCloneClearEvidence(data *Snapshot) {
	data.Events = map[string]domain.Event{}
	data.Receipts = map[string]domain.EventReceipt{}
	data.Streams = map[string]StreamState{}
	data.Segments = map[string][]domain.Segment{}
	data.Checkpoints = map[string][]domain.Checkpoint{}
}

func TestControlCloneNeverTraversesDiscardedEvidence(t *testing.T) {
	data := NewSnapshot()
	data.Sources["source"] = domain.SourceSystem{ID: "source", AllowedClientIDs: []string{"client"}}
	bad := controlCostEvent(1)
	bad.Payload["unsupported"] = func() {}
	data.Events["discarded"] = bad
	data.Receipts["discarded"] = domain.EventReceipt{AcceptedAt: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}
	for _, clone := range []func(*Snapshot) (*Snapshot, error){cloneControl, controlSnapshot} {
		got, err := clone(data)
		if err != nil || got == nil || !reflect.DeepEqual(got.Sources, data.Sources) || len(got.Events) != 0 || len(got.Receipts) != 0 {
			t.Fatalf("control projection traversed or retained unrelated evidence: %v", err)
		}
		got.Sources["source"].AllowedClientIDs[0] = "changed"
		if data.Sources["source"].AllowedClientIDs[0] != "client" {
			t.Fatal("control projection retained an evidence-adjacent alias")
		}
	}
}

func TestControlCloneNilEmptyCompatibility(t *testing.T) {
	for _, empty := range []bool{false, true} {
		data := &Snapshot{}
		if empty {
			data = NewSnapshot()
			data.Sources["k"] = domain.SourceSystem{AllowedClientIDs: []string{}}
			data.Schemas["k"] = domain.EventSchema{RequiredFields: []string{}, AllowedFields: []string{}, EncryptedFields: []string{}, SearchableFields: []string{}}
			data.ExportPlans["k"] = ExportPlan{Members: []ExportPlanMember{}, Chunks: []ExportPlanChunk{}}
			data.AggregateCheckpoints = []domain.AggregateCheckpoint{{StreamRoots: []string{}, CheckpointRefs: []domain.CheckpointRef{}}}
		}
		want, err := cloneSnapshot(data) // frozen JSON compatibility reference
		if err != nil {
			t.Fatal(err)
		}
		controlCloneClearEvidence(want)
		got, err := cloneControl(data)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("nil/empty compatibility empty=%t err=%v\ngot=%+v\nwant=%+v", empty, err, got, want)
		}
	}
}

type controlCloneList []map[string]any
type controlCloneMap map[string]controlCloneList

func TestControlCloneRejectsEveryInvalidMetadataTime(t *testing.T) {
	invalid := []time.Time{
		time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.FixedZone("invalid", 24*60*60)),
	}
	for _, bad := range invalid {
		for slot := 0; ; slot++ {
			data := controlCloneFixture()
			controlCloneClearEvidence(data)
			data.LedgeredOutbox = nil // dynamic events retain CloneEvent's contract
			remaining := slot
			if !controlCloneReplaceNthTime(reflect.ValueOf(data).Elem(), &remaining, bad) {
				if slot < 20 {
					t.Fatal("metadata time coverage unexpectedly shrank")
				}
				break
			}
			_, referenceErr := cloneSnapshot(data)
			got, err := cloneControl(data)
			var marshalErr *json.MarshalerError
			if referenceErr == nil || got != nil || !errors.As(err, &marshalErr) {
				t.Fatalf("metadata time slot=%d no longer fails closed: reference=%v err=%v", slot, referenceErr, err)
			}
		}
	}
}

func controlCloneReplaceNthTime(v reflect.Value, remaining *int, bad time.Time) bool {
	if v.Type() == reflect.TypeOf(time.Time{}) {
		if *remaining == 0 {
			v.Set(reflect.ValueOf(bad))
			return true
		}
		*remaining--
		return false
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if controlCloneReplaceNthTime(v.Field(i), remaining, bad) {
				return true
			}
		}
	case reflect.Map:
		iter := v.MapRange()
		for iter.Next() {
			item := reflect.New(v.Type().Elem()).Elem()
			item.Set(iter.Value())
			if controlCloneReplaceNthTime(item, remaining, bad) {
				v.SetMapIndex(iter.Key(), item)
				return true
			}
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if controlCloneReplaceNthTime(v.Index(i), remaining, bad) {
				return true
			}
		}
	case reflect.Pointer:
		return !v.IsNil() && controlCloneReplaceNthTime(v.Elem(), remaining, bad)
	}
	return false
}

func TestControlCloneDynamicTypesPrecisionAndTypedNil(t *testing.T) {
	var nilMap map[string]any
	var nilList []string
	var nilPointer *int
	data := NewSnapshot()
	event := controlCostEvent(1)
	event.Payload = map[string]any{
		"exact":   json.Number("90071992547409931234567890.123456789"),
		"named":   controlCloneMap{"nested": {{"number": uint64(18446744073709551615)}}},
		"nil-map": nilMap, "nil-list": nilList, "nil-pointer": nilPointer,
		"empty-map": map[string]any{}, "empty-list": []any{},
	}
	data.LedgeredOutbox["a"] = event
	data.LedgeredOutbox["b"] = event
	got, err := cloneControl(data)
	if err != nil || !reflect.DeepEqual(got, data) {
		t.Fatalf("outbox types or number digits changed: %v", err)
	}
	got.LedgeredOutbox["a"].Payload["named"].(controlCloneMap)["nested"][0]["number"] = uint64(0)
	if !reflect.DeepEqual(got.LedgeredOutbox["b"], event) || !reflect.DeepEqual(data.LedgeredOutbox["a"], event) {
		t.Fatal("independent outbox entries shared dynamic descendants")
	}
}

func TestControlCloneFailureClosesWithoutPublishing(t *testing.T) {
	cycle := map[string]any{}
	cycle["self"] = cycle
	for name, value := range map[string]any{"cycle": cycle, "function": func() {}, "channel": make(chan int), "map-key": map[int]string{1: "unsupported"}, "uninspectable": struct{ hidden string }{hidden: "unsupported"}} {
		t.Run(name, func(t *testing.T) {
			st, err := Open("")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			before := st.split.control
			bad := controlCostEvent(1)
			bad.Payload["bad"] = value
			for _, clone := range []func(*Snapshot) (*Snapshot, error){cloneControl, controlSnapshot} {
				got, err := clone(&Snapshot{LedgeredOutbox: map[string]domain.Event{"bad": bad}})
				if got != nil || !errors.Is(err, domain.ErrEventClone) {
					t.Fatalf("clone failure published a partial graph: %v", err)
				}
			}
			err = st.UpdateControl(testCtx, func(data *Snapshot) error {
				data.LedgeredOutbox["bad"] = bad
				data.Exports["terminal"] = domain.ExportJob{Status: "running"}
				return nil
			})
			if !errors.Is(err, domain.ErrEventClone) || st.split.control != before || len(before.LedgeredOutbox) != 0 || len(before.Exports) != 0 {
				t.Fatal("failed clone changed committed control")
			}
		})
	}
}

func controlCostEvent(leaves int) domain.Event {
	event := ownershipEvent("control-cost")
	values := make([]any, leaves)
	for i := range values {
		values[i] = map[string]any{"number": json.Number("9007199254740993.123456789"), "list": []string{"original"}}
	}
	event.Payload["work"] = values
	return event
}

// Fixed call counts and no latency gates: this separates actual clone work
// from file fsync, setup, map iteration/pool variance and the cold lookup.
func TestControlCostSeparatedCloneWork(t *testing.T) {
	for _, count := range []int{1, 32} {
		data := NewSnapshot()
		data.Sources["source"] = domain.SourceSystem{AllowedClientIDs: []string{"client"}}
		data.LedgeredOutbox["outbox"] = controlCostEvent(1)
		for i := 0; i < count; i++ {
			data.Events[fmt.Sprint(i)] = controlCostEvent(2)
		}
		controlCostCloneSample(t, fmt.Sprintf("control/evidence=%d", count), func() error { _, err := cloneControl(data); return err })
		controlCostCloneSample(t, fmt.Sprintf("dynamic-events/count=%d", count), func() error { _, err := cloneEventMap(data.Events); return err })
		data.Events = nil
		data.LedgeredOutbox["outbox"] = controlCostEvent(count)
		controlCostCloneSample(t, fmt.Sprintf("control/outbox-leaves=%d", count), func() error { _, err := cloneControl(data); return err })
	}
}

func controlCostCloneSample(t *testing.T, label string, fn func() error) {
	t.Helper()
	allocs := testing.AllocsPerRun(3, func() {
		if err := fn(); err != nil {
			t.Fatal(err)
		}
	})
	const calls = 64
	start := time.Now()
	for i := 0; i < calls; i++ {
		if err := fn(); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("work=%s allocs/call=%.0f duration/call=%s timed_calls=%d allocation_calls=4", label, allocs, time.Since(start)/calls, calls)
}

func controlCostLedger(count int) *TenantLedger {
	records := make([]LedgerRecord, count)
	for i := range records {
		receipt := domain.EventReceipt{TenantID: "tenant-a", EventID: fmt.Sprintf("historical-%05d", i), Status: domain.StatusArchived, IdempotencyKey: fmt.Sprintf("idem-%d", i)}
		records[i] = LedgerRecord{TenantID: "tenant-a", RecordType: LedgerReceipt, Key: EventKey("tenant-a", receipt.EventID), Version: 1, Receipt: &receipt}
	}
	return newTenantLedger("tenant-a", records)
}

func TestControlCostLedgerHistoryLookupWork(t *testing.T) {
	for _, count := range []int{1, 10000} {
		ledger := controlCostLedger(count)
		view := newTenantLedgerView(ledger)
		for _, hit := range []bool{true, false} {
			key := EventKey("tenant-a", "missing")
			if hit {
				key = ledger.records[count-1].Key
			}
			controlCostCloneSample(t, fmt.Sprintf("committed-lookup/history=%d/hit=%t/reference-scan-visits=%d", count, hit, count), func() error {
				record, found, err := view.findCommittedRecord(LedgerReceipt, key, 1)
				if err != nil || found != hit || (found && record.Key != key) {
					return fmt.Errorf("lookup changed result: found=%t err=%v", found, err)
				}
				return nil
			})
		}
	}
}

func TestControlCostLedgerDamagedHintsCannotReplaceCommittedHistory(t *testing.T) {
	for _, damage := range []string{"missing", "nil", "negative", "out-of-range", "other-record"} {
		t.Run(damage, func(t *testing.T) {
			ledger := controlCostLedger(3)
			target := ledger.records[2]
			identity := ledgerRecordIdentity(target)
			switch damage {
			case "missing":
				delete(ledger.recordPositions, identity)
			case "nil":
				ledger.recordPositions = nil
			case "negative":
				ledger.recordPositions[identity] = -1
			case "out-of-range":
				ledger.recordPositions[identity] = len(ledger.records)
			case "other-record":
				ledger.recordPositions[identity] = 0
			}
			view := newTenantLedgerView(ledger)
			got, found, err := view.findCommittedRecord(target.RecordType, target.Key, target.Version)
			if err != nil || !found || !reflect.DeepEqual(got, target) {
				t.Fatal("damaged hint hid or substituted committed evidence")
			}
			changed := cloneLedgerRecord(target)
			changed.Receipt.Hash = "conflicting-history"
			if _, err := ledger.recordsToAppend([]LedgerRecord{changed}); !errors.Is(err, ErrLedgerRecordConflict) {
				t.Fatal("damaged hint hid an immutable committed conflict")
			}
			if records, err := ledger.recordsToAppend([]LedgerRecord{target}); err != nil || len(records) != 0 {
				t.Fatal("damaged hint duplicated an exact committed retry")
			}
		})
	}
	ledger := controlCostLedger(3)
	target := ledger.records[2]
	// A forged positive hint must not invent a new version or another tenant.
	ledger.recordPositions[ledgerRecordIdentity(LedgerRecord{RecordType: target.RecordType, Key: target.Key, Version: 2})] = 2
	ledger.recordPositions[ledgerRecordIdentity(LedgerRecord{RecordType: target.RecordType, Key: EventKey("tenant-b", "historical-00002"), Version: 1})] = 2
	for _, query := range []LedgerRecord{{RecordType: target.RecordType, Key: target.Key, Version: 2}, {RecordType: target.RecordType, Key: EventKey("tenant-b", "historical-00002"), Version: 1}} {
		if _, found, err := ledger.findCommittedRecord(query.RecordType, query.Key, query.Version); err != nil || found {
			t.Fatal("forged hint became ledger truth")
		}
	}
}

func TestControlCostLedgerPendingCommittedVersionAndConflict(t *testing.T) {
	ledger := controlCostLedger(3)
	view := newTenantLedgerView(ledger)
	receipt := *ledger.records[0].Receipt
	receipt.Status = domain.StatusIndexed
	if err := view.SetReceipt(receipt); err != nil {
		t.Fatal(err)
	}
	key := EventKey("tenant-a", receipt.EventID)
	if got, ok := view.Receipt(key); !ok || got.Status != domain.StatusArchived {
		t.Fatal("pending receipt became committed before append")
	}
	pending := view.pendingRecords()
	if len(pending) != 1 || pending[0].Version != 2 {
		t.Fatal("changed receipt lost its next immutable version")
	}
	if _, ok, err := view.findCommittedRecord(LedgerReceipt, key, 2); err != nil || ok {
		t.Fatal("pending record became committed history")
	}
	if _, ok, err := view.findRecord(LedgerReceipt, key, 2); err != nil || !ok {
		t.Fatal("pending record disappeared from mutation conflict check")
	}
	if got, err := ledger.recordsToAppend(pending); err != nil || len(got) != 1 {
		t.Fatal("new receipt version rejected")
	}
	ledger.commitPending(pending)
	if got, err := ledger.recordsToAppend(pending); err != nil || len(got) != 0 {
		t.Fatal("exact committed retry appended twice")
	}
	conflict := cloneLedgerRecords(pending)
	conflict[0].Receipt.Hash = "different"
	if _, err := ledger.recordsToAppend(conflict); !errors.Is(err, ErrLedgerRecordConflict) {
		t.Fatalf("immutable conflict hidden: %v", err)
	}
	other := newTenantLedgerView(controlCostLedger(1))
	if err := other.SetReceipt(domain.EventReceipt{TenantID: "tenant-b", EventID: "wrong"}); err == nil {
		t.Fatal("lookup optimization weakened tenant ownership")
	}
}
