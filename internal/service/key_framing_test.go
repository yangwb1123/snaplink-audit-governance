package service

import (
	"errors"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// unparsableKeyCount walks every composite-key map of the snapshot and
// counts keys that store.SplitTenantKey rejects (zero or multiple
// separators). After the key-framing boundary hardening, API-created data
// must never produce such keys: they are invisible to VerifyIntegrity and
// aggregate checkpointing by construction.
func unparsableKeyCount(data *store.Snapshot) int {
	bad := 0
	for key := range data.Events {
		if _, _, ok := store.SplitTenantKey(key); !ok {
			bad++
		}
	}
	for key := range data.Receipts {
		if _, _, ok := store.SplitTenantKey(key); !ok {
			bad++
		}
	}
	for key := range data.Streams {
		if _, _, ok := store.SplitTenantKey(key); !ok {
			bad++
		}
	}
	for key := range data.Segments {
		if _, _, ok := store.SplitTenantKey(key); !ok {
			bad++
		}
	}
	for key := range data.Checkpoints {
		if _, _, ok := store.SplitTenantKey(key); !ok {
			bad++
		}
	}
	for key := range data.Sources {
		if _, _, ok := store.SplitTenantKey(key); !ok {
			bad++
		}
	}
	// Schemas are intentionally excluded: SchemaKey has three components
	// (tenant + schema_id + version) and is always looked up by full key,
	// never split — two separators are the normal, well-formed shape.
	return bad
}

// TestIngestRejectsKeyFramingStreamComponents is AC-5a: Ingest rejects 0x1F
// (and the other rejected classes) in each of the five fields that become
// composite-key components, before any store mutation. F-7: the three
// optional stream components are validated when non-empty regardless of
// which branch Event.Stream would take.
func TestIngestRejectsKeyFramingStreamComponents(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	for _, tc := range []struct {
		name   string
		mutate func(*domain.Event)
	}{
		{"event_id", func(e *domain.Event) { e.EventID = "a\x1fb" }},
		{"source_system", func(e *domain.Event) { e.SourceSystem = "x\x1fy" }},
		{"source_system space", func(e *domain.Event) { e.SourceSystem = "x y" }},
		{"aggregate_type alone", func(e *domain.Event) { e.AggregateType = "a\x1fb"; e.AggregateID = "" }},
		{"aggregate_id", func(e *domain.Event) { e.AggregateID = "a\x1fb" }},
		{"operation_id", func(e *domain.Event) { e.OperationID = "a\x1fb" }},
		{"operation_id space (source-branch stream)", func(e *domain.Event) { e.OperationID = "op ok"; e.AggregateType = ""; e.AggregateID = "" }},
	} {
		event := testEvent("keyf-"+tc.name, "op-1", at)
		tc.mutate(&event)
		if _, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered); !errors.Is(err, domain.ErrInvalid) {
			t.Errorf("%s: Ingest = %v, want ErrInvalid", tc.name, err)
		}
	}

	// Nothing was written by the rejected ingests, and a valid ingest after
	// them still produces only parseable keys (the leak vector "0x1F source
	// + event pair" is dead: AddSource rejects the source id first).
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		if bad := unparsableKeyCount(data); bad != 0 {
			t.Fatalf("rejected ingests wrote %d unparsable keys", bad)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"keyf-valid-a", "keyf-valid-b", "keyf-valid-c"} {
		event := testEvent(id, "op-ok", at.Add(time.Duration(i)*time.Second))
		if _, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatalf("valid ingest %d failed: %v", i, err)
		}
	}
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		if bad := unparsableKeyCount(data); bad != 0 {
			t.Fatalf("valid ingests produced %d unparsable keys", bad)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestAddSourceUpdateSourceRegisterSchemaRejectKeyFraming is AC-2 at the
// service boundary: AddSource (body id), UpdateSource (path id) and
// RegisterSchema (schema_id) reject key-framing IDs with ErrInvalid before
// any snapshot mutation, and rejections are side-effect free (F-6: no admin
// action appended).
func TestAddSourceUpdateSourceRegisterSchemaRejectKeyFraming(t *testing.T) {
	svc := testService(t, false)
	counts := func() (sources, schemas, actions int) {
		t.Helper()
		if err := svc.Store.Read(func(data *store.Snapshot) error {
			sources = len(data.Sources)
			schemas = len(data.Schemas)
			actions = len(data.AdminActions)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return sources, schemas, actions
	}
	beforeSources, beforeSchemas, beforeActions := counts()

	for _, id := range []string{"x\x1fy", "a b", "a/b", "\x00-nul"} {
		err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: id, Name: "X", Active: true})
		if !errors.Is(err, domain.ErrInvalid) {
			t.Errorf("AddSource(id=%q) = %v, want ErrInvalid", id, err)
		}
	}
	// UpdateSource: the path id lands in SourceKey via normalizeSource.
	if _, err := svc.UpdateSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "a\x1fb", Name: "X", Active: true}); !errors.Is(err, domain.ErrInvalid) {
		t.Errorf("UpdateSource(id=0x1F) = %v, want ErrInvalid", err)
	}
	// The valid update path still works (tenant-existence check must not
	// break legitimate updates).
	updated, err := svc.UpdateSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM v2", Active: true})
	if err != nil || updated.Name != "CRM v2" {
		t.Fatalf("valid UpdateSource failed: %+v %v", updated, err)
	}
	for _, id := range []string{"a\x1fb", "a b", "a/b"} {
		err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: id, Version: 1, EventType: "x", Active: true})
		if !errors.Is(err, domain.ErrInvalid) {
			t.Errorf("RegisterSchema(schema_id=%q) = %v, want ErrInvalid", id, err)
		}
	}
	afterSources, afterSchemas, afterActions := counts()
	if afterSources != beforeSources+0 || afterSchemas != beforeSchemas+0 {
		t.Fatalf("rejected writes mutated the snapshot: sources %d->%d schemas %d->%d",
			beforeSources, afterSources, beforeSchemas, afterSchemas)
	}
	// F-6: the valid update appended exactly one admin action; none of the
	// rejected calls did.
	if afterActions != beforeActions+1 {
		t.Fatalf("admin actions %d->%d, want exactly +1 (valid update only)", beforeActions, afterActions)
	}
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		if bad := unparsableKeyCount(data); bad != 0 {
			t.Fatalf("rejected source/schema writes produced %d unparsable keys", bad)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestIntegrityCoversEverySealedStream is AC-5b: with SegmentSize 2, ingests
// across all three stream derivation branches (source, operation,
// aggregate) seal segments whose keys all parse (i) — the fail-closed
// `!ok` skip paths in VerifyIntegrity and CreateAggregateCheckpoint become
// unreachable from API-created data; (ii) VerifyIntegrity counts every
// stored segment; (iii) the aggregate checkpoint covers every stream.
func TestIntegrityCoversEverySealedStream(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	// Stream 1 (source branch): no aggregate, no operation.
	sourceEvent := testEvent("cover-src-1", "", at)
	sourceEvent.OperationID = ""
	sourceEvent.AggregateType = ""
	sourceEvent.AggregateID = ""
	// Stream 2 (operation branch): operation id, no aggregate.
	opEvent := testEvent("cover-op-1", "op-stream-1", at.Add(time.Second))
	opEvent.AggregateType = ""
	opEvent.AggregateID = ""
	// Stream 3 (aggregate branch): aggregate fields set.
	aggEvent := testEvent("cover-agg-1", "op-x", at.Add(2*time.Second))
	aggEvent.AggregateType = "invoice"
	aggEvent.AggregateID = "inv-9"
	// Two events per stream seal one segment each (SegmentSize 2).
	second := func(base domain.Event, id string, offset time.Duration) domain.Event {
		dup := base
		dup.EventID = id
		dup.IdempotencyKey = "idem-" + id
		dup.OccurredAt = at.Add(offset)
		return dup
	}
	for _, event := range []domain.Event{
		sourceEvent, second(sourceEvent, "cover-src-2", 3*time.Second),
		opEvent, second(opEvent, "cover-op-2", 4*time.Second),
		aggEvent, second(aggEvent, "cover-agg-2", 5*time.Second),
	} {
		if _, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatalf("ingest %s failed: %v", event.EventID, err)
		}
	}

	// (i) every segment/checkpoint key parses.
	var segmentTotal, checkpointTotal int
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		if bad := unparsableKeyCount(data); bad != 0 {
			t.Fatalf("%d unparsable keys after sealed ingests", bad)
		}
		for key := range data.Segments {
			if tenantID, _, ok := store.SplitTenantKey(key); ok && tenantID == "tenant-a" {
				segmentTotal += len(data.Segments[key])
			}
		}
		for key := range data.Checkpoints {
			if tenantID, _, ok := store.SplitTenantKey(key); ok && tenantID == "tenant-a" {
				checkpointTotal += len(data.Checkpoints[key])
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if segmentTotal != 3 || checkpointTotal != 3 {
		t.Fatalf("sealed totals: segments=%d checkpoints=%d, want 3/3", segmentTotal, checkpointTotal)
	}

	// (ii) VerifyIntegrity's SegmentCount equals the stored total — the
	// fail-closed skip would undercount it.
	result, err := svc.VerifyIntegrity("tenant-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Valid {
		t.Fatalf("integrity invalid: %+v", result)
	}
	if result.SegmentCount != segmentTotal {
		t.Fatalf("VerifyIntegrity.SegmentCount = %d, want stored total %d", result.SegmentCount, segmentTotal)
	}

	// (iii) the aggregate checkpoint covers exactly the three sealed streams.
	if err := svc.CreateAggregateCheckpoint("tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		items := data.AggregateCheckpoints
		if len(items) == 0 {
			t.Fatal("no aggregate checkpoint was created")
		}
		if items[len(items)-1].StreamCount != 3 {
			t.Fatalf("aggregate StreamCount = %d, want 3 distinct streams", items[len(items)-1].StreamCount)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
