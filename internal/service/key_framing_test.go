package service

import (
	"errors"
	"strings"
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
		{"aggregate_type colon (collision pair a)", func(e *domain.Event) { e.AggregateType = "a:b"; e.AggregateID = "c" }},
		{"aggregate_id colon (collision pair b)", func(e *domain.Event) { e.AggregateType = "a"; e.AggregateID = "b:c" }},
		{"operation_id", func(e *domain.Event) { e.OperationID = "a\x1fb" }},
		{"operation_id space (source-branch stream)", func(e *domain.Event) { e.OperationID = "op ok"; e.AggregateType = ""; e.AggregateID = "" }},
	} {
		event := testEvent("keyf-"+tc.name, "op-1", at)
		tc.mutate(&event)
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); !errors.Is(err, domain.ErrInvalid) {
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
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
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

// TestIngestRejectsColonStreamComponentsMutationFree is REQ-4's mutation-free
// guarantee for the colon class specifically: ingesting the collision pair
// (AggregateType="a:b", AggregateID="c") — and its mirror — fails with
// domain.ErrInvalid before any snapshot mutation: no new Events, Receipts,
// Streams, Segments or Checkpoints entries, no admin action, and a valid
// ingest after the rejections still succeeds (store not wedged).
func TestIngestRejectsColonStreamComponentsMutationFree(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	counts := func() (events, receipts, streams, segments, checkpoints, actions int) {
		t.Helper()
		if err := svc.Store.Read(func(data *store.Snapshot) error {
			events = len(data.Events)
			receipts = len(data.Receipts)
			streams = len(data.Streams)
			segments = len(data.Segments)
			checkpoints = len(data.Checkpoints)
			actions = len(data.AdminActions)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return events, receipts, streams, segments, checkpoints, actions
	}
	type countsValue struct {
		events, receipts, streams, segments, checkpoints, actions int
	}
	snapshotCounts := func() countsValue {
		e, r, st, se, c, a := counts()
		return countsValue{e, r, st, se, c, a}
	}
	before := snapshotCounts()

	for _, tc := range []struct {
		name   string
		mutate func(*domain.Event)
	}{
		{"aggregate_type colon", func(e *domain.Event) { e.AggregateType = "a:b"; e.AggregateID = "c" }},
		{"aggregate_id colon (mirror)", func(e *domain.Event) { e.AggregateType = "a"; e.AggregateID = "b:c" }},
	} {
		event := testEvent("keyf-colon-"+tc.name, "op-1", at)
		tc.mutate(&event)
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); !errors.Is(err, domain.ErrInvalid) {
			t.Errorf("%s: Ingest = %v, want ErrInvalid", tc.name, err)
		}
	}
	after := snapshotCounts()
	if after != before {
		t.Fatalf("rejected colon ingests mutated the snapshot: before %+v after %+v", before, after)
	}

	// A valid ingest after the rejections succeeds and produces only
	// parseable keys (the store is not wedged).
	event := testEvent("keyf-colon-valid", "op-ok", at)
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatalf("valid ingest after colon rejections failed: %v", err)
	}
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		if bad := unparsableKeyCount(data); bad != 0 {
			t.Fatalf("valid ingest produced %d unparsable keys", bad)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestStoredStreamFramingByteIdentical is REQ-5's service regression: the
// framing string itself is untouched, so the three no-colon branch fixtures
// store byte-identical pre-change stream IDs, VerifyIntegrity stays Valid
// with the same SegmentCount, and the unparsable-key audit stays at zero.
func TestStoredStreamFramingByteIdentical(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	// Aggregate branch: the stream key must be the pre-change constant.
	aggEvent := testEvent("frame-agg-1", "op-x", at)
	aggEvent.AggregateType = "invoice"
	aggEvent.AggregateID = "inv-1"
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, aggEvent, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	// Operation branch.
	opEvent := testEvent("frame-op-1", "op-9", at.Add(time.Second))
	opEvent.AggregateType = ""
	opEvent.AggregateID = ""
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, opEvent, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	// Source branch.
	srcEvent := testEvent("frame-src-1", "", at.Add(2*time.Second))
	srcEvent.OperationID = ""
	srcEvent.AggregateType = ""
	srcEvent.AggregateID = ""
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, srcEvent, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}

	if err := svc.Store.Read(func(data *store.Snapshot) error {
		for eventID, want := range map[string]string{
			"frame-agg-1": "tenant-a:aggregate:invoice:inv-1",
			"frame-op-1":  "tenant-a:operation:op-9",
			"frame-src-1": "tenant-a:source:crm",
		} {
			if got := data.Events[store.EventKey("tenant-a", eventID)].StreamID; got != want {
				t.Errorf("stored StreamID for %s = %q, want the pre-change constant %q", eventID, got, want)
			}
		}
		if bad := unparsableKeyCount(data); bad != 0 {
			t.Fatalf("%d unparsable keys after framing regression ingests", bad)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// VerifyIntegrity re-verifies the sealed chain as one consistent chain
	// with the same segment count as stored.
	result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Valid {
		t.Fatalf("integrity invalid after framing regression: %+v", result)
	}
	var storedSegments int
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		for key := range data.Segments {
			if tenantID, _, ok := store.SplitTenantKey(key); ok && tenantID == "tenant-a" {
				storedSegments += len(data.Segments[key])
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if result.SegmentCount != storedSegments {
		t.Fatalf("VerifyIntegrity.SegmentCount = %d, want stored total %d", result.SegmentCount, storedSegments)
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

// TestSourceRegistrationLengthCapBoundary is the FM-1 boundary-rejection pin
// for source registration: source.ID becomes the source branch of
// Event.Stream() (an archive key component, percent-encoded 3×), so a
// registration longer than domain.MaxArchiveComponentBytes (85 bytes = POSIX
// NAME_MAX ÷ 3) is rejected at AddSource and UpdateSource with ErrInvalid and
// no snapshot mutation — the same cap ValidateBasic applies to the event's
// source_system, keeping the registration surface consistent with the ingest
// surface. Exactly 85 bytes is accepted end to end (a source is created and
// an event on it archives).
func TestSourceRegistrationLengthCapBoundary(t *testing.T) {
	svc := testService(t, true)
	punct85 := strings.Repeat("?", domain.MaxArchiveComponentBytes)
	punct86 := strings.Repeat("?", domain.MaxArchiveComponentBytes+1)
	runes28 := strings.Repeat("界", 28)
	runes29 := strings.Repeat("界", 29)

	for _, id := range []string{punct86, runes29} {
		if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: id, Name: "X", Active: true}); !errors.Is(err, domain.ErrInvalid) {
			t.Errorf("AddSource(id=%d bytes) = %v, want ErrInvalid", len(id), err)
		}
		if _, err := svc.UpdateSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: id, Name: "X", Active: true}); !errors.Is(err, domain.ErrInvalid) {
			t.Errorf("UpdateSource(id=%d bytes) = %v, want ErrInvalid", len(id), err)
		}
	}
	// Rejections are side-effect free: no new source registered (testService
	// pre-registers "crm", so compare against that baseline), no admin
	// action appended.
	sourcesBefore, actionsBefore := func() (int, int) {
		var sources, actions int
		if err := svc.Store.Read(func(data *store.Snapshot) error {
			sources = len(data.Sources)
			actions = len(data.AdminActions)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return sources, actions
	}()
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		if len(data.Sources) != sourcesBefore {
			t.Fatalf("over-cap source registrations mutated the snapshot: %d → %d sources", sourcesBefore, len(data.Sources))
		}
		if len(data.AdminActions) != actionsBefore {
			t.Fatalf("over-cap source registrations appended admin actions: %d → %d", actionsBefore, len(data.AdminActions))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Boundary: exactly 85 bytes register fine (the registration cap is the
	// raw-component bound). A safe-alphabet 85-byte source archives end to
	// end (identity encoding ⇒ the whole encoded stream component fits
	// NAME_MAX); the punctuation source, whose 3× encoded component plus the
	// tenant:source: prefix exceeds NAME_MAX, is caught by the archive
	// pre-flight and dead-lettered — the layered defense pinned by
	// TestArchiveStoreBoundaryCatchesCapBoundaryEvent.
	for _, id := range []string{punct85, runes28} {
		if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: id, Name: "X", Active: true}); err != nil {
			t.Errorf("AddSource(id=%d bytes) = %v, want nil", len(id), err)
		}
	}
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: strings.Repeat("a", domain.MaxArchiveComponentBytes), Name: "X-safe", Active: true, AllowedClientIDs: []string{"crm"}}); err != nil {
		t.Fatalf("AddSource(85 safe bytes) = %v, want nil", err)
	}
	at := time.Unix(1_700_000_010, 0).UTC()
	event := testEvent("evt-src-boundary", "", at) // no aggregate/operation ⇒ source branch
	event.AggregateType = ""
	event.AggregateID = ""
	event.SourceSystem = strings.Repeat("a", domain.MaxArchiveComponentBytes)
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusArchived)
	if err != nil {
		t.Fatalf("ingest on 85-byte source: %v", err)
	}
	if receipt.Status != domain.StatusArchived {
		t.Fatalf("ingest on 85-byte source status=%s, want StatusArchived", receipt.Status)
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
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
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
	result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "", "")
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
	if err := svc.CreateAggregateCheckpoint(testCtx, "tenant-a"); err != nil {
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
