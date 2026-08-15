package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

var crmPrincipal = domain.IngestPrincipal{ClientID: "crm"}

// recordingArchive is a Store stub that records Put keys; fail makes it
// return an error so ingest degrades to StatusIndexed and ArchivePending
// can retry later.
type recordingArchive struct {
	fail bool
	puts []string
}

func (r *recordingArchive) Put(_ context.Context, key string, _ []byte) error {
	r.puts = append(r.puts, key)
	if r.fail {
		return errors.New("archive unavailable")
	}
	return nil
}

func (r *recordingArchive) Get(context.Context, string) ([]byte, error) { return nil, nil }

func (r *recordingArchive) Ready(context.Context) error { return nil }

func testService(t *testing.T, archive bool) *Service {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	archiveDir := ""
	if archive {
		archiveDir = filepath.Join(dir, "archive")
	}
	svc, err := New(st, Config{ArchiveDir: archiveDir, SegmentSize: 2, SigningSecret: "test-secret", Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true, EventsPerSecond: 1000, Burst: 1000}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value"}}); err != nil {
		t.Fatal(err)
	}
	return svc
}

func testEvent(id, operation string, at time.Time) domain.Event {
	return domain.Event{EventID: id, SourceSystem: "crm", EventType: "audit.event", SchemaID: "audit.event", SchemaVersion: 1, OccurredAt: at, OperationID: operation, AggregateType: "invoice", AggregateID: "inv-1", AggregateVersion: 1, Actor: domain.Actor{ID: "user-1", Roles: []string{"operator"}}, Action: "update", Outcome: "success", DataClassification: "internal", RetentionClass: "standard", IdempotencyKey: "idem-" + id, Payload: map[string]any{"resource": "invoice", "value": 10}, ChangedFields: map[string]domain.FieldChange{"status": {Before: "open", After: "paid"}}}
}

// seedChronologyFixture ingests four events across two aggregate streams with
// interleaved occurred_at (A1@t0, B1@t0+3s, A2@t0+1s, B2@t0+4s). Per-stream
// sequences make the old (Sequence, EventID) order A1,B1,A2,B2 while the
// chronological order is A1,A2,B1,B2, so tests can distinguish the sort key.
func seedChronologyFixture(t *testing.T, svc *Service, t0 time.Time) {
	t.Helper()
	ingest := func(id, aggregate string, at time.Time, version int64) {
		event := testEvent(id, "op-"+aggregate, at)
		event.AggregateID = aggregate
		event.AggregateVersion = version
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatal(err)
		}
	}
	ingest("A1", "agg-1", t0, 1)
	ingest("B1", "agg-2", t0.Add(3*time.Second), 1)
	ingest("A2", "agg-1", t0.Add(time.Second), 2)
	ingest("B2", "agg-2", t0.Add(4*time.Second), 2)
}

// eventIDs extracts the event identifiers of a result slice in order.
func eventIDs(events []domain.Event) []string {
	ids := make([]string, 0, len(events))
	for _, event := range events {
		ids = append(ids, event.EventID)
	}
	return ids
}

func TestIngestIdempotencyConflictAndIntegrity(t *testing.T) {
	svc := testService(t, true)
	at := time.Unix(1_700_000_010, 0).UTC()
	first, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("evt-1", "op-1", at), domain.StatusLedgered)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != domain.StatusArchived || first.Sequence != 1 {
		t.Fatalf("unexpected first receipt: %+v", first)
	}
	duplicate, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("evt-1", "op-1", at), domain.StatusLedgered)
	if err != nil || !duplicate.Duplicate {
		t.Fatalf("duplicate not detected: %+v %v", duplicate, err)
	}
	conflicting := testEvent("evt-1", "op-1", at)
	conflicting.Payload["value"] = 11
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, conflicting, domain.StatusLedgered); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	second := testEvent("evt-2", "op-1", at.Add(time.Second))
	second.AggregateVersion = 2
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, second, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Valid || result.EventCount != 2 || result.SegmentCount != 1 {
		t.Fatalf("integrity failed: %+v", result)
	}
	archiveFile := filepath.Join(svc.Config.ArchiveDir, "events", "tenant-a", "tenant-a_aggregate_invoice_inv-1", "00000000000000000001-evt-1.json")
	if _, err := os.Stat(archiveFile); err != nil {
		t.Fatalf("archive event missing: %v", err)
	}
}

func TestIdempotencyKeyCannotBeReusedAcrossEvents(t *testing.T) {
	svc := testService(t, false)
	firstEvent := testEvent("idem-event-1", "idem-op", time.Now().UTC())
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, firstEvent, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	secondEvent := testEvent("idem-event-2", "idem-op", time.Now().UTC())
	secondEvent.IdempotencyKey = firstEvent.IdempotencyKey
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, secondEvent, domain.StatusLedgered)
	if !errors.Is(err, domain.ErrConflict) || !receipt.Conflict || receipt.ErrorCode != "idempotency_key_conflict" {
		t.Fatalf("expected idempotency conflict: %+v %v", receipt, err)
	}
}

func TestIngestBindsSourceToAuthenticatedClient(t *testing.T) {
	svc := testService(t, false)
	event := testEvent("source-bound", "source-op", time.Unix(1_700_000_010, 0).UTC())
	_, wrongErr := svc.Ingest(testCtx, "tenant-a", domain.IngestPrincipal{ClientID: "other-client"}, event, domain.StatusLedgered)
	if !errors.Is(wrongErr, domain.ErrForbidden) {
		t.Fatalf("cross-source client must be forbidden: %v", wrongErr)
	}
	_, missingErr := svc.Ingest(testCtx, "tenant-a", domain.IngestPrincipal{}, event, domain.StatusLedgered)
	if !errors.Is(missingErr, domain.ErrForbidden) || missingErr.Error() != wrongErr.Error() {
		t.Fatalf("missing client identity must fail closed: wrong=%v missing=%v", wrongErr, missingErr)
	}
	unknown := event
	unknown.SourceSystem = "unknown"
	_, unknownErr := svc.Ingest(testCtx, "tenant-a", domain.IngestPrincipal{ClientID: "other-client"}, unknown, domain.StatusLedgered)
	if !errors.Is(unknownErr, domain.ErrForbidden) || unknownErr.Error() != wrongErr.Error() {
		t.Fatalf("unknown source must be indistinguishable: wrong=%v unknown=%v", wrongErr, unknownErr)
	}
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatalf("source ID default binding rejected: %v", err)
	}
}

func TestUpdateSourceReplacesClientAllowList(t *testing.T) {
	svc := testService(t, false)
	updated, err := svc.UpdateSource("test", domain.SourceSystem{
		TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true,
		AllowedClientIDs: []string{"relay-b", "relay-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(updated.AllowedClientIDs) != 2 || updated.AllowedClientIDs[0] != "relay-a" {
		t.Fatalf("allow-list was not normalized: %+v", updated)
	}
	event := testEvent("source-updated", "source-op", time.Unix(1_700_000_010, 0).UTC())
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("old default binding must be replaced: %v", err)
	}
	if _, err := svc.Ingest(testCtx, "tenant-a", domain.IngestPrincipal{ClientID: "relay-a"}, event, domain.StatusLedgered); err != nil {
		t.Fatalf("explicitly allowed client rejected: %v", err)
	}
}

func TestIngestDerivesTenantFromUniqueServerSideSourceBinding(t *testing.T) {
	svc := testService(t, false)
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-b", Name: "Tenant B", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("test", domain.SourceSystem{
		TenantID: "tenant-b", ID: "crm", Name: "CRM B", Active: true,
		AllowedClientIDs: []string{"tenant-b-relay"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-b", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
		t.Fatal(err)
	}
	event := testEvent("derived-tenant", "derive-op", time.Unix(1_700_000_010, 0).UTC())
	// DS-08: a non-empty envelope tenant must match the server-resolved
	// tenant; a mismatched envelope is rejected (422) instead of silently
	// re-labelled, so the writer cannot attribute events to tenant-b through
	// a tenant-a token even when tenant-b owns a same-named source.
	event.TenantID = "tenant-b"
	if _, err := svc.Ingest(testCtx, "", crmPrincipal, event, domain.StatusLedgered); !errors.Is(err, domain.ErrTenantMismatch) {
		t.Fatalf("mismatched envelope tenant err=%v, want tenant mismatch", err)
	}
	// Empty envelope tenant is derived from the server-side binding.
	event.TenantID = ""
	receipt, err := svc.Ingest(testCtx, "", crmPrincipal, event, domain.StatusLedgered)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.TenantID != "tenant-a" {
		t.Fatalf("server binding was not used: %+v", receipt)
	}
	crossTenant := testEvent("cross-tenant", "derive-op", time.Unix(1_700_000_011, 0).UTC())
	if _, err := svc.Ingest(testCtx, "tenant-b", crmPrincipal, crossTenant, domain.StatusLedgered); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("signed tenant hint escaped source binding: %v", err)
	}
	if _, err := svc.UpdateSource("test", domain.SourceSystem{
		TenantID: "tenant-b", ID: "crm", Name: "CRM B", Active: true,
		AllowedClientIDs: []string{"crm", "tenant-b-relay"},
	}); err != nil {
		t.Fatal(err)
	}
	ambiguous := testEvent("ambiguous-tenant", "derive-op", time.Unix(1_700_000_012, 0).UTC())
	if _, err := svc.Ingest(testCtx, "", crmPrincipal, ambiguous, domain.StatusLedgered); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("ambiguous server-side tenant binding must fail closed: %v", err)
	}
}

func TestQueryReplayAndExport(t *testing.T) {
	svc := testService(t, true)
	at := time.Unix(1_700_000_100, 0).UTC()
	for i := 0; i < 3; i++ {
		event := testEvent("evt-q-"+string(rune('1'+i)), "op-query", at.Add(time.Duration(i)*time.Second))
		event.AggregateVersion = int64(i + 1)
		event.ChangedFields = map[string]domain.FieldChange{"status": {After: []any{"open", "paid", "closed"}[i]}}
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatal(err)
		}
	}
	query := domain.Query{From: at.Add(-time.Second), To: at.Add(10 * time.Second), PageSize: 2}
	page, err := svc.QueryEvents("tenant-a", "test", query)
	if err != nil || len(page.Items) != 2 || page.NextCursor == "" {
		t.Fatalf("unexpected first page: %+v %v", page, err)
	}
	query.Cursor = page.NextCursor
	page2, err := svc.QueryEvents("tenant-a", "test", query)
	if err != nil || len(page2.Items) != 1 {
		t.Fatalf("unexpected second page: %+v %v", page2, err)
	}
	replay, err := svc.ReplayOperation("tenant-a", "", "op-query")
	if err != nil {
		t.Fatal(err)
	}
	if replay.State["status"] != "closed" {
		t.Fatalf("unexpected replay state: %+v", replay.State)
	}
	job, err := svc.CreateExport("tenant-a", "user-1", domain.Query{From: at.Add(-time.Second), To: at.Add(10 * time.Second), PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job, err = svc.GetExport("tenant-a", job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status == "completed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if job.Status != "completed" || job.EventCount != 3 {
		t.Fatalf("export did not complete: %+v", job)
	}
}

// TestCreateExportRequiresTenantScope pins the fail-closed empty-tenant
// decision (ADR-0009 item 4): a platform token without tenant_id reaches
// CreateExport with the empty sentinel, and an empty-scope export selects
// zero events by construction (exact TenantID match in runExport). Reject
// before any read fact, job, self-audit record, or worker goroutine exists.
func TestCreateExportRequiresTenantScope(t *testing.T) {
	svc := testService(t, false)
	query := domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_010, 0).UTC()}
	if _, err := svc.CreateExport("", "platform-admin", query); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("CreateExport(\"\") err=%v, want domain.ErrInvalid", err)
	}
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		if len(data.Exports) != 0 {
			t.Fatalf("empty-scope export persisted %d job(s), want 0", len(data.Exports))
		}
		for _, action := range data.AdminActions {
			if action.TenantID == "" {
				t.Fatalf("empty-scope export appended self-audit record %s", action.Action)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestQueryEventsChronologicalAcrossStreams is AC-1: a tenant-wide query must
// order events chronologically across streams (occurred_at, sequence,
// event_id), not by the per-stream sequence coordinates.
func TestQueryEventsChronologicalAcrossStreams(t *testing.T) {
	svc := testService(t, true)
	t0 := time.Unix(1_700_000_200, 0).UTC()
	seedChronologyFixture(t, svc, t0)
	query := domain.Query{From: t0.Add(-time.Second), To: t0.Add(5 * time.Second)}
	result, err := svc.QueryEvents("tenant-a", "test", query)
	if err != nil {
		t.Fatal(err)
	}
	if result.Count != 4 {
		t.Fatalf("Count=%d, want 4", result.Count)
	}
	want := []string{"A1", "A2", "B1", "B2"}
	if got := eventIDs(result.Items); !reflect.DeepEqual(got, want) {
		t.Fatalf("order=%v, want %v", got, want)
	}
	for i := 0; i+1 < len(result.Items); i++ {
		if !result.Items[i].OccurredAt.Before(result.Items[i+1].OccurredAt) {
			t.Fatalf("items not strictly increasing by occurred_at: %v", eventIDs(result.Items))
		}
	}
	// Positive control: the same query restricted to one stream keeps the
	// chronological order.
	filtered, err := svc.QueryEvents("tenant-a", "test", domain.Query{From: query.From, To: query.To, StreamID: "tenant-a:aggregate:invoice:agg-1"})
	if err != nil {
		t.Fatal(err)
	}
	if filtered.Count != 2 || !reflect.DeepEqual(eventIDs(filtered.Items), []string{"A1", "A2"}) {
		t.Fatalf("stream-filtered result=%+v", filtered)
	}
}

// TestQueryEventsCursorReproducesSet is AC-2: a PageSize walk driven by
// next_cursor must reproduce the unpaginated result exactly — same set, same
// order, strictly increasing pages — and legacy/garbage cursors must fail
// closed with ErrInvalid.
func TestQueryEventsCursorReproducesSet(t *testing.T) {
	svc := testService(t, true)
	t0 := time.Unix(1_700_000_200, 0).UTC()
	seedChronologyFixture(t, svc, t0)
	base := domain.Query{From: t0.Add(-time.Second), To: t0.Add(5 * time.Second)}
	reference, err := svc.QueryEvents("tenant-a", "test", domain.Query{From: base.From, To: base.To, PageSize: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if reference.NextCursor != "" {
		t.Fatalf("unpaginated query must not return next_cursor")
	}
	if reference.Count != 4 || len(reference.Items) != 4 {
		t.Fatalf("reference Count=%d len=%d, want 4/4", reference.Count, len(reference.Items))
	}
	walk := domain.Query{From: base.From, To: base.To, PageSize: 2}
	var collected []string
	for pages := 0; pages < 100; pages++ {
		page, err := svc.QueryEvents("tenant-a", "test", walk)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i+1 < len(page.Items); i++ {
			if !page.Items[i].OccurredAt.Before(page.Items[i+1].OccurredAt) {
				t.Fatalf("page %d not strictly increasing: %v", pages, eventIDs(page.Items))
			}
		}
		for _, event := range page.Items {
			collected = append(collected, event.EventID)
		}
		if page.NextCursor == "" {
			break
		}
		walk.Cursor = page.NextCursor
	}
	if len(collected) != len(reference.Items) {
		t.Fatalf("walk collected %d events, reference has %d: %v", len(collected), len(reference.Items), collected)
	}
	for i, event := range reference.Items {
		if collected[i] != event.EventID {
			t.Fatalf("walk diverged from reference at %d: %v != %v", i, collected, eventIDs(reference.Items))
		}
	}
	set := map[string]bool{}
	for _, id := range collected {
		if set[id] {
			t.Fatalf("duplicate event %s in walk", id)
		}
		set[id] = true
	}
	for _, event := range reference.Items {
		if !set[event.EventID] {
			t.Fatalf("walk dropped %s", event.EventID)
		}
	}
	// Negative control: a legacy 2-tuple cursor (the pre-chronological wire
	// format) must fail closed with ErrInvalid and an explicit message.
	legacyCursor := domain.EncodeCursor(domain.Cursor{Sequence: 1, EventID: "evt-x", Legacy: true})
	bad := walk
	bad.Cursor = legacyCursor
	if _, err := svc.QueryEvents("tenant-a", "test", bad); !errors.Is(err, domain.ErrInvalid) || !strings.Contains(err.Error(), "predates chronological ordering") {
		t.Fatalf("legacy cursor must fail closed, got %v", err)
	}
	garbage := walk
	garbage.Cursor = "not-a-cursor"
	if _, err := svc.QueryEvents("tenant-a", "test", garbage); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("garbage cursor must fail closed, got %v", err)
	}
}

// TestExportMatchesQueryEventsOrder is AC-3: a compliance export must agree
// with the chronological QueryEvents order — page 1 of a paginated query is a
// strict prefix of the export, and the export set equals the unpaginated API
// result.
func TestExportMatchesQueryEventsOrder(t *testing.T) {
	svc := testService(t, true)
	t0 := time.Unix(1_700_000_200, 0).UTC()
	seedChronologyFixture(t, svc, t0)
	query := domain.Query{From: t0.Add(-time.Second), To: t0.Add(5 * time.Second), PageSize: 3}
	page1, err := svc.QueryEvents("tenant-a", "test", query)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Items) != 3 || page1.NextCursor == "" {
		t.Fatalf("page1=%+v", page1)
	}
	job, err := svc.CreateExport("tenant-a", "user-1", query)
	if err != nil {
		t.Fatal(err)
	}
	job = waitExportCompleted(t, svc, job.ID)
	if job.Status != "completed" || job.EventCount != 4 {
		t.Fatalf("export did not complete: %+v", job)
	}
	sealed, err := svc.Config.Archive.Get(context.Background(), job.ObjectPath)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := security.DecryptExport(sealed, svc.Config.EncryptionKey, security.ExportBinding(job.TenantID, job.ID))
	if err != nil {
		t.Fatal(err)
	}
	reference, err := svc.QueryEvents("tenant-a", "test", domain.Query{From: query.From, To: query.To, PageSize: 1000})
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(plain), []byte{'\n'})
	if len(lines) != len(reference.Items) {
		t.Fatalf("export has %d lines, API has %d events", len(lines), len(reference.Items))
	}
	exported := make([]domain.Event, 0, len(lines))
	for _, line := range lines {
		var event domain.Event
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("export line is not a decodable event: %v", err)
		}
		exported = append(exported, event)
	}
	// Page 1 (PageSize 3) is a strict prefix of the export.
	for i, event := range page1.Items {
		if exported[i].EventID != event.EventID {
			t.Fatalf("export[%d]=%s, want page1[%d]=%s", i, exported[i].EventID, i, event.EventID)
		}
	}
	// Set equality with the unpaginated API result.
	set := map[string]bool{}
	for _, event := range exported {
		set[event.EventID] = true
	}
	for _, event := range reference.Items {
		if !set[event.EventID] {
			t.Fatalf("export missing %s", event.EventID)
		}
	}
	// The full export line order is strictly increasing by occurred_at.
	for i := 0; i+1 < len(exported); i++ {
		if !exported[i].OccurredAt.Before(exported[i+1].OccurredAt) {
			t.Fatalf("export not strictly increasing at line %d", i)
		}
	}
}

func TestSensitivePayloadRejected(t *testing.T) {
	svc := testService(t, false)
	event := testEvent("evt-secret", "op-secret", time.Now().UTC())
	event.Payload["password"] = "do-not-store"
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, ""); err == nil || !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("expected sensitive payload rejection, got %v", err)
	}
}

func TestRetentionReportRespectsLegalHold(t *testing.T) {
	svc := testService(t, false)
	if err := svc.SetRetentionPolicy("test", domain.RetentionPolicy{TenantID: "tenant-a", ArchiveDays: 1, RetentionClass: "standard"}); err != nil {
		t.Fatal(err)
	}
	event := testEvent("evt-old", "op-old", time.Now().Add(-72*time.Hour).UTC())
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	securityEvent := testEvent("evt-security", "op-security", time.Now().Add(-72*time.Hour).UTC())
	securityEvent.RetentionClass = "security"
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, securityEvent, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	hold, err := svc.CreateLegalHold(domain.LegalHold{TenantID: "tenant-a", Name: "case-1", Reason: "investigation"})
	if err != nil {
		t.Fatal(err)
	}
	report, err := svc.EvaluateRetention("tenant-a", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if report.EligibleEvents != 0 || report.ProtectedEvents != 1 || len(report.HoldIDs) != 1 || report.HoldIDs[0] != hold.ID {
		t.Fatalf("unexpected retention report: %+v", report)
	}
}

func TestRetentionPolicyRequiresAClass(t *testing.T) {
	svc := testService(t, false)
	err := svc.SetRetentionPolicy("test", domain.RetentionPolicy{TenantID: "tenant-a", ArchiveDays: 30})
	if !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("empty retention class error=%v", err)
	}
}

func TestStateSurvivesStoreReopen(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := New(st, Config{ArchiveDir: filepath.Join(dir, "archive"), Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }, AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("persisted", "op-persisted", time.Unix(1_700_000_010, 0).UTC()), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	// The file backend holds a process-lifetime flock on <path>.lock: the
	// reopen must wait for the first store to Close (a real restart
	// releases the flock via process death).
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	reopenedService, err := New(reopened, Config{ArchiveDir: filepath.Join(dir, "archive"), AllowDevSecrets: true})
	if err != nil {
		t.Fatal(err)
	}
	event, err := reopenedService.GetEvent("tenant-a", "test", "persisted")
	if err != nil || event.Hash == "" {
		t.Fatalf("persisted event missing: %+v %v", event, err)
	}
}

func TestSchemaCompatibilityRejectsRemovedRequiredField(t *testing.T) {
	svc := testService(t, false)
	err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 2, EventType: "audit.event", Active: true, RequiredFields: []string{"different"}})
	if err == nil || !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("expected incompatible schema rejection, got %v", err)
	}
}

func TestSensitiveFieldsAreEncryptedWithoutBreakingIdempotency(t *testing.T) {
	svc := testService(t, false)
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 2, EventType: "audit.event", Active: true, RequiredFields: []string{"resource"}, AllowedFields: []string{"resource", "value", "email"}, EncryptedFields: []string{"email"}, SearchableFields: []string{"email"}}); err != nil {
		t.Fatal(err)
	}
	event := testEvent("evt-encrypted", "op-encrypted", time.Now().UTC())
	event.SchemaVersion = 2
	event.Payload["email"] = "alice@example.test"
	first, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := svc.GetEvent("tenant-a", "test", "evt-encrypted")
	if err != nil {
		t.Fatal(err)
	}
	value, _ := stored.Payload["email"].(string)
	if len(value) < len("enc:v1:") || value[:len("enc:v1:")] != "enc:v1:" {
		t.Fatalf("email was not encrypted: %v", stored.Payload["email"])
	}
	if _, ok := stored.Payload["email__search_digest"]; !ok {
		t.Fatalf("search digest missing: %+v", stored.Payload)
	}
	expectedDigest, err := security.SearchDigestBound("alice@example.test", svc.Config.EncryptionKey, "tenant-a", "email")
	if err != nil || stored.Payload["email__search_digest"] != expectedDigest {
		t.Fatalf("search digest must use original value and tenant/field binding: got=%v want=%v err=%v", stored.Payload["email__search_digest"], expectedDigest, err)
	}
	duplicate, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err != nil || !duplicate.Duplicate || first.Hash != duplicate.Hash {
		t.Fatalf("encrypted duplicate failed: %+v %v", duplicate, err)
	}
}

func TestIntegrityChecksStreamsIndependently(t *testing.T) {
	svc := testService(t, false)
	base := time.Unix(1_700_000_000, 0).UTC()
	first := testEvent("evt-a-1", "", base.Add(10*time.Second))
	second := testEvent("evt-b-1", "", base)
	second.AggregateID = "inv-2" // derived stream: tenant-a:aggregate:invoice:inv-2
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, first, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, second, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil || !result.Valid {
		t.Fatalf("independent streams should validate: %+v %v", result, err)
	}
}

// TestIngestStripsClientStreamID is AC-1: a body-supplied stream_id is
// stripped on ingest, so the stored event, receipt and archive object carry
// the server-derived stream (tenant + aggregate/operation/source) and never
// the client value. Regression property: against pre-fix code, the stored
// stream equals the crafted value and this test fails.
func TestIngestStripsClientStreamID(t *testing.T) {
	cases := []struct {
		name     string
		crafted  string
		mutate   func(*domain.Event)
		expected string
	}{
		{name: "tenant-mimicking", crafted: "tenant-b:aggregate:invoice:inv-1", expected: "tenant-a:aggregate:invoice:inv-1"},
		{name: "arbitrary", crafted: "attacker-stream", expected: "tenant-a:aggregate:invoice:inv-1"},
		{name: "operation-derived", crafted: "attacker-stream", expected: "tenant-a:operation:op-9", mutate: func(e *domain.Event) {
			e.OperationID = "op-9"
			e.AggregateType = ""
			e.AggregateID = ""
		}},
		{name: "source-derived", crafted: "attacker-stream", expected: "tenant-a:source:crm", mutate: func(e *domain.Event) {
			e.OperationID = ""
			e.AggregateType = ""
			e.AggregateID = ""
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := testService(t, true)
			base := time.Unix(1_700_000_000, 0).UTC()
			e := testEvent("evt-1", "", base)
			e.OperationID = ""
			if tc.mutate != nil {
				tc.mutate(&e)
			}
			e.StreamID = tc.crafted
			receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, e, domain.StatusLedgered)
			if err != nil {
				t.Fatal(err)
			}
			if receipt.Duplicate || receipt.Status != domain.StatusArchived {
				t.Fatalf("unexpected receipt: %+v", receipt)
			}
			if receipt.StreamID != tc.expected {
				t.Fatalf("receipt stream=%q want %q", receipt.StreamID, tc.expected)
			}
			stored, err := svc.GetEvent("tenant-a", "crm", "evt-1")
			if err != nil {
				t.Fatal(err)
			}
			if stored.StreamID != tc.expected || stored.Sequence != 1 || stored.PrevHash != "" {
				t.Fatalf("stored stream=%q seq=%d prev=%q want stream=%q seq=1 prev=\"\"", stored.StreamID, stored.Sequence, stored.PrevHash, tc.expected)
			}
			// Ingest strips its own copy only; the caller's event is untouched.
			if e.StreamID != tc.crafted {
				t.Fatalf("caller event mutated: %q", e.StreamID)
			}
			craftedBytes := []byte(tc.crafted)
			if canonical, err := json.Marshal(stored); err != nil {
				t.Fatal(err)
			} else if bytes.Contains(canonical, craftedBytes) {
				t.Fatalf("crafted stream value in stored canonical JSON: %s", canonical)
			}
			if canonical, err := json.Marshal(receipt); err != nil {
				t.Fatal(err)
			} else if bytes.Contains(canonical, craftedBytes) {
				t.Fatalf("crafted stream value in receipt JSON: %s", canonical)
			}
			archiveFile := filepath.Join(svc.Config.ArchiveDir, "events", "tenant-a", safeName(tc.expected), "00000000000000000001-evt-1.json")
			data, err := os.ReadFile(archiveFile)
			if err != nil {
				t.Fatalf("archive object missing at %s: %v", archiveFile, err)
			}
			var archived domain.Event
			if err := json.Unmarshal(data, &archived); err != nil {
				t.Fatal(err)
			}
			if archived.StreamID != tc.expected {
				t.Fatalf("archived stream=%q want %q", archived.StreamID, tc.expected)
			}
			if bytes.Contains(data, craftedBytes) {
				t.Fatalf("crafted stream value in archive object: %s", data)
			}
		})
	}
}

// TestIngestCollapsesCraftedStreamIDs is AC-2: N events with N distinct
// crafted stream_ids collapse into exactly 1 stream (1 stream state, 1
// segment chain, 1 archive directory) instead of minting N streams.
// Regression property: against pre-fix code, the snapshot holds N stream
// keys and this test fails.
func TestIngestCollapsesCraftedStreamIDs(t *testing.T) {
	svc := testService(t, true)
	base := time.Unix(1_700_000_000, 0).UTC()
	const n = 20
	const derived = "tenant-a:source:crm"
	for i := 0; i < n; i++ {
		e := testEvent(fmt.Sprintf("evt-%d", i), "", base.Add(time.Duration(i)*time.Second))
		e.OperationID = ""
		e.AggregateType = ""
		e.AggregateID = ""
		e.StreamID = fmt.Sprintf("crafted-%d", i)
		if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, e, domain.StatusLedgered); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Streams) != 1 {
		t.Fatalf("crafted stream_ids minted %d streams, want 1", len(snap.Streams))
	}
	key := store.StreamKey("tenant-a", derived)
	stream, ok := snap.Streams[key]
	if !ok {
		t.Fatalf("sole stream key %q missing; got %v", key, snapStreamKeys(snap.Streams))
	}
	if stream.NextSequence != n+1 {
		t.Fatalf("NextSequence=%d want %d", stream.NextSequence, n+1)
	}
	var prevHash string
	for i := 0; i < n; i++ {
		stored, err := svc.GetEvent("tenant-a", "crm", fmt.Sprintf("evt-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if stored.StreamID != derived || stored.Sequence != int64(i+1) || stored.PrevHash != prevHash {
			t.Fatalf("event %d stream=%q seq=%d prev=%q want stream=%q seq=%d prev=%q", i, stored.StreamID, stored.Sequence, stored.PrevHash, derived, i+1, prevHash)
		}
		prevHash = stored.Hash
	}
	if len(snap.Segments) != 1 {
		t.Fatalf("segments keyed by %d streams, want 1", len(snap.Segments))
	}
	for _, segment := range snap.Segments[key] {
		if segment.StreamID != derived {
			t.Fatalf("sealed segment stream=%q want %q", segment.StreamID, derived)
		}
	}
	if got := len(snap.Segments[key]); got != n/2 {
		t.Fatalf("sealed segment count=%d want %d (SegmentSize=2 over %d events on the single stream)", got, n/2, n)
	}
	dirs, err := os.ReadDir(filepath.Join(svc.Config.ArchiveDir, "events", "tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 1 || dirs[0].Name() != safeName(derived) {
		t.Fatalf("archive dirs=%v want exactly one %s", dirs, safeName(derived))
	}
	result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil || !result.Valid || result.EventCount != n {
		t.Fatalf("collapsed-stream integrity failed: %+v %v", result, err)
	}
}

func snapStreamKeys(streams map[string]store.StreamState) []string {
	keys := make([]string, 0, len(streams))
	for key := range streams {
		keys = append(keys, key)
	}
	return keys
}

// TestIngestDuplicateWithCraftedStreamID is L5: idempotency is keyed on
// event_id + content digest, which excludes stream_id (A1), so re-sending
// the same content with a different crafted stream_id is a duplicate and
// never mints a second stream.
func TestIngestDuplicateWithCraftedStreamID(t *testing.T) {
	svc := testService(t, false)
	base := time.Unix(1_700_000_000, 0).UTC()
	first := testEvent("evt-dup", "", base)
	first.StreamID = "crafted-a"
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, first, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	again := testEvent("evt-dup", "", base)
	again.StreamID = "crafted-b"
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, again, domain.StatusLedgered)
	if err != nil || !receipt.Duplicate {
		t.Fatalf("expected duplicate: %+v %v", receipt, err)
	}
	if receipt.StreamID != "tenant-a:aggregate:invoice:inv-1" {
		t.Fatalf("receipt stream=%q want derived tenant-a:aggregate:invoice:inv-1", receipt.StreamID)
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Streams) != 1 {
		t.Fatalf("duplicate re-send minted %d streams, want 1", len(snap.Streams))
	}
}

// TestIngestTenantMismatchPrecedesStreamStrip is M3: a mismatched envelope
// tenant_id combined with a crafted stream_id is rejected with
// ErrTenantMismatch before the strip/derivation, and no stream state is
// created even transiently.
func TestIngestTenantMismatchPrecedesStreamStrip(t *testing.T) {
	svc := testService(t, false)
	base := time.Unix(1_700_000_000, 0).UTC()
	e := testEvent("evt-x", "", base)
	e.TenantID = "tenant-b"
	e.StreamID = "crafted-foreign"
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, e, domain.StatusLedgered); !errors.Is(err, domain.ErrTenantMismatch) {
		t.Fatalf("expected ErrTenantMismatch, got %v", err)
	}
	snap, err := svc.Store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Streams) != 0 || len(snap.Events) != 0 {
		t.Fatalf("rejected ingest created state: streams=%d events=%d", len(snap.Streams), len(snap.Events))
	}
}

func TestArchivePendingRetriesIndexedEvents(t *testing.T) {
	svc := testService(t, false)
	event := testEvent("pending-archive", "op-pending", time.Now().UTC())
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	// Wire the archive store itself (not the ArchiveDir string, which
	// nothing re-derives into Config.Archive): the ingest gate above ran
	// against the default unconfigured FileStore, so the receipt is
	// StatusIndexed and ArchivePending must retry it.
	svc.Config.Archive = &archive.FileStore{Dir: filepath.Join(t.TempDir(), "archive")}
	count, err := svc.ArchivePending("tenant-a")
	if err != nil || count != 1 {
		t.Fatalf("pending archive failed: count=%d err=%v", count, err)
	}
	receipt, err := svc.GetReceipt("tenant-a", "", event.EventID)
	if err != nil || receipt.Status != domain.StatusArchived {
		t.Fatalf("receipt was not archived: %+v %v", receipt, err)
	}
}

// TestIngestArchiveMismatchStaysIndexedAndRetries is AC-2: when the archive
// destination already holds a different object at the exact key the service
// computes (service.go:966), Put must fail content verification, the receipt
// must degrade to StatusIndexed instead of StatusArchived, the tampered
// object must be preserved (WORM), and ArchivePending must converge the
// receipt once the key is free.
func TestIngestArchiveMismatchStaysIndexedAndRetries(t *testing.T) {
	svc := testService(t, true)
	event := testEvent("evt-mismatch", "op-mismatch", time.Now().UTC())
	// Seed a tampered object at the exact archive path archiveEvent computes
	// for the first event of the stream (sequence 1, same layout as the
	// pinned path in TestIngestIdempotencyConflictAndIntegrity).
	archivePath := filepath.Join(svc.Config.ArchiveDir, "events", "tenant-a", "tenant-a_aggregate_invoice_inv-1", fmt.Sprintf("%020d-%s.json", 1, event.EventID))
	if err := os.MkdirAll(filepath.Dir(archivePath), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(archivePath, []byte(`tampered`), 0o440); err != nil {
		t.Fatal(err)
	}

	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Sequence != 1 {
		t.Fatalf("unexpected sequence %d, pinned archive path assumed 1", receipt.Sequence)
	}
	if receipt.Status != domain.StatusIndexed {
		t.Fatalf("receipt status = %s, want StatusIndexed: a mismatched archive object must not be reported archived", receipt.Status)
	}
	// WORM: the tampered object was neither overwritten nor removed.
	got, err := os.ReadFile(archivePath)
	if err != nil || string(got) != `tampered` {
		t.Fatalf("tampered archive object not preserved: %v %q", err, got)
	}

	// Once the key is free, ArchivePending retries the non-Archived receipt
	// and converges it to StatusArchived with the canonical object.
	if err := os.Remove(archivePath); err != nil {
		t.Fatal(err)
	}
	count, err := svc.ArchivePending("tenant-a")
	if err != nil || count != 1 {
		t.Fatalf("pending archive failed: count=%d err=%v", count, err)
	}
	receipt, err = svc.GetReceipt("tenant-a", "", event.EventID)
	if err != nil || receipt.Status != domain.StatusArchived {
		t.Fatalf("receipt was not archived after retry: %+v %v", receipt, err)
	}
}

// TestIngestArchivesWithS3OnlyConfig is AC-1: with an S3-equivalent store
// injected and no ArchiveDir, ingest must still archive. Pre-fix the gate
// keyed on the empty ArchiveDir string skipped archiving entirely and the
// receipt stalled at StatusIndexed.
func TestIngestArchivesWithS3OnlyConfig(t *testing.T) {
	svc := testService(t, false)
	archiveStub := &recordingArchive{}
	svc.Config.Archive = archiveStub
	event := testEvent("s3-only", "op-s3-only", time.Now().UTC())
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Status != domain.StatusArchived {
		t.Fatalf("S3-only receipt not archived: %+v", receipt)
	}
	// SegmentSize is 2 and a single event seals no segments: exactly one
	// object (the event) must have been written.
	if len(archiveStub.puts) != 1 {
		t.Fatalf("expected exactly one archive Put, got %d: %v", len(archiveStub.puts), archiveStub.puts)
	}
	if !strings.HasPrefix(archiveStub.puts[0], "events/tenant-a/") {
		t.Fatalf("unexpected archive key: %s", archiveStub.puts[0])
	}
}

// TestArchivePendingWithS3OnlyConfig is AC-2: an S3-only deployment where
// archiving failed at ingest time (receipt stuck at StatusIndexed) must be
// retried by ArchivePending instead of rejected as unconfigured. Pre-fix the
// empty ArchiveDir string made ArchivePending return ErrInvalid and the
// receipt stalled forever.
func TestArchivePendingWithS3OnlyConfig(t *testing.T) {
	svc := testService(t, false)
	archiveStub := &recordingArchive{fail: true}
	svc.Config.Archive = archiveStub
	event := testEvent("s3-only-retry", "op-s3-only", time.Now().UTC())
	receipt, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Status != domain.StatusIndexed {
		t.Fatalf("ingest with failing archive must stay indexed: %+v", receipt)
	}
	archiveStub.fail = false
	count, err := svc.ArchivePending("tenant-a")
	if err != nil || count != 1 {
		t.Fatalf("pending archive failed: count=%d err=%v", count, err)
	}
	receipt, err = svc.GetReceipt("tenant-a", "", event.EventID)
	if err != nil || receipt.Status != domain.StatusArchived {
		t.Fatalf("receipt was not archived: %+v %v", receipt, err)
	}
}

func TestRestoreApprovalWorkflow(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("evt-restore", "op-restore", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	run, err := svc.CreateRestore("tenant-a", domain.RestoreRequest{OperationID: "op-restore", Reason: "user error"}, "requester-1")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != domain.RestoreStatusPendingApproval || run.CreatedBy != "requester-1" {
		t.Fatalf("unexpected created run: %+v", run)
	}

	// 分离职责：创建者不能审批或拒绝自己的请求。
	if _, err := svc.ApproveRestore("tenant-a", run.ID, "requester-1"); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("same actor approve err=%v, want forbidden", err)
	}
	if _, err := svc.RejectRestore("tenant-a", run.ID, "requester-1"); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("same actor reject err=%v, want forbidden", err)
	}

	approved, err := svc.ApproveRestore("tenant-a", run.ID, "approver-1")
	if err != nil {
		t.Fatal(err)
	}
	if approved.Status != domain.RestoreStatusApproved || approved.ApprovedBy != "approver-1" || approved.ApprovedAt == nil {
		t.Fatalf("unexpected approval: %+v", approved)
	}

	// 已批准后重复审批或拒绝都是状态冲突。
	if _, err := svc.ApproveRestore("tenant-a", run.ID, "approver-2"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("double approve err=%v, want conflict", err)
	}
	if _, err := svc.RejectRestore("tenant-a", run.ID, "approver-2"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("reject after approve err=%v, want conflict", err)
	}
	// 已决 run 上创建者审批 → 409 优先于 403（Conflict 支配 Forbidden）。
	if _, err := svc.ApproveRestore("tenant-a", run.ID, "requester-1"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("same actor approve on decided run err=%v, want conflict", err)
	}

	run2, err := svc.CreateRestore("tenant-a", domain.RestoreRequest{OperationID: "op-restore", Reason: "duplicate entry"}, "requester-2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RejectRestore("tenant-a", run2.ID, "requester-2"); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("same actor reject err=%v, want forbidden", err)
	}
	rejected, err := svc.RejectRestore("tenant-a", run2.ID, "approver-3")
	if err != nil {
		t.Fatal(err)
	}
	if rejected.Status != domain.RestoreStatusRejected || rejected.RejectedBy != "approver-3" || rejected.RejectedAt == nil {
		t.Fatalf("unexpected rejection: %+v", rejected)
	}

	// 租户边界与缺失目标都关闭为 not found；跨租户的创建者审批同样
	// 被 404 掩盖（NotFound 优先于 Forbidden）。
	if _, err := svc.ApproveRestore("tenant-b", run.ID, "approver-1"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("cross tenant approve err=%v, want not found", err)
	}
	if _, err := svc.ApproveRestore("tenant-b", run.ID, "requester-1"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("cross tenant same actor approve err=%v, want not found", err)
	}
	if _, err := svc.ApproveRestore("tenant-a", "restore-nope", "approver-1"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing run err=%v, want not found", err)
	}
}

func TestRestoreApprovalSeparationOfDutiesRefusalIsAtomic(t *testing.T) {
	// S13: a refused same-actor decision writes nothing — no state change,
	// no admin action — and a distinct actor can still decide afterwards.
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("evt-sod", "op-sod", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	run, err := svc.CreateRestore("tenant-a", domain.RestoreRequest{OperationID: "op-sod", Reason: "rollback"}, "requester-1")
	if err != nil {
		t.Fatal(err)
	}
	before, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ApproveRestore("tenant-a", run.ID, "requester-1"); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("same actor approve err=%v, want forbidden", err)
	}
	if _, err := svc.RejectRestore("tenant-a", run.ID, "requester-1"); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("same actor reject err=%v, want forbidden", err)
	}
	after, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("refused decision wrote admin action: %d → %d", len(before), len(after))
	}
	approved, err := svc.ApproveRestore("tenant-a", run.ID, "approver-1")
	if err != nil {
		t.Fatalf("distinct actor approve after refusal failed: %v", err)
	}
	if approved.Status != domain.RestoreStatusApproved || approved.ApprovedBy != "approver-1" {
		t.Fatalf("unexpected approval after refusal: %+v", approved)
	}
}

func TestRestoreApprovalConcurrentDistinctActors(t *testing.T) {
	// S11: concurrent decisions from two distinct actors serialize inside
	// Store.Update — exactly one wins, the loser sees ErrConflict.
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("evt-race", "op-race", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	run, err := svc.CreateRestore("tenant-a", domain.RestoreRequest{OperationID: "op-race", Reason: "rollback"}, "requester-1")
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	decide := func(actor string) {
		<-start
		_, err := svc.ApproveRestore("tenant-a", run.ID, actor)
		errs <- err
	}
	go decide("approver-1")
	go decide("approver-2")
	close(start)
	var approved, conflicted int
	for i := 0; i < 2; i++ {
		switch err := <-errs; {
		case err == nil:
			approved++
		case errors.Is(err, domain.ErrConflict):
			conflicted++
		default:
			t.Fatalf("unexpected race error: %v", err)
		}
	}
	if approved != 1 || conflicted != 1 {
		t.Fatalf("race outcome approved=%d conflicted=%d, want 1/1", approved, conflicted)
	}
}

func TestRestoreApprovalConcurrentSameActor(t *testing.T) {
	// S12: concurrent same-actor decisions all refuse atomically; the run
	// stays pending and a distinct actor can still decide afterwards.
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("evt-race2", "op-race2", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	run, err := svc.CreateRestore("tenant-a", domain.RestoreRequest{OperationID: "op-race2", Reason: "rollback"}, "requester-1")
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() {
			<-start
			_, err := svc.ApproveRestore("tenant-a", run.ID, "requester-1")
			errs <- err
		}()
	}
	close(start)
	for i := 0; i < 4; i++ {
		if err := <-errs; !errors.Is(err, domain.ErrForbidden) {
			t.Fatalf("same-actor race error = %v, want forbidden", err)
		}
	}
	if _, err := svc.ApproveRestore("tenant-a", run.ID, "approver-1"); err != nil {
		t.Fatalf("distinct actor after same-actor race failed: %v", err)
	}
}

func TestQueryCorrelationDimensions(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	first := testEvent("corr-evt-1", "op-corr", at)
	first.CorrelationID = "corr-outer"
	first.TraceID = "trace-1"
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, first, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	second := testEvent("corr-evt-2", "op-corr", at.Add(time.Second))
	second.CausationID = "corr-evt-1"
	second.CorrelationID = "corr-outer"
	second.TraceID = "trace-1"
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, second, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	unrelated := testEvent("corr-evt-3", "op-other", at.Add(2*time.Second))
	unrelated.CorrelationID = "corr-other"
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, unrelated, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}

	base := domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_100, 0).UTC(), PageSize: 100}
	byCorrelation, err := svc.QueryEvents("tenant-a", "test", domain.Query{From: base.From, To: base.To, CorrelationID: "corr-outer", PageSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	if byCorrelation.Count != 2 {
		t.Fatalf("correlation query count=%d, want 2", byCorrelation.Count)
	}
	byCausation, err := svc.QueryEvents("tenant-a", "test", domain.Query{From: base.From, To: base.To, CausationID: "corr-evt-1", PageSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	if byCausation.Count != 1 || byCausation.Items[0].EventID != "corr-evt-2" {
		t.Fatalf("causation query result=%+v", byCausation)
	}
}

func TestAdminActionSelfAudit(t *testing.T) {
	svc := testService(t, false)
	actions, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	// testService 已产生 tenant.created / source.created / schema.created。
	if len(actions) != 3 {
		t.Fatalf("initial admin actions=%d, want 3", len(actions))
	}

	if err := svc.SetRetentionPolicy("admin-1", domain.RetentionPolicy{TenantID: "tenant-a", HotDays: 30, WarmDays: 90, ArchiveDays: 365, RetentionClass: "standard"}); err != nil {
		t.Fatal(err)
	}
	hold, err := svc.CreateLegalHold(domain.LegalHold{TenantID: "tenant-a", Name: "case-1", Reason: "litigation", CreatedBy: "compliance-1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ReleaseLegalHold("tenant-a", hold.ID, "compliance-2"); err != nil {
		t.Fatal(err)
	}
	query := domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_100, 0).UTC()}
	exportJob, err := svc.CreateExport("tenant-a", "compliance-3", query)
	if err != nil {
		t.Fatal(err)
	}
	// runExport 是异步 goroutine：等待任务收敛（否则其写入与 TempDir 清理
	// 竞争，race 模式下可见）。
	waitExport(t, svc, exportJob.ID)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("audit-evt", "op-audit", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	run, err := svc.CreateRestore("tenant-a", domain.RestoreRequest{OperationID: "op-audit", Reason: "rollback"}, "requester-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ApproveRestore("tenant-a", run.ID, "approver-1"); err != nil {
		t.Fatal(err)
	}

	actions, err = svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	wanted := []string{
		domain.AdminActionRestoreApproved, domain.AdminActionRestoreCreated,
		domain.AdminActionExportCreated, domain.AdminActionLegalHoldReleased,
		domain.AdminActionLegalHoldCreated, domain.AdminActionRetentionPolicySet,
	}
	got := map[string]bool{}
	for _, action := range actions {
		got[action.Action] = true
	}
	for _, action := range wanted {
		if !got[action] {
			t.Fatalf("missing admin action %s in %v", action, actions)
		}
	}

	// 租户边界：tenant-b 看不到 tenant-a 的动作。
	other, err := svc.ListAdminActions("tenant-b", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(other) != 0 {
		t.Fatalf("cross tenant actions leaked: %d", len(other))
	}
	// 平台视角可见全部（含 audit 事件写入不产生管理动作，Ingest 无自审计）。
	platform, err := svc.ListAdminActions("", true, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(platform) < len(actions) {
		t.Fatalf("platform actions=%d < tenant actions=%d", len(platform), len(actions))
	}
}

func TestSourceAccessFailClosedSameError(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	withSource := func(source string) domain.Event {
		event := testEvent("enum-"+source, "op-enum", at)
		event.SourceSystem = source
		event.IdempotencyKey = "idem-" + source
		return event
	}
	// 停用来源：先创建再禁用（AddSource 强制启用，UpdateSource 可停用）。
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "disabled", Name: "Disabled", Active: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpdateSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "disabled", Name: "Disabled", Active: false}); err != nil {
		t.Fatal(err)
	}

	// 未知来源、停用来源、越权来源（client_id 不在白名单）必须返回完全相同的
	// 拒绝结果，防止来源枚举。
	_, errUnknown := svc.Ingest(testCtx, "tenant-a", crmPrincipal, withSource("ghost"), "")
	_, errDisabled := svc.Ingest(testCtx, "tenant-a", crmPrincipal, withSource("disabled"), "")
	_, errForbidden := svc.Ingest(testCtx, "tenant-a", domain.IngestPrincipal{ClientID: "other-client"}, withSource("crm"), "")
	for name, err := range map[string]error{"unknown": errUnknown, "disabled": errDisabled, "forbidden": errForbidden} {
		if err == nil {
			t.Fatalf("%s source accepted", name)
		}
		if !errors.Is(err, domain.ErrForbidden) {
			t.Fatalf("%s source error=%v, want forbidden", name, err)
		}
	}
	if errUnknown.Error() != errDisabled.Error() || errUnknown.Error() != errForbidden.Error() {
		t.Fatalf("source errors must be identical to prevent enumeration: %q vs %q vs %q", errUnknown, errDisabled, errForbidden)
	}
}

func TestQueryStreamIDFilter(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	first := testEvent("stream-1", "op-stream", at)
	first.AggregateType = ""
	first.AggregateID = ""
	first.OperationID = ""
	first.SourceSystem = "crm" // stream: tenant-a:source:crm
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, first, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	second := testEvent("stream-2", "op-other", at.Add(time.Second))
	second.AggregateType = "invoice"
	second.AggregateID = "inv-9" // stream: tenant-a:aggregate:invoice:inv-9
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, second, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	base := domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_100, 0).UTC(), PageSize: 100}
	filtered, err := svc.QueryEvents("tenant-a", "test", domain.Query{From: base.From, To: base.To, StreamID: "tenant-a:aggregate:invoice:inv-9", PageSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	if filtered.Count != 1 || filtered.Items[0].EventID != "stream-2" {
		t.Fatalf("stream filter result=%+v", filtered)
	}
}

func TestExportEncryptedAndDecryptable(t *testing.T) {
	svc := testService(t, true)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("exp-enc-1", "op-enc", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	query := domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_100, 0).UTC()}
	job, err := svc.CreateExport("tenant-a", "compliance-1", query)
	if err != nil {
		t.Fatal(err)
	}
	// 等待异步导出完成。
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, getErr := svc.GetExport("tenant-a", job.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if current.Status == "completed" || current.Status == "failed" {
			job = current
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("export stuck in %s", current.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if job.Status != "completed" {
		t.Fatalf("export failed: %+v", job)
	}
	sealed, err := svc.Config.Archive.Get(context.Background(), job.ObjectPath)
	if err != nil {
		t.Fatal(err)
	}
	// 归档内容必须是密封的（明文 JSONL 不会出现），且新密封必须携带 v2
	// 租户/任务绑定前缀。
	if contains(sealed, []byte("exp-enc-1")) {
		t.Fatal("archive object must not contain plaintext event data")
	}
	if !strings.HasPrefix(string(sealed), "export:v2:") {
		t.Fatalf("new exports must be sealed export:v2:, got prefix %q", sealed[:16])
	}
	plain, err := security.DecryptExport(sealed, svc.Config.EncryptionKey, security.ExportBinding(job.TenantID, job.ID))
	if err != nil {
		t.Fatal(err)
	}
	if !contains(plain, []byte("exp-enc-1")) {
		t.Fatal("decrypted export missing event")
	}
	if job.Digest != domain.HashBytes(plain) {
		t.Fatal("job digest must cover the decrypted content")
	}
}

// waitExportCompleted polls an export job until it reaches a terminal state
// (deadline-polled on the real condition, never a fixed sleep).
func waitExportCompleted(t *testing.T, svc *Service, jobID string) domain.ExportJob {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, getErr := svc.GetExport("tenant-a", jobID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if current.Status == "completed" || current.Status == "failed" {
			return current
		}
		if time.Now().After(deadline) {
			t.Fatalf("export stuck in %s", current.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestVerifyExportDownloadRejectsTamperedAndSwapped is the service-layer
// matrix (AC-1/AC-2): the choke point returns plaintext only for the exact
// (job, binding, digest) triple — tampered ciphertext, a swapped v2 blob
// from another job, a swapped v1 blob with valid GCM but wrong content, a
// re-seal under the correct binding with different content, and an empty
// stored digest all return an error and no bytes.
func TestVerifyExportDownloadRejectsTamperedAndSwapped(t *testing.T) {
	svc := testService(t, true)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("verify-exp-1", "op-verify", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("verify-exp-2", "op-verify", at.Add(time.Second)), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	query := domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_100, 0).UTC()}
	jobA, err := svc.CreateExport("tenant-a", "compliance-1", query)
	if err != nil {
		t.Fatal(err)
	}
	jobB, err := svc.CreateExport("tenant-a", "compliance-1", query)
	if err != nil {
		t.Fatal(err)
	}
	jobA = waitExportCompleted(t, svc, jobA.ID)
	jobB = waitExportCompleted(t, svc, jobB.ID)
	if jobA.Status != "completed" || jobB.Status != "completed" {
		t.Fatalf("exports did not complete: A=%s B=%s", jobA.Status, jobB.Status)
	}
	sealedA, err := svc.Config.Archive.Get(context.Background(), jobA.ObjectPath)
	if err != nil {
		t.Fatal(err)
	}
	sealedB, err := svc.Config.Archive.Get(context.Background(), jobB.ObjectPath)
	if err != nil {
		t.Fatal(err)
	}

	// Happy path: the job's own blob verifies, and the returned plaintext is
	// the full decrypted slice (single full-buffer compare, AC-2 ordering).
	plainA, err := svc.VerifyExportDownload(jobA.TenantID, jobA.ID, sealedA)
	if err != nil {
		t.Fatalf("legit verify failed: %v", err)
	}
	if domain.HashBytes(plainA) != jobA.Digest {
		t.Fatal("verified plaintext must hash to the stored digest")
	}

	// Tampered ciphertext (F9): flip a byte inside the sealed body.
	tampered := append([]byte(nil), sealedA...)
	tampered[len(tampered)/2] ^= 0x01
	if plain, err := svc.VerifyExportDownload(jobA.TenantID, jobA.ID, tampered); err == nil {
		t.Fatalf("tampered blob must fail, got plaintext %q", plain)
	}

	// v2 swap (F8): job B's blob is sealed under B's binding — opening it
	// against job A must fail GCM authentication.
	if plain, err := svc.VerifyExportDownload(jobA.TenantID, jobA.ID, sealedB); err == nil {
		t.Fatalf("swapped v2 blob must fail, got plaintext %q", plain)
	}

	// v1 swap (F10): a v1 blob of different content opens with nil AAD, so
	// only the digest comparison rejects it.
	v1Swap, err := security.EncryptBytes([]byte("attacker content\n"), svc.Config.EncryptionKey)
	if err != nil {
		t.Fatal(err)
	}
	if plain, err := svc.VerifyExportDownload(jobA.TenantID, jobA.ID, v1Swap); err == nil {
		t.Fatalf("swapped v1 blob must fail, got plaintext %q", plain)
	}

	// Digest mismatch with valid GCM (F12): re-seal different content under
	// job A's own binding — authentication passes, equality fails.
	resealed, err := security.EncryptBytesBound([]byte("different content\n"), svc.Config.EncryptionKey, security.ExportBinding(jobA.TenantID, jobA.ID))
	if err != nil {
		t.Fatal(err)
	}
	if plain, err := svc.VerifyExportDownload(jobA.TenantID, jobA.ID, resealed); err == nil {
		t.Fatalf("digest mismatch must fail, got plaintext %q", plain)
	}

	// Empty/malformed stored digest (F11): fail-closed format check.
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		job := data.Exports[jobA.ID]
		job.Digest = ""
		data.Exports[jobA.ID] = job
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if plain, err := svc.VerifyExportDownload(jobA.TenantID, jobA.ID, sealedA); err == nil {
		t.Fatalf("empty stored digest must fail, got plaintext %q", plain)
	}
}

// TestVerifyExportDownloadRecordsRejectionFact pins the F-2 governance
// decision: an integrity-verification failure appends exactly one
// export.download_rejected fact for the job, while a validated download
// appends no rejection fact (the audit.event.export fact is the transport's
// job, pinned by the HTTP tests).
func TestVerifyExportDownloadRecordsRejectionFact(t *testing.T) {
	svc := testService(t, true)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, testEvent("reject-exp-1", "op-reject", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	query := domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_100, 0).UTC()}
	job, err := svc.CreateExport("tenant-a", "compliance-1", query)
	if err != nil {
		t.Fatal(err)
	}
	job = waitExportCompleted(t, svc, job.ID)
	if job.Status != "completed" {
		t.Fatalf("export failed: %+v", job)
	}
	sealed, err := svc.Config.Archive.Get(context.Background(), job.ObjectPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.VerifyExportDownload(job.TenantID, job.ID, sealed); err != nil {
		t.Fatalf("legit verify failed: %v", err)
	}
	// Tampered attempt: expect exactly one rejection fact.
	tampered := append([]byte(nil), sealed...)
	tampered[len(tampered)/2] ^= 0x01
	if _, err := svc.VerifyExportDownload(job.TenantID, job.ID, tampered); err == nil {
		t.Fatal("tampered blob must fail")
	}
	var snapshot *store.Snapshot
	if err := svc.Store.Read(func(data *store.Snapshot) error {
		snapshot = data
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	rejected := 0
	for _, action := range snapshot.AdminActions {
		if action.Action == domain.AdminActionExportRejected && action.TargetID == job.ID && action.TargetType == "export" {
			rejected++
		}
	}
	if rejected != 1 {
		t.Fatalf("expected exactly one export.download_rejected fact for the job, found %d", rejected)
	}
}

func contains(haystack, needle []byte) bool {
	return strings.Contains(string(haystack), string(needle))
}

func TestAggregateCheckpointCreatedAndVerified(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	// 三条流各写一个事件，封出段（SegmentSize=2 需要两个事件……直接塞满）。
	first := testEvent("agg-a1", "", at)
	first.AggregateType = "invoice"
	first.AggregateID = "inv-a"
	first.OperationID = ""
	first.IdempotencyKey = "agg-a1"
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, first, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	second := testEvent("agg-a2", "", at.Add(time.Second))
	second.AggregateType = "invoice"
	second.AggregateID = "inv-b"
	second.OperationID = ""
	second.IdempotencyKey = "agg-a2"
	if _, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, second, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if err := svc.SealPendingSegments(testCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateAggregateCheckpoint(testCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	result, err := svc.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil || !result.Valid {
		t.Fatalf("aggregate verify failed: %+v %v", result, err)
	}

	// 篡改聚合记录（换一个 root）必须导致验证失败。
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		items := data.AggregateCheckpoints
		items[len(items)-1].Root = "0000000000000000000000000000000000000000000000000000000000000000"
		data.AggregateCheckpoints = items
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err = svc.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid {
		t.Fatal("tampered aggregate checkpoint must fail verification")
	}
}

func TestOutOfOrderOccurredAtEvents(t *testing.T) {
	svc := testService(t, true)
	// 业务时间乱序：先写入 occurred_at 更晚的事件，再写更早的事件。
	later := testEvent("ooo-later", "op-ooo", time.Unix(1_700_000_100, 0).UTC())
	later.IdempotencyKey = "ooo-1"
	earlier := testEvent("ooo-earlier", "op-ooo", time.Unix(1_700_000_010, 0).UTC())
	earlier.IdempotencyKey = "ooo-2"

	receiptLater, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, later, domain.StatusLedgered)
	if err != nil {
		t.Fatal(err)
	}
	receiptEarlier, err := svc.Ingest(testCtx, "tenant-a", crmPrincipal, earlier, domain.StatusLedgered)
	if err != nil {
		t.Fatal(err)
	}
	// 账本序号按写入顺序（接收序），与业务时间无关。
	if receiptLater.Sequence != 1 || receiptEarlier.Sequence != 2 {
		t.Fatalf("sequence must follow write order: later=%d earlier=%d", receiptLater.Sequence, receiptEarlier.Sequence)
	}
	// 查询按业务时间排序（occurred_at, sequence, event_id）：earlier 在前，
	// 与写入序/账本序解耦。
	result, err := svc.QueryEvents("tenant-a", "test", domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_200, 0).UTC(), OperationID: "op-ooo", PageSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	if result.Count != 2 || result.Items[0].EventID != "ooo-earlier" || result.Items[1].EventID != "ooo-later" {
		t.Fatalf("event query must order chronologically: %+v", result.Items)
	}
	// 操作时间线按业务时间排序：earlier 在前（与账本序解耦）。
	timeline, err := svc.OperationTimeline("tenant-a", "", "op-ooo")
	if err != nil {
		t.Fatal(err)
	}
	if len(timeline) != 2 || timeline[0].EventID != "ooo-earlier" || timeline[1].EventID != "ooo-later" {
		t.Fatalf("timeline must order by occurred_at: %+v", timeline)
	}
	// 哈希链按 sequence 链接，不受业务时间乱序影响。
	integrity, err := svc.VerifyIntegrity(testCtx, "tenant-a", "", "")
	if err != nil || !integrity.Valid {
		t.Fatalf("integrity after out-of-order ingest: %+v %v", integrity, err)
	}
}
