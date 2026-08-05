package service

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
	"github.com/snaplink/audit-governance/internal/store"
)

var crmPrincipal = domain.IngestPrincipal{ClientID: "crm"}

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
	svc := New(st, Config{ArchiveDir: archiveDir, SegmentSize: 2, SigningSecret: "test-secret", Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }})
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

func TestIngestIdempotencyConflictAndIntegrity(t *testing.T) {
	svc := testService(t, true)
	at := time.Unix(1_700_000_010, 0).UTC()
	first, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("evt-1", "op-1", at), domain.StatusLedgered)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != domain.StatusArchived || first.Sequence != 1 {
		t.Fatalf("unexpected first receipt: %+v", first)
	}
	duplicate, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("evt-1", "op-1", at), domain.StatusLedgered)
	if err != nil || !duplicate.Duplicate {
		t.Fatalf("duplicate not detected: %+v %v", duplicate, err)
	}
	conflicting := testEvent("evt-1", "op-1", at)
	conflicting.Payload["value"] = 11
	if _, err := svc.Ingest("tenant-a", crmPrincipal, conflicting, domain.StatusLedgered); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	second := testEvent("evt-2", "op-1", at.Add(time.Second))
	second.AggregateVersion = 2
	if _, err := svc.Ingest("tenant-a", crmPrincipal, second, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	result, err := svc.VerifyIntegrity("tenant-a", "")
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
	if _, err := svc.Ingest("tenant-a", crmPrincipal, firstEvent, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	secondEvent := testEvent("idem-event-2", "idem-op", time.Now().UTC())
	secondEvent.IdempotencyKey = firstEvent.IdempotencyKey
	receipt, err := svc.Ingest("tenant-a", crmPrincipal, secondEvent, domain.StatusLedgered)
	if !errors.Is(err, domain.ErrConflict) || !receipt.Conflict || receipt.ErrorCode != "idempotency_key_conflict" {
		t.Fatalf("expected idempotency conflict: %+v %v", receipt, err)
	}
}

func TestIngestBindsSourceToAuthenticatedClient(t *testing.T) {
	svc := testService(t, false)
	event := testEvent("source-bound", "source-op", time.Unix(1_700_000_010, 0).UTC())
	_, wrongErr := svc.Ingest("tenant-a", domain.IngestPrincipal{ClientID: "other-client"}, event, domain.StatusLedgered)
	if !errors.Is(wrongErr, domain.ErrForbidden) {
		t.Fatalf("cross-source client must be forbidden: %v", wrongErr)
	}
	_, missingErr := svc.Ingest("tenant-a", domain.IngestPrincipal{}, event, domain.StatusLedgered)
	if !errors.Is(missingErr, domain.ErrForbidden) || missingErr.Error() != wrongErr.Error() {
		t.Fatalf("missing client identity must fail closed: wrong=%v missing=%v", wrongErr, missingErr)
	}
	unknown := event
	unknown.SourceSystem = "unknown"
	_, unknownErr := svc.Ingest("tenant-a", domain.IngestPrincipal{ClientID: "other-client"}, unknown, domain.StatusLedgered)
	if !errors.Is(unknownErr, domain.ErrForbidden) || unknownErr.Error() != wrongErr.Error() {
		t.Fatalf("unknown source must be indistinguishable: wrong=%v unknown=%v", wrongErr, unknownErr)
	}
	if _, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
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
	if _, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("old default binding must be replaced: %v", err)
	}
	if _, err := svc.Ingest("tenant-a", domain.IngestPrincipal{ClientID: "relay-a"}, event, domain.StatusLedgered); err != nil {
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
	event.TenantID = "tenant-b"
	receipt, err := svc.Ingest("", crmPrincipal, event, domain.StatusLedgered)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.TenantID != "tenant-a" {
		t.Fatalf("body tenant overrode server binding: %+v", receipt)
	}
	crossTenant := testEvent("cross-tenant", "derive-op", time.Unix(1_700_000_011, 0).UTC())
	if _, err := svc.Ingest("tenant-b", crmPrincipal, crossTenant, domain.StatusLedgered); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("signed tenant hint escaped source binding: %v", err)
	}
	if _, err := svc.UpdateSource("test", domain.SourceSystem{
		TenantID: "tenant-b", ID: "crm", Name: "CRM B", Active: true,
		AllowedClientIDs: []string{"crm", "tenant-b-relay"},
	}); err != nil {
		t.Fatal(err)
	}
	ambiguous := testEvent("ambiguous-tenant", "derive-op", time.Unix(1_700_000_012, 0).UTC())
	if _, err := svc.Ingest("", crmPrincipal, ambiguous, domain.StatusLedgered); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("ambiguous server-side tenant binding must fail closed: %v", err)
	}
}

func TestQueryReplayAndExport(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_100, 0).UTC()
	for i := 0; i < 3; i++ {
		event := testEvent("evt-q-"+string(rune('1'+i)), "op-query", at.Add(time.Duration(i)*time.Second))
		event.AggregateVersion = int64(i + 1)
		event.ChangedFields = map[string]domain.FieldChange{"status": {After: []any{"open", "paid", "closed"}[i]}}
		if _, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
			t.Fatal(err)
		}
	}
	query := domain.Query{From: at.Add(-time.Second), To: at.Add(10 * time.Second), PageSize: 2}
	page, err := svc.QueryEvents("tenant-a", query)
	if err != nil || len(page.Items) != 2 || page.NextCursor == "" {
		t.Fatalf("unexpected first page: %+v %v", page, err)
	}
	query.Cursor = page.NextCursor
	page2, err := svc.QueryEvents("tenant-a", query)
	if err != nil || len(page2.Items) != 1 {
		t.Fatalf("unexpected second page: %+v %v", page2, err)
	}
	replay, err := svc.ReplayOperation("tenant-a", "op-query")
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

func TestSensitivePayloadRejected(t *testing.T) {
	svc := testService(t, false)
	event := testEvent("evt-secret", "op-secret", time.Now().UTC())
	event.Payload["password"] = "do-not-store"
	if _, err := svc.Ingest("tenant-a", crmPrincipal, event, ""); err == nil || !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("expected sensitive payload rejection, got %v", err)
	}
}

func TestRetentionReportRespectsLegalHold(t *testing.T) {
	svc := testService(t, false)
	if err := svc.SetRetentionPolicy("test", domain.RetentionPolicy{TenantID: "tenant-a", ArchiveDays: 1, RetentionClass: "standard"}); err != nil {
		t.Fatal(err)
	}
	event := testEvent("evt-old", "op-old", time.Now().Add(-72*time.Hour).UTC())
	if _, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	securityEvent := testEvent("evt-security", "op-security", time.Now().Add(-72*time.Hour).UTC())
	securityEvent.RetentionClass = "security"
	if _, err := svc.Ingest("tenant-a", crmPrincipal, securityEvent, domain.StatusLedgered); err != nil {
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
	svc := New(st, Config{ArchiveDir: filepath.Join(dir, "archive"), Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }})
	if err := svc.CreateTenant("test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("persisted", "op-persisted", time.Unix(1_700_000_010, 0).UTC()), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	reopenedService := New(reopened, Config{ArchiveDir: filepath.Join(dir, "archive")})
	event, err := reopenedService.GetEvent("tenant-a", "persisted")
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
	first, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := svc.GetEvent("tenant-a", "evt-encrypted")
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
	expectedDigest, err := security.SearchDigest("alice@example.test", svc.Config.EncryptionKey)
	if err != nil || stored.Payload["email__search_digest"] != expectedDigest {
		t.Fatalf("search digest must use original value: got=%v want=%v err=%v", stored.Payload["email__search_digest"], expectedDigest, err)
	}
	duplicate, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered)
	if err != nil || !duplicate.Duplicate || first.Hash != duplicate.Hash {
		t.Fatalf("encrypted duplicate failed: %+v %v", duplicate, err)
	}
}

func TestIntegrityChecksStreamsIndependently(t *testing.T) {
	svc := testService(t, false)
	base := time.Unix(1_700_000_000, 0).UTC()
	first := testEvent("stream-a", "", base.Add(10*time.Second))
	first.OperationID = ""
	first.AggregateID = ""
	first.AggregateType = ""
	first.StreamID = "stream-a"
	second := testEvent("stream-b", "", base)
	second.OperationID = ""
	second.AggregateID = ""
	second.AggregateType = ""
	second.StreamID = "stream-b"
	if _, err := svc.Ingest("tenant-a", crmPrincipal, first, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest("tenant-a", crmPrincipal, second, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	result, err := svc.VerifyIntegrity("tenant-a", "")
	if err != nil || !result.Valid {
		t.Fatalf("independent streams should validate: %+v %v", result, err)
	}
}

func TestArchivePendingRetriesIndexedEvents(t *testing.T) {
	svc := testService(t, false)
	event := testEvent("pending-archive", "op-pending", time.Now().UTC())
	if _, err := svc.Ingest("tenant-a", crmPrincipal, event, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	svc.Config.ArchiveDir = filepath.Join(t.TempDir(), "archive")
	count, err := svc.ArchivePending("tenant-a")
	if err != nil || count != 1 {
		t.Fatalf("pending archive failed: count=%d err=%v", count, err)
	}
	receipt, err := svc.GetReceipt("tenant-a", event.EventID)
	if err != nil || receipt.Status != domain.StatusArchived {
		t.Fatalf("receipt was not archived: %+v %v", receipt, err)
	}
}

func TestRestoreApprovalWorkflow(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("evt-restore", "op-restore", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	run, err := svc.CreateRestore("tenant-a", domain.RestoreRequest{OperationID: "op-restore", Reason: "user error"}, "requester-1")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != domain.RestoreStatusPendingApproval || run.CreatedBy != "requester-1" {
		t.Fatalf("unexpected created run: %+v", run)
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

	run2, err := svc.CreateRestore("tenant-a", domain.RestoreRequest{OperationID: "op-restore", Reason: "duplicate entry"}, "requester-2")
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := svc.RejectRestore("tenant-a", run2.ID, "approver-3")
	if err != nil {
		t.Fatal(err)
	}
	if rejected.Status != domain.RestoreStatusRejected || rejected.RejectedBy != "approver-3" || rejected.RejectedAt == nil {
		t.Fatalf("unexpected rejection: %+v", rejected)
	}

	// 租户边界与缺失目标都关闭为 not found。
	if _, err := svc.ApproveRestore("tenant-b", run.ID, "approver-1"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("cross tenant approve err=%v, want not found", err)
	}
	if _, err := svc.ApproveRestore("tenant-a", "restore-nope", "approver-1"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing run err=%v, want not found", err)
	}
}

func TestQueryCorrelationDimensions(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	first := testEvent("corr-evt-1", "op-corr", at)
	first.CorrelationID = "corr-outer"
	first.TraceID = "trace-1"
	if _, err := svc.Ingest("tenant-a", crmPrincipal, first, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	second := testEvent("corr-evt-2", "op-corr", at.Add(time.Second))
	second.CausationID = "corr-evt-1"
	second.CorrelationID = "corr-outer"
	second.TraceID = "trace-1"
	if _, err := svc.Ingest("tenant-a", crmPrincipal, second, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	unrelated := testEvent("corr-evt-3", "op-other", at.Add(2*time.Second))
	unrelated.CorrelationID = "corr-other"
	if _, err := svc.Ingest("tenant-a", crmPrincipal, unrelated, domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}

	base := domain.Query{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_100, 0).UTC(), PageSize: 100}
	byCorrelation, err := svc.QueryEvents("tenant-a", domain.Query{From: base.From, To: base.To, CorrelationID: "corr-outer", PageSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	if byCorrelation.Count != 2 {
		t.Fatalf("correlation query count=%d, want 2", byCorrelation.Count)
	}
	byCausation, err := svc.QueryEvents("tenant-a", domain.Query{From: base.From, To: base.To, CausationID: "corr-evt-1", PageSize: 100})
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
	if _, err := svc.CreateExport("tenant-a", "compliance-3", query); err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("audit-evt", "op-audit", at), domain.StatusLedgered); err != nil {
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
	_, errUnknown := svc.Ingest("tenant-a", crmPrincipal, withSource("ghost"), "")
	_, errDisabled := svc.Ingest("tenant-a", crmPrincipal, withSource("disabled"), "")
	_, errForbidden := svc.Ingest("tenant-a", domain.IngestPrincipal{ClientID: "other-client"}, withSource("crm"), "")
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
