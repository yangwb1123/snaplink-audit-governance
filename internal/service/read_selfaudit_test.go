package service

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
)

// TestReadSelfAuditOnQueryAndGet pins F-06: every event read through the
// service layer appends an audit.event.read fact with the caller identity,
// so no transport can serve an un-audited read.
func TestReadSelfAuditOnQueryAndGet(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("selfaudit-evt", "op-sa", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	// Empty actor (system-internal read) records nothing.
	query := domain.Query{From: at.Add(-time.Second), To: at.Add(time.Second), PageSize: 100}
	if _, err := svc.QueryEvents("tenant-a", "", query); err != nil {
		t.Fatal(err)
	}
	// Actor-bearing reads record one audit.event.read each.
	if _, err := svc.QueryEvents("tenant-a", "auditor-1", query); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GetEvent("tenant-a", "auditor-1", "selfaudit-evt"); err != nil {
		t.Fatal(err)
	}
	actions, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	var reads []domain.AdminAction
	for _, action := range actions {
		if action.Action == domain.AdminActionEventRead {
			reads = append(reads, action)
		}
	}
	if len(reads) != 2 {
		t.Fatalf("audit.event.read rows=%d, want 2 (query + get); actions=%v", len(reads), actions)
	}
	for _, read := range reads {
		if read.Actor != "auditor-1" {
			t.Fatalf("read actor=%q, want auditor-1", read.Actor)
		}
	}
	gotTargets := map[string]bool{}
	for _, read := range reads {
		gotTargets[read.TargetType+":"+read.TargetID] = true
	}
	if !gotTargets["query:"] || !gotTargets["event:selfaudit-evt"] {
		t.Fatalf("read targets=%v, want query and event:selfaudit-evt", gotTargets)
	}
}

// TestReadSelfAuditOnExportPins F-06 export legs: creating an export runs the
// query (audit.event.read for the exporter) and downloading records
// audit.event.export for the downloader.
func TestReadSelfAuditOnExport(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("export-sa-evt", "op-exp", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	query := domain.Query{From: at.Add(-time.Second), To: at.Add(time.Second), PageSize: 100}
	job, err := svc.CreateExport("tenant-a", "compliance-1", query)
	if err != nil {
		t.Fatal(err)
	}
	// runExport 是异步 goroutine：等待任务收敛，避免其写入在临时目录清理时
	// 仍在进行（race 模式下更容易触发）。
	// runExport 是异步 goroutine：等待收敛，避免其写入与 TempDir 清理竞争。
	waitExport(t, svc, job.ID)
	if err := svc.RecordExportDownload("tenant-a", "compliance-1", job.ID); err != nil {
		t.Fatal(err)
	}
	actions, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	var reads, exports int
	for _, action := range actions {
		switch action.Action {
		case domain.AdminActionEventRead:
			reads++
		case domain.AdminActionEventExport:
			exports++
			if action.TargetID != job.ID || action.Actor != "compliance-1" {
				t.Fatalf("export action=%+v, want target %s actor compliance-1", action, job.ID)
			}
		}
	}
	if reads != 1 {
		t.Fatalf("audit.event.read rows=%d, want 1 (export's internal query)", reads)
	}
	if exports != 1 {
		t.Fatalf("audit.event.export rows=%d, want 1", exports)
	}
	// Empty actor: download recording is skipped (system-internal path).
	if err := svc.RecordExportDownload("tenant-a", "", job.ID); err != nil {
		t.Fatal(err)
	}
}

// TestReadSelfAuditFailClosed documents the fail-closed rule: the read is
// only returned after the self-audit append succeeded. The append failure
// path itself is exercised at the store level (Update aborts on closure
// error); here we pin that a healthy store serves reads normally.
func TestReadSelfAuditFailClosed(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("failclosed-evt", "op-fc", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	query := domain.Query{From: at.Add(-time.Second), To: at.Add(time.Second), PageSize: 100}
	if _, err := svc.QueryEvents("tenant-a", "auditor-1", query); err != nil {
		t.Fatalf("healthy read must succeed: %v", err)
	}
}

// readFacts returns the tenant's audit.event.read rows, newest last (the
// service appends in call order; ListAdminActions walks the slice backwards).
func readFacts(t *testing.T, svc *Service) []domain.AdminAction {
	t.Helper()
	actions, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	var reads []domain.AdminAction
	for _, action := range actions {
		if action.Action == domain.AdminActionEventRead {
			reads = append(reads, action)
		}
	}
	return reads
}

// TestReadSelfAuditOnTimelineReplayReceiptVerify is AC-1 (service): the five
// formerly un-audited event-content reads append exactly one audit.event.read
// fact each, with the acting subject and the FR-4 target encoding.
func TestReadSelfAuditOnTimelineReplayReceiptVerify(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("selfaudit-evt", "op-sa", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.OperationTimeline("tenant-a", "auditor-1", "op-sa"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ReplayOperation("tenant-a", "auditor-1", "op-sa"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.AggregateTimeline("tenant-a", "auditor-1", "invoice", "inv-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GetReceipt("tenant-a", "auditor-1", "selfaudit-evt"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.VerifyIntegrity("tenant-a", "auditor-1", ""); err != nil {
		t.Fatal(err)
	}
	reads := readFacts(t, svc)
	if len(reads) != 5 {
		t.Fatalf("audit.event.read rows=%d, want 5; actions=%+v", len(reads), reads)
	}
	for _, read := range reads {
		if read.Actor != "auditor-1" {
			t.Fatalf("read actor=%q, want auditor-1", read.Actor)
		}
	}
	got := map[string]bool{}
	for _, read := range reads {
		got[read.TargetType+"|"+read.TargetID+"|"+read.Detail] = true
	}
	want := map[string]bool{
		"operation|op-sa|timeline":    true,
		"operation|op-sa|replay":      true,
		"aggregate|inv-1|invoice":     true,
		"event|selfaudit-evt|receipt": true,
		"integrity||verify":           true,
	}
	if len(got) != len(want) {
		t.Fatalf("read facts=%v, want exact set %v", got, want)
	}
	for key := range want {
		if !got[key] {
			t.Fatalf("read facts=%v, missing %q", got, key)
		}
	}
}

// TestReadSelfAuditNoDoubleAppendOnReplay is AC-3b: replay routes through the
// un-audited timeline core, so one replay call appends exactly one fact
// (detail "replay") — never the delegated timeline fact as well.
func TestReadSelfAuditNoDoubleAppendOnReplay(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("dbl-evt", "op-dbl", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ReplayOperation("tenant-a", "auditor-1", "op-dbl"); err != nil {
		t.Fatal(err)
	}
	reads := readFacts(t, svc)
	if len(reads) != 1 {
		t.Fatalf("rows after one replay=%d, want exactly 1 (no double-append); %+v", len(reads), reads)
	}
	if reads[0].TargetType != "operation" || reads[0].TargetID != "op-dbl" || reads[0].Detail != "replay" {
		t.Fatalf("replay fact=%+v, want (operation, op-dbl, replay)", reads[0])
	}
	// A replay plus a direct timeline on the same operation stays one fact
	// per call: 2 more rows, details replay + timeline.
	if _, err := svc.ReplayOperation("tenant-a", "auditor-1", "op-dbl"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.OperationTimeline("tenant-a", "auditor-1", "op-dbl"); err != nil {
		t.Fatal(err)
	}
	reads = readFacts(t, svc)
	if len(reads) != 3 {
		t.Fatalf("rows after replay+replay+timeline=%d, want 3; %+v", len(reads), reads)
	}
	details := map[string]int{}
	for _, read := range reads {
		details[read.Detail]++
	}
	if details["replay"] != 2 || details["timeline"] != 1 {
		t.Fatalf("fact details=%v, want replay x2 + timeline x1", details)
	}
}

// TestReadSelfAuditRestoreFlowRecordsNothing is AC-3c and pins the design-gate
// decision on security finding F1: restore preview replays event-derived
// state with the same permission as the audited timeline endpoints, and the
// gate chose an explicit documented rejection of audit there (empty actor ⇒
// recordReadAction appends nothing). If product later decides preview reads
// must be visible in the trail, this test is the behavior pin that changes.
func TestReadSelfAuditRestoreFlowRecordsNothing(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("restore-evt", "op-res", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	preview, err := svc.PreviewRestore("tenant-a", domain.RestoreRequest{OperationID: "op-res"})
	if err != nil {
		t.Fatalf("PreviewRestore: %v", err)
	}
	if preview.OperationID != "op-res" || preview.ProposedState == nil {
		t.Fatalf("preview=%+v, want op-res with replayed state", preview)
	}
	run, err := svc.CreateRestore("tenant-a", domain.RestoreRequest{OperationID: "op-res", Reason: "compliance review"}, "restorer-1")
	if err != nil {
		t.Fatalf("CreateRestore: %v", err)
	}
	if run.Status != domain.RestoreStatusPendingApproval {
		t.Fatalf("run status=%q, want pending approval", run.Status)
	}
	if reads := readFacts(t, svc); len(reads) != 0 {
		t.Fatalf("audit.event.read rows=%d, want 0 (restore flows stay silent by design); %+v", len(reads), reads)
	}
	actions, err := svc.ListAdminActions("tenant-a", false, 100)
	if err != nil {
		t.Fatal(err)
	}
	var created bool
	for _, action := range actions {
		if action.Action == domain.AdminActionRestoreCreated {
			created = true
			if action.Actor != "restorer-1" || action.TargetID != run.ID {
				t.Fatalf("restore.created action=%+v, want actor restorer-1 target %s", action, run.ID)
			}
		}
	}
	if !created {
		t.Fatalf("no restore.created action for the run; actions=%+v", actions)
	}
}

// TestReadSelfAuditOutOfScopeReadsRecordNothing pins design constraint 7 and
// async-review finding 2: GetExport (job status with event content) is out of
// scope and must keep recording nothing on its success path — a future change
// adding audit there fails this test. Operation (summary) is now audited
// (F-06 closure): it appends exactly one fact per call.
func TestReadSelfAuditOutOfScopeReadsRecordNothing(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("scope-evt", "op-scope", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Operation("tenant-a", "auditor-1", "op-scope"); err != nil {
		t.Fatalf("Operation: %v", err)
	}
	query := domain.Query{From: at.Add(-time.Second), To: at.Add(time.Second), PageSize: 100}
	job, err := svc.CreateExport("tenant-a", "compliance-1", query)
	if err != nil {
		t.Fatal(err)
	}
	waitExport(t, svc, job.ID)
	if _, err := svc.GetExport("tenant-a", job.ID); err != nil {
		t.Fatalf("GetExport: %v", err)
	}
	// Two read facts are expected: the operation summary (auditor-1) and the
	// export's internal query (compliance-1). GetExport itself appends
	// nothing — any fact attributable to it fails this scope pin.
	reads := readFacts(t, svc)
	if len(reads) != 2 {
		t.Fatalf("audit.event.read rows=%d, want 2 (operation summary + export's internal query); %+v", len(reads), reads)
	}
	var opFact, queryFact bool
	for _, read := range reads {
		switch {
		case read.TargetType == "operation" && read.TargetID == "op-scope" && read.Detail == "summary" && read.Actor == "auditor-1":
			opFact = true
		case read.TargetType == "query" && read.Actor == "compliance-1":
			queryFact = true
		default:
			t.Fatalf("unexpected read fact=%+v (GetExport must record nothing)", read)
		}
	}
	if !opFact || !queryFact {
		t.Fatalf("read facts=%+v, want operation|op-scope|summary by auditor-1 and query by compliance-1", reads)
	}
}

// TestReadSelfAuditOperationInvalidAppendsNothing is AC-2 (service): an empty
// operation id fails with ErrInvalid before the append point and appends
// nothing; the HTTP mapping (statusForError: ErrInvalid→400) is covered by
// the handler path tests.
func TestReadSelfAuditOperationInvalidAppendsNothing(t *testing.T) {
	svc := testService(t, false)
	if _, err := svc.Operation("tenant-a", "auditor-1", ""); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("Operation(empty id) err=%v, want ErrInvalid", err)
	}
	if reads := readFacts(t, svc); len(reads) != 0 {
		t.Fatalf("audit.event.read rows=%d, want 0 (invalid calls append nothing); %+v", len(reads), reads)
	}
}

// TestReadSelfAuditFailClosedTimelineReplayReceiptVerify is AC-2 (service):
// when the admin-action append fails, every one of the five actor-bearing
// reads fails closed and no read result is served. The backend is seeded and
// the event ingested through a clean backend FIRST, then saveErr is armed —
// arming at construction would fail the seeding writes themselves (async
// review finding 1).
func TestReadSelfAuditFailClosedTimelineReplayReceiptVerify(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("fail-evt", "op-fail", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	appendErr := errors.New("append failed")
	backend.saveErr = appendErr
	checks := []struct {
		name string
		err  error
	}{
		{"timeline", func() error { _, err := svc.OperationTimeline("tenant-a", "auditor-1", "op-fail"); return err }()},
		{"replay", func() error { _, err := svc.ReplayOperation("tenant-a", "auditor-1", "op-fail"); return err }()},
		{"aggregate timeline", func() error { _, err := svc.AggregateTimeline("tenant-a", "auditor-1", "invoice", "inv-1"); return err }()},
		{"receipt", func() error { _, err := svc.GetReceipt("tenant-a", "auditor-1", "fail-evt"); return err }()},
		{"verify", func() error { _, err := svc.VerifyIntegrity("tenant-a", "auditor-1", ""); return err }()},
	}
	for _, check := range checks {
		if !errors.Is(check.err, appendErr) {
			t.Fatalf("%s: err=%v, want append error %v (fail-closed, no read served)", check.name, check.err, appendErr)
		}
	}
	if reads := readFacts(t, svc); len(reads) != 0 {
		t.Fatalf("audit.event.read rows=%d, want 0 (failed appends persist nothing); %+v", len(reads), reads)
	}
}

// TestReadSelfAuditConflictExhaustionFailsReads pins FM-2/F3: when Save
// always reports ErrSnapshotConflict, Update's bounded retry loop exhausts
// and every actor-bearing read fails with the conflict error, appending
// nothing. alwaysConflict is armed after seeding + ingest (seed-then-arm) and
// is independent of the unexported snapshotConflictRetries constant.
func TestReadSelfAuditConflictExhaustionFailsReads(t *testing.T) {
	backend := &scriptedConflictBackend{data: store.NewSnapshot()}
	svc := testServiceWithBackend(t, backend)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("conf-evt", "op-conf", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	backend.alwaysConflict = true
	checks := []struct {
		name string
		err  error
	}{
		{"timeline", func() error { _, err := svc.OperationTimeline("tenant-a", "auditor-1", "op-conf"); return err }()},
		{"replay", func() error { _, err := svc.ReplayOperation("tenant-a", "auditor-1", "op-conf"); return err }()},
		{"aggregate timeline", func() error { _, err := svc.AggregateTimeline("tenant-a", "auditor-1", "invoice", "inv-1"); return err }()},
		{"receipt", func() error { _, err := svc.GetReceipt("tenant-a", "auditor-1", "conf-evt"); return err }()},
		{"verify", func() error { _, err := svc.VerifyIntegrity("tenant-a", "auditor-1", ""); return err }()},
	}
	for _, check := range checks {
		if !errors.Is(check.err, store.ErrSnapshotConflict) {
			t.Fatalf("%s: err=%v, want ErrSnapshotConflict after retry exhaustion", check.name, check.err)
		}
	}
	if reads := readFacts(t, svc); len(reads) != 0 {
		t.Fatalf("audit.event.read rows=%d, want 0; %+v", len(reads), reads)
	}
}

// TestVerifyIntegrityRejectsUnboundedStreamID pins security finding F2: the
// verify fact records stream_id verbatim as target_id in the unbounded admin
// trail, so stream_id is bounded (key-framing charset + 256-byte cap) at the
// service layer. Rejected calls return ErrInvalid and append nothing; the
// boundary value (exactly the cap) still verifies and records a bounded fact.
func TestVerifyIntegrityRejectsUnboundedStreamID(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("f2-evt", "op-f2", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		sid  string
	}{
		{"over cap", strings.Repeat("s", maxVerifyStreamIDLength+1)},
		{"whitespace", "stream with space"},
		{"control char", "stream\x1fid"},
		{"path separator", "stream/id"},
	}
	for _, tc := range cases {
		result, err := svc.VerifyIntegrity("tenant-a", "auditor-1", tc.sid)
		if !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("%s: err=%v, want ErrInvalid; result=%+v", tc.name, err, result)
		}
	}
	if reads := readFacts(t, svc); len(reads) != 0 {
		t.Fatalf("audit.event.read rows=%d, want 0 (rejected calls append nothing); %+v", len(reads), reads)
	}
	// Boundary: a valid stream_id of exactly the cap is accepted and its fact
	// carries the bounded target_id.
	boundary := strings.Repeat("s", maxVerifyStreamIDLength)
	result, err := svc.VerifyIntegrity("tenant-a", "auditor-1", boundary)
	if err != nil {
		t.Fatalf("boundary verify: %v", err)
	}
	if result.Valid != true {
		t.Fatalf("boundary verify invalid: %+v", result)
	}
	reads := readFacts(t, svc)
	if len(reads) != 1 {
		t.Fatalf("audit.event.read rows=%d, want 1; %+v", len(reads), reads)
	}
	if reads[0].TargetType != "integrity" || reads[0].TargetID != boundary || reads[0].Detail != "verify" {
		t.Fatalf("verify fact=%+v, want (integrity, boundary, verify)", reads[0])
	}
}

// TestReadSelfAuditReplayAggregateSingleFact pins design decision
// D-ReplayAggregate: ReplayAggregate (zero production callers) appends
// exactly one fact with detail "replay", keeping the every-content-read-
// leaves-exactly-one-fact invariant symmetric with ReplayOperation.
func TestReadSelfAuditReplayAggregateSingleFact(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("agg-evt", "op-agg", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	result, err := svc.ReplayAggregate("tenant-a", "auditor-1", "invoice", "inv-1")
	if err != nil {
		t.Fatalf("ReplayAggregate: %v", err)
	}
	if result.State == nil {
		t.Fatal("replay state is nil")
	}
	reads := readFacts(t, svc)
	if len(reads) != 1 {
		t.Fatalf("audit.event.read rows=%d, want exactly 1; %+v", len(reads), reads)
	}
	if reads[0].TargetType != "aggregate" || reads[0].TargetID != "inv-1" || reads[0].Detail != "replay" || reads[0].Actor != "auditor-1" {
		t.Fatalf("replay-aggregate fact=%+v, want (aggregate, inv-1, replay) by auditor-1", reads[0])
	}
}

// TestVerifyIntegrityDoesNotAttestAdminTrail pins the design-gate decision on
// security finding F4: the admin trail is append-only by code convention, NOT
// cryptographically attested by VerifyIntegrity (option (b); attestation via
// rolling hash/signature is deliberately scoped to a separate change). A
// DB-level insider tampering with admin_actions must not flip the verdict —
// if the trail is ever included in the verified set, this test fails and
// forces the conscious contract change (and its own regression test).
func TestVerifyIntegrityDoesNotAttestAdminTrail(t *testing.T) {
	svc := testService(t, false)
	at := time.Unix(1_700_000_010, 0).UTC()
	if _, err := svc.Ingest("tenant-a", crmPrincipal, testEvent("f4-evt", "op-f4", at), domain.StatusLedgered); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GetEvent("tenant-a", "auditor-1", "f4-evt"); err != nil {
		t.Fatal(err)
	}
	// Tamper with the trail in the snapshot (DB-level insider, capability e).
	if err := svc.Store.Update(func(data *store.Snapshot) error {
		for i := range data.AdminActions {
			data.AdminActions[i].Actor = "forged-actor"
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err := svc.VerifyIntegrity("tenant-a", "auditor-1", "")
	if err != nil {
		t.Fatalf("VerifyIntegrity: %v", err)
	}
	if !result.Valid {
		t.Fatalf("verdict=%+v, want Valid:true — the admin trail is convention-only, not attested (F4 decision b)", result)
	}
}

// waitExport polls an export job until it converges (completed/failed), so
// tests that trigger the asynchronous runExport goroutine never race its
// writes against TempDir cleanup.
func waitExport(t *testing.T, svc *Service, jobID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, err := svc.GetExport("tenant-a", jobID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Status == "completed" || current.Status == "failed" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("export %s did not finish (status=%s)", jobID, current.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
