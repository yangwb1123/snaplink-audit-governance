package httpapi

// Restore cache-hardening regression. The six restore operations must never
// let an intermediary store a response: the no-store header is set at handler
// entry, before authentication, tenant-scope resolution, request/filter
// parsing or any service read, so success, refusal, withdrawal and storage
// failure share one cache policy. This is transport hardening only: it does
// not change the silent restore read policy, approval semantics, tenant scope
// or the fact that approval is not business execution.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

const (
	restoreNoStoreSecretReason = "private-restore-reason-never-echoed"
	restoreNoStoreControlFault = "private restore control write failure"
)

var (
	restoreNoStoreNow = time.Unix(1_700_000_000, 0).UTC()

	errRestoreNoStoreControlWrite = errors.New(restoreNoStoreControlFault)
)

type restoreNoStoreArchive struct {
	inner *archive.FileStore
	mu    sync.Mutex
	puts  int
	gets  int
}

func (a *restoreNoStoreArchive) Put(ctx context.Context, key string, data []byte) error {
	a.mu.Lock()
	a.puts++
	a.mu.Unlock()
	return a.inner.Put(ctx, key, data)
}

func (a *restoreNoStoreArchive) Get(ctx context.Context, key string) ([]byte, error) {
	a.mu.Lock()
	a.gets++
	a.mu.Unlock()
	return a.inner.Get(ctx, key)
}

func (a *restoreNoStoreArchive) Ready(ctx context.Context) error {
	return a.inner.Ready(ctx)
}

func (a *restoreNoStoreArchive) counts() (puts, gets int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.puts, a.gets
}

type restoreNoStoreFixture struct {
	store   *store.Store
	service *service.Service
	handler http.Handler
	server  *httptest.Server
	archive *restoreNoStoreArchive
	armed   *atomic.Bool
}

type restoreNoStoreResponse struct {
	status int
	header http.Header
	body   []byte
}

func newRestoreNoStoreFixture(t *testing.T) *restoreNoStoreFixture {
	t.Helper()
	armed := &atomic.Bool{}
	st, err := store.OpenWithControlWriteFault(filepath.Join(t.TempDir(), "state.json"), func() error {
		if armed.Load() {
			return errRestoreNoStoreControlWrite
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	archiveDir := filepath.Join(t.TempDir(), "archive")
	files := &restoreNoStoreArchive{inner: &archive.FileStore{Dir: archiveDir}}
	svc, err := service.New(st, service.Config{
		Archive:         files,
		ArchiveDir:      archiveDir,
		Now:             func() time.Time { return restoreNoStoreNow },
		AllowDevSecrets: true,
	})
	if err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	for _, tenant := range []domain.Tenant{
		{ID: "tenant-a", Name: "Tenant A", Active: true},
		{ID: "tenant-b", Name: "Tenant B", Active: true},
	} {
		if err := svc.CreateTenant(testCtx, "fixture", tenant); err != nil {
			_ = st.Close()
			t.Fatal(err)
		}
	}
	if err := svc.AddSource(testCtx, "fixture", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	if err := svc.RegisterSchema(testCtx, "fixture", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	api := NewServer(svc, auth.Authenticator{AllowDev: true, JWTSecret: testJWTSecret, AllowLocalHS256: true}, log.New(io.Discard, "", 0))
	return &restoreNoStoreFixture{
		store:   st,
		service: svc,
		handler: api.Handler(),
		server:  httptest.NewServer(api.Handler()),
		archive: files,
		armed:   armed,
	}
}

func (f *restoreNoStoreFixture) close(t *testing.T) {
	t.Helper()
	f.server.Close()
	if err := f.store.Close(); err != nil {
		t.Errorf("close restore fixture store: %v", err)
	}
}

func (f *restoreNoStoreFixture) do(t *testing.T, method, path, token string, body []byte) restoreNoStoreResponse {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequest(method, f.server.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := f.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return restoreNoStoreResponse{status: response.StatusCode, header: response.Header.Clone(), body: raw}
}

// doHandler drives the same real handler in-process for cases whose request
// context must already be withdrawn before ServeHTTP. A live socket cannot
// deterministically observe the response of a request cancelled at entry.
func (f *restoreNoStoreFixture) doHandler(t *testing.T, ctx context.Context, method, path, token string, body []byte) restoreNoStoreResponse {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request := httptest.NewRequest(method, path, reader).WithContext(ctx)
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	f.handler.ServeHTTP(recorder, request)
	return restoreNoStoreResponse{status: recorder.Code, header: recorder.Header().Clone(), body: recorder.Body.Bytes()}
}

func (f *restoreNoStoreFixture) ingest(t *testing.T, eventID, operationID string) {
	t.Helper()
	event := domain.Event{
		EventID:            eventID,
		SourceSystem:       "crm",
		EventType:          "audit.event",
		SchemaID:           "audit.event",
		SchemaVersion:      1,
		OccurredAt:         restoreNoStoreNow.Add(time.Second),
		OperationID:        operationID,
		Actor:              domain.Actor{ID: "business-user"},
		Action:             "update",
		Outcome:            "success",
		DataClassification: "internal",
		RetentionClass:     "standard",
		IdempotencyKey:     eventID + "-idem",
		Payload:            map[string]any{"value": 1},
	}
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	response := f.do(t, http.MethodPost, "/api/v1/events", "dev:tenant-a:service:crm", body)
	if response.status != http.StatusAccepted {
		t.Fatalf("seed event %s status=%d body=%s", eventID, response.status, response.body)
	}
}

func (f *restoreNoStoreFixture) createRun(t *testing.T, token, operationID string) domain.RestoreRun {
	t.Helper()
	body, err := json.Marshal(domain.RestoreRequest{OperationID: operationID, Reason: restoreNoStoreSecretReason})
	if err != nil {
		t.Fatal(err)
	}
	response := f.do(t, http.MethodPost, "/api/v1/restores", token, body)
	assertRestoreNoStore(t, "create "+operationID, response)
	if response.status != http.StatusAccepted {
		t.Fatalf("create restore status=%d body=%s", response.status, response.body)
	}
	var run domain.RestoreRun
	if err := json.Unmarshal(response.body, &run); err != nil {
		t.Fatal(err)
	}
	if run.ID == "" || run.Status != domain.RestoreStatusPendingApproval {
		t.Fatalf("created restore run=%+v", run)
	}
	return run
}

func assertRestoreNoStore(t *testing.T, label string, response restoreNoStoreResponse) {
	t.Helper()
	if got := response.header.Get("Cache-Control"); got != "no-store" {
		t.Errorf("%s: Cache-Control=%q, want no-store (status=%d)", label, got, response.status)
	}
}

func assertRestoreError(t *testing.T, label string, response restoreNoStoreResponse, wantStatus int, wantCode string, forbidden ...string) {
	t.Helper()
	assertRestoreNoStore(t, label, response)
	if response.status != wantStatus {
		t.Fatalf("%s: status=%d, want %d (body=%s)", label, response.status, wantStatus, response.body)
	}
	var envelope errorEnvelope
	if err := json.Unmarshal(response.body, &envelope); err != nil {
		t.Fatalf("%s: error envelope decode: %v (body=%s)", label, err, response.body)
	}
	if envelope.Error.Code != wantCode {
		t.Fatalf("%s: error.code=%q, want %q (body=%s)", label, envelope.Error.Code, wantCode, response.body)
	}
	lower := strings.ToLower(string(response.body))
	for _, token := range append([]string{
		restoreNoStoreSecretReason,
		`"proposed_state"`,
		`"external_calls"`,
		`"approved_by"`,
		`"rejected_by"`,
		`"created_by"`,
		`"status"`,
	}, forbidden...) {
		if strings.Contains(lower, strings.ToLower(token)) {
			t.Errorf("%s: error response leaked %q (body=%s)", label, token, response.body)
		}
	}
}

func decodeRestoreRun(t *testing.T, label string, response restoreNoStoreResponse) domain.RestoreRun {
	t.Helper()
	assertRestoreNoStore(t, label, response)
	if response.status != http.StatusOK {
		t.Fatalf("%s: status=%d body=%s", label, response.status, response.body)
	}
	var run domain.RestoreRun
	if err := json.Unmarshal(response.body, &run); err != nil {
		t.Fatal(err)
	}
	return run
}

func (f *restoreNoStoreFixture) getRun(t *testing.T, token, runID string) domain.RestoreRun {
	t.Helper()
	return decodeRestoreRun(t, "get "+runID, f.do(t, http.MethodGet, "/api/v1/restores/"+runID, token, nil))
}

func decisionTime(t *testing.T, value *time.Time) time.Time {
	t.Helper()
	if value == nil {
		t.Fatal("decision timestamp is nil")
	}
	return *value
}

func TestHTTPRestoreNoStoreJourneyAndDecisionBoundaries(t *testing.T) {
	fixture := newRestoreNoStoreFixture(t)
	defer fixture.close(t)

	fixture.ingest(t, "restore-ns-approve-evt", "restore-ns-approve-op")
	requester := mintRestoreJWT(t, "requester-1", "tenant-a", []string{"compliance"})
	decider := mintRestoreJWT(t, "approver-1", "tenant-a", []string{"compliance"})

	beforePreview := fixture.snapshotState(t)
	preview := fixture.do(t, http.MethodPost, "/api/v1/restores/preview", requester,
		[]byte(`{"operation_id":"restore-ns-approve-op"}`))
	assertRestoreNoStore(t, "preview success", preview)
	if preview.status != http.StatusOK {
		t.Fatalf("preview status=%d body=%s", preview.status, preview.body)
	}
	var previewBody domain.RestorePreview
	if err := json.Unmarshal(preview.body, &previewBody); err != nil {
		t.Fatal(err)
	}
	if previewBody.OperationID != "restore-ns-approve-op" || previewBody.ProposedState == nil || len(previewBody.ExternalCalls) != 0 {
		t.Fatalf("preview body=%+v", previewBody)
	}
	if afterPreview := fixture.snapshotState(t); afterPreview != beforePreview {
		t.Fatalf("preview changed silent-read state: before=%+v after=%+v", beforePreview, afterPreview)
	}

	run := fixture.createRun(t, requester, "restore-ns-approve-op")
	if run.CreatedBy != "requester-1" {
		t.Fatalf("created_by=%q", run.CreatedBy)
	}
	beforeDetail := fixture.snapshotState(t)
	detail := fixture.getRun(t, requester, run.ID)
	if detail.Status != domain.RestoreStatusPendingApproval || detail.CreatedBy != "requester-1" {
		t.Fatalf("pending detail=%+v", detail)
	}
	// Detail and preview continue to append no read fact and write no control.
	if afterDetail := fixture.snapshotState(t); afterDetail != beforeDetail {
		t.Fatalf("restore detail changed silent-read state: before=%+v after=%+v", beforeDetail, afterDetail)
	}

	selfApprove := fixture.do(t, http.MethodPost, "/api/v1/restores/"+run.ID+"/approve", requester, nil)
	assertRestoreError(t, "applicant self approve", selfApprove, http.StatusForbidden, "forbidden")

	archivePuts, archiveGets := fixture.archive.counts()
	beforeApproval := fixture.snapshotState(t)
	approved := decodeRestoreRun(t, "approve", fixture.do(t, http.MethodPost, "/api/v1/restores/"+run.ID+"/approve", decider, nil))
	if approved.Status != domain.RestoreStatusApproved || approved.ApprovedBy != "approver-1" || approved.RejectedBy != "" {
		t.Fatalf("approved run=%+v", approved)
	}
	approvedAt := decisionTime(t, approved.ApprovedAt)
	if puts, gets := fixture.archive.counts(); puts != archivePuts || gets != archiveGets {
		t.Fatalf("approval performed archive I/O: puts %d->%d gets %d->%d", archivePuts, puts, archiveGets, gets)
	}
	afterApproval := fixture.snapshotState(t)
	if afterApproval.events != beforeApproval.events || afterApproval.restoreCreated != beforeApproval.restoreCreated ||
		afterApproval.rejectedFacts != beforeApproval.rejectedFacts || afterApproval.approvedFacts != beforeApproval.approvedFacts+1 {
		t.Fatalf("approval wrote unrelated success state: before=%+v after=%+v", beforeApproval, afterApproval)
	}
	if action := fixture.lastAdminAction(t); action.Action != domain.AdminActionRestoreApproved || action.Actor != "approver-1" || action.TargetID != run.ID {
		t.Fatalf("approval action=%+v", action)
	}

	final := fixture.getRun(t, decider, run.ID)
	if final.Status != domain.RestoreStatusApproved || final.ApprovedBy != "approver-1" || !final.ApprovedAt.Equal(approvedAt) || final.RejectedBy != "" {
		t.Fatalf("final approved run=%+v", final)
	}
	rejectAfterApprove := fixture.do(t, http.MethodPost, "/api/v1/restores/"+run.ID+"/reject", decider, nil)
	assertRestoreError(t, "reject after approve", rejectAfterApprove, http.StatusConflict, "conflict")
	approveAgain := fixture.do(t, http.MethodPost, "/api/v1/restores/"+run.ID+"/approve", decider, nil)
	assertRestoreError(t, "repeat approve", approveAgain, http.StatusConflict, "conflict")
	afterAttempts := fixture.getRun(t, decider, run.ID)
	if afterAttempts.Status != domain.RestoreStatusApproved || afterAttempts.ApprovedBy != "approver-1" ||
		!afterAttempts.ApprovedAt.Equal(approvedAt) || afterAttempts.RejectedBy != "" || afterAttempts.RejectedAt != nil {
		t.Fatalf("first approval changed after refused decisions: before=%+v after=%+v", final, afterAttempts)
	}

	fixture.ingest(t, "restore-ns-reject-evt", "restore-ns-reject-op")
	rejectedRun := fixture.createRun(t, requester, "restore-ns-reject-op")
	selfReject := fixture.do(t, http.MethodPost, "/api/v1/restores/"+rejectedRun.ID+"/reject", requester, nil)
	assertRestoreError(t, "applicant self reject", selfReject, http.StatusForbidden, "forbidden")
	beforeRejection := fixture.snapshotState(t)
	rejected := decodeRestoreRun(t, "reject", fixture.do(t, http.MethodPost, "/api/v1/restores/"+rejectedRun.ID+"/reject", decider, nil))
	if rejected.Status != domain.RestoreStatusRejected || rejected.RejectedBy != "approver-1" || rejected.ApprovedBy != "" {
		t.Fatalf("rejected run=%+v", rejected)
	}
	rejectedAt := decisionTime(t, rejected.RejectedAt)
	afterRejection := fixture.snapshotState(t)
	if afterRejection.events != beforeRejection.events || afterRejection.restoreCreated != beforeRejection.restoreCreated ||
		afterRejection.approvedFacts != beforeRejection.approvedFacts || afterRejection.rejectedFacts != beforeRejection.rejectedFacts+1 {
		t.Fatalf("rejection wrote unrelated success state: before=%+v after=%+v", beforeRejection, afterRejection)
	}
	approveAfterReject := fixture.do(t, http.MethodPost, "/api/v1/restores/"+rejectedRun.ID+"/approve", decider, nil)
	assertRestoreError(t, "approve after reject", approveAfterReject, http.StatusConflict, "conflict")
	rejectAgain := fixture.do(t, http.MethodPost, "/api/v1/restores/"+rejectedRun.ID+"/reject", decider, nil)
	assertRestoreError(t, "repeat reject", rejectAgain, http.StatusConflict, "conflict")
	finalRejected := fixture.getRun(t, decider, rejectedRun.ID)
	if finalRejected.Status != domain.RestoreStatusRejected || finalRejected.RejectedBy != "approver-1" ||
		!finalRejected.RejectedAt.Equal(rejectedAt) || finalRejected.ApprovedBy != "" || finalRejected.ApprovedAt != nil {
		t.Fatalf("first rejection changed after refused decisions: before=%+v after=%+v", rejected, finalRejected)
	}
	// A successful rejection records exactly one rejection decision/fact and
	// no approval or unrelated success object.
	if action := fixture.lastAdminAction(t); action.Action != domain.AdminActionRestoreRejected || action.Actor != "approver-1" || action.TargetID != rejectedRun.ID {
		t.Fatalf("rejection action=%+v", action)
	}

	list := fixture.do(t, http.MethodGet, "/api/v1/restores", decider, nil)
	assertRestoreNoStore(t, "list success", list)
	if list.status != http.StatusOK {
		t.Fatalf("list status=%d body=%s", list.status, list.body)
	}
	var page domain.JobPage[domain.RestoreRun]
	if err := json.Unmarshal(list.body, &page); err != nil {
		t.Fatal(err)
	}
	if page.Count != 2 || len(page.Items) != 2 {
		t.Fatalf("restore list page=%+v", page)
	}
}

func TestHTTPRestoreNoStoreErrorBranchesAndFailClosed(t *testing.T) {
	fixture := newRestoreNoStoreFixture(t)
	defer fixture.close(t)

	fixture.ingest(t, "restore-ns-errors-evt", "restore-ns-errors-op")
	requester := mintRestoreJWT(t, "requester-1", "tenant-a", []string{"compliance"})
	decider := mintRestoreJWT(t, "approver-1", "tenant-a", []string{"compliance"})
	reader := mintRestoreJWT(t, "auditor-1", "tenant-a", []string{"auditor"})
	writer := mintRestoreJWT(t, "writer-1", "tenant-a", []string{"service"})
	otherTenant := mintRestoreJWT(t, "other-1", "tenant-b", []string{"compliance"})
	platform := mintRestoreJWT(t, "platform-1", "", []string{"platform-admin"})

	approveRun := fixture.createRun(t, requester, "restore-ns-errors-op")
	rejectRun := fixture.createRun(t, requester, "restore-ns-errors-op")

	type restoreCase struct {
		name   string
		method string
		path   string
		token  string
		body   []byte
		status int
		code   string
	}
	cases := []restoreCase{
		{"preview unauthenticated", http.MethodPost, "/api/v1/restores/preview", "", []byte(`{"operation_id":"restore-ns-errors-op"}`), http.StatusUnauthorized, "unauthorized"},
		{"preview missing permission", http.MethodPost, "/api/v1/restores/preview", writer, []byte(`{"operation_id":"restore-ns-errors-op"}`), http.StatusForbidden, "forbidden"},
		{"preview malformed body", http.MethodPost, "/api/v1/restores/preview", requester, []byte(`{"operation_id":"restore-ns-errors-op"}{}`), http.StatusBadRequest, "invalid_request"},
		{"preview platform without tenant", http.MethodPost, "/api/v1/restores/preview", platform, []byte(`{"operation_id":"restore-ns-errors-op"}`), http.StatusBadRequest, "invalid_request"},

		{"create unauthenticated", http.MethodPost, "/api/v1/restores", "", []byte(`{"operation_id":"restore-ns-errors-op","reason":"x"}`), http.StatusUnauthorized, "unauthorized"},
		{"create missing permission", http.MethodPost, "/api/v1/restores", writer, []byte(`{"operation_id":"restore-ns-errors-op","reason":"x"}`), http.StatusForbidden, "forbidden"},
		{"create malformed body", http.MethodPost, "/api/v1/restores", requester, []byte(`{"operation_id":"restore-ns-errors-op","reason":"x"}{}`), http.StatusBadRequest, "invalid_request"},
		{"create platform without tenant", http.MethodPost, "/api/v1/restores", platform, []byte(`{"operation_id":"restore-ns-errors-op","reason":"x"}`), http.StatusBadRequest, "invalid_request"},
		{"create missing reason", http.MethodPost, "/api/v1/restores", requester, []byte(`{"operation_id":"restore-ns-errors-op"}`), http.StatusBadRequest, "invalid_request"},

		{"get unauthenticated", http.MethodGet, "/api/v1/restores/" + approveRun.ID, "", nil, http.StatusUnauthorized, "unauthorized"},
		{"get missing permission", http.MethodGet, "/api/v1/restores/" + approveRun.ID, writer, nil, http.StatusForbidden, "forbidden"},
		{"get platform without tenant", http.MethodGet, "/api/v1/restores/" + approveRun.ID, platform, nil, http.StatusBadRequest, "invalid_request"},
		{"get cross tenant", http.MethodGet, "/api/v1/restores/" + approveRun.ID, otherTenant, nil, http.StatusNotFound, "not_found"},
		{"get absent", http.MethodGet, "/api/v1/restores/absent-run", requester, nil, http.StatusNotFound, "not_found"},

		{"approve unauthenticated", http.MethodPost, "/api/v1/restores/" + approveRun.ID + "/approve", "", nil, http.StatusUnauthorized, "unauthorized"},
		{"approve missing permission", http.MethodPost, "/api/v1/restores/" + approveRun.ID + "/approve", writer, nil, http.StatusForbidden, "forbidden"},
		{"approve platform without tenant", http.MethodPost, "/api/v1/restores/" + approveRun.ID + "/approve", platform, nil, http.StatusBadRequest, "invalid_request"},
		{"approve cross tenant", http.MethodPost, "/api/v1/restores/" + approveRun.ID + "/approve", otherTenant, nil, http.StatusNotFound, "not_found"},
		{"approve applicant", http.MethodPost, "/api/v1/restores/" + approveRun.ID + "/approve", requester, nil, http.StatusForbidden, "forbidden"},

		{"reject unauthenticated", http.MethodPost, "/api/v1/restores/" + rejectRun.ID + "/reject", "", nil, http.StatusUnauthorized, "unauthorized"},
		{"reject missing permission", http.MethodPost, "/api/v1/restores/" + rejectRun.ID + "/reject", writer, nil, http.StatusForbidden, "forbidden"},
		{"reject platform without tenant", http.MethodPost, "/api/v1/restores/" + rejectRun.ID + "/reject", platform, nil, http.StatusBadRequest, "invalid_request"},
		{"reject cross tenant", http.MethodPost, "/api/v1/restores/" + rejectRun.ID + "/reject", otherTenant, nil, http.StatusNotFound, "not_found"},
		{"reject applicant", http.MethodPost, "/api/v1/restores/" + rejectRun.ID + "/reject", requester, nil, http.StatusForbidden, "forbidden"},

		{"list unauthenticated", http.MethodGet, "/api/v1/restores", "", nil, http.StatusUnauthorized, "unauthorized"},
		{"list missing permission", http.MethodGet, "/api/v1/restores", writer, nil, http.StatusForbidden, "forbidden"},
		{"list platform without tenant", http.MethodGet, "/api/v1/restores", platform, nil, http.StatusBadRequest, "invalid_request"},
		{"list invalid status", http.MethodGet, "/api/v1/restores?status=bogus", reader, nil, http.StatusBadRequest, "invalid_request"},
		{"list repeated filters", http.MethodGet, "/api/v1/restores?view=mine&view=all", reader, nil, http.StatusBadRequest, "invalid_request"},
		{"list actionable without manage", http.MethodGet, "/api/v1/restores?view=actionable", reader, nil, http.StatusForbidden, "forbidden"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			response := fixture.do(t, testCase.method, testCase.path, testCase.token, testCase.body)
			assertRestoreError(t, testCase.name, response, testCase.status, testCase.code)
		})
	}

	// A withdrawn caller is still answered with the no-store withdrawal
	// envelope; the fixture must not serve a cached/partial restore object.
	cancelled, cancel := context.WithCancel(testCtx)
	cancel()
	withdrawn := fixture.doHandler(t, cancelled, http.MethodGet, "/api/v1/restores/"+approveRun.ID, "dev:tenant-a:compliance", nil)
	assertRestoreError(t, "get caller withdrawal", withdrawn, statusClientClosedRequest, "client_cancelled", "restore-ns-errors-op", approveRun.ID)
	previewCancelled := fixture.doHandler(t, cancelled, http.MethodPost, "/api/v1/restores/preview", "dev:tenant-a:compliance", []byte(`{"operation_id":"restore-ns-errors-op"}`))
	assertRestoreError(t, "preview caller withdrawal", previewCancelled, statusClientClosedRequest, "client_cancelled", "restore-ns-errors-op")

	// Deterministic durable-write failures: create, approve and reject must
	// answer the original redacted 500 envelope and leave no success object or
	// decision fact behind.
	beforeFault := fixture.snapshotState(t)
	fixture.armed.Store(true)
	createFault := fixture.do(t, http.MethodPost, "/api/v1/restores", requester,
		[]byte(`{"operation_id":"restore-ns-errors-op","reason":"`+restoreNoStoreSecretReason+`"}`))
	assertRestoreError(t, "create control write failure", createFault, http.StatusInternalServerError, "internal_error", restoreNoStoreControlFault)
	approveFault := fixture.do(t, http.MethodPost, "/api/v1/restores/"+approveRun.ID+"/approve", decider, nil)
	assertRestoreError(t, "approve control write failure", approveFault, http.StatusInternalServerError, "internal_error", restoreNoStoreControlFault)
	rejectFault := fixture.do(t, http.MethodPost, "/api/v1/restores/"+rejectRun.ID+"/reject", decider, nil)
	assertRestoreError(t, "reject control write failure", rejectFault, http.StatusInternalServerError, "internal_error", restoreNoStoreControlFault)
	fixture.armed.Store(false)

	if afterFault := fixture.snapshotState(t); afterFault != beforeFault {
		t.Fatalf("failed restore writes changed control state: before=%+v after=%+v", beforeFault, afterFault)
	}
	pendingApprove := fixture.getRun(t, decider, approveRun.ID)
	if pendingApprove.Status != domain.RestoreStatusPendingApproval || pendingApprove.ApprovedBy != "" || pendingApprove.RejectedBy != "" {
		t.Fatalf("failed approval changed pending run: %+v", pendingApprove)
	}
	pendingReject := fixture.getRun(t, decider, rejectRun.ID)
	if pendingReject.Status != domain.RestoreStatusPendingApproval || pendingReject.ApprovedBy != "" || pendingReject.RejectedBy != "" {
		t.Fatalf("failed rejection changed pending run: %+v", pendingReject)
	}
	for _, action := range fixture.adminActions(t) {
		if action.Action == domain.AdminActionRestoreApproved || action.Action == domain.AdminActionRestoreRejected {
			t.Fatalf("failed restore decision recorded success fact: %+v", action)
		}
	}
}

type restoreNoStoreState struct {
	runs           int
	adminActions   int
	approvedFacts  int
	rejectedFacts  int
	restoreCreated int
	eventReads     int
	events         int
}

func (f *restoreNoStoreFixture) snapshotState(t *testing.T) restoreNoStoreState {
	t.Helper()
	state := restoreNoStoreState{}
	if err := f.store.ReadControl(testCtx, func(data *store.Snapshot) error {
		state.runs = len(data.RestoreRuns)
		state.adminActions = len(data.AdminActions)
		state.events = len(data.Events)
		for _, action := range data.AdminActions {
			switch action.Action {
			case domain.AdminActionRestoreApproved:
				state.approvedFacts++
			case domain.AdminActionRestoreRejected:
				state.rejectedFacts++
			case domain.AdminActionRestoreCreated:
				state.restoreCreated++
			case domain.AdminActionEventRead:
				state.eventReads++
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return state
}

func (f *restoreNoStoreFixture) adminActions(t *testing.T) []domain.AdminAction {
	t.Helper()
	var actions []domain.AdminAction
	if err := f.store.ReadControl(testCtx, func(data *store.Snapshot) error {
		actions = append(actions, data.AdminActions...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return actions
}

func (f *restoreNoStoreFixture) lastAdminAction(t *testing.T) domain.AdminAction {
	t.Helper()
	actions := f.adminActions(t)
	if len(actions) == 0 {
		t.Fatal("no admin actions recorded")
	}
	return actions[len(actions)-1]
}
