package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/store"
)

// Caller-withdrawal classification at the HTTP error boundary (AC-1).
//
// A withdrawn request — context.Canceled, or a caller-context
// context.DeadlineExceeded that reaches the boundary as the sentinel — used
// to fall through statusForError's default arm to 500 internal_error and was
// counted into audit_http_errors_total, the documented availability/error-
// budget source. It is now its own non-5xx class (499) with a stable code and
// a fixed, redacted message.
//
// Two invariants are load-bearing and pinned here:
//
//  1. the withdrawal class must NOT be reclassified from error text: a
//     client-library timeout (*url.Error) is not errors.Is(
//     context.DeadlineExceeded) and must stay a fail-closed 500;
//  2. the withdrawal arms are the LAST arms, after store.ErrOperationDeadline,
//     so the documented retryable 503 store_timeout is never stolen.

// withdrawalLeakTokens are the internal identifiers a wrapped withdrawal
// cause can carry. A 499 is below the >= 500 redaction threshold, so the
// fixed message is the only thing standing between them and the client.
var withdrawalLeakTokens = []string{"evt-", "tenant-", "events/", "postgres", "archive", "http://", "minio"}

// TestCallerWithdrawalStatusMapping is the table over statusForError and
// errorBody: bare and %w-wrapped cancellations plus a caller deadline all map
// to 499 with a stable withdrawal code and the fixed message, and the envelope
// still carries request_id.
func TestCallerWithdrawalStatusMapping(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/events/evt-1", nil)
	request = request.WithContext(context.WithValue(request.Context(), requestIDKey, "req-withdrawal"))
	cases := []struct {
		name     string
		err      error
		wantCode string
		wantMsg  string
	}{
		{"cancelled-bare", context.Canceled, "client_cancelled", cancelledMessage},
		{
			"cancelled-wrapped-archive-fallback",
			fmt.Errorf("archived event evt-1: %w", context.Canceled),
			"client_cancelled",
			cancelledMessage,
		},
		{
			"cancelled-wrapped-object-read",
			fmt.Errorf("get %s: %w", "events/tenant-a/stream/0000000001-evt-1.json", context.Canceled),
			"client_cancelled",
			cancelledMessage,
		},
		{"caller-deadline-bare", context.DeadlineExceeded, "client_deadline_exceeded", deadlineExceededMessage},
		{
			"caller-deadline-wrapped",
			fmt.Errorf("archived event evt-1: %w", context.DeadlineExceeded),
			"client_deadline_exceeded",
			deadlineExceededMessage,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status := statusForError(tc.err)
			if status != statusClientClosedRequest {
				t.Fatalf("status=%d, want %d for a caller withdrawal", status, statusClientClosedRequest)
			}
			if status >= 500 {
				t.Fatalf("withdrawal status %d is a 5xx and would be counted as an availability error", status)
			}
			body := errorBody(status, tc.err, request)
			envelope, ok := body["error"].(map[string]any)
			if !ok {
				t.Fatalf("error envelope missing: %v", body)
			}
			if envelope["code"] != tc.wantCode {
				t.Fatalf("code=%v, want %s", envelope["code"], tc.wantCode)
			}
			if envelope["message"] != tc.wantMsg {
				t.Fatalf("message=%v, want the fixed %q", envelope["message"], tc.wantMsg)
			}
			message, _ := envelope["message"].(string)
			for _, token := range withdrawalLeakTokens {
				if strings.Contains(message, token) {
					t.Fatalf("withdrawal body leaked %q: %q", token, message)
				}
			}
			if envelope["request_id"] != "req-withdrawal" {
				t.Fatalf("request_id=%v, want req-withdrawal", envelope["request_id"])
			}
		})
	}
}

// TestWithdrawalMappingDoesNotStealStoreClasses is the F1 ordering proof. The
// store wraps an exhausted bound as fmt.Errorf("%w: %w",
// ErrOperationDeadline, err) with err carrying the deadline error, so such an
// error ALSO satisfies errors.Is(err, context.DeadlineExceeded). Withdrawing
// the arms ahead of the store arm would silently turn the documented
// retryable 503 into a caller withdrawal and stop clients retrying an
// unknown-outcome write.
func TestWithdrawalMappingDoesNotStealStoreClasses(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/api/v1/events", nil)
	cases := []struct {
		name     string
		err      error
		wantCode string
	}{
		{"store-deadline-bare", store.ErrOperationDeadline, "store_timeout"},
		{
			"store-deadline-wrapping-a-deadline-cause",
			fmt.Errorf("%w: %w", store.ErrOperationDeadline, context.DeadlineExceeded),
			"store_timeout",
		},
		{"snapshot-conflict-bare", store.ErrSnapshotConflict, "snapshot_conflict"},
		{
			"snapshot-conflict-wrapped",
			fmt.Errorf("save state snapshot: %w: /var/lib/audit/state.json: version mismatch", store.ErrSnapshotConflict),
			"snapshot_conflict",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status := statusForError(tc.err)
			if status != http.StatusServiceUnavailable {
				t.Fatalf("status=%d, want 503 for the retryable store class", status)
			}
			body := errorBody(status, tc.err, request)
			envelope, ok := body["error"].(map[string]any)
			if !ok {
				t.Fatalf("error envelope missing: %v", body)
			}
			if envelope["code"] != tc.wantCode {
				t.Fatalf("code=%v, want %s", envelope["code"], tc.wantCode)
			}
			if envelope["message"] != "internal server error" {
				t.Fatalf("message=%v, want the unchanged redacted >=500 text", envelope["message"])
			}
		})
	}
}

// TestTimeoutShapedErrorStaysInternal is D5: classification is by sentinel
// identity only. A client-library timeout whose TEXT says "context deadline
// exceeded" is not the sentinel, so it must stay a fail-closed 500 rather than
// be reclassified by a string match.
func TestTimeoutShapedErrorStaysInternal(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/exports/job-1/download", nil)
	err := &url.Error{
		Op:  "Get",
		URL: "https://minio:9000/audit-bucket/events/tenant-a/evt-1.json",
		Err: errors.New("context deadline exceeded (Client.Timeout exceeded while awaiting headers)"),
	}
	status := statusForError(err)
	if status != http.StatusInternalServerError {
		t.Fatalf("status=%d, want 500 for a timeout-shaped but non-sentinel error", status)
	}
	if kind := classifyWithdrawal(err); kind != withdrawalNone {
		t.Fatalf("classifyWithdrawal=%v, want withdrawalNone", kind)
	}
	body := errorBody(status, err, request)
	envelope, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("error envelope missing: %v", body)
	}
	if envelope["code"] != "internal_error" || envelope["message"] != "internal server error" {
		t.Fatalf("envelope=%v, want internal_error / internal server error", envelope)
	}
}

// TestWithdrawalStatusAndKindAgree pins the classifier, the two literal
// statusForError arms and the counter funnel to one another: a withdrawal kind
// must always yield 499, a non-withdrawal error must never, and a store
// sentinel that also satisfies errors.Is(err, context.DeadlineExceeded) must
// still yield the retryable 503.
func TestWithdrawalStatusAndKindAgree(t *testing.T) {
	cases := []struct {
		err        error
		wantKind   withdrawalKind
		wantStatus int
	}{
		{context.Canceled, withdrawalCancelled, statusClientClosedRequest},
		{fmt.Errorf("archived event evt-1: %w", context.Canceled), withdrawalCancelled, statusClientClosedRequest},
		{context.DeadlineExceeded, withdrawalDeadline, statusClientClosedRequest},
		{fmt.Errorf("get exports/job-1.jsonl: %w", context.DeadlineExceeded), withdrawalDeadline, statusClientClosedRequest},
		{fmt.Errorf("%w: %w", store.ErrOperationDeadline, context.DeadlineExceeded), withdrawalDeadline, http.StatusServiceUnavailable},
		{store.ErrSnapshotConflict, withdrawalNone, http.StatusServiceUnavailable},
		{errors.New("open /var/lib/audit/state.json.tmp: is a directory"), withdrawalNone, http.StatusInternalServerError},
	}
	for _, tc := range cases {
		if got := classifyWithdrawal(tc.err); got != tc.wantKind {
			t.Fatalf("classifyWithdrawal(%v)=%v, want %v", tc.err, got, tc.wantKind)
		}
		if status := statusForError(tc.err); status != tc.wantStatus {
			t.Fatalf("statusForError(%v)=%d, want %d", tc.err, status, tc.wantStatus)
		}
	}
}

// TestWithdrawalIsNotCountedAsAvailabilityError is the counter half of AC-1:
// driving writeError with a 499 must move only the withdrawal counters and
// leave audit_http_errors_total (the availability signal) untouched, while a
// 500 must move only the availability counter. The withdrawal body is also
// non-cacheable, exactly like a 5xx.
func TestWithdrawalIsNotCountedAsAvailabilityError(t *testing.T) {
	s := &Server{}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/exports/job-1/download", nil)
	s.writeError(httptest.NewRecorder(), request, statusClientClosedRequest,
		fmt.Errorf("get exports/job-1.jsonl: %w", context.Canceled))
	if s.errorCount.Load() != 0 {
		t.Fatalf("audit_http_errors_total=%d after a 499, want 0 (client churn is not an availability error)", s.errorCount.Load())
	}
	if s.withdrawalCancelCount.Load() != 1 {
		t.Fatalf("withdrawal cancelled=%d, want 1", s.withdrawalCancelCount.Load())
	}
	if s.withdrawalDeadlineCount.Load() != 0 {
		t.Fatalf("withdrawal deadline=%d, want 0", s.withdrawalDeadlineCount.Load())
	}

	s.writeError(httptest.NewRecorder(), request, statusClientClosedRequest, context.DeadlineExceeded)
	if s.errorCount.Load() != 0 {
		t.Fatalf("audit_http_errors_total=%d after a deadline 499, want 0", s.errorCount.Load())
	}
	if s.withdrawalDeadlineCount.Load() != 1 {
		t.Fatalf("withdrawal deadline=%d, want 1", s.withdrawalDeadlineCount.Load())
	}

	recorder := httptest.NewRecorder()
	s.writeError(recorder, request, http.StatusInternalServerError, errors.New("state.json: permission denied"))
	if s.errorCount.Load() != 1 {
		t.Fatalf("audit_http_errors_total=%d after a 500, want 1", s.errorCount.Load())
	}
	if s.withdrawalCancelCount.Load() != 1 || s.withdrawalDeadlineCount.Load() != 1 {
		t.Fatalf("withdrawal counters moved on a 500: cancelled=%d deadline=%d",
			s.withdrawalCancelCount.Load(), s.withdrawalDeadlineCount.Load())
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("5xx Cache-Control=%q, want no-store", got)
	}
}

// TestWithdrawalResponseIsNotCacheable: a 499 body is a fixed diagnostic for a
// request nobody is waiting for, so it must not be stored by an intermediary.
func TestWithdrawalResponseIsNotCacheable(t *testing.T) {
	s := &Server{}
	recorder := httptest.NewRecorder()
	s.writeError(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/exports/job-1/download", nil),
		statusClientClosedRequest, context.Canceled)
	if recorder.Code != statusClientClosedRequest {
		t.Fatalf("recorder status=%d, want %d", recorder.Code, statusClientClosedRequest)
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("499 Cache-Control=%q, want no-store", got)
	}
}

// TestWithdrawalMessagesMatchTheGRPCTransport pins cross-transport equality of
// the two fixed strings by literal assertion (there is deliberately no shared
// package: see the constant comment in authz.go).
func TestWithdrawalMessagesMatchTheGRPCTransport(t *testing.T) {
	if cancelledMessage != "client cancelled the request" {
		t.Fatalf("cancelledMessage=%q, want %q", cancelledMessage, "client cancelled the request")
	}
	if deadlineExceededMessage != "client request deadline exceeded" {
		t.Fatalf("deadlineExceededMessage=%q, want %q", deadlineExceededMessage, "client request deadline exceeded")
	}
	if statusClientClosedRequest != 499 {
		t.Fatalf("statusClientClosedRequest=%d, want 499", statusClientClosedRequest)
	}
}
