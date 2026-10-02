package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Caller-withdrawal classification on the gRPC ingest surface (AC-3).
//
// toStatus answered every unmatched error with codes.Internal, which gRPC
// clients do NOT retry, while the transport's own rejectionCode vocabulary
// already reserved "canceled" and "deadline_exceeded". A caller withdrawal is
// now codes.Canceled / codes.DeadlineExceeded with a fixed message and no
// internal-fault log line.
//
// The load-bearing invariant is the ORDERING: store.ErrOperationDeadline is
// wrapped by Store.end as fmt.Errorf("%w: %w", ErrOperationDeadline, err) with
// err carrying the deadline error, so it also satisfies errors.Is(err,
// context.DeadlineExceeded). The withdrawal arms must stay after it, or the
// documented retryable Unavailable would silently become a caller withdrawal.

// TestToStatusCallerWithdrawal is the table over toStatus plus rejectionCode.
func TestToStatusCallerWithdrawal(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantCode   codes.Code
		wantMsg    string
		wantRejTok string
	}{
		{
			name:       "cancelled-bare",
			err:        context.Canceled,
			wantCode:   codes.Canceled,
			wantMsg:    "client cancelled the request",
			wantRejTok: "canceled",
		},
		{
			name:       "cancelled-wrapped",
			err:        fmt.Errorf("get s3://audit-bucket/events/tenant-a/evt-1.json: %w", context.Canceled),
			wantCode:   codes.Canceled,
			wantMsg:    "client cancelled the request",
			wantRejTok: "canceled",
		},
		{
			name:       "caller-deadline-bare",
			err:        context.DeadlineExceeded,
			wantCode:   codes.DeadlineExceeded,
			wantMsg:    "client request deadline exceeded",
			wantRejTok: "deadline_exceeded",
		},
		{
			name:       "caller-deadline-wrapped",
			err:        fmt.Errorf("archived event evt-1: %w", context.DeadlineExceeded),
			wantCode:   codes.DeadlineExceeded,
			wantMsg:    "client request deadline exceeded",
			wantRejTok: "deadline_exceeded",
		},
		{
			// Ordering proof: the composite store classification also matches
			// context.DeadlineExceeded and must stay Unavailable.
			name:       "store-deadline-wrapping-a-deadline-cause",
			err:        fmt.Errorf("%w: %w", store.ErrOperationDeadline, context.DeadlineExceeded),
			wantCode:   codes.Unavailable,
			wantMsg:    "store operation deadline exceeded; retry the request",
			wantRejTok: "unavailable",
		},
		{
			name:       "snapshot-conflict",
			err:        store.ErrSnapshotConflict,
			wantCode:   codes.Unavailable,
			wantMsg:    "state snapshot changed concurrently; retry the request",
			wantRejTok: "unavailable",
		},
		{
			// Default arm intact: a genuinely unmatched error is still an
			// internal fault with the fixed redacted text.
			name:       "unmatched-internal",
			err:        errors.New("marker-path /var/lib/audit/state.json: permission denied"),
			wantCode:   codes.Internal,
			wantMsg:    "internal server error",
			wantRejTok: "internal",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			converted := toStatus(tc.err)
			if got := status.Code(converted); got != tc.wantCode {
				t.Fatalf("code=%v, want %v", got, tc.wantCode)
			}
			message := status.Convert(converted).Message()
			if message != tc.wantMsg {
				t.Fatalf("message=%q, want the fixed %q", message, tc.wantMsg)
			}
			if got := rejectionCode(converted); got != tc.wantRejTok {
				t.Fatalf("rejection_code=%q, want %q", got, tc.wantRejTok)
			}
			if tc.wantCode != codes.Internal && strings.Contains(message, "/var/lib/audit") {
				t.Fatalf("backend detail leaked into a non-internal status: %q", message)
			}
		})
	}
}

// TestToStatusNeverAnswersInternalForWithdrawal is the explicit AC-3 claim:
// codes.Internal is no longer reachable for a cancellation, while the
// redaction of the default arm is unchanged.
func TestToStatusNeverAnswersInternalForWithdrawal(t *testing.T) {
	for _, err := range []error{
		context.Canceled,
		fmt.Errorf("archived event evt-1: %w", context.Canceled),
		context.DeadlineExceeded,
		fmt.Errorf("get s3://bucket/key: %w", context.DeadlineExceeded),
	} {
		if got := status.Code(toStatus(err)); got == codes.Internal {
			t.Fatalf("toStatus(%v)=Internal, want a caller-withdrawal code", err)
		}
	}
}

// TestWithdrawalMessagesMatchTheHTTPTransport pins cross-transport equality of
// the two fixed strings by literal assertion (there is deliberately no shared
// package: see the constant comment in server.go).
func TestWithdrawalMessagesMatchTheHTTPTransport(t *testing.T) {
	if canceledMessage != "client cancelled the request" {
		t.Fatalf("canceledMessage=%q, want %q", canceledMessage, "client cancelled the request")
	}
	if deadlineExceededMessage != "client request deadline exceeded" {
		t.Fatalf("deadlineExceededMessage=%q, want %q", deadlineExceededMessage, "client request deadline exceeded")
	}
	if status.Code(toStatus(context.Canceled)) != codes.Canceled ||
		status.Convert(toStatus(context.Canceled)).Message() != canceledMessage {
		t.Fatal("codes.Canceled must carry canceledMessage verbatim")
	}
	if status.Code(toStatus(context.DeadlineExceeded)) != codes.DeadlineExceeded ||
		status.Convert(toStatus(context.DeadlineExceeded)).Message() != deadlineExceededMessage {
		t.Fatal("codes.DeadlineExceeded must carry deadlineExceededMessage verbatim")
	}
}

// TestStatusErrorDoesNotLogWithdrawalAsInternal: a withdrawal is neither logged
// as an internal fault nor given backend text. A scanner can withdraw thousands
// of requests a minute; filing each one as a server error would bury the faults
// that matter. The genuinely-internal control still logs, so the assertion is
// about the withdrawal and not about a silently disabled logger.
func TestStatusErrorDoesNotLogWithdrawalAsInternal(t *testing.T) {
	var logged strings.Builder
	server := &Server{Logger: log.New(&logged, "", 0)}

	converted := server.statusError(fmt.Errorf("get %s: %w", "s3://audit-bucket/events/tenant-a/evt-1.json", context.Canceled))
	if status.Code(converted) != codes.Canceled {
		t.Fatalf("code=%v, want Canceled", status.Code(converted))
	}
	if message := status.Convert(converted).Message(); message != canceledMessage {
		t.Fatalf("message=%q, want the fixed %q", message, canceledMessage)
	}
	if logged.Len() != 0 {
		t.Fatalf("withdrawal logged as a server fault: %q", logged.String())
	}

	converted = server.statusError(fmt.Errorf("get %s: %w", "s3://audit-bucket/key", context.DeadlineExceeded))
	if status.Code(converted) != codes.DeadlineExceeded {
		t.Fatalf("code=%v, want DeadlineExceeded", status.Code(converted))
	}
	if logged.Len() != 0 {
		t.Fatalf("caller deadline logged as a server fault: %q", logged.String())
	}

	// Control: a genuine internal fault is still logged with its detail.
	converted = server.statusError(errors.New("marker-path /var/lib/audit/state.json: permission denied"))
	if status.Code(converted) != codes.Internal {
		t.Fatalf("control code=%v, want Internal", status.Code(converted))
	}
	if !strings.Contains(logged.String(), "marker-path") {
		t.Fatalf("control detail not logged: %q", logged.String())
	}
}
