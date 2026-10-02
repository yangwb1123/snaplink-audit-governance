package grpcapi

// A3 on the gRPC side: a store operation that exhausted its deadline is the
// same transient, idempotently-retryable class as an exhausted optimistic-lock
// retry, so it must map to codes.Unavailable next to store.ErrSnapshotConflict
// with a FIXED message. A fixed message matters because the wrapped cause can
// carry a DSN, a pgx detail or a state path.

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/snaplink/audit-governance/internal/store"
)

func TestToStatusOperationDeadline(t *testing.T) {
	for name, err := range map[string]error{
		"bare": store.ErrOperationDeadline,
		"wrapped": fmt.Errorf("store operation deadline exceeded: %w: context deadline exceeded "+
			"while connecting to postgres://audit:hunter2@db:5432/audit", store.ErrOperationDeadline),
		"wrapped-sentinel": fmt.Errorf("save state snapshot: %w: context deadline exceeded",
			store.ErrOperationDeadline),
	} {
		t.Run(name, func(t *testing.T) {
			converted := toStatus(err)
			if code := status.Code(converted); code != codes.Unavailable {
				t.Fatalf("code=%s, want %s (the retryable-transient class)", code, codes.Unavailable)
			}
			if message := status.Convert(converted).Message(); message != storeDeadlineMessage {
				t.Fatalf("message=%q, want the fixed constant %q", message, storeDeadlineMessage)
			}
			if strings.Contains(status.Convert(converted).Message(), "hunter2") ||
				strings.Contains(status.Convert(converted).Message(), "postgres://") {
				t.Fatalf("backend detail crossed the boundary: %q", status.Convert(converted).Message())
			}
			if token := rejectionCode(converted); token != "unavailable" {
				t.Fatalf("rejection_code=%q, want unavailable", token)
			}
		})
	}
	// The sentinel must NOT fall through to the internal default: codes.Internal
	// is not retried by the standard at-least-once policy.
	if code := status.Code(toStatus(fmt.Errorf("wrapped: %w", store.ErrOperationDeadline))); code == codes.Internal {
		t.Fatal("a store deadline must never be reported as codes.Internal")
	}
	// A caller cancellation is a different thing and stays unmapped (the store
	// propagates context.Canceled unchanged).
	if code := status.Code(toStatus(errors.New("plain"))); code != codes.Internal {
		t.Fatalf("unmapped error code=%s, want codes.Internal", code)
	}
}
