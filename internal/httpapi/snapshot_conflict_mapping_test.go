package httpapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/store"
)

// TestSnapshotConflictStatusMappingUnchanged pins the HTTP half of the
// retryable-transient contract: an exhausted optimistic-lock retry is 503
// with the machine code snapshot_conflict, which is the classification the
// gRPC ingest surface mirrors with codes.Unavailable. A wrapped sentinel (the
// shape a backend can produce) must classify identically, and the >= 500
// redaction keeps internal detail out of the body while the code survives.
func TestSnapshotConflictStatusMappingUnchanged(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/api/v1/events", nil)
	for name, err := range map[string]error{
		"bare":    store.ErrSnapshotConflict,
		"wrapped": fmt.Errorf("save state snapshot: %w: /var/lib/audit/state.json: version mismatch", store.ErrSnapshotConflict),
	} {
		t.Run(name, func(t *testing.T) {
			if status := statusForError(err); status != http.StatusServiceUnavailable {
				t.Fatalf("status=%d, want %d for an exhausted optimistic-lock retry", status, http.StatusServiceUnavailable)
			}
			body := errorBody(statusForError(err), err, request)
			envelope, ok := body["error"].(map[string]any)
			if !ok {
				t.Fatalf("error envelope missing: %v", body)
			}
			if envelope["code"] != "snapshot_conflict" {
				t.Fatalf("code=%v, want snapshot_conflict", envelope["code"])
			}
			if envelope["message"] != "internal server error" {
				t.Fatalf("message=%v, want the redacted >=500 text", envelope["message"])
			}
			if text, _ := envelope["message"].(string); strings.Contains(text, "state.json") {
				t.Fatalf("backend detail leaked in the body: %q", text)
			}
		})
	}
}
