package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/kafka"
)

func TestValidateReplayTopology(t *testing.T) {
	for _, test := range []struct {
		name     string
		accepted string
		dlq      string
		group    string
		wantErr  string
	}{
		{name: "valid", accepted: kafka.TopicAccepted, dlq: kafka.TopicDLQ, group: "audit-replay"},
		{name: "blank group", accepted: kafka.TopicAccepted, dlq: kafka.TopicDLQ, group: " \t", wantErr: "consumer group"},
		{name: "blank accepted topic", accepted: " ", dlq: kafka.TopicDLQ, group: "audit-replay", wantErr: "accepted topic"},
		{name: "blank DLQ topic", accepted: kafka.TopicAccepted, dlq: "", group: "audit-replay", wantErr: "DLQ topic"},
		{name: "self loop", accepted: "same", dlq: "same", group: "audit-replay", wantErr: "source topic"},
		{name: "stock topic collision", accepted: "custom-source", dlq: kafka.TopicLedgered, group: "audit-replay", wantErr: "DLQ topic"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := kafka.ValidateReplayTopology(test.accepted, test.dlq, test.group)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateReplayTopology() error = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("ValidateReplayTopology() error = %v, want diagnostic containing %q", err, test.wantErr)
			}
		})
	}
}

// T7 (AC-2/AC-D2): metricsText renders the split resolution counters in the
// pinned order — the five original lines keep their names and positions
// (with replayed narrowed to successful re-publishes), the renamed
// permanent line keeps its position (REQ-PERM-3), the new observational
// attempts_exhausted line sits at the end of the resolution band (after
// unparsable_marks, before republish_failures), and the auth-blocked
// counter + backlog gauge follow pending and malformed is appended last
// (campaign fail-fast-or-warn-on-empty-rotated-ingest-token). The golden text is the
// contract: a reorder or a name change fails this test.
func TestMetricsTextGolden(t *testing.T) {
	metrics := kafka.ReplayerMetrics{
		DLQRecords:         1,
		Malformed:          12,
		AcceptedScanned:    2,
		Replayed:           3,
		Permanent:          4,
		Unresolvable:       5,
		UnparsableMarks:    6,
		AttemptsExhausted:  7,
		RepublishFailures:  8,
		Pending:            9,
		AuthBlocked:        10,
		AuthBlockedPending: 11,
	}
	want := "" +
		"audit_dlq_records_total 1\n" +
		"audit_dlq_accepted_scanned_total 2\n" +
		"audit_dlq_replayed_total 3\n" +
		"audit_dlq_permanent_total 4\n" +
		"audit_dlq_unresolvable_total 5\n" +
		"audit_dlq_unparsable_marks_total 6\n" +
		"audit_dlq_attempts_exhausted_total 7\n" +
		"audit_dlq_republish_failures_total 8\n" +
		"audit_dlq_pending 9\n" +
		"audit_dlq_auth_blocked_total 10\n" +
		"audit_dlq_auth_blocked 11\n" +
		"audit_dlq_malformed_total 12\n"
	if got := metricsText(metrics); got != want {
		t.Fatalf("metricsText mismatch:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

// T7b (AC-2): the renderer is deterministic for an empty snapshot and the
// replayed line reflects only the Replayed field.
func TestMetricsTextEmptySnapshot(t *testing.T) {
	want := "" +
		"audit_dlq_records_total 0\n" +
		"audit_dlq_accepted_scanned_total 0\n" +
		"audit_dlq_replayed_total 0\n" +
		"audit_dlq_permanent_total 0\n" +
		"audit_dlq_unresolvable_total 0\n" +
		"audit_dlq_unparsable_marks_total 0\n" +
		"audit_dlq_attempts_exhausted_total 0\n" +
		"audit_dlq_republish_failures_total 0\n" +
		"audit_dlq_pending 0\n" +
		"audit_dlq_auth_blocked_total 0\n" +
		"audit_dlq_auth_blocked 0\n" +
		"audit_dlq_malformed_total 0\n"
	if got := metricsText(kafka.ReplayerMetrics{}); got != want {
		t.Fatalf("metricsText empty mismatch:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

// AC-2: the /metrics handler renders live counter snapshots through
// metricsText — exact per-code value lines, the renamed permanent line, the
// new attempts_exhausted and malformed lines, the pinned 12-line body, and the absence of
// the legacy audit_dlq_permanent_rejections_total name. The metrics-source
// closure keeps the handler testable with exact values without exported
// counter setters; serveMetrics passes replayer.Metrics (mount semantics
// unchanged: daemon-only, -once exposes no /metrics).
func TestMetricsEndpointServesPerCodeLines(t *testing.T) {
	handler := metricsHandler(func() kafka.ReplayerMetrics {
		return kafka.ReplayerMetrics{
			Replayed: 3, Permanent: 4, Unresolvable: 6, AttemptsExhausted: 5, RepublishFailures: 1, Malformed: 7,
		}
	})
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/plain; version=0.0.4" {
		t.Fatalf("Content-Type=%q, want text/plain; version=0.0.4", ct)
	}
	body := rec.Body.String()
	for _, line := range []string{
		"audit_dlq_replayed_total 3\n",
		"audit_dlq_permanent_total 4\n",
		"audit_dlq_unresolvable_total 6\n",
		"audit_dlq_attempts_exhausted_total 5\n",
		"audit_dlq_republish_failures_total 1\n",
		"audit_dlq_malformed_total 7\n",
	} {
		if !strings.Contains(body, line) {
			t.Fatalf("body missing exact line %q:\n%s", line, body)
		}
	}
	if strings.Contains(body, "audit_dlq_permanent_rejections_total") {
		t.Fatalf("legacy metric name must be absent from the exposition:\n%s", body)
	}
	if got := strings.Count(body, "\n"); got != 12 {
		t.Fatalf("body lines=%d, want 12:\n%s", got, body)
	}
}
