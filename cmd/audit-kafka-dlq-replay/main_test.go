package main

import (
	"testing"

	"github.com/snaplink/audit-governance/internal/kafka"
)

// T7 (AC-2): metricsText renders the split resolution counters in the pinned
// order — the five original lines keep their names and positions (with
// replayed narrowed to successful re-publishes) and the three new counters
// sit between replayed and republish_failures. The golden text is the
// contract: a reorder or a name change fails this test.
func TestMetricsTextGolden(t *testing.T) {
	metrics := kafka.ReplayerMetrics{
		DLQRecords:          1,
		AcceptedScanned:     2,
		Replayed:            3,
		PermanentRejections: 4,
		Unresolvable:        5,
		UnparsableMarks:     6,
		RepublishFailures:   7,
		Pending:             8,
	}
	want := "" +
		"audit_dlq_records_total 1\n" +
		"audit_dlq_accepted_scanned_total 2\n" +
		"audit_dlq_replayed_total 3\n" +
		"audit_dlq_permanent_rejections_total 4\n" +
		"audit_dlq_unresolvable_total 5\n" +
		"audit_dlq_unparsable_marks_total 6\n" +
		"audit_dlq_republish_failures_total 7\n" +
		"audit_dlq_pending 8\n"
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
		"audit_dlq_permanent_rejections_total 0\n" +
		"audit_dlq_unresolvable_total 0\n" +
		"audit_dlq_unparsable_marks_total 0\n" +
		"audit_dlq_republish_failures_total 0\n" +
		"audit_dlq_pending 0\n"
	if got := metricsText(kafka.ReplayerMetrics{}); got != want {
		t.Fatalf("metricsText empty mismatch:\ngot:\n%s\nwant:\n%s", got, want)
	}
}
