package grpcapi

import (
	"testing"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
)

func TestBatchUnattemptedIndicesAtCountBoundary(t *testing.T) {
	events := make([]*auditv1.EventEnvelope, MaxBatchEvents)
	events[MaxBatchEvents-1] = &auditv1.EventEnvelope{EventId: "last-event"}
	outcomes := appendUnattemptedOutcomes(nil, events, 0)
	if len(outcomes) != MaxBatchEvents {
		t.Fatalf("outcomes=%d", len(outcomes))
	}
	for index := int32(0); index < MaxBatchEvents; index++ {
		if outcomes[index].InputIndex != index || outcomes[index].Status != auditv1.WriteBatchOutcome_NOT_ATTEMPTED {
			t.Fatalf("incorrect suffix index or status at %d", index)
		}
	}
	if outcomes[MaxBatchEvents-1].EventId != "last-event" {
		t.Fatal("last event identity changed")
	}
	tail := appendUnattemptedOutcomes(nil, events, MaxBatchEvents-1)
	if len(tail) != 1 || tail[0].InputIndex != MaxBatchEvents-1 {
		t.Fatal("last-index suffix changed")
	}
	if got := appendUnattemptedOutcomes(tail, events, MaxBatchEvents); len(got) != 1 {
		t.Fatal("empty suffix appended an extra outcome")
	}
}
