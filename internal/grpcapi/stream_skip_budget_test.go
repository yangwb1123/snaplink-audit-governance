package grpcapi

import (
	"io"
	"log"
	"strings"
	"testing"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCWriteStreamSkipBudgetTerminatesAndBoundsLogs(t *testing.T) {
	var logged strings.Builder
	st, client, ctx := newGRPCHarnessOpts(t, grpcHarnessOptions{logger: log.New(&logged, "", 0)})
	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, MaxStreamSkips+1)
	for index := 0; index <= MaxStreamSkips; index++ {
		eventID := "grpc-stream-budget-" + string(rune('a'+index%26)) + "-" + strings.Repeat("x", index)
		ids = append(ids, eventID)
		sendErr := stream.Send(&auditv1.WriteRequest{Event: overCapStreamEvent(eventID)})
		if sendErr != nil {
			if index < MaxStreamSkips {
				t.Fatalf("stream stopped before skip budget at index %d: %v", index, sendErr)
			}
			break
		}
	}
	if _, err := stream.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("terminal stream code=%v, want ResourceExhausted", status.Code(err))
	}
	if got := strings.Count(logged.String(), "rejecting over-cap message"); got != MaxStreamSkips {
		t.Fatalf("over-cap log count=%d, want %d", got, MaxStreamSkips)
	}
	if !strings.Contains(logged.String(), "client_id=\"crm\"") || !strings.Contains(logged.String(), "tenant=\"tenant-a\"") {
		t.Fatalf("skip log lacks principal attribution: %q", logged.String())
	}
	assertNoFramedKeys(t, st, ids...)
}

func TestGRPCWriteStreamSkipBudgetBoundaryRecovers(t *testing.T) {
	var logged strings.Builder
	st, client, ctx := newGRPCHarnessOpts(t, grpcHarnessOptions{logger: log.New(&logged, "", 0)})
	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	overIDs := make([]string, 0, MaxStreamSkips)
	for index := 0; index < MaxStreamSkips-1; index++ {
		eventID := "grpc-stream-boundary-over-" + string(rune('a'+index%26)) + "-" + strings.Repeat("x", index)
		overIDs = append(overIDs, eventID)
		if err := stream.Send(&auditv1.WriteRequest{Event: overCapStreamEvent(eventID)}); err != nil {
			t.Fatal(err)
		}
	}
	valid := testProtoEvent("grpc-stream-boundary-valid", "crm")
	if err := stream.Send(&auditv1.WriteRequest{Event: valid}); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	receipt, err := stream.Recv()
	if err != nil || receipt.GetEventId() != valid.GetEventId() {
		t.Fatalf("receipt after %d skips=%v err=%v, want valid receipt", MaxStreamSkips-1, receipt, err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("stream close err=%v, want io.EOF", err)
	}
	if got := strings.Count(logged.String(), "rejecting over-cap message"); got != MaxStreamSkips-1 {
		t.Fatalf("over-cap log count=%d, want %d", got, MaxStreamSkips-1)
	}
	assertNoFramedKeys(t, st, overIDs...)
	if err := st.Read(func(data *store.Snapshot) error {
		if _, exists := data.Events[store.EventKey("tenant-a", valid.GetEventId())]; !exists {
			t.Fatal("valid event after skips was not persisted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGRPCWriteStreamSkipBudgetNilLogger(t *testing.T) {
	st, client, ctx := newGRPCHarness(t)
	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index <= MaxStreamSkips; index++ {
		sendErr := stream.Send(&auditv1.WriteRequest{Event: overCapStreamEvent("grpc-stream-nil-budget-" + string(rune('a'+index%26)))})
		if sendErr != nil && index < MaxStreamSkips {
			t.Fatalf("nil logger stream stopped early at %d: %v", index, sendErr)
		}
	}
	if _, err := stream.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("nil logger terminal code=%v, want ResourceExhausted", status.Code(err))
	}
	assertNoFramedKeys(t, st)
}

func TestGRPCWriteStreamSkipsDoNotConsumeQuota(t *testing.T) {
	st, client, ctx := newGRPCHarness(t)
	if err := st.Update(func(data *store.Snapshot) error {
		tenant := data.Tenants["tenant-a"]
		tenant.EventsPerSecond = 1
		tenant.Burst = 0
		data.Tenants["tenant-a"] = tenant
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		if err := stream.Send(&auditv1.WriteRequest{Event: overCapStreamEvent("grpc-stream-quota-over-" + string(rune('a'+index)))}); err != nil {
			t.Fatal(err)
		}
	}
	valid := testProtoEvent("grpc-stream-quota-valid", "crm")
	quotaRejected := testProtoEvent("grpc-stream-quota-rejected", "crm")
	if err := stream.Send(&auditv1.WriteRequest{Event: valid}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&auditv1.WriteRequest{Event: quotaRejected}); err != nil {
		t.Fatal(err)
	}
	receipt, err := stream.Recv()
	if err != nil || receipt.GetEventId() != valid.GetEventId() {
		t.Fatalf("valid receipt=%v err=%v, want valid event receipt", receipt, err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("quota rejection code=%v, want ResourceExhausted", status.Code(err))
	}
	assertNoFramedKeys(t, st, "grpc-stream-quota-over-a", "grpc-stream-quota-over-b", "grpc-stream-quota-over-c", quotaRejected.GetEventId())
	if err := st.Read(func(data *store.Snapshot) error {
		if _, exists := data.Events[store.EventKey("tenant-a", valid.GetEventId())]; !exists {
			t.Fatal("valid event was not persisted after skipped messages")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func overCapStreamEvent(eventID string) *auditv1.EventEnvelope {
	event := testProtoEvent(eventID, "crm")
	event.Reason = strings.Repeat("r", MaxEnvelopeFieldBytes+1)
	return event
}
