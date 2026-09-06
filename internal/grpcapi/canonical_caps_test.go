package grpcapi

import (
	"fmt"
	"io"
	"strings"
	"testing"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestGRPCCanonicalJSONCapsAcrossRPCs covers the three gRPC ingest methods
// with before_json whose compact wire spelling fits the field cap but whose
// canonical spelling does not. HTML-sensitive characters are intentionally
// left unescaped on the wire; CanonicalJSON escapes them before measuring.
func TestGRPCCanonicalJSONCapsAcrossRPCs(t *testing.T) {
	st, client, ctx := newGRPCHarness(t)
	chars := []struct {
		name  string
		value string
	}{
		{"less-than", "<"},
		{"greater-than", ">"},
		{"ampersand", "&"},
	}

	for _, tc := range chars {
		t.Run("Write/"+tc.name, func(t *testing.T) {
			event := testProtoEvent("grpc-canonical-write-"+tc.name, "crm")
			event.ChangedFields = []*auditv1.FieldChange{{Field: "value", BeforeJson: canonicalOverWireJSON(tc.value)}}
			if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: event}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Write code=%v, want InvalidArgument", status.Code(err))
			}
			assertNoFramedKeys(t, st, event.GetEventId())
		})
	}

	for _, tc := range chars {
		t.Run("WriteBatch/"+tc.name, func(t *testing.T) {
			event := testProtoEvent("grpc-canonical-batch-"+tc.name, "crm")
			event.ChangedFields = []*auditv1.FieldChange{{Field: "value", BeforeJson: canonicalOverWireJSON(tc.value)}}
			response, err := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{event}})
			if err != nil || len(response.GetOutcomes()) != 1 {
				t.Fatalf("WriteBatch response=%v err=%v, want one outcome", response, err)
			}
			outcome := response.GetOutcomes()[0]
			if outcome.GetStatus() != auditv1.WriteBatchOutcome_REJECTED || outcome.GetRejectionCode() != "invalid_argument" {
				t.Fatalf("WriteBatch outcome=%v, want invalid_argument rejection", outcome)
			}
			assertNoFramedKeys(t, st, event.GetEventId())
		})
	}

	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rejectedIDs := make([]string, 0, len(chars))
	for _, tc := range chars {
		event := testProtoEvent("grpc-canonical-stream-"+tc.name, "crm")
		event.ChangedFields = []*auditv1.FieldChange{{Field: "value", BeforeJson: canonicalOverWireJSON(tc.value)}}
		rejectedIDs = append(rejectedIDs, event.GetEventId())
		if err := stream.Send(&auditv1.WriteRequest{Event: event}); err != nil {
			t.Fatal(err)
		}
	}
	valid := testProtoEvent("grpc-canonical-stream-valid", "crm")
	if err := stream.Send(&auditv1.WriteRequest{Event: valid}); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	receipt, err := stream.Recv()
	if err != nil || receipt.GetEventId() != valid.GetEventId() {
		t.Fatalf("stream receipt=%v err=%v, want only valid event", receipt, err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("stream close err=%v, want io.EOF", err)
	}
	assertNoFramedKeys(t, st, rejectedIDs...)
}

// TestGRPCCanonicalJSONAfterCapsAcrossRPCs complements the before_json matrix
// above. Keeping the fields separate prevents a rejection caused by one field
// from masking a missing validator on the other field.
func TestGRPCCanonicalJSONAfterCapsAcrossRPCs(t *testing.T) {
	st, client, ctx := newGRPCHarness(t)
	chars := []struct {
		name  string
		value string
	}{
		{"less-than", "<"},
		{"greater-than", ">"},
		{"ampersand", "&"},
	}

	for _, tc := range chars {
		t.Run("Write/"+tc.name, func(t *testing.T) {
			event := testProtoEvent("grpc-canonical-after-write-"+tc.name, "crm")
			event.ChangedFields = []*auditv1.FieldChange{{Field: "value", AfterJson: canonicalOverWireJSON(tc.value)}}
			if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: event}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Write code=%v, want InvalidArgument", status.Code(err))
			}
			assertNoFramedKeys(t, st, event.GetEventId())
		})
	}
	for _, tc := range chars {
		t.Run("WriteBatch/"+tc.name, func(t *testing.T) {
			event := testProtoEvent("grpc-canonical-after-batch-"+tc.name, "crm")
			event.ChangedFields = []*auditv1.FieldChange{{Field: "value", AfterJson: canonicalOverWireJSON(tc.value)}}
			response, err := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{event}})
			if err != nil || len(response.GetOutcomes()) != 1 {
				t.Fatalf("WriteBatch response=%v err=%v, want one outcome", response, err)
			}
			outcome := response.GetOutcomes()[0]
			if outcome.GetStatus() != auditv1.WriteBatchOutcome_REJECTED || outcome.GetRejectionCode() != "invalid_argument" {
				t.Fatalf("WriteBatch outcome=%v, want invalid_argument rejection", outcome)
			}
			assertNoFramedKeys(t, st, event.GetEventId())
		})
	}

	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range chars {
		event := testProtoEvent("grpc-canonical-after-stream-"+tc.name, "crm")
		event.ChangedFields = []*auditv1.FieldChange{{Field: "value", AfterJson: canonicalOverWireJSON(tc.value)}}
		if err := stream.Send(&auditv1.WriteRequest{Event: event}); err != nil {
			t.Fatal(err)
		}
	}
	valid := testProtoEvent("grpc-canonical-after-stream-valid", "crm")
	if err := stream.Send(&auditv1.WriteRequest{Event: valid}); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	receipt, err := stream.Recv()
	if err != nil || receipt.GetEventId() != valid.GetEventId() {
		t.Fatalf("stream receipt=%v err=%v, want only valid event", receipt, err)
	}
	if err := assertStreamEOF(stream); err != nil {
		t.Fatal(err)
	}
	assertNoFramedKeys(t, st, "grpc-canonical-after-stream-less-than", "grpc-canonical-after-stream-greater-than", "grpc-canonical-after-stream-ampersand")
}

func assertStreamEOF(stream auditv1.Ingest_WriteStreamClient) error {
	if _, err := stream.Recv(); err != io.EOF {
		return fmt.Errorf("stream close err=%v, want io.EOF", err)
	}
	return nil
}

func canonicalOverWireJSON(char string) string {
	for count := 1; count <= domain.MaxEnvelopeFieldBytes; count++ {
		value := strings.Repeat(char, count)
		raw := `"` + value + `"`
		canonical, err := domain.CanonicalJSON(value)
		if err == nil && len(raw) <= domain.MaxEnvelopeFieldBytes && len(canonical) > domain.MaxEnvelopeFieldBytes {
			return raw
		}
	}
	panic(fmt.Sprintf("could not construct canonical JSON over-cap value for %q", char))
}
