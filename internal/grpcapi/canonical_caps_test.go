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
// with JSON whose compact wire spelling fits the field cap but whose canonical
// spelling does not. HTML-sensitive characters are intentionally left
// unescaped on the wire; CanonicalJSON escapes them before measuring.
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
			event.ChangedFields = []*auditv1.FieldChange{{Field: "value", AfterJson: canonicalOverWireJSON(tc.value)}}
			if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: event}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Write code=%v, want InvalidArgument", status.Code(err))
			}
			assertNoFramedKeys(t, st, event.GetEventId())
		})
	}

	for _, tc := range chars {
		t.Run("WriteBatch/"+tc.name, func(t *testing.T) {
			event := testProtoEvent("grpc-canonical-batch-"+tc.name, "crm")
			event.ChangedFields = []*auditv1.FieldChange{{Field: "value", AfterJson: canonicalOverWireJSON(tc.value)}}
			response, err := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{event}})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("WriteBatch code=%v, want InvalidArgument", status.Code(err))
			}
			if response != nil && len(response.GetReceipts()) != 0 {
				t.Fatalf("WriteBatch receipts=%d, want 0", len(response.GetReceipts()))
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
		event.ChangedFields = []*auditv1.FieldChange{{Field: "value", AfterJson: canonicalOverWireJSON(tc.value)}}
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
