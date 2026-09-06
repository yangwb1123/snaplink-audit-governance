package grpcapi

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestFromProtoRejectsDuplicateChangedFields(t *testing.T) {
	tests := []struct {
		name  string
		field string
	}{
		{name: "ordinary", field: "status"},
		{name: "empty", field: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			event := testProtoEvent("duplicate-"+tc.name, "crm")
			event.ChangedFields = []*auditv1.FieldChange{
				{Field: tc.field, BeforeJson: `"before"`},
				// Invalid JSON here proves duplicate detection precedes decoding.
				{Field: tc.field, AfterJson: `not-json`},
			}
			converted, err := fromProto(event)
			if !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("err=%v, want ErrInvalid", err)
			}
			if !reflect.DeepEqual(converted, domain.Event{}) {
				t.Fatalf("converted event=%+v, want zero event", converted)
			}
			if !strings.Contains(err.Error(), `duplicate changed_fields.field`) || !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("err=%q, want duplicate field %q", err, tc.field)
			}
			if status.Code(toStatus(err)) != codes.InvalidArgument {
				t.Fatalf("status=%v, want InvalidArgument", status.Code(toStatus(err)))
			}
		})
	}

	// Exact string equality keeps case and whitespace variants distinct.
	unique := testProtoEvent("unique-changed-fields", "crm")
	unique.ChangedFields = []*auditv1.FieldChange{
		{Field: "", BeforeJson: `12345678901234567890`, AfterJson: `"empty"`},
		{Field: "status", BeforeJson: `"open"`, AfterJson: `"paid"`},
		{Field: "Status", BeforeJson: `1`, AfterJson: `2`},
		{Field: " status", BeforeJson: `true`, AfterJson: `false`},
	}
	converted, err := fromProto(unique)
	if err != nil {
		t.Fatalf("unique field names rejected: %v", err)
	}
	if len(converted.ChangedFields) != len(unique.ChangedFields) {
		t.Fatalf("changed fields=%d, want %d", len(converted.ChangedFields), len(unique.ChangedFields))
	}
	if got := converted.ChangedFields[""].Before; got != json.Number("12345678901234567890") {
		t.Fatalf("empty-key before=%#v, want exact json.Number", got)
	}
	for field, want := range map[string]any{
		"status":  "paid",
		"Status":  json.Number("2"),
		" status": false,
	} {
		if got := converted.ChangedFields[field].After; !reflect.DeepEqual(got, want) {
			t.Fatalf("field %q after=%#v, want %#v", field, got, want)
		}
	}

	// Count-cap precedence is retained even when the over-limit slice also
	// contains duplicates.
	over := testProtoEvent("duplicate-over-limit", "crm")
	over.ChangedFields = make([]*auditv1.FieldChange, MaxChangedFields+1)
	for i := range over.ChangedFields {
		over.ChangedFields[i] = &auditv1.FieldChange{Field: "same"}
	}
	_, err = fromProto(over)
	if !errors.Is(err, ErrEnvelopeTooLarge) || !strings.Contains(err.Error(), "changed_fields count") {
		t.Fatalf("err=%v, want changed_fields count cap before duplicate", err)
	}
}

// FuzzRejectDuplicateChangedFields pins the exact-string duplicate invariant:
// a repeated FieldChange.field is rejected with domain.ErrInvalid, while a
// slice of distinct names that survives conversion must preserve every key.
// Names differing only by case or whitespace are distinct, and "" is a
// valid single key whose second occurrence is a duplicate.
func FuzzRejectDuplicateChangedFields(f *testing.F) {
	f.Add("status", "status", "amount")
	f.Add("", "", "x")
	f.Add("status", "Status", " status")
	f.Fuzz(func(t *testing.T, a, b, c string) {
		event := testProtoEvent("fuzz-duplicate", "crm")
		event.ChangedFields = []*auditv1.FieldChange{
			{Field: a, BeforeJson: `"a"`},
			{Field: b, BeforeJson: `"b"`},
			{Field: c, BeforeJson: `"c"`},
		}
		converted, err := fromProto(event)
		hasDuplicate := a == b || a == c || b == c
		if hasDuplicate {
			if !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("duplicate fields %q/%q/%q accepted: err=%v, want ErrInvalid", a, b, c, err)
			}
			return
		}
		if err != nil {
			return // rejection for unrelated reasons (caps, JSON) is out of scope
		}
		for _, name := range []string{a, b, c} {
			if _, ok := converted.ChangedFields[name]; !ok {
				t.Fatalf("distinct field %q missing from converted event", name)
			}
		}
	})
}

func TestChangedFieldOrderDoesNotChangeSourceDigest(t *testing.T) {
	first := testProtoEvent("changed-order-digest", "crm")
	first.ChangedFields = []*auditv1.FieldChange{
		{Field: "status", BeforeJson: `"open"`, AfterJson: `"paid"`},
		{Field: "amount", BeforeJson: `1`, AfterJson: `2`},
	}
	second := testProtoEvent("changed-order-digest", "crm")
	second.ChangedFields = []*auditv1.FieldChange{first.ChangedFields[1], first.ChangedFields[0]}
	firstEvent, err := fromProto(first)
	if err != nil {
		t.Fatal(err)
	}
	secondEvent, err := fromProto(second)
	if err != nil {
		t.Fatal(err)
	}
	firstDigest, err := domain.EventDigest(firstEvent)
	if err != nil {
		t.Fatal(err)
	}
	secondDigest, err := domain.EventDigest(secondEvent)
	if err != nil {
		t.Fatal(err)
	}
	if firstDigest != secondDigest {
		t.Fatalf("reordered distinct fields changed digest: %q != %q", firstDigest, secondDigest)
	}
}

func TestFromProtoRejectsUnknownSupportedFields(t *testing.T) {
	unknown := validationUnknownWire()
	tests := []struct {
		name   string
		path   string
		mutate func(*auditv1.EventEnvelope)
	}{
		{name: "envelope", path: "EventEnvelope", mutate: func(event *auditv1.EventEnvelope) {
			event.ProtoReflect().SetUnknown(unknown)
		}},
		{name: "actor", path: "EventEnvelope.actor", mutate: func(event *auditv1.EventEnvelope) {
			event.Actor.ProtoReflect().SetUnknown(unknown)
		}},
		{name: "target", path: "EventEnvelope.targets[0]", mutate: func(event *auditv1.EventEnvelope) {
			event.Targets = []*auditv1.Target{{Type: "invoice"}}
			event.Targets[0].ProtoReflect().SetUnknown(unknown)
		}},
		{name: "changed-field", path: "EventEnvelope.changed_fields[0]", mutate: func(event *auditv1.EventEnvelope) {
			event.ChangedFields = []*auditv1.FieldChange{{Field: "status"}}
			event.ChangedFields[0].ProtoReflect().SetUnknown(unknown)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			event := testProtoEvent("unknown-"+tc.name, "crm")
			tc.mutate(event)
			decoded := &auditv1.EventEnvelope{}
			roundTripProtoMessage(t, event, decoded)
			_, err := fromProto(decoded)
			if !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("err=%v, want ErrInvalid", err)
			}
			if !strings.Contains(err.Error(), tc.path) {
				t.Fatalf("err=%q, want logical path %q", err, tc.path)
			}
			if strings.Contains(err.Error(), string(unknown)) || strings.Contains(err.Error(), hex.EncodeToString(unknown)) {
				t.Fatalf("err=%q leaks unknown wire data", err)
			}
		})
	}

	// Unknown data wins over size classification, which keeps WriteStream from
	// incorrectly treating this ordinary invalid message as skippable.
	over := testProtoEvent("unknown-over-limit", "crm")
	over.Reason = strings.Repeat("r", MaxEnvelopeFieldBytes+1)
	over.ProtoReflect().SetUnknown(unknown)
	_, err := fromProto(over)
	if !strings.Contains(err.Error(), "unknown protobuf fields in EventEnvelope") || errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatalf("err=%v, want unknown-field rejection before size cap", err)
	}
}

func TestGRPCRejectsDuplicateChangedFields(t *testing.T) {
	st, client, ctx := newGRPCHarness(t)
	duplicate := testProtoEvent("grpc-duplicate-write", "crm")
	duplicate.ChangedFields = []*auditv1.FieldChange{
		{Field: "status", BeforeJson: `"open"`},
		{Field: "status", AfterJson: `"paid"`},
	}
	if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: duplicate}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Write code=%v, want InvalidArgument", status.Code(err))
	}

	prefix := testProtoEvent("grpc-duplicate-prefix", "crm")
	batchDuplicate := testProtoEvent("grpc-duplicate-batch", "crm")
	batchDuplicate.ChangedFields = []*auditv1.FieldChange{{Field: "x"}, {Field: "x"}}
	later := testProtoEvent("grpc-duplicate-later", "crm")
	response, err := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{prefix, batchDuplicate, later}})
	if err != nil || len(response.GetReceipts()) != 1 || len(response.GetOutcomes()) != 3 {
		t.Fatalf("WriteBatch response=%v err=%v, want prefix receipt and three outcomes", response, err)
	}
	if response.GetOutcomes()[1].GetStatus() != auditv1.WriteBatchOutcome_REJECTED || response.GetOutcomes()[1].GetRejectionCode() != "invalid_argument" || response.GetOutcomes()[2].GetStatus() != auditv1.WriteBatchOutcome_NOT_ATTEMPTED {
		t.Fatalf("unexpected batch outcomes: %v", response.GetOutcomes())
	}

	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	streamDuplicate := testProtoEvent("grpc-duplicate-stream", "crm")
	streamDuplicate.ChangedFields = []*auditv1.FieldChange{{Field: "x"}, {Field: "x"}}
	if err := stream.Send(&auditv1.WriteRequest{Event: streamDuplicate}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteStream code=%v, want InvalidArgument termination", status.Code(err))
	}

	assertNoFramedKeys(t, st, duplicate.GetEventId(), batchDuplicate.GetEventId(), later.GetEventId(), streamDuplicate.GetEventId())
	if err := st.Read(func(data *store.Snapshot) error {
		if _, exists := data.Events[store.EventKey("tenant-a", prefix.GetEventId())]; !exists {
			t.Fatal("valid batch prefix was not committed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGRPCRejectsUnknownSupportedFields(t *testing.T) {
	st, client, ctx := newGRPCHarness(t)
	unknown := validationUnknownWire()

	write := testProtoEvent("grpc-unknown-write", "crm")
	write.ProtoReflect().SetUnknown(unknown)
	write = roundTripEnvelope(t, write)
	_, err := client.Write(ctx, &auditv1.WriteRequest{Event: write})
	assertUnknownGRPCError(t, err, unknown)

	batch := testProtoEvent("grpc-unknown-batch", "crm")
	batch.Actor.ProtoReflect().SetUnknown(unknown)
	batch = roundTripEnvelope(t, batch)
	response, err := client.WriteBatch(ctx, &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{batch}})
	if err != nil || len(response.GetReceipts()) != 0 || len(response.GetOutcomes()) != 1 {
		t.Fatalf("WriteBatch response=%v err=%v, want one rejected outcome", response, err)
	}
	if outcome := response.GetOutcomes()[0]; outcome.GetStatus() != auditv1.WriteBatchOutcome_REJECTED || outcome.GetRejectionCode() != "invalid_argument" {
		t.Fatalf("batch outcome=%v, want invalid_argument rejection", outcome)
	}

	streamEvent := testProtoEvent("grpc-unknown-stream", "crm")
	streamEvent.ChangedFields = []*auditv1.FieldChange{{Field: "status"}}
	streamEvent.ChangedFields[0].ProtoReflect().SetUnknown(unknown)
	streamEvent = roundTripEnvelope(t, streamEvent)
	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&auditv1.WriteRequest{Event: streamEvent}); err != nil {
		t.Fatal(err)
	}
	streamErr := receiveStreamError(stream)
	assertUnknownGRPCError(t, streamErr, unknown)

	// Unknown fields are checked before caps, so this message terminates the
	// stream instead of taking the existing oversized-message skip path.
	unknownOver := testProtoEvent("grpc-unknown-over-limit", "crm")
	unknownOver.Reason = strings.Repeat("r", MaxEnvelopeFieldBytes+1)
	unknownOver.ProtoReflect().SetUnknown(unknown)
	stream, err = client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&auditv1.WriteRequest{Event: unknownOver}); err != nil {
		t.Fatal(err)
	}
	assertUnknownGRPCError(t, receiveStreamError(stream), unknown)

	assertNoFramedKeys(t, st, write.GetEventId(), batch.GetEventId(), streamEvent.GetEventId(), unknownOver.GetEventId())
}

func TestUnknownFieldsOutsideSupportedScopeRemainAccepted(t *testing.T) {
	st, client, ctx := newGRPCHarness(t)
	unknown := validationUnknownWire()

	write := &auditv1.WriteRequest{Event: testProtoEvent("grpc-unknown-wrapper", "crm")}
	write.ProtoReflect().SetUnknown(unknown)
	write.Event.OccurredAt.ProtoReflect().SetUnknown(unknown)
	decodedWrite := &auditv1.WriteRequest{}
	roundTripProtoMessage(t, write, decodedWrite)
	if _, err := client.Write(ctx, decodedWrite); err != nil {
		t.Fatalf("unknown WriteRequest/Timestamp rejected: %v", err)
	}

	batch := &auditv1.WriteBatchRequest{Events: []*auditv1.EventEnvelope{testProtoEvent("grpc-unknown-batch-wrapper", "crm")}}
	batch.ProtoReflect().SetUnknown(unknown)
	batch.Events[0].OccurredAt.ProtoReflect().SetUnknown(unknown)
	decodedBatch := &auditv1.WriteBatchRequest{}
	roundTripProtoMessage(t, batch, decodedBatch)
	if _, err := client.WriteBatch(ctx, decodedBatch); err != nil {
		t.Fatalf("unknown WriteBatchRequest/Timestamp rejected: %v", err)
	}

	// Stream: the per-message WriteRequest wrapper and the Timestamp are also
	// outside the supported-message validation scope, so unknown fields on
	// them must not terminate the stream.
	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	streamMsg := &auditv1.WriteRequest{Event: testProtoEvent("grpc-unknown-stream-wrapper", "crm")}
	streamMsg.ProtoReflect().SetUnknown(unknown)
	streamMsg.Event.OccurredAt.ProtoReflect().SetUnknown(unknown)
	decodedStreamMsg := &auditv1.WriteRequest{}
	roundTripProtoMessage(t, streamMsg, decodedStreamMsg)
	if err := stream.Send(decodedStreamMsg); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("unknown WriteRequest/Timestamp in stream rejected: %v", err)
	}

	if err := st.Read(func(data *store.Snapshot) error {
		for _, id := range []string{"grpc-unknown-wrapper", "grpc-unknown-batch-wrapper", "grpc-unknown-stream-wrapper"} {
			if _, exists := data.Events[store.EventKey("tenant-a", id)]; !exists {
				t.Fatalf("accepted out-of-scope unknown event %q was not persisted", id)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func assertUnknownGRPCError(t *testing.T, err error, unknown []byte) {
	t.Helper()
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code=%v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
	message := status.Convert(err).Message()
	if strings.Contains(message, string(unknown)) || strings.Contains(message, hex.EncodeToString(unknown)) {
		t.Fatalf("client error leaks unknown wire data: %q", message)
	}
}

func receiveStreamError(stream auditv1.Ingest_WriteStreamClient) error {
	_, err := stream.Recv()
	return err
}

func validationUnknownWire() []byte {
	wire := protowire.AppendTag(nil, 100, protowire.BytesType)
	return protowire.AppendBytes(wire, []byte("opaque-unknown"))
}

func roundTripEnvelope(t *testing.T, event *auditv1.EventEnvelope) *auditv1.EventEnvelope {
	t.Helper()
	decoded := &auditv1.EventEnvelope{}
	roundTripProtoMessage(t, event, decoded)
	return decoded
}

func roundTripProtoMessage(t *testing.T, input, output proto.Message) {
	t.Helper()
	wire, err := proto.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(wire, output); err != nil {
		t.Fatal(err)
	}
}
