// Command grpcwrite exercises the gRPC ingest surface end-to-end: it dials
// the audit-api gRPC listener (AUDIT_GRPC_LISTEN), authenticates with a
// bearer token in metadata (same Authenticator as HTTP) and writes one
// canonical event, printing the receipt. Used by test/e2e/fullstack.sh to
// cover the B1-6 "supported inbound" contract over a real socket.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	address := flag.String("address", "localhost:19051", "audit-api gRPC address")
	eventID := flag.String("event-id", fmt.Sprintf("grpc-e2e-%d", time.Now().Unix()), "event id")
	token := flag.String("token", os.Getenv("AUDIT_E2E_TOKEN"), "bearer token (defaults to AUDIT_E2E_TOKEN)")
	flag.Parse()
	if *token == "" {
		log.Fatalf("token is required: pass -token or set AUDIT_E2E_TOKEN")
	}
	connection, err := grpc.NewClient(*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer connection.Close()
	client := auditv1.NewIngestClient(connection)
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+*token))
	request := &auditv1.WriteRequest{WaitFor: "ledgered", Event: &auditv1.EventEnvelope{
		EventId: *eventID, SourceSystem: "demo", EventType: "audit.event",
		SchemaId: "audit.event", SchemaVersion: 1,
		OccurredAt: timestamppb.New(time.Now().UTC()),
		Actor:      &auditv1.Actor{Id: "grpc-e2e"},
		Action:     "update", Outcome: "success",
		DataClassification: "internal", RetentionClass: "standard",
		IdempotencyKey: *eventID + "-idem",
		PayloadJson:    []byte(`{"resource":"grpc-e2e"}`),
	}}
	receipt, err := client.Write(ctx, request)
	if err != nil {
		log.Fatalf("grpc write failed: %v (code=%s)", err, status.Code(err))
	}
	if receipt.GetTenantId() != "demo" || receipt.GetStatus() == "" || receipt.GetEventId() != *eventID {
		log.Fatalf("unexpected receipt: %+v", receipt)
	}
	fmt.Printf("grpc-write-ok event_id=%s tenant=%s status=%s sequence=%d hash=%s\n",
		receipt.GetEventId(), receipt.GetTenantId(), receipt.GetStatus(), receipt.GetSequence(), receipt.GetHash())
}
