package grpcapi

import (
	"context"
	"io"
	"testing"
	"time"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestGRPCWriteStreamJWTExpiryWhileIdle proves that expiry is observed while
// the server is blocked in Recv, rather than waiting for a follow-up message.
func TestGRPCWriteStreamJWTExpiryWhileIdle(t *testing.T) {
	st, client, _ := newGRPCHarnessOpts(t, grpcHarnessOptions{
		authenticator: jwtExpiryAuthenticator(),
	})
	exp := time.Now().Add(2 * time.Second).Unix()
	ctx, cancel := jwtStreamContext(t, exp)
	defer cancel()

	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("idle expiry code=%v, want Unauthenticated (err=%v)", status.Code(err), err)
	}
	assertNoFramedKeys(t, st)
}

// TestGRPCWriteStreamRejectsPostExpiryMessage verifies the acceptance oracle:
// Send may succeed because the request is buffered, but an event received at
// or after the effective boundary is neither ingested nor receipted.
func TestGRPCWriteStreamRejectsPostExpiryMessage(t *testing.T) {
	st, client, _ := newGRPCHarnessOpts(t, grpcHarnessOptions{
		authenticator: jwtExpiryAuthenticator(),
		publisher:     &failingLedgeredPublisher{},
	})
	exp := time.Now().Add(3 * time.Second).Unix()
	ctx, cancel := jwtStreamContext(t, exp)
	defer cancel()

	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	accepted := testProtoEvent("grpc-expiry-prefix", "crm")
	if err := stream.Send(&auditv1.WriteRequest{Event: accepted}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("pre-expiry message rejected: %v", err)
	}

	waitForUnixBoundary(t, exp)
	rejected := testProtoEvent("grpc-expiry-tail", "crm")
	_ = stream.Send(&auditv1.WriteRequest{Event: rejected})
	if _, err := stream.Recv(); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("post-expiry code=%v, want Unauthenticated (err=%v)", status.Code(err), err)
	}
	assertNoFramedKeys(t, st, rejected.GetEventId())
	if err := st.Read(testCtx, func(data *store.Snapshot) error {
		if _, ok := data.Events[store.EventKey("tenant-a", accepted.GetEventId())]; !ok {
			t.Fatal("pre-expiry event was not persisted")
		}
		if _, ok := data.LedgeredOutbox[store.EventKey("tenant-a", accepted.GetEventId())]; !ok {
			t.Fatal("pre-expiry event has no durable outbox witness")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestGRPCWriteStreamHonorsClockSkew verifies that the stream monitor uses
// exp + ClockSkew, not nominal exp, while the exact effective boundary still
// terminates the stream.
func TestGRPCWriteStreamHonorsClockSkew(t *testing.T) {
	const skew = 2 * time.Second
	st, client, _ := newGRPCHarnessOpts(t, grpcHarnessOptions{
		authenticator: &auth.Authenticator{JWTSecret: testJWTSecret, AllowLocalHS256: true, ClockSkew: skew},
		publisher:     &failingLedgeredPublisher{},
	})
	nominalExpiry := time.Now().Add(3 * time.Second).Unix()
	effectiveExpiry := nominalExpiry + int64(skew/time.Second)
	ctx, cancel := jwtStreamContext(t, nominalExpiry)
	defer cancel()

	stream, err := client.WriteStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	withinSkew := testProtoEvent("grpc-expiry-skew-prefix", "crm")
	waitForUnixBoundary(t, nominalExpiry)
	if err := stream.Send(&auditv1.WriteRequest{Event: withinSkew}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("message within configured skew rejected: %v", err)
	}

	waitForUnixBoundary(t, effectiveExpiry)
	atBoundary := testProtoEvent("grpc-expiry-skew-boundary", "crm")
	_ = stream.Send(&auditv1.WriteRequest{Event: atBoundary})
	if _, err := stream.Recv(); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("effective-boundary code=%v, want Unauthenticated (err=%v)", status.Code(err), err)
	}
	assertNoFramedKeys(t, st, atBoundary.GetEventId())
}

// TestGRPCWriteStreamClaimsAreNotExtendedByEventFields pins the transport
// trust boundary: payload and actor data remain event data, while source and
// tenant/client authorization continue to come from the verified claims and
// service-side source binding.
func TestGRPCWriteStreamClaimsAreNotExtendedByEventFields(t *testing.T) {
	st, client, _ := newGRPCHarnessOpts(t, grpcHarnessOptions{
		authenticator: jwtExpiryAuthenticator(),
		publisher:     &failingLedgeredPublisher{},
	})
	writeToken := signGRPCJWT(t, map[string]any{
		"sub": "service-subject", "tenant_id": "tenant-a", "client_id": "crm",
		"roles": []string{"service"}, "exp": time.Now().Add(time.Hour).Unix(),
	})
	writeCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+writeToken))
	stream, err := client.WriteStream(writeCtx)
	if err != nil {
		t.Fatal(err)
	}
	trusted := testProtoEvent("grpc-claims-event-data", "crm")
	trusted.Actor.Roles = []string{"platform-admin"}
	trusted.PayloadJson = []byte(`{"tenant_id":"tenant-b","client_id":"attacker","permissions":["audit:event:write"]}`)
	if err := stream.Send(&auditv1.WriteRequest{Event: trusted}); err != nil {
		t.Fatal(err)
	}
	receipt, err := stream.Recv()
	if err != nil {
		t.Fatalf("claims-isolation prefix rejected: %v", err)
	}
	if receipt.GetTenantId() != "tenant-a" {
		t.Fatalf("receipt tenant=%q, want server-derived tenant-a", receipt.GetTenantId())
	}
	if err := st.Read(testCtx, func(data *store.Snapshot) error {
		stored, ok := data.Events[store.EventKey("tenant-a", trusted.GetEventId())]
		if !ok {
			t.Fatal("claims-isolation event was not persisted")
		}
		if stored.TenantID != "tenant-a" || stored.SourceSystem != "crm" {
			t.Fatalf("stored server-derived scope=%q/%q, want tenant-a/crm", stored.TenantID, stored.SourceSystem)
		}
		if stored.Payload["tenant_id"] != "tenant-b" || stored.Payload["client_id"] != "attacker" {
			t.Fatal("payload was not retained as event data")
		}
		key := store.EventKey("tenant-a", trusted.GetEventId())
		if _, ok := data.Receipts[key]; !ok {
			t.Fatal("claims-isolation event has no durable receipt witness")
		}
		if _, ok := data.LedgeredOutbox[key]; !ok {
			t.Fatal("claims-isolation event has no durable outbox witness")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	noWriteToken := signGRPCJWT(t, map[string]any{
		"sub": "auditor-subject", "tenant_id": "tenant-a", "client_id": "crm",
		"roles": []string{"auditor"}, "exp": time.Now().Add(time.Hour).Unix(),
	})
	noWriteCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	noWriteCtx = metadata.NewOutgoingContext(noWriteCtx, metadata.Pairs("authorization", "Bearer "+noWriteToken))
	denied, err := client.WriteStream(noWriteCtx)
	if err != nil {
		t.Fatal(err)
	}
	// With no initial response headers, Header waits for the server's
	// trailers-only rejection. This forces the denied-before-Send order on
	// the real transport without a sleep or a shortened JWT lifetime.
	if headers, err := denied.Header(); err != nil || headers != nil {
		t.Fatalf("denied stream Header: metadata present=%t code=%v", headers != nil, status.Code(err))
	}
	spoofed := testProtoEvent("grpc-claims-permission-spoof", "crm")
	spoofed.Actor.Roles = []string{"service"}
	spoofed.PayloadJson = []byte(`{"permissions":["audit:event:write"]}`)
	// SendMsg reports a completed remote rejection as io.EOF, not the RPC
	// status. Requiring nil here made this authorization test scheduler-dependent.
	if err := denied.Send(&auditv1.WriteRequest{Event: spoofed}); err != io.EOF {
		t.Fatalf("Send after rejection code=%v, want io.EOF", status.Code(err))
	}
	if receipt, err := denied.Recv(); receipt != nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("actor/payload permission spoof receipt present=%t code=%v, want no receipt and PermissionDenied", receipt != nil, status.Code(err))
	}
	assertNoFramedKeys(t, st, spoofed.GetEventId())
	if err := st.Read(testCtx, func(data *store.Snapshot) error {
		if len(data.Events) != 1 || len(data.Receipts) != 1 || len(data.LedgeredOutbox) != 1 {
			t.Fatal("rejected stream changed persistence beyond the authorized prefix")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func jwtExpiryAuthenticator() *auth.Authenticator {
	return &auth.Authenticator{JWTSecret: testJWTSecret, AllowLocalHS256: true}
}

func jwtStreamContext(t *testing.T, exp int64) (context.Context, context.CancelFunc) {
	t.Helper()
	token := signGRPCJWT(t, map[string]any{
		"sub":       "service-subject",
		"tenant_id": "tenant-a",
		"client_id": "crm",
		"roles":     []string{"service"},
		"exp":       exp,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+token)), cancel
}

func waitForUnixBoundary(t *testing.T, boundary int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Unix() < boundary {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for Unix boundary %d", boundary)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
