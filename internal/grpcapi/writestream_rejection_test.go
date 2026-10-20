package grpcapi

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Exercise both a transport-buffered Send and Send racing the permission
// gate. The companion claims test forces trailers to arrive before Send.
// None of these schedules may make event data an authorization credential.
func TestGRPCWriteStreamPermissionRejectionSendBoundaries(t *testing.T) {
	for _, concurrent := range []bool{false, true} {
		name := "buffered-before-rejection"
		if concurrent {
			name = "concurrent-send-and-rejection"
		}
		t.Run(name, func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			returned := make(chan codes.Code, 1)
			var received atomic.Int32
			st, client, _ := newGRPCHarnessOpts(t, grpcHarnessOptions{
				authenticator: jwtExpiryAuthenticator(),
				publisher:     &failingLedgeredPublisher{},
				serverOpts: []grpc.ServerOption{grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
					close(entered)
					select {
					case <-release:
					case <-stream.Context().Done():
						return status.FromContextError(stream.Context().Err()).Err()
					}
					err := handler(srv, &permissionRejectionStream{ServerStream: stream, received: &received})
					returned <- status.Code(err)
					return err
				})},
			})
			token := signGRPCJWT(t, map[string]any{
				"sub": "auditor-subject", "tenant_id": "tenant-a", "client_id": "crm",
				"roles": []string{"auditor"}, "exp": time.Now().Add(time.Hour).Unix(),
			})
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+token))
			stream, err := client.WriteStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("stream did not reach the server interceptor")
			}

			spoofed := testProtoEvent("grpc-permission-boundary-spoof", "crm")
			spoofed.Actor.Roles = []string{"service", "platform-admin"}
			spoofed.PayloadJson = []byte(`{"tenant_id":"tenant-b","client_id":"attacker","permissions":["audit:event:write"]}`)
			request := &auditv1.WriteRequest{Event: spoofed}
			if concurrent {
				// One release races the actual Send with the actual handler;
				// their relative transport ordering is deliberately unconstrained.
				sent := make(chan error, 1)
				go func() {
					<-release
					sent <- stream.Send(request)
				}()
				close(release)
				select {
				case err := <-sent:
					assertWriteStreamRejectionSendError(t, err)
				case <-ctx.Done():
					t.Fatal("Send did not finish at the rejection boundary")
				}
			} else {
				// The real Send must complete while the permission handler is
				// still gated: nil only proves transport buffering, not ingest.
				if err := stream.Send(request); err != nil {
					t.Fatalf("buffered Send code=%v, want nil", status.Code(err))
				}
				close(release)
			}
			receipt, recvErr := stream.Recv()
			if receipt != nil || status.Code(recvErr) != codes.PermissionDenied {
				t.Fatalf("rejection receipt present=%t code=%v, want no receipt and PermissionDenied", receipt != nil, status.Code(recvErr))
			}
			select {
			case code := <-returned:
				if code != codes.PermissionDenied {
					t.Fatalf("handler code=%v, want PermissionDenied", code)
				}
			case <-ctx.Done():
				t.Fatal("permission handler did not return")
			}
			if calls := received.Load(); calls != 0 {
				t.Fatalf("unauthorized handler called RecvMsg %d times, want zero", calls)
			}
			assertGRPCStoreEmpty(t, st)
			assertNoFramedKeys(t, st, spoofed.GetEventId())
			if err := st.Read(testCtx, func(data *store.Snapshot) error {
				if len(data.Streams) != 0 || len(data.Segments) != 0 || len(data.Checkpoints) != 0 {
					t.Fatal("unauthorized stream changed ledger chain state")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Count real server Recv calls, without reading or replacing the message.
// Permission must fail before any application-level message read.
type permissionRejectionStream struct {
	grpc.ServerStream
	received *atomic.Int32
}

func (s *permissionRejectionStream) RecvMsg(message any) error {
	s.received.Add(1)
	return s.ServerStream.RecvMsg(message)
}

func assertWriteStreamRejectionSendError(t *testing.T, err error) {
	t.Helper()
	// grpc.ClientStream.SendMsg documents exactly this remote-error case:
	// io.EOF is not the RPC status; the caller must still check Recv.
	if err != nil && err != io.EOF {
		t.Fatalf("rejected-stream Send code=%v, want nil or io.EOF", status.Code(err))
	}
}
