package main

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/grpcapi"
	"github.com/snaplink/audit-governance/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Only fixture preparation may reconcile the documented store-timeout outcome.
// One immutable request (including occurred_at/idempotency_key) is reused; a
// fresh stream replaces a terminated attempt, never a changed event. The 1s
// child store/drain bound and post-SIGTERM ceiling are not extended or retried.
func prepareDrainIngest(ctx context.Context, client auditv1.IngestClient, request *auditv1.WriteRequest, idleStream bool) (*auditv1.WriteResponse, context.CancelFunc, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		receipt, release, err := drainPreparationExchange(ctx, client, request, idleStream)
		if err == nil {
			if ctx.Err() != nil {
				release()
				return nil, nil, ctx.Err()
			}
			return receipt, release, nil
		}
		release()
		// Do not hide auth, conflict, transport or arbitrary Unavailable
		// failures. This is the production mapper's fixed retryable class.
		if status.Code(err) != codes.Unavailable || status.Convert(err).Message() != "store operation deadline exceeded; retry the request" {
			return nil, nil, err
		}
	}
}

func drainPreparationExchange(preparation context.Context, client auditv1.IngestClient, request *auditv1.WriteRequest, idleStream bool) (*auditv1.WriteResponse, context.CancelFunc, error) {
	// A successful idle stream must keep the original long-lived auth shape,
	// not inherit the preparation guard as a second expiry/drain mechanism.
	ctx, cancel := context.WithCancel(context.WithoutCancel(preparation))
	stop := context.AfterFunc(preparation, cancel)
	defer stop()
	if !idleStream {
		receipt, err := client.Write(ctx, request)
		return receipt, cancel, err
	}
	stream, err := client.WriteStream(ctx)
	if err != nil {
		return nil, cancel, err
	}
	sendErr := stream.Send(request)
	if sendErr != nil && !errors.Is(sendErr, io.EOF) {
		return nil, cancel, sendErr
	}
	receipt, err := stream.Recv() // EOF from Send is never the final RPC status
	if err == nil && sendErr != nil {
		return nil, cancel, sendErr
	}
	return receipt, cancel, err
}

type drainPreparationClient struct {
	auditv1.IngestClient
	write func(context.Context, *auditv1.WriteRequest) (*auditv1.WriteResponse, error)
}

func (c drainPreparationClient) Write(ctx context.Context, request *auditv1.WriteRequest, _ ...grpc.CallOption) (*auditv1.WriteResponse, error) {
	return c.write(ctx, request)
}

func TestDrainPreparationRejectsOtherFailures(t *testing.T) {
	for _, rejection := range []error{
		status.Error(codes.PermissionDenied, "permission denied"),
		status.Error(codes.AlreadyExists, "conflict"),
		status.Error(codes.Unavailable, "transport unavailable"),
		status.Error(codes.Internal, "store operation deadline exceeded; retry the request"),
		io.EOF,
	} {
		calls := 0
		client := drainPreparationClient{write: func(context.Context, *auditv1.WriteRequest) (*auditv1.WriteResponse, error) {
			calls++
			return nil, rejection
		}}
		_, release, err := prepareDrainIngest(context.Background(), client, &auditv1.WriteRequest{}, false)
		if err != rejection || release != nil || calls != 1 {
			t.Fatal("preparation hid or retried a non-store-timeout failure")
		}
	}
}

func TestDrainPreparationGuardCancelsBlockedExchange(t *testing.T) {
	entered := make(chan context.Context, 1)
	client := drainPreparationClient{write: func(ctx context.Context, _ *auditv1.WriteRequest) (*auditv1.WriteResponse, error) {
		entered <- ctx
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := prepareDrainIngest(ctx, client, &auditv1.WriteRequest{}, false)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("preparation never reached the blocked exchange")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("preparation lost caller cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("preparation guard did not cancel the actual blocked exchange")
	}
}

func TestDrainPreparationSuccessKeepsLiveContext(t *testing.T) {
	var live context.Context
	client := drainPreparationClient{write: func(ctx context.Context, _ *auditv1.WriteRequest) (*auditv1.WriteResponse, error) {
		live = ctx
		return &auditv1.WriteResponse{}, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, release, err := prepareDrainIngest(ctx, client, &auditv1.WriteRequest{}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	guardEnded := make(chan struct{})
	context.AfterFunc(ctx, func() { close(guardEnded) })
	cancel()
	<-guardEnded
	if _, deadline := live.Deadline(); deadline || live.Err() != nil {
		t.Fatal("successful exchange inherited preparation cancellation or expiry")
	}
	release()
	if !errors.Is(live.Err(), context.Canceled) {
		t.Fatal("successful exchange cleanup did not release its context")
	}
}

type deadlineDrainStream struct {
	grpc.ServerStream
	fault *atomic.Bool
}

func (s deadlineDrainStream) SendMsg(message any) error {
	if s.fault.CompareAndSwap(true, false) {
		return status.Error(codes.Unavailable, "store operation deadline exceeded; retry the request")
	}
	return s.ServerStream.SendMsg(message)
}

// Inject the unknown-result response only AFTER the production handler has
// really committed. Both transports must reconcile to a duplicate receipt with
// exactly one ledger entry, not abandon an accepted write or mint a new event.
func TestDrainPreparationReconcilesCommittedStoreTimeout(t *testing.T) {
	for _, idleStream := range []bool{false, true} {
		name := "unary"
		if idleStream {
			name = "stream"
		}
		t.Run(name, func(t *testing.T) {
			st, err := store.Open("")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			svc := seededDrainService(t, st)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			var fault atomic.Bool
			fault.Store(true)
			server := grpc.NewServer(
				grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
					response, err := next(ctx, req)
					if err == nil && fault.CompareAndSwap(true, false) {
						return nil, status.Error(codes.Unavailable, "store operation deadline exceeded; retry the request")
					}
					return response, err
				}),
				grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
					return next(srv, deadlineDrainStream{ServerStream: stream, fault: &fault})
				}),
			)
			grpcapi.Register(server, svc, auth.Authenticator{AllowDev: true})
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)
			connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = connection.Close() })
			ctx, cancel := context.WithTimeout(devAuthContext(), 30*time.Second)
			defer cancel()
			request := &auditv1.WriteRequest{Event: drainChildEvent("drain-preparation"), WaitFor: domain.StatusLedgered}
			receipt, release, err := prepareDrainIngest(ctx, auditv1.NewIngestClient(connection), request, idleStream)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			if fault.Load() || !receipt.GetDuplicate() || receipt.GetEventId() != request.Event.EventId || receipt.GetSequence() != 1 || receipt.GetHash() == "" {
				t.Fatal("preparation did not reconcile the actual committed timeout")
			}
			if receipts, events, found, hot := drainSnapshotCounts(t, st, request.Event.EventId); receipts != 1 || events != 1 || !found || !hot {
				t.Fatal("preparation replay did not preserve exactly one durable entry")
			}
		})
	}
}
