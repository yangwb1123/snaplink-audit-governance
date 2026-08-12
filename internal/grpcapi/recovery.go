package grpcapi

import (
	"context"
	"log"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// recoveryLogger returns l, or log.Default() when l is nil — mirroring
// Register's logger defaulting (FR-5), so a nil logger never drops the
// server-side panic detail silently.
func recoveryLogger(l *log.Logger) *log.Logger {
	if l == nil {
		return log.Default()
	}
	return l
}

// recoveryStatus converts a recovered panic value into the redacted
// codes.Internal status and logs the full detail (value + goroutine stack)
// server-side through the same logger statusError uses — preserving the
// M-1 redaction contract (FR-1.1, NFR-3): the client sees exactly
// "internal server error", never panic internals.
func recoveryStatus(recovered any, logger *log.Logger) error {
	logger.Printf("grpc panic recovered: %v\n%s", recovered, debug.Stack())
	return status.Error(codes.Internal, "internal server error")
}

// RecoveryUnaryServerInterceptor returns a unary interceptor that contains
// handler panics per-request and converts them to the redacted codes.Internal
// status (FR-1.1, FR-1.3, FR-1.4, FR-1.5). A non-panicking handler's
// (response, error) is forwarded unchanged; a handler that both returns an
// error and panics is treated as panicking — the panic wins.
//
// The interceptor contains only the handler goroutine: a panic in a goroutine
// spawned by the handler is not recoverable here and still crashes the
// process. Handler authors must not let panics escape worker goroutines.
func RecoveryUnaryServerInterceptor(logger *log.Logger) grpc.UnaryServerInterceptor {
	l := recoveryLogger(logger)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = recoveryStatus(recovered, l)
			}
		}()
		return handler(ctx, req)
	}
}

// RecoveryStreamServerInterceptor returns a stream interceptor that contains
// WriteStream handler panics; the stream terminates with the redacted
// codes.Internal status at trailer time (FR-1.2, FR-1.3, FR-1.4). Messages
// already sent before the panic are delivered first, then the error status —
// inherent to gRPC's streaming model. No ServerStream wrapper is needed: the
// interceptor only observes handler completion.
func RecoveryStreamServerInterceptor(logger *log.Logger) grpc.StreamServerInterceptor {
	l := recoveryLogger(logger)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = recoveryStatus(recovered, l)
			}
		}()
		return handler(srv, ss)
	}
}
