package grpcapi

import (
	"time"

	"google.golang.org/grpc/keepalive"
)

// Compile-time keepalive values for the gRPC ingest listener — the single
// tunable location (FR-3.2). Values are conservative and standard; tuned by
// editing only (no config plumbing in this direction).
const (
	keepaliveMaxConnectionIdle = 5 * time.Minute
	keepaliveTime              = 60 * time.Second
	keepaliveTimeout           = 20 * time.Second
	keepaliveMinTime           = 10 * time.Second
)

// KeepaliveParams returns the server keepalive parameters (FR-3.1/3.2):
// idle connections are closed after keepaliveMaxConnectionIdle, the server
// pings otherwise-idle peers every keepaliveTime, and an unanswered ping
// closes the connection after keepaliveTimeout. Transport-level only — RPC
// semantics and authentication are untouched (FR-3.4).
func KeepaliveParams() keepalive.ServerParameters {
	return keepalive.ServerParameters{
		MaxConnectionIdle: keepaliveMaxConnectionIdle,
		Time:              keepaliveTime,
		Timeout:           keepaliveTimeout,
	}
}

// KeepaliveEnforcementPolicy returns the optional enforcement policy
// (FR-3.3): clients may not ping more often than keepaliveMinTime, and idle
// connections (no active stream) remain ping-eligible so LB/NAT mappings
// stay warm. Only affects clients configured to send keepalive pings;
// grpc-go clients do not ping by default, so existing clients are
// unaffected.
func KeepaliveEnforcementPolicy() keepalive.EnforcementPolicy {
	return keepalive.EnforcementPolicy{
		MinTime:             keepaliveMinTime,
		PermitWithoutStream: true,
	}
}
