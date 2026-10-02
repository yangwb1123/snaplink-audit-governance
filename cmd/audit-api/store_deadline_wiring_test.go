package main

// F-C / A5, pinned at the binary that owns the invariant.
//
// The retryable 503 a store timeout classifies as can only reach the client if
// the connection is still alive when it is written. audit-api fixes
// WriteTimeout, so a store deadline at or above it would silently turn a
// documented retry into an opaque transport error — which is why the relation
// is validated at startup against the same constant the server uses.

import (
	"io"
	"log"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/runtimeconfig"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

// authForTest is the minimal authenticator prepareServer accepts.
func authForTest() auth.Authenticator {
	return auth.Authenticator{JWTSecret: testJWTSecret, AllowLocalHS256: true}
}

func TestOperationTimeoutMustPrecedeWriteTimeout(t *testing.T) {
	t.Run("default-is-legal", func(t *testing.T) {
		if store.DefaultOperationTimeout >= httpWriteTimeout {
			t.Fatalf("DefaultOperationTimeout=%s must be strictly below the %s HTTP write timeout, "+
				"otherwise the retryable store_timeout status can never be written",
				store.DefaultOperationTimeout, httpWriteTimeout)
		}
	})

	t.Run("server-rejects-a-longer-store-deadline", func(t *testing.T) {
		t.Setenv(runtimeconfig.EnvStoreOperationTimeout, httpWriteTimeout.String())
		st, err := store.OpenWithOptions("", store.Options{OperationTimeout: httpWriteTimeout})
		if err != nil {
			t.Fatal(err)
		}
		svc, err := service.New(st, service.Config{AllowDevSecrets: true, SigningSecret: "test-secret"})
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = prepareServer(log.New(io.Discard, "", 0), svc, authForTest(), "", "127.0.0.1:0")
		if err == nil {
			t.Fatal("prepareServer accepted a store deadline equal to the HTTP write timeout")
		}
		if !strings.Contains(err.Error(), runtimeconfig.EnvStoreOperationTimeout) {
			t.Fatalf("error %q does not name %s", err, runtimeconfig.EnvStoreOperationTimeout)
		}
	})

	t.Run("server-accepts-the-default", func(t *testing.T) {
		st, err := store.OpenWithOptions("", store.Options{})
		if err != nil {
			t.Fatal(err)
		}
		svc, err := service.New(st, service.Config{AllowDevSecrets: true, SigningSecret: "test-secret"})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := prepareServer(log.New(io.Discard, "", 0), svc, authForTest(), "", "127.0.0.1:0"); err != nil {
			t.Fatalf("prepareServer rejected the shipped default store deadline: %v", err)
		}
	})
}

// TestOpenStoreAppliesTheSharedPoolConfiguration pins A5's "one implementation,
// four binaries" property for the two serving paths: audit-api and
// audit-governance-worker both resolve the store operation deadline through
// runtimeconfig, so an operator's AUDIT_STORE_OPERATION_TIMEOUT means the same
// thing on both sides of the evidence chain.
func TestOpenStoreAppliesTheSharedPoolConfiguration(t *testing.T) {
	t.Setenv(runtimeconfig.EnvStoreOperationTimeout, "3s")
	t.Setenv(runtimeconfig.EnvPostgresMaxOpenConns, "not-a-number")
	// The pool is resolved right after sql.Open, before the reachability
	// probe, so an unreachable DSN still proves the fail-closed parse: the
	// error must name the variable, not the socket.
	_, err := openStore("", "postgres://audit@127.0.0.1:1/audit?sslmode=disable", log.New(io.Discard, "", 0))
	if err == nil {
		t.Fatal("an invalid pool variable must fail the binary closed")
	}
	if !strings.Contains(err.Error(), runtimeconfig.EnvPostgresMaxOpenConns) {
		t.Fatalf("error %q does not name %s", err, runtimeconfig.EnvPostgresMaxOpenConns)
	}
	// With a valid value the file-backed path still opens, which proves the
	// timeout resolution ran without needing a PostgreSQL server.
	for _, name := range []string{
		runtimeconfig.EnvPostgresMaxOpenConns,
		runtimeconfig.EnvPostgresMaxIdleConns,
		runtimeconfig.EnvPostgresConnMaxLifetime,
		runtimeconfig.EnvPostgresConnMaxIdleTime,
	} {
		t.Setenv(name, "")
	}
	t.Setenv(runtimeconfig.EnvStoreOperationTimeout, "")
	st, err := openStore(t.TempDir()+"/state.json", "", log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if got := st.OperationTimeout(); got != store.DefaultOperationTimeout {
		t.Fatalf("OperationTimeout()=%s, want the shipped default %s", got, store.DefaultOperationTimeout)
	}
}
