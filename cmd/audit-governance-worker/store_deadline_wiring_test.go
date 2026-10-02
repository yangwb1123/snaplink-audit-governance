package main

// The worker's half of the shared store/pool configuration.
//
// The worker seals segments and archives pending events against the SAME
// control-plane store as the API, so the two binaries must resolve
// AUDIT_STORE_OPERATION_TIMEOUT and the four pool knobs through one
// implementation. A worker that opened a differently-configured pool or a
// differently-bounded store would make an operator's incident unreproducible
// across the two halves of the evidence chain.

import (
	"io"
	"log"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/runtimeconfig"
	"github.com/snaplink/audit-governance/internal/store"
)

func TestWorkerOpenStoreAppliesTheSharedPoolConfiguration(t *testing.T) {
	t.Setenv(runtimeconfig.EnvPostgresMaxIdleConns, "not-a-number")
	_, err := openStore("", "postgres://audit@127.0.0.1:1/audit?sslmode=disable", log.New(io.Discard, "", 0))
	if err == nil {
		t.Fatal("an invalid pool variable must fail the worker closed")
	}
	if !strings.Contains(err.Error(), runtimeconfig.EnvPostgresMaxIdleConns) {
		t.Fatalf("error %q does not name %s", err, runtimeconfig.EnvPostgresMaxIdleConns)
	}

	for _, name := range []string{
		runtimeconfig.EnvPostgresMaxOpenConns,
		runtimeconfig.EnvPostgresMaxIdleConns,
		runtimeconfig.EnvPostgresConnMaxLifetime,
		runtimeconfig.EnvPostgresConnMaxIdleTime,
		runtimeconfig.EnvStoreOperationTimeout,
	} {
		t.Setenv(name, "")
	}
	st, err := openStore(t.TempDir()+"/state.json", "", log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if got := st.OperationTimeout(); got != store.DefaultOperationTimeout {
		t.Fatalf("OperationTimeout()=%s, want the shipped default %s", got, store.DefaultOperationTimeout)
	}
}
