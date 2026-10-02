// Command audit-pg-migrate performs the explicit PostgreSQL v1 to v2
// hot/cold cutover. It is intentionally separate from service startup: an
// operator must stop writers, apply migration 006, and acknowledge the
// destructive-looking (but backup-protected) cutover explicitly.
package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/snaplink/audit-governance/internal/runtimeconfig"
	"github.com/snaplink/audit-governance/internal/store"
)

// pgMigrateTimeout bounds the one-shot cutover. It is deliberately larger
// than store.DefaultOperationTimeout: a full ledger conversion is a bounded
// but potentially long operation, and an operator can raise it further with
// AUDIT_STORE_OPERATION_TIMEOUT.
const pgMigrateTimeout = 30 * time.Minute

func main() {
	dsn := flag.String("postgres-dsn", os.Getenv("AUDIT_POSTGRES_DSN"), "PostgreSQL DSN (defaults to AUDIT_POSTGRES_DSN)")
	confirm := flag.String("confirm", "", "must be MIGRATE to execute the cutover")
	flag.Parse()
	if *dsn == "" {
		log.Fatal("postgres-dsn is required")
	}
	if *confirm != "MIGRATE" {
		log.Fatal("refusing PostgreSQL cutover: pass -confirm MIGRATE after stopping all writers and applying migration 006")
	}
	db, err := sql.Open("pgx", *dsn)
	if err != nil {
		log.Fatalf("open PostgreSQL: %v", err)
	}
	defer db.Close()
	// The cutover command shares the servers' pool configuration, so it can
	// never run with a different pool than the processes it cuts over. The
	// former hard-coded SetConnMaxLifetime(10m) is replaced by
	// AUDIT_POSTGRES_CONN_MAX_LIFETIME (rollback: set it to 10m); the value
	// is immaterial for a one-shot command either way.
	if err := runtimeconfig.ApplyPostgresPoolConfigFromEnv(db); err != nil {
		log.Fatalf("database pool: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtimeconfig.PingPostgresStartup(ctx, db); err != nil {
		log.Fatalf("ping PostgreSQL: %v", err)
	}
	// The cutover is a bounded, one-shot operation with no request deadline
	// of its own; give it a generous explicit store-operation timeout so a
	// large conversion is not aborted at the serving default.
	operationTimeout, err := runtimeconfig.ResolveStoreOperationTimeout(pgMigrateTimeout)
	if err != nil {
		log.Fatalf("store operation timeout: %v", err)
	}
	cutoverCtx, cutoverCancel := context.WithTimeout(ctx, operationTimeout)
	defer cutoverCancel()
	if err := store.MigratePostgresSnapshot(cutoverCtx, db); err != nil {
		log.Fatalf("migrate PostgreSQL snapshot: %v", err)
	}
	log.Print("PostgreSQL hot/cold cutover complete; keep audit_state_snapshot_v1_backup until validation finishes")
}
