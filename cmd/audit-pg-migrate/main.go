// Command audit-pg-migrate performs the explicit PostgreSQL v1 to v2
// hot/cold cutover. It is intentionally separate from service startup: an
// operator must stop writers, apply migration 006, and acknowledge the
// destructive-looking (but backup-protected) cutover explicitly.
package main

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/snaplink/audit-governance/internal/store"
)

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
	db.SetConnMaxLifetime(10 * time.Minute)
	if err := db.Ping(); err != nil {
		log.Fatalf("ping PostgreSQL: %v", err)
	}
	if err := store.MigratePostgresSnapshot(db); err != nil {
		log.Fatalf("migrate PostgreSQL snapshot: %v", err)
	}
	log.Print("PostgreSQL hot/cold cutover complete; keep audit_state_snapshot_v1_backup until validation finishes")
}
