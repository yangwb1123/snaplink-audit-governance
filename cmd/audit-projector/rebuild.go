package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/projection"
	"github.com/snaplink/audit-governance/internal/runtimeconfig"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

// Rebuild is deliberately a separate subcommand: it never constructs a Kafka
// producer/consumer, resets group offsets, deletes tables, or executes recovery.
func runRebuildCLI(ctx context.Context, args []string, logger *log.Logger) error {
	flags := flag.NewFlagSet("rebuild", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	tenant := flags.String("tenant", "", "required single tenant")
	state := flags.String("state", envOr("AUDIT_STATE_PATH", "./data/state.json"), "file ledger path (requires exclusive writer lock)")
	pageSize := flags.Int("page-size", store.MaxLedgerPageSize, "cold records per bounded page, at most 100")
	sweeps := flags.Int("sweeps", 2, "zero-origin finite sweeps (late commits require rescan)")
	if err := flags.Parse(args); err != nil {
		return errors.New("invalid rebuild flags")
	}
	if flags.NArg() != 0 || *tenant == "" || domain.ValidTenantIDComponent("tenant_id", *tenant) != nil || *pageSize < 1 || *pageSize > store.MaxLedgerPageSize || *sweeps < 1 {
		return fmt.Errorf("%w: tenant, page-size 1..100 and positive sweeps required", domain.ErrInvalid)
	}
	cfg, external, err := rebuildServiceConfig(os.Getenv)
	if err != nil {
		return err
	}
	// Validate keys/transport before opening the ledger or remote destination.
	svc, err := service.New(nil, cfg)
	if err != nil {
		return err
	}
	external.SigningSecret, external.EncryptionKey = svc.Config.SigningSecret, svc.Config.EncryptionKey
	signer, err := external.Signer()
	if err != nil {
		return err
	}
	if signer != nil {
		svc.Config.Signer = signer
	}
	archive, err := external.Archive()
	if err != nil {
		return err
	}
	svc.Config.Archive = archive
	ledger, err := openRebuildLedger(ctx, *state, os.Getenv("AUDIT_POSTGRES_DSN"))
	if err != nil {
		return err
	}
	defer ledger.Close()
	svc.Store = ledger
	var exists bool
	if err := ledger.ReadControl(ctx, func(data *store.Snapshot) error {
		_, exists = data.Tenants[*tenant]
		return nil
	}); err != nil {
		return errors.New("rebuild ledger unavailable")
	}
	if !exists {
		return domain.ErrNotFound
	}
	destination, err := projection.Open(envOr("AUDIT_CLICKHOUSE_DSN", "clickhouse://audit:audit-local-only@localhost:19000/audit"))
	if err != nil {
		return errors.New("rebuild destination unavailable")
	}
	defer destination.Close()
	if err := destination.EnsureSchema(ctx); err != nil {
		return errors.New("rebuild destination schema unavailable")
	}
	return rebuildSweeps(ctx, svc, destination, *tenant, *pageSize, *sweeps, logger)
}

func rebuildSweeps(ctx context.Context, svc *service.Service, destination service.ProjectionWriter, tenantID string, pageSize, sweeps int, logger *log.Logger) error {
	failures := 0
	for sweep := 0; sweep < sweeps; sweep++ {
		cursor := store.LedgerPageCursor{}
		for {
			result, err := svc.RebuildProjectionPage(ctx, tenantID, cursor, pageSize, destination)
			if err != nil {
				// Never print remote errors, DSNs, archive keys or object content.
				return errors.New("projection rebuild interrupted; rerun safely from zero")
			}
			failures += result.Failed
			if logger != nil {
				logger.Printf("rebuild sweep=%d scanned=%d writes=%d evidence_failures=%d done=%t", sweep+1, result.Scanned, result.Written, result.Failed, result.Done)
			}
			if result.Done {
				break
			}
			cursor = result.Next
		}
	}
	if failures > 0 {
		return errors.New("projection rebuild has unavailable evidence; not complete")
	}
	if logger != nil {
		logger.Printf("rebuild sweeps_finished=%d completeness=not_asserted; compare current stream watermarks against ledger heads", sweeps)
	}
	return nil
}

func openRebuildLedger(ctx context.Context, statePath, dsn string) (*store.Store, error) {
	timeout, err := runtimeconfig.ResolveStoreOperationTimeout(store.DefaultOperationTimeout)
	if err != nil {
		return nil, err
	}
	if dsn == "" {
		if _, err := os.Stat(statePath); err != nil {
			return nil, errors.New("rebuild requires an existing file ledger")
		}
		return store.OpenWithOptions(statePath, store.Options{OperationTimeout: timeout})
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, errors.New("invalid rebuild ledger connection configuration")
	}
	if err := runtimeconfig.ApplyPostgresPoolConfigFromEnv(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := runtimeconfig.PingPostgresStartup(ctx, db); err != nil {
		_ = db.Close()
		return nil, errors.New("rebuild ledger unavailable")
	}
	ledger, err := store.OpenPostgresWithOptions(db, store.Options{OperationTimeout: timeout})
	if err != nil {
		_ = db.Close()
		return nil, errors.New("rebuild ledger layout unavailable")
	}
	return ledger, nil
}

func rebuildServiceConfig(getenv func(string) string) (service.Config, runtimeconfig.SigningArchive, error) {
	cfg := service.Config{SigningSecret: getenv(runtimeconfig.EnvSigningSecret), EncryptionKey: getenv(runtimeconfig.EnvEncryptionKey)}
	external := runtimeconfig.SigningArchive{
		ArchiveDir: envOrWith(getenv, "AUDIT_ARCHIVE_DIR", "./data/archive"),
		VaultAddr:  getenv("AUDIT_VAULT_ADDR"), VaultToken: getenv("AUDIT_VAULT_TOKEN"), VaultTransitKey: getenv("AUDIT_VAULT_TRANSIT_KEY"),
		S3Endpoint: getenv("AUDIT_S3_ENDPOINT"), S3Bucket: getenv("AUDIT_S3_BUCKET"), S3AccessKey: getenv("AUDIT_S3_ACCESS_KEY"), S3SecretKey: getenv("AUDIT_S3_SECRET_KEY"),
	}
	for name, target := range map[string]*bool{
		runtimeconfig.EnvDevSecrets:                 &cfg.AllowDevSecrets,
		runtimeconfig.EnvS3UseSSL:                   &external.S3UseSSL,
		runtimeconfig.EnvAllowInsecureVaultLoopback: &external.AllowInsecureVaultLoopback,
	} {
		if value := getenv(name); value != "" {
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return cfg, external, fmt.Errorf("invalid boolean for %s", name)
			}
			*target = parsed
		}
	}
	if value := getenv(runtimeconfig.EnvArchiveRetentionDays); value != "" {
		parsed, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return cfg, external, fmt.Errorf("invalid positive duration for %s", runtimeconfig.EnvArchiveRetentionDays)
		}
		external.ArchiveRetentionDays = uint(parsed)
	}
	cfg.ArchiveDir = external.ArchiveDir
	return cfg, external, nil
}
