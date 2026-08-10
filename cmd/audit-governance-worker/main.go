package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/runtimeconfig"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
)

func main() {
	statePath := flag.String("state", envOr("AUDIT_STATE_PATH", "./data/state.json"), "state snapshot path")
	postgresDSN := flag.String("postgres-dsn", os.Getenv("AUDIT_POSTGRES_DSN"), "PostgreSQL DSN for the control-plane state snapshot; overrides -state")
	archiveDir := flag.String("archive", envOr("AUDIT_ARCHIVE_DIR", "./data/archive"), "archive directory")
	vaultAddr := flag.String("vault-addr", os.Getenv("AUDIT_VAULT_ADDR"), "HashiCorp Vault address for Transit checkpoint signatures (must match audit-api)")
	vaultToken := flag.String("vault-token", os.Getenv("AUDIT_VAULT_TOKEN"), "Vault token for the Transit signer")
	vaultTransitKey := flag.String("vault-transit-key", os.Getenv("AUDIT_VAULT_TRANSIT_KEY"), "Vault Transit key name")
	s3Endpoint := flag.String("s3-endpoint", os.Getenv("AUDIT_S3_ENDPOINT"), "S3-compatible endpoint for the compliance archive (must match audit-api)")
	s3Bucket := flag.String("s3-bucket", os.Getenv("AUDIT_S3_BUCKET"), "S3 bucket for the compliance archive")
	s3AccessKey := flag.String("s3-access-key", os.Getenv("AUDIT_S3_ACCESS_KEY"), "S3 access key")
	s3SecretKey := flag.String("s3-secret-key", os.Getenv("AUDIT_S3_SECRET_KEY"), "S3 secret key")
	allowDevSecrets := flag.Bool("allow-dev-secrets", boolEnv(runtimeconfig.EnvDevSecrets, false), "enable well-known development signing/encryption secrets; never enable in production")
	checkConfig := flag.Bool("check-config", false, "validate secrets and external signing/archive configuration, then exit without opening the store or network")
	interval := flag.Duration("interval", durationEnv("AUDIT_GOVERNANCE_INTERVAL", 5*time.Minute), "retention evaluation interval")
	once := flag.Bool("once", false, "evaluate once and exit")
	flag.Parse()
	if *interval <= 0 {
		log.Fatalf("interval must be positive")
	}
	logger := log.New(os.Stdout, "audit-governance-worker ", log.LstdFlags|log.Lmicroseconds)
	if *allowDevSecrets {
		logger.Printf("warning=development_secrets_enabled")
	}
	cfg := service.Config{ArchiveDir: *archiveDir, SigningSecret: os.Getenv(runtimeconfig.EnvSigningSecret), EncryptionKey: os.Getenv(runtimeconfig.EnvEncryptionKey), AllowDevSecrets: *allowDevSecrets}
	external := runtimeconfig.SigningArchive{ArchiveDir: *archiveDir, VaultAddr: *vaultAddr, VaultToken: *vaultToken, VaultTransitKey: *vaultTransitKey, S3Endpoint: *s3Endpoint, S3Bucket: *s3Bucket, S3AccessKey: *s3AccessKey, S3SecretKey: *s3SecretKey}
	if *checkConfig {
		os.Exit(runCheckConfig(logger, cfg, external))
	}
	st, err := openStore(*statePath, *postgresDSN, logger)
	if err != nil {
		logger.Fatalf("open store: %v", err)
	}
	defer st.Close()
	svc, err := service.New(st, cfg)
	if err != nil {
		logger.Fatalf("invalid secrets: %v", err)
	}
	logSecretWarnings(logger, svc.Config)
	external.SigningSecret = svc.Config.SigningSecret
	external.EncryptionKey = svc.Config.EncryptionKey
	if signer, signErr := external.Signer(); signErr != nil {
		logger.Fatalf("signer: %v", signErr)
	} else if signer != nil {
		svc.Config.Signer = signer
		logger.Printf("signer=vault-transit key=%s", *vaultTransitKey)
	}
	if archiveStore, archiveErr := external.Archive(); archiveErr != nil {
		logger.Fatalf("archive: %v", archiveErr)
	} else {
		svc.Config.Archive = archiveStore
		if *s3Endpoint != "" {
			logger.Printf("archive=s3 bucket=%s", *s3Bucket)
		}
	}
	evaluate := func() {
		tenants, listErr := svc.ListTenants()
		if listErr != nil {
			logger.Printf("list tenants: %v", listErr)
			return
		}
		for _, tenant := range tenants {
			if err := svc.SealPendingSegments(tenant.ID); err != nil {
				logger.Printf("tenant=%s checkpoint_error=%v", tenant.ID, err)
			}
			if err := svc.CreateAggregateCheckpoint(tenant.ID); err != nil {
				logger.Printf("tenant=%s aggregate_checkpoint_error=%v", tenant.ID, err)
			}
			archived, archiveErr := svc.ArchivePending(tenant.ID)
			if archiveErr != nil {
				// Persisted exhaustion signal: the per-tenant counter survives
				// restarts and is reset by the next successful pass. The
				// increment is best-effort — a failed counter write must not
				// mask the pass failure.
				if errors.Is(archiveErr, store.ErrSnapshotConflict) {
					if recErr := svc.RecordArchivePassConflict(tenant.ID); recErr != nil {
						logger.Printf("tenant=%s archive_conflict_record_error=%v", tenant.ID, recErr)
					}
				}
				failures, _ := svc.ArchivePassConflictFailures(tenant.ID)
				logger.Printf("tenant=%s archive_error=%v archived=%d conflict_failures=%d", tenant.ID, archiveErr, archived, failures)
			}
			report, reportErr := svc.EvaluateRetention(tenant.ID, time.Time{})
			if reportErr != nil {
				logger.Printf("tenant=%s retention_error=%v", tenant.ID, reportErr)
				continue
			}
			logger.Printf("tenant=%s eligible=%d protected=%d action=%s", tenant.ID, report.EligibleEvents, report.ProtectedEvents, report.Action)
		}
	}
	evaluate()
	if *once {
		return
	}
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-ticker.C:
			evaluate()
		case <-stop:
			_ = st.Flush()
			return
		}
	}
}

func openStore(statePath, postgresDSN string, logger *log.Logger) (*store.Store, error) {
	if postgresDSN != "" {
		// Migration hazard guard: an existing non-empty file ledger must not
		// silently vanish behind an empty PostgreSQL snapshot (missing row ==
		// fresh deployment on the PG backend). Explicit opt-out documented in
		// the error message.
		if os.Getenv("AUDIT_ALLOW_PG_EMPTY_LEDGER") != "true" {
			if err := store.CheckFileToPostgresMigrationHazard(statePath); err != nil {
				return nil, err
			}
		}
		db, err := sql.Open("pgx", postgresDSN)
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			_ = db.Close()
			return nil, err
		}
		st, err := store.OpenPostgres(db)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		logger.Printf("state_backend=postgres")
		return st, nil
	}
	logger.Printf("state_backend=file path=%s", statePath)
	return store.Open(statePath)
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func boolEnv(name string, fallback bool) bool {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

// runCheckConfig validates the resolved configuration without opening the
// store or touching the network: service.New performs the secret fail-fast
// checks (and resolves dev defaults), then the external signer/archive
// configuration is validated. Output prints names and lengths only, never
// secret values, so CI can compare API and worker outputs.
func runCheckConfig(logger *log.Logger, cfg service.Config, external runtimeconfig.SigningArchive) int {
	svc, err := service.New(nil, cfg)
	if err != nil {
		logger.Printf("invalid secrets: %v", err)
		return 1
	}
	logSecretWarnings(logger, svc.Config)
	external.SigningSecret = svc.Config.SigningSecret
	external.EncryptionKey = svc.Config.EncryptionKey
	signer, signErr := external.Signer()
	if signErr != nil {
		logger.Printf("signer: %v", signErr)
		return 1
	}
	archiveStore, archiveErr := external.Archive()
	if archiveErr != nil {
		logger.Printf("archive: %v", archiveErr)
		return 1
	}
	signerName := "hmac-sha256"
	if signer != nil {
		signerName = signer.Algorithm()
	}
	archiveName := "file"
	if _, ok := archiveStore.(*archive.FileStore); !ok {
		archiveName = "s3"
	}
	logger.Printf("check_config=ok signing_secret_length=%d encryption_key_length=%d signer=%s archive=%s", len(svc.Config.SigningSecret), len(svc.Config.EncryptionKey), signerName, archiveName)
	return 0
}

// logSecretWarnings reports loudly whenever a well-known development secret
// is actually in use, so operators cannot miss the opt-in.
func logSecretWarnings(logger *log.Logger, cfg service.Config) {
	for _, secret := range service.KnownDefaultSecrets() {
		if secret == cfg.SigningSecret {
			logger.Printf("warning=signing_secret=well-known-default")
		}
		if secret == cfg.EncryptionKey {
			logger.Printf("warning=encryption_key=well-known-default")
		}
	}
}

func durationEnv(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
