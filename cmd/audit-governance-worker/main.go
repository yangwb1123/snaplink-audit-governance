package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

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
	interval := flag.Duration("interval", durationEnv("AUDIT_GOVERNANCE_INTERVAL", 5*time.Minute), "retention evaluation interval")
	once := flag.Bool("once", false, "evaluate once and exit")
	flag.Parse()
	if *interval <= 0 {
		log.Fatalf("interval must be positive")
	}
	logger := log.New(os.Stdout, "audit-governance-worker ", log.LstdFlags|log.Lmicroseconds)
	st, err := openStore(*statePath, *postgresDSN, logger)
	if err != nil {
		logger.Fatalf("open store: %v", err)
	}
	defer st.Close()
	svc := service.New(st, service.Config{ArchiveDir: *archiveDir, SigningSecret: envOr("AUDIT_SIGNING_SECRET", "development-signing-key-change-me"), EncryptionKey: envOr("AUDIT_ENCRYPTION_KEY", "development-encryption-key-change-me")})
	external := runtimeconfig.SigningArchive{ArchiveDir: *archiveDir, SigningSecret: svc.Config.SigningSecret, VaultAddr: *vaultAddr, VaultToken: *vaultToken, VaultTransitKey: *vaultTransitKey, S3Endpoint: *s3Endpoint, S3Bucket: *s3Bucket, S3AccessKey: *s3AccessKey, S3SecretKey: *s3SecretKey}
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
				logger.Printf("tenant=%s archive_error=%v archived=%d", tenant.ID, archiveErr, archived)
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
