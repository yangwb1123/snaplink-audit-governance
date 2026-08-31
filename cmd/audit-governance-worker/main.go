package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
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

// archiveReadyTimeout bounds every archive readiness probe (startup,
// -check-config and each evaluate pass) so a hung or black-holed S3 endpoint
// can never block startup or a pass indefinitely. Single named constant for
// all probe sites (REQ-7), mirroring openStore's 10 s ping precedent.
const archiveReadyTimeout = 5 * time.Second

// archivePassTimeout bounds one complete governance pass (all tenants, all
// archive writes). A black-holed S3 endpoint that passes the 5s Ready probe
// must be able to stall the pass for at most this long: Put is ctx-threaded
// (REQ-2), so the deadline cancels in-flight Stat/Put/verify round trips
// instead of waiting out the transport timeouts. The effective bound is
// min(signal ctx, archivePassTimeout): SIGINT/SIGTERM still wins earlier.
// Healthy passes finish in well under this bound; the default 5m interval
// retries a timed-out pass on the next tick.
const archivePassTimeout = 2 * time.Minute

// newArchiveStore builds the configured archive store. Package-level seam for
// tests: worker tests substitute a store built on a scripted S3 client
// (archive.NewS3StoreWithClient) to drive runCheckConfig and the startup
// probe without a real endpoint. Production behavior is exactly
// runtimeconfig.SigningArchive.Archive.
var newArchiveStore = func(external runtimeconfig.SigningArchive) (archive.Store, error) {
	return external.Archive()
}

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
	s3UseSSL := flag.Bool("s3-use-ssl", strictBoolEnv(runtimeconfig.EnvS3UseSSL, false), "use TLS for the S3-compatible archive endpoint; plaintext is local-development-only (AUDIT_S3_USE_SSL)")
	// archiveRetentionDays is the per-object COMPLIANCE retention duration in
	// days applied by every S3 archive Put (F1 leg (i), mandatory for an S3
	// archive: a zero value makes runtimeconfig.SigningArchive.Archive fail
	// closed, so the worker can never write objects without explicit
	// retention). Shared with audit-api via runtimeconfig.EnvArchiveRetentionDays.
	archiveRetentionDays := flag.Uint("archive-retention-days", uintEnv(runtimeconfig.EnvArchiveRetentionDays), "per-object COMPLIANCE retention for the S3 archive in days (AUDIT_ARCHIVE_RETENTION_DAYS; required when an S3 archive is configured, e.g. 365)")
	allowInsecureVaultLoopback := flag.Bool("allow-insecure-vault-loopback", strictBoolEnv(runtimeconfig.EnvAllowInsecureVaultLoopback, false), "allow plaintext HTTP Vault only on loopback hosts (AUDIT_ALLOW_INSECURE_VAULT_LOOPBACK)")
	allowDevSecrets := flag.Bool("allow-dev-secrets", boolEnv(runtimeconfig.EnvDevSecrets, false), "enable well-known development signing/encryption secrets; never enable in production")
	checkConfig := flag.Bool("check-config", false, "validate secrets and external signing/archive configuration, probe the archive destination (bounded; S3 touches the network), then exit without opening the state store or binding listeners")
	consistencyKeyMode := flag.Bool("consistency-key", false, "resolve and print the non-secret signer/archive consistency key without opening the store or probing external services")
	interval := flag.Duration("interval", durationEnv("AUDIT_GOVERNANCE_INTERVAL", 5*time.Minute), "retention evaluation interval")
	stuckExportAge := flag.Duration("stuck-export-age", durationEnv("AUDIT_GOVERNANCE_STUCK_EXPORT_AGE", service.DefaultStuckExportAge), "fail export jobs stuck in running past this age (0 = default)")
	once := flag.Bool("once", false, "evaluate once and exit")
	flag.Parse()
	if *interval <= 0 {
		log.Fatalf("interval must be positive")
	}
	logger := log.New(os.Stdout, "audit-governance-worker ", log.LstdFlags|log.Lmicroseconds)
	if *allowDevSecrets {
		logger.Printf("warning=development_secrets_enabled")
	}
	// A governance interval shorter than the per-pass deadline means a pass
	// that legitimately needs most of its bound gets restarted before its
	// work converges (healthy-but-slow passes starve). Warn at startup — the
	// operator chose the interval, and the pass bound stays a hard ceiling.
	if *interval < archivePassTimeout {
		logger.Printf("warning=interval_below_pass_timeout interval=%s pass_timeout=%s", *interval, archivePassTimeout)
	}
	// AUDIT_AGGREGATE_CHECKPOINT_HISTORY caps retained aggregate-checkpoint
	// history per tenant (drop-oldest; default 1000, floor 1). Invalid or
	// non-positive values fall back to the default with a warning so a typo
	// never disables retention or kills the worker.
	aggregateRetention := intEnv("AUDIT_AGGREGATE_CHECKPOINT_HISTORY", service.DefaultAggregateCheckpointRetention)
	if raw := os.Getenv("AUDIT_AGGREGATE_CHECKPOINT_HISTORY"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err != nil || parsed <= 0 {
			logger.Printf("warning=invalid_aggregate_checkpoint_history value=%q using_default=%d", raw, aggregateRetention)
		}
	}
	cfg := service.Config{ArchiveDir: *archiveDir, SigningSecret: os.Getenv(runtimeconfig.EnvSigningSecret), EncryptionKey: os.Getenv(runtimeconfig.EnvEncryptionKey), AllowDevSecrets: *allowDevSecrets, AggregateCheckpointRetention: aggregateRetention, StuckExportAge: *stuckExportAge}
	external := runtimeconfig.SigningArchive{ArchiveDir: *archiveDir, VaultAddr: *vaultAddr, VaultToken: *vaultToken, VaultTransitKey: *vaultTransitKey, S3Endpoint: *s3Endpoint, S3Bucket: *s3Bucket, S3AccessKey: *s3AccessKey, S3SecretKey: *s3SecretKey, S3UseSSL: *s3UseSSL, ArchiveRetentionDays: *archiveRetentionDays, AllowInsecureVaultLoopback: *allowInsecureVaultLoopback}
	if *consistencyKeyMode {
		os.Exit(runConsistencyKey(logger, cfg, external))
	}
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
	archiveStore, archiveErr := newArchiveStore(external)
	if archiveErr != nil {
		logger.Fatalf("archive: %v", archiveErr)
	}
	svc.Config.Archive = archiveStore
	if *s3Endpoint != "" {
		logger.Printf("archive=s3 bucket=%s", *s3Bucket)
	}
	key, keyErr := external.ConsistencyKey()
	if keyErr != nil {
		logger.Fatalf("consistency_key: %v", keyErr)
	}
	logger.Printf("consistency_key=%s", key)
	// REQ-1: probe the archive destination before the first pass (also in
	// -once mode). A misconfigured destination is a boot error, not a
	// per-pass surprise: with S3 the bucket must exist with Object Lock and
	// versioning enabled; with a local archive the dir must be writable.
	if err := probeArchiveReady(svc.Config.Archive, logger); err != nil {
		logger.Fatalf("archive ready: %v", err)
	}
	// REQ-3: the pass context is cancelled by SIGINT/SIGTERM so in-flight
	// Vault sign calls abort promptly instead of blocking on the client
	// timeout. stop() must be released (async F4: signal.NotifyContext's
	// internal registration would otherwise leak for the process lifetime).
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	evaluate := func() { runEvaluatePass(ctx, logger, svc) }
	evaluate()
	if *once {
		return
	}
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			evaluate()
		case <-ctx.Done():
			_ = st.Flush()
			return
		}
	}
}

// runEvaluatePass executes one governance pass over all tenants. The archive
// readiness probe runs once per pass (never per tenant), before any archiving:
// when the configured destination is not WORM-ready, the archiving step is
// skipped for every tenant — no receipt is marked StatusArchived, no
// Store.Update window is opened and the tenant's conflict counter is not
// reset — while SealPendingSegments, CreateAggregateCheckpoint and
// EvaluateRetention proceed unchanged. The failure is surfaced once per pass
// (the probe line) and once per tenant (the skip line). When the probe
// passes, the pass is byte-identical to the pre-probe behavior.
func runEvaluatePass(ctx context.Context, logger *log.Logger, svc *service.Service) {
	// REQ-1: bound the whole pass. WithTimeout on an already-cancelled or
	// already-deadlined parent yields the earlier bound, so SIGINT/SIGTERM
	// still wins and tests shrink the effective deadline via a short parent
	// ctx without touching the production constant.
	ctx, cancel := context.WithTimeout(ctx, archivePassTimeout)
	defer cancel()
	if err := svc.Store.Ready(ctx); err != nil {
		logger.Printf("store_not_ready=%v", err)
		return
	}
	tenants, listErr := svc.ListTenants()
	if listErr != nil {
		logger.Printf("list tenants: %v", listErr)
		return
	}
	archiveReady := probeArchiveReady(svc.Config.Archive, logger) == nil
	for _, tenant := range tenants {
		if resumed, resumeErr := svc.ResumePendingExports(tenant.ID); resumeErr != nil {
			logger.Printf("tenant=%s pending_export_resume_error=%v", tenant.ID, resumeErr)
		} else {
			logger.Printf("tenant=%s pending_exports_resumed=%d", tenant.ID, resumed)
		}
		// Stuck-export recovery runs first, outside the archiveReady gate:
		// it performs no archive I/O, so it behaves identically whether or not
		// the readiness probe passed. The line is emitted unconditionally on
		// success (including =0) so operators can confirm the step ran.
		if recovered, recErr := svc.RecoverStuckExports(tenant.ID); recErr != nil {
			logger.Printf("tenant=%s export_recovery_error=%v", tenant.ID, recErr)
		} else {
			logger.Printf("tenant=%s stuck_exports_recovered=%d", tenant.ID, recovered)
		}
		if err := svc.SealPendingSegments(ctx, tenant.ID); err != nil {
			logger.Printf("tenant=%s checkpoint_error=%v", tenant.ID, err)
		}
		if err := svc.CreateAggregateCheckpoint(ctx, tenant.ID); err != nil {
			logger.Printf("tenant=%s aggregate_checkpoint_error=%v", tenant.ID, err)
		}
		if !archiveReady {
			logger.Printf("tenant=%s archive_skipped=ready_probe_failed", tenant.ID)
		} else if archived, archiveErr := svc.ArchivePending(ctx, tenant.ID); archiveErr != nil {
			handleArchiveError(logger, svc, tenant.ID, archived, archiveErr)
		} else {
			// Dead-lettered objects are a handled outcome, not a pass error:
			// log the outstanding count so operators see the FM-1/F-2 state
			// on the normal success line (ListDeadLetters lists them).
			deadLetters, _ := svc.ListDeadLetters(tenant.ID)
			logger.Printf("tenant=%s archived=%d dead_lettered=%d", tenant.ID, archived, len(deadLetters))
		}
		report, reportErr := svc.EvaluateRetention(tenant.ID, time.Time{})
		if reportErr != nil {
			logger.Printf("tenant=%s retention_error=%v", tenant.ID, reportErr)
		} else {
			logger.Printf("tenant=%s eligible=%d protected=%d action=%s", tenant.ID, report.EligibleEvents, report.ProtectedEvents, report.Action)
		}
		// Between-tenant abort (REQ-1): the in-flight tenant's failure has
		// already surfaced through handleArchiveError / checkpoint_error /
		// aggregate_checkpoint_error; do not run further tenants against a dead
		// context (each step would only fail fast). End-of-iteration placement
		// is deliberate: the first tenant still runs its full step sequence on
		// an already-cancelled context (pinned by
		// TestRunEvaluatePassAbortsOnCancelledContext and
		// TestRunEvaluatePassAbortsBetweenTenants).
		if ctx.Err() != nil {
			logger.Printf("pass_aborted=%v", ctx.Err())
			return
		}
	}
}

// probeArchiveReady probes a configured archive destination under
// archiveReadyTimeout and logs the outcome. Unconfigured destinations (nil or
// an empty-dir FileStore) are skipped, mirroring /readyz via archive.Configured
// (OQ-1). The caller decides the failure handling: main fails fast at startup,
// runCheckConfig exits non-zero, runEvaluatePass skips archiving for the pass.
func probeArchiveReady(store archive.Store, logger *log.Logger) error {
	if !archive.Configured(store) {
		logger.Printf("archive_ready=skipped")
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), archiveReadyTimeout)
	defer cancel()
	if err := store.Ready(ctx); err != nil {
		logger.Printf("archive_ready=failed store=%T error=%v", store, err)
		return err
	}
	logger.Printf("archive_ready=ok")
	return nil
}

// handleArchiveError logs a failed archive pass and persists the per-tenant
// exhausted-retry signal: when the pass aborted on ErrSnapshotConflict, the
// tenant's counter is incremented (best-effort — a failed counter write is
// logged separately and never masks the pass error) so the exhaustion
// survives restarts and is queryable after the fact; the next successful
// pass resets it inside its atomic batch commit.
func handleArchiveError(logger *log.Logger, svc *service.Service, tenantID string, archived int, archiveErr error) {
	if errors.Is(archiveErr, store.ErrSnapshotConflict) {
		if recErr := svc.RecordArchivePassConflict(tenantID); recErr != nil {
			logger.Printf("tenant=%s archive_conflict_record_error=%v", tenantID, recErr)
		}
	}
	failures, _ := svc.ArchivePassConflictFailures(tenantID)
	logger.Printf("tenant=%s archive_error=%v archived=%d conflict_failures=%d", tenantID, archiveErr, archived, failures)
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
		readyCtx, readyCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer readyCancel()
		if err := st.Ready(readyCtx); err != nil {
			_ = st.Close()
			return nil, fmt.Errorf("postgres store is not ready: %w", err)
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

// strictBoolEnv parses an environment variable as a boolean for the two
// security-relevant transport knobs (AUDIT_S3_USE_SSL,
// AUDIT_ALLOW_INSECURE_VAULT_LOOPBACK). Unset or empty values fall back to
// fallback; a present-but-malformed value exits 1 naming the variable before
// flag.Parse, so there is no fail-open path to plaintext (REQ-TLS-7). The
// worker's other booleans keep the lenient boolEnv behavior.
func strictBoolEnv(name string, fallback bool) bool {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		fmt.Fprintf(os.Stderr, "audit-governance-worker: %s=%q is not a valid boolean\n", name, value)
		os.Exit(1)
	}
	return parsed
}

// runConsistencyKey resolves only the signer/archive identity needed by the
// cross-process parity gate. It deliberately skips authentication, state-store
// and archive-readiness checks; Transport() validates pure endpoint syntax
// without constructing a client or making a network call.
func runConsistencyKey(logger *log.Logger, cfg service.Config, external runtimeconfig.SigningArchive) int {
	svc, err := service.New(nil, cfg)
	if err != nil {
		logger.Printf("invalid secrets: %v", err)
		return 1
	}
	external.SigningSecret = svc.Config.SigningSecret
	external.EncryptionKey = svc.Config.EncryptionKey
	if _, _, err := external.Transport(); err != nil {
		logger.Printf("consistency_key: %v", err)
		return 1
	}
	key, err := external.ConsistencyKey()
	if err != nil {
		logger.Printf("consistency_key: %v", err)
		return 1
	}
	logger.Printf("consistency_key=%s", key)
	return 0
}

// runCheckConfig validates the resolved configuration without opening the
// state store or binding listeners. service.New performs the secret fail-fast
// checks (and resolves dev defaults), then the external signer/archive
// configuration is validated; finally the configured archive destination is
// probed with Ready under a bounded timeout (archiveReadyTimeout) — for S3
// this touches the network, for a local archive the filesystem. Output prints
// names and lengths only, never secret values, so CI can compare API and
// worker outputs.
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
	archiveStore, archiveErr := newArchiveStore(external)
	if archiveErr != nil {
		logger.Printf("archive: %v", archiveErr)
		return 1
	}
	// REQ-2: probe the destination before reporting ok; check_config=ok is
	// printed only when the configured archive is WORM-ready.
	if err := probeArchiveReady(archiveStore, logger); err != nil {
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
	// Per-leg transport labels (REQ-TLS-6/F3): the S3 and Vault legs are each
	// reported ("tls"/"http"/"local"), byte-identical in shape to the API's
	// line. A Transport() error is a check failure: the line is never printed
	// with an unresolved transport (FM-8).
	s3Transport, vaultTransport, transportErr := external.Transport()
	if transportErr != nil {
		logger.Printf("transport: %v", transportErr)
		return 1
	}
	// transport_grpc is the gRPC ingest listener's transport label; the worker
	// has no gRPC listener, so it is unconditionally "disabled", keeping the
	// ok-line field set/order byte-identical in shape to the API's line (C6:
	// CI compares API and worker outputs).
	consistencyKey, keyErr := external.ConsistencyKey()
	if keyErr != nil {
		logger.Printf("consistency_key: %v", keyErr)
		return 1
	}
	logger.Printf("check_config=ok signing_secret_length=%d encryption_key_length=%d signer=%s archive=%s transport_s3=%s transport_vault=%s transport_grpc=%s consistency_key=%s", len(svc.Config.SigningSecret), len(svc.Config.EncryptionKey), signerName, archiveName, s3Transport, vaultTransport, "disabled", consistencyKey)
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

// uintEnv parses a non-negative integer environment variable, falling back
// to 0 (which the S3 retention check then treats as unset/fail-closed) when
// the value is missing, not an integer, or negative. A malformed value must
// never kill the worker or bypass the fail-closed retention requirement.
func uintEnv(name string) uint {
	value := os.Getenv(name)
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0
	}
	return uint(parsed)
}

// intEnv parses an integer environment variable, falling back to the given
// default when the value is missing, not an integer, or non-positive. It
// mirrors durationEnv/boolEnv: a malformed value must never kill the worker
// or disable a guard.
func intEnv(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
