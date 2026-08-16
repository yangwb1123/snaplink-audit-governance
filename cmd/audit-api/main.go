package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/snaplink/audit-governance/internal/archive"
	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/grpcapi"
	"github.com/snaplink/audit-governance/internal/httpapi"
	"github.com/snaplink/audit-governance/internal/runtimeconfig"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
	"github.com/snaplink/audit-governance/internal/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
)

// envDevAuth is the environment variable that both sets the -allow-dev-auth
// default and, alone, satisfies the check-config dev-auth allowlist (AC-3:
// the flag deliberately cannot satisfy it).
const envDevAuth = "AUDIT_ALLOW_DEV_AUTH"

func main() {
	listen := flag.String("listen", envOr("AUDIT_LISTEN", ":8089"), "HTTP listen address")
	statePath := flag.String("state", envOr("AUDIT_STATE_PATH", "./data/state.json"), "local state snapshot path")
	postgresDSN := flag.String("postgres-dsn", os.Getenv("AUDIT_POSTGRES_DSN"), "PostgreSQL DSN for the control-plane state snapshot; overrides -state")
	archiveDir := flag.String("archive", envOr("AUDIT_ARCHIVE_DIR", "./data/archive"), "local append-only archive directory")
	jwtSecret := flag.String("jwt-secret", os.Getenv("AUDIT_JWT_SECRET"), "local-only HS256 JWT secret; requires explicit opt-in")
	allowLocalHS256 := flag.Bool("allow-local-hs256", strictBoolEnv("AUDIT_ALLOW_LOCAL_HS256", false), "enable isolated local HS256 verification; incompatible with public key or JWKS trust")
	jwtPublicKey := flag.String("jwt-public-key", os.Getenv("AUDIT_JWT_PUBLIC_KEY_PEM"), "local asymmetric JWT verification public key in PEM format")
	jwtPublicKeyAlgorithm := flag.String("jwt-public-key-alg", envOr("AUDIT_JWT_PUBLIC_KEY_ALG", "RS256"), "algorithm pinned to the local public key")
	jwksURL := flag.String("jwks-url", os.Getenv("AUDIT_JWKS_URL"), "OIDC JWKS URL for EdDSA, ES256/384/512, RS256 or PS256 tokens")
	allowInsecureJWKS := flag.Bool("allow-insecure-jwks-loopback", strictBoolEnv("AUDIT_ALLOW_INSECURE_JWKS_LOOPBACK", false), "allow HTTP JWKS only on loopback for local verification")
	issuer := flag.String("jwt-issuer", os.Getenv("AUDIT_JWT_ISSUER"), "expected JWT issuer")
	audience := flag.String("jwt-audience", os.Getenv("AUDIT_JWT_AUDIENCE"), "expected JWT audience")
	allowDev := flag.Bool("allow-dev-auth", strictBoolEnv(envDevAuth, false), "enable dev:<tenant>:<role> tokens; disable in production")
	allowDevSecrets := flag.Bool("allow-dev-secrets", strictBoolEnv(runtimeconfig.EnvDevSecrets, false), "enable well-known development signing/encryption secrets; never enable in production")
	checkConfig := flag.Bool("check-config", false, "validate secrets and external signing/archive configuration, then exit without opening the store or network")
	bootstrapTenant := flag.String("bootstrap-tenant", envOr("AUDIT_BOOTSTRAP_TENANT", "demo"), "create a local bootstrap tenant when missing")
	segmentSize := flag.Int("segment-size", intEnv("AUDIT_SEGMENT_SIZE", 100), "events per integrity segment")
	// adminActionsCap bounds Snapshot.AdminActions (control-plane mutation
	// facts, drop-oldest); values <= 0 select service.DefaultMaxAdminActions.
	// adminTrailCap bounds the separate read self-audit trail
	// (<state>.admin-trail.jsonl / admin_action_trail) that absorbs read-path
	// facts so reads never rewrite the snapshot; 0 = unbounded append-only
	// (operator-explicit), negative values select
	// service.DefaultMaxAdminTrailActions.
	adminActionsCap := flag.Int("admin-actions-cap", intEnv("AUDIT_ADMIN_ACTIONS_CAP", service.DefaultMaxAdminActions), "cap on snapshot admin-action facts (drop-oldest; <= 0 selects the default)")
	adminTrailCap := flag.Int("admin-trail-cap", intEnv("AUDIT_ADMIN_TRAIL_CAP", service.DefaultMaxAdminTrailActions), "cap on the read self-audit trail (0 = unbounded append-only)")
	grpcListen := flag.String("grpc-listen", os.Getenv("AUDIT_GRPC_LISTEN"), "optional gRPC listen address")
	grpcTLSCert := flag.String("grpc-tls-cert", os.Getenv("AUDIT_GRPC_TLS_CERT"), "PEM certificate path for the gRPC ingest listener (must be set together with -grpc-tls-key)")
	grpcTLSKey := flag.String("grpc-tls-key", os.Getenv("AUDIT_GRPC_TLS_KEY"), "PEM private key path for the gRPC ingest listener (must be set together with -grpc-tls-cert)")
	otlpEndpoint := flag.String("otlp-endpoint", os.Getenv("AUDIT_OTLP_ENDPOINT"), "OTLP/HTTP trace endpoint such as http://jaeger:4318; empty disables tracing")
	vaultAddr := flag.String("vault-addr", os.Getenv("AUDIT_VAULT_ADDR"), "HashiCorp Vault address; with token+transit key, checkpoint signatures go through the Transit engine")
	vaultToken := flag.String("vault-token", os.Getenv("AUDIT_VAULT_TOKEN"), "Vault token for the Transit signer")
	vaultTransitKey := flag.String("vault-transit-key", os.Getenv("AUDIT_VAULT_TRANSIT_KEY"), "Vault Transit key name for checkpoint signatures")
	s3Endpoint := flag.String("s3-endpoint", os.Getenv("AUDIT_S3_ENDPOINT"), "S3-compatible endpoint (e.g. localhost:19010); with bucket+keys the compliance archive goes to an Object Lock bucket")
	s3Bucket := flag.String("s3-bucket", os.Getenv("AUDIT_S3_BUCKET"), "S3 bucket for the compliance archive")
	s3AccessKey := flag.String("s3-access-key", os.Getenv("AUDIT_S3_ACCESS_KEY"), "S3 access key")
	s3SecretKey := flag.String("s3-secret-key", os.Getenv("AUDIT_S3_SECRET_KEY"), "S3 secret key")
	s3UseSSL := flag.Bool("s3-use-ssl", strictBoolEnv(runtimeconfig.EnvS3UseSSL, false), "use TLS for the S3-compatible archive endpoint (AUDIT_S3_USE_SSL)")
	// archiveRetentionDays is the per-object COMPLIANCE retention duration in
	// days applied by every S3 archive Put (F1 leg (i), mandatory for an S3
	// archive: a zero value makes runtimeconfig.SigningArchive.Archive fail
	// closed, so the API can never write objects without explicit retention).
	// Shared with audit-governance-worker via runtimeconfig.EnvArchiveRetentionDays.
	archiveRetentionDays := flag.Uint("archive-retention-days", uintEnv(runtimeconfig.EnvArchiveRetentionDays), "per-object COMPLIANCE retention for the S3 archive in days (AUDIT_ARCHIVE_RETENTION_DAYS; required when an S3 archive is configured, e.g. 365)")
	allowInsecureVaultLoopback := flag.Bool("allow-insecure-vault-loopback", strictBoolEnv(runtimeconfig.EnvAllowInsecureVaultLoopback, false), "allow plaintext HTTP Vault only on loopback hosts (AUDIT_ALLOW_INSECURE_VAULT_LOOPBACK)")
	flag.Parse()

	logger := log.New(os.Stdout, "audit-api ", log.LstdFlags|log.Lmicroseconds)
	if *allowDev {
		logger.Printf("warning=development_auth_enabled")
	}
	if *allowDevSecrets {
		logger.Printf("warning=development_secrets_enabled")
	}
	cfg := service.Config{ServerVersion: "audit-governance/0.1.0", ArchiveDir: *archiveDir, SegmentSize: *segmentSize, SigningSecret: os.Getenv(runtimeconfig.EnvSigningSecret), EncryptionKey: os.Getenv(runtimeconfig.EnvEncryptionKey), AllowDevSecrets: *allowDevSecrets, MaxAdminActions: *adminActionsCap, MaxAdminTrailActions: *adminTrailCap}
	external := runtimeconfig.SigningArchive{ArchiveDir: *archiveDir, VaultAddr: *vaultAddr, VaultToken: *vaultToken, VaultTransitKey: *vaultTransitKey, S3Endpoint: *s3Endpoint, S3Bucket: *s3Bucket, S3AccessKey: *s3AccessKey, S3SecretKey: *s3SecretKey, S3UseSSL: *s3UseSSL, ArchiveRetentionDays: *archiveRetentionDays, AllowInsecureVaultLoopback: *allowInsecureVaultLoopback}
	authenticator := auth.Authenticator{JWTSecret: *jwtSecret, AllowLocalHS256: *allowLocalHS256, JWTPublicKeyPEM: *jwtPublicKey, JWTPublicKeyAlgorithm: *jwtPublicKeyAlgorithm, JWKSURL: *jwksURL, AllowInsecureJWKSLoopback: *allowInsecureJWKS, Issuer: *issuer, Audience: *audience, AllowDev: *allowDev}
	if *checkConfig {
		os.Exit(runCheckConfig(logger, cfg, external, authenticator, *grpcListen, *grpcTLSCert, *grpcTLSKey))
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
	if err := wireExternal(logger, svc, external, *vaultTransitKey, *vaultAddr, *s3Bucket, *s3Endpoint); err != nil {
		logger.Fatalf("%v", err)
	}
	// F1 companion: the API now probes the archive destination at boot, with
	// worker parity (bounded by archiveReadyTimeout, fail-fast on a
	// non-WORM-ready destination). The primary write path (ingest) archives
	// without a per-request probe; this one-time check plus leg (i)
	// per-object retention closes the drift window: every object written by
	// this process is COMPLIANCE-retained regardless of probe recency.
	if err := probeArchiveReady(svc.Config.Archive, logger); err != nil {
		logger.Fatalf("archive ready: %v", err)
	}
	if err := bootstrap(svc, *bootstrapTenant); err != nil {
		logger.Fatalf("bootstrap: %v", err)
	}
	server, tracer, err := prepareServer(logger, svc, authenticator, *otlpEndpoint, *listen)
	if err != nil {
		logger.Fatalf("%v", err)
	}
	var grpcServer *grpc.Server
	var healthServer *health.Server
	var grpcListener net.Listener
	if *grpcListen != "" {
		// REQ-4: the startup path applies the same fail-closed transport gate
		// as -check-config through the shared resolveGRPCTransport helper (so
		// preflight and runtime cannot diverge) before any listener is bound;
		// TLS credentials are loaded exactly once and reused for grpc.Creds.
		grpcTransport, grpcCreds, transportErr := resolveGRPCTransport(*grpcListen, *grpcTLSCert, *grpcTLSKey)
		if transportErr != nil {
			logger.Fatalf("%v", transportErr)
		}
		// F3 parity: when the explicit dev/verify-stack allowlist re-opens a
		// non-loopback plaintext listener, warn loudly so the opt-in is never
		// invisible in production logs.
		if grpcTransport == "insecure" && !loopbackListenAddr(*grpcListen) {
			logger.Printf("warning: %s=true: the gRPC ingest listener serves plaintext on a non-loopback address (%s) — verify-stack/dev-only, never production: the ingest bearer token would travel in clear", grpcInsecureAllowlistEnv, *grpcListen)
		}
		grpcListener, err = net.Listen("tcp", *grpcListen)
		if err != nil {
			logger.Fatalf("listen gRPC: %v", err)
		}
		// Panic-recovery interceptors must stay the outermost chain entries so
		// they also contain panics from any inner interceptors added later.
		// Keepalive is transport-level only (FR-3.4): RPC semantics,
		// authentication, and message framing are untouched.
		grpcServerOptions := []grpc.ServerOption{
			grpc.KeepaliveParams(grpcapi.KeepaliveParams()),
			grpc.KeepaliveEnforcementPolicy(grpcapi.KeepaliveEnforcementPolicy()),
			// Receive-size parity with the HTTP surface: a single gRPC ingest
			// message is capped at the HTTP request-body cap (512KB) instead of
			// grpc-go's 4MB default, so neither transport accepts a request the
			// other rejects on size (8x storage/CPU amplification closed).
			grpc.MaxRecvMsgSize(grpcapi.MaxRecvBytes),
			grpc.ChainUnaryInterceptor(grpcapi.RecoveryUnaryServerInterceptor(logger)),
			grpc.ChainStreamInterceptor(grpcapi.RecoveryStreamServerInterceptor(logger)),
		}
		// TLS is the only transport credential on the listener: grpc-go then
		// rejects plaintext (h2c) clients at the handshake (REQ-2.3), so the
		// authorization bearer metadata is never sent on a plaintext channel.
		if grpcCreds != nil {
			grpcServerOptions = append(grpcServerOptions, grpc.Creds(grpcCreds))
		}
		grpcServer = grpc.NewServer(grpcServerOptions...)
		grpcapi.Register(grpcServer, svc, authenticator)
		// bootstrap() completed before server construction, so health can report
		// SERVING immediately (FR-2.2); the handle is kept so the shutdown path
		// can drain probes to NOT_SERVING first (FR-2.3).
		healthServer = grpcapi.RegisterHealth(grpcServer)
		grpcapi.MarkServing(healthServer)
		go func() {
			logger.Printf("grpc_listen=%s", *grpcListen)
			if serveErr := grpcServer.Serve(grpcListener); serveErr != nil {
				logger.Printf("grpc_serve=%v", serveErr)
			}
		}()
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		logger.Printf("listen=%s", *listen)
		if serveErr := server.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			logger.Fatalf("serve: %v", serveErr)
		}
	}()
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
	if tracer != nil {
		_ = tracer.Shutdown(ctx)
	}
	if grpcServer != nil {
		// Drain the health service before GracefulStop (FR-2.3) so probes and
		// load balancers observe NOT_SERVING while in-flight RPCs drain.
		if healthServer != nil {
			healthServer.Shutdown()
		}
		grpcServer.GracefulStop()
	}
	_ = st.Close()
}

// archiveReadyTimeout bounds the boot archive readiness probe so a hung or
// black-holed S3 endpoint can never block startup indefinitely. Mirrors the
// worker's constant (worker parity for the probe contract, REQ-1).
const archiveReadyTimeout = 5 * time.Second

// probeArchiveReady probes a configured archive destination under
// archiveReadyTimeout and logs the outcome. Unconfigured destinations (nil or
// an empty-dir FileStore) are skipped, mirroring /readyz via archive.Configured
// (OQ-1). The caller decides the failure handling: main fails fast at startup
// (logger.Fatalf); the worker additionally re-probes per pass.
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

// wireExternal attaches the resolved external signer and archive to the
// service configuration, logging which provider is in use. Extracted from
// main so the startup wiring (REQ-TLS-4/5 fail-fast at boot) is
// unit-testable: the store and service are already open when this runs, and
// any validation error aborts startup with the same "signer:"/"archive:"
// message main previously emitted directly.
func wireExternal(logger *log.Logger, svc *service.Service, external runtimeconfig.SigningArchive, vaultTransitKey, vaultAddr, s3Bucket, s3Endpoint string) error {
	external.SigningSecret = svc.Config.SigningSecret
	external.EncryptionKey = svc.Config.EncryptionKey
	if signer, signErr := external.Signer(); signErr != nil {
		return fmt.Errorf("signer: %w", signErr)
	} else if signer != nil {
		svc.Config.Signer = signer
		logger.Printf("signer=vault-transit key=%s addr=%s", vaultTransitKey, vaultAddr)
	}
	if archiveStore, archiveErr := external.Archive(); archiveErr != nil {
		return fmt.Errorf("archive: %w", archiveErr)
	} else {
		svc.Config.Archive = archiveStore
		if s3Endpoint != "" {
			logger.Printf("archive=s3 bucket=%s endpoint=%s", s3Bucket, s3Endpoint)
		}
	}
	return nil
}

// prepareServer validates the authentication configuration (including the
// AC-3 dev-auth allowlist: a flag-only -allow-dev-auth can never start the
// server, so CI cannot bless a config the runtime would reject), builds the
// HTTP handler, initializes telemetry for the given OTLP endpoint (empty
// disables tracing) and returns the server. Extracted from main so the
// pre-server startup path is unit-testable; error text matches what main
// previously emitted directly.
func prepareServer(logger *log.Logger, svc *service.Service, authenticator auth.Authenticator, otlpEndpoint, listen string) (*http.Server, *telemetry.Tracer, error) {
	if err := validateAuthConfig(authenticator, svc.Config); err != nil {
		return nil, nil, fmt.Errorf("invalid authentication configuration: %w", err)
	}
	if authenticator.AllowDev && !devAuthAllowlisted() {
		return nil, nil, fmt.Errorf("invalid authentication configuration: development auth (-allow-dev-auth) requires AUDIT_ALLOW_DEV_AUTH=true in the environment (flag-only dev auth is not allowed)")
	}
	api := httpapi.NewServer(svc, authenticator, logger)
	tracer, err := telemetry.Init(context.Background(), otlpEndpoint, "audit-api")
	if err != nil {
		return nil, nil, fmt.Errorf("init telemetry: %w", err)
	}
	if tracer != nil {
		logger.Printf("tracing=otlp endpoint=%s", otlpEndpoint)
	}
	server := &http.Server{Addr: listen, Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	return server, tracer, nil
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

func bootstrap(svc *service.Service, tenantID string) error {
	if strings.TrimSpace(tenantID) == "" {
		return nil
	}
	items, err := svc.ListTenants()
	if err != nil {
		return err
	}
	found := false
	for _, item := range items {
		if item.ID == tenantID {
			found = true
			break
		}
	}
	if !found {
		if err := svc.CreateTenant("bootstrap", domain.Tenant{ID: tenantID, Name: "Local development tenant", HomeRegion: "local", DataRegion: "local", Active: true, EventsPerSecond: 1000, Burst: 1000}); err != nil {
			return err
		}
	}
	if schemas, err := svc.ListSchemas(tenantID); err != nil {
		return err
	} else if len(schemas) == 0 {
		if err := svc.RegisterSchema("bootstrap", domain.EventSchema{TenantID: tenantID, SchemaID: "audit.event", Version: 1, EventType: "audit.event", Classification: "internal", Active: true}); err != nil {
			return err
		}
	}
	if sources, err := svc.ListSources(tenantID); err != nil {
		return err
	} else if len(sources) == 0 {
		if err := svc.AddSource("bootstrap", domain.SourceSystem{TenantID: tenantID, ID: "demo", Name: "Local development source", Active: true}); err != nil {
			return err
		}
	}
	return nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// runCheckConfig validates the resolved configuration without opening the
// store or touching the network: service.New performs the secret fail-fast
// checks (and resolves dev defaults), the authentication configuration is
// validated with the same rules as startup, and then the external
// signer/archive configuration is checked. Output prints names and lengths
// only, never secret values, so CI can compare API and worker outputs.
//
// The dev-auth allowlist is environment-only: -allow-dev-auth alone can never
// satisfy the preflight (AC-3), so a flag-only CI invocation fails with an
// auditable marker instead of blessing a config that diverges from startup.
// grpcListen/grpcTLSCert/grpcTLSKey are the resolved gRPC listener inputs:
// runCheckConfig applies the same fail-closed transport gate as startup
// (REQ-4, shared resolveGRPCTransport) and reports transport_grpc on the ok
// line.
func runCheckConfig(logger *log.Logger, cfg service.Config, external runtimeconfig.SigningArchive, authenticator auth.Authenticator, grpcListen, grpcTLSCert, grpcTLSKey string) int {
	svc, err := service.New(nil, cfg)
	if err != nil {
		logger.Printf("invalid secrets: %v", err)
		return 1
	}
	logSecretWarnings(logger, svc.Config)
	devAuthGateFailed := authenticator.AllowDev && !devAuthAllowlisted()
	if devAuthGateFailed {
		logger.Printf("check_config=fail auth=dev_auth_flag_not_allowlisted: development auth (-allow-dev-auth) requires AUDIT_ALLOW_DEV_AUTH=true in the environment")
	}
	if err := validateAuthConfig(authenticator, svc.Config); err != nil {
		logger.Printf("invalid authentication configuration: %v", err)
		return 1
	}
	if devAuthGateFailed {
		return 1
	}
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
	// Per-leg transport labels (REQ-TLS-6/F3): the S3 and Vault legs are each
	// reported ("tls"/"http"/"local"), so a mixed configuration never hides
	// one leg's transport. A Transport() error is a check failure: the line
	// is never printed with an unresolved transport (FM-8).
	s3Transport, vaultTransport, transportErr := external.Transport()
	if transportErr != nil {
		logger.Printf("transport: %v", transportErr)
		return 1
	}
	// REQ-3 (transport_grpc gate): runs after the secret/auth/external checks
	// and before check_config=ok. A non-loopback plaintext gRPC listener
	// without TLS and without the env-only allowlist fails closed with the
	// auditable marker naming both variables; partial TLS and unreadable/
	// mismatched PEM pairs are hard errors (REQ-3.2/3.3). The ok line gains
	// the unconditional final transport_grpc=<tls|insecure|disabled> field
	// (REQ-3.4), keeping check-config a pure function of resolved config.
	grpcTransport, _, transportGRPCErr := resolveGRPCTransport(grpcListen, grpcTLSCert, grpcTLSKey)
	if transportGRPCErr != nil {
		logger.Printf("%v", transportGRPCErr)
		return 1
	}
	logger.Printf("check_config=ok signing_secret_length=%d encryption_key_length=%d jwt_secret_length=%d signer=%s archive=%s transport_s3=%s transport_vault=%s transport_grpc=%s", len(svc.Config.SigningSecret), len(svc.Config.EncryptionKey), len(authenticator.JWTSecret), signerName, archiveName, s3Transport, vaultTransport, grpcTransport)
	return 0
}

// validateAuthConfig applies the shared authentication-configuration checks
// used by both startup (prepareServer) and preflight (runCheckConfig): the
// authenticator must pass auth.ValidateConfiguration (trust-source rules,
// including the local-HS256 32-byte minimum), and the local HS256 secret
// must be distinct from the signing and encryption secrets (SEC-2) so a
// single operator-chosen string cannot simultaneously forge tokens and
// checkpoint signatures or decrypt protected fields and exports. The auth
// package cannot enforce distinctness itself: the signing/encryption
// secrets live in service config, so the join point is here in the binary.
func validateAuthConfig(authenticator auth.Authenticator, cfg service.Config) error {
	if err := authenticator.ValidateConfiguration(); err != nil {
		return err
	}
	if authenticator.JWTSecret != "" && (authenticator.JWTSecret == cfg.SigningSecret || authenticator.JWTSecret == cfg.EncryptionKey) {
		return fmt.Errorf("local HS256 JWT secret must be distinct from the signing and encryption secrets")
	}
	return nil
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

// strictBoolEnv parses an environment variable as a boolean. Unset or empty
// values fall back to fallback; a present-but-malformed value exits 1 naming
// the variable before flag.Parse, so there is no fail-open path (C2).
func strictBoolEnv(name string, fallback bool) bool {
	parsed, ok, err := strictBoolValue(name, os.Getenv(name))
	if err != nil {
		fmt.Fprintf(os.Stderr, "audit-api: %v\n", err)
		os.Exit(1)
	}
	if !ok {
		return fallback
	}
	return parsed
}

// strictBoolValue is the single strict parse path shared by strictBoolEnv and
// the check-config dev-auth allowlist predicate: strconv.ParseBool literals
// (1, t, TRUE, ...), never a literal "true" comparison.
func strictBoolValue(name, value string) (parsed, present bool, err error) {
	if value == "" {
		return false, false, nil
	}
	parsed, err = strconv.ParseBool(value)
	if err != nil {
		return false, true, fmt.Errorf("%s=%q is not a valid boolean", name, value)
	}
	return parsed, true, nil
}

// devAuthAllowlisted reports whether the check-config dev-auth allowlist is
// satisfied: only the environment variable AUDIT_ALLOW_DEV_AUTH parsed
// strictly counts; the -allow-dev-auth flag is deliberately ignored so a
// flag alone can never satisfy the preflight gate (AC-3).
func devAuthAllowlisted() bool {
	return strictBoolEnv(envDevAuth, false)
}

// grpcInsecureAllowlistEnv is the explicit dev/verify-stack escape hatch for
// a non-loopback plaintext gRPC ingest listener (F3 parity:
// AUDIT_ALLOW_INSECURE_API_URL gates AUDIT_OUTBOX_API_URL; this gates
// AUDIT_GRPC_LISTEN). It is environment-only — deliberately no
// -allow-insecure-grpc-* flag, exactly like F3 — so a flag alone can never
// satisfy an allowlist gate (AC-3). Never set in production: the ingest
// authorization bearer token would otherwise travel in clear.
const grpcInsecureAllowlistEnv = "AUDIT_ALLOW_INSECURE_GRPC_LISTEN"

// loopbackListenAddr reports whether addr binds explicitly to a loopback
// interface: localhost or a loopback IP (127.x, ::1). Fail-closed
// conservatism for the server side: an empty host (":50051" binds all
// interfaces), a wildcard IP (0.0.0.0), and Docker host-gateway aliases
// (host.docker.internal / gateway.docker.internal are gateway aliases, not
// loopback) all resolve to non-loopback, so a listener can never be assumed
// loopback unless it says so explicitly. Mirrors outbox.loopbackHost intent
// while staying conservative for listen addresses.
func loopbackListenAddr(addr string) bool {
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}

// grpcAllowlisted reports whether the env-only insecure gRPC allowlist is
// strictly satisfied (REQ-1.3/1.4): a present-but-malformed value is a hard
// error (never fail-open), and any strictly-parsed value other than true is
// treated as not-allowlisted (fail closed).
func grpcAllowlisted() (bool, error) {
	parsed, present, err := strictBoolValue(grpcInsecureAllowlistEnv, os.Getenv(grpcInsecureAllowlistEnv))
	if err != nil {
		return false, err
	}
	return present && parsed, nil
}

// resolveGRPCTransport is the single fail-closed gate implementation shared
// by -check-config and startup (REQ-4): it resolves the gRPC ingest
// listener's transport configuration into the deterministic transport_grpc
// label ("tls"|"insecure"|"disabled") plus, when TLS is configured, the
// grpc transport credentials. It fails closed on every unsafe combination:
// non-loopback plaintext without the env-only allowlist, partial cert/key
// pairs (REQ-1.2), unreadable/mismatched PEM files, and a malformed
// allowlist value. Sharing one helper structurally prevents preflight/runtime
// divergence and double TLS loads.
func resolveGRPCTransport(grpcListen, tlsCert, tlsKey string) (label string, creds credentials.TransportCredentials, err error) {
	certSet := tlsCert != ""
	keySet := tlsKey != ""
	if certSet != keySet {
		return "", nil, fmt.Errorf("partial TLS configuration: -grpc-tls-cert and -grpc-tls-key must be set together (AUDIT_GRPC_TLS_CERT/AUDIT_GRPC_TLS_KEY)")
	}
	// REQ-3.3: TLS files are validated whenever either path is non-empty,
	// even when the listener is unset, so typos fail preflight early. No
	// expiry/time checks (REQ-7): check-config stays deterministic.
	if certSet {
		loaded, loadErr := grpcapi.ServerCredentials(tlsCert, tlsKey)
		if loadErr != nil {
			return "", nil, fmt.Errorf("gRPC TLS: %w", loadErr)
		}
		creds = loaded
	}
	if grpcListen == "" {
		return "disabled", creds, nil
	}
	if creds != nil {
		return "tls", creds, nil
	}
	if loopbackListenAddr(grpcListen) {
		return "insecure", nil, nil
	}
	allowlisted, allowErr := grpcAllowlisted()
	if allowErr != nil {
		return "", nil, allowErr
	}
	if allowlisted {
		return "insecure", nil, nil
	}
	return "", nil, fmt.Errorf("check_config=fail grpc=plaintext_without_allowlist: AUDIT_GRPC_LISTEN=%q binds a non-loopback interface without TLS; set AUDIT_GRPC_TLS_CERT/AUDIT_GRPC_TLS_KEY, or AUDIT_ALLOW_INSECURE_GRPC_LISTEN=true for local verification stacks only (never production: the ingest bearer token would travel in clear)", grpcListen)
}

func intEnv(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

// uintEnv parses a non-negative integer environment variable, falling back
// to 0 (which the S3 retention check then treats as unset/fail-closed) when
// the value is missing, not an integer, or negative. A malformed value must
// never kill the API or bypass the fail-closed retention requirement.
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
