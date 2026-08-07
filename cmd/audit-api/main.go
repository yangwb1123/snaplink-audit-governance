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
	grpcListen := flag.String("grpc-listen", os.Getenv("AUDIT_GRPC_LISTEN"), "optional gRPC listen address")
	otlpEndpoint := flag.String("otlp-endpoint", os.Getenv("AUDIT_OTLP_ENDPOINT"), "OTLP/HTTP trace endpoint such as http://jaeger:4318; empty disables tracing")
	vaultAddr := flag.String("vault-addr", os.Getenv("AUDIT_VAULT_ADDR"), "HashiCorp Vault address; with token+transit key, checkpoint signatures go through the Transit engine")
	vaultToken := flag.String("vault-token", os.Getenv("AUDIT_VAULT_TOKEN"), "Vault token for the Transit signer")
	vaultTransitKey := flag.String("vault-transit-key", os.Getenv("AUDIT_VAULT_TRANSIT_KEY"), "Vault Transit key name for checkpoint signatures")
	s3Endpoint := flag.String("s3-endpoint", os.Getenv("AUDIT_S3_ENDPOINT"), "S3-compatible endpoint (e.g. localhost:19010); with bucket+keys the compliance archive goes to an Object Lock bucket")
	s3Bucket := flag.String("s3-bucket", os.Getenv("AUDIT_S3_BUCKET"), "S3 bucket for the compliance archive")
	s3AccessKey := flag.String("s3-access-key", os.Getenv("AUDIT_S3_ACCESS_KEY"), "S3 access key")
	s3SecretKey := flag.String("s3-secret-key", os.Getenv("AUDIT_S3_SECRET_KEY"), "S3 secret key")
	flag.Parse()

	logger := log.New(os.Stdout, "audit-api ", log.LstdFlags|log.Lmicroseconds)
	if *allowDev {
		logger.Printf("warning=development_auth_enabled")
	}
	if *allowDevSecrets {
		logger.Printf("warning=development_secrets_enabled")
	}
	cfg := service.Config{ServerVersion: "audit-governance/0.1.0", ArchiveDir: *archiveDir, SegmentSize: *segmentSize, SigningSecret: os.Getenv(runtimeconfig.EnvSigningSecret), EncryptionKey: os.Getenv(runtimeconfig.EnvEncryptionKey), AllowDevSecrets: *allowDevSecrets}
	external := runtimeconfig.SigningArchive{ArchiveDir: *archiveDir, VaultAddr: *vaultAddr, VaultToken: *vaultToken, VaultTransitKey: *vaultTransitKey, S3Endpoint: *s3Endpoint, S3Bucket: *s3Bucket, S3AccessKey: *s3AccessKey, S3SecretKey: *s3SecretKey}
	authenticator := auth.Authenticator{JWTSecret: *jwtSecret, AllowLocalHS256: *allowLocalHS256, JWTPublicKeyPEM: *jwtPublicKey, JWTPublicKeyAlgorithm: *jwtPublicKeyAlgorithm, JWKSURL: *jwksURL, AllowInsecureJWKSLoopback: *allowInsecureJWKS, Issuer: *issuer, Audience: *audience, AllowDev: *allowDev}
	if *checkConfig {
		os.Exit(runCheckConfig(logger, cfg, external, authenticator))
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
		logger.Printf("signer=vault-transit key=%s addr=%s", *vaultTransitKey, *vaultAddr)
	}
	if archiveStore, archiveErr := external.Archive(); archiveErr != nil {
		logger.Fatalf("archive: %v", archiveErr)
	} else {
		svc.Config.Archive = archiveStore
		if *s3Endpoint != "" {
			logger.Printf("archive=s3 bucket=%s endpoint=%s", *s3Bucket, *s3Endpoint)
		}
	}
	if err := bootstrap(svc, *bootstrapTenant); err != nil {
		logger.Fatalf("bootstrap: %v", err)
	}

	if err := authenticator.ValidateConfiguration(); err != nil {
		logger.Fatalf("invalid authentication configuration: %v", err)
	}
	api := httpapi.NewServer(svc, authenticator, logger)
	tracer, err := telemetry.Init(context.Background(), *otlpEndpoint, "audit-api")
	if err != nil {
		logger.Fatalf("init telemetry: %v", err)
	}
	if tracer != nil {
		logger.Printf("tracing=otlp endpoint=%s", *otlpEndpoint)
	}
	server := &http.Server{Addr: *listen, Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	var grpcServer *grpc.Server
	var grpcListener net.Listener
	if *grpcListen != "" {
		grpcListener, err = net.Listen("tcp", *grpcListen)
		if err != nil {
			logger.Fatalf("listen gRPC: %v", err)
		}
		grpcServer = grpc.NewServer()
		grpcapi.Register(grpcServer, svc, authenticator)
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
		grpcServer.GracefulStop()
	}
	_ = st.Close()
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
func runCheckConfig(logger *log.Logger, cfg service.Config, external runtimeconfig.SigningArchive, authenticator auth.Authenticator) int {
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
	if err := authenticator.ValidateConfiguration(); err != nil {
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
