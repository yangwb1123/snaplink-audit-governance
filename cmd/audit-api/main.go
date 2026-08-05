package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
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

	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/grpcapi"
	"github.com/snaplink/audit-governance/internal/httpapi"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
	"github.com/snaplink/audit-governance/internal/telemetry"
	"google.golang.org/grpc"
)

func main() {
	listen := flag.String("listen", envOr("AUDIT_LISTEN", ":8089"), "HTTP listen address")
	statePath := flag.String("state", envOr("AUDIT_STATE_PATH", "./data/state.json"), "local state snapshot path")
	postgresDSN := flag.String("postgres-dsn", os.Getenv("AUDIT_POSTGRES_DSN"), "PostgreSQL DSN for the control-plane state snapshot; overrides -state")
	archiveDir := flag.String("archive", envOr("AUDIT_ARCHIVE_DIR", "./data/archive"), "local append-only archive directory")
	jwtSecret := flag.String("jwt-secret", os.Getenv("AUDIT_JWT_SECRET"), "local-only HS256 JWT secret; requires explicit opt-in")
	allowLocalHS256 := flag.Bool("allow-local-hs256", boolEnv("AUDIT_ALLOW_LOCAL_HS256", false), "enable isolated local HS256 verification; incompatible with public key or JWKS trust")
	jwtPublicKey := flag.String("jwt-public-key", os.Getenv("AUDIT_JWT_PUBLIC_KEY_PEM"), "local asymmetric JWT verification public key in PEM format")
	jwtPublicKeyAlgorithm := flag.String("jwt-public-key-alg", envOr("AUDIT_JWT_PUBLIC_KEY_ALG", "RS256"), "algorithm pinned to the local public key")
	jwksURL := flag.String("jwks-url", os.Getenv("AUDIT_JWKS_URL"), "OIDC JWKS URL for EdDSA, ES256/384/512, RS256 or PS256 tokens")
	allowInsecureJWKS := flag.Bool("allow-insecure-jwks-loopback", boolEnv("AUDIT_ALLOW_INSECURE_JWKS_LOOPBACK", false), "allow HTTP JWKS only on loopback for local verification")
	issuer := flag.String("jwt-issuer", os.Getenv("AUDIT_JWT_ISSUER"), "expected JWT issuer")
	audience := flag.String("jwt-audience", os.Getenv("AUDIT_JWT_AUDIENCE"), "expected JWT audience")
	allowDev := flag.Bool("allow-dev-auth", boolEnv("AUDIT_ALLOW_DEV_AUTH", true), "enable dev:<tenant>:<role> tokens; disable in production")
	bootstrapTenant := flag.String("bootstrap-tenant", envOr("AUDIT_BOOTSTRAP_TENANT", "demo"), "create a local bootstrap tenant when missing")
	segmentSize := flag.Int("segment-size", intEnv("AUDIT_SEGMENT_SIZE", 100), "events per integrity segment")
	grpcListen := flag.String("grpc-listen", os.Getenv("AUDIT_GRPC_LISTEN"), "optional gRPC listen address")
	otlpEndpoint := flag.String("otlp-endpoint", os.Getenv("AUDIT_OTLP_ENDPOINT"), "OTLP/HTTP trace endpoint such as http://jaeger:4318; empty disables tracing")
	flag.Parse()

	logger := log.New(os.Stdout, "audit-api ", log.LstdFlags|log.Lmicroseconds)
	if *allowDev {
		logger.Printf("warning=development_auth_enabled")
	}
	st, err := openStore(*statePath, *postgresDSN, logger)
	if err != nil {
		logger.Fatalf("open store: %v", err)
	}
	defer st.Close()
	svc := service.New(st, service.Config{ServerVersion: "audit-governance/0.1.0", ArchiveDir: *archiveDir, SegmentSize: *segmentSize, SigningSecret: envOr("AUDIT_SIGNING_SECRET", "development-signing-key-change-me"), EncryptionKey: envOr("AUDIT_ENCRYPTION_KEY", "development-encryption-key-change-me")})
	if err := bootstrap(svc, *bootstrapTenant); err != nil {
		logger.Fatalf("bootstrap: %v", err)
	}

	authenticator := auth.Authenticator{JWTSecret: *jwtSecret, AllowLocalHS256: *allowLocalHS256, JWTPublicKeyPEM: *jwtPublicKey, JWTPublicKeyAlgorithm: *jwtPublicKeyAlgorithm, JWKSURL: *jwksURL, AllowInsecureJWKSLoopback: *allowInsecureJWKS, Issuer: *issuer, Audience: *audience, AllowDev: *allowDev}
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
