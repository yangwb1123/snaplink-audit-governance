package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/snaplink/audit-governance/internal/kafka"
	"github.com/snaplink/audit-governance/internal/outbox"
)

func main() {
	dsn := flag.String("dsn", os.Getenv("AUDIT_OUTBOX_DSN"), "PostgreSQL DSN of the database holding audit_outbox")
	apiURL := flag.String("api-url", envOr("AUDIT_OUTBOX_API_URL", "http://localhost:8089"), "audit API base URL")
	token := flag.String("token", os.Getenv("AUDIT_OUTBOX_TOKEN"), "bearer token for audit API ingestion")
	interval := flag.Duration("interval", durationEnv("AUDIT_OUTBOX_INTERVAL", 5*time.Second), "poll interval")
	once := flag.Bool("once", false, "process due outbox records once and exit")
	batch := flag.Int("batch", intEnv("AUDIT_OUTBOX_BATCH", 100), "records per poll")
	maxAttempts := flag.Int("max-attempts", intEnv("AUDIT_OUTBOX_MAX_ATTEMPTS", 8), "delivery attempts before dead-lettering")
	timeout := flag.Duration("timeout", durationEnv("AUDIT_OUTBOX_TIMEOUT", 30*time.Second), "per-record delivery timeout")
	kafkaBrokers := flag.String("kafka-brokers", os.Getenv("AUDIT_OUTBOX_KAFKA_BROKERS"), "comma-separated Kafka brokers; when set, delivery uses Kafka instead of HTTP")
	flag.Parse()
	if *dsn == "" {
		log.Fatalf("dsn is required: pass -dsn or set AUDIT_OUTBOX_DSN")
	}
	if *batch <= 0 || *maxAttempts <= 0 || *interval <= 0 || *timeout <= 0 {
		log.Fatalf("batch, max-attempts, interval and timeout must be positive")
	}
	logger := log.New(os.Stdout, "audit-outbox-relay ", log.LstdFlags|log.Lmicroseconds)
	db, err := sql.Open("pgx", *dsn)
	if err != nil {
		logger.Fatalf("open database: %v", err)
	}
	defer db.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := db.PingContext(ctx); err != nil {
		logger.Fatalf("ping database: %v", err)
	}
	relay := outbox.Relay{
		Store:       outbox.NewPostgresStore(db),
		Deliver:     outbox.HTTPDeliverer(*apiURL, *token, &http.Client{Timeout: *timeout}),
		BatchSize:   *batch,
		MaxAttempts: *maxAttempts,
		Logger:      logger,
	}
	var kafkaProducer *kafka.Producer
	if *kafkaBrokers != "" {
		kafkaProducer = kafka.NewProducer(strings.Split(*kafkaBrokers, ","), kafka.TopicAccepted)
		defer kafkaProducer.Close()
		relay.Deliver = kafkaProducer.Deliver
		logger.Printf("delivery=kafka brokers=%s topic=%s", *kafkaBrokers, kafka.TopicAccepted)
	} else {
		// F3 (RFC 6750 §1): the ingest bearer token must never travel over
		// plaintext http except to loopback targets (or the explicit
		// dev/verify-stack opt-in). Fails closed before any delivery.
		if err := outbox.ValidateAPIURL(*apiURL, outbox.InsecureAPIURLAllowed()); err != nil {
			logger.Fatalf("api-url: %v", err)
		}
		if outbox.InsecureAPIURLAllowed() && outbox.IsInsecureHTTPURL(*apiURL) {
			logger.Printf("warning: %s=true: the ingest bearer token is sent over plaintext HTTP (%s) — verify-stack/dev-only, never production", outbox.APIURLInsecureEnv, *apiURL)
		}
		if strings.TrimSpace(*token) == "" {
			logger.Printf("warning: AUDIT_OUTBOX_TOKEN is empty; the audit API will reject every delivery (401) and the backlog will retry until attempts are exhausted — set the token before starting")
		}
		logger.Printf("delivery=http api_url=%s", *apiURL)
	}
	logger.Printf("interval=%s batch=%d max_attempts=%d", *interval, *batch, *maxAttempts)
	runOnce := func() {
		handled, runErr := relay.RunOnce(ctx)
		if runErr != nil {
			logger.Printf("run: %v", runErr)
			return
		}
		logger.Printf("handled=%d", handled)
	}
	runOnce()
	if *once {
		return
	}
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			runOnce()
		case <-ctx.Done():
			logger.Printf("shutting down")
			return
		}
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
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
