package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/kafka"
	"github.com/snaplink/audit-governance/internal/outbox"
)

func main() {
	brokers := flag.String("brokers", os.Getenv("AUDIT_KAFKA_BROKERS"), "comma-separated Kafka brokers")
	topic := flag.String("topic", envOr("AUDIT_KAFKA_TOPIC", kafka.TopicAccepted), "source topic")
	group := flag.String("group", envOr("AUDIT_KAFKA_GROUP", "audit-ledger-consumer"), "consumer group id")
	apiURL := flag.String("api-url", envOr("AUDIT_OUTBOX_API_URL", "http://localhost:8089"), "audit API base URL")
	token := flag.String("token", os.Getenv("AUDIT_OUTBOX_TOKEN"), "bearer token for audit API ingestion")
	backoff := flag.Duration("backoff", durationEnv("AUDIT_KAFKA_BACKOFF", 2*time.Second), "retry backoff on ingest failure")
	timeout := flag.Duration("timeout", durationEnv("AUDIT_KAFKA_TIMEOUT", 30*time.Second), "per-message ingest timeout")
	maxAttempts := flag.Int("max-attempts", intEnv("AUDIT_KAFKA_MAX_ATTEMPTS", 8), "max ingest attempts per message before dead-lettering")
	dlqTopic := flag.String("dlq-topic", envOr("AUDIT_KAFKA_DLQ_TOPIC", kafka.TopicDLQ), "dead-letter topic")
	metricsListen := flag.String("metrics-listen", os.Getenv("AUDIT_KAFKA_METRICS"), "optional metrics listen address (e.g. :9092)")
	flag.Parse()
	if *brokers == "" {
		log.Fatalf("brokers are required: pass -brokers or set AUDIT_KAFKA_BROKERS")
	}
	if *backoff <= 0 || *timeout <= 0 || *maxAttempts <= 0 {
		log.Fatalf("backoff, timeout and max-attempts must be positive")
	}
	logger := log.New(os.Stdout, "audit-kafka-consumer ", log.LstdFlags|log.Lmicroseconds)
	// F3 (RFC 6750 §1): the ingest bearer token must never travel over
	// plaintext http except to loopback targets (or the explicit
	// dev/verify-stack opt-in). Fails closed BEFORE any Kafka connection or
	// delivery attempt.
	if err := outbox.ValidateAPIURL(*apiURL, outbox.InsecureAPIURLAllowed()); err != nil {
		logger.Fatalf("api-url: %v", err)
	}
	if outbox.InsecureAPIURLAllowed() && outbox.IsInsecureHTTPURL(*apiURL) {
		logger.Printf("warning: %s=true: the ingest bearer token is sent over plaintext HTTP (%s) — verify-stack/dev-only, never production", outbox.APIURLInsecureEnv, *apiURL)
	}
	// REQ-7.1 (G1): an empty token means the API rejects every delivery with
	// 401 and the whole accepted stream retries until attempts are exhausted
	// — warn before any consumption (parity with audit-outbox-relay).
	if strings.TrimSpace(*token) == "" {
		logger.Printf("warning: AUDIT_OUTBOX_TOKEN is empty; the audit API will reject every delivery (401) and the accepted stream will retry until attempts are exhausted — set the token before starting")
	}
	// Build the deliverer once: the per-message IngestFunc closure captures it,
	// so every Kafka message reuses the same HTTP client and connection pool
	// instead of allocating a fresh transport per message.
	deliver := outbox.HTTPDeliverer(*apiURL, *token, &http.Client{Timeout: *timeout})
	ingest := kafka.IngestFunc(func(ctx context.Context, event domain.Event) error {
		_, err := deliver(ctx, event)
		return err
	})
	options := []kafka.ConsumerOption{kafka.WithMaxAttempts(*maxAttempts)}
	if *dlqTopic != "" {
		dlqProducer := kafka.NewProducer(strings.Split(*brokers, ","), *dlqTopic)
		defer dlqProducer.Close()
		options = append(options, kafka.WithDLQ(dlqProducer))
	}
	consumer := kafka.NewConsumer(strings.Split(*brokers, ","), *topic, *group, ingest, *backoff, logger, options...)
	defer consumer.Close()
	if *metricsListen != "" {
		go serveMetrics(*metricsListen, consumer, logger)
	}
	logger.Printf("brokers=%s topic=%s group=%s api_url=%s max_attempts=%d dlq_topic=%s", *brokers, *topic, *group, *apiURL, *maxAttempts, *dlqTopic)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Fatalf("consume: %v", err)
	}
	logger.Printf("shutting down")
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

// serveMetrics exposes the consumer's domain counters in the text format
// used by the audit-api /metrics endpoint, so Prometheus can alert on DLQ
// traffic (the release-notes follow-up: DLQ consumer + traffic alert).
func serveMetrics(address string, consumer *kafka.Consumer, logger *log.Logger) {
	http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, metricsText(consumer.Metrics()))
	})
	server := &http.Server{Addr: address}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Printf("metrics listen=%s error=%v", address, err)
	}
}

// metricsText renders the consumer counters in the Prometheus text format.
// The line order is pinned (golden-tested): audit_consumer_unauthorized_total
// sits after dead_lettered (it is a subset of dead-lettered events).
// Consumers must parse by metric name (as Prometheus does), not by position.
func metricsText(m kafka.ConsumerMetrics) string {
	return fmt.Sprintf(
		"audit_consumer_ingest_failures_total %d\n"+
			"audit_consumer_dead_lettered_total %d\n"+
			"audit_consumer_unauthorized_total %d\n"+
			"audit_consumer_dlq_published_total %d\n"+
			"audit_consumer_messages_committed_total %d\n",
		m.IngestFailures, m.DeadLettered, m.Unauthorized,
		m.DLQPublished, m.MessagesCommitted)
}
