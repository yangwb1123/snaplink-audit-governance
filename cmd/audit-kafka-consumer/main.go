package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

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
	flag.Parse()
	if *brokers == "" {
		log.Fatalf("brokers are required: pass -brokers or set AUDIT_KAFKA_BROKERS")
	}
	if *backoff <= 0 || *timeout <= 0 {
		log.Fatalf("backoff and timeout must be positive")
	}
	logger := log.New(os.Stdout, "audit-kafka-consumer ", log.LstdFlags|log.Lmicroseconds)
	ingest := kafka.IngestFunc(outbox.HTTPDeliverer(*apiURL, *token, &http.Client{Timeout: *timeout}))
	consumer := kafka.NewConsumer(strings.Split(*brokers, ","), *topic, *group, ingest, *backoff, logger)
	defer consumer.Close()
	logger.Printf("brokers=%s topic=%s group=%s api_url=%s", *brokers, *topic, *group, *apiURL)
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
