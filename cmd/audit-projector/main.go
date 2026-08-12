package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/snaplink/audit-governance/internal/kafka"
	"github.com/snaplink/audit-governance/internal/projection"
)

func main() {
	brokers := flag.String("brokers", os.Getenv("AUDIT_KAFKA_BROKERS"), "comma-separated Kafka brokers")
	topic := flag.String("topic", envOr("AUDIT_KAFKA_TOPIC", kafka.TopicLedgered), "source topic")
	group := flag.String("group", envOr("AUDIT_KAFKA_GROUP", "audit-projector"), "consumer group id")
	dsn := flag.String("clickhouse-dsn", envOr("AUDIT_CLICKHOUSE_DSN", "clickhouse://audit:audit-local-only@localhost:19000/audit"), "ClickHouse native DSN")
	backoff := flag.Duration("backoff", durationEnv("AUDIT_KAFKA_BACKOFF", 2*time.Second), "retry backoff on projection failure")
	flag.Parse()
	if *brokers == "" {
		log.Fatalf("brokers are required: pass -brokers or set AUDIT_KAFKA_BROKERS")
	}
	if *backoff <= 0 {
		log.Fatalf("backoff must be positive")
	}
	logger := log.New(os.Stdout, "audit-projector ", log.LstdFlags|log.Lmicroseconds)
	// FIRST LINE: the resolved source topic is observable even when
	// ClickHouse/Kafka are unreachable (REQ-3, AC-3).
	logger.Printf("brokers=%s topic=%s group=%s clickhouse=%s", *brokers, *topic, *group, *dsn)
	store, err := projection.Open(*dsn)
	if err != nil {
		logger.Fatalf("clickhouse: %v", err)
	}
	defer store.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := store.EnsureSchema(ctx); err != nil {
		logger.Fatalf("schema: %v", err)
	}
	consumer := kafka.NewConsumer(strings.Split(*brokers, ","), *topic, *group, kafka.IngestFunc(store.Insert), *backoff, logger)
	defer consumer.Close()
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
