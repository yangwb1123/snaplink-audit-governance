package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/kafka"
	"github.com/snaplink/audit-governance/internal/projection"
)

type projectorConfig struct {
	brokers       string
	sourceTopic   string // effective topic; "" becomes kafka.TopicAccepted
	group         string
	clickhouseDSN string
	backoff       time.Duration
	maxAttempts   int
	dlqTopic      string // exact empty string means disabled
}

// projectorStore, failureProducer, and projectorConsumer are private
// composition-root seams. They let startup validation and lifecycle tests
// replace external ClickHouse/Kafka construction without creating a runtime
// dependency-injection mechanism.
type projectorStore interface {
	Insert(context.Context, domain.Event) error
	EnsureSchema(context.Context) error
	Close() error
}

type failureProducer interface {
	kafka.FailurePublisher
	Close() error
}

type projectorConsumer interface {
	Run(context.Context) error
	Close() error
}

// projectorFactories is a test seam for proving startup ordering and that
// invalid topology never constructs an external resource. Production uses
// productionProjectorFactories; it is not a runtime extension point.
type projectorFactories struct {
	openStore   func(string) (projectorStore, error)
	newProducer func([]string, string) failureProducer
	newConsumer func(
		[]string,
		string,
		string,
		kafka.IngestFunc,
		time.Duration,
		*log.Logger,
		...kafka.ConsumerOption,
	) projectorConsumer
}

func resolveProjectorConfig(args []string, getenv func(string) string) (projectorConfig, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	flags := flag.NewFlagSet("audit-projector", flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	brokers := flags.String("brokers", envOrWith(getenv, "AUDIT_KAFKA_BROKERS", ""), "comma-separated Kafka brokers")
	topic := flags.String("topic", envOrWith(getenv, "AUDIT_KAFKA_TOPIC", kafka.TopicLedgered), "source topic")
	group := flags.String("group", envOrWith(getenv, "AUDIT_KAFKA_GROUP", "audit-projector"), "consumer group id")
	dsn := flags.String("clickhouse-dsn", envOrWith(getenv, "AUDIT_CLICKHOUSE_DSN", "clickhouse://audit:audit-local-only@localhost:19000/audit"), "ClickHouse native DSN")
	backoff := flags.Duration("backoff", durationEnvWith(getenv, "AUDIT_KAFKA_BACKOFF", 2*time.Second), "retry backoff on projection failure")
	maxAttempts := flags.Int("max-attempts", intEnvWith(getenv, "AUDIT_KAFKA_MAX_ATTEMPTS", 8), "max projection attempts per message before dead-lettering")
	dlqTopic := flags.String("dlq-topic", envOrWith(getenv, "AUDIT_KAFKA_DLQ_TOPIC", kafka.TopicDLQ), "dead-letter topic; empty disables the DLQ publisher")
	if err := flags.Parse(args); err != nil {
		return projectorConfig{}, fmt.Errorf("parse flags: %w", err)
	}

	cfg := projectorConfig{
		brokers:       *brokers,
		sourceTopic:   effectiveSourceTopic(*topic),
		group:         *group,
		clickhouseDSN: *dsn,
		backoff:       *backoff,
		maxAttempts:   *maxAttempts,
		dlqTopic:      *dlqTopic,
	}
	if err := validateProjectorTopology(cfg); err != nil {
		return projectorConfig{}, err
	}
	return cfg, nil
}

func effectiveSourceTopic(topic string) string {
	if topic == "" {
		return kafka.TopicAccepted
	}
	return topic
}

func validateProjectorTopology(cfg projectorConfig) error {
	if cfg.dlqTopic != "" && cfg.sourceTopic == cfg.dlqTopic {
		return fmt.Errorf(
			"invalid projector topology: source topic %q equals enabled DLQ topic %q",
			cfg.sourceTopic, cfg.dlqTopic,
		)
	}
	if strings.TrimSpace(cfg.group) == "" {
		return errors.New(
			"invalid projector topology: consumer group must contain non-whitespace characters",
		)
	}
	return nil
}

func validateProjectorConfig(cfg projectorConfig) error {
	if err := validateProjectorTopology(cfg); err != nil {
		return err
	}
	if cfg.brokers == "" {
		return errors.New("brokers are required: pass -brokers or set AUDIT_KAFKA_BROKERS")
	}
	if cfg.backoff <= 0 || cfg.maxAttempts <= 0 {
		return errors.New("backoff and max-attempts must be positive")
	}
	return nil
}

func productionProjectorFactories() projectorFactories {
	return projectorFactories{
		openStore: func(dsn string) (projectorStore, error) {
			return projection.Open(dsn)
		},
		newProducer: func(brokers []string, topic string) failureProducer {
			return kafka.NewProducer(brokers, topic)
		},
		newConsumer: func(
			brokers []string,
			topic string,
			group string,
			ingest kafka.IngestFunc,
			backoff time.Duration,
			logger *log.Logger,
			options ...kafka.ConsumerOption,
		) projectorConsumer {
			return kafka.NewConsumer(brokers, topic, group, ingest, backoff, logger, options...)
		},
	}
}

func runProjector(ctx context.Context, cfg projectorConfig, logger *log.Logger, factories projectorFactories) error {
	if err := validateProjectorConfig(cfg); err != nil {
		return err
	}
	if logger != nil {
		logger.Printf("brokers=%q topic=%q group=%q clickhouse=redacted max_attempts=%d dlq_topic=%q", cfg.brokers, cfg.sourceTopic, cfg.group, cfg.maxAttempts, cfg.dlqTopic)
	}

	store, err := factories.openStore(cfg.clickhouseDSN)
	if err != nil {
		return fmt.Errorf("clickhouse: %w", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.EnsureSchema(ctx); err != nil {
		return fmt.Errorf("schema: %w", err)
	}

	// Projection owns the ledgered channel contract even when operators
	// override the source address for migration or replay. Never allow an
	// accepted-shaped event to reach ClickHouse through that override.
	options := []kafka.ConsumerOption{
		kafka.WithMaxAttempts(cfg.maxAttempts),
		kafka.WithInputSchema(kafka.LedgeredEventSchema),
	}
	if cfg.dlqTopic != "" {
		producer := factories.newProducer(strings.Split(cfg.brokers, ","), cfg.dlqTopic)
		defer func() { _ = producer.Close() }()
		options = append(options, kafka.WithDLQ(producer))
	}
	consumer := factories.newConsumer(
		strings.Split(cfg.brokers, ","),
		cfg.sourceTopic,
		cfg.group,
		kafka.IngestFunc(store.Insert),
		cfg.backoff,
		logger,
		options...,
	)
	defer func() { _ = consumer.Close() }()
	if err := consumer.Run(ctx); err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	return nil
}

func envOr(name, fallback string) string {
	return envOrWith(os.Getenv, name, fallback)
}

func envOrWith(getenv func(string) string, name, fallback string) string {
	if value := getenv(name); value != "" {
		return value
	}
	return fallback
}

func durationEnv(name string, fallback time.Duration) time.Duration {
	return durationEnvWith(os.Getenv, name, fallback)
}

func durationEnvWith(getenv func(string) string, name string, fallback time.Duration) time.Duration {
	value := getenv(name)
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
	return intEnvWith(os.Getenv, name, fallback)
}

func intEnvWith(getenv func(string) string, name string, fallback int) int {
	value := getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
