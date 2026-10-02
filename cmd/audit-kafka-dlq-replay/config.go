package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/snaplink/audit-governance/internal/kafka"
)

// replayConfig is the fully resolved command line. Resolution is split from
// wiring so the startup order is testable without a broker: every check below
// runs before kafka.LoadReplayState, kafka.NewProducer and kafka.NewReplayer
// are reached, so an invalid topology cannot have connected to anything.
type replayConfig struct {
	brokers           string
	dlqTopic          string
	acceptedTopic     string
	group             string
	statePath         string
	apiURL            string
	token             string
	tenantScope       string
	metricsListen     string
	once              bool
	replayAuthBlocked bool
	interval          time.Duration
	timeout           time.Duration
}

// resolveReplayConfig parses args over the environment and returns the
// validated configuration, or the zero value plus the first validation error.
// The zero value is returned on every failure so a caller cannot accidentally
// build a producer or replayer from a rejected configuration.
//
// The default DLQ topic stays kafka.TopicDLQ: this tool recovers originals
// from the accepted topic, so it drains the ingest DLQ only. The projection
// DLQ is rejected by kafka.ValidateReplayTopology — pointing this tool at it
// would close projection failures that it can never resolve.
func resolveReplayConfig(args []string, getenv func(string) string) (replayConfig, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	flags := flag.NewFlagSet("audit-kafka-dlq-replay", flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	brokers := flags.String("brokers", getenv("AUDIT_KAFKA_BROKERS"), "comma-separated Kafka brokers")
	dlqTopic := flags.String("dlq-topic", envOrWith(getenv, "AUDIT_KAFKA_DLQ_TOPIC", kafka.TopicDLQ), "dead-letter topic")
	acceptedTopic := flags.String("accepted-topic", envOrWith(getenv, "AUDIT_KAFKA_ACCEPTED_TOPIC", kafka.TopicAccepted), "accepted topic to recover original events from")
	group := flags.String("group", envOrWith(getenv, "AUDIT_KAFKA_DLQ_REPLAY_GROUP", "audit-dlq-replay"), "consumer group id prefix")
	statePath := flags.String("state", envOrWith(getenv, "AUDIT_DLQ_REPLAY_STATE", "./data/dlq-replay-state.json"), "replayed event id state file")
	once := flags.Bool("once", false, "run a single replay round and exit")
	interval := flags.Duration("interval", durationEnvWith(getenv, "AUDIT_DLQ_REPLAY_INTERVAL", 5*time.Minute), "round interval in daemon mode")
	apiURL := flags.String("api-url", envOrWith(getenv, "AUDIT_OUTBOX_API_URL", ""), "audit API base URL; when set, recovered events are re-ingested via HTTP instead of re-published to Kafka")
	token := flags.String("token", getenv("AUDIT_OUTBOX_TOKEN"), "bearer token for audit API ingestion")
	tenantScope := flags.String("tenant-scope", getenv("AUDIT_DLQ_REPLAY_TENANT_SCOPE"), "explicit single-tenant scope for the static API replay token (required for token use)")
	timeout := flags.Duration("timeout", durationEnvWith(getenv, "AUDIT_KAFKA_TIMEOUT", 30*time.Second), "per-event ingest timeout in API mode")
	replayAuthBlocked := flags.Bool("replay-auth-blocked", false, "re-publish DLQ records with error_code=unauthorized (set only after the ingest token is fixed)")
	metricsListen := flags.String("metrics-listen", getenv("AUDIT_DLQ_REPLAY_METRICS"), "optional metrics listen address (e.g. :9091)")
	if err := flags.Parse(args); err != nil {
		return replayConfig{}, fmt.Errorf("parse flags: %w", err)
	}

	cfg := replayConfig{
		brokers:           *brokers,
		dlqTopic:          *dlqTopic,
		acceptedTopic:     *acceptedTopic,
		group:             *group,
		statePath:         *statePath,
		apiURL:            *apiURL,
		token:             *token,
		tenantScope:       *tenantScope,
		metricsListen:     *metricsListen,
		once:              *once,
		replayAuthBlocked: *replayAuthBlocked,
		interval:          *interval,
		timeout:           *timeout,
	}
	if err := kafka.ValidateReplayTopology(cfg.acceptedTopic, cfg.dlqTopic, cfg.group); err != nil {
		return replayConfig{}, err
	}
	if cfg.brokers == "" {
		return replayConfig{}, errors.New("brokers are required: pass -brokers or set AUDIT_KAFKA_BROKERS")
	}
	return cfg, nil
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
