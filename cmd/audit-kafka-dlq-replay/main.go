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

	"github.com/snaplink/audit-governance/internal/kafka"
	"github.com/snaplink/audit-governance/internal/outbox"
)

// audit-kafka-dlq-replay recovers events that the ledger consumer
// dead-lettered. DLQ records carry only failure metadata, so the original
// event is recovered from the accepted topic by key and re-published
// byte-for-byte (default) or re-ingested through the audit API
// (-api-url/-token). Replayed event IDs are persisted to -state, making
// re-scans idempotent. Run with -once from a scheduler, or as a daemon.
func main() {
	brokers := flag.String("brokers", os.Getenv("AUDIT_KAFKA_BROKERS"), "comma-separated Kafka brokers")
	dlqTopic := flag.String("dlq-topic", envOr("AUDIT_KAFKA_DLQ_TOPIC", kafka.TopicDLQ), "dead-letter topic")
	acceptedTopic := flag.String("accepted-topic", envOr("AUDIT_KAFKA_ACCEPTED_TOPIC", kafka.TopicAccepted), "accepted topic to recover original events from")
	group := flag.String("group", envOr("AUDIT_KAFKA_DLQ_REPLAY_GROUP", "audit-dlq-replay"), "consumer group id prefix")
	statePath := flag.String("state", envOr("AUDIT_DLQ_REPLAY_STATE", "./data/dlq-replay-state.json"), "replayed event id state file")
	once := flag.Bool("once", false, "run a single replay round and exit")
	interval := flag.Duration("interval", durationEnv("AUDIT_DLQ_REPLAY_INTERVAL", 5*time.Minute), "round interval in daemon mode")
	apiURL := flag.String("api-url", envOr("AUDIT_OUTBOX_API_URL", ""), "audit API base URL; when set, recovered events are re-ingested via HTTP instead of re-published to Kafka")
	token := flag.String("token", os.Getenv("AUDIT_OUTBOX_TOKEN"), "bearer token for audit API ingestion")
	timeout := flag.Duration("timeout", durationEnv("AUDIT_KAFKA_TIMEOUT", 30*time.Second), "per-event ingest timeout in API mode")
	metricsListen := flag.String("metrics-listen", os.Getenv("AUDIT_DLQ_REPLAY_METRICS"), "optional metrics listen address (e.g. :9091)")
	flag.Parse()
	if *brokers == "" {
		log.Fatalf("brokers are required: pass -brokers or set AUDIT_KAFKA_BROKERS")
	}
	logger := log.New(os.Stdout, "audit-kafka-dlq-replay ", log.LstdFlags|log.Lmicroseconds)
	state, err := kafka.LoadReplayState(*statePath)
	if err != nil {
		logger.Fatalf("state: %v", err)
	}
	var republish kafka.RepublishFunc
	if *apiURL != "" {
		deliver := outbox.HTTPDeliverer(*apiURL, *token, &http.Client{Timeout: *timeout})
		republish = func(ctx context.Context, key, value []byte) error {
			event, err := kafka.EventFromCanonical(value)
			if err != nil {
				return err
			}
			_, err = deliver(ctx, event)
			return err
		}
	} else {
		producer := kafka.NewProducer(strings.Split(*brokers, ","), *acceptedTopic)
		defer producer.Close()
		republish = producer.Republish
	}
	replayer := kafka.NewReplayer(strings.Split(*brokers, ","), *dlqTopic, *acceptedTopic, *group, state, republish, logger)
	defer replayer.Close()
	// -once 使用独立 group：与常驻实例共享 group 会在 rebalance 中竞争
	// accepted partition，导致单轮扫描读不到消息（静默 replayed=0）。
	if *once {
		if err := replayer.Close(); err != nil {
			logger.Printf("close initial replayer: %v", err)
		}
		replayer = kafka.NewReplayer(strings.Split(*brokers, ","), *dlqTopic, *acceptedTopic, *group+"-once", state, republish, logger)
		defer replayer.Close()
	}
	if *metricsListen != "" && !*once {
		go serveMetrics(*metricsListen, replayer, logger)
	}
	logger.Printf("brokers=%s dlq_topic=%s accepted_topic=%s group=%s api_mode=%v state=%s", *brokers, *dlqTopic, *acceptedTopic, *group, *apiURL != "", *statePath)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *once {
		replayed, err := replayer.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			logger.Fatalf("replay round: %v", err)
		}
		logger.Printf("replay round done replayed=%d", replayed)
		return
	}
	if err := replayer.ReplayScheduler(ctx, *interval); err != nil && ctx.Err() == nil {
		logger.Fatalf("replay scheduler: %v", err)
	}
	logger.Printf("shutting down")
}

// serveMetrics exposes the replayer's counters in the text format used by
// the audit-api /metrics endpoint, so Prometheus can alert on DLQ traffic.
func serveMetrics(address string, replayer *kafka.Replayer, logger *log.Logger) {
	http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		dlqRecords, acceptedSeen, replayed, republishFailures, pending := replayer.Metrics()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "audit_dlq_records_total %d\n", dlqRecords)
		fmt.Fprintf(w, "audit_dlq_accepted_scanned_total %d\n", acceptedSeen)
		fmt.Fprintf(w, "audit_dlq_replayed_total %d\n", replayed)
		fmt.Fprintf(w, "audit_dlq_republish_failures_total %d\n", republishFailures)
		fmt.Fprintf(w, "audit_dlq_pending %d\n", pending)
	})
	server := &http.Server{Addr: address}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Printf("metrics listen=%s error=%v", address, err)
	}
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
