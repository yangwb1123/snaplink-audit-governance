package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/snaplink/audit-governance/internal/kafka"
	"github.com/snaplink/audit-governance/internal/outbox"
)

// audit-kafka-dlq-replay recovers events that the ledger consumer
// dead-lettered. DLQ records carry only failure metadata, so the original
// event is recovered from the accepted topic by key and re-published
// byte-for-byte (default) or re-ingested through the audit API
// (-api-url/-token). Replayed event IDs are persisted to -state, making
// re-scans idempotent. Run with -once from a scheduler, or as a daemon.
//
// Because the recovery source is the accepted topic, this tool owns the
// ingest DLQ (kafka.TopicDLQ) only. The projector's DLQ
// (kafka.TopicProjectionDLQ) holds failures whose originals live on the
// ledgered topic, so resolveReplayConfig rejects it: replaying it would
// produce redundant accepted re-ingests or permanent unresolvable closes,
// never a retried ClickHouse projection.
func main() {
	// resolveReplayConfig owns flag/env resolution AND validation, so both
	// run before kafka.LoadReplayState, kafka.NewProducer and
	// kafka.NewReplayer: an invalid topology (notably -dlq-topic pointed at
	// the projection DLQ) exits non-zero without constructing a client.
	cfg, err := resolveReplayConfig(os.Args[1:], os.Getenv)
	if err != nil {
		log.Fatalf("replay topology: %v", err)
	}
	logger := log.New(os.Stdout, "audit-kafka-dlq-replay ", log.LstdFlags|log.Lmicroseconds)
	// F3 (RFC 6750 §1): in HTTP mode the re-ingest token must never travel
	// over plaintext http except to loopback targets (or the explicit
	// dev/verify-stack opt-in). Kafka republish mode (no -api-url) is
	// unaffected.
	if cfg.apiURL != "" {
		if err := outbox.ValidateAPIURL(cfg.apiURL, outbox.InsecureAPIURLAllowed()); err != nil {
			logger.Fatalf("api-url: %v", err)
		}
		if outbox.InsecureAPIURLAllowed() && outbox.IsInsecureHTTPURL(cfg.apiURL) {
			logger.Printf("warning: %s=true: the re-ingest bearer token is sent over plaintext HTTP (%s) — verify-stack/dev-only, never production", outbox.APIURLInsecureEnv, cfg.apiURL)
		}
	}
	state, err := kafka.LoadReplayState(cfg.statePath)
	if err != nil {
		logger.Fatalf("state: %v", err)
	}
	if cfg.tenantScope != "" {
		state.SetLegacyTenantScope(cfg.tenantScope)
	}
	var republish kafka.RepublishFunc
	if cfg.apiURL != "" {
		if strings.TrimSpace(cfg.tenantScope) == "" {
			logger.Printf("warning: AUDIT_DLQ_REPLAY_TENANT_SCOPE is empty; API-mode replay will keep every recovered event pending and will not apply the static token")
		}
		if strings.TrimSpace(cfg.token) == "" {
			logger.Printf("warning: AUDIT_OUTBOX_TOKEN is empty; API-mode replay will keep records pending until a tenant-scoped credential is configured")
		}
		resolver := outbox.StaticTenantCredentialResolver(cfg.tenantScope, cfg.token)
		deliver := outbox.HTTPDelivererForTenant(cfg.apiURL, resolver, &http.Client{Timeout: cfg.timeout})
		republish = func(ctx context.Context, key, value []byte) error {
			event, err := kafka.EventFromCanonical(value)
			if err != nil {
				return err
			}
			_, err = deliver(ctx, event)
			return err
		}
	} else {
		producer := kafka.NewProducer(strings.Split(cfg.brokers, ","), cfg.acceptedTopic)
		defer producer.Close()
		republish = producer.Republish
	}
	replayer := kafka.NewReplayer(strings.Split(cfg.brokers, ","), cfg.dlqTopic, cfg.acceptedTopic, cfg.group, state, republish, logger)
	defer replayer.Close()
	// -replay-auth-blocked is the operator-acknowledgment switch: default off
	// holds error_code=unauthorized records blocked (pending); on re-admits
	// them. Must be set on every replayer instance, including the -once
	// replacement below.
	replayer.SetReplayAuthBlocked(cfg.replayAuthBlocked)
	if cfg.replayAuthBlocked {
		if cfg.apiURL != "" && strings.TrimSpace(cfg.token) == "" {
			logger.Printf("warning: -replay-auth-blocked enabled but AUDIT_OUTBOX_TOKEN is empty; HTTP-mode re-ingest will be rejected (401) and records will re-enter the DLQ each round — fix the token first")
		}
		logger.Printf("replay-auth-blocked enabled: unauthorized DLQ records will be re-published; ensure the ingest token is valid")
	}
	// -once 使用独立 group：与常驻实例共享 group 会在 rebalance 中竞争
	// accepted partition，导致单轮扫描读不到消息（静默 replayed=0）。
	if cfg.once {
		if err := replayer.Close(); err != nil {
			logger.Printf("close initial replayer: %v", err)
		}
		replayer = kafka.NewReplayer(strings.Split(cfg.brokers, ","), cfg.dlqTopic, cfg.acceptedTopic, cfg.group+"-once", state, republish, logger)
		defer replayer.Close()
		replayer.SetReplayAuthBlocked(cfg.replayAuthBlocked)
	}
	if cfg.metricsListen != "" && !cfg.once {
		go serveMetrics(cfg.metricsListen, replayer, logger)
	}
	logger.Printf("brokers=%s dlq_topic=%s accepted_topic=%s group=%s api_mode=%v state=%s", cfg.brokers, cfg.dlqTopic, cfg.acceptedTopic, cfg.group, cfg.apiURL != "", cfg.statePath)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if cfg.once {
		replayed, err := replayer.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			logger.Fatalf("replay round: %v", err)
		}
		logger.Printf("replay round done replayed=%d", replayed)
		return
	}
	if err := replayer.ReplayScheduler(ctx, cfg.interval); err != nil && ctx.Err() == nil {
		logger.Fatalf("replay scheduler: %v", err)
	}
	logger.Printf("shutting down")
}

// serveMetrics exposes the replayer's counters in the text format used by
// the audit-api /metrics endpoint, so Prometheus can alert on DLQ traffic.
// Mount semantics unchanged: daemon-only; -once exposes no /metrics.
func serveMetrics(address string, replayer *kafka.Replayer, logger *log.Logger) {
	http.HandleFunc("/metrics", metricsHandler(replayer.Metrics))
	server := &http.Server{Addr: address}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Printf("metrics listen=%s error=%v", address, err)
	}
}

// metricsHandler renders one /metrics response from a metrics source (the
// replayer's live counters; tests inject a fixed snapshot). Unexported:
// the mount decision stays in serveMetrics.
func metricsHandler(metrics func() kafka.ReplayerMetrics) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, metricsText(metrics()))
	}
}

// metricsText renders the replay resolution counters in the Prometheus text
// format. The line order is pinned (golden-tested, T7): the split
// resolution counters sit between replayed and republish_failures, pushing
// republish_failures and pending down; the auth-blocked counter and backlog
// gauge follow pending, and malformed is appended last (campaign
// fail-fast-or-warn-on-empty-rotated-ingest-token). The renamed permanent line
// (audit_dlq_permanent_rejections_total → audit_dlq_permanent_total,
// REQ-PERM-3) keeps its position, and the new observational
// audit_dlq_attempts_exhausted_total sits at the end of the resolution
// band (after unparsable_marks, before republish_failures). Consumers must
// parse by metric name (as Prometheus does), not by position; the fixed
// order keeps the output deterministic for tests and dashboards.
func metricsText(m kafka.ReplayerMetrics) string {
	return fmt.Sprintf(
		"audit_dlq_records_total %d\n"+
			"audit_dlq_accepted_scanned_total %d\n"+
			"audit_dlq_replayed_total %d\n"+
			"audit_dlq_permanent_total %d\n"+
			"audit_dlq_unresolvable_total %d\n"+
			"audit_dlq_unparsable_marks_total %d\n"+
			"audit_dlq_attempts_exhausted_total %d\n"+
			"audit_dlq_republish_failures_total %d\n"+
			"audit_dlq_pending %d\n"+
			"audit_dlq_auth_blocked_total %d\n"+
			"audit_dlq_auth_blocked %d\n"+
			"audit_dlq_malformed_total %d\n",
		m.DLQRecords, m.AcceptedScanned, m.Replayed, m.Permanent,
		m.Unresolvable, m.UnparsableMarks, m.AttemptsExhausted, m.RepublishFailures, m.Pending,
		m.AuthBlocked, m.AuthBlockedPending, m.Malformed)
}
