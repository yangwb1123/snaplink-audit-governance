package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/kafka"
)

// Regression tests for the consumer binary's flag/env wiring (REQ-6):
// -max-attempts / AUDIT_KAFKA_MAX_ATTEMPTS and -dlq-topic /
// AUDIT_KAFKA_DLQ_TOPIC must reach the validation and option construction
// paths. Validation fatals fire before any Kafka connection, so the binary
// can be exercised without a broker.

// consumerBin is built once per test run in TestMain.
var consumerBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "audit-kafka-consumer-test-")
	if err != nil {
		os.Exit(1)
	}
	consumerBin = filepath.Join(dir, "audit-kafka-consumer")
	if output, err := exec.Command("go", "build", "-o", consumerBin, ".").CombinedOutput(); err != nil {
		os.Stderr.Write(output)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// runBinary runs the built consumer with the given args, stripping any
// inherited AUDIT_KAFKA_* variables so tests control the wiring. It returns
// combined output and the process exit code.
func runBinary(t *testing.T, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command(consumerBin, args...)
	var clean []string
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "AUDIT_KAFKA_") {
			clean = append(clean, entry)
		}
	}
	cmd.Env = clean
	output, err := cmd.CombinedOutput()
	exit := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exit = exitErr.ExitCode()
		} else {
			t.Fatalf("run consumer: %v", err)
		}
	}
	return string(output), exit
}

// runBinaryCtx is the deadline-kill variant of runBinary (design F2): the
// consumer never exits on an unreachable broker (kafka-go dials lazily and
// retries forever), so tests that only assert on startup output kill the
// process after the deadline and assert on the accumulated output, never on
// the exit code. Same env filtering as runBinary — tests must pass explicit
// flags for anything the harness does not strip (e.g. -token "" for the
// empty-token case, F-07).
func runBinaryCtx(t *testing.T, ctx context.Context, args ...string) (string, int) {
	t.Helper()
	cmd := exec.CommandContext(ctx, consumerBin, args...)
	var clean []string
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "AUDIT_KAFKA_") {
			clean = append(clean, entry)
		}
	}
	cmd.Env = clean
	output, err := cmd.CombinedOutput() // returns accumulated output when the deadline kills the process
	exit := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exit = exitErr.ExitCode()
		} else {
			t.Fatalf("run consumer: %v", err)
		}
	}
	return string(output), exit
}

func TestConsumerRejectsMissingBrokers(t *testing.T) {
	output, exit := runBinary(t)
	if exit != 1 {
		t.Fatalf("exit=%d, want 1 (brokers required); output:\n%s", exit, output)
	}
	if !strings.Contains(output, "brokers are required") {
		t.Fatalf("output missing broker guard message:\n%s", output)
	}
}

func TestConsumerRejectsNonPositiveMaxAttemptsFlag(t *testing.T) {
	output, exit := runBinary(t, "-brokers", "localhost:9092", "-max-attempts", "0")
	if exit != 1 {
		t.Fatalf("exit=%d, want 1 (max-attempts must be positive); output:\n%s", exit, output)
	}
	if !strings.Contains(output, "backoff, timeout and max-attempts must be positive") {
		t.Fatalf("output missing positive-values guard message:\n%s", output)
	}
}

func TestConsumerMaxAttemptsReadFromEnv(t *testing.T) {
	cmd := exec.Command(consumerBin, "-brokers", "localhost:9092")
	cmd.Env = []string{"AUDIT_KAFKA_MAX_ATTEMPTS=0", "PATH=" + os.Getenv("PATH")}
	output, err := cmd.CombinedOutput()
	exit := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exit = exitErr.ExitCode()
		} else {
			t.Fatalf("run consumer: %v", err)
		}
	}
	if exit != 1 || !strings.Contains(string(output), "must be positive") {
		t.Fatalf("exit=%d, output:\n%s\nwant env-wired max-attempts=0 rejected", exit, output)
	}
}

func TestIntEnvParsing(t *testing.T) {
	t.Setenv("AUDIT_KAFKA_MAX_ATTEMPTS", "5")
	if got := intEnv("AUDIT_KAFKA_MAX_ATTEMPTS", 8); got != 5 {
		t.Fatalf("intEnv = %d, want 5", got)
	}
	t.Setenv("AUDIT_KAFKA_MAX_ATTEMPTS", "not-a-number")
	if got := intEnv("AUDIT_KAFKA_MAX_ATTEMPTS", 8); got != 8 {
		t.Fatalf("intEnv = %d, want fallback 8 for unparsable value", got)
	}
	t.Setenv("AUDIT_KAFKA_MAX_ATTEMPTS", "")
	if got := intEnv("AUDIT_KAFKA_MAX_ATTEMPTS", 8); got != 8 {
		t.Fatalf("intEnv = %d, want fallback 8 for empty value", got)
	}
}

func TestDurationEnvParsing(t *testing.T) {
	t.Setenv("AUDIT_KAFKA_BACKOFF", "3s")
	if got := durationEnv("AUDIT_KAFKA_BACKOFF", time.Second); got != 3*time.Second {
		t.Fatalf("durationEnv = %s, want 3s", got)
	}
	t.Setenv("AUDIT_KAFKA_BACKOFF", "bogus")
	if got := durationEnv("AUDIT_KAFKA_BACKOFF", time.Second); got != time.Second {
		t.Fatalf("durationEnv = %s, want fallback 1s", got)
	}
	t.Setenv("AUDIT_KAFKA_BACKOFF", "")
	if got := durationEnv("AUDIT_KAFKA_BACKOFF", time.Second); got != time.Second {
		t.Fatalf("durationEnv = %s, want fallback 1s for empty value", got)
	}
}

func TestEnvOrFallback(t *testing.T) {
	t.Setenv("AUDIT_KAFKA_TOPIC", "")
	if got := envOr("AUDIT_KAFKA_TOPIC", "fallback-topic"); got != "fallback-topic" {
		t.Fatalf("envOr = %q, want fallback for empty value", got)
	}
	t.Setenv("AUDIT_KAFKA_TOPIC", "audit.events.accepted.v1")
	if got := envOr("AUDIT_KAFKA_TOPIC", "fallback-topic"); got != "audit.events.accepted.v1" {
		t.Fatalf("envOr = %q, want env value", got)
	}
}

// AC-7.1.1/7.1.2 + AC-7.4.1 (F2 harness): with an empty token the binary
// prints the REQ-7.1 warning (substrings AUDIT_OUTBOX_TOKEN is empty and 401)
// BEFORE any consumption — asserted on captured output only, never on the
// exit code: the process blocks on the unreachable broker (kafka-go dials
// lazily and retries forever) and is killed by the deadline.
func TestConsumerWarnsOnEmptyToken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	output, _ := runBinaryCtx(t, ctx, "-brokers", "localhost:9092", "-token", "")
	if !strings.Contains(output, "AUDIT_OUTBOX_TOKEN is empty") || !strings.Contains(output, "401") {
		t.Fatalf("warning missing from output:\n%s", output)
	}
}

// AC-7.4.2/AC-7.1.3: a non-empty explicit token produces no warning (no
// false positive; -token is passed explicitly so the test is hermetic
// against an inherited AUDIT_OUTBOX_TOKEN — the harness strips only
// AUDIT_KAFKA_*, F-07).
func TestConsumerSilentWithToken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	output, _ := runBinaryCtx(t, ctx, "-brokers", "localhost:9092", "-token", "some-token")
	if strings.Contains(output, "AUDIT_OUTBOX_TOKEN is empty") {
		t.Fatalf("unexpected warning with a token set:\n%s", output)
	}
	if !strings.Contains(output, "brokers=localhost:9092") {
		t.Fatalf("startup log missing:\n%s", output)
	}
}

// F-03/F-07: the loopback default (http://localhost:8089) passes the F3
// transport gate and starts normally (no api-url error), warning only on the
// token state.
func TestConsumerLoopbackAPIURLDefaultsPass(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	output, _ := runBinaryCtx(t, ctx, "-brokers", "localhost:9092", "-token", "some-token")
	if strings.Contains(output, "api-url:") {
		t.Fatalf("loopback api-url must pass the F3 gate:\n%s", output)
	}
}

// F3: a non-loopback plaintext http api-url fails closed at startup — the
// ingest bearer token must never travel in clear (RFC 6750 §1). The test
// env strips AUDIT_ALLOW_INSECURE_API_URL (and all other vars) so the gate
// is exercised without the dev/verify-stack opt-in.
func TestConsumerRejectsNonLoopbackPlaintextAPIURL(t *testing.T) {
	cmd := exec.Command(consumerBin, "-brokers", "localhost:9092", "-api-url", "http://example.invalid", "-token", "some-token")
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("non-loopback plaintext http api-url must fail at startup:\n%s", output)
	}
	if !strings.Contains(string(output), "api-url:") || !strings.Contains(string(output), "HTTPS") {
		t.Fatalf("startup guard message missing:\n%s", output)
	}
}

// F3 opt-in: with AUDIT_ALLOW_INSECURE_API_URL=true the non-loopback
// plaintext http api-url starts (dev/verify-stack only) with a loud warning
// that the bearer token travels in clear.
func TestConsumerInsecureAPIURLOptInWarns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, consumerBin, "-brokers", "localhost:9092", "-api-url", "http://example.invalid", "-token", "some-token")
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "AUDIT_ALLOW_INSECURE_API_URL=true"}
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("consumer must still run (killed by deadline), output:\n%s", output)
	}
	if !strings.Contains(string(output), "plaintext HTTP") || !strings.Contains(string(output), "verify-stack/dev-only") {
		t.Fatalf("opt-in warning missing:\n%s", output)
	}
}

// AC-7.2.3: metricsText renders the consumer counters in the pinned order
// with the new audit_consumer_unauthorized_total line (golden — a reorder
// or a name change fails this test). The new disjoint third class
// (audit_consumer_foreign_tenant_skipped_total) is appended last: a skip is
// committed but is neither ingested nor dead-lettered, so its counter sits
// outside both families.
func TestConsumerMetricsTextGolden(t *testing.T) {
	metrics := kafka.ConsumerMetrics{
		IngestFailures:       1,
		DeadLettered:         2,
		Unauthorized:         3,
		DLQPublished:         4,
		MessagesCommitted:    5,
		ForeignTenantSkipped: 6,
	}
	want := "" +
		"audit_consumer_ingest_failures_total 1\n" +
		"audit_consumer_dead_lettered_total 2\n" +
		"audit_consumer_unauthorized_total 3\n" +
		"audit_consumer_dlq_published_total 4\n" +
		"audit_consumer_messages_committed_total 5\n" +
		"audit_consumer_foreign_tenant_skipped_total 6\n"
	if got := metricsText(metrics); got != want {
		t.Fatalf("metricsText mismatch:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

// AC-2 / REQ-2+7 (T3): the -tenant flag and AUDIT_KAFKA_TENANT env both reach
// the consumer wiring and the startup log; absent both, tenant= renders empty
// (REQ-7 backward compatible). The harness strips AUDIT_KAFKA_*, so the env
// form needs the custom cmd.Env pattern (TestConsumerMaxAttemptsReadFromEnv).
func TestConsumerTenantFlagAndEnvWiring(t *testing.T) {
	t.Run("flag form", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		output, _ := runBinaryCtx(t, ctx, "-brokers", "localhost:9092", "-token", "dev:demo:service", "-tenant", "demo")
		if !strings.Contains(output, "tenant=demo") {
			t.Fatalf("startup log missing tenant=demo:\n%s", output)
		}
		if strings.Contains(output, "tenant scope mismatch") {
			t.Fatalf("aligned dev token + tenant must not warn:\n%s", output)
		}
	})
	t.Run("env form", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, consumerBin, "-brokers", "localhost:9092")
		cmd.Env = []string{"AUDIT_KAFKA_TENANT=demo", "PATH=" + os.Getenv("PATH")}
		output, _ := cmd.CombinedOutput()
		if !strings.Contains(string(output), "tenant=demo") {
			t.Fatalf("startup log missing tenant=demo from env:\n%s", output)
		}
	})
	t.Run("unset renders empty tenant (REQ-7)", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		output, _ := runBinaryCtx(t, ctx, "-brokers", "localhost:9092", "-token", "dev:demo:service")
		if !strings.Contains(output, "dlq_topic=audit.events.dlq.v1 tenant=\n") {
			t.Fatalf("startup log must render an empty tenant= field:\n%s", output)
		}
	})
}

// AC-5 / REQ-2+1 (T11): binary-level proof that the consumer runs
// tenant-scoped with the token — dev:demo:service + -tenant demo are aligned
// (no mismatch warning) and the startup log carries tenant=demo.
func TestConsumerBinaryTenantScopedToken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	output, _ := runBinaryCtx(t, ctx, "-brokers", "localhost:9092", "-token", "dev:demo:service", "-tenant", "demo")
	if !strings.Contains(output, "tenant=demo") {
		t.Fatalf("startup log missing tenant=demo:\n%s", output)
	}
	if strings.Contains(output, "tenant scope mismatch") {
		t.Fatalf("aligned dev token + tenant must not warn:\n%s", output)
	}
}

// P2 (identity protocol F-2): a dev token whose tenant differs from
// AUDIT_KAFKA_TENANT logs a non-fatal startup warning carrying both tenants,
// then keeps running (the process reaches the startup log — not a fatal).
// This converts the silent FM-3 availability failure into an operator-visible
// signal while preserving REQ-7 (empty -tenant never warns).
func TestConsumerWarnsOnTenantTokenMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	output, _ := runBinaryCtx(t, ctx, "-brokers", "localhost:9092", "-token", "dev:acme:service", "-tenant", "demo")
	if !strings.Contains(output, "tenant scope mismatch") ||
		!strings.Contains(output, "tenant_claim=acme") ||
		!strings.Contains(output, "consumer_tenant=demo") {
		t.Fatalf("mismatch warning missing both tenant values:\n%s", output)
	}
	// Non-fatal: the resolved-config startup log follows the warning.
	if !strings.Contains(output, "brokers=localhost:9092") {
		t.Fatalf("consumer must keep running after the warning (startup log missing):\n%s", output)
	}
}
