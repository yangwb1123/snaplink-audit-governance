package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Binary-level tests for flag/env wiring (REQ-2/REQ-3, AC-3). The resolved
// config is printed as the first stdout line, before projection.Open and
// EnsureSchema, so the effective source topic is observable even when
// ClickHouse and Kafka are unreachable. The real binary is built once per
// test run (TestMain) and exercised with a clean environment.

// projectorBin is built once per test run in TestMain.
var projectorBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "audit-projector-test-")
	if err != nil {
		os.Exit(1)
	}
	projectorBin = filepath.Join(dir, "audit-projector")
	if output, err := exec.Command("go", "build", "-o", projectorBin, ".").CombinedOutput(); err != nil {
		os.Stderr.Write(output)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// cleanEnv strips AUDIT_KAFKA_* and AUDIT_CLICKHOUSE_DSN so the binary
// resolves its flag defaults: a live AUDIT_CLICKHOUSE_DSN would let the
// binary reach consumer.Run and make the exit path/timing environment-
// dependent.
func cleanEnv() []string {
	var clean []string
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "AUDIT_KAFKA_") && !strings.HasPrefix(entry, "AUDIT_CLICKHOUSE_DSN=") {
			clean = append(clean, entry)
		}
	}
	return clean
}

// runProjector runs the built binary with a clean environment plus the given
// env overrides. The process is killed after the timeout — the config line
// is emitted before any blocking call, so the captured output is complete;
// the exit code is not asserted, the first log line is the contract under
// test.
func runProjector(t *testing.T, timeout time.Duration, env []string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, projectorBin, args...)
	cmd.Env = env
	output, _ := cmd.CombinedOutput()
	return string(output)
}

func firstLine(output string) string {
	if idx := strings.IndexByte(output, '\n'); idx >= 0 {
		return output[:idx]
	}
	return output
}

// AC-3.1: with no -topic / AUDIT_KAFKA_TOPIC override, the resolved default
// source topic is kafka.TopicLedgered and it is the first log line.
func TestResolvedDefaultTopicIsLedgered(t *testing.T) {
	output := runProjector(t, 5*time.Second, cleanEnv(), "-brokers", "127.0.0.1:1")
	first := firstLine(output)
	if !strings.Contains(first, "topic=audit.events.ledgered.v1") {
		t.Fatalf("first log line %q: want topic=audit.events.ledgered.v1; full output:\n%s", first, output)
	}
}

// AC-3.2: an explicit -topic override keeps working (migration window).
func TestTopicOverrideFlag(t *testing.T) {
	output := runProjector(t, 5*time.Second, cleanEnv(), "-brokers", "127.0.0.1:1", "-topic", "audit.events.accepted.v1")
	first := firstLine(output)
	if !strings.Contains(first, "topic=audit.events.accepted.v1") {
		t.Fatalf("first log line %q: want override topic=audit.events.accepted.v1; full output:\n%s", first, output)
	}
}

// AC-3.2: the AUDIT_KAFKA_TOPIC env form of the override also works.
func TestTopicOverrideEnv(t *testing.T) {
	env := append(cleanEnv(), "AUDIT_KAFKA_TOPIC=audit.events.accepted.v1")
	output := runProjector(t, 5*time.Second, env, "-brokers", "127.0.0.1:1")
	first := firstLine(output)
	if !strings.Contains(first, "topic=audit.events.accepted.v1") {
		t.Fatalf("first log line %q: want env override topic=audit.events.accepted.v1; full output:\n%s", first, output)
	}
}

// AC-3.1 (DLQ config): with no -max-attempts / -dlq-topic override, the
// resolved defaults are the consumer's attempt cap (8) and kafka.TopicDLQ,
// both visible in the first log line.
func TestResolvedDefaultDLQConfig(t *testing.T) {
	output := runProjector(t, 5*time.Second, cleanEnv(), "-brokers", "127.0.0.1:1")
	first := firstLine(output)
	if !strings.Contains(first, "max_attempts=8") {
		t.Fatalf("first log line %q: want max_attempts=8; full output:\n%s", first, output)
	}
	if !strings.Contains(first, "dlq_topic=audit.events.dlq.v1") {
		t.Fatalf("first log line %q: want dlq_topic=audit.events.dlq.v1; full output:\n%s", first, output)
	}
}

// AC-3.2: explicit flags override the DLQ config defaults.
func TestDLQConfigFlagOverrides(t *testing.T) {
	output := runProjector(t, 5*time.Second, cleanEnv(), "-brokers", "127.0.0.1:1", "-max-attempts", "5", "-dlq-topic", "my.dlq.v1")
	first := firstLine(output)
	if !strings.Contains(first, "max_attempts=5") {
		t.Fatalf("first log line %q: want max_attempts=5; full output:\n%s", first, output)
	}
	if !strings.Contains(first, "dlq_topic=my.dlq.v1") {
		t.Fatalf("first log line %q: want dlq_topic=my.dlq.v1; full output:\n%s", first, output)
	}
}

// AC-3.2: the AUDIT_KAFKA_MAX_ATTEMPTS / AUDIT_KAFKA_DLQ_TOPIC env forms
// override the defaults when no flags are passed.
func TestDLQConfigEnvOverrides(t *testing.T) {
	env := append(cleanEnv(), "AUDIT_KAFKA_MAX_ATTEMPTS=5", "AUDIT_KAFKA_DLQ_TOPIC=my.dlq.v1")
	output := runProjector(t, 5*time.Second, env, "-brokers", "127.0.0.1:1")
	first := firstLine(output)
	if !strings.Contains(first, "max_attempts=5") {
		t.Fatalf("first log line %q: want env override max_attempts=5; full output:\n%s", first, output)
	}
	if !strings.Contains(first, "dlq_topic=my.dlq.v1") {
		t.Fatalf("first log line %q: want env override dlq_topic=my.dlq.v1; full output:\n%s", first, output)
	}
}

// REQ-5: a non-positive -max-attempts is rejected at startup with a fatal
// error before any consumer/producer construction; the process must exit
// nonzero and the message must mention max-attempts.
func TestMaxAttemptsNonPositiveRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, projectorBin, "-brokers", "127.0.0.1:1", "-max-attempts", "0")
	cmd.Env = cleanEnv()
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("-max-attempts 0: want nonzero exit; output:\n%s", output)
	}
	if !strings.Contains(string(output), "max-attempts") {
		t.Fatalf("-max-attempts 0: fatal message %q: want mention of max-attempts", output)
	}
}
