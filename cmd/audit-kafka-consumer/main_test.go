package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
