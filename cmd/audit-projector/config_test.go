package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/kafka"
)

func mapEnv(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func TestResolveProjectorConfigPrecedence(t *testing.T) {
	env := map[string]string{
		"AUDIT_KAFKA_BROKERS":      "env-broker",
		"AUDIT_KAFKA_TOPIC":        "env-source",
		"AUDIT_KAFKA_GROUP":        "env-group",
		"AUDIT_CLICKHOUSE_DSN":     "clickhouse://env-user:env-secret@host/audit",
		"AUDIT_KAFKA_BACKOFF":      "3s",
		"AUDIT_KAFKA_MAX_ATTEMPTS": "4",
		"AUDIT_KAFKA_DLQ_TOPIC":    "env-dlq",
	}
	cfg, err := resolveProjectorConfig([]string{
		"-brokers", "flag-broker",
		"-topic", "flag-source",
		"-group", "flag-group",
		"-clickhouse-dsn", "clickhouse://flag-user:flag-secret@host/audit",
		"-backoff", "5s",
		"-max-attempts", "6",
		"-dlq-topic", "flag-dlq",
	}, mapEnv(env))
	if err != nil {
		t.Fatalf("resolveProjectorConfig() error = %v", err)
	}
	want := projectorConfig{
		brokers:       "flag-broker",
		sourceTopic:   "flag-source",
		group:         "flag-group",
		clickhouseDSN: "clickhouse://flag-user:flag-secret@host/audit",
		backoff:       5 * time.Second,
		maxAttempts:   6,
		dlqTopic:      "flag-dlq",
	}
	if cfg != want {
		t.Fatalf("resolved config = %#v, want %#v", cfg, want)
	}
}

func TestResolveProjectorConfigDefaultsAndEmptyValueSemantics(t *testing.T) {
	env := map[string]string{
		"AUDIT_KAFKA_TOPIC":        "",
		"AUDIT_KAFKA_GROUP":        "",
		"AUDIT_KAFKA_DLQ_TOPIC":    "",
		"AUDIT_KAFKA_BACKOFF":      "",
		"AUDIT_KAFKA_MAX_ATTEMPTS": "",
	}
	cfg, err := resolveProjectorConfig(nil, mapEnv(env))
	if err != nil {
		t.Fatalf("resolve defaults error = %v", err)
	}
	if cfg.sourceTopic != kafka.TopicLedgered || cfg.dlqTopic != kafka.TopicDLQ || cfg.group != "audit-projector" {
		t.Fatalf("defaults = source %q, dlq %q, group %q", cfg.sourceTopic, cfg.dlqTopic, cfg.group)
	}
	if cfg.backoff != 2*time.Second || cfg.maxAttempts != 8 {
		t.Fatalf("scalar defaults = backoff %s, max attempts %d", cfg.backoff, cfg.maxAttempts)
	}

	cfg, err = resolveProjectorConfig([]string{"-topic", "", "-dlq-topic", ""}, mapEnv(env))
	if err != nil {
		t.Fatalf("explicit empty values error = %v", err)
	}
	if cfg.sourceTopic != kafka.TopicAccepted {
		t.Fatalf("empty source = %q, want effective accepted topic", cfg.sourceTopic)
	}
	if cfg.dlqTopic != "" {
		t.Fatalf("empty DLQ = %q, want disabled", cfg.dlqTopic)
	}
}

func TestResolveProjectorConfigRejectsTopologyCollisions(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
	}{
		{
			name: "equal flags",
			args: []string{"-topic", "custom", "-dlq-topic", "custom"},
		},
		{
			name: "equal environment",
			env:  map[string]string{"AUDIT_KAFKA_TOPIC": "custom", "AUDIT_KAFKA_DLQ_TOPIC": "custom"},
		},
		{
			name: "source flag and DLQ environment",
			args: []string{"-topic", "custom"},
			env:  map[string]string{"AUDIT_KAFKA_DLQ_TOPIC": "custom"},
		},
		{
			name: "source environment and DLQ flag",
			args: []string{"-dlq-topic", "custom"},
			env:  map[string]string{"AUDIT_KAFKA_TOPIC": "custom"},
		},
		{
			name: "accepted AsyncAPI topic",
			args: []string{"-topic", kafka.TopicAccepted, "-dlq-topic", kafka.TopicAccepted},
		},
		{
			name: "DLQ AsyncAPI topic",
			args: []string{"-topic", kafka.TopicDLQ, "-dlq-topic", kafka.TopicDLQ},
		},
		{
			name: "empty source fallback",
			args: []string{"-topic", "", "-dlq-topic", kafka.TopicAccepted},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := resolveProjectorConfig(test.args, mapEnv(test.env))
			if err == nil {
				t.Fatal("resolveProjectorConfig() error = nil, want topology error")
			}
			if !strings.Contains(err.Error(), "invalid projector topology") {
				t.Fatalf("error = %q, want topology diagnostic", err)
			}
		})
	}
}

func TestResolveProjectorConfigRejectsBlankGroups(t *testing.T) {
	for _, group := range []string{"", " ", "\t", " \t\n "} {
		t.Run("flag-"+group, func(t *testing.T) {
			_, err := resolveProjectorConfig([]string{"-group", group}, mapEnv(nil))
			if err == nil || !strings.Contains(err.Error(), "consumer group") {
				t.Fatalf("error = %v, want blank-group diagnostic", err)
			}
		})
	}
	_, err := resolveProjectorConfig(nil, mapEnv(map[string]string{"AUDIT_KAFKA_GROUP": " \t"}))
	if err == nil || !strings.Contains(err.Error(), "consumer group") {
		t.Fatalf("environment error = %v, want blank-group diagnostic", err)
	}
}

func TestRunProjectorRejectsInvalidTopologyBeforeFactories(t *testing.T) {
	cases := []struct {
		name string
		cfg  projectorConfig
	}{
		{
			name: "equal topics",
			cfg: projectorConfig{
				brokers: "broker", sourceTopic: "same", group: "group",
				backoff: time.Second, maxAttempts: 1, dlqTopic: "same",
			},
		},
		{
			name: "blank group",
			cfg: projectorConfig{
				brokers: "broker", sourceTopic: "source", group: " \t",
				backoff: time.Second, maxAttempts: 1, dlqTopic: "dlq",
			},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			called := false
			factories := projectorFactories{
				openStore: func(string) (projectorStore, error) {
					called = true
					return nil, errors.New("store must not open")
				},
				newProducer: func([]string, string) failureProducer {
					called = true
					return nil
				},
				newConsumer: func([]string, string, string, kafka.IngestFunc, time.Duration, *log.Logger, ...kafka.ConsumerOption) projectorConsumer {
					called = true
					return nil
				},
			}
			if err := runProjector(context.Background(), test.cfg, log.New(&bytes.Buffer{}, "", 0), factories); err == nil {
				t.Fatal("runProjector() error = nil, want validation error")
			}
			if called {
				t.Fatal("a projector factory was called for invalid topology")
			}
		})
	}
}

type projectorTestStore struct {
	events *[]string
}

func (s *projectorTestStore) Insert(context.Context, domain.Event) error { return nil }

func (s *projectorTestStore) EnsureSchema(context.Context) error {
	*s.events = append(*s.events, "ensure schema")
	return nil
}

func (s *projectorTestStore) Close() error {
	*s.events = append(*s.events, "close store")
	return nil
}

type projectorTestProducer struct {
	events *[]string
}

func (p *projectorTestProducer) PublishFailure(context.Context, kafka.Failure) error { return nil }

func (p *projectorTestProducer) Close() error {
	*p.events = append(*p.events, "close producer")
	return nil
}

type projectorTestConsumer struct {
	events *[]string
}

func (c *projectorTestConsumer) Run(context.Context) error {
	*c.events = append(*c.events, "run consumer")
	return nil
}

func (c *projectorTestConsumer) Close() error {
	*c.events = append(*c.events, "close consumer")
	return nil
}

func TestRunProjectorStartupOrderAndDLQLifecycle(t *testing.T) {
	for _, test := range []struct {
		name       string
		dlqTopic   string
		wantPrefix []string
	}{
		{name: "enabled", dlqTopic: "dlq", wantPrefix: []string{"open store", "ensure schema", "new producer", "new consumer", "run consumer"}},
		{name: "disabled", dlqTopic: "", wantPrefix: []string{"open store", "ensure schema", "new consumer", "run consumer"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			events := []string{}
			factories := projectorFactories{
				openStore: func(string) (projectorStore, error) {
					events = append(events, "open store")
					return &projectorTestStore{events: &events}, nil
				},
				newProducer: func([]string, string) failureProducer {
					events = append(events, "new producer")
					return &projectorTestProducer{events: &events}
				},
				newConsumer: func(_ []string, _ string, _ string, _ kafka.IngestFunc, _ time.Duration, _ *log.Logger, options ...kafka.ConsumerOption) projectorConsumer {
					wantOptions := 1
					if test.dlqTopic != "" {
						wantOptions = 2
					}
					if len(options) != wantOptions {
						t.Errorf("consumer options = %d, want %d", len(options), wantOptions)
					}
					events = append(events, "new consumer")
					return &projectorTestConsumer{events: &events}
				},
			}
			cfg := projectorConfig{
				brokers: "broker", sourceTopic: "source", group: "group",
				backoff: 2 * time.Second, maxAttempts: 3, dlqTopic: test.dlqTopic,
			}
			if err := runProjector(context.Background(), cfg, log.New(&bytes.Buffer{}, "", 0), factories); err != nil {
				t.Fatalf("runProjector() error = %v", err)
			}
			if len(events) < len(test.wantPrefix) {
				t.Fatalf("lifecycle = %v, want prefix %v", events, test.wantPrefix)
			}
			for i, want := range test.wantPrefix {
				if events[i] != want {
					t.Fatalf("lifecycle = %v, want prefix %v", events, test.wantPrefix)
				}
			}
			if test.dlqTopic == "" && strings.Contains(strings.Join(events, ","), "producer") {
				t.Fatalf("disabled DLQ lifecycle constructed producer: %v", events)
			}
		})
	}
}

func TestRunProjectorLogsSafeResolvedConfiguration(t *testing.T) {
	var output bytes.Buffer
	events := []string{}
	factories := projectorFactories{
		openStore: func(string) (projectorStore, error) {
			return &projectorTestStore{events: &events}, nil
		},
		newConsumer: func([]string, string, string, kafka.IngestFunc, time.Duration, *log.Logger, ...kafka.ConsumerOption) projectorConsumer {
			return &projectorTestConsumer{events: &events}
		},
	}
	cfg := projectorConfig{
		brokers: "broker", sourceTopic: "source\nforged", group: "group\tvalue",
		clickhouseDSN: "clickhouse://user:super-secret@host/audit",
		backoff:       time.Second, maxAttempts: 1,
	}
	if err := runProjector(context.Background(), cfg, log.New(&output, "", 0), factories); err != nil {
		t.Fatalf("runProjector() error = %v", err)
	}
	logOutput := output.String()
	if strings.Contains(logOutput, "super-secret") || strings.Contains(logOutput, "source\nforged") {
		t.Fatalf("unsafe startup log = %q", logOutput)
	}
	if !strings.Contains(logOutput, `topic="source\nforged"`) || !strings.Contains(logOutput, `group="group\tvalue"`) {
		t.Fatalf("startup log = %q, want escaped topic and group", logOutput)
	}
}
