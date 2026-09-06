package kafka

import (
	"strings"
	"testing"
)

func TestValidateConsumerGroup(t *testing.T) {
	for _, group := range []string{"", " ", "\t", " \t\n "} {
		t.Run("blank-"+strings.ReplaceAll(group, " ", "space"), func(t *testing.T) {
			if err := ValidateConsumerGroup(group); err == nil {
				t.Fatalf("group %q accepted; want validation error", group)
			}
		})
	}
	if err := ValidateConsumerGroup("audit-consumer"); err != nil {
		t.Fatalf("valid group rejected: %v", err)
	}
}

func TestValidateTopicTopology(t *testing.T) {
	for _, test := range []struct {
		name   string
		source string
		dlq    string
		valid  bool
	}{
		{name: "distinct topics", source: TopicAccepted, dlq: TopicDLQ, valid: true},
		{name: "empty source uses accepted default", source: "", dlq: TopicDLQ, valid: true},
		{name: "disabled DLQ is library-compatible", source: TopicAccepted, dlq: "", valid: true},
		{name: "self loop", source: "same", dlq: "same", valid: false},
		{name: "default source self loop", source: "", dlq: TopicAccepted, valid: false},
		{name: "whitespace around collision", source: " same ", dlq: "same", valid: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateTopicTopology(test.source, test.dlq)
			if test.valid && err != nil {
				t.Fatalf("ValidateTopicTopology() error = %v, want nil", err)
			}
			if !test.valid && (err == nil || !strings.Contains(err.Error(), "source topic")) {
				t.Fatalf("ValidateTopicTopology() error = %v, want source-topic diagnostic", err)
			}
		})
	}
}
