package kafka

import (
	"errors"
	"fmt"
	"strings"
)

// ValidateConsumerGroup rejects the ambiguous group values that would make
// manual offset commits unreliable. Stock ingest and projector composition
// roots validate their group before opening an external resource.
func ValidateConsumerGroup(groupID string) error {
	if strings.TrimSpace(groupID) == "" {
		return errors.New("consumer group must contain non-whitespace characters")
	}
	return nil
}

// ValidateTopicTopology rejects a source/DLQ self-loop. An empty source uses
// the Consumer default, TopicAccepted; an empty DLQ means that the caller has
// explicitly selected the library's legacy no-publisher compatibility mode.
// Stock composition roots decide whether that compatibility mode is allowed.
func ValidateTopicTopology(sourceTopic, dlqTopic string) error {
	source := strings.TrimSpace(sourceTopic)
	if source == "" {
		source = TopicAccepted
	}
	dlq := strings.TrimSpace(dlqTopic)
	if dlq == "" {
		return nil
	}
	if source == dlq {
		return fmt.Errorf("source topic %q equals enabled DLQ topic %q", source, dlq)
	}
	return nil
}
