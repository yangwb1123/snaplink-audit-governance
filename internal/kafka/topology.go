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

// ValidateTopicTopology rejects a source/DLQ self-loop or a DLQ collision
// with one of the stock event topics. An empty source uses the Consumer
// default, TopicAccepted; an empty DLQ means that the caller has explicitly
// selected the library's legacy no-publisher compatibility mode. Stock
// composition roots decide whether that compatibility mode is allowed.
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
	for _, eventTopic := range []string{TopicAccepted, TopicLedgered} {
		if dlq == eventTopic {
			return fmt.Errorf("DLQ topic %q collides with event topic %q", dlq, eventTopic)
		}
	}
	return nil
}

// ValidateReplayTopology validates the replay composition root. Replay always
// needs explicit accepted and DLQ destinations plus a non-blank group; unlike
// the generic Consumer API it may not use the no-publisher compatibility mode.
func ValidateReplayTopology(acceptedTopic, dlqTopic, groupID string) error {
	if err := ValidateConsumerGroup(groupID); err != nil {
		return err
	}
	if strings.TrimSpace(acceptedTopic) == "" {
		return errors.New("accepted topic is required for DLQ replay")
	}
	if strings.TrimSpace(dlqTopic) == "" {
		return errors.New("DLQ topic is required for DLQ replay")
	}
	return ValidateTopicTopology(acceptedTopic, dlqTopic)
}
