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

// ValidateProjectionDLQ rejects a projector DLQ that is not its own topic.
// The ingest replay DLQ recovers originals from the accepted topic, while a
// projection failure's original lives on TopicLedgered: sharing the topic turns
// every projection failure into a redundant accepted re-ingest or an
// unresolvable permanent close. An empty DLQ keeps the library's legacy
// no-publisher compatibility mode; the projector's composition root rejects it
// separately (an unset DLQ is not durable failure evidence).
func ValidateProjectionDLQ(sourceTopic, dlqTopic string) error {
	if err := ValidateTopicTopology(sourceTopic, dlqTopic); err != nil {
		return err
	}
	dlq := strings.TrimSpace(dlqTopic)
	switch dlq {
	case "", TopicProjectionDLQ:
		return nil
	case TopicDLQ:
		return fmt.Errorf("projector DLQ topic %q is the ingest replay DLQ %q; projection originals live on %q, so %q can neither resolve nor close them — use %q or a dedicated topic",
			dlqTopic, TopicDLQ, TopicLedgered, TopicDLQ, TopicProjectionDLQ)
	}
	return nil
}

// ValidateIngestDLQ rejects an ingest-side DLQ that is the projection DLQ: no
// recovery tool drains it, so ingest failures published there would be
// invisible evidence. Directional, so the shared ValidateTopicTopology rule set
// stays unchanged and its table keeps holding for both pipelines.
func ValidateIngestDLQ(dlqTopic string) error {
	if strings.TrimSpace(dlqTopic) == TopicProjectionDLQ {
		return fmt.Errorf("ingest DLQ topic %q is the projection DLQ %q; projection failures are drained by a projection-specific tool, so ingest records there are never recovered — use %q",
			dlqTopic, TopicProjectionDLQ, TopicDLQ)
	}
	return nil
}

// ValidateReplayTopology validates the replay composition root. Replay always
// needs explicit accepted and DLQ destinations plus a non-blank group; unlike
// the generic Consumer API it may not use the no-publisher compatibility mode.
// The projection DLQ is rejected as well: replay recovers originals from the
// accepted topic, so it can neither resolve nor close a projection failure.
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
	if err := ValidateIngestDLQ(dlqTopic); err != nil {
		return err
	}
	return ValidateTopicTopology(acceptedTopic, dlqTopic)
}
