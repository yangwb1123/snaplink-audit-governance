package main

import (
	"context"
	"log"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/kafka"
	"github.com/snaplink/audit-governance/internal/service"
)

// kafkaLedgeredPublisher keeps Kafka transport details at the command
// adapter boundary. The service package depends only on its publisher port.
type kafkaLedgeredPublisher struct {
	producer *kafka.Producer
}

func (p kafkaLedgeredPublisher) Publish(ctx context.Context, event domain.Event) error {
	_, err := p.producer.Deliver(ctx, event)
	return err
}

const ledgeredOutboxFlushInterval = time.Second

// runLedgeredOutboxLoop drains publications that survived an ingest response
// or an API restart. The ledger remains authoritative; this loop only repairs
// the rebuildable projection feed and therefore never blocks HTTP/gRPC startup.
func runLedgeredOutboxLoop(ctx context.Context, svc *service.Service, logger *log.Logger) {
	flush := func() {
		published, err := svc.FlushLedgeredOutbox(ctx, 100)
		if err != nil {
			logger.Printf("ledgered outbox flush failed: %v", err)
			return
		}
		if published > 0 {
			logger.Printf("ledgered outbox flushed=%d", published)
		}
	}
	flush()
	ticker := time.NewTicker(ledgeredOutboxFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flush()
		}
	}
}
