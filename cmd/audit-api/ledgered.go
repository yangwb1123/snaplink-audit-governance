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

const ledgeredOutboxMaxRetryInterval = 30 * time.Second

// runLedgeredOutboxLoop drains publications that survived an ingest response
// or an API restart. The ledger remains authoritative; this loop only repairs
// the rebuildable projection feed and therefore never blocks HTTP/gRPC startup.
func runLedgeredOutboxLoop(ctx context.Context, svc *service.Service, logger *log.Logger) {
	reconciler := &ledgeredReconciler{}
	flush := func() bool {
		result, reconcileErr := reconciler.next(ctx, svc)
		if reconcileErr != nil {
			logger.Printf("ledgered reconciliation failed")
		}
		if result.Repaired > 0 || result.Failed > 0 {
			logger.Printf("ledgered reconciliation scanned=%d repaired=%d evidence_failures=%d", result.Scanned, result.Repaired, result.Failed)
		}
		before := svc.LedgeredPublishFailures()
		published, err := svc.FlushLedgeredOutbox(ctx, 100)
		if err != nil {
			logger.Printf("ledgered outbox flush failed: %v", err)
		}
		if published > 0 {
			logger.Printf("ledgered outbox flushed=%d", published)
		}
		// Broker refusals deliberately retain the durable intent and return
		// no domain error. Observe the existing failure counter as well.
		// A concurrent request-path failure may conservatively delay one
		// round, never suppress it or remove an intent.
		return reconcileErr != nil || result.Failed > 0 || err != nil || svc.LedgeredPublishFailures() > before
	}
	failures := 0
	for ctx.Err() == nil {
		if flush() {
			if failures < 8 {
				failures++
			}
		} else {
			failures = 0
		}
		delay := ledgeredFlushDelay(failures, nil)
		if failures > 0 {
			logger.Printf("ledgered outbox retry delay=%s consecutive_failures=%d", delay, failures)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
