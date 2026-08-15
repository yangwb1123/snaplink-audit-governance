package outbox

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

const (
	// StatusPending marks a record waiting for delivery or retry.
	StatusPending = "pending"
	// StatusDelivered marks a record accepted by the audit API.
	StatusDelivered = "delivered"
	// StatusFailed is the dead-letter state after permanent errors or
	// exhausted retries.
	StatusFailed = "failed"
)

// Update is the persisted effect of one delivery attempt on an outbox
// record.
type Update struct {
	Status           string
	Attempts         int
	NextAttemptAt    time.Time
	LastError        string
	DeliveredAt      time.Time
	DeliveredEventID string
	APIStatus        string
}

// CorruptRecord reports a scanned row whose payload failed to decode into
// domain.Event. The relay dead-letters it; it is never delivered.
type CorruptRecord struct {
	ID       int64
	Attempts int
	Err      error
}

// Store is the outbox persistence view required by Relay. PostgresStore is
// the production implementation; tests use an in-memory fake.
type Store interface {
	// ListPending returns up to limit due records (status pending and
	// next_attempt_at reached), ordered by retry time, plus a report of
	// scanned rows whose payloads failed to decode into domain.Event.
	// Decode failures no longer abort the batch: the corrupt rows are
	// excluded from records and reported for the relay to dead-letter.
	ListPending(ctx context.Context, limit int) ([]Record, []CorruptRecord, error)
	// Update applies one delivery outcome under the optimistic condition
	// status = 'pending'. It returns false when another Relay instance
	// already completed the record; the audit API idempotency absorbs the
	// duplicate delivery.
	Update(ctx context.Context, id int64, patch Update) (bool, error)
}

// DeliveryError classifies a failed delivery. Permanent errors (client
// errors such as 403/409/422) dead-letter immediately instead of retrying;
// 401 is deliberately retryable so a token rotation heals the backlog.
// StatusCode lets callers discriminate the failure cause (401 = credential
// problem) without parsing the message; it is 0 for non-HTTP errors.
type DeliveryError struct {
	Permanent bool
	// StatusCode is the HTTP status that produced the failure, or 0 for
	// non-HTTP errors (dial, timeout, DNS). Populated by HTTPDeliverer at
	// the single non-2xx classification site and by the unverified 2xx
	// receipt path, so it is never misleading.
	StatusCode int
	Err        error
}

func (e *DeliveryError) Error() string { return e.Err.Error() }
func (e *DeliveryError) Unwrap() error { return e.Err }

// DeliverFunc delivers one canonical event to the audit ingestion endpoint.
// On success it returns the audit API receipt the deliverer verified
// (event_id matched, status proves ledgering) — or (nil, nil) when the
// transport provides no receipt at all (e.g. Kafka topic write). A 2xx that
// cannot be verified is an error, never a nil receipt.
type DeliverFunc func(ctx context.Context, event domain.Event) (*domain.EventReceipt, error)

// Relay consumes pending audit_outbox records written by business
// transactions and delivers them to the audit API. Success marks a record
// delivered; failures retry with exponential backoff until MaxAttempts is
// reached or a permanent error occurs, then dead-letter the record. The
// audit API is idempotent per event_id, so concurrent Relay instances may
// safely process the same record.
type Relay struct {
	Store       Store
	Deliver     DeliverFunc
	BatchSize   int
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	Logger      *log.Logger
	Now         func() time.Time
}

func (r *Relay) batchSize() int {
	if r.BatchSize <= 0 {
		return 100
	}
	return r.BatchSize
}

func (r *Relay) maxAttempts() int {
	if r.MaxAttempts <= 0 {
		return 8
	}
	return r.MaxAttempts
}

func (r *Relay) baseBackoff() time.Duration {
	if r.BaseBackoff <= 0 {
		return time.Second
	}
	return r.BaseBackoff
}

func (r *Relay) maxBackoff() time.Duration {
	if r.MaxBackoff <= 0 {
		return 5 * time.Minute
	}
	return r.MaxBackoff
}

func (r *Relay) now() time.Time {
	if r.Now == nil {
		return time.Now().UTC()
	}
	return r.Now().UTC()
}

func (r *Relay) logf(format string, args ...any) {
	if r.Logger != nil {
		r.Logger.Printf(format, args...)
	}
}

// RunOnce processes one batch of due records and returns how many records
// were handled (successfully, failed, or quarantined as corrupt). Corrupt
// rows are dead-lettered first so they can never wedge the batch; delivery
// of the decodable records is unchanged.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	records, corrupt, err := r.Store.ListPending(ctx, r.batchSize())
	if err != nil {
		return 0, err
	}
	handled := 0
	for _, c := range corrupt {
		r.quarantine(ctx, c)
		handled++
	}
	for _, record := range records {
		receipt, err := r.Deliver(ctx, record.Event)
		if err != nil {
			r.fail(ctx, record, err)
		} else {
			r.succeed(ctx, record, receipt)
		}
		handled++
	}
	return handled, nil
}

// quarantine dead-letters a corrupt payload. A payload that cannot decode
// is permanently undeliverable regardless of MaxAttempts, so it bypasses
// the retry classification in fail. Best-effort: a failed or lost Update
// is logged and the row is re-reported on the next poll.
func (r *Relay) quarantine(ctx context.Context, corrupt CorruptRecord) {
	now := r.now()
	applied, err := r.Store.Update(ctx, corrupt.ID, Update{
		Status:        StatusFailed,
		Attempts:      corrupt.Attempts + 1,
		NextAttemptAt: now,
		LastError:     corrupt.Err.Error(),
	})
	if err != nil {
		r.logf("outbox id=%d corrupt payload quarantine failed: %v", corrupt.ID, err)
		return
	}
	if applied {
		r.logf("outbox id=%d corrupt payload quarantined: %v", corrupt.ID, corrupt.Err)
	} else {
		r.logf("outbox id=%d corrupt payload already handled by another instance", corrupt.ID)
	}
}

// succeed persists only receipt-derived delivery bookkeeping (REQ-5). When
// the transport provides no receipt (receipt == nil, e.g. Kafka), the two
// API-derived columns stay empty and PostgresStore maps them to SQL NULL —
// the migration contract says api_status is the "API receipt status observed
// at delivery time", and none was observed. No field is fabricated.
func (r *Relay) succeed(ctx context.Context, record Record, receipt *domain.EventReceipt) {
	now := r.now()
	deliveredEventID, apiStatus := "", ""
	if receipt != nil {
		deliveredEventID = receipt.EventID
		apiStatus = receipt.Status
	}
	applied, err := r.Store.Update(ctx, record.ID, Update{
		Status:           StatusDelivered,
		Attempts:         record.Attempts + 1,
		NextAttemptAt:    now,
		DeliveredAt:      now,
		DeliveredEventID: deliveredEventID,
		APIStatus:        apiStatus,
	})
	if err != nil {
		r.logf("outbox id=%d delivered but status update failed: %v", record.ID, err)
		return
	}
	if applied {
		if receipt != nil {
			r.logf("outbox id=%d delivered event_id=%s api_status=%s", record.ID, receipt.EventID, receipt.Status)
		} else {
			r.logf("outbox id=%d delivered event_id=%s (no audit receipt; transport provides none)", record.ID, record.Event.EventID)
		}
	} else {
		r.logf("outbox id=%d already handled by another instance", record.ID)
	}
}

func (r *Relay) fail(ctx context.Context, record Record, cause error) {
	var deliveryErr *DeliveryError
	permanent := errors.As(cause, &deliveryErr) && deliveryErr.Permanent
	attempts := record.Attempts + 1
	now := r.now()
	status := StatusPending
	nextAttempt := now.Add(r.backoff(attempts))
	if permanent || attempts >= r.maxAttempts() {
		status = StatusFailed
		nextAttempt = now
	}
	applied, err := r.Store.Update(ctx, record.ID, Update{
		Status:        status,
		Attempts:      attempts,
		NextAttemptAt: nextAttempt,
		LastError:     cause.Error(),
	})
	if err != nil {
		r.logf("outbox id=%d failure status update failed: %v", record.ID, err)
		return
	}
	if applied {
		r.logf("outbox id=%d attempt=%d status=%s error=%v", record.ID, attempts, status, cause)
	}
}

// backoff returns the wait before the given attempt: attempt 1 waits
// BaseBackoff, then doubles until MaxBackoff.
func (r *Relay) backoff(attempt int) time.Duration {
	backoff := r.baseBackoff()
	for i := 1; i < attempt && backoff < r.maxBackoff(); i++ {
		backoff *= 2
		if backoff > r.maxBackoff() {
			return r.maxBackoff()
		}
	}
	return backoff
}
