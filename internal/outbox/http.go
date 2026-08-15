package outbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

// maxReceiptBytes bounds the 2xx receipt body read. The real receipt echoes
// event_id at most twice (receipt + receipt_url) plus a handful of fixed
// fields, so every legitimate receipt is far smaller than this cap; a body
// that still exceeds it fails JSON decode and is classified retryable-
// unverified — never a permanent false dead-letter of an in-ledger event.
const maxReceiptBytes = 64 * 1024

// HTTPDeliverer returns a DeliverFunc that posts events to the audit
// ingestion endpoint and verifies the returned receipt. On a 2xx the body is
// parsed into the ReceiptResponse envelope and checked (event_id match,
// post-ledger status, ledgered_at stamped, hash present) before the delivery
// is reported as verified. A 2xx that cannot be verified is a retryable
// error, never a delivery. Client errors (403/409/422 etc.) are classified
// permanent and dead-letter immediately; 401 (token expiry/rotation), 429
// and 5xx are retryable. The given client's redirect policy is replaced with
// the deliverer's guard, so the audit body is never re-POSTed to a redirect
// target; in-repo callers construct a dedicated client for the deliverer.
func HTTPDeliverer(apiURL, token string, client *http.Client) DeliverFunc {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	// Redirects are classified deterministically as retryable (any 3xx), and
	// a 307/308 body must never be forwarded to the redirect target (Go
	// strips Authorization cross-host but forwards the body on 307/308).
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	// REQ-1: wait_for=ledgered makes the API return the receipt only after
	// the event is ledgered, so status/ledgered_at/hash prove ledgering.
	endpoint := strings.TrimRight(apiURL, "/") + "/api/v1/events?wait_for=ledgered"
	return func(ctx context.Context, event domain.Event) (*domain.EventReceipt, error) {
		body, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("User-Agent", "snaplink-audit-outbox-relay")
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := client.Do(request)
		if err != nil {
			return nil, err
		}
		defer response.Body.Close()
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			// REQ-2/REQ-3: parse and verify the receipt; a 2xx that cannot be
			// verified proves nothing and must not mark the row delivered.
			var envelope struct {
				Receipt domain.EventReceipt `json:"receipt"`
			}
			if err := json.NewDecoder(io.LimitReader(response.Body, maxReceiptBytes)).Decode(&envelope); err != nil {
				return nil, unverified(response.StatusCode, response.Status, fmt.Errorf("receipt body decode failed: %v", err))
			}
			if err := verifyReceipt(event, envelope.Receipt); err != nil {
				return nil, unverified(response.StatusCode, response.Status, err)
			}
			return &envelope.Receipt, nil
		}
		// REQ-6: non-2xx handling — drain and classify. 401 is retryable:
		// an expired/rotated credential heals once fixed, so it must keep the
		// backlog pending instead of dead-lettering it in one poll.
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxReceiptBytes))
		permanent := response.StatusCode >= 400 && response.StatusCode < 500 &&
			response.StatusCode != http.StatusUnauthorized &&
			response.StatusCode != http.StatusTooManyRequests
		return nil, &DeliveryError{Permanent: permanent, StatusCode: response.StatusCode,
			Err: fmt.Errorf("audit api returned %s", response.Status)}
	}
}

// unverified classifies a 2xx that cannot be proven as a receipt-verified
// delivery: retryable, never permanent (a 2xx gives no evidence the API
// rejected the event). The status code is threaded through so the field is
// honest about the transport result (a 2xx is never 401, so classification
// is unaffected).
func unverified(statusCode int, status string, reason error) error {
	return &DeliveryError{Permanent: false, StatusCode: statusCode,
		Err: fmt.Errorf("audit api returned %s without a verified receipt: %v", status, reason)}
}

// verifyReceipt asserts the four checks that prove THIS event was ledgered
// by the audit API: the receipt event_id matches the payload (V1), the
// status proves ledgering (V2), the ledger timestamp is stamped (V3) and the
// event was hashed into the chain (V4). "ledgered" is included in V2 because
// the API's duplicate path returns the stored receipt, which can still carry
// status "ledgered" between commit and the archive/index rewrite.
func verifyReceipt(event domain.Event, receipt domain.EventReceipt) error {
	if receipt.EventID != event.EventID {
		return fmt.Errorf("receipt event_id %q does not match payload event_id %q", receipt.EventID, event.EventID)
	}
	switch receipt.Status {
	case domain.StatusLedgered, domain.StatusIndexed, domain.StatusArchived:
	default:
		return fmt.Errorf("receipt status %q does not prove ledgering (want ledgered/indexed/archived)", receipt.Status)
	}
	if receipt.LedgeredAt.IsZero() {
		return fmt.Errorf("receipt ledgered_at is zero (wait_for=ledgered was not honored)")
	}
	if receipt.Hash == "" {
		return fmt.Errorf("receipt hash is empty")
	}
	return nil
}
