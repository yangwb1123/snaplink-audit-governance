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

// maxErrorBodyBytes bounds non-2xx inspection. Only the structured error.code
// is used for tenant-scope classification; response bodies are never logged
// or persisted, and an oversized body cannot consume unbounded memory.
const maxErrorBodyBytes = 64 * 1024

// HTTPDeliverer returns a fixed-credential DeliverFunc for existing relay and
// consumer callers. DLQ replay API mode must use HTTPDelivererForTenant so a
// static token cannot silently be applied to a recovered event from another
// tenant.
func HTTPDeliverer(apiURL, token string, client *http.Client) DeliverFunc {
	return newHTTPDeliverer(apiURL, client, func(context.Context, string) (string, error) {
		return token, nil
	}, false)
}

// HTTPDelivererForTenant returns a DeliverFunc that resolves the bearer token
// from event.TenantID on every delivery. The event tenant is selected from
// the recovered canonical envelope; the API still resolves and authorizes
// the tenant independently. Resolver failures are structured as pending
// tenant-scope failures and never as permanent delivery errors.
func HTTPDelivererForTenant(apiURL string, resolver TenantCredentialResolver, client *http.Client) DeliverFunc {
	if resolver == nil {
		resolver = func(_ context.Context, tenantID string) (string, error) {
			return "", &TenantCredentialError{TenantID: tenantID, Reason: "no tenant credential resolver is configured"}
		}
	}
	return newHTTPDeliverer(apiURL, client, resolver, true)
}

// StaticTenantCredentialResolver adapts the existing one-token deployment to
// an explicit single-tenant replay scope. A blank scope or token is a
// structured configuration failure; a token is never returned for a
// different recovered tenant.
func StaticTenantCredentialResolver(scope, token string) TenantCredentialResolver {
	scope = strings.TrimSpace(scope)
	return func(_ context.Context, tenantID string) (string, error) {
		if scope == "" {
			return "", &TenantCredentialError{TenantID: tenantID, Reason: "replay tenant scope is not configured"}
		}
		if tenantID != scope {
			return "", &TenantCredentialError{TenantID: tenantID, Reason: "recovered tenant is outside the configured replay scope"}
		}
		if strings.TrimSpace(token) == "" {
			return "", &TenantCredentialError{TenantID: tenantID, Reason: "replay credential is not configured"}
		}
		return token, nil
	}
}

func newHTTPDeliverer(apiURL string, client *http.Client, resolve func(context.Context, string) (string, error), tenantAware bool) DeliverFunc {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	// Redirects are classified deterministically as retryable (any 3xx), and
	// a 307/308 body must never be forwarded to the redirect target (Go
	// strips Authorization cross-host but forwards the body on 307/308).
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	// wait_for=ledgered makes the API return the receipt only after the event
	// is ledgered, so status/ledgered_at/hash prove ledgering.
	endpoint := strings.TrimRight(apiURL, "/") + "/api/v1/events?wait_for=ledgered"
	return func(ctx context.Context, event domain.Event) (*domain.EventReceipt, error) {
		token, resolveErr := resolve(ctx, event.TenantID)
		if resolveErr != nil {
			if tenantAware {
				return nil, &DeliveryError{TenantScopeBlocked: true, Err: resolveErr}
			}
			return nil, resolveErr
		}
		if tenantAware && strings.TrimSpace(token) == "" {
			return nil, &DeliveryError{TenantScopeBlocked: true,
				Err: &TenantCredentialError{TenantID: event.TenantID, Reason: "resolver returned an empty credential"}}
		}
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
			return verifiedReceipt(response, event)
		}
		responseBody, _ := io.ReadAll(io.LimitReader(response.Body, maxErrorBodyBytes))
		tenantMismatch := response.StatusCode == http.StatusUnprocessableEntity && responseErrorCode(responseBody) == "tenant_mismatch"
		// All other 4xx classifications retain the existing permanent policy;
		// only the exact structured tenant_mismatch response is retryable.
		permanent := response.StatusCode >= 400 && response.StatusCode < 500 &&
			response.StatusCode != http.StatusUnauthorized &&
			response.StatusCode != http.StatusTooManyRequests && !tenantMismatch
		reason := fmt.Errorf("audit api returned %s", response.Status)
		if tenantMismatch {
			reason = fmt.Errorf("audit api returned %s with error_code=tenant_mismatch", response.Status)
		}
		return nil, &DeliveryError{Permanent: permanent, StatusCode: response.StatusCode,
			TenantMismatch: tenantMismatch, TenantScopeBlocked: tenantMismatch, Err: reason}
	}
}

func verifiedReceipt(response *http.Response, event domain.Event) (*domain.EventReceipt, error) {
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

func responseErrorCode(body []byte) string {
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return ""
	}
	return envelope.Error.Code
}

// unverified classifies a 2xx that cannot be proven as a receipt-verified
// delivery: retryable, never permanent (a 2xx gives no evidence the API
// rejected the event).
func unverified(statusCode int, status string, reason error) error {
	return &DeliveryError{Permanent: false, StatusCode: statusCode,
		Err: fmt.Errorf("audit api returned %s without a verified receipt: %v", status, reason)}
}

// verifyReceipt asserts the four checks that prove THIS event was ledgered
// by the audit API: the receipt event_id matches the payload (V1), the
// status proves ledgering, the ledger timestamp is stamped and the event was
// hashed into the chain. wait_for=ledgered is part of the endpoint contract.
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
