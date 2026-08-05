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

// HTTPDeliverer returns a DeliverFunc that posts events to the REST
// ingestion endpoint. Client errors (400/403/409/422 etc.) are classified
// as permanent and dead-letter immediately; 429 and 5xx are retryable.
func HTTPDeliverer(apiURL, token string, client *http.Client) DeliverFunc {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	endpoint := strings.TrimRight(apiURL, "/") + "/api/v1/events"
	return func(ctx context.Context, event domain.Event) error {
		body, err := json.Marshal(event)
		if err != nil {
			return err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return err
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("User-Agent", "snaplink-audit-outbox-relay")
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			return nil
		}
		permanent := response.StatusCode >= 400 && response.StatusCode < 500 &&
			response.StatusCode != http.StatusTooManyRequests
		return &DeliveryError{Permanent: permanent, Err: fmt.Errorf("audit api returned %s", response.Status)}
	}
}
