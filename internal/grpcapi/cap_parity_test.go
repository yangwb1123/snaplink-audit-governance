package grpcapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCCapAliasesUseDomainConstants(t *testing.T) {
	if MaxEnvelopeFieldBytes != domain.MaxEnvelopeFieldBytes ||
		MaxActorRoles != domain.MaxActorRoles ||
		MaxTargetsPerEvent != domain.MaxTargetsPerEvent ||
		MaxChangedFields != domain.MaxChangedFields ||
		MaxBatchEvents != domain.MaxBatchEvents {
		t.Fatalf("gRPC cap aliases drifted from domain constants")
	}
}

// TestGRPCHTTPResourceCapParity replaces the former transport asymmetry pin.
// Every rejection is checked at both protocol boundaries and in both backing
// snapshots. The mutators derive all boundaries from domain constants so a
// runtime limit change cannot leave a numeric test fixture silently stale.
func TestGRPCHTTPResourceCapParity(t *testing.T) {
	grpcStore, client, ctx := newGRPCHarness(t)
	httpServer, httpStore := newHTTPIngestLeg(t)

	over := strings.Repeat("x", domain.MaxEnvelopeFieldBytes+1)
	utf8Over := strings.Repeat("é", domain.MaxEnvelopeFieldBytes/len("é")) + "x"
	canonicalOver := canonicalJSONString(strings.Repeat("v", domain.MaxEnvelopeFieldBytes-1))
	cases := []struct {
		name       string
		mutateGRPC func(*auditv1.EventEnvelope)
		mutateHTTP func(*domain.Event)
	}{
		{
			name:       "top-level string",
			mutateGRPC: func(event *auditv1.EventEnvelope) { event.Reason = over },
			mutateHTTP: func(event *domain.Event) { event.Reason = over },
		},
		{
			name:       "top-level UTF-8 one-byte-over",
			mutateGRPC: func(event *auditv1.EventEnvelope) { event.Reason = utf8Over },
			mutateHTTP: func(event *domain.Event) { event.Reason = utf8Over },
		},
		{
			name:       "actor role string",
			mutateGRPC: func(event *auditv1.EventEnvelope) { event.Actor.Roles = []string{over} },
			mutateHTTP: func(event *domain.Event) { event.Actor.Roles = []string{over} },
		},
		{
			name: "actor role count",
			mutateGRPC: func(event *auditv1.EventEnvelope) {
				event.Actor.Roles = make([]string, domain.MaxActorRoles+1)
			},
			mutateHTTP: func(event *domain.Event) {
				event.Actor.Roles = make([]string, domain.MaxActorRoles+1)
			},
		},
		{
			name: "target string",
			mutateGRPC: func(event *auditv1.EventEnvelope) {
				event.Targets = []*auditv1.Target{{Name: over}}
			},
			mutateHTTP: func(event *domain.Event) {
				event.Targets = []domain.Target{{Name: over}}
			},
		},
		{
			name: "target count",
			mutateGRPC: func(event *auditv1.EventEnvelope) {
				event.Targets = make([]*auditv1.Target, domain.MaxTargetsPerEvent+1)
			},
			mutateHTTP: func(event *domain.Event) {
				event.Targets = make([]domain.Target, domain.MaxTargetsPerEvent+1)
			},
		},
		{
			name: "changed-field canonical before value",
			mutateGRPC: func(event *auditv1.EventEnvelope) {
				event.ChangedFields = []*auditv1.FieldChange{{Field: "field", BeforeJson: canonicalOver}}
			},
			mutateHTTP: func(event *domain.Event) {
				event.ChangedFields = map[string]domain.FieldChange{
					"field": {Before: strings.Repeat("v", domain.MaxEnvelopeFieldBytes-1)},
				}
			},
		},
		{
			name: "changed-field canonical after value",
			mutateGRPC: func(event *auditv1.EventEnvelope) {
				event.ChangedFields = []*auditv1.FieldChange{{Field: "field", AfterJson: canonicalOver}}
			},
			mutateHTTP: func(event *domain.Event) {
				event.ChangedFields = map[string]domain.FieldChange{
					"field": {After: strings.Repeat("v", domain.MaxEnvelopeFieldBytes-1)},
				}
			},
		},
		{
			name: "changed-field escaped less-than canonical value",
			mutateGRPC: func(event *auditv1.EventEnvelope) {
				event.ChangedFields = []*auditv1.FieldChange{{Field: "field", AfterJson: rawJSONString(strings.Repeat("<", 1366))}}
			},
			mutateHTTP: func(event *domain.Event) {
				event.ChangedFields = map[string]domain.FieldChange{
					"field": {After: strings.Repeat("<", 1366)},
				}
			},
		},
		{
			name: "changed-field escaped greater-than canonical value",
			mutateGRPC: func(event *auditv1.EventEnvelope) {
				event.ChangedFields = []*auditv1.FieldChange{{Field: "field", AfterJson: rawJSONString(strings.Repeat(">", 1366))}}
			},
			mutateHTTP: func(event *domain.Event) {
				event.ChangedFields = map[string]domain.FieldChange{
					"field": {After: strings.Repeat(">", 1366)},
				}
			},
		},
		{
			name: "changed-field escaped ampersand canonical value",
			mutateGRPC: func(event *auditv1.EventEnvelope) {
				event.ChangedFields = []*auditv1.FieldChange{{Field: "field", AfterJson: rawJSONString(strings.Repeat("&", 1366))}}
			},
			mutateHTTP: func(event *domain.Event) {
				event.ChangedFields = map[string]domain.FieldChange{
					"field": {After: strings.Repeat("&", 1366)},
				}
			},
		},
		{
			name: "changed-field count",
			mutateGRPC: func(event *auditv1.EventEnvelope) {
				event.ChangedFields = protoChangedFields(domain.MaxChangedFields + 1)
			},
			mutateHTTP: func(event *domain.Event) {
				event.ChangedFields = domainChangedFields(domain.MaxChangedFields + 1)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := "parity-cap-" + strings.ReplaceAll(tc.name, " ", "-")
			grpcEvent := parityEnvelope(id, `{"value":1}`)
			tc.mutateGRPC(grpcEvent)
			if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: grpcEvent}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("gRPC code=%v, want InvalidArgument", status.Code(err))
			}

			httpEvent := parityHTTPEvent(id, map[string]any{"value": json.Number("1")})
			tc.mutateHTTP(&httpEvent)
			response := postParityHTTPEvent(t, httpServer.URL, httpEvent)
			if response.Status != http.StatusBadRequest || response.Code != "invalid_request" {
				t.Fatalf("HTTP response=%+v, want 400 invalid_request", response)
			}
			assertCapEventAbsent(t, grpcStore, id)
			assertCapEventAbsent(t, httpStore, id)
		})
	}

	t.Run("exact boundaries", func(t *testing.T) {
		id := "parity-cap-boundary"
		exact := strings.Repeat("é", domain.MaxEnvelopeFieldBytes/len("é"))
		escapedExactValue := strings.Repeat("<", (domain.MaxEnvelopeFieldBytes-2)/6)
		canonicalExact := rawJSONString(escapedExactValue)
		grpcEvent := parityEnvelope(id, `{"value":1}`)
		grpcEvent.Reason = exact
		grpcEvent.Actor.Roles = make([]string, domain.MaxActorRoles)
		grpcEvent.Targets = make([]*auditv1.Target, domain.MaxTargetsPerEvent)
		grpcEvent.ChangedFields = protoChangedFields(domain.MaxChangedFields)
		grpcEvent.ChangedFields[0].AfterJson = canonicalExact
		if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: grpcEvent}); err != nil {
			t.Fatalf("gRPC exact-boundary event rejected: %v", err)
		}

		httpEvent := parityHTTPEvent(id, map[string]any{"value": json.Number("1")})
		httpEvent.Reason = exact
		httpEvent.Actor.Roles = make([]string, domain.MaxActorRoles)
		httpEvent.Targets = make([]domain.Target, domain.MaxTargetsPerEvent)
		httpEvent.ChangedFields = domainChangedFields(domain.MaxChangedFields)
		httpEvent.ChangedFields["field-0"] = domain.FieldChange{After: escapedExactValue}
		response := postParityHTTPEvent(t, httpServer.URL, httpEvent)
		if response.Status != http.StatusAccepted {
			t.Fatalf("HTTP exact-boundary status=%d, want 202 (code=%q message=%q)", response.Status, response.Code, response.Message)
		}
		assertCapEventPresent(t, grpcStore, id)
		assertCapEventPresent(t, httpStore, id)
	})
}

// TestGRPCHTTPCanonicalWireWhitespaceParity proves that the cap is applied
// to the canonical persisted value rather than to a larger, whitespace-padded
// proto spelling. Both transports must accept the same logical before/after
// values and derive the same source digest.
func TestGRPCHTTPCanonicalWireWhitespaceParity(t *testing.T) {
	grpcStore, client, ctx := newGRPCHarness(t)
	httpServer, httpStore := newHTTPIngestLeg(t)

	const eventID = "parity-canonical-whitespace"
	raw := strings.Repeat(" ", domain.MaxEnvelopeFieldBytes+128) + `{"v":1}`
	grpcEvent := parityEnvelope(eventID, `{"value":1}`)
	grpcEvent.ChangedFields = []*auditv1.FieldChange{{Field: "amount", BeforeJson: raw, AfterJson: raw}}
	if _, err := client.Write(ctx, &auditv1.WriteRequest{Event: grpcEvent}); err != nil {
		t.Fatalf("gRPC accepted canonical value was rejected: %v", err)
	}

	httpEvent := parityHTTPEvent(eventID, map[string]any{"value": json.Number("1")})
	httpEvent.ChangedFields = map[string]domain.FieldChange{
		"amount": {Before: map[string]any{"v": json.Number("1")}, After: map[string]any{"v": json.Number("1")}},
	}
	response := postParityHTTPEvent(t, httpServer.URL, httpEvent)
	if response.Status != http.StatusAccepted {
		t.Fatalf("HTTP accepted canonical value status=%d code=%q message=%q", response.Status, response.Code, response.Message)
	}
	if grpcDigest, httpDigest := storedDigest(t, grpcStore, eventID), storedDigest(t, httpStore, eventID); grpcDigest != httpDigest {
		t.Fatalf("canonical whitespace source digests differ: gRPC=%s HTTP=%s", grpcDigest, httpDigest)
	}
}

func canonicalJSONString(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

// rawJSONString deliberately leaves HTML-sensitive characters unescaped. It
// is valid JSON for the test inputs and models a compact proto spelling whose
// raw bytes fit the cap while CanonicalJSON expands <, >, and & to \u003c,
// \u003e, and \u0026.
func rawJSONString(value string) string {
	return `"` + value + `"`
}

func protoChangedFields(count int) []*auditv1.FieldChange {
	changes := make([]*auditv1.FieldChange, count)
	for i := range changes {
		changes[i] = &auditv1.FieldChange{Field: fmt.Sprintf("field-%d", i)}
	}
	return changes
}

func domainChangedFields(count int) map[string]domain.FieldChange {
	changes := make(map[string]domain.FieldChange, count)
	for i := 0; i < count; i++ {
		changes[fmt.Sprintf("field-%d", i)] = domain.FieldChange{}
	}
	return changes
}

type parityHTTPResponse struct {
	Status  int
	Code    string
	Message string
}

func postParityHTTPEvent(t *testing.T, baseURL string, event domain.Event) parityHTTPResponse {
	t.Helper()
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, baseURL+"/api/v1/events", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer dev:tenant-a:service:crm")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	return parityHTTPResponse{Status: response.StatusCode, Code: envelope.Error.Code, Message: envelope.Error.Message}
}

func assertCapEventAbsent(t *testing.T, st *store.Store, eventID string) {
	t.Helper()
	if err := st.Read(func(data *store.Snapshot) error {
		key := store.EventKey("tenant-a", eventID)
		if _, exists := data.Events[key]; exists {
			t.Fatalf("rejected event %q was persisted", eventID)
		}
		if _, exists := data.Receipts[key]; exists {
			t.Fatalf("rejected receipt %q was persisted", eventID)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func assertCapEventPresent(t *testing.T, st *store.Store, eventID string) {
	t.Helper()
	if err := st.Read(func(data *store.Snapshot) error {
		key := store.EventKey("tenant-a", eventID)
		if _, exists := data.Events[key]; !exists {
			t.Fatalf("accepted event %q was not persisted", eventID)
		}
		if _, exists := data.Receipts[key]; !exists {
			t.Fatalf("accepted receipt %q was not persisted", eventID)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
