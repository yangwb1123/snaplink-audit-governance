package httpapi

// REQ-7 (D-7): fixture-driven rejection matrix for the fixed AsyncAPI
// EventEnvelope contract. api/asyncapi/conformance/producer_minimal.json
// carries three envelopes with machine-checked expected_status/expected_code:
//
//   - producer_minimal_payloadless → 400 invalid_request (ErrInvalid, the
//     direction's core correction: NOT 422 — 422 is reserved for
//     schema_not_found / tenant_mismatch / occurred_at_out_of_range);
//   - tenant_mismatch_contrast → 422 tenant_mismatch (DS-08, after
//     ValidateBasic passes);
//   - occurred_at_out_of_range_contrast → 422 occurred_at_out_of_range
//     (ErrOccurredAtOutOfRange matched before ErrInvalid in statusForError).
//
// Each case runs through the full handler harness (erpTestHTTPServer +
// postTestEvent, same shape as erp_contract_test.go). FM-7: for every case
// where ValidateBasic rejects the envelope, the test derives the expected
// status from the mapping boundary (ValidateBasic → statusForError) BEFORE
// the HTTP round-trip, so a fixture that documents a status the runtime
// cannot produce fails on first read even if the handler someday changes.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

func TestAsyncAPIProducerMinimalRejectionMatrix(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "api", "asyncapi", "conformance", "producer_minimal.json"))
	if err != nil {
		t.Fatalf("read conformance fixture: %v", err)
	}
	var cases []struct {
		Name           string          `json:"name"`
		Envelope       json.RawMessage `json:"envelope"`
		ExpectedStatus int             `json:"expected_status"`
		ExpectedCode   string          `json:"expected_code"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatalf("decode conformance fixture: %v", err)
	}
	if len(cases) != 3 {
		t.Fatalf("conformance fixture carries %d cases, want 3", len(cases))
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			var event domain.Event
			if err := json.Unmarshal(c.Envelope, &event); err != nil {
				t.Fatalf("case %q: decode envelope: %v", c.Name, err)
			}
			// FM-7: derive the expectation from the mapping boundary before
			// the round-trip. ValidateBasic rejection → statusForError is the
			// only status the handler can produce for that error; a fixture
			// that claims anything else (e.g. 422 for the payload-less case)
			// is a documentation lie and must red here, not in review.
			if err := event.ValidateBasic(); err != nil {
				derived := statusForError(err)
				if c.ExpectedStatus != derived {
					t.Fatalf("case %q: fixture expected_status=%d but ValidateBasic→statusForError=%d; the fixture documents a status the runtime cannot produce for this envelope", c.Name, c.ExpectedStatus, derived)
				}
			}

			server := erpTestHTTPServer(t)
			defer server.Close()
			result := postTestEvent(t, server.URL, "dev:tenant-a:service:sverp-web", event)
			if result.Status != c.ExpectedStatus {
				t.Fatalf("case %q: status=%d code=%q message=%q, want status=%d", c.Name, result.Status, result.Code, result.Message, c.ExpectedStatus)
			}
			if result.Code != c.ExpectedCode {
				t.Fatalf("case %q: error.code=%q, want %q", c.Name, result.Code, c.ExpectedCode)
			}
		})
	}
}
