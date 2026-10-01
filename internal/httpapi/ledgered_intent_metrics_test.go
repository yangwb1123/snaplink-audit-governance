package httpapi

// Regression coverage for audit_ledgered_intent_repaired_total.
//
// The counter is the operator's only evidence that a tenant commit died
// between its durable cold append and its control-plane write, so it has to be
// exposed unlabelled (bounded cardinality: no tenant, event or route label) and
// it has to be safe on a Server that has no Service behind it.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestMetricsWithoutServiceOmitsLedgeredIntentRepairs pins the nil guard: the
// metrics handler must stay safe on a Server with no Service behind it, which
// several handler tests construct directly. The counter is then simply absent
// (no data), not a fabricated zero from a process that cannot ingest at all.
func TestMetricsWithoutServiceOmitsLedgeredIntentRepairs(t *testing.T) {
	recorder := httptest.NewRecorder()
	(&Server{}).metrics(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	body, err := io.ReadAll(recorder.Result().Body)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "audit_ledgered_intent_repaired_total") {
		t.Fatalf("a server without a service must not report the repair counter:\n%s", body)
	}
}

func TestMetricsReportsLedgeredIntentRepairs(t *testing.T) {
	server := testHTTPServer(t)
	defer server.Close()

	request, _ := http.NewRequest(http.MethodGet, server.URL+"/metrics", nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("metrics status=%d, want 200", response.StatusCode)
	}
	body, _ := io.ReadAll(response.Body)
	text := string(body)
	if !strings.Contains(text, "audit_ledgered_intent_repaired_total 0") {
		t.Fatalf("metrics missing the repair counter in:\n%s", text)
	}
	// Bounded cardinality: a tenant, event or route label would let one label
	// per repaired event through, which is exactly what must not happen.
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "audit_ledgered_intent_repaired_total") && strings.Contains(line, "{") {
			t.Fatalf("repair counter must carry no labels, got %q", line)
		}
	}
}
