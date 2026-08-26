package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/snaplink/audit-governance/internal/outbox"
)

func tenantFailureMessage(eventID, tenant, code string, offset int64) kafka.Message {
	failure := Failure{EventID: eventID, TenantID: tenant, ErrorCode: code, ErrorMessage: "boom"}
	value, _ := json.Marshal(failure)
	return kafka.Message{Topic: TopicDLQ, Key: []byte(eventID), Value: value, Partition: 0, Offset: offset}
}

func tenantAcceptedMessage(eventID, tenant string, offset int64) kafka.Message {
	value := []byte(fmt.Sprintf(`{"event_id":%q,"tenant_id":%q}`, eventID, tenant))
	return kafka.Message{Topic: TopicAccepted, Key: []byte(eventID), Value: value, Partition: 0, Offset: offset}
}

func runTenantReplayer(broker *brokerDLQ, accepted []kafka.Message, state *ReplayState, deliver RepublishFunc) *Replayer {
	replayer := newTenantAwareReplayerWithFactories(broker.freshDLQSession(), freshAcceptedReader(accepted, nil), state, deliver, nil)
	replayer.drainTimeout = 10 * time.Millisecond
	return replayer
}

// TestTenantMismatchLeavesPermanentErrorPending pins the primary loss
// boundary: a misleading Failure claim and permanent_error code cannot make a
// 422 tenant_mismatch enter the one-shot closure path.
func TestTenantMismatchLeavesPermanentErrorPending(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if got := r.Header.Get("Authorization"); got != "Bearer tenant-a-token" {
			t.Errorf("authorization=%q, want tenant-a token for the deliberately mis-scoped resolver", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"error":{"code":"tenant_mismatch","message":"wrong tenant"}}`))
	}))
	defer server.Close()

	statePath := filepath.Join(t.TempDir(), "replay-state.json")
	state, err := LoadReplayState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{
		tenantFailureMessage("evt-b", "tenant-a", ErrorCodePermanentError, 0),
	}}
	deliver := outbox.HTTPDelivererForTenant(server.URL, func(context.Context, string) (string, error) {
		return "tenant-a-token", nil
	}, nil)
	replayer := runTenantReplayer(broker, []kafka.Message{tenantAcceptedMessage("evt-b", "tenant-b", 0)}, state, func(ctx context.Context, key, value []byte) error {
		event, err := EventFromCanonical(value)
		if err != nil {
			return err
		}
		_, err = deliver(ctx, event)
		return err
	})
	defer replayer.Close()

	if _, err := replayer.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 1 {
		t.Fatalf("HTTP requests=%d, want one attempted delivery", requests.Load())
	}
	if state.Marked("tenant-b", "evt-b") || state.Replayed["evt-b"] {
		t.Fatal("tenant mismatch must not create a tenant/event or compatibility mark")
	}
	if broker.committed != 0 || len(broker.commits) != 0 {
		t.Fatalf("DLQ committed=%d commits=%d, want unchanged 0/0", broker.committed, len(broker.commits))
	}
	metrics := replayer.Metrics()
	if metrics.Replayed != 0 || metrics.Permanent != 0 || metrics.Unresolvable != 0 || metrics.Pending != 0 {
		t.Fatalf("resolution metrics=%+v, tenant mismatch must not resolve the record", metrics)
	}
	if metrics.RepublishFailures != 1 || metrics.TenantScopeMismatches != 1 || metrics.TenantScopePending != 1 {
		t.Fatalf("scope metrics=%+v, want failure=1 mismatch=1 pending=1", metrics)
	}
}

// TestRecoveredTenantSelectsCredential covers both the misleading and absent
// Failure.tenant_id forms. The resolver and request body must use the
// accepted envelope tenant, never the DLQ claim.
func TestRecoveredTenantSelectsCredential(t *testing.T) {
	for _, claim := range []string{"tenant-a", ""} {
		t.Run(map[bool]string{true: "claim-present", false: "claim-absent"}[claim != ""], func(t *testing.T) {
			var resolvedTenant string
			var requestTenant string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("Authorization"); got != "Bearer tenant-b-token" {
					t.Errorf("authorization=%q, want tenant-b token", got)
				}
				var body struct {
					EventID  string `json:"event_id"`
					TenantID string `json:"tenant_id"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("decode request: %v", err)
				}
				requestTenant = body.TenantID
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(w, `{"receipt":{"event_id":%q,"status":"ledgered","ledgered_at":"2026-08-04T12:00:01Z","hash":"abc123"}}`, body.EventID)
			}))
			defer server.Close()
			state, err := LoadReplayState(filepath.Join(t.TempDir(), "state.json"))
			if err != nil {
				t.Fatal(err)
			}
			broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{tenantFailureMessage("evt-b", claim, ErrorCodeAttemptsExhausted, 0)}}
			deliver := outbox.HTTPDelivererForTenant(server.URL, func(_ context.Context, tenantID string) (string, error) {
				resolvedTenant = tenantID
				return "tenant-b-token", nil
			}, nil)
			replayer := runTenantReplayer(broker, []kafka.Message{tenantAcceptedMessage("evt-b", "tenant-b", 0)}, state, func(ctx context.Context, key, value []byte) error {
				event, err := EventFromCanonical(value)
				if err != nil {
					return err
				}
				_, err = deliver(ctx, event)
				return err
			})
			defer replayer.Close()
			if _, err := replayer.RunOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			if resolvedTenant != "tenant-b" || requestTenant != "tenant-b" {
				t.Fatalf("resolvedTenant=%q requestTenant=%q, want tenant-b/tenant-b", resolvedTenant, requestTenant)
			}
			if !state.Marked("tenant-b", "evt-b") || broker.committed != 1 {
				t.Fatalf("state/commit=%v/%d, want tenant-b mark and committed offset 1", state.Marked("tenant-b", "evt-b"), broker.committed)
			}
		})
	}
}

// TestTenantReplaySucceedsAcrossRestart proves the pending record is
// re-delivered after the mis-scoped round and that a durable scoped mark is
// the idempotency boundary before the broker commit.
func TestTenantReplaySucceedsAcrossRestart(t *testing.T) {
	var mode atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if mode.Load() {
			if got := r.Header.Get("Authorization"); got != "Bearer tenant-b-token" {
				t.Errorf("retry authorization=%q, want tenant-b token", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"receipt":{"event_id":"evt-b","status":"ledgered","ledgered_at":"2026-08-04T12:00:01Z","hash":"abc123"}}`)
			return
		}
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = fmt.Fprint(w, `{"error":{"code":"tenant_mismatch","message":"wrong tenant"}}`)
	}))
	defer server.Close()

	statePath := filepath.Join(t.TempDir(), "state.json")
	state, err := LoadReplayState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{tenantFailureMessage("evt-b", "tenant-a", ErrorCodePermanentError, 0)}}
	resolver := func(_ context.Context, tenantID string) (string, error) {
		if mode.Load() {
			return "tenant-b-token", nil
		}
		return "tenant-a-token", nil
	}
	makeDeliver := func() RepublishFunc {
		deliver := outbox.HTTPDelivererForTenant(server.URL, resolver, nil)
		return func(ctx context.Context, key, value []byte) error {
			event, err := EventFromCanonical(value)
			if err != nil {
				return err
			}
			_, err = deliver(ctx, event)
			return err
		}
	}
	accepted := []kafka.Message{tenantAcceptedMessage("evt-b", "tenant-b", 0)}
	first := runTenantReplayer(broker, accepted, state, makeDeliver())
	if _, err := first.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if broker.committed != 0 || state.Marked("tenant-b", "evt-b") {
		t.Fatalf("first round must stay pending: committed=%d marked=%v", broker.committed, state.Marked("tenant-b", "evt-b"))
	}

	reloaded, err := LoadReplayState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	mode.Store(true)
	second := runTenantReplayer(broker, accepted, reloaded, makeDeliver())
	if _, err := second.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reloaded.Marked("tenant-b", "evt-b") || broker.committed != 1 {
		t.Fatalf("retry state/commit=%v/%d, want true/1", reloaded.Marked("tenant-b", "evt-b"), broker.committed)
	}
	final, err := LoadReplayState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if !final.Marked("tenant-b", "evt-b") || final.Marked("tenant-a", "evt-b") {
		t.Fatalf("restart state=%v, want only tenant-b scoped mark", final)
	}

	// A subsequent round sees no uncommitted DLQ record and cannot submit the
	// event again. This also pins that the original record, not a new ID, was
	// the commit boundary.
	third := runTenantReplayer(broker, accepted, final, makeDeliver())
	if _, err := third.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTenantReplayStateIsInjectiveAndLegacyFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state, err := LoadReplayState(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.MarkTenant("tenant-a", "same"); err != nil {
		t.Fatal(err)
	}
	if err := state.MarkTenant("tenant-b", "same"); err != nil {
		t.Fatal(err)
	}
	reloaded, err := LoadReplayState(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reloaded.Marked("tenant-a", "same") || !reloaded.Marked("tenant-b", "same") {
		t.Fatalf("scoped marks lost: a=%v b=%v", reloaded.Marked("tenant-a", "same"), reloaded.Marked("tenant-b", "same"))
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"tenant_id":"tenant-a"`) || !strings.Contains(string(raw), `"tenant_id":"tenant-b"`) {
		t.Fatalf("state does not persist both tenant components: %s", raw)
	}

	legacyPath := filepath.Join(t.TempDir(), "legacy.json")
	if err := os.WriteFile(legacyPath, []byte(`{"replayed":{"same":true}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	legacy, err := LoadReplayState(legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Marked("tenant-a", "same") {
		t.Fatal("legacy event-ID-only mark must not be honored without a proven scope")
	}
	legacy.SetLegacyTenantScope("tenant-a")
	if !legacy.Marked("tenant-a", "same") || legacy.Marked("tenant-b", "same") {
		t.Fatal("legacy scope must honor only the explicitly proven tenant")
	}
}

func TestTenantReplayKeepsSameEventRecordsIndependent(t *testing.T) {
	t.Run("one canonical tenant only resolves compatible claim", func(t *testing.T) {
		state, err := LoadReplayState(filepath.Join(t.TempDir(), "state.json"))
		if err != nil {
			t.Fatal(err)
		}
		broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{
			tenantFailureMessage("evt", "tenant-b", ErrorCodeAttemptsExhausted, 0),
			tenantFailureMessage("evt", "tenant-a", ErrorCodeAttemptsExhausted, 1),
		}}
		var delivered int
		replayer := runTenantReplayer(broker, []kafka.Message{tenantAcceptedMessage("evt", "tenant-b", 0)}, state, func(_ context.Context, _, value []byte) error {
			event, err := EventFromCanonical(value)
			if err != nil {
				return err
			}
			if event.TenantID != "tenant-b" {
				t.Fatalf("delivered tenant=%q, want tenant-b", event.TenantID)
			}
			delivered++
			return nil
		})
		defer replayer.Close()
		if count, err := replayer.RunOnce(context.Background()); err != nil || count != 1 {
			t.Fatalf("round result count=%d err=%v, want 1/nil", count, err)
		}
		if delivered != 1 || !state.Marked("tenant-b", "evt") || state.Marked("tenant-a", "evt") {
			t.Fatalf("delivered=%d marks a=%v b=%v, want 1/false/true", delivered, state.Marked("tenant-a", "evt"), state.Marked("tenant-b", "evt"))
		}
		if len(broker.commits) != 1 || broker.commits[0].Offset != 0 {
			t.Fatalf("commits=%v, want only compatible offset 0", broker.commits)
		}
	})

	t.Run("both canonical tenants resolve and survive reload", func(t *testing.T) {
		statePath := filepath.Join(t.TempDir(), "state.json")
		state, err := LoadReplayState(statePath)
		if err != nil {
			t.Fatal(err)
		}
		broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{
			tenantFailureMessage("evt", "tenant-a", ErrorCodeAttemptsExhausted, 0),
			tenantFailureMessage("evt", "tenant-b", ErrorCodeAttemptsExhausted, 1),
		}}
		delivered := map[string]int{}
		replayer := runTenantReplayer(broker, []kafka.Message{
			tenantAcceptedMessage("evt", "tenant-a", 0),
			tenantAcceptedMessage("evt", "tenant-b", 1),
		}, state, func(_ context.Context, _, value []byte) error {
			event, err := EventFromCanonical(value)
			if err != nil {
				return err
			}
			delivered[event.TenantID]++
			return nil
		})
		defer replayer.Close()
		if count, err := replayer.RunOnce(context.Background()); err != nil || count != 2 {
			t.Fatalf("round result count=%d err=%v, want 2/nil", count, err)
		}
		if delivered["tenant-a"] != 1 || delivered["tenant-b"] != 1 || broker.committed != 2 {
			t.Fatalf("delivered=%v committed=%d, want one per tenant and offset 2", delivered, broker.committed)
		}
		reloaded, err := LoadReplayState(statePath)
		if err != nil {
			t.Fatal(err)
		}
		if !reloaded.Marked("tenant-a", "evt") || !reloaded.Marked("tenant-b", "evt") {
			t.Fatalf("reloaded marks a=%v b=%v, want both exact tenant/event marks", reloaded.Marked("tenant-a", "evt"), reloaded.Marked("tenant-b", "evt"))
		}
		if reloaded.Marked("tenant-c", "evt") {
			t.Fatal("a same-ID mark must not authorize an unrelated tenant")
		}
	})
}

// TestTenantReplayRejectsIncompatibleClaimAfterSiblingCommit pins the
// cross-round boundary: once the tenant-A sibling is durably marked and its
// offset committed, the tenant-B-claimed physical record must not inherit
// tenant A's canonical candidate merely because it is now collected alone.
func TestTenantReplayRejectsIncompatibleClaimAfterSiblingCommit(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	state, err := LoadReplayState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{
		tenantFailureMessage("evt", "tenant-a", ErrorCodeAttemptsExhausted, 0),
		tenantFailureMessage("evt", "tenant-b", ErrorCodeAttemptsExhausted, 1),
	}}
	accepted := []kafka.Message{tenantAcceptedMessage("evt", "tenant-a", 0)}
	var deliveries atomic.Int64
	deliver := func(_ context.Context, _, value []byte) error {
		event, err := EventFromCanonical(value)
		if err != nil {
			return err
		}
		if event.TenantID != "tenant-a" {
			t.Fatalf("delivered tenant=%q, want tenant-a", event.TenantID)
		}
		deliveries.Add(1)
		return nil
	}

	first := runTenantReplayer(broker, accepted, state, deliver)
	if count, err := first.RunOnce(context.Background()); err != nil || count != 1 {
		t.Fatalf("round 1 result count=%d err=%v, want 1/nil", count, err)
	}
	if !state.Marked("tenant-a", "evt") || state.Marked("tenant-b", "evt") {
		t.Fatalf("round 1 marks a=%v b=%v, want true/false", state.Marked("tenant-a", "evt"), state.Marked("tenant-b", "evt"))
	}
	if broker.committed != 1 || len(broker.commits) != 1 || broker.commits[0].Offset != 0 {
		t.Fatalf("round 1 committed=%d commits=%v, want offset 0 only", broker.committed, broker.commits)
	}

	reloaded, err := LoadReplayState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	second := runTenantReplayer(broker, accepted, reloaded, deliver)
	if count, err := second.RunOnce(context.Background()); err != nil || count != 0 {
		t.Fatalf("round 2 result count=%d err=%v, want 0/nil", count, err)
	}
	if deliveries.Load() != 1 {
		t.Fatalf("deliveries=%d, want exactly one canonical tenant-A delivery", deliveries.Load())
	}
	if reloaded.Marked("tenant-b", "evt") {
		t.Fatal("incompatible tenant-B record must not inherit tenant-A mark")
	}
	if broker.committed != 1 || len(broker.commits) != 1 {
		t.Fatalf("round 2 committed=%d commits=%v, want offset 1 still pending", broker.committed, broker.commits)
	}
}

func TestTenantReplayMixedErrorCodesRemainRecordScoped(t *testing.T) {
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{
		tenantFailureMessage("evt", "tenant-a", ErrorCodePermanentError, 0),
		tenantFailureMessage("evt", "tenant-b", ErrorCodeAttemptsExhausted, 1),
	}}
	attempts := map[string]int{}
	replayer := runTenantReplayer(broker, []kafka.Message{
		tenantAcceptedMessage("evt", "tenant-a", 0),
		tenantAcceptedMessage("evt", "tenant-b", 1),
	}, state, func(_ context.Context, _, value []byte) error {
		event, err := EventFromCanonical(value)
		if err != nil {
			return err
		}
		attempts[event.TenantID]++
		if event.TenantID == "tenant-a" {
			return errors.New("permanent record failed")
		}
		return errors.New("transient record failed")
	})
	defer replayer.Close()
	if count, err := replayer.RunOnce(context.Background()); err != nil || count != 0 {
		t.Fatalf("round result count=%d err=%v, want 0/nil", count, err)
	}
	if attempts["tenant-a"] != 1 || attempts["tenant-b"] != 1 {
		t.Fatalf("attempts=%v, want one attempt per physical record", attempts)
	}
	if !state.Marked("tenant-a", "evt") || state.Marked("tenant-b", "evt") {
		t.Fatalf("marks a=%v b=%v, want only permanent record closed", state.Marked("tenant-a", "evt"), state.Marked("tenant-b", "evt"))
	}
	if replayer.Metrics().Permanent != 1 || broker.committed != 1 || len(broker.commits) != 1 {
		t.Fatalf("metrics=%+v committed=%d commits=%d, want one closure and only offset 0 committed", replayer.Metrics(), broker.committed, len(broker.commits))
	}
}

func TestTenantReplayRejectsAmbiguousAndKeyFallbackCandidates(t *testing.T) {
	cases := []struct {
		name     string
		accepted []kafka.Message
	}{
		{name: "two tenants", accepted: []kafka.Message{
			tenantAcceptedMessage("evt", "tenant-a", 0), tenantAcceptedMessage("evt", "tenant-b", 1),
		}},
		{name: "valid payload defeats key fallback", accepted: []kafka.Message{{
			Topic: TopicAccepted, Key: []byte("evt"), Value: []byte(`{"event_id":"other","tenant_id":"tenant-b"}`), Partition: 0, Offset: 0,
		}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state, err := LoadReplayState("")
			if err != nil {
				t.Fatal(err)
			}
			broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{tenantFailureMessage("evt", "tenant-a", ErrorCodeAttemptsExhausted, 0)}}
			var attempts atomic.Int64
			replayer := runTenantReplayer(broker, tc.accepted, state, func(context.Context, []byte, []byte) error {
				attempts.Add(1)
				return errors.New("must not deliver ambiguous candidate")
			})
			defer replayer.Close()
			if _, err := replayer.RunOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			if attempts.Load() != 0 || broker.committed != 0 || len(state.Replayed) != 0 {
				t.Fatalf("attempts=%d committed=%d state=%v, wanted pending without guess", attempts.Load(), broker.committed, state.Replayed)
			}
		})
	}
}
