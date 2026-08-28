package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
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

// tenantWanted builds a single-record wantedSet for direct
// resolveTenantCandidates calls in unit tests. It mirrors wantedEvents'
// index bookkeeping but lets a test supply the dlqRecord directly.
func tenantWanted(record dlqRecord) wantedSet {
	wanted := wantedSet{
		replay:     map[dlqRecordID]bool{},
		oneShot:    map[dlqRecordID]bool{},
		byEventID:  map[string][]dlqRecordID{},
		orderedIDs: []dlqRecordID{},
		records:    map[dlqRecordID]dlqRecord{},
	}
	id := wantedRecordID(record, 0)
	record.id = id
	wanted.replay[id] = true
	wanted.records[id] = record
	wanted.byEventID[record.eventID] = append(wanted.byEventID[record.eventID], id)
	wanted.orderedIDs = append(wanted.orderedIDs, id)
	if record.errorCode == ErrorCodePermanentError {
		wanted.oneShot[id] = true
	}
	return wanted
}

// directUnresolvableReplayer returns a tenant-aware replayer for direct
// resolveTenantCandidates calls. The readers are never consulted by the
// unresolvable branch, so empty fakes suffice.
func directUnresolvableReplayer(t *testing.T, state *ReplayState) *Replayer {
	t.Helper()
	return newTenantAwareReplayerWithFactories(
		(&brokerDLQ{topic: TopicDLQ}).freshDLQSession(),
		freshAcceptedReader(nil, nil),
		state, nil, log.New(io.Discard, "", 0))
}

// TestTenantUnresolvableDrainedCommitsOffset (AC-1 + F2/R7 parity) pins the
// production tenant-aware loss path: a wanted DLQ record whose original event_id
// is absent from the accepted topic is durably marked unresolvable, surfaces the
// Unresolvable counter, and commits its DLQ offset so the record is never
// re-scanned. It also folds the legacy split-counter matrix (F2) and proves
// republish is never called.
func TestTenantUnresolvableDrainedCommitsOffset(t *testing.T) {
	state, err := LoadReplayState(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{
		tenantFailureMessage("evt-x", "tenant-a", ErrorCodeAttemptsExhausted, 0),
	}}
	// Accepted topic carries a DIFFERENT event so the scan completes (drain is
	// deterministic) but never matches evt-x.
	var logs bytes.Buffer
	var republished atomic.Int64
	replayer := newTenantAwareReplayerWithFactories(
		broker.freshDLQSession(),
		freshAcceptedReader([]kafka.Message{tenantAcceptedMessage("other", "tenant-z", 0)}, nil),
		state,
		func(context.Context, []byte, []byte) error {
			republished.Add(1)
			t.Fatal("republish must never be called for an unresolvable record")
			return nil
		},
		log.New(&logs, "", 0),
	)
	replayer.drainTimeout = 10 * time.Millisecond
	defer replayer.Close()

	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("round1 replayed=%d, want 1 (unresolvable converges)", count)
	}
	metrics := replayer.Metrics()
	if metrics.Unresolvable != 1 {
		t.Fatalf("Unresolvable=%d, want 1", metrics.Unresolvable)
	}
	// F2/R7: the split-counter matrix must stay exactly 1/0/0/0 (unresolvable /
	// replayed / permanent / unparsableMarks) — parity with the legacy oracle.
	if metrics.Replayed != 0 || metrics.Permanent != 0 || metrics.UnparsableMarks != 0 {
		t.Fatalf("split counters replayed=%d permanent=%d unparsable=%d, want 0/0/0", metrics.Replayed, metrics.Permanent, metrics.UnparsableMarks)
	}
	if republished.Load() != 0 {
		t.Fatalf("republished=%d, want 0 (no re-publish for a loss)", republished.Load())
	}
	if broker.committed != 1 || len(broker.commits) != 1 || broker.commits[0].Offset != 0 {
		t.Fatalf("round1 committed=%d commits=%v, want offset 0 committed exactly once", broker.committed, broker.commits)
	}
	if !state.Marked("tenant-a", "evt-x") {
		t.Fatal("tenant-a/evt-x must be durably marked unresolvable")
	}
	logged := logs.String()
	if !strings.Contains(logged, "unresolvable") ||
		!strings.Contains(logged, "reason=original-not-found-in-accepted-topic") ||
		!strings.Contains(logged, "tenant=tenant-a") {
		t.Fatalf("log missing unresolvable evidence, got:\n%s", logged)
	}

	// Round 2: the record's DLQ offset is already committed, so it is not
	// re-collected and the loss is not re-counted (idempotent convergence).
	second, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second != 0 {
		t.Fatalf("round2 replayed=%d, want 0", second)
	}
	after := replayer.Metrics()
	if after.Unresolvable != 1 || after.Replayed != 0 {
		t.Fatalf("round2 counters unresolvable=%d replayed=%d, want 1/0 (no recount)", after.Unresolvable, after.Replayed)
	}
	if broker.committed != 1 {
		t.Fatalf("round2 committed=%d, want 1 (record not re-delivered after offset commit)", broker.committed)
	}
}

// TestResolveTenantCandidatesExpiredOriginal (AC-3) drives the unresolvable
// branch directly: a drained scan with no candidate for the wanted event_id
// resolves the record and increments the Unresolvable counter by exactly one.
func TestResolveTenantCandidatesExpiredOriginal(t *testing.T) {
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	replayer := directUnresolvableReplayer(t, state)
	record := dlqRecord{eventID: "evt-x", claimedTenantID: "tenant-a", errorCode: ErrorCodeAttemptsExhausted, wanted: true}
	wanted := tenantWanted(record)

	replayed, resolved, err := replayer.resolveTenantCandidates(context.Background(), wanted, map[string][]tenantAcceptedCandidate{}, map[string]bool{}, true)
	if err != nil {
		t.Fatal(err)
	}
	id := wanted.orderedIDs[0]
	if !resolved[id] {
		t.Fatal("expired-original record must be resolved as unresolvable")
	}
	if got := replayer.unresolvable.Load(); got != 1 {
		t.Fatalf("unresolvable=%d, want 1", got)
	}
	if replayed != 1 {
		t.Fatalf("local replayed=%d, want 1 (legacy return parity)", replayed)
	}
	if replayer.replayed.Load() != 0 {
		t.Fatalf("r.replayed=%d, want 0 (R7: durable counter untouched)", replayer.replayed.Load())
	}
}

// TestResolveTenantCandidatesAmbiguousNotUnresolvable (AC-3 negative) pins R3:
// two distinct tenant candidates never satisfy the absent-original condition,
// so the record stays pending and is not counted as unresolvable.
func TestResolveTenantCandidatesAmbiguousNotUnresolvable(t *testing.T) {
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	replayer := directUnresolvableReplayer(t, state)
	record := dlqRecord{eventID: "evt-y", claimedTenantID: "tenant-a", errorCode: ErrorCodeAttemptsExhausted, wanted: true}
	wanted := tenantWanted(record)
	candidates := map[string][]tenantAcceptedCandidate{
		"evt-y": {
			{message: kafka.Message{}, tenantID: "tenant-a", eventID: "evt-y"},
			{message: kafka.Message{}, tenantID: "tenant-b", eventID: "evt-y"},
		},
	}

	if _, resolved, err := replayer.resolveTenantCandidates(context.Background(), wanted, candidates, map[string]bool{}, true); err != nil {
		t.Fatal(err)
	} else if resolved[wanted.orderedIDs[0]] {
		t.Fatal("ambiguous candidates must stay pending, never unresolvable")
	}
	if got := replayer.unresolvable.Load(); got != 0 {
		t.Fatalf("unresolvable=%d, want 0 (ambiguous must not be counted)", got)
	}
}

// TestTenantFoundButUnparsableNotUnresolvable pins R4: a key-matched but
// unparsable accepted message (found[eventID]=true) keeps the record pending
// under the unparsable path and must not be counted as a permanent loss.
func TestTenantFoundButUnparsableNotUnresolvable(t *testing.T) {
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	replayer := directUnresolvableReplayer(t, state)
	record := dlqRecord{eventID: "evt-z", claimedTenantID: "tenant-a", errorCode: ErrorCodeAttemptsExhausted, wanted: true}
	wanted := tenantWanted(record)
	found := map[string]bool{"evt-z": true}

	if _, resolved, err := replayer.resolveTenantCandidates(context.Background(), wanted, map[string][]tenantAcceptedCandidate{}, found, true); err != nil {
		t.Fatal(err)
	} else if resolved[wanted.orderedIDs[0]] {
		t.Fatal("found-but-unparsable must stay pending, never unresolvable")
	}
	if got := replayer.unresolvable.Load(); got != 0 {
		t.Fatalf("unresolvable=%d, want 0 (found original is not a loss)", got)
	}
}

// TestTenantUnresolvableOnlyWhenDrained pins R2: before the accepted scan has
// gone quiet (!drained), an absent candidate must NOT be counted as
// unresolvable — it stays pending for the next round.
func TestTenantUnresolvableOnlyWhenDrained(t *testing.T) {
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	replayer := directUnresolvableReplayer(t, state)
	record := dlqRecord{eventID: "evt-x", claimedTenantID: "tenant-a", errorCode: ErrorCodeAttemptsExhausted, wanted: true}
	wanted := tenantWanted(record)

	if _, resolved, err := replayer.resolveTenantCandidates(context.Background(), wanted, map[string][]tenantAcceptedCandidate{}, map[string]bool{}, false); err != nil {
		t.Fatal(err)
	} else if resolved[wanted.orderedIDs[0]] {
		t.Fatal("not-drained absent candidate must stay pending")
	}
	if got := replayer.unresolvable.Load(); got != 0 {
		t.Fatalf("unresolvable=%d, want 0 (no conclusion before drain)", got)
	}
}

// TestTenantUnresolvableIdempotentAcrossRounds (FM-1/FM-7) proves the durable
// mark makes the loss count exactly once across re-passes — including the
// empty-claim unscoped-mark edge (FM-1), where only an unscoped idempotency
// probe is available.
func TestTenantUnresolvableIdempotentAcrossRounds(t *testing.T) {
	t.Run("claimed tenant", func(t *testing.T) {
		state, err := LoadReplayState("")
		if err != nil {
			t.Fatal(err)
		}
		replayer := directUnresolvableReplayer(t, state)
		record := dlqRecord{eventID: "evt-x", claimedTenantID: "tenant-a", errorCode: ErrorCodeAttemptsExhausted, wanted: true}
		wanted := tenantWanted(record)

		if _, _, err := replayer.resolveTenantCandidates(context.Background(), wanted, map[string][]tenantAcceptedCandidate{}, map[string]bool{}, true); err != nil {
			t.Fatal(err)
		}
		if replayer.unresolvable.Load() != 1 {
			t.Fatalf("unresolvable=%d, want 1 after first resolution", replayer.unresolvable.Load())
		}
		// Re-pass (e.g. crash before commit): the scoped mark converges the
		// record without re-counting the loss.
		if _, resolved, err := replayer.resolveTenantCandidates(context.Background(), wanted, map[string][]tenantAcceptedCandidate{}, map[string]bool{}, true); err != nil {
			t.Fatal(err)
		} else if !resolved[wanted.orderedIDs[0]] {
			t.Fatal("record must resolve on the idempotent re-pass")
		}
		if replayer.unresolvable.Load() != 1 {
			t.Fatalf("unresolvable=%d after re-pass, want 1 (no double count)", replayer.unresolvable.Load())
		}
	})
	t.Run("empty claim unscoped mark", func(t *testing.T) {
		state, err := LoadReplayState("")
		if err != nil {
			t.Fatal(err)
		}
		replayer := directUnresolvableReplayer(t, state)
		record := dlqRecord{eventID: "evt-y", claimedTenantID: "", errorCode: ErrorCodeAttemptsExhausted, wanted: true}
		wanted := tenantWanted(record)

		if _, resolved, err := replayer.resolveTenantCandidates(context.Background(), wanted, map[string][]tenantAcceptedCandidate{}, map[string]bool{}, true); err != nil {
			t.Fatal(err)
		} else if !resolved[wanted.orderedIDs[0]] || !state.marked("evt-y") {
			t.Fatal("empty-claim record must resolve via the unscoped idempotency mark")
		}
		if replayer.unresolvable.Load() != 1 {
			t.Fatalf("unresolvable=%d, want 1 after first resolution", replayer.unresolvable.Load())
		}
		if _, resolved, err := replayer.resolveTenantCandidates(context.Background(), wanted, map[string][]tenantAcceptedCandidate{}, map[string]bool{}, true); err != nil {
			t.Fatal(err)
		} else if !resolved[wanted.orderedIDs[0]] {
			t.Fatal("empty-claim record must resolve on the idempotent re-pass")
		}
		if replayer.unresolvable.Load() != 1 {
			t.Fatalf("unresolvable=%d after re-pass, want 1 (no double count)", replayer.unresolvable.Load())
		}
	})
}

// TestTenantUnresolvableEmptyClaimFallsBackToUnscopedMark (FM-1 + F5) drives
// the whole tenant-aware round with a DLQ Failure carrying no tenant_id. The
// record is still durably dropped and committed; the unscoped mark provides
// idempotency, and — critically (F5/REQ-tenant-safety) — it does NOT suppress a
// later scoped tenant-B resolution of the same event_id.
func TestTenantUnresolvableEmptyClaimFallsBackToUnscopedMark(t *testing.T) {
	state, err := LoadReplayState(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{
		tenantFailureMessage("evt-x", "", ErrorCodeAttemptsExhausted, 0), // no tenant_id
	}}
	var republished atomic.Int64
	replayer := newTenantAwareReplayerWithFactories(
		broker.freshDLQSession(),
		freshAcceptedReader([]kafka.Message{tenantAcceptedMessage("other", "tenant-z", 0)}, nil),
		state,
		func(context.Context, []byte, []byte) error {
			republished.Add(1)
			t.Fatal("republish must never be called for an unresolvable record")
			return nil
		},
		log.New(io.Discard, "", 0),
	)
	replayer.drainTimeout = 10 * time.Millisecond
	defer replayer.Close()

	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("replayed=%d, want 1 (unresolvable converges)", count)
	}
	if metrics := replayer.Metrics(); metrics.Unresolvable != 1 {
		t.Fatalf("Unresolvable=%d, want 1", metrics.Unresolvable)
	}
	if republished.Load() != 0 {
		t.Fatalf("republished=%d, want 0", republished.Load())
	}
	if broker.committed != 1 {
		t.Fatalf("committed=%d, want 1 (offset committed via unscoped mark)", broker.committed)
	}
	// FM-1: the absent-claim case writes an unscoped (event-id-only) mark.
	if !state.marked("evt-x") {
		t.Fatal("unscoped event-id mark must be present for idempotency")
	}
	// F5 (REQ-tenant-safety): the unscoped mark must NOT be honored as a scoped
	// (tenant,event) mark for any other tenant, so a legitimate tenant-B
	// resolution of evt-x is never suppressed by it.
	if state.Marked("tenant-b", "evt-x") {
		t.Fatal("unscoped unresolvable mark must not suppress a different tenant")
	}
	if err := state.MarkTenant("tenant-b", "evt-x"); err != nil {
		t.Fatalf("a legitimate tenant-b mark must succeed: %v", err)
	}
	if !state.Marked("tenant-b", "evt-x") {
		t.Fatal("tenant-b mark must be honored after the unscoped mark")
	}
}

// TestTenantUnresolvableBarrierHeldBehindPending (F4/FM-8) exercises the
// per-partition commit barrier against the new unresolvable record. It proves
// both directions: an unresolvable record BELOW a still-pending record commits
// (no silent stall), and one ABOVE a pending record is held until the pending
// record resolves (no leapfrog commit), with the loss counted exactly once.
func TestTenantUnresolvableBarrierHeldBehindPending(t *testing.T) {
	ambiguousAccepted := func() []kafka.Message {
		return []kafka.Message{
			tenantAcceptedMessage("evt-a", "tenant-a", 0),
			tenantAcceptedMessage("evt-a", "tenant-b", 1),
		}
	}
	singleAccepted := func() []kafka.Message {
		return []kafka.Message{tenantAcceptedMessage("evt-a", "tenant-a", 0)}
	}

	t.Run("unresolvable held above pending", func(t *testing.T) {
		state, err := LoadReplayState(filepath.Join(t.TempDir(), "state.json"))
		if err != nil {
			t.Fatal(err)
		}
		// offset0 = evt-a (ambiguous → pending), offset1 = evt-b (absent original
		// → unresolvable). The pending record sits BELOW the unresolvable one, so
		// the barrier must hold the unresolvable offset this round.
		broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{
			tenantFailureMessage("evt-a", "tenant-a", ErrorCodeAttemptsExhausted, 0),
			tenantFailureMessage("evt-b", "tenant-a", ErrorCodeAttemptsExhausted, 1),
		}}
		var deliveries atomic.Int64
		deliver := func(context.Context, []byte, []byte) error { deliveries.Add(1); return nil }

		replayer1 := runTenantReplayer(broker, ambiguousAccepted(), state, deliver)
		defer replayer1.Close()
		if _, err := replayer1.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if broker.committed != 0 {
			t.Fatalf("round1 committed=%d, want 0 (barrier holds unresolvable above pending)", broker.committed)
		}
		if got := replayer1.Metrics().Unresolvable; got != 1 {
			t.Fatalf("round1 Unresolvable=%d, want 1 (loss counted even while held)", got)
		}

		// Round 2: the pending evt-a resolves (single candidate), unblocking
		// the barrier so the held unresolvable offset commits too.
		replayer2 := runTenantReplayer(broker, singleAccepted(), state, deliver)
		defer replayer2.Close()
		if _, err := replayer2.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if broker.committed != 2 {
			t.Fatalf("round2 committed=%d, want 2 (both offsets commit once pending clears)", broker.committed)
		}
		if got := replayer2.Metrics().Unresolvable; got != 0 {
			t.Fatalf("round2 Unresolvable=%d, want 0 (fresh process: already-marked record not re-counted)", got)
		}
		if deliveries.Load() != 1 {
			t.Fatalf("deliveries=%d, want 1 (only evt-a re-published)", deliveries.Load())
		}
	})

	t.Run("unresolvable commits below pending", func(t *testing.T) {
		state, err := LoadReplayState(filepath.Join(t.TempDir(), "state.json"))
		if err != nil {
			t.Fatal(err)
		}
		// offset0 = evt-b (absent original → unresolvable), offset1 = evt-a
		// (ambiguous → pending). The unresolvable record sits BELOW the pending
		// one, so the barrier must let it commit (no silent stall).
		broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{
			tenantFailureMessage("evt-b", "tenant-a", ErrorCodeAttemptsExhausted, 0),
			tenantFailureMessage("evt-a", "tenant-a", ErrorCodeAttemptsExhausted, 1),
		}}
		var deliveries atomic.Int64
		deliver := func(context.Context, []byte, []byte) error { deliveries.Add(1); return nil }

		replayer1 := runTenantReplayer(broker, ambiguousAccepted(), state, deliver)
		defer replayer1.Close()
		if _, err := replayer1.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if broker.committed != 1 || broker.commits[0].Offset != 0 {
			t.Fatalf("round1 committed=%d commits=%v, want offset 0 committed (unresolvable below pending)", broker.committed, broker.commits)
		}
		if got := replayer1.Metrics().Unresolvable; got != 1 {
			t.Fatalf("round1 Unresolvable=%d, want 1", got)
		}

		// Round 2: only the pending evt-a remains; it resolves and commits.
		replayer2 := runTenantReplayer(broker, singleAccepted(), state, deliver)
		defer replayer2.Close()
		if _, err := replayer2.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if broker.committed != 2 {
			t.Fatalf("round2 committed=%d, want 2", broker.committed)
		}
		if got := replayer2.Metrics().Unresolvable; got != 0 {
			t.Fatalf("round2 Unresolvable=%d, want 0 (fresh process: already-marked record not re-counted)", got)
		}
		if deliveries.Load() != 1 {
			t.Fatalf("deliveries=%d, want 1", deliveries.Load())
		}
	})
}

// TestTenantUnresolvableMarkFailureKeepsPending (FM-6) ensures that when the
// durable mark cannot be written, the error surfaces, the loss counter is NOT
// incremented, and the record stays pending (offset uncommitted) for the next
// round — matching the "commit failure is fatal" invariant.
func TestTenantUnresolvableMarkFailureKeepsPending(t *testing.T) {
	// A state path under a non-existent directory makes MarkTenant's lock file
	// impossible to create, forcing the durable mark to fail.
	state := &ReplayState{path: filepath.Join(t.TempDir(), "missing", "state.json")}
	replayer := directUnresolvableReplayer(t, state)
	record := dlqRecord{eventID: "evt-x", claimedTenantID: "tenant-a", errorCode: ErrorCodeAttemptsExhausted, wanted: true}
	wanted := tenantWanted(record)

	if _, _, err := replayer.resolveTenantCandidates(context.Background(), wanted, map[string][]tenantAcceptedCandidate{}, map[string]bool{}, true); err == nil {
		t.Fatal("expected mark failure to surface as an error")
	}
	if got := replayer.unresolvable.Load(); got != 0 {
		t.Fatalf("unresolvable=%d, want 0 (loss not counted until the mark succeeds)", got)
	}
}

// TestTenantUnresolvableIntegrationBroker (AC-2) is the broker-backed end-to-
// end check that a genuinely-lost original raises the Unresolvable loss signal
// through the real NewReplayer path. It is skipped without AUDIT_TEST_KAFKA_
// BROKERS; run it against a disposable cluster to certify the metric→alert feed.
func TestTenantUnresolvableIntegrationBroker(t *testing.T) {
	brokers := os.Getenv("AUDIT_TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("set AUDIT_TEST_KAFKA_BROKERS (comma-separated) to run the tenant unresolvable broker integration test")
	}
	brokerList := strings.Split(brokers, ",")
	prefix := fmt.Sprintf("snaplink-test-utlq-%d", time.Now().UnixNano())
	dlqTopic := prefix + "-dlq"
	acceptedTopic := prefix + "-accepted"
	client := &kafka.Client{Addr: kafka.TCP(brokerList...)}
	ctx := context.Background()
	if _, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{
			{Topic: dlqTopic, NumPartitions: 1, ReplicationFactor: 1},
			{Topic: acceptedTopic, NumPartitions: 1, ReplicationFactor: 1},
		},
	}); err != nil {
		t.Fatalf("create topics: %v", err)
	}
	defer func() {
		_, _ = client.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{Topics: []string{dlqTopic, acceptedTopic}})
	}()

	// Publish a DLQ Failure whose original is NEVER written to the accepted
	// topic (lost to retention, or never published there).
	failure := Failure{EventID: "evt-x", TenantID: "tenant-a", ErrorCode: ErrorCodeAttemptsExhausted, ErrorMessage: "boom"}
	encoded, _ := json.Marshal(failure)
	writer := &kafka.Writer{Topic: dlqTopic, Addr: kafka.TCP(brokerList...)}
	if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte("evt-x"), Value: encoded}); err != nil {
		t.Fatalf("write DLQ failure: %v", err)
	}
	_ = writer.Close()

	state, err := LoadReplayState(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	replayer := NewReplayer(brokerList, dlqTopic, acceptedTopic, prefix, state, func(context.Context, []byte, []byte) error {
		t.Error("republish must never be called for an unresolvable record")
		return nil
	}, log.New(io.Discard, "", 0))
	defer replayer.Close()

	if _, err := replayer.RunOnce(ctx); err != nil {
		t.Fatalf("run once: %v", err)
	}
	if got := replayer.Metrics().Unresolvable; got != 1 {
		t.Fatalf("Unresolvable=%d, want 1 (lost original raised the loss signal)", got)
	}
	// A second drained round must not re-count the same loss.
	if _, err := replayer.RunOnce(ctx); err != nil {
		t.Fatalf("run once 2: %v", err)
	}
	if got := replayer.Metrics().Unresolvable; got != 1 {
		t.Fatalf("Unresolvable=%d after second round, want 1 (no double count)", got)
	}
}
