package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/snaplink/audit-governance/internal/outbox"
)

// Shared dimensions for the AC-1/AC-4 concurrency tests: W writers × K
// distinct IDs plus a shared overlapping core, all on one state path.
const (
	replayConcurrentWriters   = 8
	replayConcurrentPerWriter = 50
	replayConcurrentShared    = 10
)

// fakeReplayReader is a messageReader whose FetchMessage returns queued
// messages and then blocks until the context expires (drain window), exactly
// like the real kafka-go reader at the end of a topic.
type fakeReplayReader struct {
	messages []kafka.Message
	index    int
	commits  []kafka.Message
	topic    string
	// delay makes each queued message available only after delay, simulating
	// sustained ingest whose arrival rate is below the drain window.
	delay time.Duration
	// fetchErr is returned once the queue is exhausted instead of blocking,
	// simulating a transport failure mid-scan.
	fetchErr error
	// fetchLog, when non-nil, records every fetched message offset (T2 round
	// re-fetch proof).
	fetchLog *[]int64
	// closeErr, when non-nil, is returned by Close (F4: reset abort path).
	closeErr error
}

func (f *fakeReplayReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if f.index < len(f.messages) {
		message := f.messages[f.index]
		f.index++
		if f.fetchLog != nil {
			*f.fetchLog = append(*f.fetchLog, message.Offset)
		}
		if f.delay > 0 {
			select {
			case <-time.After(f.delay):
			case <-ctx.Done():
				return kafka.Message{}, ctx.Err()
			}
		}
		return message, nil
	}
	if f.fetchErr != nil {
		return kafka.Message{}, f.fetchErr
	}
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (f *fakeReplayReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	f.commits = append(f.commits, msgs...)
	return nil
}

func (f *fakeReplayReader) Config() kafka.ReaderConfig { return kafka.ReaderConfig{Topic: f.topic} }
func (f *fakeReplayReader) Close() error               { return f.closeErr }

// freshAcceptedReader returns a factory that hands out a NEW fake reader per
// call with the same queue, modeling the production per-round accepted-reader
// recreation: a fresh reader starts at the first retained offset (index 0)
// even though the previous round's reader consumed the whole queue. fetchLog,
// when non-nil, is shared across the readers so a test can prove round N+1
// re-fetched offsets round N already consumed.
func freshAcceptedReader(messages []kafka.Message, fetchLog *[]int64) func() messageReader {
	return func() messageReader {
		return &fakeReplayReader{topic: TopicAccepted, messages: messages, fetchLog: fetchLog}
	}
}

// brokerDLQ models the broker-side state Kafka keeps for a DLQ partition: ONE
// committed offset, advanced to max(committed, offset+1) per commit, shared
// by every reader session. A fresh session only delivers messages at or
// above the committed offset — so a record committed past a pending record
// is never re-delivered (the exact mid-batch loss the commit barrier must
// prevent).
type brokerDLQ struct {
	topic     string
	messages  []kafka.Message // ascending offsets within one partition
	committed int64           // broker-side committed offset (starts at 0)
	commits   []kafka.Message
}

// brokerReplayReader is one DLQ reader session against a brokerDLQ. The
// session position (index) starts at the committed offset and never rewinds;
// recreating the reader (a fresh session) is the only way a later round sees
// records that were fetched-but-uncommitted by an earlier session — the F1
// per-round DLQ reader recreation the production code performs.
type brokerReplayReader struct {
	broker *brokerDLQ
	index  int
}

func (b *brokerReplayReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	for b.index < len(b.broker.messages) {
		message := b.broker.messages[b.index]
		b.index++
		if message.Offset < b.broker.committed {
			continue // already committed: a fresh session would not deliver it
		}
		return message, nil
	}
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (b *brokerReplayReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	b.broker.commits = append(b.broker.commits, msgs...)
	for _, m := range msgs {
		if m.Offset+1 > b.broker.committed {
			b.broker.committed = m.Offset + 1
		}
	}
	return nil
}

func (b *brokerReplayReader) Config() kafka.ReaderConfig {
	return kafka.ReaderConfig{Topic: b.broker.topic}
}
func (b *brokerReplayReader) Close() error { return nil }

// freshDLQSession returns a factory handing out a NEW DLQ reader session per
// round against the same broker state (the committed offset persists across
// sessions, exactly like a real broker), modeling the F1 per-round DLQ
// reader recreation: each round drains from the committed offset, so pending
// records are re-delivered and converged records are not.
func (b *brokerDLQ) freshDLQSession() func() messageReader {
	return func() messageReader { return &brokerReplayReader{broker: b} }
}

func dlqMessage(eventID string) kafka.Message {
	return dlqMessageAt(eventID, 1)
}

// dlqMessageAt builds a dead-letter Failure record at an explicit offset for
// broker-semantics tests: offsets are unique per partition, so tests that
// mix pending and resolved records in one partition must use distinct
// offsets.
func dlqMessageAt(eventID string, offset int64) kafka.Message {
	failure := Failure{EventID: eventID, ErrorCode: ErrorCodeAttemptsExhausted, ErrorMessage: "boom"}
	encoded, _ := json.Marshal(failure)
	return kafka.Message{Key: []byte(eventID), Value: encoded, Partition: 0, Offset: offset}
}

func acceptedMessage(eventID string) kafka.Message {
	value := []byte(`{"event_id":"` + eventID + `","source_system":"crm"}`)
	return kafka.Message{Key: []byte(eventID), Value: value, Partition: 0, Offset: 1}
}

// TestReplayOnceRecoversAndRepublishes pins the happy path: a DLQ failure
// for event X is matched against the accepted topic and the original message
// is re-published byte-for-byte exactly once.
func TestReplayOnceRecoversAndRepublishes(t *testing.T) {
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-a"), dlqMessage("evt-b")}}
	accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{acceptedMessage("evt-a"), acceptedMessage("other"), acceptedMessage("evt-b")}}
	state, err := LoadReplayState(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var republished []kafka.Message
	replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
		republished = append(republished, kafka.Message{Key: key, Value: value})
		return nil
	})
	replayer.drainTimeout = 20 * time.Millisecond
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("replayed=%d, want 2", count)
	}
	if len(republished) != 2 {
		t.Fatalf("republished=%d, want 2", len(republished))
	}
	if got := string(republished[0].Key); got != "evt-a" {
		t.Fatalf("first republish key=%s, want evt-a", got)
	}
	// The un-wanted message was not re-published, and both replayed IDs are
	// persisted so a second round is a no-op.
	if _, ok := state.Replayed["other"]; ok {
		t.Fatal("un-wanted message marked replayed")
	}
	if !state.Replayed["evt-a"] || !state.Replayed["evt-b"] {
		t.Fatal("replayed IDs not persisted")
	}
	dlq.index = 0 // second round: the DLQ is drained (no new records)
	second, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second != 0 || len(republished) != 2 {
		t.Fatalf("second round replayed=%d republished=%d, want 0/2 (idempotent)", second, len(republished))
	}
}

// TestReplayPersistsStateAcrossRestarts pins recovery: a new Replayer with a
// re-loaded state file skips already-replayed events.
func TestReplayPersistsStateAcrossRestarts(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	state, err := LoadReplayState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Mark("evt-done"); err != nil {
		t.Fatal(err)
	}
	reloaded, err := LoadReplayState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if !reloaded.Replayed["evt-done"] {
		t.Fatal("replayed set lost across restart")
	}
}

// AC-1 (REQ-4): the rewrite path (first write / legacy conversion) fsyncs
// the unique temp contents BEFORE the rename and the parent directory AFTER
// the rename; the append path (steady-state log) fsyncs the state file after
// the append — no rename, no dir sync. The recorded-hook proof is
// deterministic (no privileges, no power-loss simulation): the syncFile hook
// must observe the file existing on disk, the syncDir hook must observe the
// TARGET file existing and decoding to the just-marked event (so the rename
// provably ran between the two hooks), and the recorded sequence is exactly
// [file:<unique temp>, dir:<parent>] on the rewrite path and [file:<state>]
// on the append path. Fails pre-fix: pre-fix Mark used the deterministic
// <state>.tmp name and performed zero dir syncs.
func TestReplayStateMarkSyncsFileBeforeRenameAndDirAfterRename(t *testing.T) {
	t.Run("first write: content fsync before rename, dir fsync after", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		var order []string
		state.syncFile = func(p string) error {
			if p == path || p == path+".tmp" {
				t.Errorf("syncFile path=%s: the rewrite path must sync the UNIQUE temp, never the target or the deterministic name", p)
			}
			if _, err := os.Stat(p); err != nil {
				t.Errorf("syncFile must observe the temp file existing: %v", err)
			}
			order = append(order, "file:"+filepath.Base(p))
			return nil
		}
		state.syncDir = func(p string) error {
			if p != filepath.Dir(path) {
				t.Errorf("syncDir path=%s, want parent %s", p, filepath.Dir(path))
			}
			disk, err := LoadReplayState(path)
			if err != nil {
				t.Errorf("syncDir must observe a decodable target: %v", err)
			}
			if err == nil && !disk.Replayed["evt-1"] {
				t.Error("syncDir must observe the target holding the just-marked event")
			}
			order = append(order, "dir:"+filepath.Base(p))
			return nil
		}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		if len(order) != 2 || !strings.HasPrefix(order[0], "file:") || !strings.HasPrefix(order[1], "dir:") {
			t.Fatalf("sync order = %v, want exactly [file:<unique temp>, dir:<parent>] (file fsync before rename, dir fsync after)", order)
		}
		if order[0] == "file:state.json" || order[0] == "file:state.json.tmp" {
			t.Fatalf("sync order = %v: rewrite path must sync the unique temp, never the target or the deterministic name", order)
		}
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if !reloaded.Replayed["evt-1"] {
			t.Fatal("marked event lost after reload")
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".tmp") {
				t.Fatalf(".tmp left behind after successful Mark: %s", entry.Name())
			}
		}
	})
	t.Run("append path: file fsync after append, no rename, no dir sync", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		var order []string
		state.syncFile = func(p string) error {
			if p != path {
				t.Errorf("syncFile path=%s, want the state file %s (append path fsyncs the target, no temp)", p, path)
			}
			if _, err := os.Stat(p); err != nil {
				t.Errorf("syncFile must observe the state file existing: %v", err)
			}
			order = append(order, "file:"+filepath.Base(p))
			return nil
		}
		state.syncDir = func(p string) error {
			t.Errorf("append path must never sync a directory, got %s", p)
			return nil
		}
		if err := state.Mark("evt-2"); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(order, []string{"file:state.json"}) {
			t.Fatalf("sync order = %v, want exactly [file:state.json] (append → file fsync; no rename, no dir sync)", order)
		}
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if !reloaded.Replayed["evt-1"] || !reloaded.Replayed["evt-2"] {
			t.Fatalf("both marks must survive the append path: %v", reloaded.Replayed)
		}
	})
	t.Run("production defaults", func(t *testing.T) {
		// Nil hooks resolve to the real fsutil.SyncFile/fsutil.SyncDir on a
		// real tempdir (mirrors TestSyncDirChainNilSyncDir).
		realPath := filepath.Join(t.TempDir(), "state.json")
		realState, err := LoadReplayState(realPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := realState.Mark("evt-2"); err != nil {
			t.Fatalf("Mark with default hooks: %v", err)
		}
		reloaded, err := LoadReplayState(realPath)
		if err != nil {
			t.Fatal(err)
		}
		if !reloaded.Replayed["evt-2"] {
			t.Fatal("marked event lost with default hooks")
		}
	})
}

// AC-2 (REQ-5): LoadReplayState tolerates every crash leftover a crashed
// Mark can leave. A stale .tmp is ignored — never read, parsed, or deleted —
// alongside a valid state file; a zero-length or whitespace-only target is
// an empty state; a missing target is an empty state.
func TestLoadReplayStateToleratesCrashLeftovers(t *testing.T) {
	t.Run("stale garbage tmp alongside valid state", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte(`{"replayed":{"evt-a":true}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path+".tmp", []byte("\x00garbage-not-json{"), 0o600); err != nil {
			t.Fatal(err)
		}
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("stale .tmp must not fail the load: %v", err)
		}
		if !state.Replayed["evt-a"] {
			t.Fatalf("valid state lost alongside stale .tmp: %v", state.Replayed)
		}
		raw, err := os.ReadFile(path + ".tmp")
		if err != nil {
			t.Fatalf("stale .tmp must not be deleted: %v", err)
		}
		if string(raw) != "\x00garbage-not-json{" {
			t.Fatalf("stale .tmp content modified: %q", raw)
		}
	})
	t.Run("stale unique temp and lock file alongside a log state", func(t *testing.T) {
		// The new write path leaves unique-named temps (crash residue) and a
		// sibling .lock file; neither may be read, parsed, or deleted by the
		// loader (REQ-6(c), FM-12).
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte("{\"format\":\"replay-log-v1\"}\n{\"event_id\":\"evt-a\"}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path+".lock", nil, 0o600); err != nil {
			t.Fatal(err)
		}
		stale := filepath.Join(filepath.Dir(path), "state.json-999999.tmp")
		if err := os.WriteFile(stale, []byte("\x00partial-write-garbage"), 0o600); err != nil {
			t.Fatal(err)
		}
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("stale temp/lock must not fail the load: %v", err)
		}
		if !state.Replayed["evt-a"] {
			t.Fatalf("log state lost alongside stale artifacts: %v", state.Replayed)
		}
		if _, err := os.Stat(stale); err != nil {
			t.Fatalf("stale unique temp must not be deleted: %v", err)
		}
		if _, err := os.Stat(path + ".lock"); err != nil {
			t.Fatalf("lock file must not be touched: %v", err)
		}
	})
	t.Run("zero-length target is empty state", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("zero-length target must load: %v", err)
		}
		if len(state.Replayed) != 0 {
			t.Fatalf("zero-length target must be an empty map, got %v", state.Replayed)
		}
	})
	t.Run("whitespace-only target is empty state", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte(" \n\t"), 0o600); err != nil {
			t.Fatal(err)
		}
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("whitespace target must load: %v", err)
		}
		if len(state.Replayed) != 0 {
			t.Fatalf("whitespace target must be an empty map, got %v", state.Replayed)
		}
	})
	t.Run("missing target is empty state", func(t *testing.T) {
		state, err := LoadReplayState(filepath.Join(t.TempDir(), "absent.json"))
		if err != nil {
			t.Fatalf("missing target must load: %v", err)
		}
		if len(state.Replayed) != 0 {
			t.Fatalf("missing target must be an empty map, got %v", state.Replayed)
		}
	})
}

// AC-3 (REQ-4/REQ-6(c) crash model): the deterministic replay model shows
// every crash position of the log write paths leaves a DECODABLE state file:
//   - append + file fsync complete → the log holds {evt-1, evt-2};
//   - crash between append and file fsync → the append may revert → the
//     target holds {evt-1} (last durable content), still decodable;
//   - crash before the first write's temp fsync → the write never happened
//     → target absent, or zeroed by a power failure → empty state.
//
// All decode with nil error (boot proceeds; the next round re-scans —
// at-least-once, tolerated). The non-vacuous control: a torn byte prefix —
// the pre-fix F1 state — is a decode error, proving the model can produce an
// undecodable file and that the fsync ordering is what prevents it. A torn
// TRAILING log line (crash mid-append) is skipped, never a mark, never an
// error (REQ-6(c)).
func TestReplayStateMarkSurvivesSimulatedCrash(t *testing.T) {
	t.Run("fully durable marks decode after every step", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		// Real fsutil.SyncFile/SyncDir defaults: the full protocol runs.
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		if err := state.Mark("evt-2"); err != nil {
			t.Fatal(err)
		}
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("fully durable target must decode: %v", err)
		}
		if !reloaded.Replayed["evt-1"] || !reloaded.Replayed["evt-2"] {
			t.Fatalf("both marks must survive the crash: %v", reloaded.Replayed)
		}
	})
	t.Run("crash between append and file sync may revert the append", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		// Mark evt-1 durably (real syncs) and capture its bytes: those are
		// what the target reverts to when evt-2's append is not durable.
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		durableAfterEvt1, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		// The second mark's file fsync is dropped (crash cuts the process
		// between the append write and the fsync): Mark reports the failure
		// (FM-3) while the append's bytes are already in the file.
		state.syncFile = func(string) error { return errors.New("injected file sync failure") }
		if err := state.Mark("evt-2"); err == nil {
			t.Fatal("Mark must fail when the file sync fails")
		}
		if state.Replayed["evt-2"] {
			t.Fatal("in-memory mark must not be set on persist failure (security F1)")
		}
		// Outcome A — the unsynced append reverts on power failure: the
		// target is the last durable content {evt-1}. Still decodable.
		if err := os.WriteFile(path, durableAfterEvt1, 0o600); err != nil {
			t.Fatal(err)
		}
		reverted, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("reverted target must decode with nil error: %v", err)
		}
		if !reverted.Replayed["evt-1"] || reverted.Replayed["evt-2"] {
			t.Fatalf("reverted target must hold only evt-1: %v", reverted.Replayed)
		}
		// Outcome B — the append survived: the target holds {evt-1, evt-2}
		// and also decodes with nil error (both states decode).
		state.syncFile = nil
		if err := state.Mark("evt-2"); err != nil {
			t.Fatal(err)
		}
		survived, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("survived-append target must decode with nil error: %v", err)
		}
		if !survived.Replayed["evt-1"] || !survived.Replayed["evt-2"] {
			t.Fatalf("survived-append target must hold both marks: %v", survived.Replayed)
		}
	})
	t.Run("crash before the first write's temp sync loses the write", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		// The first write's temp-file fsync is dropped: Mark fails (FM-4),
		// the temp is removed, and no target was ever created.
		state.syncFile = func(string) error { return errors.New("injected file sync failure") }
		if err := state.Mark("evt-1"); err == nil {
			t.Fatal("Mark must fail when the first write's temp sync fails")
		}
		if state.Replayed["evt-1"] {
			t.Fatal("in-memory mark must not be set on persist failure (security F1)")
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("target must not exist after a failed first write: %v", err)
		}
		missing, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("absent target must decode with nil error: %v", err)
		}
		if len(missing.Replayed) != 0 {
			t.Fatalf("absent target must be an empty map, got %v", missing.Replayed)
		}
		// Crash outcome — the un-flushed contents are lost entirely and the
		// target comes back zeroed: an empty state, NOT a decode error
		// (boot proceeds; the next round re-scans — at-least-once).
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		zeroed, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("zeroed target must decode with nil error: %v", err)
		}
		if len(zeroed.Replayed) != 0 {
			t.Fatalf("zeroed target must be an empty map, got %v", zeroed.Replayed)
		}
	})
	t.Run("torn byte prefix is a decode error (non-vacuous)", func(t *testing.T) {
		// The pre-fix F1 state: an un-fsynced write can leave a torn byte
		// prefix after power loss. This is the ONLY crash outcome that fails
		// to decode, and it is exactly what the fsync-before-rename ordering
		// removes. The sub-check proves the model is non-vacuous: not every
		// crash outcome decodes, so the assertions above are meaningful.
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte(`{"replayed":{"evt-1":tru`), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadReplayState(path); err == nil {
			t.Fatal("torn target must be a decode error (fail-loud, pinned by REQ-5)")
		}
	})
	t.Run("torn trailing log line is skipped, never a mark or error", func(t *testing.T) {
		// A crash mid-append leaves a partial trailing line: the loader
		// skips it (REQ-6(c)) — never a mark, never a decode failure — and
		// the next Mark repairs the tail before appending.
		path := filepath.Join(t.TempDir(), "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte(`{"event_id":"evt-2`)); err != nil {
			file.Close()
			t.Fatal(err)
		}
		file.Close()
		loaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("torn trailing line must not fail the load: %v", err)
		}
		if !loaded.Replayed["evt-1"] || loaded.Replayed["evt-2"] {
			t.Fatalf("torn trailing line must be skipped, never a mark: %v", loaded.Replayed)
		}
	})
}

// REQ-3/REQ-7 (FM-1…FM-7): every Mark failure path removes only its own
// temp file best-effort and preserves the authoritative state — the target
// stays untouched when the failure precedes the rename (rewrite-path file
// sync failure, write failure), KEEPS the new content when the failure
// follows it (dir-sync failure, D4: both old and new content decode), and
// decodes as a SUPERSET when the failure is a post-append file fsync (the
// append itself succeeded). Under the amended REQ-7 (security F1) the
// in-memory set is updated only after a successful persist, so a failed
// Mark never suppresses the record's retry. Deterministic: injected hook
// failures plus real-filesystem failure cases (ENOTDIR lock path, directory
// target) that need no privileges.
func TestReplayStateMarkErrorPathsCleanTempAndPreserveTarget(t *testing.T) {
	t.Run("legacy conversion file sync failure: temp removed, target untouched", func(t *testing.T) {
		// The failing Mark is a legacy→log conversion (a rewrite path): the
		// temp-file sync failure precedes the rename, so the target stays
		// byte-identical to the authoritative legacy file.
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte(`{"replayed":{"evt-1":true}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		state.syncFile = func(string) error { return errors.New("injected file sync failure") }
		if err := state.Mark("evt-2"); err == nil {
			t.Fatal("Mark must fail when the temp-file sync fails (FM-4)")
		}
		if state.Replayed["evt-2"] {
			t.Fatal("in-memory mark must not be set on persist failure (security F1)")
		}
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(after, before) {
			t.Fatalf("target changed after file-sync failure: %s", after)
		}
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if !reloaded.Replayed["evt-1"] || reloaded.Replayed["evt-2"] {
			t.Fatalf("target must hold the authoritative pre-failure state: %v", reloaded.Replayed)
		}
	})
	t.Run("append file sync failure: error, no temp, target decodes as a superset", func(t *testing.T) {
		// On the append path the line is written before the file fsync, so a
		// failed fsync leaves a decodable SUPERSET (the new REQ-7 invariant:
		// "target = previous lines + new line", replacing the old
		// byte-identical-target invariant). The in-memory set lacks the mark
		// (security F1), so the next round re-reads the DLQ record as wanted
		// and retries until the mark is durable — a DLQ offset is never
		// committed without the durable mark.
		path := filepath.Join(t.TempDir(), "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		state.syncFile = func(string) error { return errors.New("injected file sync failure") }
		if err := state.Mark("evt-2"); err == nil {
			t.Fatal("Mark must fail when the post-append file sync fails (FM-3)")
		}
		if state.Replayed["evt-2"] {
			t.Fatal("in-memory mark must not be set on persist failure (security F1)")
		}
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("superset target must decode: %v", err)
		}
		if !reloaded.Replayed["evt-1"] || !reloaded.Replayed["evt-2"] {
			t.Fatalf("superset target must hold both marks: %v", reloaded.Replayed)
		}
		entries, err := os.ReadDir(filepath.Dir(path))
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".tmp") {
				t.Fatalf(".tmp left behind after append failure: %s", entry.Name())
			}
		}
	})
	t.Run("dir sync failure after legacy conversion: target keeps the new content", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte(`{"replayed":{"evt-1":true}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		state.syncDir = func(string) error { return errors.New("injected dir sync failure") }
		if err := state.Mark("evt-2"); err == nil {
			t.Fatal("Mark must fail when the parent-dir sync fails (FM-6)")
		}
		if state.Replayed["evt-2"] {
			t.Fatal("in-memory mark must not be set on persist failure (security F1)")
		}
		// D4: the rename already succeeded, so the target holds the NEW log
		// content — removing it would lose committed marks; the next Mark
		// retries the durability step. Both old and new content decode.
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("target after dir-sync failure must decode: %v", err)
		}
		if !reloaded.Replayed["evt-1"] || !reloaded.Replayed["evt-2"] {
			t.Fatalf("target must keep the new content after dir-sync failure: %v", reloaded.Replayed)
		}
	})
	t.Run("lock path under a regular file: no temp or lock artifact anywhere", func(t *testing.T) {
		dir := t.TempDir()
		blocker := filepath.Join(dir, "blocker")
		if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		// The state file's parent is a regular file: the lock open fails
		// with ENOTDIR (REQ-1 — lock failure is a persist error).
		// LoadReplayState would fail the same way, so construct the state
		// directly (same package).
		state := &ReplayState{Replayed: map[string]bool{"evt-1": true}, path: filepath.Join(blocker, "state.json")}
		if err := state.Mark("evt-2"); err == nil {
			t.Fatal("Mark must fail when the lock path hits ENOTDIR")
		}
		if state.Replayed["evt-2"] {
			t.Fatal("in-memory mark must not be set on persist failure (security F1)")
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".tmp") || strings.HasSuffix(entry.Name(), ".lock") {
				t.Fatalf("artifact left behind after ENOTDIR failure: %s", entry.Name())
			}
		}
	})
	t.Run("stale deterministic temp residue does not block a Mark", func(t *testing.T) {
		// Pre-fix writers left <state>.tmp artifacts, and a crashed write
		// could even leave a directory squatting on that name. The log
		// writer never touches the deterministic name (REQ-3), so stale
		// residue is harmless and must be left untouched.
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.Mkdir(path+".tmp", 0o700); err != nil {
			t.Fatal(err)
		}
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatalf("Mark must succeed despite stale <state>.tmp residue: %v", err)
		}
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if !reloaded.Replayed["evt-1"] {
			t.Fatalf("mark lost: %v", reloaded.Replayed)
		}
		info, err := os.Stat(path + ".tmp")
		if err != nil {
			t.Fatal(err)
		}
		if !info.IsDir() {
			t.Fatal("stale residue must be left untouched")
		}
	})
	t.Run("directory target: no temp left behind, target untouched", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		// The target path is an existing directory: classification fails
		// reading it (EISDIR), so the Mark fails before any temp exists
		// (FM-5); the directory is untouched.
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		state := &ReplayState{Replayed: map[string]bool{"evt-1": true}, path: path}
		if err := state.Mark("evt-2"); err == nil {
			t.Fatal("Mark must fail when the target is a directory")
		}
		if state.Replayed["evt-2"] {
			t.Fatal("in-memory mark must not be set on persist failure (security F1)")
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".tmp") {
				t.Fatalf(".tmp left behind after directory-target failure: %s", entry.Name())
			}
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if !info.IsDir() {
			t.Fatal("target must stay untouched after directory-target failure")
		}
	})
}

// Rolling-restart and mixed-version contract: the on-disk bytes are part of
// the compatibility surface, so pin the exact schema. A future format
// change breaks these tests loudly instead of silently invalidating the
// "zero data migration" claim. Also pins the 0o600 permission contract,
// the legacy-format load path (REQ-6(a)), the JSON-aware format
// discriminator (protocol F-1: unknown/malformed formats fail loudly), and
// torn-header self-healing (FM-10).
func TestReplayStateOnDiskFormatIsStable(t *testing.T) {
	const golden = "{\"format\":\"replay-log-v1\"}\n{\"event_id\":\"evt-1\"}\n"
	t.Run("Mark writes exactly the golden bytes", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != golden {
			t.Fatalf("persisted bytes = %q, want golden %q", raw, golden)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Fatalf("state file mode = %o, want 600", perm)
		}
	})
	t.Run("golden bytes load back", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte(golden), 0o600); err != nil {
			t.Fatal(err)
		}
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(state.Replayed) != 1 || !state.Replayed["evt-1"] {
			t.Fatalf("golden bytes must decode to exactly {evt-1}: %v", state.Replayed)
		}
	})
	t.Run("legacy file loads and converts in place on the next Mark", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte(`{"replayed":{"evt-1":true}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(state.Replayed) != 1 || !state.Replayed["evt-1"] {
			t.Fatalf("legacy file must decode to {evt-1}: %v", state.Replayed)
		}
		if err := state.Mark("evt-2"); err != nil {
			t.Fatalf("Mark must convert the legacy file in place: %v", err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		want := "{\"format\":\"replay-log-v1\"}\n{\"event_id\":\"evt-1\"}\n{\"event_id\":\"evt-2\"}\n"
		if string(raw) != want {
			t.Fatalf("converted bytes = %q, want %q", raw, want)
		}
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if !reloaded.Replayed["evt-1"] || !reloaded.Replayed["evt-2"] {
			t.Fatalf("converted file must hold both marks: %v", reloaded.Replayed)
		}
	})
	t.Run("unknown replay format fails loudly", func(t *testing.T) {
		// protocol F-1: a future format must never decode as an empty legacy
		// state (which would let the next Mark destroy its marks silently).
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte("{\"format\":\"replay-log-v2\"}\n{\"event_id\":\"x\"}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := LoadReplayState(path)
		if err == nil || !strings.Contains(err.Error(), "unknown replay state format") {
			t.Fatalf("unknown format must fail loudly, got %v", err)
		}
	})
	t.Run("malformed v1 header fails loudly", func(t *testing.T) {
		// A valid-JSON first line with format=replay-log-v1 but extra fields
		// is never produced by this writer: fail loudly instead of healing
		// it silently (protocol F-1).
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte("{\"format\":\"replay-log-v1\",\"x\":1}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := LoadReplayState(path)
		if err == nil || !strings.Contains(err.Error(), "invalid replay state header") {
			t.Fatalf("malformed header must fail loudly, got %v", err)
		}
	})
	t.Run("torn header fails loudly at load and self-heals on the next Mark", func(t *testing.T) {
		// FM-10: a never-fsynced torn first write fails loud at boot (the
		// same fail-loud outcome as today's torn-target pin); the next Mark
		// classifies it as torn and rewrites a fresh log, publishing the
		// union without losing anything (the torn content was never durable).
		path := filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(path, []byte("{\"format\":\"replay-log-v1"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadReplayState(path); err == nil {
			t.Fatal("torn header must fail loudly at load (FM-10)")
		}
		state := &ReplayState{Replayed: map[string]bool{}, path: path}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatalf("next Mark must self-heal the torn header: %v", err)
		}
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if !reloaded.Replayed["evt-1"] {
			t.Fatalf("healed file must hold the mark: %v", reloaded.Replayed)
		}
	})
}

// Crash→reboot idempotency through the replayer: a crash AFTER Mark
// persisted the replay decision but BEFORE the DLQ offsets were committed
// (the round aborts mid-scan with a transport error) must not re-republish
// on the next daemon round — the durable state file alone carries the
// idempotency across a reboot, which is the convergence the pre-fix F1
// torn-file boot Fatalf destroyed. Sub-case "reverted rename" pins the F2
// residue bound: if the crash reverted the second mark's rename (FM-4:
// crash between rename and parent-dir sync), only the reverted event is
// re-published, exactly once; the durably-marked event is not.
func TestReplayCrashAfterMarkRebootDoesNotRepublish(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	dlqMsgs := []kafka.Message{dlqMessageAt("evt-a", 0), dlqMessageAt("evt-b", 1)}
	acceptedMsgs := []kafka.Message{acceptedMessage("evt-a"), acceptedMessage("evt-b")}
	collect := func(dst *[]string) func(ctx context.Context, key, value []byte) error {
		return func(ctx context.Context, key, value []byte) error {
			*dst = append(*dst, string(key))
			return nil
		}
	}

	// Round 1 replays both events and Marks them durably (real fsyncs on a
	// real tempdir); then the accepted scan hits a transport failure BEFORE
	// commitResolved runs — the crash window "after the last Mark, before
	// the DLQ commit".
	broker := &brokerDLQ{topic: TopicDLQ, messages: dlqMsgs}
	state, err := LoadReplayState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	accepted := &fakeReplayReader{topic: TopicAccepted, messages: acceptedMsgs}
	accepted.fetchErr = errors.New("injected accepted-topic transport failure after both marks")
	var republished []string
	first := newReplayerWithFactories(broker.freshDLQSession(), func() messageReader { return accepted }, state, collect(&republished), log.New(io.Discard, "", 0))
	first.drainTimeout = 20 * time.Millisecond
	count, err := first.RunOnce(context.Background())
	if err == nil {
		t.Fatal("round must abort on the accepted transport failure (crash before DLQ commit)")
	}
	if count != 2 || len(republished) != 2 {
		t.Fatalf("round 1 replayed=%d republished=%v, want 2/[evt-a evt-b]", count, republished)
	}
	if broker.committed != 0 {
		t.Fatalf("DLQ offsets must be uncommitted after the crash: committed=%d", broker.committed)
	}

	// Reboot: the state file is the only surviving decision record.
	reloaded, err := LoadReplayState(statePath)
	if err != nil {
		t.Fatalf("reboot must load the durable state: %v", err)
	}
	if !reloaded.Replayed["evt-a"] || !reloaded.Replayed["evt-b"] {
		t.Fatalf("marks must survive the crash: %v", reloaded.Replayed)
	}

	// Round 2 (fresh sessions, as after a daemon restart): both DLQ records
	// are re-read, but the state file makes them already-replayed, so they
	// are committed without any republish.
	republished = nil
	reboot := newReplayerWithFactories(broker.freshDLQSession(), freshAcceptedReader(acceptedMsgs, nil), reloaded, collect(&republished), log.New(io.Discard, "", 0))
	reboot.drainTimeout = 20 * time.Millisecond
	count, err = reboot.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 || len(republished) != 0 {
		t.Fatalf("reboot round replayed=%d republished=%v, want 0/none (state file carries idempotency)", count, republished)
	}
	if broker.committed != 2 {
		t.Fatalf("reboot round must commit the re-read records: committed=%d", broker.committed)
	}

	t.Run("second mark rename reverted by the crash", func(t *testing.T) {
		path2 := filepath.Join(t.TempDir(), "state2.json")
		broker2 := &brokerDLQ{topic: TopicDLQ, messages: dlqMsgs}
		state2, err := LoadReplayState(path2)
		if err != nil {
			t.Fatal(err)
		}
		accepted2 := &fakeReplayReader{topic: TopicAccepted, messages: acceptedMsgs}
		accepted2.fetchErr = errors.New("injected accepted-topic transport failure after both marks")
		var repub2 []string
		first2 := newReplayerWithFactories(broker2.freshDLQSession(), func() messageReader { return accepted2 }, state2, collect(&repub2), log.New(io.Discard, "", 0))
		first2.drainTimeout = 20 * time.Millisecond
		if _, err := first2.RunOnce(context.Background()); err == nil {
			t.Fatal("round must abort on the accepted transport failure")
		}
		if broker2.committed != 0 {
			t.Fatalf("DLQ offsets must be uncommitted after the crash: committed=%d", broker2.committed)
		}
		// FM-4: the crash between evt-b's rename and its parent-dir sync may
		// revert the rename — the target holds the last durable content
		// ({evt-a} only), still decodable.
		if err := os.WriteFile(path2, []byte(`{"replayed":{"evt-a":true}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		reverted, err := LoadReplayState(path2)
		if err != nil {
			t.Fatalf("reverted target must decode: %v", err)
		}
		if !reverted.Replayed["evt-a"] || reverted.Replayed["evt-b"] {
			t.Fatalf("reverted target must hold only the durably-marked evt-a: %v", reverted.Replayed)
		}
		repub2 = nil
		reboot2 := newReplayerWithFactories(broker2.freshDLQSession(), freshAcceptedReader(acceptedMsgs, nil), reverted, collect(&repub2), log.New(io.Discard, "", 0))
		reboot2.drainTimeout = 20 * time.Millisecond
		count, err := reboot2.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		// Only the reverted event is re-published, exactly once; the
		// durably-marked event is not.
		if count != 1 || len(repub2) != 1 || repub2[0] != "evt-b" {
			t.Fatalf("reboot after rename revert replayed=%d republished=%v, want 1/[evt-b]", count, repub2)
		}
		if broker2.committed != 2 {
			t.Fatalf("reboot round must commit both re-read records: committed=%d", broker2.committed)
		}
	})
}

// TestReplayTransientRepublishFailureRetriesNextRound pins convergence: a
// transient republish error leaves the event pending (DLQ offset NOT
// committed, and the per-partition barrier keeps the committed offset below
// it), and the next round re-reads the same record and retries instead of
// losing it. Re-delivery works because RunOnce recreates the DLQ reader per
// round (resetDLQToCommittedOffset): kafka-go never re-delivers a fetched
// record within one group session, so a fresh session starting at the
// committed offset is the only mechanism that can re-read a pending record.
// The accepted reader is recreated too (freshAcceptedReader), so the
// original is re-scanned from the first retained offset.
func TestReplayTransientRepublishFailureRetriesNextRound(t *testing.T) {
	broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{dlqMessageAt("evt-t", 1)}}
	acceptedQueue := []kafka.Message{acceptedMessage("evt-t")}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	attempts := 0
	replayer := newReplayerWithFactories(broker.freshDLQSession(), freshAcceptedReader(acceptedQueue, nil), state, func(ctx context.Context, key, value []byte) error {
		attempts++
		if attempts == 1 {
			return context.DeadlineExceeded
		}
		return nil
	}, log.New(io.Discard, "", 0))
	replayer.drainTimeout = 20 * time.Millisecond
	if _, err := replayer.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state.Replayed["evt-t"] {
		t.Fatal("transient failure must not mark the event replayed")
	}
	if broker.committed != 0 {
		t.Fatalf("DLQ committed offset=%d after transient failure, want 0: the record must stay pending for the next round", broker.committed)
	}
	// Next round: the recreated DLQ session re-delivers the uncommitted
	// record from the committed offset and the event converges.
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || attempts != 2 {
		t.Fatalf("second round replayed=%d attempts=%d, want 1/2", count, attempts)
	}
	if !state.Replayed["evt-t"] {
		t.Fatal("evt-t must be marked replayed after the converged round")
	}
	if broker.committed != 2 {
		t.Fatalf("DLQ committed offset=%d after converged round, want 2", broker.committed)
	}
}

// TestReplayPermanentFailureConverges pins the anti-loop rule: an API-mode
// permanent rejection is marked replayed so the round converges and the
// operator investigates via the DLQ record instead of retrying forever.
func TestReplayPermanentFailureConverges(t *testing.T) {
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-p")}}
	accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{acceptedMessage("evt-p")}}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
		return &outbox.DeliveryError{Permanent: true}
	})
	replayer.drainTimeout = 20 * time.Millisecond
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || !state.Replayed["evt-p"] {
		t.Fatalf("permanent failure must converge: replayed=%d marked=%v", count, state.Replayed["evt-p"])
	}
	metrics := replayer.Metrics()
	if metrics.DLQRecords != 1 || metrics.AcceptedScanned != 1 || metrics.Replayed != 0 || metrics.RepublishFailures != 1 || metrics.Pending != 0 {
		t.Fatalf("metrics dlq=%d accepted=%d replayed=%d failures=%d pending=%d, want 1/1/0/1/0 (permanent rejection is not a replay)", metrics.DLQRecords, metrics.AcceptedScanned, metrics.Replayed, metrics.RepublishFailures, metrics.Pending)
	}
	if metrics.PermanentRejections != 1 || metrics.Unresolvable != 0 || metrics.UnparsableMarks != 0 {
		t.Fatalf("split counters permanent=%d unresolvable=%d unparsable=%d, want 1/0/0 (permanent rejection is its own resolution)", metrics.PermanentRejections, metrics.Unresolvable, metrics.UnparsableMarks)
	}
}

// AC-1 (REQ-1): an accepted-topic message with a missing key is still matched
// by its payload event_id: the original is republished byte-for-byte (nil key
// preserved) and the DLQ record is committed. A second round is a replay
// no-op: zero accepted fetches, zero republishes; the re-read record is only
// re-committed (a real broker's committed group offset would not re-deliver
// it).
func TestReplayEmptyKeyAcceptedMessageCommits(t *testing.T) {
	original := []byte(`{"event_id":"evt-e","source_system":"crm"}`)
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-e")}}
	accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{{Key: nil, Value: original, Partition: 0, Offset: 1}}}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	var republished []kafka.Message
	replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
		republished = append(republished, kafka.Message{Key: key, Value: value})
		return nil
	})
	replayer.drainTimeout = 20 * time.Millisecond
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("replayed=%d, want 1", count)
	}
	if len(republished) != 1 {
		t.Fatalf("republished=%d, want 1 (empty-key message must match by payload event_id)", len(republished))
	}
	if republished[0].Key != nil {
		t.Fatalf("republish key=%q, want nil (original key preserved byte-for-byte)", republished[0].Key)
	}
	if !bytes.Equal(republished[0].Value, original) {
		t.Fatalf("republished value=%s, want byte-identical original %s", republished[0].Value, original)
	}
	if !state.Replayed["evt-e"] {
		t.Fatal("payload event_id must be marked replayed")
	}
	if len(dlq.commits) != 1 {
		t.Fatalf("DLQ commits=%d, want 1", len(dlq.commits))
	}
	// Round 2 re-reads the DLQ record (the fake re-delivers it because the
	// fake never persists offsets): the state file makes it a replay no-op
	// — zero accepted fetches, zero republishes — and the already-resolved
	// record is re-committed (2 total).
	dlq.index = 0
	second, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second != 0 {
		t.Fatalf("second round replayed=%d, want 0 (idempotent)", second)
	}
	if len(republished) != 1 {
		t.Fatalf("second round republished=%d, want 1 (no re-replay)", len(republished))
	}
	if got := replayer.acceptedSeen.Load(); got != 1 {
		t.Fatalf("acceptedSeen=%d, want 1 (second round must not re-scan)", got)
	}
	if len(dlq.commits) != 2 {
		t.Fatalf("DLQ commits=%d, want 2 (re-read record re-committed, never replayed)", len(dlq.commits))
	}
}

// AC-2 (REQ-1): a key that differs from the payload event_id is overridden
// by the payload: the accepted message is republished exactly once with its
// original key preserved, the DLQ record is committed under the payload ID,
// and the stale key is never marked replayed.
func TestReplayMismatchedKeyResolvedByPayloadEventID(t *testing.T) {
	original := []byte(`{"event_id":"evt-m","source_system":"crm"}`)
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-m")}}
	accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{{Key: []byte("stale-key"), Value: original, Partition: 0, Offset: 1}}}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	var republished []kafka.Message
	replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
		republished = append(republished, kafka.Message{Key: key, Value: value})
		return nil
	})
	replayer.drainTimeout = 20 * time.Millisecond
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(republished) != 1 {
		t.Fatalf("replayed=%d republished=%d, want 1/1", count, len(republished))
	}
	if got := string(republished[0].Key); got != "stale-key" {
		t.Fatalf("republish key=%q, want stale-key (original key preserved byte-for-byte)", got)
	}
	if !bytes.Equal(republished[0].Value, original) {
		t.Fatalf("republished value=%s, want byte-identical original", republished[0].Value)
	}
	if !state.Replayed["evt-m"] {
		t.Fatal("payload event_id must be marked replayed")
	}
	if state.Replayed["stale-key"] {
		t.Fatal("stale key must never be marked replayed")
	}
	if len(dlq.commits) != 1 {
		t.Fatalf("DLQ commits=%d, want 1", len(dlq.commits))
	}
	// Round 2: replay no-op — zero accepted fetches, zero republishes.
	dlq.index = 0
	second, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second != 0 || len(republished) != 1 {
		t.Fatalf("second round replayed=%d republished=%d, want 0/1 (idempotent)", second, len(republished))
	}
	if got := replayer.acceptedSeen.Load(); got != 1 {
		t.Fatalf("acceptedSeen=%d, want 1 (no re-scan)", got)
	}
}

// AC-4 (REQ-2): a wanted DLQ record whose original is absent from the
// accepted topic converges in one bounded scan: when the topic goes quiet,
// the record is marked unresolvable (with a durable log line carrying the
// event_id) and committed. A second round performs zero accepted fetches.
func TestReplayUnresolvableRecordConvergesWithBoundedScans(t *testing.T) {
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-x")}}
	accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{
		{Key: []byte("k1"), Value: []byte(`{"event_id":"other-1","source_system":"crm"}`), Partition: 0, Offset: 1},
		{Key: []byte("k2"), Value: []byte("not-json"), Partition: 0, Offset: 2},
		{Key: nil, Value: []byte(`{"event_id":"other-2","source_system":"crm"}`), Partition: 0, Offset: 3},
	}}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	replayer := newReplayerWithReadersAndLogger(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
		t.Fatal("republish must never be called: no accepted message matches evt-x")
		return nil
	}, log.New(&logs, "", 0))
	replayer.drainTimeout = 20 * time.Millisecond
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("replayed=%d, want 1 (unresolvable record converges in one round)", count)
	}
	if !state.Replayed["evt-x"] {
		t.Fatal("absent original must be marked unresolvable at drain")
	}
	if len(dlq.commits) != 1 {
		t.Fatalf("DLQ commits=%d, want 1", len(dlq.commits))
	}
	if got := replayer.acceptedSeen.Load(); got != 3 {
		t.Fatalf("acceptedSeen=%d, want 3 (exactly one bounded scan)", got)
	}
	logged := logs.String()
	if !strings.Contains(logged, "unresolvable") || !strings.Contains(logged, "evt-x") {
		t.Fatalf("log missing unresolvable evidence for evt-x, got:\n%s", logged)
	}
	// Round 2: evt-x is already replayed, so no accepted fetches happen.
	dlq.index = 0
	second, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second != 0 {
		t.Fatalf("second round replayed=%d, want 0", second)
	}
	if got := replayer.acceptedSeen.Load(); got != 3 {
		t.Fatalf("acceptedSeen=%d, want 3 (round 2 must not scan)", got)
	}
}

// T1 (AC-1): a drained round whose wanted event is absent from the accepted
// scan yields Unresolvable=1 and Replayed=0 on the split metrics, while DLQ
// offset commit behavior stays unchanged (one commit; a second round is a
// no-op that only re-commits).
func TestReplayUnresolvableCounterSplit(t *testing.T) {
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-x")}}
	accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{
		{Key: []byte("k1"), Value: []byte(`{"event_id":"other-1","source_system":"crm"}`), Partition: 0, Offset: 1},
	}}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	replayer := newReplayerWithReadersAndLogger(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
		t.Fatal("republish must never be called: no accepted message matches evt-x")
		return nil
	}, log.New(&logs, "", 0))
	replayer.drainTimeout = 20 * time.Millisecond
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("replayed=%d, want 1 (unresolvable record converges in one round)", count)
	}
	if !state.Replayed["evt-x"] {
		t.Fatal("absent original must be marked replayed")
	}
	if len(dlq.commits) != 1 {
		t.Fatalf("DLQ commits=%d, want 1 (commit behavior unchanged)", len(dlq.commits))
	}
	metrics := replayer.Metrics()
	if metrics.Unresolvable != 1 || metrics.Replayed != 0 || metrics.PermanentRejections != 0 || metrics.UnparsableMarks != 0 {
		t.Fatalf("metrics unresolvable=%d replayed=%d permanent=%d unparsable=%d, want 1/0/0/0", metrics.Unresolvable, metrics.Replayed, metrics.PermanentRejections, metrics.UnparsableMarks)
	}
	logged := logs.String()
	if !strings.Contains(logged, "unresolvable") || !strings.Contains(logged, "reason=original-not-found-in-accepted-topic") {
		t.Fatalf("log missing unresolvable evidence, got:\n%s", logged)
	}
	// Round 2: evt-x is already marked, so no counter increments; the
	// re-read DLQ record is only re-committed (2 total).
	dlq.index = 0
	second, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second != 0 {
		t.Fatalf("second round replayed=%d, want 0 (idempotent)", second)
	}
	after := replayer.Metrics()
	if after.Unresolvable != 1 || after.Replayed != 0 {
		t.Fatalf("second round must not increment split counters: unresolvable=%d replayed=%d, want 1/0", after.Unresolvable, after.Replayed)
	}
	if len(dlq.commits) != 2 {
		t.Fatalf("DLQ commits=%d, want 2 (re-read record re-committed, never re-resolved)", len(dlq.commits))
	}
}

// T2 (AC-1): Metrics exposes distinct named counters (Replayed /
// UnparsableMarks / PermanentRejections / Unresolvable) that are
// independently addressable — a regression that re-conflates the paths into
// one value cannot compile this test.
func TestReplayMetricsNamedCountersIndependentlyAddressable(t *testing.T) {
	metrics := ReplayerMetrics{
		DLQRecords: 1, AcceptedScanned: 2, Replayed: 3, PermanentRejections: 4,
		Unresolvable: 5, UnparsableMarks: 6, RepublishFailures: 7, Pending: 8,
	}
	if metrics.Replayed != 3 || metrics.PermanentRejections != 4 || metrics.Unresolvable != 5 || metrics.UnparsableMarks != 6 {
		t.Fatalf("named counter fields not independently addressable: %+v", metrics)
	}
}

// T4 (AC-2): a successful republish round counts Replayed=1 and leaves all
// three split counters at zero — audit_dlq_replayed_total means successful
// re-publishes only.
func TestReplaySuccessfulRepublishCountsOnlyReplayed(t *testing.T) {
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-a")}}
	accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{acceptedMessage("evt-a")}}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	republished := 0
	replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
		republished++
		return nil
	})
	replayer.drainTimeout = 20 * time.Millisecond
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || republished != 1 {
		t.Fatalf("replayed=%d republished=%d, want 1/1", count, republished)
	}
	metrics := replayer.Metrics()
	if metrics.Replayed != 1 || metrics.UnparsableMarks != 0 || metrics.PermanentRejections != 0 || metrics.Unresolvable != 0 {
		t.Fatalf("metrics replayed=%d unparsable=%d permanent=%d unresolvable=%d, want 1/0/0/0", metrics.Replayed, metrics.UnparsableMarks, metrics.PermanentRejections, metrics.Unresolvable)
	}
}

// T5 (AC-2): a key-matched accepted message with a non-canonical value
// takes the anti-loop mark path: UnparsableMarks=1 and Replayed=0 — the
// mark is a convergence, not a replay.
func TestReplayUnparsableMarkIsNotReplayed(t *testing.T) {
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-u")}}
	accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{
		{Key: []byte("evt-u"), Value: []byte("not-json"), Partition: 0, Offset: 1},
	}}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
		t.Fatal("republish must never be called for an unparsable value")
		return nil
	})
	replayer.drainTimeout = 20 * time.Millisecond
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || !state.Replayed["evt-u"] || len(dlq.commits) != 1 {
		t.Fatalf("unparsable convergence replayed=%d marked=%v commits=%d, want 1/true/1", count, state.Replayed["evt-u"], len(dlq.commits))
	}
	metrics := replayer.Metrics()
	if metrics.UnparsableMarks != 1 || metrics.Replayed != 0 || metrics.PermanentRejections != 0 || metrics.Unresolvable != 0 {
		t.Fatalf("metrics unparsable=%d replayed=%d permanent=%d unresolvable=%d, want 1/0/0/0", metrics.UnparsableMarks, metrics.Replayed, metrics.PermanentRejections, metrics.Unresolvable)
	}
}

// T6 (AC-2): a mixed round with one event per resolution path increments
// exactly the counter of its own path, the four first-resolution counters
// sum to the round's first-resolution count, and a transient republish
// failure increments none of them. The transient record stays pending and
// the commit barrier never commits at/above its offset.
func TestReplayMixedRoundSplitCountersAndCommitBarrier(t *testing.T) {
	// Offsets 1..5 in one partition: evt-a republished (1), evt-b unparsable
	// anti-loop (2), evt-c permanent rejection (3), evt-t transient failure
	// (4, stays pending), evt-d unresolvable (5, absent from the accepted
	// topic). The barrier commits only offsets below the pending evt-t (4),
	// so evt-d at offset 5 stays uncommitted too.
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{
		dlqMessageAt("evt-a", 1),
		dlqMessageAt("evt-b", 2),
		dlqMessageAt("evt-c", 3),
		dlqMessageAt("evt-t", 4),
		dlqMessageAt("evt-d", 5),
	}}
	accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{
		acceptedMessage("evt-a"),
		{Key: []byte("evt-b"), Value: []byte("not-json"), Partition: 0, Offset: 2},
		acceptedMessage("evt-c"),
		acceptedMessage("evt-t"),
	}}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
		switch string(key) {
		case "evt-a":
			return nil
		case "evt-b":
			t.Fatal("republish must never be called for an unparsable value")
			return nil
		case "evt-c":
			return &outbox.DeliveryError{Permanent: true}
		case "evt-t":
			return errors.New("transient transport failure") // non-permanent: stays pending
		default:
			t.Fatalf("unexpected republish key=%s", key)
			return nil
		}
	})
	replayer.drainTimeout = 20 * time.Millisecond
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Fatalf("replayed=%d, want 4 (four first-resolutions; evt-t stays pending)", count)
	}
	metrics := replayer.Metrics()
	if metrics.Replayed != 1 || metrics.UnparsableMarks != 1 || metrics.PermanentRejections != 1 || metrics.Unresolvable != 1 {
		t.Fatalf("split counters replayed=%d unparsable=%d permanent=%d unresolvable=%d, want 1/1/1/1", metrics.Replayed, metrics.UnparsableMarks, metrics.PermanentRejections, metrics.Unresolvable)
	}
	if got := metrics.Replayed + metrics.UnparsableMarks + metrics.PermanentRejections + metrics.Unresolvable; got != 4 {
		t.Fatalf("first-resolution sum=%d, want 4 (invariant: one counter per durable first resolution)", got)
	}
	if metrics.RepublishFailures != 2 {
		t.Fatalf("republishFailures=%d, want 2 (permanent + transient, inclusive semantics kept)", metrics.RepublishFailures)
	}
	if metrics.Pending != 0 {
		t.Fatalf("pending=%d after round, want 0", metrics.Pending)
	}
	for _, id := range []string{"evt-a", "evt-b", "evt-c", "evt-d"} {
		if !state.Replayed[id] {
			t.Fatalf("%s must be marked replayed", id)
		}
	}
	if state.Replayed["evt-t"] {
		t.Fatal("transient failure must never be marked replayed")
	}
	// Commit barrier: only offsets 1..3 (below the pending evt-t at 4) are
	// committed; evt-d at offset 5 stays pending with evt-t.
	if len(dlq.commits) != 3 {
		t.Fatalf("DLQ commits=%d, want 3 (barrier: never commit at/above the pending offset)", len(dlq.commits))
	}
	for i, message := range dlq.commits {
		if want := int64(i + 1); message.Offset != want {
			t.Fatalf("commit[%d] offset=%d, want %d (no pending record may be leapfrogged)", i, message.Offset, want)
		}
	}
}

// AC-5 (failure mode: sustained ingest / cut-off): a topic that never goes
// quiet within 2x drainTimeout cuts the round off WITHOUT declaring it
// drained: nothing is marked, DLQ records stay pending, the round returns no
// error, and the next round retries. The scan is bounded by the 2-window cap
// (3 messages at 45ms cadence inside two 100ms windows). Once the topic goes
// quiet, the same record converges as unresolvable.
func TestReplaySustainedIngestCutsRoundOffWithoutMarking(t *testing.T) {
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-y")}}
	// delay=45ms < window=100ms: messages keep arriving inside each fetch
	// window, so no quiet window is observable before the 2x cap fires.
	accepted := &fakeReplayReader{topic: TopicAccepted, delay: 45 * time.Millisecond, messages: []kafka.Message{
		{Key: []byte("o1"), Value: []byte(`{"event_id":"other-1","source_system":"crm"}`), Partition: 0, Offset: 1},
		{Key: []byte("o2"), Value: []byte(`{"event_id":"other-2","source_system":"crm"}`), Partition: 0, Offset: 2},
		{Key: []byte("o3"), Value: []byte(`{"event_id":"other-3","source_system":"crm"}`), Partition: 0, Offset: 3},
	}}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
		t.Fatal("no accepted message matches evt-y; republish must never be called")
		return nil
	})
	replayer.drainTimeout = 100 * time.Millisecond
	for round := 1; round <= 2; round++ {
		count, err := replayer.RunOnce(context.Background())
		if err != nil {
			t.Fatalf("round %d: cut-off must not be an error, got %v", round, err)
		}
		if count != 0 {
			t.Fatalf("round %d: replayed=%d, want 0 (nothing marked on cut-off)", round, count)
		}
		if state.Replayed["evt-y"] {
			t.Fatalf("round %d: evt-y marked replayed on cut-off, want pending", round)
		}
		if len(dlq.commits) != 0 {
			t.Fatalf("round %d: DLQ commits=%d, want 0 (record must stay pending)", round, len(dlq.commits))
		}
		dlq.index = 0
		accepted.index = 0
	}
	// The round is bounded by the 2x drainTimeout cap, not by a quiet window.
	if got := replayer.acceptedSeen.Load(); got != 6 {
		t.Fatalf("acceptedSeen=%d, want 6 (two bounded 2-window scans of 3 messages)", got)
	}
	// Once the topic goes quiet the same record converges as unresolvable.
	accepted.index = len(accepted.messages)
	dlq.index = 0
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || !state.Replayed["evt-y"] || len(dlq.commits) != 1 {
		t.Fatalf("quiet round replayed=%d marked=%v commits=%d, want 1/true/1", count, state.Replayed["evt-y"], len(dlq.commits))
	}
}

// AC-6 (failure mode: transport error): a non-drain FetchMessage error
// aborts the round with NO DLQ commits — even events already resolved stay
// pending (at-least-once; the state file keeps replay idempotent) — and the
// next round retries. This pins the promised scanErr behavior.
func TestReplayTransportErrorLeavesRecordsPending(t *testing.T) {
	sentinel := errors.New("accepted topic transport failure")
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-a"), dlqMessage("evt-b")}}
	// The transport error strikes after evt-a's original was fetched but
	// before evt-b's original is reached: evt-a is resolved, evt-b is not.
	accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{acceptedMessage("evt-a")}, fetchErr: sentinel}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	republished := 0
	replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
		republished++
		return nil
	})
	replayer.drainTimeout = 20 * time.Millisecond
	count, err := replayer.RunOnce(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("round 1 err=%v, want the transport sentinel", err)
	}
	if count != 1 || republished != 1 {
		t.Fatalf("round 1 replayed=%d republished=%d, want 1/1 (evt-a resolved before the error)", count, republished)
	}
	if !state.Replayed["evt-a"] {
		t.Fatal("evt-a must be marked replayed (idempotency) even though the round errored")
	}
	if len(dlq.commits) != 0 {
		t.Fatalf("round 1 DLQ commits=%d, want 0 (transport error commits nothing, not even resolved records)", len(dlq.commits))
	}
	// Round 2 with the transport restored: the pending record is retried and
	// both records reach a durable decision.
	accepted.fetchErr = nil
	accepted.messages = append(accepted.messages, acceptedMessage("evt-b"))
	dlq.index = 0
	accepted.index = 0
	count, err = replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || republished != 2 {
		t.Fatalf("round 2 replayed=%d republished=%d, want 1/2", count, republished)
	}
	if !state.Replayed["evt-b"] {
		t.Fatal("evt-b must be replayed in round 2")
	}
	if len(dlq.commits) != 2 {
		t.Fatalf("round 2 DLQ commits=%d, want 2 (both records reach a durable decision)", len(dlq.commits))
	}
	if got := replayer.acceptedSeen.Load(); got != 3 {
		t.Fatalf("acceptedSeen=%d, want 3 (1 in round 1, 2 in round 2)", got)
	}
}

// AC-7 (drained-vs-cutoff boundary): the scan declares drained only after a
// full quiet window with the outer context still live; a last message inside
// the final window is a cut-off, not a drain. The boundary cases:
//   - exactly one full quiet window => drained => unresolvable marked;
//   - last message < one window before the 2x cap => cut-off, nothing marked;
//   - outer cancellation mid-scan => never marked;
//   - drained but found this round (transient republish failure) => pending.
func TestReplayDrainedVsCutoffBoundary(t *testing.T) {
	t.Run("full quiet window marks unresolvable", func(t *testing.T) {
		// Boundary equality: the last message is fetched, then exactly one
		// full window passes with nothing arriving => drained=true.
		dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-x")}}
		accepted := &fakeReplayReader{topic: TopicAccepted}
		state, err := LoadReplayState("")
		if err != nil {
			t.Fatal(err)
		}
		var logs bytes.Buffer
		replayer := newReplayerWithReadersAndLogger(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
			t.Fatal("republish must never be called")
			return nil
		}, log.New(&logs, "", 0))
		replayer.drainTimeout = 60 * time.Millisecond
		count, err := replayer.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if count != 1 || !state.Replayed["evt-x"] || len(dlq.commits) != 1 {
			t.Fatalf("quiet drain replayed=%d marked=%v commits=%d, want 1/true/1", count, state.Replayed["evt-x"], len(dlq.commits))
		}
		if !strings.Contains(logs.String(), "unresolvable") {
			t.Fatalf("log missing unresolvable evidence:\n%s", logs.String())
		}
	})
	t.Run("last message inside final window is cut-off, not drain", func(t *testing.T) {
		// 45ms message cadence < 60ms window: the second message lands at
		// 90ms (> one window), so the 2x cap (120ms) fires with the last
		// message only 30ms old => drained=false, nothing marked, no commits.
		dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-x")}}
		accepted := &fakeReplayReader{topic: TopicAccepted, delay: 45 * time.Millisecond, messages: []kafka.Message{
			{Key: []byte("o1"), Value: []byte(`{"event_id":"other-1","source_system":"crm"}`), Partition: 0, Offset: 1},
			{Key: []byte("o2"), Value: []byte(`{"event_id":"other-2","source_system":"crm"}`), Partition: 0, Offset: 2},
		}}
		state, err := LoadReplayState("")
		if err != nil {
			t.Fatal(err)
		}
		replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
			t.Fatal("republish must never be called")
			return nil
		})
		replayer.drainTimeout = 60 * time.Millisecond
		count, err := replayer.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 || state.Replayed["evt-x"] || len(dlq.commits) != 0 {
			t.Fatalf("cut-off replayed=%d marked=%v commits=%d, want 0/false/0", count, state.Replayed["evt-x"], len(dlq.commits))
		}
		if got := replayer.acceptedSeen.Load(); got != 2 {
			t.Fatalf("acceptedSeen=%d, want 2 (two messages inside the 2-window scan)", got)
		}
		// Once the topic goes quiet the same record converges.
		accepted.index = len(accepted.messages)
		dlq.index = 0
		count, err = replayer.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if count != 1 || !state.Replayed["evt-x"] || len(dlq.commits) != 1 {
			t.Fatalf("quiet round replayed=%d marked=%v commits=%d, want 1/true/1", count, state.Replayed["evt-x"], len(dlq.commits))
		}
	})
	t.Run("outer cancellation never marks", func(t *testing.T) {
		dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-x")}}
		accepted := &fakeReplayReader{topic: TopicAccepted}
		state, err := LoadReplayState("")
		if err != nil {
			t.Fatal(err)
		}
		replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
			t.Fatal("republish must never be called")
			return nil
		})
		replayer.drainTimeout = 100 * time.Millisecond
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()
		count, err := replayer.RunOnce(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 || state.Replayed["evt-x"] || len(dlq.commits) != 0 {
			t.Fatalf("cancelled round replayed=%d marked=%v commits=%d, want 0/false/0", count, state.Replayed["evt-x"], len(dlq.commits))
		}
	})
	t.Run("drained but found stays pending", func(t *testing.T) {
		// D-2: at drain only never-seen wanted IDs are marked unresolvable.
		// evt-y was found this round (transient republish failure), so it
		// stays pending even though the topic went quiet; evt-x was never
		// seen and converges. Distinct offsets (1 and 2) model real Kafka
		// offsets (unique per partition); with the commit barrier, evt-x is
		// committable (offset 1 < pending offset 2) and evt-y stays
		// uncommitted.
		dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessageAt("evt-x", 1), dlqMessageAt("evt-y", 2)}}
		accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{acceptedMessage("evt-y")}}
		state, err := LoadReplayState("")
		if err != nil {
			t.Fatal(err)
		}
		attempts := 0
		replayer := newReplayerWithReaders(dlq, accepted, state, func(ctx context.Context, key, value []byte) error {
			attempts++
			if attempts == 1 {
				return context.DeadlineExceeded // transient: found but not resolved
			}
			return nil
		})
		replayer.drainTimeout = 20 * time.Millisecond
		count, err := replayer.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if count != 1 || !state.Replayed["evt-x"] || state.Replayed["evt-y"] {
			t.Fatalf("drain replayed=%d evt-x=%v evt-y=%v, want 1/true/false", count, state.Replayed["evt-x"], state.Replayed["evt-y"])
		}
		if len(dlq.commits) != 1 {
			t.Fatalf("DLQ commits=%d, want 1 (only evt-x; found-but-failed evt-y stays pending)", len(dlq.commits))
		}
		// Round 2: evt-y retries and succeeds; both records converge.
		dlq.index = 0
		accepted.index = 0
		count, err = replayer.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if count != 1 || !state.Replayed["evt-y"] || len(dlq.commits) != 3 {
			t.Fatalf("round 2 replayed=%d evt-y=%v commits=%d, want 1/true/3", count, state.Replayed["evt-y"], len(dlq.commits))
		}
	})
}

// T1 / AC-1 (REQ-3): daemon-mode two-round regression. Round 1 resolves
// evt-a; round 2 collects a NEW DLQ record for evt-b whose accepted original
// predates round 1's scan end (it was consumed, not wanted, in round 1). The
// accepted reader must be recreated per round, so round 2 re-scans from the
// first retained offset and evt-b is actually re-published — NOT logged
// unresolvable and dropped. Fails pre-fix: without the reset, round 2's scan
// starts where round 1 ended, evt-b is marked unresolvable, the DLQ offset is
// committed, and republished stays 1.
func TestReplayDaemonRound2RepublishesOlderAcceptedOriginal(t *testing.T) {
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-a")}}
	// evt-b's original is at a higher offset than evt-a's: both are consumed
	// by round 1's scan, so evt-b predates round 1's scan end.
	acceptedQueue := []kafka.Message{acceptedMessage("evt-a"), acceptedMessage("evt-b")}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	republished := 0
	replayer := newReplayerWithAcceptedFactory(dlq, freshAcceptedReader(acceptedQueue, nil), state, func(ctx context.Context, key, value []byte) error {
		republished++
		return nil
	}, log.New(&logs, "", 0))
	replayer.drainTimeout = 20 * time.Millisecond

	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || republished != 1 {
		t.Fatalf("round 1 replayed=%d republished=%d, want 1/1", count, republished)
	}
	if !state.Replayed["evt-a"] {
		t.Fatal("round 1 must mark evt-a replayed")
	}
	if len(dlq.commits) != 1 {
		t.Fatalf("round 1 DLQ commits=%d, want 1", len(dlq.commits))
	}

	// Round 2: a NEW DLQ record for evt-b arrives. The DLQ fake's persistent
	// index models the real reader's advanced position; accepted.index is
	// deliberately NOT reset — the per-round accepted-reader recreation is
	// exactly what must make evt-b's older original reachable again.
	dlq.messages = append(dlq.messages, dlqMessage("evt-b"))
	count, err = replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || republished != 2 {
		t.Fatalf("round 2 replayed=%d republished=%d, want 1/2 (evt-b must be actually re-published, not marked unresolvable)", count, republished)
	}
	if !state.Replayed["evt-b"] {
		t.Fatal("evt-b must be marked replayed")
	}
	if len(dlq.commits) != 2 {
		t.Fatalf("DLQ commits=%d, want 2 (both records reach a durable decision)", len(dlq.commits))
	}
	if logged := logs.String(); strings.Contains(logged, "unresolvable") {
		t.Fatalf("round 2 must not log unresolvable for evt-b, got:\n%s", logged)
	}
}

// T2 / AC-2 (REQ-4): round 2 re-fetches every message round 1 already
// consumed, proving the accepted reader was re-seeked to the first retained
// offset instead of resuming at round 1's end. Both proofs are asserted:
// acceptedSeen grows by N again (3 -> 6) and round 2's first fetched offset
// equals round 1's first fetched offset.
func TestReplayRound2RefetchesRound1Messages(t *testing.T) {
	dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-1")}}
	acceptedQueue := []kafka.Message{
		{Key: []byte("evt-1"), Value: []byte(`{"event_id":"evt-1","source_system":"crm"}`), Partition: 0, Offset: 1},
		{Key: []byte("evt-2"), Value: []byte(`{"event_id":"evt-2","source_system":"crm"}`), Partition: 0, Offset: 2},
		{Key: []byte("other"), Value: []byte(`{"event_id":"other","source_system":"crm"}`), Partition: 0, Offset: 3},
	}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	var fetchLog []int64
	republished := 0
	replayer := newReplayerWithAcceptedFactory(dlq, freshAcceptedReader(acceptedQueue, &fetchLog), state, func(ctx context.Context, key, value []byte) error {
		republished++
		return nil
	}, log.New(io.Discard, "", 0))
	replayer.drainTimeout = 20 * time.Millisecond

	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || republished != 1 {
		t.Fatalf("round 1 replayed=%d republished=%d, want 1/1", count, republished)
	}
	if got := replayer.acceptedSeen.Load(); got != 3 {
		t.Fatalf("round 1 acceptedSeen=%d, want 3", got)
	}

	// Round 2 wants evt-2, whose original (offset 2) was consumed in round 1.
	dlq.messages = append(dlq.messages, dlqMessage("evt-2"))
	count, err = replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || republished != 2 {
		t.Fatalf("round 2 replayed=%d republished=%d, want 1/2", count, republished)
	}
	if got := replayer.acceptedSeen.Load(); got != 6 {
		t.Fatalf("round 2 acceptedSeen=%d, want 6 (round 2 must re-fetch all 3 messages from the first retained offset)", got)
	}
	if !state.Replayed["evt-2"] {
		t.Fatal("evt-2 must be replayed")
	}
	if len(fetchLog) < 6 {
		t.Fatalf("fetchLog=%v, want 6 fetches across two rounds", fetchLog)
	}
	if fetchLog[0] != fetchLog[3] {
		t.Fatalf("round 2 first fetched offset=%d, want round 1 first offset %d (re-seek to the first retained offset)", fetchLog[3], fetchLog[0])
	}
}

// T3 / AC-3 (REQ-5): daemon mode (one Replayer, consecutive rounds with the
// per-round reset) and -once mode (a fresh Replayer per round, mirroring
// main.go's -once fresh-reader pattern) must produce identical OUTCOMES for
// identical DLQ input referencing older accepted messages. The assertion is
// on republished outcomes / state marks / DLQ commits / absence of the
// unresolvable log — NOT the raw replayed count: unresolvable marks inflate
// the daemon count, so a count-only comparison passes on buggy code.
func TestReplayDaemonEqualsOnceOutcomes(t *testing.T) {
	daemonStatePath := filepath.Join(t.TempDir(), "daemon-state.json")
	onceStatePath := filepath.Join(t.TempDir(), "once-state.json")
	acceptedQueue := []kafka.Message{acceptedMessage("evt-a"), acceptedMessage("evt-b")}

	// Scenario (a): daemon mode — one Replayer, two consecutive rounds.
	daemonState, err := LoadReplayState(daemonStatePath)
	if err != nil {
		t.Fatal(err)
	}
	daemonDLQ := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessage("evt-a")}}
	daemonRepublished := 0
	daemonLogs := &bytes.Buffer{}
	daemon := newReplayerWithAcceptedFactory(daemonDLQ, freshAcceptedReader(acceptedQueue, nil), daemonState, func(ctx context.Context, key, value []byte) error {
		daemonRepublished++
		return nil
	}, log.New(daemonLogs, "", 0))
	daemon.drainTimeout = 20 * time.Millisecond
	if _, err := daemon.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	daemonDLQ.messages = append(daemonDLQ.messages, dlqMessage("evt-b"))
	if _, err := daemon.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Scenario (b): -once mode — a fresh Replayer (fresh readers) per round,
	// sharing the -state file across runs exactly like the binary does.
	onceRepublished := 0
	onceLogs := &bytes.Buffer{}
	onceCommits := 0
	for round, dlqInput := range [][]kafka.Message{{dlqMessage("evt-a")}, {dlqMessage("evt-b")}} {
		onceState, err := LoadReplayState(onceStatePath)
		if err != nil {
			t.Fatal(err)
		}
		onceDLQ := &fakeReplayReader{topic: TopicDLQ, messages: dlqInput}
		once := newReplayerWithAcceptedFactory(onceDLQ, freshAcceptedReader(acceptedQueue, nil), onceState, func(ctx context.Context, key, value []byte) error {
			onceRepublished++
			return nil
		}, log.New(onceLogs, "", 0))
		once.drainTimeout = 20 * time.Millisecond
		if _, err := once.RunOnce(context.Background()); err != nil {
			t.Fatalf("once round %d: %v", round+1, err)
		}
		onceCommits += len(onceDLQ.commits)
	}

	// Outcome equivalence — republished deliveries, state marks, DLQ commits,
	// and the absence of unresolvable marks. These fail on buggy code (the
	// daemon marks evt-b unresolvable instead of re-publishing it), while the
	// raw replayed counts (2 == 2) would not.
	if daemonRepublished != 2 || onceRepublished != 2 {
		t.Fatalf("republished daemon=%d once=%d, want 2/2 (both events actually delivered in both modes)", daemonRepublished, onceRepublished)
	}
	if !daemonState.Replayed["evt-a"] || !daemonState.Replayed["evt-b"] {
		t.Fatalf("daemon state=%v, want {evt-a, evt-b}", daemonState.Replayed)
	}
	onceStateFinal, err := LoadReplayState(onceStatePath)
	if err != nil {
		t.Fatal(err)
	}
	if !onceStateFinal.Replayed["evt-a"] || !onceStateFinal.Replayed["evt-b"] {
		t.Fatalf("once state=%v, want {evt-a, evt-b}", onceStateFinal.Replayed)
	}
	if len(daemonDLQ.commits) != 2 || onceCommits != 2 {
		t.Fatalf("DLQ commits daemon=%d once=%d, want 2/2", len(daemonDLQ.commits), onceCommits)
	}
	if strings.Contains(daemonLogs.String(), "unresolvable") || strings.Contains(onceLogs.String(), "unresolvable") {
		t.Fatalf("neither mode may mark unresolvable:\ndaemon:\n%s\nonce:\n%s", daemonLogs.String(), onceLogs.String())
	}
}

// F1 / F-A: a transiently-failed mid-batch DLQ record must never be
// leapfrogged by a later commit in the same partition. Kafka stores ONE
// committed offset per (group, partition) and kafka-go's CommitMessages
// commits offset+1 of the highest message passed, so committing the resolved
// record at offset 3 would advance the partition to 4 and the next round
// would never re-deliver the pending record at offset 1 — silent permanent
// loss. The commitResolved per-partition barrier leaves resolved records at
// or above the first pending offset uncommitted, so round 2's recreated DLQ
// session (freshDLQSession) re-reads both records, evt-p converges, and the
// committed offset only advances once nothing is pending. Fails pre-fix:
// without the barrier, round 1 commits offset 3 -> committed=4, round 2
// delivers nothing, and evt-p stays unmarked forever (attempts==1).
func TestReplayMidBatchPendingRecordIsNotSkipped(t *testing.T) {
	broker := &brokerDLQ{topic: TopicDLQ, messages: []kafka.Message{
		dlqMessageAt("evt-p", 1), // transient republish failure in round 1
		dlqMessageAt("evt-r", 3), // resolved in round 1
	}}
	acceptedQueue := []kafka.Message{acceptedMessage("evt-p"), acceptedMessage("evt-r")}
	state, err := LoadReplayState("")
	if err != nil {
		t.Fatal(err)
	}
	attempts := map[string]int{}
	replayer := newReplayerWithFactories(broker.freshDLQSession(), freshAcceptedReader(acceptedQueue, nil), state, func(ctx context.Context, key, value []byte) error {
		attempts[string(key)]++
		if string(key) == "evt-p" && attempts["evt-p"] == 1 {
			return context.DeadlineExceeded // transient: retried next round
		}
		return nil
	}, log.New(io.Discard, "", 0))
	replayer.drainTimeout = 20 * time.Millisecond

	// Round 1: evt-p fails (pending), evt-r succeeds (resolved). The barrier
	// keeps BOTH uncommitted — committing evt-r at offset 3 would cross the
	// pending offset 1 — so the partition's committed offset stays 0.
	count, err := replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("round 1 replayed=%d, want 1 (evt-r)", count)
	}
	if !state.Replayed["evt-r"] || state.Replayed["evt-p"] {
		t.Fatalf("round 1 state=%v, want {evt-r} only (evt-p pending)", state.Replayed)
	}
	if broker.committed != 0 {
		t.Fatalf("round 1 committed offset=%d, want 0: the partition must never commit past a pending record", broker.committed)
	}
	if len(broker.commits) != 0 {
		t.Fatalf("round 1 commits=%d, want 0 (evt-r must stay uncommitted behind the barrier)", len(broker.commits))
	}

	// Round 2: the recreated DLQ session re-reads both records from the
	// committed offset; evt-p is retried and converges, evt-r (already
	// replayed) is re-committed without a re-republish. The committed offset
	// advances only now that nothing is pending.
	count, err = replayer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("round 2 replayed=%d, want 1 (evt-p)", count)
	}
	if !state.Replayed["evt-p"] {
		t.Fatal("evt-p must converge in round 2")
	}
	if attempts["evt-p"] != 2 || attempts["evt-r"] != 1 {
		t.Fatalf("attempts evt-p=%d evt-r=%d, want 2/1 (evt-p retried exactly once, evt-r never re-republished)", attempts["evt-p"], attempts["evt-r"])
	}
	if broker.committed != 4 {
		t.Fatalf("committed offset=%d after convergence, want 4 (both records durable)", broker.committed)
	}
}

// F4: a reader close failure on either per-round reset aborts the round
// BEFORE anything is collected, republished, marked, or committed; the
// pending metric stays 0 (nothing is in flight), and the next round retries.
// kafka-go Close is idempotent and returns nil, so these paths are
// defensive — but the abort must not leak a stale audit_dlq_pending reading.
func TestReplayResetFailureAbortsRound(t *testing.T) {
	t.Run("dlq reader close failure", func(t *testing.T) {
		closeErr := errors.New("dlq close failure")
		dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessageAt("evt-a", 1)}, closeErr: closeErr}
		state, err := LoadReplayState("")
		if err != nil {
			t.Fatal(err)
		}
		republishCalled := false
		replayer := newReplayerWithFactories(func() messageReader { return dlq }, freshAcceptedReader([]kafka.Message{acceptedMessage("evt-a")}, nil), state, func(ctx context.Context, key, value []byte) error {
			republishCalled = true
			return nil
		}, log.New(io.Discard, "", 0))
		replayer.drainTimeout = 20 * time.Millisecond
		_, err = replayer.RunOnce(context.Background())
		if !errors.Is(err, closeErr) {
			t.Fatalf("err=%v, want the dlq close error", err)
		}
		if republishCalled {
			t.Fatal("republish must never be called after a reset abort")
		}
		if len(state.Replayed) != 0 || len(dlq.commits) != 0 {
			t.Fatalf("aborted round must not mark or commit: state=%v commits=%d", state.Replayed, len(dlq.commits))
		}
		if m := replayer.Metrics(); m.Pending != 0 {
			t.Fatalf("pending=%d after aborted round, want 0 (no stale metric)", m.Pending)
		}
	})
	t.Run("accepted reader close failure", func(t *testing.T) {
		closeErr := errors.New("accepted close failure")
		dlq := &fakeReplayReader{topic: TopicDLQ, messages: []kafka.Message{dlqMessageAt("evt-a", 1)}}
		accepted := &fakeReplayReader{topic: TopicAccepted, messages: []kafka.Message{acceptedMessage("evt-a")}, closeErr: closeErr}
		state, err := LoadReplayState("")
		if err != nil {
			t.Fatal(err)
		}
		republishCalled := false
		replayer := newReplayerWithFactories(func() messageReader { return dlq }, func() messageReader { return accepted }, state, func(ctx context.Context, key, value []byte) error {
			republishCalled = true
			return nil
		}, log.New(io.Discard, "", 0))
		replayer.drainTimeout = 20 * time.Millisecond
		_, err = replayer.RunOnce(context.Background())
		if !errors.Is(err, closeErr) {
			t.Fatalf("err=%v, want the accepted close error", err)
		}
		if republishCalled {
			t.Fatal("republish must never be called after a reset abort")
		}
		if len(state.Replayed) != 0 || len(dlq.commits) != 0 {
			t.Fatalf("aborted round must not mark or commit: state=%v commits=%d", state.Replayed, len(dlq.commits))
		}
		if m := replayer.Metrics(); m.Pending != 0 {
			t.Fatalf("pending=%d after aborted round, want 0 (no stale metric)", m.Pending)
		}
	})
}

// AC-1 (REQ-1/REQ-2/REQ-8): a concurrent Mark storm on one state path yields
// a decodable file with the union of every writer's marks. Separate
// instances model separate processes with stale boot-time views; the shared
// sub-case exercises one instance from many goroutines (race-clean via mu).
// Fails pre-fix: last-writer-wins drops marks, and the shared sub-case is a
// data race.
func TestReplayStateConcurrentMarksUnion(t *testing.T) {
	t.Run("separate instances", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		instances := make([]*ReplayState, replayConcurrentWriters)
		for w := range instances {
			s, err := LoadReplayState(path)
			if err != nil {
				t.Fatal(err)
			}
			instances[w] = s
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for w, s := range instances {
			wg.Add(1)
			go func(w int, s *ReplayState) {
				defer wg.Done()
				<-start
				for i := 0; i < replayConcurrentPerWriter; i++ {
					if err := s.Mark(fmt.Sprintf("w%d-e%d", w, i)); err != nil {
						t.Errorf("writer %d: %v", w, err)
						return
					}
				}
				for i := 0; i < replayConcurrentShared; i++ {
					if err := s.Mark(fmt.Sprintf("core-%d", i)); err != nil {
						t.Errorf("writer %d core: %v", w, err)
						return
					}
				}
			}(w, s)
		}
		close(start)
		wg.Wait()
		assertReplayUnion(t, path)
	})
	t.Run("shared instance", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for w := 0; w < replayConcurrentWriters; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				<-start
				for i := 0; i < replayConcurrentPerWriter; i++ {
					if err := state.Mark(fmt.Sprintf("w%d-e%d", w, i)); err != nil {
						t.Errorf("writer %d: %v", w, err)
						return
					}
				}
				for i := 0; i < replayConcurrentShared; i++ {
					if err := state.Mark(fmt.Sprintf("core-%d", i)); err != nil {
						t.Errorf("writer %d core: %v", w, err)
						return
					}
				}
			}(w)
		}
		close(start)
		wg.Wait()
		assertReplayUnion(t, path)
	})
}

// assertReplayUnion asserts the state file decodes and holds the union of
// every concurrent writer's distinct IDs plus the shared core.
func assertReplayUnion(t *testing.T, path string) {
	t.Helper()
	state, err := LoadReplayState(path)
	if err != nil {
		t.Fatalf("union must decode: %v", err)
	}
	for w := 0; w < replayConcurrentWriters; w++ {
		for i := 0; i < replayConcurrentPerWriter; i++ {
			if !state.Replayed[fmt.Sprintf("w%d-e%d", w, i)] {
				t.Errorf("missing mark w%d-e%d (union incomplete)", w, i)
			}
		}
	}
	for i := 0; i < replayConcurrentShared; i++ {
		if !state.Replayed[fmt.Sprintf("core-%d", i)] {
			t.Errorf("missing shared mark core-%d (union incomplete)", i)
		}
	}
}

// AC-2 (REQ-3/REQ-6(c)): no two writers ever share one temp inode; the
// deterministic <state>.tmp name is never used; no *.tmp residue remains
// after a storm; stale temps (crash residue) and torn append tails are never
// interpreted as marks and never break a later Mark.
func TestReplayStateUniqueTempPerWrite(t *testing.T) {
	t.Run("first writes use distinct unique temps", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		var temps []string
		state.newTemp = func(dir, pattern string) (*os.File, error) {
			file, err := os.CreateTemp(dir, pattern)
			if err == nil {
				temps = append(temps, file.Name())
			}
			return file, err
		}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		// Delete the state file between marks: every Mark re-triggers the
		// first-write rewrite, forcing two rewriteFull attempts on one path
		// — each must use its own unique temp (async F1).
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := state.Mark("evt-2"); err != nil {
			t.Fatal(err)
		}
		if len(temps) != 2 {
			t.Fatalf("temps=%d, want 2 (one per first write)", len(temps))
		}
		if temps[0] == temps[1] {
			t.Fatal("two rewrites shared one temp inode")
		}
		for _, temp := range temps {
			if temp == path+".tmp" {
				t.Fatal("deterministic <state>.tmp name must never be used")
			}
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".tmp") {
				t.Fatalf("*.tmp residue after Mark: %s", entry.Name())
			}
		}
	})
	t.Run("concurrent storm leaves no temp residue", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for w := 0; w < replayConcurrentWriters; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				<-start
				for i := 0; i < replayConcurrentPerWriter; i++ {
					if err := state.Mark(fmt.Sprintf("w%d-e%d", w, i)); err != nil {
						t.Errorf("writer %d: %v", w, err)
						return
					}
				}
			}(w)
		}
		close(start)
		wg.Wait()
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".tmp") {
				t.Fatalf("*.tmp residue after the storm: %s", entry.Name())
			}
		}
	})
	t.Run("crash residue temp is ignored and a later Mark publishes the union", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		// A crashed writer's unique temp survives next to the state file
		// (simulated crash residue — no cleanup ran).
		stale := filepath.Join(dir, "state.json-1234567890.tmp")
		if err := os.WriteFile(stale, []byte("\x00partial-write-garbage"), 0o600); err != nil {
			t.Fatal(err)
		}
		reloaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("stale temp must not fail the load: %v", err)
		}
		if !reloaded.Replayed["evt-1"] {
			t.Fatal("state lost alongside stale temp")
		}
		if err := state.Mark("evt-2"); err != nil {
			t.Fatalf("Mark must succeed despite stale temp: %v", err)
		}
		final, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if !final.Replayed["evt-1"] || !final.Replayed["evt-2"] {
			t.Fatalf("union must be published despite stale temp: %v", final.Replayed)
		}
		if _, err := os.Stat(stale); err != nil {
			t.Fatalf("loader must never delete a stale temp: %v", err)
		}
	})
	t.Run("torn append tail is never a mark and the next Mark self-heals", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.Mark("evt-1"); err != nil {
			t.Fatal(err)
		}
		// Crash mid-append: a partial line without its terminating newline.
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte(`{"event_id":"evt-torn`)); err != nil {
			file.Close()
			t.Fatal(err)
		}
		file.Close()
		loaded, err := LoadReplayState(path)
		if err != nil {
			t.Fatalf("torn tail must not fail the load: %v", err)
		}
		if !loaded.Replayed["evt-1"] || loaded.Replayed["evt-torn"] {
			t.Fatalf("torn tail must be skipped, never a mark: %v", loaded.Replayed)
		}
		if err := state.Mark("evt-2"); err != nil {
			t.Fatalf("next Mark must repair the tail: %v", err)
		}
		final, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		if !final.Replayed["evt-1"] || !final.Replayed["evt-2"] || final.Replayed["evt-torn"] {
			t.Fatalf("repair must publish the union and drop the torn fragment: %v", final.Replayed)
		}
	})
}

// AC-3 (REQ-5): the persistence cost of one Mark is not O(replayed-set
// size). The bytes-written bound is deterministic (a size-recording syncFile
// hook): after seeding M=10,000 marks, K=1,000 more marks must write O(M+K)
// bytes total — pre-fix, every Mark rewrote the whole map (≈10.5 MB for
// M=10,000, K=1,000 vs the ≈176 KB bound). Growth-independence via
// AllocsPerRun: per-Mark allocations at M=10,000 must be ≤ 4× those at
// M=100 (pre-fix, json.Marshal of the full map scales with the set size).
func TestReplayStateMarkCostNotOOfSetSize(t *testing.T) {
	t.Run("total bytes written is O(M+K)", func(t *testing.T) {
		const m = 10_000
		const k = 1_000
		dir := t.TempDir()
		path := filepath.Join(dir, "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		var sizes []int64
		state.syncFile = func(p string) error {
			info, err := os.Stat(p)
			if err != nil {
				t.Fatal(err)
			}
			sizes = append(sizes, info.Size())
			return nil
		}
		for i := 0; i < m; i++ {
			if err := state.Mark(fmt.Sprintf("seed-%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		if len(sizes) != m {
			t.Fatalf("seed syncs=%d, want %d (one sync per Mark)", len(sizes), m)
		}
		base := sizes[len(sizes)-1]
		for i := 0; i < k; i++ {
			if err := state.Mark(fmt.Sprintf("mark-%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		if len(sizes) != m+k {
			t.Fatalf("syncs=%d, want %d", len(sizes), m+k)
		}
		total := sizes[len(sizes)-1] - base
		if bound := 16 * int64(m+k); total > bound {
			t.Fatalf("bytes written for %d marks = %d, want ≤ %d (O(M+K)); per-mark cost is not O(set size)", k, total, bound)
		}
	})
	t.Run("growth-independence: per-Mark allocations do not scale with M", func(t *testing.T) {
		allocsAt := func(m int) float64 {
			dir := t.TempDir()
			path := filepath.Join(dir, "state.json")
			state, err := LoadReplayState(path)
			if err != nil {
				t.Fatal(err)
			}
			state.syncFile = func(string) error { return nil } // isolate the append path, not fsync
			for i := 0; i < m; i++ {
				if err := state.Mark(fmt.Sprintf("seed-%d", i)); err != nil {
					t.Fatal(err)
				}
			}
			id := 0
			return testing.AllocsPerRun(100, func() {
				if err := state.Mark(fmt.Sprintf("mark-%d", id)); err != nil {
					t.Fatal(err)
				}
				id++
			})
		}
		small := allocsAt(100)
		large := allocsAt(10_000)
		if large > 4*small {
			t.Fatalf("allocs/op at M=10,000 (%v) > 4× M=100 (%v): per-Mark cost scales with the set size", large, small)
		}
	})
}

// BenchmarkReplayStateMark reports per-Mark ns/op and B/op over seeded set
// sizes {100, 1,000, 10,000}; a flat profile across M is the supporting
// evidence for the append-only log (constant per line). No-op syncs isolate
// the append path's own cost from filesystem fsync latency.
func BenchmarkReplayStateMark(b *testing.B) {
	for _, m := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("M=%d", m), func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "state.json")
			state, err := LoadReplayState(path)
			if err != nil {
				b.Fatal(err)
			}
			state.syncFile = func(string) error { return nil }
			state.syncDir = func(string) error { return nil }
			for i := 0; i < m; i++ {
				if err := state.Mark(fmt.Sprintf("seed-%d", i)); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := state.Mark(fmt.Sprintf("mark-%d", i)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// AC-4 (REQ-1/REQ-2): daemon and -once writers run as separate PROCESSES
// against one state path; the file stays decodable and holds the union of
// every child's marks. Uses the standard helper-process pattern: the test
// binary re-executes itself as N children (inheriting -race when the parent
// runs under it), each simulating one replay round (load → Mark → exit).
func TestReplayStateTwoProcessConcurrentRounds(t *testing.T) {
	if os.Getenv("REPLAY_STATE_HELPER") == "1" {
		runReplayStateHelper()
		return
	}
	path := filepath.Join(t.TempDir(), "state.json")
	errs := make(chan error, replayConcurrentWriters)
	var wg sync.WaitGroup
	for w := 0; w < replayConcurrentWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cmd := exec.Command(os.Args[0], "-test.run=^TestReplayStateTwoProcessConcurrentRounds$")
			cmd.Env = append(os.Environ(),
				"REPLAY_STATE_HELPER=1",
				"REPLAY_STATE_PATH="+path,
				fmt.Sprintf("REPLAY_STATE_WRITER=%d", w),
			)
			out, err := cmd.CombinedOutput()
			if err != nil {
				errs <- fmt.Errorf("helper %d: %v\n%s", w, err, out)
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	assertReplayUnion(t, path)
}

// runReplayStateHelper is the child side of the two-process test: load the
// shared state, mark one writer's distinct IDs plus the shared core, exit.
func runReplayStateHelper() {
	path := os.Getenv("REPLAY_STATE_PATH")
	writer, err := strconv.Atoi(os.Getenv("REPLAY_STATE_WRITER"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad writer:", err)
		os.Exit(1)
	}
	state, err := LoadReplayState(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "load:", err)
		os.Exit(1)
	}
	for i := 0; i < replayConcurrentPerWriter; i++ {
		if err := state.Mark(fmt.Sprintf("w%d-e%d", writer, i)); err != nil {
			fmt.Fprintln(os.Stderr, "mark:", err)
			os.Exit(1)
		}
	}
	for i := 0; i < replayConcurrentShared; i++ {
		if err := state.Mark(fmt.Sprintf("core-%d", i)); err != nil {
			fmt.Fprintln(os.Stderr, "mark core:", err)
			os.Exit(1)
		}
	}
}

// REQ-1: a lock-acquisition failure is a persist error — the round aborts,
// the previous state stays authoritative, nothing is marked in memory
// (security F1), and no temp or lock artifact is left behind. flock
// auto-releases on process death, so the only failure classes are the open
// failing (ENOTDIR etc.) and a contended lock timing out (a hung-but-alive
// peer cannot stall recovery forever).
func TestReplayStateLockFailureAbortsPersist(t *testing.T) {
	t.Run("lock path under a regular file fails with a wrapped error", func(t *testing.T) {
		dir := t.TempDir()
		blocker := filepath.Join(dir, "blocker")
		if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		state := &ReplayState{Replayed: map[string]bool{}, path: filepath.Join(blocker, "state.json")}
		err := state.Mark("evt-1")
		if err == nil {
			t.Fatal("Mark must fail when the lock cannot be opened")
		}
		if !strings.Contains(err.Error(), "replay state lock") {
			t.Fatalf("error must wrap the lock failure: %v", err)
		}
		if state.Replayed["evt-1"] {
			t.Fatal("in-memory mark must not be set when the persist failed (security F1)")
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".tmp") || strings.HasSuffix(entry.Name(), ".lock") {
				t.Fatalf("artifact left behind after lock failure: %s", entry.Name())
			}
		}
	})
	t.Run("contended lock times out with a wrapped error", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		state, err := LoadReplayState(path)
		if err != nil {
			t.Fatal(err)
		}
		lock, err := lockReplayState(path, 0) // hold the flock in this process
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Close()
		state.lockWait = 30 * time.Millisecond
		err = state.Mark("evt-1")
		if err == nil {
			t.Fatal("Mark must fail while another writer holds the lock")
		}
		if !strings.Contains(err.Error(), "timed out") {
			t.Fatalf("error must report the lock timeout: %v", err)
		}
		if state.Replayed["evt-1"] {
			t.Fatal("in-memory mark must not be set when the persist failed (security F1)")
		}
	})
}
