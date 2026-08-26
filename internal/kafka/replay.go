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
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/fsutil"
	"github.com/snaplink/audit-governance/internal/outbox"
)

// RepublishFunc redelivers one recovered event. Implementations write the
// original accepted-topic message back (Producer.Republish) or ingest it
// through the audit API (outbox.HTTPDeliverer).
type RepublishFunc func(ctx context.Context, key, value []byte) error

// ReplayState is the persisted set of dead-letter event IDs that have
// already been replayed, so a re-scan of the accepted topic never replays an
// event twice. The file is an append-only JSONL log
// ({"format":"replay-log-v1"} header + one {"event_id":"..."} line per
// mark), so one Mark costs O(1) regardless of the accumulated set size, and
// concurrent writers (daemon + cron -once sharing the state path) serialize
// on an exclusive advisory flock and can never lose a committed mark
// (union by accumulation — no writer ever overwrites another's lines).
type ReplayState struct {
	// Replayed is the legacy event-ID view kept for source compatibility with
	// older embedders and diagnostics. Tenant-aware replay never consults this
	// map for authorization or correlation; it uses scopedMarks instead.
	Replayed map[string]bool `json:"replayed"`
	// scopedMarks is keyed by an injective length-framed (tenant_id,event_id)
	// identity. It is intentionally unexported so callers cannot accidentally
	// treat the compatibility event-ID view as tenant-safe state.
	scopedMarks map[string]bool
	// legacyMarks contains event-ID-only marks loaded from v1 state. They are
	// honored by tenant-aware replay only after an operator explicitly proves
	// the old file was single-tenant with SetLegacyTenantScope.
	legacyMarks       map[string]bool
	legacyTenantScope string
	path              string
	// mu serializes the in-memory map mutation and the persist critical
	// section, making concurrent Mark calls on one instance race-free
	// (REQ-8); separate instances sharing a path are serialized by the
	// flock (REQ-1).
	mu sync.Mutex
	// syncFile/syncDir are the durability hooks used by Mark (defaults:
	// fsutil.SyncFile / fsutil.SyncDir). They are unexported and never
	// marshaled, so the persisted bytes stay stable; same-package tests
	// replace them to record the ordered sync set or fault-inject a step
	// (mirrors archive.FileStore.syncDir / store.fileBackend.syncDir).
	syncFile func(path string) error
	syncDir  func(path string) error
	// newTemp is the unique-temp factory for the rewrite paths (default
	// os.CreateTemp); same-package tests replace it to record every temp
	// path (AC-2: no two writers ever share one temp inode, REQ-3).
	newTemp func(dir, pattern string) (*os.File, error)
	// lockWait bounds how long Mark waits for a contended flock before
	// failing the persist (a hung-but-alive peer cannot stall DLQ recovery
	// forever); defaults to defaultLockWait.
	lockWait time.Duration
}

// On-disk log format. The header discriminates the append-only JSONL log
// from the legacy single-object {"replayed":{...}} format, which keeps
// loading (REQ-6(a)). The header and every mark line are immutable and
// self-contained: a mark is one {"event_id":"<id>"}\n line appended under
// the flock, so no writer ever overwrites another's committed marks
// (last-writer-wins is impossible by construction).
const (
	replayLogFormat     = "replay-log-v1"
	replayLogHeader     = `{"format":"replay-log-v1"}` + "\n"
	replayLogHeaderLine = `{"format":"replay-log-v1"}`
	replayLogPrefix     = `{"format":"replay-log-v1` // torn-header detector (no closing brace)
	classifyReadCap     = 512                        // first-line probe; header is ~30 bytes
	tailCheckWindow     = 4096                       // torn-tail probe window
	defaultLockWait     = 2 * time.Second
)

// stateKind classifies the on-disk state file before a write. Only the
// first line is probed (bounded read, O(1) in the steady state).
type stateKind int

const (
	stateAbsent    stateKind = iota // file does not exist
	stateEmpty                      // size 0
	stateLog                        // first line == the replay-log header
	stateTorn                       // first line is a header prefix but not exact (never-fsynced first write)
	stateLegacy                     // legacy {"replayed":{...}} single object
	stateTenantLog                  // tenant-aware replay log v2
)

// LoadReplayState reads the state file. A missing, zero-length, or
// whitespace-only file starts an empty set. A replay-log file (first line
// exactly {"format":"replay-log-v1"}) is scanned line by line: the header
// is dropped, a malformed TRAILING line is skipped (a torn append fragment
// is never a mark and never a decode failure — REQ-6(c)), and a malformed
// NON-last line is genuine corruption → wrapped decode error (REQ-6(d),
// fail-loud). Any other content goes through the legacy single-object
// decode, byte-identical to the pre-log behavior (REQ-6(a)); a torn
// never-fsynced first write fails that decode loudly, exactly like today's
// torn-target pin. Temp and lock files are never read, parsed, or deleted.
func LoadReplayState(path string) (*ReplayState, error) {
	state := &ReplayState{Replayed: map[string]bool{}, scopedMarks: map[string]bool{}, legacyMarks: map[string]bool{}, path: path}
	if path == "" {
		return state, nil
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return state, nil
		}
		return nil, fmt.Errorf("load replay state: %w", err)
	}
	if len(bytes.TrimSpace(encoded)) == 0 {
		return state, nil
	}
	kind, err := classifyFirstLine(firstLine(encoded))
	if err != nil {
		return nil, err
	}
	if kind == stateLog {
		marks, err := parseLogMarks(encoded)
		if err != nil {
			return nil, fmt.Errorf("decode replay state: %w", err)
		}
		for id := range marks {
			state.Replayed[id] = true
			state.legacyMarks[id] = true
		}
		return state, nil
	}
	if kind == stateTenantLog {
		scoped, legacy, err := parseTenantLogMarks(encoded)
		if err != nil {
			return nil, fmt.Errorf("decode replay state: %w", err)
		}
		for identity := range scoped {
			state.scopedMarks[identity] = true
			if eventID := replayEventIDFromIdentity(identity); eventID != "" {
				state.Replayed[eventID] = true
			}
		}
		for id := range legacy {
			state.Replayed[id] = true
			state.legacyMarks[id] = true
		}
		return state, nil
	}
	if err := json.Unmarshal(encoded, state); err != nil {
		return nil, fmt.Errorf("decode replay state: %w", err)
	}
	if state.Replayed == nil {
		state.Replayed = map[string]bool{}
	}
	for id := range state.Replayed {
		state.legacyMarks[id] = true
	}
	return state, nil
}

// firstLine returns the first line of encoded, capped at classifyReadCap.
func firstLine(encoded []byte) []byte {
	if idx := bytes.IndexByte(encoded, '\n'); idx >= 0 && idx < classifyReadCap {
		return encoded[:idx]
	}
	if len(encoded) > classifyReadCap {
		return encoded[:classifyReadCap]
	}
	return encoded
}

// classifyFirstLine decides the state kind from the file's first line.
// JSON-aware (protocol F-1): a first line that parses as JSON with a
// "format" member is NEVER a legacy file — an unknown format or a malformed
// v1 header fails loudly instead of silently decoding as an empty legacy
// state and losing marks on the next rewrite.
func classifyFirstLine(first []byte) (stateKind, error) {
	line := first
	if idx := bytes.IndexByte(line, '\n'); idx >= 0 {
		line = line[:idx]
	}
	line = bytes.TrimRight(line, "\r")
	if bytes.Equal(line, []byte(replayLogHeaderLine)) {
		return stateLog, nil
	}
	if bytes.Equal(line, []byte(replayTenantLogHeaderLine)) {
		return stateTenantLog, nil
	}
	var probe struct {
		Format string `json:"format"`
	}
	if json.Unmarshal(line, &probe) == nil && probe.Format != "" {
		if probe.Format != replayLogFormat && probe.Format != replayTenantLogFormat {
			return 0, fmt.Errorf("unknown replay state format %q", probe.Format)
		}
		return 0, fmt.Errorf("invalid replay state header %q", line)
	}
	if bytes.HasPrefix(line, []byte(replayLogPrefix)) || bytes.HasPrefix(line, []byte(replayTenantLogPrefix)) {
		// Torn header from a never-fsynced first write: the line is a strict
		// prefix of the header (invalid JSON), so the loader fails loudly
		// via the legacy decode (FM-10) and the next Mark self-heals with a
		// full rewrite.
		return stateTorn, nil
	}
	return stateLegacy, nil
}

// classifyStateFile probes the state file under the flock (bounded read:
// stat + first classifyReadCap bytes), distinguishing absent/empty/log/
// torn/legacy for the write decision.
func classifyStateFile(path string) (stateKind, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return stateAbsent, nil
		}
		return 0, fmt.Errorf("stat replay state: %w", err)
	}
	if info.Size() == 0 {
		return stateEmpty, nil
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return 0, fmt.Errorf("open replay state: %w", err)
	}
	defer file.Close()
	buf := make([]byte, classifyReadCap)
	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("read replay state: %w", err)
	}
	return classifyFirstLine(buf[:n])
}

// lockReplayState takes an exclusive advisory flock on <path>.lock, created
// 0o600 next to the state file. The kernel releases the lock when the fd
// closes or the process dies, so no stale-lock detection is ever needed.
// LOCK_NB with a bounded retry (wait) keeps a hung-but-alive peer (e.g. a
// Mark blocked on a stuck disk fsync) from stalling DLQ recovery forever:
// the persist fails with a wrapped error and records stay pending for the
// next round. A lock-acquisition failure is a persist error (REQ-1): the
// round aborts and the previous state stays authoritative.
func lockReplayState(path string, wait time.Duration) (io.Closer, error) {
	file, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open replay state lock: %w", err)
	}
	deadline := time.Now().Add(wait)
	backoff := time.Millisecond
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			file.Close()
			return nil, fmt.Errorf("flock replay state lock: %w", err)
		}
		if time.Now().After(deadline) {
			file.Close()
			return nil, errors.New("flock replay state lock: timed out waiting for a peer writer")
		}
		time.Sleep(backoff)
		if backoff < 10*time.Millisecond {
			backoff *= 2
		}
	}
}

// markLine renders one immutable, self-contained log line for a mark.
func markLine(eventID string) ([]byte, error) {
	line, err := json.Marshal(struct {
		EventID string `json:"event_id"`
	}{eventID})
	if err != nil {
		return nil, fmt.Errorf("marshal replay mark: %w", err)
	}
	return append(line, '\n'), nil
}

// Mark appends one event ID to the replayed set and persists it under the
// exclusive advisory flock (REQ-1): a log file gets one O(1) O_APPEND line
// + file fsync (REQ-4); a fresh/torn/legacy file gets a full rewrite via a
// UNIQUE temp + rename + dir fsync (first write or one-time legacy
// conversion). A failed persist is returned so the caller can keep the
// event pending instead of losing the record.
//
// The in-memory set is updated only AFTER a successful persist (amended
// REQ-7, security F1): if the durable ledger never held the mark, the DLQ
// record stays pending and the next round retries it — a DLQ offset is
// never committed without the corresponding durable mark.
func (s *ReplayState) Mark(eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initializeReplayMaps()
	if s.path == "" {
		s.Replayed[eventID] = true
		s.legacyMarks[eventID] = true
		return nil
	}
	lockWait := s.lockWait
	if lockWait <= 0 {
		lockWait = defaultLockWait
	}
	lock, err := lockReplayState(s.path, lockWait)
	if err != nil {
		return err
	}
	defer lock.Close()
	line, err := markLine(eventID)
	if err != nil {
		return err
	}
	kind, err := classifyStateFile(s.path)
	if err != nil {
		return fmt.Errorf("inspect replay state: %w", err)
	}
	if kind == stateLog || kind == stateTenantLog {
		// The append WRITE is serialized by the flock (a concurrent rewrite
		// must never race an append into a dying inode); the durability
		// flush (file fsync) does not mutate the file and needs no mutual
		// exclusion, so it runs after the lock is released — the flock
		// serializes the read-modify-write (classify + append/rewrite), not
		// the flush, keeping the critical section microsecond-scale under
		// concurrent writers and making the bounded flock wait meaningful
		// (only a genuinely stuck peer can time it out). A Mark's fsync
		// flushes every line appended before it (same inode).
		if err := s.appendLogLine(line); err != nil {
			return err
		}
		lock.Close() // early release before the flush; the deferred Close is a no-op
		if err := s.syncFileOr(s.path); err != nil {
			return err
		}
		s.Replayed[eventID] = true
		s.legacyMarks[eventID] = true
		return nil
	}
	// Rewrite paths (first write / torn repair / legacy conversion) publish
	// via unique temp + fsync + rename + dir fsync, all under the flock:
	// the rename is the atomic publication and must never interleave with
	// another writer's classification.
	if err := s.rewriteFull(kind, line); err != nil {
		return err
	}
	s.Replayed[eventID] = true
	s.legacyMarks[eventID] = true
	return nil
}

// marked reports whether eventID is in the in-memory replayed set. The
// mutex makes concurrent readers race-free with concurrent Mark calls
// (REQ-8); the replayer calls it from the single round goroutine today.
func (s *ReplayState) marked(eventID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Replayed[eventID]
}

// appendLogLine appends one line to the log (called under the flock). A
// dirty tail (crash mid-append) is repaired FIRST, so a later append can
// never orphan a malformed non-last line. On a partial or failed write the
// append fails with a wrapped error; the torn tail is skipped by the
// loader's trailing-line rule and self-healed by the next Mark's repair.
// The O_APPEND single-write atomicity and the flock guarantee assume a
// LOCAL POSIX filesystem (the default ./data/): on NFS/SMB the append may
// tear beyond the last fsync — the torn-tail repair and loader skip remain
// the bounded fallback. The file fsync that makes the append durable runs
// in Mark AFTER the flock is released (it does not mutate the file).
func (s *ReplayState) appendLogLine(line []byte) error {
	if err := s.repairTornTail(); err != nil {
		return err
	}
	file, err := os.OpenFile(s.path, os.O_WRONLY|os.O_APPEND|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("open replay state for append: %w", err)
	}
	n, err := file.Write(line)
	if err != nil || n != len(line) {
		file.Close()
		if err == nil {
			err = io.ErrShortWrite
		}
		return fmt.Errorf("append replay state: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close replay state: %w", err)
	}
	return nil
}

// repairTornTail heals a crash-torn append tail before the next append: an
// O(1) probe of the last ≤4KB. If the file does not end with '\n', the
// fragment after the last '\n' in the window is truncated away (a torn
// append fragment is by construction shorter than one line); if the window
// contains no '\n' at all — a never-fsynced torn first write whose content
// was never durable, or external corruption of a whole block — the log is
// rebuilt from its durable marks via the unique-temp rewrite protocol.
func (s *ReplayState) repairTornTail() error {
	file, err := os.OpenFile(s.path, os.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open replay state for repair: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return fmt.Errorf("stat replay state: %w", err)
	}
	size := info.Size()
	if size == 0 {
		file.Close()
		return nil
	}
	window := size
	if window > tailCheckWindow {
		window = tailCheckWindow
	}
	tail := make([]byte, window)
	if _, err := file.ReadAt(tail, size-window); err != nil {
		file.Close()
		return fmt.Errorf("read replay state tail: %w", err)
	}
	if tail[window-1] == '\n' {
		file.Close()
		return nil // clean tail: no repair needed
	}
	if idx := bytes.LastIndexByte(tail[:window-1], '\n'); idx >= 0 {
		err = file.Truncate(size - window + int64(idx) + 1)
		file.Close()
		if err != nil {
			return fmt.Errorf("truncate replay state tail: %w", err)
		}
		return nil
	}
	file.Close()
	return s.rebuildLog()
}

// rebuildLog rewrites the log from its durable marks (header + sorted
// lines), dropping a torn/corrupt un-terminated tail, via the same
// unique-temp + fsync + rename + dir-fsync protocol as rewriteFull.
func (s *ReplayState) rebuildLog() error {
	if kind, err := classifyStateFile(s.path); err == nil && kind == stateTenantLog {
		return s.rebuildTenantLog()
	}
	encoded, err := os.ReadFile(s.path)
	if err != nil {
		return fmt.Errorf("read replay state: %w", err)
	}
	marks, err := parseLogMarks(encoded)
	if err != nil {
		return err
	}
	return s.rewriteBytes(appendLogBytes(marks, nil))
}

// rewriteFull publishes a fresh log (first write / torn repair / legacy
// conversion): the header, any legacy marks in sorted order, and the new
// line, via the durable protocol — unique temp (newTemp seam), write,
// content fsync, rename, parent-dir fsync (REQ-4). The on-disk state is
// read under the flock, so a stale boot-time view can never drop another
// writer's committed marks (REQ-2).
func (s *ReplayState) rewriteFull(kind stateKind, newLine []byte) error {
	var content []byte
	switch kind {
	case stateAbsent, stateEmpty, stateTorn:
		// Fresh target or a never-fsynced torn first write (its marks were
		// never durable): the new log starts with the header and the mark.
		content = appendLogBytes(nil, newLine)
	case stateLegacy:
		marks, err := loadLegacyMarks(s.path)
		if err != nil {
			return err
		}
		content = appendLogBytes(marks, newLine)
	default:
		return fmt.Errorf("rewrite replay state: unexpected state kind %d", kind)
	}
	return s.rewriteBytes(content)
}

// rewriteBytes publishes content through the durable protocol: a UNIQUE
// temp (newTemp seam — no two writers can ever share one temp inode,
// REQ-3), write, content fsync BEFORE rename, rename, parent-dir fsync
// AFTER rename (REQ-4). Every error path removes only the writer's own
// temp; a dir-sync failure after rename keeps the new target content (D4
// semantics: both old and new content decode).
func (s *ReplayState) rewriteBytes(content []byte) error {
	newTemp := s.newTemp
	if newTemp == nil {
		newTemp = os.CreateTemp
	}
	temp, err := newTemp(filepath.Dir(s.path), filepath.Base(s.path)+"-*.tmp")
	if err != nil {
		return fmt.Errorf("create replay state temp: %w", err)
	}
	tempPath := temp.Name()
	defer func() {
		if tempPath != "" {
			_ = os.Remove(tempPath)
		}
	}()
	if _, err := temp.Write(content); err != nil {
		temp.Close()
		return fmt.Errorf("write replay state: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close replay state temp: %w", err)
	}
	if err := s.syncFileOr(tempPath); err != nil {
		return err
	}
	if err := os.Rename(tempPath, s.path); err != nil {
		return fmt.Errorf("rename replay state: %w", err)
	}
	tempPath = "" // rename succeeded: nothing left to remove
	syncDir := s.syncDir
	if syncDir == nil {
		syncDir = fsutil.SyncDir
	}
	if err := syncDir(filepath.Dir(s.path)); err != nil {
		return fmt.Errorf("sync replay state directory: %w", err)
	}
	return nil
}

// syncFileOr runs the syncFile hook (default fsutil.SyncFile): the content
// fsync that makes a completed append (or rewritten temp) durable before
// Mark reports success (REQ-4).
func (s *ReplayState) syncFileOr(path string) error {
	syncFile := s.syncFile
	if syncFile == nil {
		syncFile = fsutil.SyncFile
	}
	if err := syncFile(path); err != nil {
		return fmt.Errorf("sync replay state: %w", err)
	}
	return nil
}

// appendLogBytes renders the log content: the header, the given marks in
// sorted order (deterministic golden bytes), and the new line (may be nil).
func appendLogBytes(marks map[string]bool, newLine []byte) []byte {
	ids := make([]string, 0, len(marks)+1)
	for id := range marks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	buf := make([]byte, 0, len(replayLogHeader)+(len(ids)+1)*40)
	buf = append(buf, replayLogHeader...)
	for _, id := range ids {
		line, _ := markLine(id) // string-only struct: Marshal cannot fail
		buf = append(buf, line...)
	}
	buf = append(buf, newLine...)
	return buf
}

// loadLegacyMarks decodes a legacy {"replayed":{...}} single-object file
// (REQ-6(a)) for the in-place conversion rewrite. Missing/empty/whitespace
// targets yield an empty set; genuinely undecodable content is a wrapped
// decode error (fail-loud, FM-7).
func loadLegacyMarks(path string) (map[string]bool, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]bool{}, nil
		}
		return nil, fmt.Errorf("load replay state: %w", err)
	}
	if len(bytes.TrimSpace(encoded)) == 0 {
		return map[string]bool{}, nil
	}
	var legacy ReplayState
	if err := json.Unmarshal(encoded, &legacy); err != nil {
		return nil, fmt.Errorf("decode replay state: %w", err)
	}
	if legacy.Replayed == nil {
		return map[string]bool{}, nil
	}
	return legacy.Replayed, nil
}

// parseLogMarks scans replay-log content into the mark set. The header line
// is dropped. A file that does not end with '\n' has a potentially-torn
// trailing segment (REQ-6(c)): it is never a mark, even when it parses — a
// line is only committed when its terminating '\n' was written. A malformed
// NON-last line is genuine corruption → wrapped decode error (REQ-6(d),
// fail-loud). Only lines decoding with a non-empty event_id are marks.
func parseLogMarks(encoded []byte) (map[string]bool, error) {
	marks := map[string]bool{}
	tornTail := len(encoded) > 0 && encoded[len(encoded)-1] != '\n'
	segments := bytes.Split(encoded, []byte{'\n'})
	last := len(segments) - 1
	for i, seg := range segments {
		if i == last && tornTail {
			continue // potentially-torn trailing segment: never a mark
		}
		trimmed := bytes.TrimRight(seg, "\r")
		if i == 0 && bytes.Equal(trimmed, []byte(replayLogHeaderLine)) {
			continue
		}
		if len(trimmed) == 0 {
			continue // blank line (trailing newline)
		}
		var mark struct {
			EventID string `json:"event_id"`
		}
		if err := json.Unmarshal(seg, &mark); err != nil || mark.EventID == "" {
			if i == last {
				continue // torn trailing line: skipped, never an error
			}
			return nil, fmt.Errorf("malformed replay log line %d", i+1)
		}
		marks[mark.EventID] = true
	}
	return marks, nil
}

// defaultDrainTimeout bounds one RunOnce round: Kafka readers block on
// FetchMessage, so "drain the topic" is expressed as "fetch until no
// message arrives within this window".
const defaultDrainTimeout = 5 * time.Second

// maxScanWindows bounds one scanAccepted round under sustained ingest: a
// topic that never goes quiet must not hold the round open. Two windows
// give a slow topic at least one full quiet-window chance to signal
// "end of retained topic" before the round is cut off.
const maxScanWindows = 2

// Replayer redelivers dead-lettered events. DLQ records carry only failure
// metadata (event_id, error_code, error_message), so the original event is
// recovered from the accepted topic by key and re-published (or re-ingested
// through the API). The accepted topic is always scanned from the first
// retained offset: every round recreates the accepted reader with
// StartOffset: kafka.FirstOffset (kafka-go honors StartOffset only at
// reader creation and rejects SetOffset for group readers), so no round
// reuses the previous round's end position. The DLQ topic is always drained
// from the group's committed offset: every round also recreates the DLQ
// reader, because kafka-go never re-delivers a fetched record within one
// group session (the fetch position advances past every FetchMessage,
// committed or not) — recreation is the only mechanism that lets a
// transiently-failed record be retried by a later round. The per-partition
// commit barrier in commitResolved guarantees the group's committed offset
// never crosses a still-pending record, so a recreated session always
// re-delivers the pending records. Replayed IDs in the state file make the
// re-scan idempotent, and committing the accepted scan position would make
// new DLQ records miss older original messages.
type Replayer struct {
	dlqReader   messageReader
	dlqNew      func() messageReader // per-round DLQ reader recreation (F1)
	accepted    messageReader
	acceptedNew func() messageReader // per-round accepted reader recreation (REQ-1)
	republish   RepublishFunc
	state       *ReplayState
	// tenantAware is true for production replayers. The legacy test seam is
	// deliberately kept false so old byte-compatible event-ID fixtures remain
	// useful; production Kafka and API modes both use canonical tenant state.
	tenantAware  bool
	logger       *log.Logger
	drainTimeout time.Duration
	dlqRecords   atomic.Uint64
	acceptedSeen atomic.Uint64
	// replayed counts only successful re-publishes (resolution path a).
	// Every other first-resolution path has its own counter below, so the
	// metric never conflates a replay with a convergence (REQ-1).
	replayed atomic.Uint64
	// permanent counts permanent closures — durable first resolutions where
	// a record is closed WITHOUT replay, under two policies: (1) the one-shot
	// closure of an error_code=permanent_error record whose single republish
	// attempt failed for any reason (transient or permanent; REQ-PERM-1),
	// and (2) the legacy live-rejection closure of any other record whose
	// republish was rejected with DeliveryError.Permanent. Both converge
	// the round and are alertable via republishFail, but never a replay
	// (REQ-PERM-2).
	permanent atomic.Uint64
	// attemptsExhausted counts collected DLQ records with
	// error_code=attempts_exhausted: per-round collection-time increments
	// mirroring dlqRecords semantics — a traffic counter, not a resolution
	// counter (re-delivered records count again per round; REQ-PERM-3).
	attemptsExhausted atomic.Uint64
	// unresolvable counts drained-unresolvable events (path d): the original
	// is absent from the accepted topic (retention expiry or recreation),
	// the DLQ offset is committed, and the event is dropped — a permanent
	// loss signal surfaced by the AuditDLQUnresolvableDrop alert.
	unresolvable atomic.Uint64
	// unparsableMarks counts anti-loop marks for key-matched accepted
	// messages whose value carries no canonical event_id (path b): neither
	// a replay nor a loss, but a first resolution that must stay observable.
	unparsableMarks atomic.Uint64
	republishFail   atomic.Uint64
	pending         atomic.Uint64
	// authBlocked counts error_code=unauthorized DLQ records held blocked
	// (per-round increments, mirroring dlqRecords semantics): a counter that
	// never falls, exported as audit_dlq_auth_blocked_total.
	authBlocked atomic.Uint64
	// authBlockedPending is the gauge of blocked records collected by the
	// LATEST round (audit_dlq_auth_blocked): it reflects the current blocked
	// backlog, falls to 0 once a round collects none (drained or flag
	// re-admitted them), and is the only signal for a STATIC backlog —
	// blocked records are excluded from the wanted set, so audit_dlq_pending
	// stays 0 in blocked-only rounds.
	authBlockedPending atomic.Uint64
	// replayAuthBlocked gates the block: default off (= the operator has not
	// acknowledged the credential is fixed) holds unauthorized records
	// pending; on, they are re-admitted to the wanted set and replay normally.
	replayAuthBlocked atomic.Bool
	// authBlockedDetailBudget is the per-round budget for the per-record
	// auth-blocked detail log lines (H1: the full backlog must never flood
	// the log); the round summary line always reports the total.
	authBlockedDetailBudget atomic.Uint64
	// tenantScopeMismatches counts exact API tenant_mismatch responses and
	// resolver/configuration scope failures. tenantScopePending is the latest
	// round's tenant-safety backlog gauge; neither path marks or commits.
	tenantScopeMismatches atomic.Uint64
	tenantScopePending    atomic.Uint64
}

// NewReplayer creates the DLQ and accepted-topic readers. The DLQ reader
// commits offsets only for durable decisions (committed once resolved, with
// the commitResolved barrier); the accepted reader never commits and always
// starts at the first retained offset — because StartOffset is honored only
// at reader creation, RunOnce recreates the accepted reader at the start of
// every round (resetAcceptedToFirstOffset), including the first round (the
// eagerly-created reader is replaced before its first fetch; kafka-go joins
// the group lazily, so this costs nothing). RunOnce recreates the DLQ reader
// as well (resetDLQToCommittedOffset), so each round drains from the group's
// committed offset and transiently-failed records are re-delivered.
func NewReplayer(brokers []string, dlqTopic, acceptedTopic, groupID string, state *ReplayState, republish RepublishFunc, logger *log.Logger) *Replayer {
	if dlqTopic == "" {
		dlqTopic = TopicDLQ
	}
	if acceptedTopic == "" {
		acceptedTopic = TopicAccepted
	}
	if groupID == "" {
		groupID = "audit-dlq-replay"
	}
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	dlqConfig := kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          dlqTopic,
		GroupID:        groupID + "-dlq",
		MinBytes:       1,
		MaxBytes:       1 << 20,
		CommitInterval: 0, // manual commits only
		StartOffset:    kafka.FirstOffset,
	}
	dlqReader := kafka.NewReader(dlqConfig)
	acceptedConfig := kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          acceptedTopic,
		GroupID:        groupID + "-accepted",
		MinBytes:       1,
		MaxBytes:       1 << 20,
		CommitInterval: 0, // never commit: every round re-scans from the start
		StartOffset:    kafka.FirstOffset,
	}
	acceptedReader := kafka.NewReader(acceptedConfig)
	return &Replayer{dlqReader: dlqReader, dlqNew: func() messageReader { return kafka.NewReader(dlqConfig) }, accepted: acceptedReader, acceptedNew: func() messageReader { return kafka.NewReader(acceptedConfig) }, republish: republish, state: state, tenantAware: true, logger: logger, drainTimeout: defaultDrainTimeout}
}

// newReplayerWithReaders is the test seam: the same messageReader contract
// as Consumer, so replay behavior is exercised without a broker.
func newReplayerWithReaders(dlq, accepted messageReader, state *ReplayState, republish RepublishFunc) *Replayer {
	return newReplayerWithReadersAndLogger(dlq, accepted, state, republish, log.New(io.Discard, "", 0))
}

// newReplayerWithReadersAndLogger is the test seam for acceptance tests that
// capture the durable log lines (e.g. the unresolvable mark) in addition to
// reader-level behavior.
func newReplayerWithReadersAndLogger(dlq, accepted messageReader, state *ReplayState, republish RepublishFunc, logger *log.Logger) *Replayer {
	// dlqNew/acceptedNew return the same injected fakes: rounds keep the
	// fakes' positions unless a test replaces them (existing retry tests
	// reset dlq.index/accepted.index manually to simulate redelivery).
	return &Replayer{dlqReader: dlq, dlqNew: func() messageReader { return dlq }, accepted: accepted, acceptedNew: func() messageReader { return accepted }, republish: republish, state: state, logger: logger, drainTimeout: defaultDrainTimeout}
}

// newReplayerWithAcceptedFactory is the REQ-1 test seam: a factory that
// returns a FRESH accepted reader per round models the production per-round
// recreation (a fresh reader joins with no committed offset and starts at the
// first retained offset), which is the mechanism under test. The initial
// reader is created eagerly, mirroring NewReplayer.
func newReplayerWithAcceptedFactory(dlq messageReader, acceptedNew func() messageReader, state *ReplayState, republish RepublishFunc, logger *log.Logger) *Replayer {
	return &Replayer{dlqReader: dlq, dlqNew: func() messageReader { return dlq }, accepted: acceptedNew(), acceptedNew: acceptedNew, republish: republish, state: state, logger: logger, drainTimeout: defaultDrainTimeout}
}

// newReplayerWithFactories is the F1 test seam: factories that hand out a
// FRESH reader per round for BOTH the DLQ and accepted sides, modeling the
// production per-round recreation. The initial readers are created eagerly,
// mirroring NewReplayer.
func newReplayerWithFactories(dlqNew, acceptedNew func() messageReader, state *ReplayState, republish RepublishFunc, logger *log.Logger) *Replayer {
	return &Replayer{dlqReader: dlqNew(), dlqNew: dlqNew, accepted: acceptedNew(), acceptedNew: acceptedNew, republish: republish, state: state, logger: logger, drainTimeout: defaultDrainTimeout}
}

// maxAuthBlockedLogDetails bounds the per-round per-record auth-blocked log
// detail lines. The blocked backlog can reach millions of records (the exact
// incident scenario: a rotated token dead-letters the whole accepted stream);
// logging every record every round would flood the log at ~3k lines/s. The
// first records of each round get a detail line, and the round summary
// always reports the total (H1).
const maxAuthBlockedLogDetails = 10

// SetReplayAuthBlocked re-admits error_code=unauthorized DLQ records to the
// replay set. Call only after the ingest credential is verified working;
// while the credential is still broken each round re-publishes once and the
// consumer re-dead-letters (visible via AuditDLQTraffic and
// AuditDLQAuthBlocked).
func (r *Replayer) SetReplayAuthBlocked(enabled bool) {
	r.replayAuthBlocked.Store(enabled)
}

// authBlockedCount reports how many collected records are held auth-blocked.
func authBlockedCount(collected []dlqRecord) int {
	blocked := 0
	for _, record := range collected {
		if record.authBlocked {
			blocked++
		}
	}
	return blocked
}

// sanitizeLogField strips control characters (log-forging defense, CWE-117:
// an attacker-influenced DLQ field must never inject lines into the replay
// log) and truncates the value to limit runes, bounding log growth. Used for
// attacker-influenced fields echoed by the auth-blocked log line.
func sanitizeLogField(value string, limit int) string {
	sanitized := strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '?'
		}
		return r
	}, value)
	runes := []rune(sanitized)
	if len(runes) > limit {
		runes = runes[:limit]
	}
	return string(runes)
}

// ReplayerMetrics is the /metrics snapshot of the replay counters. Every DLQ
// event that reaches a durable first resolution increments exactly one of
// Replayed / UnparsableMarks / Permanent / Unresolvable; a transient
// republish failure of a non-permanent-code record increments none of them
// (the record stays pending for the next round). Permanent counts closures
// under two policies: (1) the one-shot — a wanted error_code=permanent_error
// record whose single republish attempt failed for ANY reason (REQ-PERM-1);
// (2) the legacy live rejection — a non-permanent-code record whose
// republish was rejected with DeliveryError.Permanent. Both are durable
// closures, never a replay. AttemptsExhausted is observational
// (collection-time, per round) and explicitly excluded from the resolution
// invariant: re-delivered records count again per round (REQ-PERM-3).
// AuthBlocked increments per round a blocked record is re-collected;
// AuthBlockedPending is the latest-round blocked backlog gauge.
type ReplayerMetrics struct {
	DLQRecords            uint64 // DLQ Failure records collected
	AcceptedScanned       uint64 // accepted-topic messages scanned
	Replayed              uint64 // successful re-publishes only
	Permanent             uint64 // permanent closures (one-shot + legacy live rejection)
	Unresolvable          uint64 // drained-unresolvable drops (permanent loss)
	UnparsableMarks       uint64 // anti-loop marks for key-matched unparsable messages
	AttemptsExhausted     uint64 // error_code=attempts_exhausted records collected (per-round traffic, NOT a resolution)
	RepublishFailures     uint64 // transient + permanent republish failures (inclusive)
	Pending               uint64 // wanted events pending in the current round
	AuthBlocked           uint64 // error_code=unauthorized records held blocked (per-round increments)
	AuthBlockedPending    uint64 // blocked records collected by the latest round (gauge)
	TenantScopeMismatches uint64 // resolver/API scope failures (counter)
	TenantScopePending    uint64 // tenant-safety backlog in the latest round (gauge)
}

// Metrics returns the replay resolution counters for the /metrics endpoint.
func (r *Replayer) Metrics() ReplayerMetrics {
	return ReplayerMetrics{
		DLQRecords:            r.dlqRecords.Load(),
		AcceptedScanned:       r.acceptedSeen.Load(),
		Replayed:              r.replayed.Load(),
		Permanent:             r.permanent.Load(),
		Unresolvable:          r.unresolvable.Load(),
		UnparsableMarks:       r.unparsableMarks.Load(),
		AttemptsExhausted:     r.attemptsExhausted.Load(),
		RepublishFailures:     r.republishFail.Load(),
		Pending:               r.pending.Load(),
		AuthBlocked:           r.authBlocked.Load(),
		AuthBlockedPending:    r.authBlockedPending.Load(),
		TenantScopeMismatches: r.tenantScopeMismatches.Load(),
		TenantScopePending:    r.tenantScopePending.Load(),
	}
}

// RunOnce performs one replay round: drain the currently available DLQ
// failures, recover the matching original events from the accepted topic and
// re-publish them. Draining is time-bounded (drainTimeout of quiet topic
// time); returns the number of events replayed this round. The DLQ reader is
// recreated at the start of every round (resetDLQToCommittedOffset) so the
// round starts at the broker's committed offset and transiently-failed
// records are re-delivered; the accepted reader is recreated before every
// scan (resetAcceptedToFirstOffset) so the round always begins at the first
// retained offset, never at the previous round's end position.
//
// DLQ offsets are committed per record, only for events that reached a
// durable decision (replayed, permanently rejected, unparsable, or already
// replayed in an earlier round), and only below the first still-pending
// record in each partition (the commitResolved barrier). A transient
// republish failure (or a crash mid-round) leaves that record uncommitted
// and never leapfrogged, so the next round's recreated DLQ session re-reads
// it and retries. Replayed IDs in the state file keep re-reads idempotent.
func (r *Replayer) RunOnce(ctx context.Context) (int, error) {
	// A scope backlog gauge describes the latest completed round, not the
	// previous round's stale value. The tenant-aware scan repopulates it after
	// a complete accepted-topic pass.
	r.tenantScopePending.Store(0)
	// F1: recreate the DLQ reader so this round drains from the broker's
	// committed offset, not from the previous round's session position.
	// kafka-go never re-delivers a fetched record within one group session
	// (the fetch position advances past every FetchMessage, committed or
	// not), so without recreation a transiently-failed record left
	// uncommitted by round N could never be retried by round N+1. The fresh
	// session joins the group and resumes at the committed offset, and the
	// commitResolved barrier guarantees that offset never crossed a pending
	// record — so every pending record IS re-delivered. A close failure
	// aborts the round: nothing is collected, marked, or committed.
	if err := r.resetDLQToCommittedOffset(); err != nil {
		return 0, err
	}
	// Each phase gets its own drain window: a shared window would let
	// collectFailures burn the whole budget waiting for the DLQ to go quiet,
	// leaving scanAccepted with an already-expired context (real kafka-go
	// returns ctx.Err() before any queued data, so the scan would replay
	// nothing).
	collected, err := r.collectFailures(ctx)
	if err != nil && !isDrained(ctx, err) {
		return 0, err
	}
	// Auth-blocked records (error_code=unauthorized, block enabled) are
	// excluded from the wanted set below but stay pending as commit
	// barriers. Surface them as the latest-round backlog gauge and one
	// bounded summary line per round (per-record detail lines are budgeted
	// in collectFailures, H1). The gauge is set even on the early path so a
	// blocked-only round is observable via audit_dlq_auth_blocked even
	// though audit_dlq_pending stays 0.
	r.authBlockedPending.Store(uint64(authBlockedCount(collected)))
	if blocked := r.authBlockedPending.Load(); blocked > 0 {
		r.logger.Printf("dlq round: %d auth-blocked records pending (fix AUDIT_OUTBOX_TOKEN before replay)", blocked)
	}
	wanted := wantedEvents(collected)
	if len(wanted.replay) == 0 {
		// Every collected record is already replayed (or unparsable):
		// consume them so the next round does not re-read the same offsets.
		if commitErr := r.commitResolved(ctx, collected, nil); commitErr != nil {
			return 0, commitErr
		}
		return 0, nil
	}
	// REQ-1: every round re-seeks the accepted scan to the first retained
	// offset. kafka-go honors StartOffset only at reader creation and
	// rejects SetOffset for group readers, so the accepted reader is
	// recreated per round; without this, round N+1 starts where round N
	// ended and older originals are silently marked unresolvable and dropped.
	if err := r.resetAcceptedToFirstOffset(); err != nil {
		return 0, err
	}
	// pending is set only while a scan is actually in flight, so an abort on
	// either reset path never leaks a stale audit_dlq_pending reading.
	r.pending.Store(uint64(len(wanted.replay)))
	replayed, resolved, scanErr := r.scanAccepted(ctx, wanted)
	r.pending.Store(0)
	if scanErr != nil && !isDrained(ctx, scanErr) {
		// Transport failure: the round did not complete. Leave every
		// collected record uncommitted so the next round retries.
		return replayed, scanErr
	}
	// Replay work is done (window expired or transport returned): commit
	// only the records that reached a durable decision; transient failures
	// stay pending for the next round. A commit failure is fatal: the
	// records stay pending and the next round re-reads them (state file
	// keeps replay idempotent).
	if commitErr := r.commitResolved(ctx, collected, resolved); commitErr != nil {
		return replayed, commitErr
	}
	return replayed, nil
}

// resetDLQToCommittedOffset replaces the DLQ reader so this round's
// collectFailures starts at the broker's committed offset for the group.
// Within one group session kafka-go never re-delivers a fetched message
// (the fetch position advances past every FetchMessage, committed or not),
// so a transiently-failed record left uncommitted at round N cannot be
// re-read by round N+1 unless the reader is recreated: the fresh session
// joins the group and resumes at the committed offset. The commitResolved
// barrier guarantees the committed offset never crossed a pending record,
// so every pending record is re-delivered. The accepted reader is
// untouched: its StartOffset: FirstOffset re-scan semantics are governed by
// resetAcceptedToFirstOffset. A close failure aborts the round so nothing
// is collected, marked, or committed (records stay pending for the next
// round).
func (r *Replayer) resetDLQToCommittedOffset() error {
	if r.dlqReader != nil {
		if err := r.dlqReader.Close(); err != nil {
			return fmt.Errorf("reset dlq reader: close: %w", err)
		}
	}
	r.dlqReader = r.dlqNew()
	return nil
}

// resetAcceptedToFirstOffset replaces the accepted reader with a fresh one
// so this round's scan starts at the first retained offset. kafka-go honors
// StartOffset only at reader creation ("when it finds a partition without a
// committed offset") and SetOffset returns errNotAvailableWithGroup for
// group readers, so recreation is the only seek mechanism for the
// group-bound accepted reader (groupID + "-accepted"). The fresh reader
// joins with no committed offset (the accepted reader never commits) and
// begins at the first retained offset. The DLQ reader is untouched: its
// group membership and manual commits keep "failures consumed once"
// semantics. A close failure aborts the round so nothing is marked or
// committed (records stay pending for the next round).
func (r *Replayer) resetAcceptedToFirstOffset() error {
	if r.accepted != nil {
		if err := r.accepted.Close(); err != nil {
			return fmt.Errorf("reset accepted reader: close: %w", err)
		}
	}
	r.accepted = r.acceptedNew()
	return nil
}

// drainWindow returns the configured drain timeout with the default fallback.
func (r *Replayer) drainWindow() time.Duration {
	if r.drainTimeout <= 0 {
		return defaultDrainTimeout
	}
	return r.drainTimeout
}

// dlqRecordID identifies one physical DLQ record. It deliberately contains
// only the effective topic, partition, and offset: neither an untrusted Kafka
// key nor the Failure event_id can collapse two records into one.
type dlqRecordID struct {
	topic     string
	partition int
	offset    int64
	// sequence is normally zero because Kafka guarantees a unique
	// (topic, partition, offset). It only disambiguates duplicate identities
	// supplied by broker-free test readers; production records never need it.
	sequence uint32
}

// dlqRecord is one collected DLQ failure with its parsed event metadata and
// physical record identity.
type dlqRecord struct {
	id              dlqRecordID
	message         kafka.Message
	eventID         string
	claimedTenantID string
	errorCode       string
	wanted          bool
	// authBlocked marks an error_code=unauthorized record held blocked this
	// round: excluded from the wanted set (scanAccepted and the
	// drain-to-unresolvable loop never touch it) yet kept wanted=true so the
	// commit barrier keeps its offset pending (never marked, never
	// committed, never resolvable) until the credential is fixed and
	// -replay-auth-blocked re-admits it.
	authBlocked bool
}

// wantedSet is the round's replay target set. replay, oneShot, orderedIDs,
// and commit decisions are all keyed by physical DLQ record identity.
// byEventID is only a lookup index for locating accepted-topic candidates; it
// is never used to authorize a mark or commit an offset. records retains the
// claim and event metadata needed by tenant-aware correlation.
type wantedSet struct {
	replay     map[dlqRecordID]bool
	oneShot    map[dlqRecordID]bool
	byEventID  map[string][]dlqRecordID
	orderedIDs []dlqRecordID
	records    map[dlqRecordID]dlqRecord
}

// wantedRecordID supplies deterministic identities for same-package tests
// that construct dlqRecord values directly. Kafka-collected records always
// receive their topic/partition/offset identity in collectFailures.
func wantedRecordID(record dlqRecord, index int) dlqRecordID {
	if record.id != (dlqRecordID{}) {
		return record.id
	}
	return dlqRecordID{partition: index, offset: int64(index)}
}

// wantedEvents returns the round's replay target records: collected records
// not yet marked replayed and not held auth-blocked, plus the per-record
// subset whose DLQ record carries error_code=permanent_error. error_code must
// never be echoed into a log line or metric label at the classification sites
// — the vocabulary is a free string from an untrusted boundary (CWE-117).
func wantedEvents(collected []dlqRecord) wantedSet {
	wanted := wantedSet{
		replay:     map[dlqRecordID]bool{},
		oneShot:    map[dlqRecordID]bool{},
		byEventID:  map[string][]dlqRecordID{},
		orderedIDs: []dlqRecordID{},
		records:    map[dlqRecordID]dlqRecord{},
	}
	for index, record := range collected {
		if !record.wanted || record.authBlocked {
			continue
		}
		id := wantedRecordID(record, index)
		if wanted.replay[id] {
			continue
		}
		record.id = id
		wanted.replay[id] = true
		wanted.records[id] = record
		wanted.byEventID[record.eventID] = append(wanted.byEventID[record.eventID], id)
		wanted.orderedIDs = append(wanted.orderedIDs, id)
		if record.errorCode == ErrorCodePermanentError {
			wanted.oneShot[id] = true
		}
	}
	return wanted
}

// resolveLegacyEventRecords bridges the historical event-ID replay seam to
// the physical commit set. Legacy mode may still share one state mark for
// duplicate event IDs, but every collected DLQ record gets its own durable
// commit decision.
func resolveLegacyEventRecords(wanted wantedSet, eventID string, resolved map[dlqRecordID]bool) {
	for _, id := range wanted.byEventID[eventID] {
		resolved[id] = true
	}
}

func legacyEventResolved(wanted wantedSet, eventID string, resolved map[dlqRecordID]bool) bool {
	ids := wanted.byEventID[eventID]
	if len(ids) == 0 {
		return false
	}
	for _, id := range ids {
		if !resolved[id] {
			return false
		}
	}
	return true
}

func legacyEventOneShot(wanted wantedSet, eventID string) bool {
	for _, id := range wanted.byEventID[eventID] {
		if wanted.oneShot[id] {
			return true
		}
	}
	return false
}

// commitResolved advances the DLQ reader past every collected record whose
// event reached a durable decision (record.wanted false — already replayed
// or unparsable at collect time — or present in resolved this round) AND
// whose offset is below the first still-pending record in its partition.
//
// Kafka consumer groups track ONE committed offset per partition, and
// kafka-go's CommitMessages commits offset+1 of the highest message passed
// (offsetStash.merge keeps the per-partition max), so committing a later
// record silently commits past every earlier record in the partition —
// including a pending one, which the next round would then never re-deliver
// (permanent loss). The barrier therefore leaves resolved records at or
// above the first pending offset uncommitted: the next round re-reads them
// and the state file makes the re-read idempotent (no re-republish).
func (r *Replayer) commitResolved(ctx context.Context, collected []dlqRecord, resolved map[dlqRecordID]bool) error {
	type partitionKey struct {
		topic     string
		partition int
	}
	// firstPending[partition] = lowest offset of a record that must stay
	// pending; no commit may cross it.
	firstPending := map[partitionKey]int64{}
	for _, record := range collected {
		id := record.id
		if id == (dlqRecordID{}) {
			id = dlqRecordID{topic: record.message.Topic, partition: record.message.Partition, offset: record.message.Offset}
		}
		if record.authBlocked || (record.wanted && !resolved[id]) {
			key := partitionKey{topic: id.topic, partition: id.partition}
			if offset, ok := firstPending[key]; !ok || id.offset < offset {
				firstPending[key] = id.offset
			}
		}
	}
	for _, record := range collected {
		id := record.id
		if id == (dlqRecordID{}) {
			id = dlqRecordID{topic: record.message.Topic, partition: record.message.Partition, offset: record.message.Offset}
		}
		if record.authBlocked || (record.wanted && !resolved[id]) {
			continue // auth-blocked: config-fixable, stays pending until the credential is restored (or -replay-auth-blocked)
		}
		key := partitionKey{topic: id.topic, partition: id.partition}
		if pending, ok := firstPending[key]; ok && id.offset >= pending {
			continue // resolved but at/above the barrier: committing would leapfrog the pending record
		}
		if err := r.dlqReader.CommitMessages(ctx, record.message); err != nil {
			return err
		}
	}
	return nil
}

// isDrained reports whether err is the drain window expiring (or the outer
// context being cancelled), as opposed to a transport failure.
func isDrained(ctx context.Context, err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || ctx.Err() != nil
}

// collectFailures drains the DLQ topic (until the drain window expires)
// WITHOUT committing offsets — records are committed by RunOnce only after
// the round's replay work, so transient republish failures or crashes leave
// them pending for the next round. Returns the held messages and the event
// IDs that are not yet marked replayed.
func (r *Replayer) collectFailures(ctx context.Context) ([]dlqRecord, error) {
	drainCtx, cancel := context.WithTimeout(ctx, r.drainWindow())
	defer cancel()
	// Per-round budget for the per-record auth-blocked detail log lines:
	// the first maxAuthBlockedLogDetails blocked records of THIS round get a
	// detail line; every round still counts and summarizes the total.
	r.authBlockedDetailBudget.Store(0)
	var collected []dlqRecord
	seenPhysical := map[dlqRecordID]uint32{}
	for {
		message, err := r.dlqReader.FetchMessage(drainCtx)
		if err != nil {
			return collected, err
		}
		effectiveTopic := message.Topic
		if effectiveTopic == "" {
			effectiveTopic = r.dlqReader.Config().Topic
		}
		physicalID := dlqRecordID{topic: effectiveTopic, partition: message.Partition, offset: message.Offset}
		if sequence := seenPhysical[physicalID]; sequence > 0 {
			physicalID.sequence = sequence
		}
		seenPhysical[dlqRecordID{topic: effectiveTopic, partition: message.Partition, offset: message.Offset}]++
		record := dlqRecord{
			id:      physicalID,
			message: message,
			eventID: string(message.Key),
		}
		var failure Failure
		decoder := json.NewDecoder(bytes.NewReader(message.Value))
		decoder.UseNumber()
		decodeErr := decoder.Decode(&failure)
		if decodeErr == nil {
			r.dlqRecords.Add(1)
			record.eventID = failure.EventID
			record.claimedTenantID = failure.TenantID
			record.errorCode = failure.ErrorCode
			// REQ-PERM-3: per-round collection-time traffic counter for
			// error_code=attempts_exhausted records, mirroring dlqRecords
			// semantics (re-delivered records count again per round).
			if failure.ErrorCode == ErrorCodeAttemptsExhausted {
				r.attemptsExhausted.Add(1)
			}
			record.wanted = r.failureWanted(failure.EventID)
			// error_code=unauthorized is config-fixable: while the operator has
			// not acknowledged the credential is fixed (-replay-auth-blocked
			// off, the default), the record is blocked — excluded from the
			// wanted set, never marked, never committed. The per-record detail
			// line is budgeted (H1); the echoed fields are sanitized because
			// the DLQ topic is an untrusted input boundary (M3/CWE-117).
			record.authBlocked = failure.ErrorCode == ErrorCodeUnauthorized &&
				record.wanted && !r.replayAuthBlocked.Load()
			if record.authBlocked {
				r.authBlocked.Add(1)
				if r.authBlockedDetailBudget.Add(1) <= maxAuthBlockedLogDetails {
					r.logger.Printf("dlq record auth-blocked topic=%s partition=%d offset=%d event_id=%s error=%s (fix AUDIT_OUTBOX_TOKEN before replay)",
						r.dlqReader.Config().Topic, message.Partition, message.Offset,
						sanitizeLogField(failure.EventID, 64), sanitizeLogField(failure.ErrorMessage, 200))
				}
			}
		} else {
			r.logger.Printf("dlq record unparsable topic=%s partition=%d offset=%d error=%v", r.dlqReader.Config().Topic, message.Partition, message.Offset, decodeErr)
		}
		collected = append(collected, record)
	}
}

// scanAccepted walks the accepted topic from its first retained offset and
// re-publishes every message whose payload event_id (authoritative) or,
// only when the payload probe is empty, the key (fallback for unparsable
// values) matches a wanted event ID. The key fallback never overrides a
// valid payload event_id, even a conflicting one: a message whose payload
// carries a non-empty but unwanted event_id is skipped, so a DLQ record
// whose only trace is a stale key cannot close the wrong DLQ record. Replay
// is idempotent: events already in the state file are skipped. A key-matched
// but value-unparsable message is marked replayed with a log line, mirroring
// the consumer's anti-loop rule.
//
// The scan start is guaranteed by RunOnce's per-round
// resetAcceptedToFirstOffset: a fresh accepted reader (StartOffset:
// kafka.FirstOffset, no committed offsets) is created before every scan, so
// a wanted event whose original predates an earlier round's scan end is
// still reachable. Without that reset the drained path below would mark such
// records unresolvable even though their originals are retained.
//
// The scan ends when (a) one full drain window passes with no message
// (drained=true: the reader is caught up to the high watermark, so the
// retained topic was fully scanned) or (b) the round reaches
// maxScanWindows x drainTimeout (drained=false: sustained ingest cut the
// round off — nothing is marked unresolvable and everything unresolved
// stays pending for the next round). A transport error sets scanErr and
// leaves everything pending. Returns the number of events actually
// re-published this round (permanent closures excluded — REQ-PERM-2) and
// the set of event IDs that reached a durable decision this round
// (replayed, permanently closed, unparsable, or converged-as-unresolvable
// — NOT transiently failed).
func (r *Replayer) scanAcceptedLegacy(ctx context.Context, wanted wantedSet) (int, map[dlqRecordID]bool, error) {
	window := r.drainWindow()
	scanCtx, cancel := context.WithTimeout(ctx, maxScanWindows*window)
	defer cancel()
	replayed := 0
	resolved := map[dlqRecordID]bool{}
	found := map[string]bool{} // wanted IDs whose message was matched this round
	drained := false           // true only after a full quiet window with no message
	var scanErr error
	lastMessage := time.Now()
	for {
		fetchCtx, fetchCancel := context.WithTimeout(scanCtx, window)
		message, err := r.accepted.FetchMessage(fetchCtx)
		fetchCancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil && time.Since(lastMessage) >= window {
				drained = true // topic quiet: the retained topic was fully scanned
			} else if ctx.Err() == nil {
				scanErr = err // transport failure: leave everything pending
			}
			break
		}
		lastMessage = time.Now()
		r.acceptedSeen.Add(1)
		payloadID := eventIDFromValue(message.Value) // decode once per message
		eventID := ""
		if payloadID != "" && len(wanted.byEventID[payloadID]) > 0 && !r.state.marked(payloadID) {
			eventID = payloadID // payload event_id is authoritative
		} else if keyID := string(message.Key); payloadID == "" && len(wanted.byEventID[keyID]) > 0 && !r.state.marked(keyID) {
			eventID = keyID // fallback only when the payload probe is empty (unparsable values whose only signal is the key; mirrors deadLetterUnparsable)
		}
		if eventID == "" {
			continue
		}
		found[eventID] = true
		if payloadID == "" {
			// Key-matched, value not canonical: existing anti-loop mark + log.
			r.logger.Printf("accepted message unparsable topic=%s partition=%d offset=%d event_id=%s; marking replayed to avoid loop", r.accepted.Config().Topic, message.Partition, message.Offset, eventID)
			if err := r.state.Mark(eventID); err != nil {
				return replayed, resolved, err
			}
			resolveLegacyEventRecords(wanted, eventID, resolved)
			r.unparsableMarks.Add(1)
			replayed++
			continue
		}
		if err := r.republish(ctx, message.Key, message.Value); err != nil {
			r.republishFail.Add(1) // inclusive semantics unchanged (2026-08-15)
			// REQ-PERM-1: a wanted error_code=permanent_error record gets ONE
			// republish attempt in its lifetime; any failure closes it as a
			// permanent closure (poison-pill policy). Non-permanent codes keep
			// the legacy policy: only a live DeliveryError.Permanent rejection
			// closes; transient failures retry next round. The one-shot check
			// runs BEFORE the live-rejection check so a permanent_error record
			// never reaches the legacy path (which would count replayed++).
			var deliveryErr *outbox.DeliveryError
			oneShot := legacyEventOneShot(wanted, eventID)
			liveRejected := !oneShot && errors.As(err, &deliveryErr) && deliveryErr.Permanent
			if oneShot || liveRejected {
				if oneShot {
					r.logger.Printf("PERMANENT closure event_id=%s code=permanent_error one-shot republish attempt failed: %s; DLQ offset committed",
						sanitizeLogField(eventID, 64), sanitizeLogField(fmt.Sprintf("%v", err), 200))
				} else {
					// Legacy live-rejection closure for non-permanent-code
					// records: behavior unchanged (design §2.3), log modernized.
					r.logger.Printf("PERMANENT closure event_id=%s republish rejected (permanent): %s; DLQ offset committed",
						sanitizeLogField(eventID, 64), sanitizeLogField(fmt.Sprintf("%v", err), 200))
				}
				if markErr := r.state.Mark(eventID); markErr != nil {
					return replayed, resolved, markErr
				}
				resolveLegacyEventRecords(wanted, eventID, resolved)
				r.permanent.Add(1)
				if liveRejected {
					replayed++ // legacy return contribution preserved; the one-shot never counts (REQ-PERM-2)
				}
				continue
			}
			r.logger.Printf("republish failed event_id=%s error=%v; will retry next round", eventID, err)
			continue // found => stays pending, never marked unresolvable
		}
		if err := r.state.Mark(eventID); err != nil {
			return replayed, resolved, err
		}
		resolveLegacyEventRecords(wanted, eventID, resolved)
		r.replayed.Add(1)
		replayed++
		r.logger.Printf("replayed event_id=%s", eventID)
	}
	if drained {
		// REQ-2: a quiet-topic scan from the first retained offset without a
		// match is definitive (the consumer published the Failure only after
		// fetching the original). Mark unresolvable this round so
		// commitResolved commits the DLQ offset — no attempt cap needed.
		// Found-but-failed events (transient republish error) stay pending.
		for eventID := range wanted.byEventID {
			if found[eventID] || legacyEventResolved(wanted, eventID, resolved) {
				continue
			}
			r.logger.Printf("unresolvable event_id=%s reason=original-not-found-in-accepted-topic round=%s", eventID, time.Now().Format(time.RFC3339))
			if err := r.state.Mark(eventID); err != nil {
				return replayed, resolved, err
			}
			resolveLegacyEventRecords(wanted, eventID, resolved)
			r.unresolvable.Add(1)
			replayed++
		}
	}
	return replayed, resolved, scanErr
}

// ReplayScheduler loops RunOnce with backoff until ctx is cancelled. A
// bounded scan pause lets the topic catch up between rounds.
func (r *Replayer) ReplayScheduler(ctx context.Context, interval time.Duration) error {
	for {
		if _, err := r.RunOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.logger.Printf("replay round failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// Close releases both readers.
func (r *Replayer) Close() error {
	dlqErr := r.dlqReader.Close()
	acceptedErr := r.accepted.Close()
	if dlqErr != nil {
		return dlqErr
	}
	return acceptedErr
}

// EventFromCanonical decodes a recovered accepted-topic message for
// API-based re-ingestion. Numbers stay exact so the derived digest is
// identical to the original producer's.
func EventFromCanonical(value []byte) (domain.Event, error) {
	var event domain.Event
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	if err := decoder.Decode(&event); err != nil {
		return domain.Event{}, fmt.Errorf("decode canonical event: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			err = errors.New("canonical event contains multiple JSON values")
		}
		return domain.Event{}, fmt.Errorf("decode canonical event: %w", err)
	}
	return event, nil
}
