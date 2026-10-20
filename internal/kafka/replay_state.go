package kafka

// The persisted replay mark set: an append-only JSONL log of dead-letter event
// IDs that have already been replayed. Kept apart from the round driver in
// replay.go so the state format, its torn-tail repair and its locking rules
// can change without touching the scan and republish path.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/snaplink/audit-governance/internal/fsutil"
)

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
	// pinned DataDir.CreateTemp); same-package tests replace it to record every temp
	// path (AC-2: no two writers ever share one temp inode, REQ-3).
	newTemp func(dir, pattern string) (*os.File, error)
	// lockWait bounds how long Mark waits for a contended flock before
	// failing the persist (a hung-but-alive peer cannot stall DLQ recovery
	// forever); defaults to defaultLockWait.
	lockWait time.Duration
	// Instance-local seam at the real flock boundary; nil uses lockReplayState.
	lockFile func(path string, wait time.Duration) (io.Closer, error)
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
	encoded, err := fsutil.ReadDataFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
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
func classifyStateFile(path string) (kind stateKind, err error) {
	file, err := fsutil.OpenDataFile(path, os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return stateAbsent, nil
		}
		return 0, fmt.Errorf("open replay state: %w", err)
	}
	defer func() { err = closeReplayFile(file, err) }()
	info, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat replay state: %w", err)
	}
	if info.Size() == 0 {
		return stateEmpty, nil
	}
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
	file, err := fsutil.OpenDataFile(path+".lock", os.O_CREATE|os.O_RDWR, fsutil.PrivateFileMode)
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
			return nil, closeReplayFile(file, fmt.Errorf("flock replay state lock: %w", err))
		}
		if time.Now().After(deadline) {
			return nil, closeReplayFile(file, errors.New("flock replay state lock: timed out waiting for a peer writer"))
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
func (s *ReplayState) Mark(eventID string) (resultErr error) {
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
	lock, err := s.acquireReplayLock(lockWait)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
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
		if err := lock.Close(); err != nil {
			return err
		} // early release before the flush; deferred Close is now a real no-op
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
	if err := lock.Close(); err != nil {
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
	file, err := fsutil.OpenDataFile(s.path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return fmt.Errorf("open replay state for append: %w", err)
	}
	n, err := file.Write(line)
	if err != nil || n != len(line) {
		if err == nil {
			err = io.ErrShortWrite
		}
		return closeReplayFile(file, fmt.Errorf("append replay state: %w", err))
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
	file, err := fsutil.OpenDataFile(s.path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open replay state for repair: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return closeReplayFile(file, fmt.Errorf("stat replay state: %w", err))
	}
	size := info.Size()
	if size == 0 {
		return closeReplayFile(file, nil)
	}
	window := size
	if window > tailCheckWindow {
		window = tailCheckWindow
	}
	tail := make([]byte, window)
	if _, err := file.ReadAt(tail, size-window); err != nil {
		return closeReplayFile(file, fmt.Errorf("read replay state tail: %w", err))
	}
	if tail[window-1] == '\n' {
		return closeReplayFile(file, nil) // clean tail: no repair needed
	}
	if idx := bytes.LastIndexByte(tail[:window-1], '\n'); idx >= 0 {
		err = file.Truncate(size - window + int64(idx) + 1)
		if err != nil {
			err = fmt.Errorf("truncate replay state tail: %w", err)
		}
		return closeReplayFile(file, err)
	}
	if err := closeReplayFile(file, nil); err != nil {
		return err
	}
	return s.rebuildLog()
}

// rebuildLog rewrites the log from its durable marks (header + sorted
// lines), dropping a torn/corrupt un-terminated tail, via the same
// unique-temp + fsync + rename + dir-fsync protocol as rewriteFull.
func (s *ReplayState) rebuildLog() error {
	if kind, err := classifyStateFile(s.path); err == nil && kind == stateTenantLog {
		return s.rebuildTenantLog()
	}
	encoded, err := fsutil.ReadDataFile(s.path)
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
	encoded, err := fsutil.ReadDataFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
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
