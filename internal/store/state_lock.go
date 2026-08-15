package store

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// ErrStateFileLocked is returned by Open when another live process holds
// the exclusive advisory lock on <path>.lock. The error is wrapped with the
// state path, the holder identity (best-effort) and an actionable hint, and
// is errors.Is-compatible so callers and tests can distinguish contention
// from other open failures without string matching.
var ErrStateFileLocked = errors.New("state file is locked by another process")

// stateFileLockRetryWindow bounds the LOCK_NB retry in acquireStateLock so a
// holder that is mid-close (or a transient kernel hiccup) does not fail
// spuriously, while a live holder still fails fast — never an unbounded wait.
// It mirrors lockReplayState's bounded-wait contract (internal/kafka/replay.go).
const stateFileLockRetryWindow = 250 * time.Millisecond

// stateLockedError carries the failed open's context: the configured state
// path, the best-effort holder identity read back from the lock file, and
// the metadata-read failure (if any). Unwrap exposes ErrStateFileLocked.
type stateLockedError struct {
	path   string // state path as configured (absolute in the message)
	holder string // "<pid> <exe> (start <RFC3339>)" or "unknown"
	cause  error  // metadata-read failure, if any (best-effort)
}

func (e *stateLockedError) Error() string {
	msg := fmt.Sprintf("state file %s is locked by another audit process (%s): stop the other audit-api or audit-governance-worker instance on this state path, or configure a distinct -state/-postgres-dsn", absPath(e.path), e.holder)
	if e.cause != nil {
		msg += fmt.Sprintf(" (holder identity unavailable: %v)", e.cause)
	}
	return msg
}

func (e *stateLockedError) Unwrap() error { return ErrStateFileLocked }

// absPath resolves relative -state values so the contention error (and test
// assertions) always name an absolute path.
func absPath(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abs
}

// acquireStateLock takes an exclusive advisory flock on <path>.lock, created
// 0o600 next to the state file (the lockReplayState pattern: the state file
// itself is never locked because Save renames a fresh inode over it, while
// <path>.lock is never renamed). The kernel releases the lock when the fd
// closes or the process dies, so no stale-lock detection is ever needed.
// LOCK_NB with a bounded retry keeps a live second writer failing fast: the
// final failure wraps ErrStateFileLocked with the holder identity read back
// from the lock file.
func acquireStateLock(path string) (io.Closer, error) {
	lockPath := path + ".lock"
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open state lock %s: %w", lockPath, err)
	}
	deadline := time.Now().Add(stateFileLockRetryWindow)
	backoff := time.Millisecond
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			// Best-effort: the kernel flock is the authority; the metadata
			// only improves the contention error. Truncate-then-write on the
			// held fd is deliberately unsynchronized with contenders, who
			// only read it after failing the flock.
			writeHolderIdentity(file)
			return file, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			_ = file.Close()
			return nil, fmt.Errorf("flock state lock %s: %w", lockPath, err)
		}
		if time.Now().After(deadline) {
			holder, readErr := readHolderIdentity(lockPath)
			_ = file.Close()
			return nil, &stateLockedError{path: path, holder: holder, cause: readErr}
		}
		time.Sleep(backoff)
		if backoff < 10*time.Millisecond {
			backoff *= 2
		}
	}
}

// writeHolderIdentity records the holder's pid, executable base name and
// start time in the lock file, truncating it. Best-effort by design: every
// error is swallowed because the flock itself is the mutual-exclusion
// authority and the metadata only improves error messages.
func writeHolderIdentity(f *os.File) {
	exe, err := os.Executable()
	if err != nil {
		exe = "unknown"
	}
	line := fmt.Sprintf("%d\t%s\t%s\n", os.Getpid(), filepath.Base(exe), time.Now().UTC().Format(time.RFC3339))
	_ = f.Truncate(0)
	_, _ = f.Seek(0, 0)
	_, _ = f.WriteString(line)
}

// readHolderIdentity reads the holder identity written by the flock owner.
// Best-effort: an unreadable, empty or torn file yields "unknown" (still
// actionable — the stateLockedError always names the path and the fix).
func readHolderIdentity(lockPath string) (string, error) {
	b, err := os.ReadFile(lockPath)
	if err != nil {
		return "unknown", err
	}
	parts := strings.SplitN(strings.TrimSpace(string(b)), "\t", 3)
	if len(parts) < 2 || parts[0] == "" {
		return "unknown", nil
	}
	started := ""
	if len(parts) == 3 {
		started = ", started " + parts[2]
	}
	return fmt.Sprintf("pid %s (%s%s)", parts[0], parts[1], started), nil
}
