package kafka

import (
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"
)

type replayFaultCloser struct {
	file  io.Closer
	err   error
	calls int
}

func (c *replayFaultCloser) Close() error {
	c.calls++
	if c.file != nil {
		return errors.Join(c.err, c.file.Close())
	}
	return c.err
}

func TestReplayCloseRetainsPrimaryAndCleanupFailures(t *testing.T) {
	primary, cleanup := errors.New("primary failure"), errors.New("close failure")
	closer := &replayFaultCloser{err: cleanup}
	err := closeReplayFile(closer, primary)
	if !errors.Is(err, primary) || !errors.Is(err, cleanup) || closer.calls != 1 {
		t.Fatalf("close lost a cause: %v calls=%d", err, closer.calls)
	}
	lock := &replayLock{file: closer}
	if err := lock.Close(); !errors.Is(err, cleanup) {
		t.Fatalf("first close=%v", err)
	}
	if err := lock.Close(); err != nil || closer.calls != 2 {
		t.Fatalf("second close retried descriptor: err=%v calls=%d", err, closer.calls)
	}
}

func TestReplayLockCloseFailureNeverMarksSuccessfulInMemory(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		for _, appendMode := range []bool{false, true} {
			path := filepath.Join(t.TempDir(), "replay.json")
			state, err := LoadReplayState(path)
			if err != nil {
				t.Fatal(err)
			}
			mark := func(id string) error {
				if scoped {
					return state.MarkTenant("tenant-a", id)
				}
				return state.Mark(id)
			}
			if appendMode {
				if err := mark("seed"); err != nil {
					t.Fatal(err)
				}
			}
			cleanup := errors.New("injected lock close failure")
			var closer *replayFaultCloser
			state.lockFile = func(path string, wait time.Duration) (io.Closer, error) {
				file, err := lockReplayState(path, wait) // actual exclusive flock
				if err != nil {
					return nil, err
				}
				closer = &replayFaultCloser{file: file, err: cleanup}
				return closer, nil
			}
			if err := mark("unknown"); !errors.Is(err, cleanup) {
				t.Fatalf("scoped=%v append=%v returned success: %v", scoped, appendMode, err)
			}
			if closer.calls != 1 || state.marked("unknown") || state.Marked("tenant-a", "unknown") {
				t.Fatalf("scoped=%v append=%v failed close advanced memory or retried fd", scoped, appendMode)
			}
			state.lockFile = nil
			if err := mark("unknown"); err != nil {
				t.Fatalf("retry after unknown close result failed: %v", err)
			}
			reopened, err := LoadReplayState(path)
			if err != nil || reopened == nil || !reopened.marked("unknown") {
				t.Fatalf("successful retry is not readable: %v", err)
			}
			if scoped && (!reopened.Marked("tenant-a", "unknown") || reopened.Marked("tenant-b", "unknown")) {
				t.Fatal("close/retry changed tenant isolation")
			}
		}
	}
}
