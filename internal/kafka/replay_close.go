package kafka

import (
	"errors"
	"fmt"
	"io"
	"time"
)

// Keep the operation's primary error AND a cleanup failure. A failed close
// must not turn a partial write/repair into a successful durable replay mark.
func closeReplayFile(file io.Closer, prior error) error {
	if err := file.Close(); err != nil {
		return errors.Join(prior, fmt.Errorf("close replay state file: %w", err))
	}
	return prior
}

// Each Mark owns this guard under ReplayState.mu. Early release and deferred
// cleanup close the underlying descriptor exactly once, including error paths.
type replayLock struct{ file io.Closer }

func (l *replayLock) Close() error {
	if l.file == nil {
		return nil
	}
	file := l.file
	l.file = nil
	return closeReplayFile(file, nil)
}

func (s *ReplayState) acquireReplayLock(wait time.Duration) (*replayLock, error) {
	acquire := s.lockFile
	if acquire == nil {
		acquire = lockReplayState
	}
	file, err := acquire(s.path, wait)
	if err != nil {
		return nil, err
	}
	return &replayLock{file: file}, nil
}
