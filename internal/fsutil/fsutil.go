// Package fsutil holds the shared durability helpers for file-backed
// persistence: fsync of a written file and of the directory that contains a
// renamed file. A rename is not durable until the parent directory entry is
// synced; without these, a crash right after rename can lose the last
// committed state (snapshot, replay marks, archive objects).
package fsutil

import (
	"fmt"
	"os"
)

// SyncFile flushes file contents to stable storage. The caller must have
// written and closed (or be about to close) the file; Sync returns an error
// when the underlying device reports a failure, so a silently lost write is
// surfaced instead of being reported as committed.
func SyncFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open %s for sync: %w", path, err)
	}
	defer file.Close()
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync %s: %w", path, err)
	}
	return nil
}

// SyncDir flushes the directory entry of path so a completed rename survives
// a crash. Opening a directory read-only and syncing it is the standard
// pattern; it is a no-op on filesystems without directory fsync support.
func SyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory %s for sync: %w", path, err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync directory %s: %w", path, err)
	}
	return nil
}
