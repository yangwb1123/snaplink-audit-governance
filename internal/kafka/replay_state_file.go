package kafka

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/snaplink/audit-governance/internal/fsutil"
)

// rewriteBytes keeps creation, rename and cleanup in ONE pinned parent. The
// unique temp is fsynced before rename and the actual parent after; hooks still
// observe the original ordering. Only our temp is cleaned up. A post-rename
// error is an UNKNOWN outcome, not rollback and not an in-memory replay mark.
func (s *ReplayState) rewriteBytes(content []byte) (err error) {
	parent, err := fsutil.OpenDataDir(filepath.Dir(s.path), false)
	if err != nil {
		return fmt.Errorf("open replay state directory: %w", err)
	}
	defer func() { err = errors.Join(err, parent.Close()) }()
	temp, err := s.newReplayTemp(parent)
	if err != nil {
		return fmt.Errorf("create replay state temp: %w", err)
	}
	tempName := filepath.Base(temp.Name())
	owned := true
	defer func() {
		if owned {
			err = errors.Join(err, parent.Remove(tempName))
		}
	}()
	if n, err := temp.Write(content); err != nil || n != len(content) {
		if err == nil {
			err = io.ErrShortWrite
		}
		return closeReplayFile(temp, fmt.Errorf("write replay state: %w", err))
	}
	// Never rely on re-opening a lexical path for the actual content fsync.
	if err := closeReplayFile(temp, temp.Sync()); err != nil {
		return err
	}
	if err := s.syncFileOr(temp.Name()); err != nil {
		return err
	}
	target := filepath.Base(s.path)
	existing, openErr := parent.OpenFile(target, os.O_RDONLY, 0)
	if openErr != nil && !errors.Is(openErr, os.ErrNotExist) {
		return openErr
	}
	if existing != nil {
		if err := closeReplayFile(existing, nil); err != nil {
			return err
		}
	}
	if err := parent.Rename(tempName, target); err != nil {
		return fmt.Errorf("rename replay state: %w", err)
	}
	owned = false
	if err := parent.Sync(); err != nil {
		return fmt.Errorf("sync replay state directory: %w", err)
	}
	syncDir := s.syncDir
	if syncDir == nil {
		syncDir = fsutil.SyncDir
	}
	if err := syncDir(filepath.Dir(s.path)); err != nil {
		return fmt.Errorf("sync replay state directory: %w", err)
	}
	return nil
}

func (s *ReplayState) newReplayTemp(parent *fsutil.DataDir) (*os.File, error) {
	pattern := filepath.Base(s.path) + "-*.tmp"
	if s.newTemp == nil {
		return parent.CreateTemp(pattern)
	}
	// Keep the existing instance-local factory seam, but verify it really gave
	// us the same private regular inode in our pinned directory before write.
	file, err := s.newTemp(filepath.Dir(s.path), pattern)
	if err != nil {
		return nil, err
	}
	if file == nil {
		return nil, fmt.Errorf("%w: missing replay temp", fsutil.ErrUnsafeFile)
	}
	if err := verifyReplayTemp(parent, file); err != nil {
		return nil, closeReplayFile(file, err)
	}
	return file, nil
}

func verifyReplayTemp(parent *fsutil.DataDir, file *os.File) (err error) {
	candidate, err := parent.OpenFile(filepath.Base(file.Name()), os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer func() { err = closeReplayFile(candidate, err) }()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	other, err := candidate.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(info, other) || info.Mode().Perm()&0o077 != 0 || info.Size() != 0 {
		return fmt.Errorf("%w: replay temp must be our empty private inode", fsutil.ErrUnsafeFile)
	}
	return nil
}
