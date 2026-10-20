package fsutil

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ErrUnsafeFile distinguishes unsafe local file topology from missing data.
// These helpers are for service-owned data, not symlink-based secret mounts.
var ErrUnsafeFile = errors.New("unsafe local data file")

// ErrDataDirectorySync means rename completed but its durability could not be
// established. Callers must not confuse this with a pre-rename write failure.
var ErrDataDirectorySync = errors.New("data directory sync failed after rename")

const PrivateFileMode os.FileMode = 0o600

// DataDir pins a directory descriptor. Operations accept a SINGLE basename,
// never a joined path; parent replacement cannot redirect rename or cleanup.
// The caller owns Close and must not share directory iteration concurrently.
type DataDir struct{ file *os.File }

func (d *DataDir) Close() error                    { return d.file.Close() }
func (d *DataDir) Sync() error                     { return d.file.Sync() }
func (d *DataDir) ReadDir() ([]os.DirEntry, error) { return d.file.ReadDir(-1) }

func dataFileName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\\\x00") {
		return fmt.Errorf("%w: a single file name is required", ErrUnsafeFile)
	}
	return nil
}

// OpenDataFile rejects symlinks in ALL components, hard-linked and special
// files. Existing legacy permissions are not changed; create callers choose
// owner-only modes. A requested truncate occurs only after fd verification.
func OpenDataFile(path string, flag int, mode os.FileMode) (*os.File, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: empty path", ErrUnsafeFile)
	}
	dir, err := OpenDataDir(filepath.Dir(path), false)
	if err != nil {
		return nil, err
	}
	file, openErr := dir.OpenFile(filepath.Base(path), flag, mode)
	closeErr := dir.Close()
	if closeErr == nil {
		return file, openErr // preserve *PathError for os.IsNotExist compatibility
	}
	if err := errors.Join(openErr, closeErr); err != nil {
		if file != nil {
			err = errors.Join(err, file.Close())
		}
		return nil, err
	}
	return file, nil
}

func ReadDataFile(path string) (raw []byte, err error) {
	file, err := OpenDataFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	return io.ReadAll(file)
}

func ReadDataDir(path string) (entries []os.DirEntry, err error) {
	dir, err := OpenDataDir(path, false)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	return dir.ReadDir()
}

func MakeDataDir(path string) error {
	dir, err := OpenDataDir(path, true)
	if err != nil {
		return err
	}
	return dir.Close()
}

func StatDataFile(path string) (info os.FileInfo, err error) {
	file, err := OpenDataFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	return file.Stat()
}

// AtomicDataFile preserves the write/sync/rename/directory-chain ordering.
// An existing .tmp is UNKNOWN evidence, never truncated or cleaned up. Only
// this operation's exclusively created temp is removed. All leaf operations
// stay relative to the same pinned parent, including post-rename cleanup.
func AtomicDataFile(path string, data []byte, mode os.FileMode, syncDir func(string) error) (err error) {
	if path == "" {
		return fmt.Errorf("%w: empty path", ErrUnsafeFile)
	}
	parent := filepath.Dir(path)
	root := DeepestExistingAncestor(parent)
	dir, err := OpenDataDir(parent, true)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	name := filepath.Base(path)
	existing, openErr := dir.OpenFile(name, os.O_RDONLY, 0)
	if openErr != nil && !errors.Is(openErr, os.ErrNotExist) {
		return openErr
	}
	if existing != nil {
		if err := existing.Close(); err != nil {
			return err
		}
	}
	temp := name + ".tmp"
	file, err := dir.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	owned := true
	defer func() {
		if owned {
			err = errors.Join(err, dir.Remove(temp))
		}
	}()
	n, writeErr := file.Write(data)
	if writeErr == nil && n != len(data) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	if err := errors.Join(writeErr, file.Close()); err != nil {
		return err
	}
	if err := dir.Rename(temp, name); err != nil {
		return err
	}
	owned = false
	if err := dir.SyncChain(root, syncDir); err != nil {
		return fmt.Errorf("%w: %w", ErrDataDirectorySync, err)
	}
	return nil
}
