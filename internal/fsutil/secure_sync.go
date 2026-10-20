package fsutil

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// SyncChain syncs the actual pinned parent and all required ancestors in
// leaf-to-root order. syncDir is the existing observation/failure seam; even
// when supplied it never substitutes for syncing the pinned parent itself.
func (d *DataDir) SyncChain(root string, syncDir func(string) error) error {
	if syncDir == nil {
		syncDir = d.syncAncestor
	}
	return syncDataDirChain(root, d.file.Name(), d.Sync, syncDir)
}

// Keep the original safe path-open/close failure boundary at the leaf, but
// do not fsync the same directory twice. A replacement real directory also
// fails closed rather than being synced in place of the pinned inode.
func (d *DataDir) syncAncestor(path string) (err error) {
	if path != d.file.Name() {
		return SyncDir(path)
	}
	current, err := OpenDataDir(path, false)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, current.Close()) }()
	before, err := d.file.Stat()
	if err != nil {
		return err
	}
	after, err := current.file.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(before, after) {
		return fmt.Errorf("%w: data directory changed after capture", ErrUnsafeFile)
	}
	return nil
}

func syncDataDirChain(root, parent string, syncPinned func() error, syncDir func(string) error) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	if err := syncPinned(); err != nil {
		return err
	}
	return SyncDirChain(root, parent, func(path string) error {
		if syncDir != nil {
			return syncDir(path)
		}
		if path == parent {
			return nil // already fsynced through the actual captured descriptor
		}
		return SyncDir(path)
	})
}
