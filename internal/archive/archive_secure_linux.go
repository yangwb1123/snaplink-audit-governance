//go:build linux

package archive

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const secureArchiveUnsupported = "secure local archive operations require Linux openat primitives"

type secureDir struct {
	fd   int
	path string
}

type secureRoot struct {
	root    *secureDir
	parent  *secureDir
	chain   []*secureDir
	opened  []*secureDir
	created bool
}

func openArchiveDir(fd int, name, path string) (*secureDir, error) {
	flags := unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW
	child, err := unix.Openat(fd, name, flags, 0)
	if err != nil {
		return nil, err
	}
	return &secureDir{fd: child, path: path}, nil
}

// openArchiveRoot resolves every configured root component through directory
// descriptors. No path component is checked and then reused through a joined
// pathname, so replacement of a component cannot redirect later operations.
func openArchiveRoot(dir string, create bool) (*secureRoot, error) {
	if dir == "" {
		return nil, fmt.Errorf("archive directory is not configured")
	}
	clean := filepath.Clean(dir)
	absolute := filepath.IsAbs(clean)
	startName := "."
	startPath := "."
	if absolute {
		startName, startPath = string(filepath.Separator), string(filepath.Separator)
	}
	start, err := openArchiveDir(unix.AT_FDCWD, startName, startPath)
	if err != nil {
		return nil, fmt.Errorf("open archive start %s: %w", startPath, err)
	}
	result := &secureRoot{opened: []*secureDir{start}}
	all := []*secureDir{start}
	components := strings.Split(clean, string(filepath.Separator))
	if absolute {
		components = components[1:]
	}
	createdFrom := -1
	current := start
	for _, component := range components {
		if component == "" || component == "." {
			continue
		}
		path := filepath.Join(current.path, component)
		child, openErr := openArchiveDir(current.fd, component, path)
		if openErr != nil && os.IsNotExist(openErr) && create {
			if mkdirErr := unix.Mkdirat(current.fd, component, 0o750); mkdirErr != nil && !os.IsExist(mkdirErr) {
				result.close()
				return nil, fmt.Errorf("create archive directory %s: %w", path, mkdirErr)
			}
			if createdFrom < 0 {
				createdFrom = len(all)
			}
			child, openErr = openArchiveDir(current.fd, component, path)
		}
		if openErr != nil {
			result.close()
			if openErr == unix.ENOENT {
				return nil, &os.PathError{Op: "open", Path: path, Err: openErr}
			}
			return nil, fmt.Errorf("open archive directory %s: %w", path, openErr)
		}
		result.opened = append(result.opened, child)
		all = append(all, child)
		current = child
	}
	result.root = current
	if len(all) > 1 {
		result.parent = all[len(all)-2]
	}
	if createdFrom >= 0 {
		result.created = true
		for i := len(all) - 1; i >= createdFrom-1; i-- {
			result.chain = append(result.chain, all[i])
		}
	} else {
		result.chain = []*secureDir{result.root}
	}
	return result, nil
}

func (r *secureRoot) close() {
	for i := len(r.opened) - 1; i >= 0; i-- {
		_ = unix.Close(r.opened[i].fd)
	}
}

func (f *FileStore) rootNeedsParent() bool {
	if value := f.rootState.Load(); value != nil {
		return value.(rootState).needsParentSync
	}
	return false
}

func (f *FileStore) setRootNeedsParent(value bool, basePath string) {
	state := rootState{needsParentSync: value, basePath: basePath}
	f.rootState.Store(state)
}

func appendUniqueDir(chain []*secureDir, dir *secureDir) []*secureDir {
	for _, existing := range chain {
		if existing.fd == dir.fd {
			return chain
		}
	}
	return append(chain, dir)
}

func (f *FileStore) syncArchiveDirs(chain []*secureDir) error {
	if f.syncDir != nil {
		for _, dir := range chain {
			if err := f.syncDir(dir.path); err != nil {
				return err
			}
		}
		return nil
	}
	for _, dir := range chain {
		if err := unix.Fsync(dir.fd); err != nil {
			return fmt.Errorf("sync directory %s: %w", dir.path, err)
		}
	}
	return nil
}

func (f *FileStore) rootSyncChain(root *secureRoot) []*secureDir {
	chain := append([]*secureDir(nil), root.chain...)
	if !f.rootNeedsParent() {
		return chain
	}
	state := f.rootState.Load().(rootState)
	base := -1
	for i, dir := range root.opened {
		if dir.path == state.basePath {
			base = i
			break
		}
	}
	if base >= 0 {
		for i := len(root.opened) - 2; i >= base; i-- {
			chain = appendUniqueDir(chain, root.opened[i])
		}
	} else if root.parent != nil {
		chain = appendUniqueDir(chain, root.parent)
	}
	return chain
}

func walkArchiveKey(root *secureRoot, cleanKey string, create bool) (*secureDir, []*secureDir, error) {
	components := strings.Split(cleanKey, string(filepath.Separator))
	if len(components) == 0 || components[len(components)-1] == "" {
		return nil, nil, fmt.Errorf("archive key %q has no final component", cleanKey)
	}
	current := root.root
	opened := []*secureDir{current}
	for _, component := range components[:len(components)-1] {
		path := filepath.Join(current.path, component)
		child, err := openArchiveDir(current.fd, component, path)
		if err != nil && os.IsNotExist(err) && create {
			if mkdirErr := unix.Mkdirat(current.fd, component, 0o750); mkdirErr != nil && !os.IsExist(mkdirErr) {
				return nil, nil, fmt.Errorf("create archive directory %s: %w", path, mkdirErr)
			}
			child, err = openArchiveDir(current.fd, component, path)
		}
		if err != nil {
			if err == unix.ENOENT {
				return nil, nil, &os.PathError{Op: "open", Path: path, Err: err}
			}
			return nil, nil, fmt.Errorf("open archive directory %s: %w", path, err)
		}
		root.opened = append(root.opened, child)
		opened = append(opened, child)
		current = child
	}
	chain := make([]*secureDir, 0, len(opened)+len(root.chain))
	for i := len(opened) - 1; i >= 0; i-- {
		chain = append(chain, opened[i])
	}
	for _, dir := range root.chain[1:] {
		chain = appendUniqueDir(chain, dir)
	}
	return current, chain, nil
}

func openRegularAt(parent *secureDir, name, display string) (*os.File, error) {
	fd, err := unix.Openat(parent.fd, name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		if err == unix.ELOOP {
			return nil, nonRegularArchiveError(display)
		}
		return nil, err
	}
	file := os.NewFile(uintptr(fd), display)
	info, statErr := file.Stat()
	if statErr != nil {
		_ = file.Close()
		return nil, statErr
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, nonRegularArchiveError(display)
	}
	return file, nil
}

func verifyExistingAt(parent *secureDir, name, display string, data []byte) error {
	file, err := openRegularAt(parent, name, display)
	if err != nil {
		return fmt.Errorf("archive path %s already exists and cannot be verified: %w", display, err)
	}
	defer file.Close()
	existing, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("archive path %s already exists and cannot be verified: %w", display, err)
	}
	if string(existing) != string(data) {
		return fmt.Errorf("archive path %s: %w", display, ErrObjectConflict)
	}
	return nil
}

func unlinkPublishedIfSame(parent *secureDir, name string, file *os.File) {
	var published, current unix.Stat_t
	if unix.Fstat(int(file.Fd()), &published) != nil ||
		unix.Fstatat(parent.fd, name, &current, unix.AT_SYMLINK_NOFOLLOW) != nil {
		return
	}
	if published.Dev != current.Dev || published.Ino != current.Ino {
		return
	}
	_ = unix.Unlinkat(parent.fd, name, 0)
}

func (f *FileStore) Put(_ context.Context, key string, data []byte) error {
	cleanKey, err := validateArchiveKey(f.Dir, key)
	if err != nil {
		return err
	}
	if err := checkKeyLength(key); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	root, err := openArchiveRoot(f.Dir, true)
	if err != nil {
		return err
	}
	defer root.close()
	if root.created {
		f.setRootNeedsParent(true, root.chain[len(root.chain)-1].path)
	}
	parent, chain, err := walkArchiveKey(root, cleanKey, true)
	if err != nil {
		return err
	}
	display := archiveDisplayPath(f.Dir, cleanKey)
	name := filepath.Base(cleanKey)
	if existing, openErr := openRegularAt(parent, name, display); openErr == nil {
		defer existing.Close()
		contents, readErr := io.ReadAll(existing)
		if readErr != nil {
			return fmt.Errorf("archive path %s already exists and cannot be verified: %w", display, readErr)
		}
		if string(contents) != string(data) {
			return fmt.Errorf("archive path %s: %w", display, ErrObjectConflict)
		}
		return nil
	} else if openErr != unix.ENOENT {
		return openErr
	}
	fd, err := unix.Openat(parent.fd, ".", unix.O_WRONLY|unix.O_CLOEXEC|unix.O_TMPFILE, 0o440)
	if err != nil {
		return fmt.Errorf("create unpublished archive object %s: %w", display, err)
	}
	file := os.NewFile(uintptr(fd), display)
	defer func() { _ = file.Close() }()
	if err := writeArchiveFile(file, data); err != nil {
		return err
	}
	if err := unix.Linkat(int(file.Fd()), "", parent.fd, name, unix.AT_EMPTY_PATH); err != nil {
		if err == unix.EEXIST {
			_ = file.Close()
			return verifyExistingAt(parent, name, display, data)
		}
		return fmt.Errorf("publish archive object %s: %w", display, err)
	}
	fullChain := chain
	for _, dir := range f.rootSyncChain(root)[1:] {
		fullChain = appendUniqueDir(fullChain, dir)
	}
	if err := f.syncArchiveDirs(fullChain); err != nil {
		// The production path never removes by name: an atomic publication
		// may have been replaced by another actor, and unlinking then could
		// destroy that actor's object. The legacy test seam has no filesystem
		// synchronization semantics, so retain its historical cleanup result.
		if f.syncDir != nil {
			unlinkPublishedIfSame(parent, name, file)
		}
		return fmt.Errorf("sync directory chain for %s: %w", display, err)
	}
	if root.created || f.rootNeedsParent() {
		f.setRootNeedsParent(false, "")
	}
	return nil
}

func writeArchiveFile(file *os.File, data []byte) error {
	for len(data) > 0 {
		n, err := file.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return nil
}

func (f *FileStore) Get(_ context.Context, key string) ([]byte, error) {
	cleanKey, err := validateArchiveKey(f.Dir, key)
	if err != nil {
		return nil, err
	}
	if err := checkKeyLength(key); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	root, err := openArchiveRoot(f.Dir, false)
	if err != nil {
		return nil, err
	}
	defer root.close()
	parent, _, err := walkArchiveKey(root, cleanKey, false)
	if err != nil {
		return nil, err
	}
	display := archiveDisplayPath(f.Dir, cleanKey)
	file, err := openRegularAt(parent, filepath.Base(cleanKey), display)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(file)
}

func (f *FileStore) Ready(_ context.Context) error {
	if f.Dir == "" {
		return fmt.Errorf("archive directory is not configured")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	root, err := openArchiveRoot(f.Dir, true)
	if err != nil {
		return err
	}
	defer root.close()
	if root.created {
		f.setRootNeedsParent(true, root.chain[len(root.chain)-1].path)
	}
	fd, err := unix.Openat(root.root.fd, ".", unix.O_WRONLY|unix.O_CLOEXEC|unix.O_TMPFILE, 0o640)
	if err != nil {
		return fmt.Errorf("create archive readiness probe: %w", err)
	}
	probe := os.NewFile(uintptr(fd), "archive readiness probe")
	if err := writeArchiveFile(probe, []byte("ok")); err != nil {
		_ = probe.Close()
		return err
	}
	if err := probe.Close(); err != nil {
		return err
	}
	if err := f.syncArchiveDirs(f.rootSyncChain(root)); err != nil {
		return fmt.Errorf("sync archive root %s: %w", f.Dir, err)
	}
	f.setRootNeedsParent(false, "")
	return nil
}
