//go:build linux

package fsutil

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// OpenDataDir walks every ancestor through O_NOFOLLOW directory descriptors.
// Newly created directories are 0700; existing directories are never chmod'd.
// The configured filesystem/ancestors/mounts remain an operator trust boundary:
// descriptors prevent symlink redirection, not hostile mount administration.
func OpenDataDir(path string, create bool) (*DataDir, error) {
	if path == "" || strings.IndexByte(path, 0) >= 0 {
		return nil, fmt.Errorf("%w: invalid directory", ErrUnsafeFile)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	flags := unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW
	fd, err := unix.Open("/", flags, 0)
	if err != nil {
		return nil, err
	}
	current, err := dataDescriptor(fd, "/")
	if err != nil {
		return nil, err
	}
	resolved := "/"
	for _, component := range strings.Split(strings.TrimPrefix(absolute, "/"), "/") {
		if component == "" {
			continue
		}
		resolved = filepath.Join(resolved, component)
		next, openErr := unix.Openat(int(current.Fd()), component, flags, 0)
		if errors.Is(openErr, unix.ENOENT) && create {
			mkdirErr := unix.Mkdirat(int(current.Fd()), component, 0o700)
			if mkdirErr != nil && !errors.Is(mkdirErr, unix.EEXIST) {
				return nil, errors.Join(&os.PathError{Op: "mkdir", Path: resolved, Err: mkdirErr}, current.Close())
			}
			if err := current.Sync(); err != nil {
				return nil, errors.Join(err, current.Close())
			}
			next, openErr = unix.Openat(int(current.Fd()), component, flags, 0)
		}
		if openErr != nil {
			return nil, errors.Join(dataPathError("open directory", resolved, openErr), current.Close())
		}
		child, descriptorErr := dataDescriptor(next, resolved)
		closeErr := current.Close()
		if err := errors.Join(descriptorErr, closeErr); err != nil {
			if child != nil {
				err = errors.Join(err, child.Close())
			}
			return nil, err
		}
		current = child
	}
	return &DataDir{file: current}, nil
}

func dataDescriptor(fd int, name string) (*os.File, error) {
	if fd < 0 {
		return nil, fmt.Errorf("%w: invalid file descriptor", ErrUnsafeFile)
	}
	return os.NewFile(uintptr(fd), name), nil
}

func dataPathError(op, path string, err error) error {
	cause := error(&os.PathError{Op: op, Path: path, Err: err})
	if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.ENOTDIR) || errors.Is(err, unix.EISDIR) || errors.Is(err, unix.ENXIO) {
		cause = errors.Join(ErrUnsafeFile, cause)
	}
	return cause
}

func (d *DataDir) OpenFile(name string, flag int, mode os.FileMode) (*os.File, error) {
	if err := dataFileName(name); err != nil {
		return nil, err
	}
	if mode.Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%w: new data files must be owner-only", ErrUnsafeFile)
	}
	// O_NONBLOCK avoids FIFO/device preparation stalls; reject special and
	// hard-linked inodes BEFORE truncation or any caller writes via the fd.
	flags := flag&^unix.O_TRUNC | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK
	fd, err := unix.Openat(int(d.file.Fd()), name, flags, uint32(mode.Perm()))
	if err != nil {
		return nil, dataPathError("open data file", filepath.Join(d.file.Name(), name), err)
	}
	file, err := dataDescriptor(fd, filepath.Join(d.file.Name(), name))
	if err != nil {
		return nil, err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 {
		return nil, errors.Join(fmt.Errorf("%w: regular single-link file required", ErrUnsafeFile), file.Close())
	}
	if flag&unix.O_TRUNC != 0 {
		if err := file.Truncate(0); err != nil {
			return nil, errors.Join(err, file.Close())
		}
	}
	return file, nil
}

func (d *DataDir) Rename(old, new string) error {
	if err := dataFileName(old); err != nil {
		return err
	}
	if err := dataFileName(new); err != nil {
		return err
	}
	return unix.Renameat(int(d.file.Fd()), old, int(d.file.Fd()), new)
}

func (d *DataDir) Remove(name string) error {
	if err := dataFileName(name); err != nil {
		return err
	}
	return unix.Unlinkat(int(d.file.Fd()), name, 0)
}
