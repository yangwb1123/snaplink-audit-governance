package fsutil

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"strings"
)

// CreateTemp creates a private, unique inode INSIDE the pinned directory.
// Unknown temp files are not enumerated, reused, truncated or deleted.
// Like os.CreateTemp, the last '*' is replaced (otherwise a suffix is added).
func (d *DataDir) CreateTemp(pattern string) (*os.File, error) {
	if err := dataFileName(pattern); err != nil {
		return nil, err
	}
	prefix, suffix := pattern, ""
	if star := strings.LastIndexByte(pattern, '*'); star >= 0 {
		prefix, suffix = pattern[:star], pattern[star+1:]
	}
	for attempt := 0; attempt < 100; attempt++ {
		file, err := d.OpenFile(prefix+rand.Text()+suffix, os.O_RDWR|os.O_CREATE|os.O_EXCL, PrivateFileMode)
		if !errors.Is(err, os.ErrExist) {
			return file, err
		}
	}
	return nil, fmt.Errorf("create private data temp: %w", os.ErrExist)
}
