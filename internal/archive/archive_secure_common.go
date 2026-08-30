package archive

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/snaplink/audit-governance/internal/fsutil"
)

const maxKeyBytes = fsutil.MaxS3KeyBytes

var ErrArchiveKeyTooLong = errors.New("archive key exceeds the destination length limit")

type rootState struct {
	needsParentSync bool
	basePath        string
}

// checkKeyLength validates the key before resolving or creating any part of
// the local archive tree.
func checkKeyLength(key string) error {
	if len(key) > maxKeyBytes {
		return fmt.Errorf("%w: key %q is %d bytes (limit %d)", ErrArchiveKeyTooLong, key, len(key), maxKeyBytes)
	}
	for _, component := range strings.Split(key, "/") {
		if len(component) > fsutil.MaxFileStoreComponentBytes {
			return fmt.Errorf("%w: key %q component %q is %d bytes (limit %d)", ErrArchiveKeyTooLong, key, component, len(component), fsutil.MaxFileStoreComponentBytes)
		}
	}
	return nil
}

// validateArchiveKey applies the historical lexical contract. The secure
// implementations then resolve the resulting components relative to an open
// root descriptor rather than joining them into a path for filesystem use.
func validateArchiveKey(dir, key string) (string, error) {
	if dir == "" {
		return "", fmt.Errorf("archive directory is not configured")
	}
	clean := filepath.Clean(filepath.FromSlash(key))
	if clean == "." || filepath.IsAbs(clean) || clean == ".." ||
		strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("archive key %q escapes the archive directory", key)
	}
	return clean, nil
}

func archiveDisplayPath(dir, cleanKey string) string {
	return filepath.Join(filepath.Clean(dir), filepath.FromSlash(cleanKey))
}

func nonRegularArchiveError(path string) error {
	return fmt.Errorf("archive path %s exists and is not a regular file", path)
}
