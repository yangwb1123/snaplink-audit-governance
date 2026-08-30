//go:build !linux

package archive

import (
	"context"
	"fmt"
)

const secureArchiveUnsupported = "secure local archive operations are unsupported on this platform"

func unsupportedArchive() error { return fmt.Errorf("%s", secureArchiveUnsupported) }

func (f *FileStore) Put(_ context.Context, key string, _ []byte) error {
	if _, err := validateArchiveKey(f.Dir, key); err != nil {
		return err
	}
	if err := checkKeyLength(key); err != nil {
		return err
	}
	return unsupportedArchive()
}

func (f *FileStore) Get(_ context.Context, key string) ([]byte, error) {
	if _, err := validateArchiveKey(f.Dir, key); err != nil {
		return nil, err
	}
	if err := checkKeyLength(key); err != nil {
		return nil, err
	}
	return nil, unsupportedArchive()
}

func (f *FileStore) Ready(_ context.Context) error {
	if f.Dir == "" {
		return fmt.Errorf("archive directory is not configured")
	}
	return unsupportedArchive()
}
