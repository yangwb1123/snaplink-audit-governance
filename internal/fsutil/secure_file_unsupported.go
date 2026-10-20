//go:build !linux

package fsutil

import (
	"fmt"
	"os"
)

func unsupportedDataFile() error {
	return fmt.Errorf("%w: secure local data operations require Linux openat primitives", ErrUnsafeFile)
}

func OpenDataDir(string, bool) (*DataDir, error) { return nil, unsupportedDataFile() }
func (*DataDir) OpenFile(string, int, os.FileMode) (*os.File, error) {
	return nil, unsupportedDataFile()
}
func (*DataDir) Rename(string, string) error { return unsupportedDataFile() }
func (*DataDir) Remove(string) error         { return unsupportedDataFile() }
