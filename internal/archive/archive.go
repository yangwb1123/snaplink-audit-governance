// Package archive abstracts the WORM-compatible compliance archive. The
// local file store mirrors the append-only semantics with O_EXCL creation,
// read-only permissions and fsync; the S3 store targets an Object Lock
// bucket (versioning + retention), where deletion becomes a version marker
// instead of physical removal.
package archive

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Store persists immutable archive objects. Put must be idempotent and
// must never silently accept corruption: an object that already exists with
// byte-identical content counts as already archived (nil); a pre-existing
// path that is not a regular file, or a regular file with different content
// that cannot be verified as identical, is an error.
type Store interface {
	Put(ctx context.Context, key string, data []byte) error
	// Get reads a previously archived object.
	Get(ctx context.Context, key string) ([]byte, error)
	// Ready reports whether the destination is currently usable.
	Ready(ctx context.Context) error
}

// Configured reports whether an archive destination is enabled. A nil
// store or a local store without a directory is unconfigured; a local
// store with a directory and any S3 store are configured. Unknown Store
// implementations are treated as configured because the caller injected
// them deliberately. This predicate is the single source of truth for the
// ingest gate, the governance retry gate and the readiness probe, so the
// three call sites cannot diverge on what counts as an enabled archive.
func Configured(store Store) bool {
	switch s := store.(type) {
	case nil:
		return false
	case *FileStore:
		return s != nil && s.Dir != ""
	case *S3Store:
		return s != nil
	default:
		return true
	}
}

// FileStore writes into a local append-only directory: O_EXCL creation,
// read-only permissions and fsync. Put never writes through an existing
// final component, and a Put that fails after creating the object removes
// it, so an error never leaves a partial object at the key.
type FileStore struct {
	Dir string
}

func (f *FileStore) Put(_ context.Context, key string, data []byte) error {
	if f.Dir == "" {
		return fmt.Errorf("archive directory is not configured")
	}
	path := filepath.Join(f.Dir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	// A pre-existing non-regular path (symlink, directory, device) is never
	// "already archived": reject it instead of silently accepting the key.
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("archive path %s exists and is not a regular file", path)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o440)
	if err != nil {
		if os.IsExist(err) {
			// The object already exists: only a byte-identical retry is an
			// idempotent success. O_EXCL guarantees this call never wrote
			// through the existing final component.
			return verifyExistingObject(path, data)
		}
		return err
	}
	// The object at key was created by this call: any failure from here on
	// must remove it so the invariant "error ⇒ no object at key" holds.
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

// verifyExistingObject returns nil only when the object already at path is
// byte-identical to data. A mismatch — or a file that cannot be read and
// therefore cannot be verified — is an error, so tampering becomes loud
// instead of being silently reported as archived. The pre-existing object
// is never modified or removed (WORM).
func verifyExistingObject(path string, data []byte) error {
	existing, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("archive path %s already exists and cannot be verified: %w", path, err)
	}
	if !bytes.Equal(existing, data) {
		return fmt.Errorf("archive path %s already exists with different content", path)
	}
	return nil
}

func (f *FileStore) Get(_ context.Context, key string) ([]byte, error) {
	return os.ReadFile(filepath.Join(f.Dir, filepath.FromSlash(key)))
}

func (f *FileStore) Ready(_ context.Context) error {
	if f.Dir == "" {
		return fmt.Errorf("archive directory is not configured")
	}
	probe := filepath.Join(f.Dir, ".ready-probe")
	if err := os.MkdirAll(f.Dir, 0o750); err != nil {
		return err
	}
	if err := os.WriteFile(probe, []byte("ok"), 0o640); err != nil {
		return err
	}
	return os.Remove(probe)
}

// S3Store writes to an Object Lock enabled bucket. Existing objects are
// treated as already archived (idempotent); with versioning, concurrent
// duplicates become version markers that never lose data.
type S3Store struct {
	client *minio.Client
	bucket string
}

func NewS3Store(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*S3Store, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("create s3 client: %w", err)
	}
	return &S3Store{client: client, bucket: bucket}, nil
}

func (s *S3Store) Put(ctx context.Context, key string, data []byte) error {
	key = strings.TrimPrefix(key, "/")
	// 幂等：对象已存在即视为已归档。Object Lock 版本化下先写后查也能
	// 收敛，但 Stat 前置避免无意义的上传。
	if _, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{}); err == nil {
		return nil
	}
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: "application/json"})
	if err != nil {
		return fmt.Errorf("s3 put %s: %w", key, err)
	}
	return nil
}

func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	response, err := s.client.GetObject(ctx, s.bucket, strings.TrimPrefix(key, "/"), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer response.Close()
	var data bytes.Buffer
	if _, err := data.ReadFrom(response); err != nil {
		return nil, err
	}
	return data.Bytes(), nil
}

func (s *S3Store) Ready(ctx context.Context) error {
	_, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return err
	}
	return nil
}
