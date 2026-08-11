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
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/snaplink/audit-governance/internal/fsutil"
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
	// The directory entry of the new object must be durable before Put
	// reports success (M-12); a crash right after close can otherwise lose
	// the object from a power failure even though the file itself was
	// synced.
	if err := fsutil.SyncDir(f.Dir); err != nil {
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

// S3Client is the subset of the minio client used by S3Store. The interface
// exists so the WORM contract (byte-identical idempotent Put, Object Lock
// readiness probe) is testable with a fake client; *minio.Client satisfies it
// via minioS3Client. It is exported (together with NewS3StoreWithClient) so
// callers outside this package — the governance worker's tests — can inject a
// scripted client; production callers keep using NewS3Store.
type S3Client interface {
	StatObject(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error)
	BucketExists(ctx context.Context, bucketName string) (bool, error)
	GetObjectLockConfig(ctx context.Context, bucketName string) (objectLock string, mode *minio.RetentionMode, validity *uint, unit *minio.ValidityUnit, err error)
	GetBucketVersioning(ctx context.Context, bucketName string) (minio.BucketVersioningConfiguration, error)
}

// s3Client is the historical unexported name, aliased so existing internal
// references (S3Store.client, minioS3Client) and same-package tests compile
// unchanged. The alias adds no runtime branch.
type s3Client = S3Client

// S3Store writes to an Object Lock enabled bucket. Put is idempotent only
// for byte-identical retries: an existing object is read back and compared
// (mirroring FileStore.verifyExistingObject), so a corrupted or tampered
// object is rejected loudly instead of being reported as archived. Ready
// verifies the bucket actually has Object Lock and versioning enabled, so a
// misconfigured bucket cannot silently degrade the WORM guarantee.
type S3Store struct {
	client s3Client
	bucket string
}

// minioS3Client adapts *minio.Client to S3Client. The concrete GetObject
// returns *minio.Object (an io.ReadCloser); Go interfaces require exact
// return types, so the adapter narrows the return and forwards the rest.
type minioS3Client struct {
	client *minio.Client
}

func (m *minioS3Client) StatObject(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
	return m.client.StatObject(ctx, bucketName, objectName, opts)
}

func (m *minioS3Client) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	return m.client.PutObject(ctx, bucketName, objectName, reader, objectSize, opts)
}

func (m *minioS3Client) GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
	return m.client.GetObject(ctx, bucketName, objectName, opts)
}

func (m *minioS3Client) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	return m.client.BucketExists(ctx, bucketName)
}

func (m *minioS3Client) GetObjectLockConfig(ctx context.Context, bucketName string) (string, *minio.RetentionMode, *uint, *minio.ValidityUnit, error) {
	return m.client.GetObjectLockConfig(ctx, bucketName)
}

func (m *minioS3Client) GetBucketVersioning(ctx context.Context, bucketName string) (minio.BucketVersioningConfiguration, error) {
	return m.client.GetBucketVersioning(ctx, bucketName)
}

// NewS3StoreWithClient builds an S3Store over an already-constructed client.
// It exists so tests can inject a scripted client (see S3Client); production
// callers use NewS3Store, whose signature and behavior are unchanged.
func NewS3StoreWithClient(client S3Client, bucket string) *S3Store {
	return &S3Store{client: client, bucket: bucket}
}

// NewS3Store builds the Object Lock archive backed by an S3-compatible
// endpoint. It returns an error only for invalid client configuration; the
// bucket itself is verified by Ready.
func NewS3Store(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*S3Store, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("create s3 client: %w", err)
	}
	return &S3Store{client: &minioS3Client{client: client}, bucket: bucket}, nil
}

func (s *S3Store) Put(ctx context.Context, key string, data []byte) error {
	key = strings.TrimPrefix(key, "/")
	// 幂等：对象已存在时必须逐字节核对，不能仅凭 Stat 命中就视为已归档 ——
	// 否则被篡改/损坏的对象会被静默报告为 archived（与 FileStore 的
	// verifyExistingObject 对齐）。Object Lock 版本化下先写后查也能收敛，
	// 但 Stat 前置避免无意义的上传。
	if _, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{}); err == nil {
		return s.verifyExistingObject(ctx, key, data)
	}
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: "application/json"})
	if err != nil {
		return fmt.Errorf("s3 put %s: %w", key, err)
	}
	return nil
}

// verifyExistingObject returns nil only when the object already at key is
// byte-identical to data. A mismatch — or an object that cannot be read and
// therefore cannot be verified — is an error, so tampering becomes loud
// instead of being silently accepted as archived. The existing object is
// never modified or removed (WORM).
func (s *S3Store) verifyExistingObject(ctx context.Context, key string, data []byte) error {
	object, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("archive object %s already exists and cannot be verified: %w", key, err)
	}
	defer object.Close()
	existing, err := io.ReadAll(io.LimitReader(object, int64(len(data))+1))
	if err != nil {
		return fmt.Errorf("archive object %s already exists and cannot be verified: %w", key, err)
	}
	if !bytes.Equal(existing, data) {
		return fmt.Errorf("archive object %s already exists with different content", key)
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
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("archive bucket %s does not exist", s.bucket)
	}
	// The WORM guarantee for S3 rests on the bucket configuration: without
	// Object Lock (and its mandatory versioning) an overwrite or delete is
	// physically possible, so the archive silently loses immutability.
	// Ready must fail on a misconfigured bucket instead of trusting it.
	enabled, _, _, _, err := s.client.GetObjectLockConfig(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("archive bucket %s has no object lock configuration: %w", s.bucket, err)
	}
	if enabled != "Enabled" {
		return fmt.Errorf("archive bucket %s object lock is not enabled", s.bucket)
	}
	versioning, err := s.client.GetBucketVersioning(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("archive bucket %s versioning probe failed: %w", s.bucket, err)
	}
	if !versioning.Enabled() {
		return fmt.Errorf("archive bucket %s does not have versioning enabled", s.bucket)
	}
	return nil
}
