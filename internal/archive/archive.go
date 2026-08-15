// Package archive abstracts the WORM-compatible compliance archive. The
// local file store mirrors the append-only semantics with O_EXCL creation,
// read-only permissions and fsync; the S3 store targets an Object Lock
// bucket (versioning + retention), where deletion becomes a version marker
// instead of physical removal. The S3 store enforces the WORM guarantee:
// Ready requires Object Lock enabled, a bucket default retention rule of
// COMPLIANCE mode with positive validity (DAYS > 0 or YEARS > 0), and
// versioning enabled — a GOVERNANCE or no/zero-validity default is a
// readiness failure, so a misconfigured bucket cannot silently degrade the
// guarantee. Stores built with a positive retainFor additionally write every
// object with explicit per-object COMPLIANCE retention.
package archive

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

	// syncDir is the per-directory fsync hook used by Put's and Ready's
	// directory-chain durability step. It defaults to fsutil.SyncDir; tests
	// replace it to record the ordered sync set or fault-inject a specific
	// path. Nil at construction (all production literals), so there is no
	// constructor and no exported surface change.
	syncDir func(path string) error

	// mu serializes Put/Get so the create→write→sync→remove critical
	// section cannot interleave with a concurrent idempotent retry: without
	// it, a failing Put could remove an object that a concurrent retry had
	// just verified as byte-identical and reported as archived, violating
	// the "nil ⇒ durable" contract. It also prevents torn reads of an
	// object mid-write.
	mu sync.Mutex

	// rootState tracks whether this instance created the archive root and
	// whether its parent dentry has been confirmed durable (see rootState).
	// A single atomic.Value keeps the fields consistent for the lock-free
	// Ready and the locked Put without a data race; lost updates between
	// concurrent observations are benign (any recorded missingBase is a
	// valid chain root that covers the parent).
	rootState atomic.Value
}

// containedPath validates key against the archive-root containment contract
// and returns the joined filesystem path. Keys are relative POSIX-style
// object names; anything absolute, empty, or containing a ".." component
// that escapes the root is rejected before any filesystem mutation, so a
// caller (or a compromised upstream) cannot write or read outside the
// archive directory. The empty-Dir error is byte-identical to Ready's so the
// existing fail-closed preflight message is preserved.
func containedPath(dir, key string) (string, error) {
	if dir == "" {
		return "", fmt.Errorf("archive directory is not configured")
	}
	clean := filepath.Clean(filepath.FromSlash(key))
	if clean == "." || filepath.IsAbs(clean) || clean == ".." ||
		strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("archive key %q escapes the archive directory", key)
	}
	return filepath.Join(dir, clean), nil
}

// rootState is the durability bookkeeping for the archive root within one
// FileStore instance. missingBase is the deepest existing ancestor observed
// when this instance last saw the root missing — the chain root that covers
// every directory created since (including the fresh root's own dentry in
// its parent); missingSeen records that observation. parentDurable records
// that a chain sync reaching missingBase has since succeeded, so later
// operations may terminate the chain at the configured root again.
//
// A root that pre-existed this instance (missingSeen false) is treated as
// durable per the scope guard: steady-state chains keep terminating at the
// configured root. A root created by this instance whose covering sync
// failed keeps missingBase in the chain until one sync succeeds, so a
// nil-returning Put can never treat a non-durable root as durable (M-12).
type rootState struct {
	missingBase   string
	missingSeen   bool
	parentDurable bool
}

// loadRootState returns the current root durability state.
func (f *FileStore) loadRootState() rootState {
	if v := f.rootState.Load(); v != nil {
		return v.(rootState)
	}
	return rootState{}
}

// observeRootMissing records that this operation observed the archive root
// missing before its MkdirAll: the root is (being) created within this
// instance's lifetime, probed is the deepest existing ancestor whose child
// was created, and the parent dentry is unconfirmed until a chain reaching
// probed succeeds.
func (f *FileStore) observeRootMissing(probed string) {
	st := f.loadRootState()
	st.missingSeen = true
	st.missingBase = probed
	st.parentDurable = false
	f.rootState.Store(st)
}

// markRootDurable records that a chain sync reaching the root's parent
// dentry just succeeded.
func (f *FileStore) markRootDurable() {
	st := f.loadRootState()
	st.parentDurable = true
	f.rootState.Store(st)
}

// chainRoot returns the chain root for the durability sync. probed is the
// deepest existing ancestor of f.Dir observed before this operation's
// MkdirAll: when it lies above f.Dir the root is (being) created by this
// operation, so the chain must extend through the parent to make the fresh
// root's dentry durable (M-12). When the root pre-exists, the chain
// terminates at f.Dir — unless this instance itself observed the root
// missing earlier and has not yet confirmed the parent dentry durable (a
// failed first attempt created the root but its covering sync never
// succeeded): then the chain extends to the originally observed deepest
// existing ancestor, so every directory created since is covered.
func (f *FileStore) chainRoot(probed string) string {
	if probed != f.Dir {
		return probed
	}
	st := f.loadRootState()
	if st.missingSeen && !st.parentDurable {
		return st.missingBase
	}
	return f.Dir
}

func (f *FileStore) Put(_ context.Context, key string, data []byte) error {
	path, err := containedPath(f.Dir, key)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	// REQ-2: chain root = deepest ancestor that pre-exists this Put, probed
	// BEFORE MkdirAll so a freshly created archive root is included in the
	// sync chain (its dentry inside the parent must be durable for
	// "nil ⇒ durable", M-12). When f.Dir pre-exists the probe returns f.Dir
	// and the chain is unchanged (root == dir collapses to one sync); a root
	// this instance created earlier without confirming the parent dentry
	// durable keeps the parent in the chain (see chainRoot).
	probed := fsutil.DeepestExistingAncestor(f.Dir)
	root := f.chainRoot(probed)
	if probed != f.Dir {
		f.observeRootMissing(probed)
	}
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
	// reports success (M-12): a crash right after close can otherwise lose
	// the object from a power failure even though the file itself was
	// synced. Directory fsync is per-directory, so the object's immediate
	// parent and every ancestor up to the chain root must each be synced,
	// leaf-to-root; the chain root is the deepest pre-existing ancestor
	// probed before MkdirAll when this Put created the archive root, and
	// the configured f.Dir otherwise. The mutex guarantees no concurrent
	// retry can observe or remove the object mid-sync.
	syncDir := f.syncDir
	if syncDir == nil {
		syncDir = fsutil.SyncDir
	}
	if err := fsutil.SyncDirChain(root, filepath.Dir(path), syncDir); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("sync directory chain for %s: %w", path, err)
	}
	// A chain whose root is above the configured root covered the parent
	// dentry; once that succeeds the root is durable for subsequent
	// operations (chainRoot terminates at f.Dir again).
	if root != f.Dir {
		f.markRootDurable()
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
	path, err := containedPath(f.Dir, key)
	if err != nil {
		return nil, err
	}
	// The mutex also serializes Get against an in-flight Put, so a reader
	// never observes a torn (mid-write) object.
	f.mu.Lock()
	defer f.mu.Unlock()
	return os.ReadFile(path)
}

func (f *FileStore) Ready(_ context.Context) error {
	if f.Dir == "" {
		return fmt.Errorf("archive directory is not configured")
	}
	// REQ-3: probe before MkdirAll so a root freshly created by this Ready
	// is included in the sync chain; its dentry inside the parent is made
	// durable (M-12). A pre-existing root collapses to one sync (root ==
	// dir), so steady-state readiness does not re-sync ancestors above the
	// configured root; a root this instance created earlier without a
	// confirmed parent dentry keeps the parent in the chain (chainRoot).
	probed := fsutil.DeepestExistingAncestor(f.Dir)
	root := f.chainRoot(probed)
	if probed != f.Dir {
		f.observeRootMissing(probed)
	}
	probe := filepath.Join(f.Dir, ".ready-probe")
	if err := os.MkdirAll(f.Dir, 0o750); err != nil {
		return err
	}
	if err := os.WriteFile(probe, []byte("ok"), 0o640); err != nil {
		return err
	}
	if err := os.Remove(probe); err != nil {
		return err
	}
	// The probe-file removal — and, when the root is fresh, the root's own
	// dentry inside its parent — must be durable before Ready reports
	// ready: a non-durable root is not ready, so a chain-sync failure here
	// fails Ready closed.
	syncDir := f.syncDir
	if syncDir == nil {
		syncDir = fsutil.SyncDir
	}
	if err := fsutil.SyncDirChain(root, f.Dir, syncDir); err != nil {
		return fmt.Errorf("sync archive root %s: %w", f.Dir, err)
	}
	if root != f.Dir {
		f.markRootDurable()
	}
	return nil
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
// enforces the WORM guarantee: it requires the bucket to have Object Lock
// enabled, a default retention rule of COMPLIANCE mode with positive
// validity (DAYS > 0 or YEARS > 0), and versioning enabled — a GOVERNANCE or
// no/zero-validity default is a readiness failure, so a misconfigured bucket
// cannot silently degrade the WORM guarantee.
//
// retainFor is the per-object COMPLIANCE retention duration applied by Put
// when positive: every object written inherits explicit COMPLIANCE retention
// until now+retainFor at write time, so the StatusArchived receipt is true
// regardless of bucket-default drift. Zero means leg-(ii)-only: Put sends no
// explicit retention and objects inherit the Ready-enforced bucket default.
// Zero is legal at the archive level for tests and legacy construction;
// production wiring (runtimeconfig) never reaches it for an S3 archive.
type S3Store struct {
	client    s3Client
	bucket    string
	retainFor time.Duration
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
// callers use NewS3Store, whose signature and behavior are unchanged. The
// store is leg-(ii)-only (retainFor=0): Put sends no explicit retention and
// objects inherit the Ready-enforced COMPLIANCE bucket default.
func NewS3StoreWithClient(client S3Client, bucket string) *S3Store {
	return &S3Store{client: client, bucket: bucket}
}

// NewS3StoreWithRetention builds an S3Store over an already-constructed
// client with per-object COMPLIANCE retention applied by every Put. It
// returns an error when retainFor <= 0 — a zero or negative duration is a
// configuration error, never a silent no-retention default (the caller must
// opt into leg (ii) explicitly via NewS3StoreWithClient).
func NewS3StoreWithRetention(client S3Client, bucket string, retainFor time.Duration) (*S3Store, error) {
	if retainFor <= 0 {
		return nil, fmt.Errorf("archive retention duration must be positive, got %v", retainFor)
	}
	return &S3Store{client: client, bucket: bucket, retainFor: retainFor}, nil
}

// NewS3StoreRetention builds the Object Lock archive backed by an
// S3-compatible endpoint with per-object COMPLIANCE retention applied by
// every Put. retainFor must be positive (same fail-closed contract as
// NewS3StoreWithRetention): production wiring never reaches an S3 archive
// that writes without explicit retention. The bucket itself is verified by
// Ready.
func NewS3StoreRetention(endpoint, accessKey, secretKey, bucket string, useSSL bool, retainFor time.Duration) (*S3Store, error) {
	if retainFor <= 0 {
		return nil, fmt.Errorf("archive retention duration must be positive, got %v", retainFor)
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("create s3 client: %w", err)
	}
	return &S3Store{client: &minioS3Client{client: client}, bucket: bucket, retainFor: retainFor}, nil
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
	// 幂等且防篡改，与 FileStore 的 Lstat 处理对齐：Stat 命中时必须逐字节核对
	// （verifyExistingObject）；Stat 失败时必须区分“确实不存在”（只有 minio 的
	// NoSuchKey 才算）与“探针失败”——其余错误（限流、鉴权、网络、被包装的
	// 非 ErrorResponse 错误）一律中止，绝不落入盲写。ToErrorResponse 是类型
	// switch 而非 errors.As，因此被包装的 NoSuchKey 也会被判为失败：偏向
	// fail-closed，永不 fail-open。
	if _, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{}); err == nil {
		return s.verifyExistingObject(ctx, key, data)
	} else if minio.ToErrorResponse(err).Code != minio.NoSuchKey {
		return fmt.Errorf("s3 stat %s: %w", key, err)
	}
	// 此刻 key 被证明不存在。写入后必须读回校验（verifyAfterWrite），关闭
	// Stat→Put 的 TOCTOU 窗口：并发写入者在窗口内抢先落盘的内容绝不会被
	// 当作“已验证”而静默返回 nil。
	// 当 retainFor > 0（leg (i)）时每次写入都携带显式 COMPLIANCE 留存：即使
	// 桶默认留存被移除/降级，写入窗口内的对象也受 COMPLIANCE 保护，
	// StatusArchived 回执与写入时刻的保护状态无关（F1 漂移窗口关闭）。
	opts := minio.PutObjectOptions{ContentType: "application/json"}
	if s.retainFor > 0 {
		opts.Mode = minio.Compliance
		opts.RetainUntilDate = time.Now().Add(s.retainFor)
	}
	if _, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), opts); err != nil {
		return fmt.Errorf("s3 put %s: %w", key, err)
	}
	return s.verifyAfterWrite(ctx, key, data)
}

// readBack returns up to len(data)+1 bytes of the object at key. The bound
// mirrors the existing verifier: a superset object is detected as differing
// content instead of being read unboundedly. Never writes.
func (s *S3Store) readBack(ctx context.Context, key string, data []byte) ([]byte, error) {
	object, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer object.Close()
	return io.ReadAll(io.LimitReader(object, int64(len(data))+1))
}

// verifyExistingObject returns nil only when the object already at key is
// byte-identical to data. A mismatch — or an object that cannot be read and
// therefore cannot be verified — is an error, so tampering becomes loud
// instead of being silently accepted as archived. The existing object is
// never modified or removed (WORM).
func (s *S3Store) verifyExistingObject(ctx context.Context, key string, data []byte) error {
	existing, err := s.readBack(ctx, key, data)
	if err != nil {
		return fmt.Errorf("archive object %s already exists and cannot be verified: %w", key, err)
	}
	if !bytes.Equal(existing, data) {
		return fmt.Errorf("archive object %s already exists with different content", key)
	}
	return nil
}

// verifyAfterWrite returns nil only when the object just written at key reads
// back byte-identical to data. It closes the stat→put TOCTOU window: a
// concurrent writer that created or replaced content between the NoSuchKey
// stat and this read is surfaced as an error instead of being reported as
// archived. The object is never modified or removed (WORM); a later
// byte-identical retry converges via the existing-object path.
func (s *S3Store) verifyAfterWrite(ctx context.Context, key string, data []byte) error {
	existing, err := s.readBack(ctx, key, data)
	if err != nil {
		return fmt.Errorf("archive object %s cannot be verified after write: %w", key, err)
	}
	if !bytes.Equal(existing, data) {
		// Keep the existing mismatch wording verbatim (F-3.2): the object at
		// the key now holds different content, e.g. a concurrent writer's
		// version, so the failure reads exactly like the pre-existing
		// mismatch case for operator triage.
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
	enabled, mode, validity, unit, err := s.client.GetObjectLockConfig(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("archive bucket %s has no object lock configuration: %w", s.bucket, err)
	}
	if enabled != "Enabled" {
		return fmt.Errorf("archive bucket %s object lock is not enabled", s.bucket)
	}
	// R-1 (AC-1): the WORM guarantee requires the bucket default retention to
	// be COMPLIANCE with positive validity. Every Put without explicit
	// retention inherits this default at write time, so a GOVERNANCE, absent,
	// or zero-validity default leaves every archived object deletable and
	// must fail Ready closed. Two distinct, mutually exclusive error classes:
	// R-3a (no/zero default retention or invalid unit) and R-3b (any
	// non-COMPLIANCE mode).
	if mode == nil || validity == nil || unit == nil || *validity == 0 ||
		(*unit != minio.Days && *unit != minio.Years) {
		return fmt.Errorf("archive bucket %s has no default retention rule with positive validity: set the bucket default retention to COMPLIANCE mode with validity > 0 (Days > 0 or Years > 0); without it, every archived object is deletable by any principal with s3:DeleteObject", s.bucket)
	}
	if *mode != minio.Compliance {
		return fmt.Errorf("archive bucket %s default retention mode is %s, not COMPLIANCE: GOVERNANCE-mode default retention lets principals with s3:BypassGovernanceRetention delete retained objects; set the bucket default retention mode to COMPLIANCE", s.bucket, sanitizeModeEcho(*mode))
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

// maxModeEchoRunes bounds the R-3b diagnostic echo of the backend-reported
// retention mode. The value is attacker-influenced: minio-go parses Mode
// from GetObjectLockConfig XML without IsValid() on read, and XML character
// data may carry control characters, so a hostile S3-compatible endpoint
// could otherwise inject log lines or unbounded output via this echo
// (CWE-117). Legitimate modes pass through unchanged and stay actionable.
const maxModeEchoRunes = 64

// sanitizeModeEcho renders a retention mode for operator diagnostics without
// log injection: control characters are replaced with '?' and the value is
// capped at maxModeEchoRunes runes. Legitimate modes (GOVERNANCE, COMPLIANCE,
// vendor-specific strings) pass through unchanged, so the R-3b echo stays
// actionable while the raw backend value can never reach a log verbatim.
// Mirrors the per-package sanitizer precedent (kafka.sanitizeLogField,
// runtimeconfig.sanitizeAddr); consolidating into a shared package is a
// non-goal.
func sanitizeModeEcho(mode minio.RetentionMode) string {
	sanitized := strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '?'
		}
		return r
	}, string(mode))
	runes := []rune(sanitized)
	if len(runes) > maxModeEchoRunes {
		runes = runes[:maxModeEchoRunes]
	}
	return string(runes)
}
