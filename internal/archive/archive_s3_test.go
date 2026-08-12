package archive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/minio/minio-go/v7"
)

// fakeS3Client is a scripted s3Client for the WORM contract tests: objects
// live in a map (tampering is simulated by mutating the stored bytes), and
// the bucket configuration flags control Ready outcomes. All state is
// guarded by mu so the concurrency acceptance test runs clean under -race.
type fakeS3Client struct {
	mu         sync.Mutex
	objects    map[string][]byte
	bucket     bool
	lockErr    error
	lockStatus string
	versioning minio.BucketVersioningConfiguration

	// statErrs is a per-call script queue for StatObject: each call pops the
	// front; a single-element queue repeats forever (per-test scripting).
	// Absence is modeled by a typed NoSuchKey, matching the real client's
	// HEAD 404 mapping — never a plain error.
	statErrs []error
	// getErr forces GetObject to fail (sticky).
	getErr error
	// putHook, if set, transforms the bytes before they are persisted —
	// simulates a concurrent writer between the stat and the verify GET.
	putHook func(objectName string, data []byte) []byte

	// Recording (all under mu).
	putCalls int
	putData  [][]byte // bytes received by each PutObject (copies)
	gets     [][]byte // bytes served by each GetObject (copies)
}

func (f *fakeS3Client) StatObject(_ context.Context, bucketName, objectName string, _ minio.StatObjectOptions) (minio.ObjectInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.statErrs) > 0 {
		err := f.statErrs[0]
		if len(f.statErrs) > 1 {
			f.statErrs = f.statErrs[1:]
		}
		return minio.ObjectInfo{}, err
	}
	if _, ok := f.objects[objectName]; !ok {
		return minio.ObjectInfo{}, minio.ErrorResponse{
			StatusCode: http.StatusNotFound,
			Code:       minio.NoSuchKey,
			BucketName: bucketName,
			Key:        objectName,
		}
	}
	return minio.ObjectInfo{Key: objectName}, nil
}

func (f *fakeS3Client) PutObject(_ context.Context, _, objectName string, reader io.Reader, _ int64, _ minio.PutObjectOptions) (minio.UploadInfo, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return minio.UploadInfo{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.putHook != nil {
		data = f.putHook(objectName, data)
	}
	f.objects[objectName] = append([]byte(nil), data...)
	f.putCalls++
	f.putData = append(f.putData, append([]byte(nil), data...))
	return minio.UploadInfo{Key: objectName}, nil
}

func (f *fakeS3Client) GetObject(_ context.Context, _, objectName string, _ minio.GetObjectOptions) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	data, ok := f.objects[objectName]
	if !ok {
		return nil, errors.New("no such object")
	}
	// Record a copy of exactly what is served: a mutator flipping the map
	// after this point cannot falsify the verify record.
	f.gets = append(f.gets, append([]byte(nil), data...))
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (f *fakeS3Client) BucketExists(_ context.Context, _ string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bucket, nil
}

func (f *fakeS3Client) GetObjectLockConfig(_ context.Context, _ string) (string, *minio.RetentionMode, *uint, *minio.ValidityUnit, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lockErr != nil {
		return "", nil, nil, nil, f.lockErr
	}
	return f.lockStatus, nil, nil, nil, nil
}

func (f *fakeS3Client) GetBucketVersioning(_ context.Context, _ string) (minio.BucketVersioningConfiguration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.versioning, nil
}

func lockedFakeS3() *fakeS3Client {
	return &fakeS3Client{
		objects:    map[string][]byte{},
		bucket:     true,
		lockStatus: "Enabled",
		versioning: minio.BucketVersioningConfiguration{Status: "Enabled"},
	}
}

// TestS3StorePutVerifiesExistingObjectBytes pins H-2: an existing object is
// only an idempotent success when it is byte-identical. A tampered or
// unreadable object must fail loudly, exactly like FileStore's
// verifyExistingObject — the S3 path must not report "archived" for
// corruption the file store would reject.
func TestS3StorePutVerifiesExistingObjectBytes(t *testing.T) {
	client := lockedFakeS3()
	store := &S3Store{client: client, bucket: "worm-audit"}
	key := "events/demo/evt.json"
	payload := []byte(`{"event_id":"evt"}`)
	if err := store.Put(context.Background(), key, payload); err != nil {
		t.Fatal(err)
	}
	// Byte-identical retry: idempotent success.
	if err := store.Put(context.Background(), key, payload); err != nil {
		t.Fatalf("byte-identical retry must succeed: %v", err)
	}
	// Tampered object: loud error, object left untouched (WORM).
	client.objects[key] = []byte(`tampered`)
	if err := store.Put(context.Background(), key, payload); err == nil {
		t.Fatal("Put with mismatched content must fail instead of reporting archived")
	}
	if got := string(client.objects[key]); got != `tampered` {
		t.Fatalf("verify must never modify the existing object: %s", got)
	}
	// Unreadable existing object: loud error, never "already archived".
	client.objects[key] = payload
	client.getErr = errors.New("read-back denied")
	if err := store.Put(context.Background(), key, payload); err == nil {
		t.Fatal("unverifiable existing object must fail")
	}
	client.getErr = nil
	// Larger existing object (content mismatch even though payload is a prefix).
	client.objects[key] = append(append([]byte{}, payload...), []byte("extra")...)
	if err := store.Put(context.Background(), key, payload); err == nil {
		t.Fatal("superset object must be detected as different content")
	}
}

// TestS3StoreReadyVerifiesObjectLockAndVersioning pins the readiness
// contract: a bucket without Object Lock configuration, without lock enabled,
// or without versioning must fail Ready so /readyz surfaces the WORM
// misconfiguration instead of silently accepting a mutable archive.
func TestS3StoreReadyVerifiesObjectLockAndVersioning(t *testing.T) {
	t.Run("misconfigured buckets fail", func(t *testing.T) {
		cases := []struct {
			name    string
			mutate  func(*fakeS3Client)
			wantErr string
		}{
			{"missing bucket", func(f *fakeS3Client) { f.bucket = false }, "does not exist"},
			{"no lock config", func(f *fakeS3Client) { f.lockErr = errors.New("NoSuchObjectLockConfiguration") }, "no object lock configuration"},
			{"lock disabled", func(f *fakeS3Client) { f.lockStatus = "" }, "not enabled"},
			{"versioning disabled", func(f *fakeS3Client) { f.versioning = minio.BucketVersioningConfiguration{} }, "versioning"},
		}
		for _, test := range cases {
			t.Run(test.name, func(t *testing.T) {
				client := lockedFakeS3()
				test.mutate(client)
				store := &S3Store{client: client, bucket: "worm-audit"}
				err := store.Ready(context.Background())
				if err == nil {
					t.Fatal("Ready must fail on a misconfigured bucket")
				}
				if !bytes.Contains([]byte(err.Error()), []byte(test.wantErr)) {
					t.Fatalf("Ready error=%v, want mention of %q", err, test.wantErr)
				}
			})
		}
	})
	t.Run("properly locked bucket is ready", func(t *testing.T) {
		store := &S3Store{client: lockedFakeS3(), bucket: "worm-audit"}
		if err := store.Ready(context.Background()); err != nil {
			t.Fatalf("locked bucket must be ready: %v", err)
		}
	})
}

// TestS3StorePutFailsClosedOnStatError pins F-1/AC-1: any StatObject error
// that does not prove absence (minio NoSuchKey) aborts the Put and never
// reaches PutObject. A wrapped NoSuchKey is deliberately NOT absence —
// ToErrorResponse is a type switch, so the wrap yields Code "" and the
// discrimination stays fail-closed.
func TestS3StorePutFailsClosedOnStatError(t *testing.T) {
	key := "events/demo/e.json"
	payload := []byte(`{"event_id":"evt"}`)
	transient := []struct {
		name  string
		cause error
	}{
		{"context deadline exceeded", context.DeadlineExceeded},
		{"slow down throttling", minio.ErrorResponse{StatusCode: http.StatusServiceUnavailable, Code: "SlowDown"}},
		{"wrapped no such key is not absence", fmt.Errorf("proxy: %w", minio.ErrorResponse{Code: minio.NoSuchKey})},
	}
	for _, tc := range transient {
		t.Run(tc.name, func(t *testing.T) {
			client := lockedFakeS3()
			client.statErrs = []error{tc.cause}
			store := &S3Store{client: client, bucket: "worm-audit"}
			err := store.Put(context.Background(), key, payload)
			if err == nil {
				t.Fatal("Put must fail closed on a non-NoSuchKey stat error")
			}
			if !errors.Is(err, tc.cause) {
				t.Fatalf("Put error %v must wrap the cause %v", err, tc.cause)
			}
			if !strings.Contains(err.Error(), "s3 stat") {
				t.Fatalf("Put error %v must mention the s3 stat stage", err)
			}
			if client.putCalls != 0 {
				t.Fatalf("PutObject must never be invoked on stat failure, got %d calls", client.putCalls)
			}
		})
	}
}

// TestS3StorePutAbsentKeyHappyPath pins F-2/AC-2: a typed NoSuchKey stat is
// the only path into PutObject, and the fresh put then verifies its own
// write (read-back byte-identical ⇒ nil).
func TestS3StorePutAbsentKeyHappyPath(t *testing.T) {
	client := lockedFakeS3()
	store := &S3Store{client: client, bucket: "worm-audit"}
	key := "events/demo/evt.json"
	payload := []byte(`{"event_id":"evt"}`)
	if err := store.Put(context.Background(), key, payload); err != nil {
		t.Fatalf("fresh put on absent key must succeed: %v", err)
	}
	if client.putCalls != 1 {
		t.Fatalf("PutObject must be invoked exactly once, got %d", client.putCalls)
	}
	if !bytes.Equal(client.objects[key], payload) {
		t.Fatalf("stored bytes = %q, want %q", client.objects[key], payload)
	}
	// The key now exists: a byte-identical retry takes the stat-hit path and
	// still converges to nil.
	if err := store.Put(context.Background(), key, payload); err != nil {
		t.Fatalf("byte-identical retry after fresh put must succeed: %v", err)
	}
}

// TestS3StorePutVerifiesAfterWrite pins F-3/AC-3: after a successful
// PutObject on the fresh path, the object must be read back and byte-compared
// before nil is returned. A concurrent writer's different content (mutation
// hook) or an unreadable read-back must surface loudly and never return nil;
// the object at the key is left exactly as the backend stored it (WORM).
func TestS3StorePutVerifiesAfterWrite(t *testing.T) {
	key := "events/demo/evt.json"
	payload := []byte(`{"event_id":"evt"}`)

	t.Run("concurrent writer different content fails loudly", func(t *testing.T) {
		client := lockedFakeS3()
		client.putHook = func(_ string, _ []byte) []byte { return []byte(`tampered`) }
		store := &S3Store{client: client, bucket: "worm-audit"}
		err := store.Put(context.Background(), key, payload)
		if err == nil {
			t.Fatal("Put must fail when the written object reads back different")
		}
		if !strings.Contains(err.Error(), "already exists with different content") {
			t.Fatalf("mismatch error %v must reuse the existing-content wording", err)
		}
		if got := string(client.objects[key]); got != `tampered` {
			t.Fatalf("verify must never modify the object after write: %s", got)
		}
	})

	t.Run("byte-identical read-back succeeds", func(t *testing.T) {
		client := lockedFakeS3()
		store := &S3Store{client: client, bucket: "worm-audit"}
		if err := store.Put(context.Background(), key, payload); err != nil {
			t.Fatalf("fresh put with verified read-back must succeed: %v", err)
		}
	})

	t.Run("unreadable read-back fails loudly", func(t *testing.T) {
		client := lockedFakeS3()
		client.getErr = errors.New("read-back denied")
		store := &S3Store{client: client, bucket: "worm-audit"}
		err := store.Put(context.Background(), key, payload)
		if err == nil {
			t.Fatal("Put must fail when the verify read-back errors")
		}
		if !strings.Contains(err.Error(), "cannot be verified after write") {
			t.Fatalf("Put error %v must mention the verify-after-write stage", err)
		}
		if got := string(client.objects[key]); got != string(payload) {
			t.Fatalf("verify-after-write failure must leave the written object in place: %s", got)
		}
	})
}

// TestS3StorePutConcurrentNoUnverifiedNil pins F-3/AC-4 under -race: with
// two writers racing the same key and a third goroutine flipping the stored
// bytes between the caller's payload and corrupt content, a Put may only
// return nil when the verify read-back actually served the caller's payload.
func TestS3StorePutConcurrentNoUnverifiedNil(t *testing.T) {
	const (
		iters   = 300
		numPuts = 2
		key     = "events/demo/evt.json"
		payload = `{"event_id":"evt"}`
		corrupt = `tampered`
	)
	client := lockedFakeS3()
	store := &S3Store{client: client, bucket: "worm-audit"}

	var wg sync.WaitGroup
	var nilPuts int
	var countMu sync.Mutex
	for p := 0; p < numPuts; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				if err := store.Put(context.Background(), key, []byte(payload)); err == nil {
					countMu.Lock()
					nilPuts++
					countMu.Unlock()
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			client.mu.Lock()
			if bytes.Equal(client.objects[key], []byte(payload)) {
				client.objects[key] = []byte(corrupt)
			} else {
				client.objects[key] = []byte(payload)
			}
			client.mu.Unlock()
		}
	}()
	wg.Wait()

	client.mu.Lock()
	defer client.mu.Unlock()
	verified := 0
	for _, got := range client.gets {
		if bytes.Equal(got, []byte(payload)) {
			verified++
		}
	}
	if nilPuts != verified {
		t.Fatalf("nil Puts (%d) must equal verify GETs that served the payload (%d); unverified content was accepted", nilPuts, verified)
	}
	if len(client.putData) != client.putCalls {
		t.Fatalf("recording mismatch: putData=%d putCalls=%d", len(client.putData), client.putCalls)
	}
	for _, got := range client.putData {
		if !bytes.Equal(got, []byte(payload)) && !bytes.Equal(got, []byte(corrupt)) {
			t.Fatalf("recorded put bytes %q are neither payload nor corrupt", got)
		}
	}
}
