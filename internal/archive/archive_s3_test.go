package archive

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/minio/minio-go/v7"
)

// fakeS3Client is a scripted s3Client for the WORM contract tests: objects
// live in a map (tampering is simulated by mutating the stored bytes), and
// the bucket configuration flags control Ready outcomes.
type fakeS3Client struct {
	objects    map[string][]byte
	bucket     bool
	lockErr    error
	lockStatus string
	versioning minio.BucketVersioningConfiguration
	statErr    error
	getErr     error
}

func (f *fakeS3Client) StatObject(_ context.Context, _, objectName string, _ minio.StatObjectOptions) (minio.ObjectInfo, error) {
	if f.statErr != nil {
		return minio.ObjectInfo{}, f.statErr
	}
	if _, ok := f.objects[objectName]; !ok {
		return minio.ObjectInfo{}, errors.New("no such object")
	}
	return minio.ObjectInfo{Key: objectName}, nil
}

func (f *fakeS3Client) PutObject(_ context.Context, _, objectName string, reader io.Reader, _ int64, _ minio.PutObjectOptions) (minio.UploadInfo, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return minio.UploadInfo{}, err
	}
	f.objects[objectName] = data
	return minio.UploadInfo{Key: objectName}, nil
}

func (f *fakeS3Client) GetObject(_ context.Context, _, objectName string, _ minio.GetObjectOptions) (io.ReadCloser, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	data, ok := f.objects[objectName]
	if !ok {
		return nil, errors.New("no such object")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (f *fakeS3Client) BucketExists(_ context.Context, _ string) (bool, error) {
	return f.bucket, nil
}

func (f *fakeS3Client) GetObjectLockConfig(_ context.Context, _ string) (string, *minio.RetentionMode, *uint, *minio.ValidityUnit, error) {
	if f.lockErr != nil {
		return "", nil, nil, nil, f.lockErr
	}
	return f.lockStatus, nil, nil, nil, nil
}

func (f *fakeS3Client) GetBucketVersioning(_ context.Context, _ string) (minio.BucketVersioningConfiguration, error) {
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
