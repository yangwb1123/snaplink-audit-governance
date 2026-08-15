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
	"time"

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

	// lockMode/lockValidity/lockUnit are the bucket default retention rule
	// reported by GetObjectLockConfig. lockedFakeS3() defaults them to
	// COMPLIANCE/365/DAYS (the only state Ready accepts); tests script the
	// deletability states (nil default, GOVERNANCE, zero validity, invalid
	// unit, hostile mode) by mutating them.
	lockMode     *minio.RetentionMode
	lockValidity *uint
	lockUnit     *minio.ValidityUnit

	// Recording (all under mu).
	putCalls int
	putData  [][]byte                 // bytes received by each PutObject (copies)
	putOpts  []minio.PutObjectOptions // options received by each PutObject
	gets     [][]byte                 // bytes served by each GetObject (copies)
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

func (f *fakeS3Client) PutObject(_ context.Context, _, objectName string, reader io.Reader, _ int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
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
	f.putOpts = append(f.putOpts, opts)
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
	return f.lockStatus, f.lockMode, f.lockValidity, f.lockUnit, nil
}

func (f *fakeS3Client) GetBucketVersioning(_ context.Context, _ string) (minio.BucketVersioningConfiguration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.versioning, nil
}

// lockedFakeS3 returns a scripted client that Ready passes: bucket exists,
// Object Lock "Enabled" with a COMPLIANCE/365/DAYS default retention rule
// (the only default-rentention state Ready accepts, R-1), versioning
// "Enabled".
func lockedFakeS3() *fakeS3Client {
	mode := minio.Compliance
	validity := uint(365)
	unit := minio.Days
	return &fakeS3Client{
		objects:      map[string][]byte{},
		bucket:       true,
		lockStatus:   "Enabled",
		lockMode:     &mode,
		lockValidity: &validity,
		lockUnit:     &unit,
		versioning:   minio.BucketVersioningConfiguration{Status: "Enabled"},
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
// without versioning, or whose default retention rule is not COMPLIANCE with
// positive validity (R-1) must fail Ready so /readyz surfaces the WORM
// misconfiguration instead of silently accepting a mutable archive. The
// deletability states (no default, GOVERNANCE, zero validity, invalid unit,
// unknown/hostile mode) each fail with a distinct, actionable error class
// (R-3a vs R-3b) that is mutually exclusive by marker.
func TestS3StoreReadyVerifiesObjectLockAndVersioning(t *testing.T) {
	const bucket = "worm-audit"
	u32 := func(v uint) *uint { return &v }
	mode := func(m minio.RetentionMode) *minio.RetentionMode { return &m }
	unit := func(u minio.ValidityUnit) *minio.ValidityUnit { return &u }

	t.Run("misconfigured buckets fail", func(t *testing.T) {
		cases := []struct {
			name       string
			mutate     func(*fakeS3Client)
			wantErr    string
			notWantErr string
			wantBucket bool
		}{
			{"missing bucket", func(f *fakeS3Client) { f.bucket = false }, "does not exist", "", false},
			{"no lock config", func(f *fakeS3Client) { f.lockErr = errors.New("NoSuchObjectLockConfiguration") }, "no object lock configuration", "", false},
			{"lock disabled", func(f *fakeS3Client) { f.lockStatus = "" }, "not enabled", "", false},
			{"versioning disabled", func(f *fakeS3Client) { f.versioning = minio.BucketVersioningConfiguration{} }, "versioning", "", false},
			// R-3a class: no/zero default retention or invalid unit.
			{"no default retention rule", func(f *fakeS3Client) { f.lockMode, f.lockValidity, f.lockUnit = nil, nil, nil },
				"no default retention", "GOVERNANCE", true},
			{"zero-day default retention", func(f *fakeS3Client) { f.lockValidity = u32(0) },
				"no default retention", "GOVERNANCE", true},
			{"invalid validity unit", func(f *fakeS3Client) { f.lockUnit = unit("MONTHS") },
				"no default retention", "GOVERNANCE", true},
			// R-3b class: any non-COMPLIANCE mode with positive validity.
			{"governance default retention", func(f *fakeS3Client) { f.lockMode = mode(minio.Governance) },
				"GOVERNANCE", "no default retention", true},
			{"unknown non-compliance mode", func(f *fakeS3Client) { f.lockMode = mode("CUSTOM") },
				"CUSTOM", "no default retention", true},
			// F2: a hostile backend can report a mode carrying control chars;
			// the R-3b echo must sanitize it (no raw newline/ESC, bounded
			// length) while staying actionable and marker-exclusive.
			{"hostile mode with control chars", func(f *fakeS3Client) { f.lockMode = mode("GOVERNANCE\ninjected\x1b[31m") },
				"default retention", "no default retention", true},
		}
		for _, test := range cases {
			t.Run(test.name, func(t *testing.T) {
				client := lockedFakeS3()
				test.mutate(client)
				store := &S3Store{client: client, bucket: bucket}
				err := store.Ready(context.Background())
				if err == nil {
					t.Fatal("Ready must fail on a misconfigured bucket")
				}
				msg := err.Error()
				if !strings.Contains(msg, test.wantErr) {
					t.Fatalf("Ready error=%q, want mention of %q", msg, test.wantErr)
				}
				if test.notWantErr != "" && strings.Contains(msg, test.notWantErr) {
					t.Fatalf("Ready error=%q must NOT contain %q (marker exclusivity R-3)", msg, test.notWantErr)
				}
				if test.wantBucket && !strings.Contains(msg, bucket) {
					t.Fatalf("Ready error=%q must name the bucket %q (AC-3)", msg, bucket)
				}
				if strings.Contains(msg, "\n") || strings.Contains(msg, "\x1b") {
					t.Fatalf("Ready error=%q must not carry raw control characters (log injection, CWE-117)", msg)
				}
				if len([]rune(msg)) > 512 {
					t.Fatalf("Ready error length=%d exceeds the bounded 512 runes", len([]rune(msg)))
				}
			})
		}
	})
	t.Run("properly locked bucket is ready", func(t *testing.T) {
		store := &S3Store{client: lockedFakeS3(), bucket: bucket}
		if err := store.Ready(context.Background()); err != nil {
			t.Fatalf("locked bucket must be ready: %v", err)
		}
	})
	t.Run("compliance positive validity is the only accepted default", func(t *testing.T) {
		u32 := func(v uint) *uint { return &v }
		mode := func(m minio.RetentionMode) *minio.RetentionMode { return &m }
		unit := func(u minio.ValidityUnit) *minio.ValidityUnit { return &u }
		cases := []struct {
			name   string
			mutate func(*fakeS3Client)
		}{
			{"compliance positive days", func(f *fakeS3Client) {
				f.lockMode, f.lockValidity, f.lockUnit = mode(minio.Compliance), u32(365), unit(minio.Days)
			}},
			{"compliance positive years", func(f *fakeS3Client) {
				f.lockMode, f.lockValidity, f.lockUnit = mode(minio.Compliance), u32(2), unit(minio.Years)
			}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				client := lockedFakeS3()
				tc.mutate(client)
				store := &S3Store{client: client, bucket: bucket}
				if err := store.Ready(context.Background()); err != nil {
					t.Fatalf("Ready must accept COMPLIANCE with positive validity: %v", err)
				}
			})
		}
	})
}

// TestS3StorePutCarriesPerObjectRetention pins AC-2 leg (i) (F1): a store
// built with a positive retainFor writes every object with explicit
// COMPLIANCE retention reaching at least now+retainFor, so the StatusArchived
// receipt is true regardless of bucket-default drift (probe timing becomes
// irrelevant to the write's protection). The lower-bound assertion cannot
// false-fail on execution time. A zero/negative retainFor is a fail-closed
// construction error; the legacy constructors (retainFor=0) record no
// explicit retention — leg (ii) — never a silent unprotected default.
func TestS3StorePutCarriesPerObjectRetention(t *testing.T) {
	const (
		bucket = "worm-audit"
		key    = "events/demo/evt.json"
	)
	payload := []byte(`{"event_id":"evt"}`)

	t.Run("every put carries compliance retention reaching now+retainFor", func(t *testing.T) {
		client := lockedFakeS3()
		retainFor := 24 * time.Hour
		store, err := NewS3StoreWithRetention(client, bucket, retainFor)
		if err != nil {
			t.Fatal(err)
		}
		now := time.Now()
		keys := []string{key, "events/demo/evt2.json"}
		for _, k := range keys {
			if err := store.Put(context.Background(), k, payload); err != nil {
				t.Fatalf("put %s: %v", k, err)
			}
		}
		client.mu.Lock()
		defer client.mu.Unlock()
		if len(client.putOpts) != 2 {
			t.Fatalf("putOpts recorded=%d, want 2", len(client.putOpts))
		}
		for i, opts := range client.putOpts {
			if opts.Mode != minio.Compliance {
				t.Fatalf("put %d opts.Mode=%q, want COMPLIANCE", i, opts.Mode)
			}
			if opts.RetainUntilDate.IsZero() || opts.RetainUntilDate.Before(now.Add(retainFor)) {
				t.Fatalf("put %d RetainUntilDate=%v, want >= now+%v", i, opts.RetainUntilDate, retainFor)
			}
			if opts.ContentType != "application/json" {
				t.Fatalf("put %d ContentType=%q, want application/json", i, opts.ContentType)
			}
		}
	})

	t.Run("fail-closed construction rejects non-positive retainFor", func(t *testing.T) {
		client := lockedFakeS3()
		for _, bad := range []time.Duration{0, -time.Hour} {
			if _, err := NewS3StoreWithRetention(client, bucket, bad); err == nil {
				t.Fatalf("NewS3StoreWithRetention(%v) must fail closed", bad)
			}
		}
	})

	t.Run("legacy construction is leg ii and records no explicit retention", func(t *testing.T) {
		// NewS3StoreWithClient (and &S3Store{} literals) keep retainFor=0:
		// Put sends no explicit retention and objects inherit the
		// Ready-enforced COMPLIANCE bucket default. This pins "never a silent
		// unprotected default" — production wiring (runtimeconfig) can never
		// reach this state for an S3 archive.
		client := lockedFakeS3()
		store := NewS3StoreWithClient(client, bucket)
		if err := store.Put(context.Background(), key, payload); err != nil {
			t.Fatal(err)
		}
		client.mu.Lock()
		defer client.mu.Unlock()
		if len(client.putOpts) != 1 {
			t.Fatalf("putOpts recorded=%d, want 1", len(client.putOpts))
		}
		if client.putOpts[0].Mode != "" || !client.putOpts[0].RetainUntilDate.IsZero() {
			t.Fatalf("legacy put opts=%+v, want no explicit retention (leg ii)", client.putOpts[0])
		}
	})

	t.Run("sanitizeModeEcho bounds hostile mode echoes", func(t *testing.T) {
		if got := sanitizeModeEcho(minio.RetentionMode("GOVERNANCE\ninjected\x1b[31m")); strings.Contains(got, "\n") || strings.Contains(got, "\x1b") {
			t.Fatalf("sanitizeModeEcho must strip control chars, got %q", got)
		}
		if got := sanitizeModeEcho(minio.RetentionMode("COMPLIANCE")); got != "COMPLIANCE" {
			t.Fatalf("sanitizeModeEcho must pass legitimate modes through, got %q", got)
		}
		long := strings.Repeat("A", 200)
		if got := sanitizeModeEcho(minio.RetentionMode(long)); len([]rune(got)) > maxModeEchoRunes {
			t.Fatalf("sanitizeModeEcho must cap at %d runes, got %d", maxModeEchoRunes, len([]rune(got)))
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
