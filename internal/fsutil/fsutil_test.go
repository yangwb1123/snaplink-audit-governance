package fsutil

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

// TestSyncDirChainLeafToRoot pins the chain order: leaf first, archive root
// last, every intermediate directory in between. A single directory (root ==
// dir) collapses to exactly one sync, preserving the historical SyncDir
// behavior for flat keys.
func TestSyncDirChainLeafToRoot(t *testing.T) {
	cases := []struct {
		name string
		root string
		dir  string
		want []string
	}{
		{
			name: "nested chain",
			root: "/archive",
			dir:  "/archive/events/demo/stream",
			want: []string{"/archive/events/demo/stream", "/archive/events/demo", "/archive/events", "/archive"},
		},
		{
			name: "flat key parent is direct child",
			root: "/archive",
			dir:  "/archive/exports",
			want: []string{"/archive/exports", "/archive"},
		},
		{
			name: "root equals dir",
			root: "/archive",
			dir:  "/archive",
			want: []string{"/archive"},
		},
		{
			name: "trailing slashes are normalized",
			root: "/archive/",
			dir:  "/archive/events/",
			want: []string{"/archive/events", "/archive"},
		},
		{
			name: "relative root and dir",
			root: "archive",
			dir:  "archive/events/demo",
			want: []string{"archive/events/demo", "archive/events", "archive"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			err := SyncDirChain(tc.root, tc.dir, func(path string) error {
				got = append(got, path)
				return nil
			})
			if err != nil {
				t.Fatalf("SyncDirChain: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("sync order = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSyncDirChainFailurePropagates pins abort-on-first-failure: the walk
// stops at the failing member (leaf-to-root, so members below the failure
// are synced first) and the injected error is reachable via errors.Is.
func TestSyncDirChainFailurePropagates(t *testing.T) {
	sentinel := errors.New("injected sync failure")
	called := 0
	err := SyncDirChain("/a", "/a/b/c", func(path string) error {
		called++
		if path == "/a/b" {
			return sentinel
		}
		return nil
	})
	if err == nil {
		t.Fatal("SyncDirChain must propagate the sync error")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error %v must wrap the injected sentinel", err)
	}
	if called != 2 {
		t.Fatalf("walk aborted after %d syncs, want 2 (leaf then failing parent)", called)
	}
}

// TestSyncDirChainRootNotAncestor pins the defensive guard: a dir outside
// the root is a configuration error, not a silent no-op.
func TestSyncDirChainRootNotAncestor(t *testing.T) {
	called := 0
	err := SyncDirChain("/a", "/b/c", func(string) error {
		called++
		return nil
	})
	if err == nil {
		t.Fatal("SyncDirChain must reject a dir outside the root")
	}
	if called != 0 {
		t.Fatalf("no sync must run for an invalid chain, got %d", called)
	}
}

// TestDeepestExistingAncestor pins the shared probe (REQ-1): it returns the
// deepest directory on the path that already exists, walking upward from the
// requested dir, so callers can extend a post-MkdirAll sync chain to cover
// every directory they themselves created. The filesystem root always
// exists and terminates the walk.
func TestDeepestExistingAncestor(t *testing.T) {
	t.Run("existing dir returns itself", func(t *testing.T) {
		dir := t.TempDir()
		if got := DeepestExistingAncestor(dir); got != dir {
			t.Fatalf("DeepestExistingAncestor(%q) = %q, want %q", dir, got, dir)
		}
	})

	t.Run("missing nested path returns deepest existing ancestor", func(t *testing.T) {
		base := t.TempDir()
		got := DeepestExistingAncestor(filepath.Join(base, "missing", "deeper"))
		if got != base {
			t.Fatalf("DeepestExistingAncestor = %q, want %q", got, base)
		}
	})

	t.Run("multi-level missing returns existing ancestor", func(t *testing.T) {
		base := t.TempDir()
		got := DeepestExistingAncestor(filepath.Join(base, "a", "b", "c"))
		if got != base {
			t.Fatalf("DeepestExistingAncestor = %q, want %q", got, base)
		}
	})

	t.Run("bare missing name under existing dir", func(t *testing.T) {
		base := t.TempDir()
		got := DeepestExistingAncestor(filepath.Join(base, "missing"))
		if got != base {
			t.Fatalf("DeepestExistingAncestor = %q, want %q", got, base)
		}
	})

	t.Run("relative path walk terminates at cwd", func(t *testing.T) {
		base := t.TempDir()
		t.Chdir(base)
		got := DeepestExistingAncestor(filepath.Join("missing", "deeper"))
		if got != "." {
			t.Fatalf("DeepestExistingAncestor = %q, want %q", got, ".")
		}
	})
}

// TestEncodeKeyComponentCorpusInjectiveRoundTrip is AC-1 T1: every pair the
// historical lossy safeName collapsed onto one archive key must encode to
// distinct values, and decode(encode(x)) == x for the whole corpus. Every
// corpus input must be ValidKeyComponent-valid (the rule that gates the
// generic identifier layers), and safe-alphabet inputs must be emitted
// verbatim (R3).
func TestEncodeKeyComponentCorpusInjectiveRoundTrip(t *testing.T) {
	corpus := []string{
		// Collision pairs of the historical lossy safeName.
		"a:b", "a_b",
		"a?b", "a*b",
		"tëstant", "t_stant",
		"", "unnamed",
		".", "..",
		// R5 escape fixed point: a literal '%' must never collide with a
		// percent escape (naive encoders map both "a%b" and "a%25b" to
		// "a%25b").
		"a%b", "a%25b",
		// R3 identity fixtures (UUID policy: byte-identical to today).
		"a-b_c.d", "audit.event", "tenant-a", "evt-1",
		"123e4567-e89b-12d3-a456-426614174000",
	}
	// Precondition: the corpus is exactly what the validators admit today.
	for _, input := range corpus {
		if err := domain.ValidKeyComponent("corpus", input); err != nil {
			t.Fatalf("corpus input %q must be ValidKeyComponent-valid: %v", input, err)
		}
	}
	seen := map[string]string{} // encoded → input
	for _, input := range corpus {
		got := EncodeKeyComponent(input)
		if prev, exists := seen[got]; exists {
			t.Fatalf("collision: %q and %q both encode to %q", prev, input, got)
		}
		seen[got] = input
		decoded, err := DecodeKeyComponent(got)
		if err != nil {
			t.Fatalf("DecodeKeyComponent(%q) failed: %v", got, err)
		}
		if decoded != input {
			t.Fatalf("round-trip failed: encode(%q)=%q decode=%q", input, got, decoded)
		}
	}
	// R3 pin: safe-alphabet inputs are emitted byte-identical.
	for _, input := range []string{"a-b_c.d", "audit.event", "tenant-a", "evt-1", "123e4567-e89b-12d3-a456-426614174000"} {
		if got := EncodeKeyComponent(input); got != input {
			t.Fatalf("safe-alphabet input %q must be identity-encoded, got %q", input, got)
		}
	}
	// R4/R5 explicit pins: the marker and the exact escape outputs.
	for input, want := range map[string]string{
		"":        "%",
		".":       "%2E",
		"..":      "%2E%2E",
		"a:b":     "a%3Ab",
		"a?b":     "a%3Fb",
		"a*b":     "a%2Ab",
		"tëstant": "t%C3%ABstant",
		"a%b":     "a%25b",
		"a%25b":   "a%2525b",
	} {
		if got := EncodeKeyComponent(input); got != want {
			t.Fatalf("EncodeKeyComponent(%q) = %q, want %q", input, got, want)
		}
	}
}

// TestEncodeKeyComponentFuzzUniqueRoundTrip is AC-1 T2: a seeded PRNG over
// an alphabet including the collision-causing punctuation and a non-ASCII
// rune — deterministic across CI runs. Every accepted component round-trips
// and distinct components never share an encoding.
func TestEncodeKeyComponentFuzzUniqueRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(20240815))
	alphabet := []rune("ab:?*%._-0ë")
	randString := func() string {
		n := 1 + rng.Intn(8)
		runes := make([]rune, n)
		for i := range runes {
			runes[i] = alphabet[rng.Intn(len(alphabet))]
		}
		return string(runes)
	}
	seen := map[string]string{} // encoded → input
	iterations := 10000
	for i := 0; i < iterations; i++ {
		input := randString()
		if err := domain.ValidKeyComponent("fuzz", input); err != nil {
			// The boundary rejects the component before framing; the
			// encoding contract only covers accepted components.
			continue
		}
		got := EncodeKeyComponent(input)
		if prev, exists := seen[got]; exists {
			if prev != input {
				t.Fatalf("iteration %d: fuzz collision: %q and %q share encoding %q", i, prev, input, got)
			}
			continue // deterministic repeat of the same input, not a collision
		}
		seen[got] = input
		decoded, err := DecodeKeyComponent(got)
		if err != nil {
			t.Fatalf("iteration %d: DecodeKeyComponent(%q) failed: %v", i, got, err)
		}
		if decoded != input {
			t.Fatalf("iteration %d: round-trip failed: %q → %q → %q", i, input, got, decoded)
		}
	}
}

// TestEncodeKeyComponentContainment is AC-3 item 2: the encoded output is
// non-empty, contains no '/' or '\\', never equals "." / "..", is never
// absolute, and round-trips. Validator-rejected inputs are included as
// defense-in-depth: the encoding is total, so even a snapshot-injected
// control-character identifier can never frame a traversal or absolute key.
func TestEncodeKeyComponentContainment(t *testing.T) {
	corpus := []string{
		"a:b", "a_b", "a?b", "a*b", "tëstant", "t_stant",
		"a%b", "a%25b", "", "unnamed", ".", "..",
		"a-b_c.d", "audit.event", "tenant-a",
		// Defense-in-depth: inputs the validators reject must still encode
		// to safe components (never a traversal segment, never absolute).
		"a\x1fb", "a b", "a/b", "a\\b", "/abs", "../up", "%2E",
	}
	for _, input := range corpus {
		got := EncodeKeyComponent(input)
		if got == "" {
			t.Fatalf("encode(%q) must be non-empty", input)
		}
		if strings.ContainsAny(got, "/\\") {
			t.Fatalf("encode(%q) = %q contains a path separator", input, got)
		}
		if got == "." || got == ".." {
			t.Fatalf("encode(%q) = %q forms a traversal segment", input, got)
		}
		if strings.HasPrefix(got, "/") || filepath.IsAbs(got) {
			t.Fatalf("encode(%q) = %q is absolute", input, got)
		}
		decoded, err := DecodeKeyComponent(got)
		if err != nil {
			t.Fatalf("DecodeKeyComponent(%q) failed: %v", got, err)
		}
		if decoded != input {
			t.Fatalf("round-trip encode(%q)=%q decode=%q", input, got, decoded)
		}
	}
}

// TestDecodeKeyComponentRejectsNonImage is AC-1's negative pin: strings
// outside the encoder's image (bare '%', truncated escapes, non-hex
// escapes, unescaped non-safe bytes) return a deterministic error and never
// panic.
func TestDecodeKeyComponentRejectsNonImage(t *testing.T) {
	for _, input := range []string{"a%", "a%3", "a%3G", "a%GGb", "a b", "a\x1fb", "a/b"} {
		if _, err := DecodeKeyComponent(input); err == nil {
			t.Errorf("DecodeKeyComponent(%q) must fail for a non-image string", input)
		}
	}
}

// BenchmarkEncodeKeyComponent covers the R3 identity path (UUID), the escape
// path, and the FM-1 adversarial boundaries so the harness (cli.py bench)
// can watch for regressions on the archive hot path.
func BenchmarkEncodeKeyComponent(b *testing.B) {
	inputs := []string{
		"123e4567-e89b-12d3-a456-426614174000", // R3 identity: UUID policy
		"tenant-a:aggregate:invoice:inv-1",     // escaped stream component
		strings.Repeat("?", 85),                // FM-1 boundary: punctuation
		strings.Repeat("界", 28),                // FM-1 boundary: 3-byte runes
	}
	for _, input := range inputs {
		b.Run(fmt.Sprintf("len-%d", len(input)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				EncodeKeyComponent(input)
			}
		})
	}
}

// TestEncodeKeyComponentLengthBounds is F3 (algorithm review): pins the
// FM-1 expansion thresholds the release note documents. 3× expansion means
// the archive package's 255-byte NAME_MAX per-component bound allows at most
// 85 unsafe ASCII bytes / 28 three-byte runes per component, and its
// 1024-byte S3 ceiling allows at most 329 unsafe content bytes in a
// composite key (3·329 + 35 fixed ≤ 1024). The archive pre-flight check
// enforces those bounds; this test pins the encoding expansion itself so the
// documented numbers cannot drift.
func TestEncodeKeyComponentLengthBounds(t *testing.T) {
	if got := len(EncodeKeyComponent(strings.Repeat("?", 85))); got != 255 {
		t.Fatalf("85 '?' must encode to exactly 255 bytes (NAME_MAX ceiling), got %d", got)
	}
	if got := len(EncodeKeyComponent(strings.Repeat("?", 86))); got != 258 {
		t.Fatalf("86 '?' must encode to 258 bytes (over NAME_MAX), got %d", got)
	}
	if got := len(EncodeKeyComponent(strings.Repeat("中", 28))); got != 252 {
		t.Fatalf("28 three-byte runes must encode to 252 bytes (fits NAME_MAX), got %d", got)
	}
	if got := len(EncodeKeyComponent(strings.Repeat("中", 29))); got != 261 {
		t.Fatalf("29 three-byte runes must encode to 261 bytes (over NAME_MAX), got %d", got)
	}
	// S3 composite-key ceiling: 3·329 content bytes + 35 fixed ≤ 1024 fits;
	// 3·330 + 35 = 1025 exceeds it.
	if got := 3*329 + 35; got > 1024 {
		t.Fatalf("composite bound math drifted: 3·329+35=%d must fit the 1024-byte S3 ceiling", got)
	}
	if got := 3*330 + 35; got <= 1024 {
		t.Fatalf("composite bound math drifted: 3·330+35=%d must exceed the 1024-byte S3 ceiling", got)
	}
}

// TestEncodeKeyComponentAtMostOneAllocation is F2 (algorithm review): the
// hot archive path preallocates the 3× worst case, so encoding never
// reallocates — at most one allocation per encoded component (the builder's
// buffer), for both the identity (UUID) and punctuation paths.
func TestEncodeKeyComponentAtMostOneAllocation(t *testing.T) {
	for _, input := range []string{
		"123e4567-e89b-12d3-a456-426614174000", // identity (UUID contract)
		"a:b", "a?b", "tëstant", "a%b", "a\x1fb",
	} {
		allocs := testing.AllocsPerRun(100, func() {
			_ = EncodeKeyComponent(input)
		})
		if allocs > 1 {
			t.Fatalf("EncodeKeyComponent(%q) allocated %.0f times per op, want ≤ 1 (no reallocation)", input, allocs)
		}
	}
}

// TestSyncDirChainNilSyncDir covers the production default: a nil hook falls
// back to SyncDir and a real directory chain (including a freshly created
// intermediate) syncs without error.
func TestSyncDirChainNilSyncDir(t *testing.T) {
	root := filepath.Join(t.TempDir(), "archive")
	if err := os.MkdirAll(filepath.Join(root, "events", "demo"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := SyncDirChain(root, filepath.Join(root, "events", "demo"), nil); err != nil {
		t.Fatalf("SyncDirChain with nil hook: %v", err)
	}
}
