// Package fsutil holds the shared durability helpers for file-backed
// persistence: fsync of a written file and of the directory that contains a
// renamed file. A rename is not durable until the parent directory entry is
// synced; without these, a crash right after rename can lose the last
// committed state (snapshot, replay marks, archive objects). It also owns
// the injective, reversible archive-key component framing (EncodeKeyComponent
// / DecodeKeyComponent) so every writer of archive object keys shares one
// collision-free implementation.
package fsutil

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// SyncFile flushes file contents to stable storage. The caller must have
// written and closed (or be about to close) the file; Sync returns an error
// when the underlying device reports a failure, so a silently lost write is
// surfaced instead of being reported as committed.
func SyncFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open %s for sync: %w", path, err)
	}
	defer file.Close()
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync %s: %w", path, err)
	}
	return nil
}

// SyncDir flushes the directory entry of path so a completed rename survives
// a crash. Opening a directory read-only and syncing it is the standard
// pattern; it is a no-op on filesystems without directory fsync support.
func SyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory %s for sync: %w", path, err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync directory %s: %w", path, err)
	}
	return nil
}

// SyncDirChain flushes, leaf-to-root, every directory on the path from dir up
// to and including root. A file created at a nested key is only fully durable
// when each ancestor directory's entry for its child has been synced: syncing
// just the archive root (or just the immediate parent) leaves a crash window
// in which a freshly created intermediate directory — and everything under it
// — can vanish after a power failure. dir must be root or a descendant of
// root; syncDir is the per-directory fsync operation and defaults to SyncDir
// when nil. The first failure aborts the walk and is returned wrapped, so
// callers can distinguish a chain-sync failure from a pre-existing one via
// errors.Is.
func SyncDirChain(root, dir string, syncDir func(string) error) error {
	if syncDir == nil {
		syncDir = SyncDir
	}
	root = filepath.Clean(root)
	var chain []string
	for d := filepath.Clean(dir); ; d = filepath.Dir(d) {
		chain = append(chain, d)
		if d == root {
			break
		}
		if filepath.Dir(d) == d {
			// Walked past the root without finding it: the caller's path
			// is not under the configured root (defensive; cannot happen
			// for paths built with filepath.Join(root, key)).
			return fmt.Errorf("sync chain: %s is not under root %s", dir, root)
		}
	}
	for i := 0; i < len(chain); i++ {
		if err := syncDir(chain[i]); err != nil {
			return fmt.Errorf("sync directory chain %s: %w", chain[i], err)
		}
	}
	return nil
}

// KeyComponentEmptyMarker is the encoded form of the empty key component:
// non-empty (a bare component would collapse an archive key to a double
// slash), and never produced by encoding any input, because a bare '%' is
// outside the image of EncodeKeyComponent — every '%' it emits is followed
// by exactly two hex digits. The empty component and "unnamed" therefore
// can no longer collapse onto the same key component.
const KeyComponentEmptyMarker = "%"

// MaxFileStoreComponentBytes is POSIX NAME_MAX: the maximum byte length of a
// single path component on the FileStore backend. An encoded archive
// component longer than this fails at MkdirAll/OpenFile with ENAMETOOLONG;
// internal/archive's pre-flight checkKeyLength rejects it with the typed
// ErrArchiveKeyTooLong before any filesystem mutation.
const MaxFileStoreComponentBytes = 255

// MaxS3KeyBytes is the S3 object-key length limit (1024 bytes). A composite
// archive key longer than this is rejected by internal/archive's pre-flight
// checkKeyLength with the typed ErrArchiveKeyTooLong.
const MaxS3KeyBytes = 1024

// hexUpperDigits is the uppercase hex alphabet used by the percent-escape
// (RFC 3986 §2.1 recommends uppercase; the documented archive-key examples
// such as events/a%3Fb/… depend on it).
const hexUpperDigits = "0123456789ABCDEF"

// isSafeComponentByte reports whether c is in the safe archive-key alphabet
// [a-zA-Z0-9._-]. Safe bytes pass through EncodeKeyComponent verbatim, so
// UUID-derived identifiers (the platform contract) produce byte-identical
// archive keys to the historical lossy framing: only punctuation and
// non-ASCII inputs change encoding.
func isSafeComponentByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' ||
		c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.'
}

// EncodeKeyComponent returns the injective, reversible key-component
// encoding of value. Safe-alphabet bytes [a-zA-Z0-9._-] pass through
// verbatim; every other UTF-8 byte is emitted as '%' plus two uppercase hex
// digits (per byte, so multi-byte runes are deterministic and never
// coalesced); '%' is itself escaped (a literal '%' followed by two hex
// digits is never ambiguous with an escape); "" maps to
// KeyComponentEmptyMarker; whole components "." / ".." are escaped to
// "%2E" / "%2E%2E" so no encoded component forms a traversal segment. The
// result is non-empty, contains no '/' or '\\', never equals "." / "..",
// and is never absolute, so archive keys built from encoded components stay
// root-contained (containedPath in internal/archive). Encoding is a
// deterministic pure function of the byte string (stable across processes
// and ArchivePending retries).
//
// Injectivity is by construction, not by fuzz: safe bytes never include '%'
// and every emitted '%' is followed by exactly two hex digits, so the
// encoding is a prefix code — distinct inputs always produce distinct
// outputs, and DecodeKeyComponent(EncodeKeyComponent(x)) == x for every
// input x. The fuzz test is a regression pin for this argument, not its
// proof.
func EncodeKeyComponent(value string) string {
	if value == "" {
		return KeyComponentEmptyMarker
	}
	if value == "." {
		return "%2E"
	}
	if value == ".." {
		return "%2E%2E"
	}
	// Preallocate the worst-case encoded length (3× input bytes): the builder
	// never reallocates on the hot archive path — at most one allocation
	// total per encoded component.
	var b strings.Builder
	b.Grow(3 * len(value))
	for i := 0; i < len(value); i++ {
		if c := value[i]; isSafeComponentByte(c) {
			b.WriteByte(c)
		} else {
			b.WriteByte('%')
			b.WriteByte(hexUpperDigits[c>>4])
			b.WriteByte(hexUpperDigits[c&0x0f])
		}
	}
	return b.String()
}

// hexDigitValue converts one ASCII hex digit to its value. Both cases are
// accepted so operator tooling can decode hand-written keys.
func hexDigitValue(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// DecodeKeyComponent inverts EncodeKeyComponent: decode(encode(x)) == x for
// every input x. It returns a deterministic error for strings outside the
// encoder's image (a bare '%', a '%' not followed by two hex digits, or an
// unescaped non-safe byte); it never panics. Production code does not call
// Decode — archive keys are stored and read by path — it exists to make the
// encoding's reversibility a testable contract and to support operator
// tooling.
func DecodeKeyComponent(value string) (string, error) {
	if value == KeyComponentEmptyMarker {
		return "", nil
	}
	var b strings.Builder
	b.Grow(3 * len(value)) // worst case is 3 bytes per input byte; one buffer covers every input
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case isSafeComponentByte(c):
			b.WriteByte(c)
		case c == '%':
			if i+2 >= len(value) {
				return "", fmt.Errorf("invalid component %q: truncated escape", value)
			}
			hi, ok1 := hexDigitValue(value[i+1])
			lo, ok2 := hexDigitValue(value[i+2])
			if !ok1 || !ok2 {
				return "", fmt.Errorf("invalid component %q: non-hex escape", value)
			}
			b.WriteByte(hi<<4 | lo)
			i += 2
		default:
			return "", fmt.Errorf("invalid component %q: unescaped byte 0x%02x", value, c)
		}
	}
	return b.String(), nil
}

// DeepestExistingAncestor returns the deepest directory on the path to dir
// that already exists (walking upward with os.Stat; the filesystem root
// always exists and terminates the walk). Only these directories have
// durable dentries already; every directory between the returned root and
// dir is created by the caller's MkdirAll and must itself be synced after
// creation. A Stat error other than "confirmed present" is treated as
// not-existing: over-syncing a pre-existing ancestor is harmless (one extra
// fsync), while under-syncing a freshly created intermediate would reopen
// the M-12 window — the probe can therefore never compromise durability.
func DeepestExistingAncestor(dir string) string {
	for d := filepath.Clean(dir); ; d = filepath.Dir(d) {
		if _, err := os.Stat(d); err == nil {
			return d
		}
		parent := filepath.Dir(d)
		if parent == d {
			return d // filesystem root always exists
		}
	}
}
