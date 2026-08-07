package store

import (
	"errors"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

// TestValidTenantID is AC-1 at the key-framing primitive: control characters
// (incl. the 0x1F key separator), whitespace and path separators are rejected
// with domain.ErrInvalid; everything else passes. Rejection-based (not
// allowlist) semantics: non-ASCII letters and punctuation stay valid.
func TestValidTenantID(t *testing.T) {
	rejected := []string{
		"a\x1fb",   // KeySeparator injection: the cross-tenant leak vector
		"\x00",     // NUL
		"a\tb",     // TAB
		"a\nb",     // LF
		"a\rb",     // CR
		"a\vb",     // VT
		"a\fb",     // FF
		"a b",      // space
		" a",       // leading space
		"a\u00a0b", // NBSP (unicode.IsSpace)
		"a/b",      // path separator
		`a\b`,      // path separator (windows)
	}
	for _, id := range rejected {
		if err := ValidTenantID(id); !errors.Is(err, domain.ErrInvalid) {
			t.Errorf("ValidTenantID(%q) = %v, want ErrInvalid", id, err)
		}
	}
	accepted := []string{
		"tenant-a",
		"a-b_c.d",
		"tëstant", // non-ASCII letters stay valid (rejection-based rule)
		"a:b",
		"123",
	}
	for _, id := range accepted {
		if err := ValidTenantID(id); err != nil {
			t.Errorf("ValidTenantID(%q) = %v, want nil", id, err)
		}
	}
}

// TestSplitTenantKey is REQ-2's fail-closed parse foundation: a key belongs
// to a tenant only when it contains exactly one KeySeparator. The empty-rest
// key (StreamKey(t, "")) must keep parsing — the defensive empty-stream path
// in the governance scans depends on it.
func TestSplitTenantKey(t *testing.T) {
	cases := []struct {
		key      string
		tenantID string
		rest     string
		ok       bool
	}{
		{StreamKey("a", "s"), "a", "s", true},
		{StreamKey("a", ""), "a", "", true}, // empty rest must not regress (F3)
		{KeySeparator + "s", "", "s", true}, // empty tenant component parses
		{"a\x1fb\x1fs", "", "", false},      // multi-separator: fail-closed, no tenant
		{"a", "", "", false},                // no separator: not a composite key
		{"", "", "", false},
	}
	for _, tc := range cases {
		tenantID, rest, ok := SplitTenantKey(tc.key)
		if tenantID != tc.tenantID || rest != tc.rest || ok != tc.ok {
			t.Errorf("SplitTenantKey(%q) = (%q, %q, %v), want (%q, %q, %v)",
				tc.key, tenantID, rest, ok, tc.tenantID, tc.rest, tc.ok)
		}
	}
}

// TestStreamKeyPrefixInjectiveForValidTenantIDs is REQ-3's no-key-format-
// change property: for ValidTenantID-passing tenant IDs the composite key
// space is prefix-injective, so distinct (tenant, stream) pairs never
// collide. This is exactly what ValidTenantID guarantees: without the
// separator in tenant IDs, the first KeySeparator splits deterministically.
func TestStreamKeyPrefixInjectiveForValidTenantIDs(t *testing.T) {
	tenantIDs := []string{"a", "ab", "a-b", "tëstant", "a:b", "123"}
	allStreamIDs := []string{"", "s", "s1", "x\x1f y-invalid-stream-but-distinct"}
	for _, tenantID := range tenantIDs {
		if err := ValidTenantID(tenantID); err != nil {
			t.Fatalf("test fixture %q must be valid: %v", tenantID, err)
		}
	}
	// Injectivity holds for every stream ID: without a separator in the
	// tenant ID, the first KeySeparator splits deterministically.
	seen := map[string]string{}
	for _, tenantID := range tenantIDs {
		for _, streamID := range allStreamIDs {
			key := StreamKey(tenantID, streamID)
			if other, exists := seen[key]; exists {
				t.Fatalf("key collision: StreamKey(%q,%q) == StreamKey(%q,...) == %q", tenantID, streamID, other, key)
			}
			seen[key] = tenantID
		}
	}
	// Round-trip holds for well-formed stream IDs (no separator).
	wellFormed := []string{"", "s", "s1"}
	for _, tenantID := range tenantIDs {
		for _, streamID := range wellFormed {
			if tid, sid, ok := SplitTenantKey(StreamKey(tenantID, streamID)); !ok || tid != tenantID || sid != streamID {
				t.Fatalf("round-trip failed for StreamKey(%q,%q): got (%q,%q,%v)", tenantID, streamID, tid, sid, ok)
			}
		}
	}
}
