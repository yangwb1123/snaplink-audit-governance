package security

import (
	"encoding/json"
	"testing"
)

func TestFieldEncryptionAndSearchDigest(t *testing.T) {
	encoded, err := EncryptJSON("alice@example.test", "tenant-key", "tenant-a/email/evt-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) < len(encryptedPrefix) || encoded[:len(encryptedPrefix)] != encryptedPrefix {
		t.Fatalf("missing encryption prefix")
	}
	decoded, err := DecryptJSON(encoded, "tenant-key", "tenant-a/email/evt-1")
	if err != nil {
		t.Fatal(err)
	}
	if decoded != "alice@example.test" {
		t.Fatalf("unexpected decoded value: %v", decoded)
	}
	first, err := SearchDigest("alice@example.test", "tenant-key")
	if err != nil {
		t.Fatal(err)
	}
	second, err := SearchDigest("alice@example.test", "tenant-key")
	if err != nil || first != second {
		t.Fatalf("search digest is not stable")
	}
}

func TestSearchDigestBoundIsTenantAndFieldScoped(t *testing.T) {
	value := "alice@example.test"
	key := "tenant-key"
	// Stable within one (tenantID, field) binding.
	first, err := SearchDigestBound(value, key, "tenant-a", "email")
	if err != nil {
		t.Fatal(err)
	}
	second, err := SearchDigestBound(value, key, "tenant-a", "email")
	if err != nil || first != second {
		t.Fatalf("bound digest is not stable: %q vs %q err=%v", first, second, err)
	}
	// Self-describing format marker; ':' is outside the base64url alphabet,
	// so a legacy unbound digest can never carry it (collision impossible).
	if !IsBoundSearchDigest(first) {
		t.Fatalf("bound digest must carry the sd2: prefix: %q", first)
	}
	// Different tenant or different field for the identical value → different
	// digests: cross-tenant correlation by digest equality is impossible.
	otherTenant, err := SearchDigestBound(value, key, "tenant-b", "email")
	if err != nil || otherTenant == first {
		t.Fatalf("same value in another tenant must differ: %q err=%v", otherTenant, err)
	}
	otherField, err := SearchDigestBound(value, key, "tenant-a", "ip_address")
	if err != nil || otherField == first {
		t.Fatalf("same value under another field must differ: %q err=%v", otherField, err)
	}
	// Wrong key → different digest.
	wrongKey, err := SearchDigestBound(value, "other-key", "tenant-a", "email")
	if err != nil || wrongKey == first {
		t.Fatalf("bound digest must depend on the key: %q err=%v", wrongKey, err)
	}
	// The bound digest never collides with the legacy unbound format.
	legacy, err := SearchDigest(value, key)
	if err != nil {
		t.Fatal(err)
	}
	if legacy == first {
		t.Fatal("bound and legacy digests must differ")
	}
	if IsBoundSearchDigest(legacy) {
		t.Fatalf("legacy digest must not carry the bound marker: %q", legacy)
	}
}

func TestStripSearchDigestsRemovesRecursivelyOnCopy(t *testing.T) {
	payload := map[string]any{
		"email__search_digest": "sd2:abc",
		"email":                "alice@example.test",
		"nested": map[string]any{
			"inner__search_digest": "x",
			"keep":                 "y",
			"deeper":               map[string]any{"z__search_digest": "z", "v": 1},
		},
		"list": []any{
			map[string]any{"item__search_digest": "q", "id": 1},
			"plain",
		},
		"large": json.Number("9007199254740993"),
	}
	copied, err := StripSearchDigests(payload)
	if err != nil {
		t.Fatal(err)
	}
	// The original map is untouched (deep copy, never in-place mutation).
	if _, ok := payload["email__search_digest"]; !ok {
		t.Fatal("original payload must be left intact")
	}
	nested := payload["nested"].(map[string]any)
	if _, ok := nested["inner__search_digest"]; !ok {
		t.Fatal("original nested map must be left intact")
	}
	// The copy is stripped at every nesting depth.
	if _, ok := copied["email__search_digest"]; ok {
		t.Fatal("top-level digest key must be stripped")
	}
	copiedNested := copied["nested"].(map[string]any)
	if _, ok := copiedNested["inner__search_digest"]; ok {
		t.Fatal("nested digest key must be stripped")
	}
	deeper := copiedNested["deeper"].(map[string]any)
	if _, ok := deeper["z__search_digest"]; ok {
		t.Fatal("deeply nested digest key must be stripped")
	}
	items := copied["list"].([]any)
	if _, ok := items[0].(map[string]any)["item__search_digest"]; ok {
		t.Fatal("digest key inside an array element must be stripped")
	}
	// Non-digest content survives and numbers keep exact digits (int64 > 2^53).
	if copied["email"] != "alice@example.test" || copiedNested["keep"] != "y" || items[1] != "plain" {
		t.Fatalf("non-digest content must survive: %+v", copied)
	}
	if copied["large"] != json.Number("9007199254740993") {
		t.Fatalf("large number lost exact digits: %v (%T)", copied["large"], copied["large"])
	}
}

func TestExportBytesRoundTrip(t *testing.T) {
	plain := []byte("line1\nline2\n")
	sealed, err := EncryptBytes(plain, "export-key")
	if err != nil {
		t.Fatal(err)
	}
	if string(sealed[:len(exportPrefix)]) != exportPrefix {
		t.Fatalf("sealed output missing prefix: %q", sealed[:16])
	}
	if len(sealed) <= len(plain) {
		t.Fatalf("sealed length %d not larger than plain %d", len(sealed), len(plain))
	}
	decrypted, err := DecryptBytes(sealed, "export-key")
	if err != nil {
		t.Fatal(err)
	}
	if string(decrypted) != string(plain) {
		t.Fatalf("round trip mismatch: %q", decrypted)
	}
	// 错误密钥或错误前缀必须失败。
	if _, err := DecryptBytes(sealed, "wrong-key"); err == nil {
		t.Fatal("wrong key must fail")
	}
	if _, err := DecryptBytes([]byte("plaintext"), "export-key"); err == nil {
		t.Fatal("missing prefix must fail")
	}
}

// TestExportBindingFraming pins the exact byte layout of ExportBinding,
// including a hostile tenant ID whose embedded '|', ':' and digit runs must
// not shift the length-prefixed framing boundaries.
func TestExportBindingFraming(t *testing.T) {
	got := string(ExportBinding("tenant-a", "export-abc123"))
	want := "export:v2:8:tenant-a|13:export-abc123"
	if got != want {
		t.Fatalf("ExportBinding(%q, %q) = %q, want %q", "tenant-a", "export-abc123", got, want)
	}
	// len("5:abc|2:xy") == 10: the length prefix fixes the exact byte span.
	got = string(ExportBinding("5:abc|2:xy", "job"))
	want = "export:v2:10:5:abc|2:xy|3:job"
	if got != want {
		t.Fatalf("ExportBinding hostile = %q, want %q", got, want)
	}
}

// TestExportBindingInjective is the property-form of the collision-freedom
// argument: over a grid of hostile (tenantID, jobID) values, every distinct
// pair derives distinct binding bytes, and re-derivation is deterministic.
func TestExportBindingInjective(t *testing.T) {
	values := []string{"a", "5:abc|2:xy", "10:abc", "a|b", "1:2", ":", "|", ""}
	seen := map[string][2]string{}
	for _, tenant := range values {
		for _, job := range values {
			binding := string(ExportBinding(tenant, job))
			if prev, ok := seen[binding]; ok {
				t.Fatalf("binding collision: (%q,%q) and (%q,%q) both derive %q", tenant, job, prev[0], prev[1], binding)
			}
			seen[binding] = [2]string{tenant, job}
		}
	}
	if string(ExportBinding("tenant-a", "job-1")) != string(ExportBinding("tenant-a", "job-1")) {
		t.Fatal("ExportBinding must be deterministic")
	}
}

// TestExportV2Binding is the v2 binding matrix (AC-3): a blob sealed under
// (T1,J1) opens only with the identical binding — wrong tenant, wrong job,
// nil binding and wrong key all fail GCM authentication, and an invalid
// prefix is rejected.
func TestExportV2Binding(t *testing.T) {
	key := "export-key"
	plain := []byte("line1\nline2\n")
	t1j1 := ExportBinding("tenant-a", "job-1")
	sealed, err := EncryptBytesBound(plain, key, t1j1)
	if err != nil {
		t.Fatal(err)
	}
	if string(sealed[:len(exportV2Prefix)]) != exportV2Prefix {
		t.Fatalf("sealed output missing v2 prefix: %q", sealed[:16])
	}
	opened, err := DecryptExport(sealed, key, t1j1)
	if err != nil || string(opened) != string(plain) {
		t.Fatalf("correct binding must open: err=%v", err)
	}
	if _, err := DecryptExport(sealed, key, ExportBinding("tenant-a", "job-2")); err == nil {
		t.Fatal("wrong job binding must fail GCM authentication")
	}
	if _, err := DecryptExport(sealed, key, ExportBinding("tenant-b", "job-1")); err == nil {
		t.Fatal("wrong tenant binding must fail GCM authentication")
	}
	if _, err := DecryptExport(sealed, key, nil); err == nil {
		t.Fatal("nil binding must fail for a v2 blob")
	}
	if _, err := DecryptExport(sealed, "wrong-key", t1j1); err == nil {
		t.Fatal("wrong key must fail")
	}
	if _, err := DecryptExport([]byte("garbage"), key, t1j1); err == nil {
		t.Fatal("invalid prefix must fail")
	}
}

// TestDecryptExportVersions pins the version dispatch: v1 blobs open with
// nil AAD (binding ignored), v2 blobs open only with the correct binding,
// and a v2 blob fed to the v1-only DecryptBytes is rejected.
func TestDecryptExportVersions(t *testing.T) {
	key := "export-key"
	binding := ExportBinding("tenant-a", "job-1")
	plain := []byte("line1\n")
	v1, err := EncryptBytes(plain, key)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := DecryptExport(v1, key, nil)
	if err != nil || string(opened) != string(plain) {
		t.Fatalf("v1 blob with nil binding must open: err=%v", err)
	}
	opened, err = DecryptExport(v1, key, binding)
	if err != nil || string(opened) != string(plain) {
		t.Fatalf("v1 blob with non-nil binding must still open (binding ignored): err=%v", err)
	}
	v2, err := EncryptBytesBound(plain, key, binding)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecryptBytes(v2, key); err == nil {
		t.Fatal("v2 blob must be rejected by v1-only DecryptBytes")
	}
	if _, err := DecryptExport(v2[:len(exportV2Prefix)+5], key, binding); err == nil {
		t.Fatal("truncated v2 envelope must fail")
	}
}

// FuzzDecryptExport exercises the version-dispatching open on arbitrary
// attacker-controlled bytes: it must never panic and never return partial
// output — a nil error always means the full plaintext.
func FuzzDecryptExport(f *testing.F) {
	key := "export-key"
	binding := ExportBinding("tenant-a", "job-1")
	validV1, err := EncryptBytes([]byte("line1\n"), key)
	if err != nil {
		f.Fatal(err)
	}
	validV2, err := EncryptBytesBound([]byte("line2\n"), key, binding)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(validV1)
	f.Add(validV2)
	f.Add([]byte("export:v1:"))
	f.Add([]byte("export:v2:"))
	f.Add([]byte("export:v2:garbage"))
	f.Add([]byte("not-an-export"))
	f.Fuzz(func(t *testing.T, sealed []byte) {
		plain, err := DecryptExport(sealed, key, binding)
		if err == nil && len(plain) == 0 {
			t.Fatal("successful open returned empty plaintext")
		}
	})
}
