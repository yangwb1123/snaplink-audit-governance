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
