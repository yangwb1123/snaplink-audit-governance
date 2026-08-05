package security

import "testing"

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
