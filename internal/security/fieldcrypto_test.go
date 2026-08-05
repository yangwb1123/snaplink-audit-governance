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
