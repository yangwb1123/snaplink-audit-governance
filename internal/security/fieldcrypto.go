package security

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

const encryptedPrefix = "enc:v1:"

// exportPrefix marks byte-level encrypted export files (independent
// encryption of the whole JSONL payload, architecture plan section 14).
const exportPrefix = "export:v1:"

func keyBytes(key string) []byte {
	sum := sha256.Sum256([]byte(key))
	return sum[:]
}

func EncryptJSON(value any, key, associatedData string) (string, error) {
	plain, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(keyBytes(key))
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	ciphertext := gcm.Seal(nil, nonce, plain, []byte(associatedData))
	combined := append(nonce, ciphertext...)
	return encryptedPrefix + base64.RawURLEncoding.EncodeToString(combined), nil
}

func DecryptJSON(encoded, key, associatedData string) (any, error) {
	if len(encoded) < len(encryptedPrefix) || encoded[:len(encryptedPrefix)] != encryptedPrefix {
		return nil, fmt.Errorf("invalid encrypted field")
	}
	combined, err := base64.RawURLEncoding.DecodeString(encoded[len(encryptedPrefix):])
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(keyBytes(key))
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(combined) < gcm.NonceSize() {
		return nil, fmt.Errorf("invalid encrypted field")
	}
	plain, err := gcm.Open(nil, combined[:gcm.NonceSize()], combined[gcm.NonceSize():], []byte(associatedData))
	if err != nil {
		return nil, err
	}
	var value any
	// UseNumber: the decrypted value feeds digest re-derivation
	// (reconstructAndDerive); a float64 decode would collapse int64 values
	// > 2^53 and break parity with the ingest-time digest.
	decoder := json.NewDecoder(bytes.NewReader(plain))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func SearchDigest(value any, key string) (string, error) {
	plain, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, keyBytes(key))
	_, _ = mac.Write(plain)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

// searchDigestPrefix marks tenant/field-bound search digests (format v2).
// ':' is outside the base64url alphabet, so a legacy unbound digest (format
// v1, produced by SearchDigest) can never carry the prefix: the two formats
// are distinguishable from the value alone, without a versioned struct.
const searchDigestPrefix = "sd2:"

// SearchDigestBound derives a tenant- and field-scoped search digest. The
// HMAC input binds the canonical JSON value to the tenant ID and field
// name, so the same plaintext in two tenants (or under two field names)
// yields different digests: an actor holding read access to several tenants
// can no longer correlate records across tenants purely by digest equality
// (threat-model boundary D). The output is self-describing ("sd2:" prefix);
// see IsBoundSearchDigest.
func SearchDigestBound(value any, key, tenantID, field string) (string, error) {
	plain, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, keyBytes(key))
	_, _ = mac.Write(plain)
	// NUL framing: json.Marshal never emits a raw 0x00 byte (control
	// characters are escaped as \uXXXX), so value/tenantID/field are
	// unambiguously delimited even for hostile tenant IDs or field names.
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(tenantID))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(field))
	return searchDigestPrefix + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

// IsBoundSearchDigest reports whether digest carries the self-describing
// tenant/field-bound format marker. Legacy unbound digests never match.
func IsBoundSearchDigest(digest string) bool {
	return strings.HasPrefix(digest, searchDigestPrefix)
}

// StripSearchDigests returns a deep copy of payload with every key ending
// in "__search_digest" removed at any nesting depth (nested maps and array
// elements included). The caller's map is never mutated: stored payloads
// are shared by reference with the store snapshot, so API responses must be
// redacted on copies. Numbers round-trip through json.Number so int64
// values > 2^53 keep their exact digits (mirroring the store's snapshot
// decoding).
func StripSearchDigests(payload map[string]any) (map[string]any, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	var copyPayload map[string]any
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&copyPayload); err != nil {
		return nil, err
	}
	stripSearchDigestKeys(copyPayload)
	return copyPayload, nil
}

func stripSearchDigestKeys(value any) {
	switch v := value.(type) {
	case map[string]any:
		for key := range v {
			if strings.HasSuffix(key, "__search_digest") {
				delete(v, key)
			}
		}
		for _, child := range v {
			stripSearchDigestKeys(child)
		}
	case []any:
		for _, child := range v {
			stripSearchDigestKeys(child)
		}
	}
}

// EncryptBytes seals raw bytes with AES-GCM. The output carries the
// export:v1: prefix, a random nonce and the ciphertext; the caller keeps
// the key out of the archive.
func EncryptBytes(plain []byte, key string) ([]byte, error) {
	block, err := aes.NewCipher(keyBytes(key))
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nil, nonce, plain, nil)
	combined := append([]byte(exportPrefix), nonce...)
	combined = append(combined, ciphertext...)
	return combined, nil
}

// DecryptBytes opens an export:v1: blob produced by EncryptBytes.
func DecryptBytes(sealed []byte, key string) ([]byte, error) {
	if len(sealed) < len(exportPrefix) || string(sealed[:len(exportPrefix)]) != exportPrefix {
		return nil, fmt.Errorf("invalid encrypted export")
	}
	body := sealed[len(exportPrefix):]
	block, err := aes.NewCipher(keyBytes(key))
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(body) < gcm.NonceSize() {
		return nil, fmt.Errorf("invalid encrypted export")
	}
	plain, err := gcm.Open(nil, body[:gcm.NonceSize()], body[gcm.NonceSize():], nil)
	if err != nil {
		return nil, err
	}
	return plain, nil
}
