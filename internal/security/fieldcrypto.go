package security

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	if err := json.Unmarshal(plain, &value); err != nil {
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
