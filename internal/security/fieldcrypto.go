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
