package security

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// VaultTransitSigner signs and verifies checkpoint manifests through the
// HashiCorp Vault Transit engine. The signature key stays inside Vault: the
// application can request signatures but can never export the private key
// (architecture plan section 10). Uses plain HTTP against the Vault API to
// avoid a heavyweight SDK dependency.
type VaultTransitSigner struct {
	addr   string
	token  string
	key    string
	client *http.Client
}

func NewVaultTransitSigner(addr, token, key string) *VaultTransitSigner {
	return &VaultTransitSigner{
		addr:   addr,
		token:  token,
		key:    key,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// Algorithm reports the checkpoint algorithm label stored with segments.
func (v *VaultTransitSigner) Algorithm() string { return "vault-transit:" + v.key }

func (v *VaultTransitSigner) Sign(data []byte) (string, error) {
	return v.call("sign", map[string]any{"input": base64.StdEncoding.EncodeToString(data)}, func(decoded map[string]any) (string, error) {
		signature, _ := decoded["signature"].(string)
		if signature == "" {
			return "", fmt.Errorf("vault transit sign: empty signature")
		}
		return signature, nil
	})
}

func (v *VaultTransitSigner) Verify(data []byte, signature string) (bool, error) {
	valid, err := v.call("verify", map[string]any{"input": base64.StdEncoding.EncodeToString(data), "signature": signature}, func(decoded map[string]any) (string, error) {
		if valid, ok := decoded["valid"].(bool); ok {
			return fmt.Sprintf("%t", valid), nil
		}
		return "", fmt.Errorf("vault transit verify: missing valid flag")
	})
	if err != nil {
		return false, err
	}
	return valid == "true", nil
}

// call posts a transit operation and extracts a string result from
// response.data.
func (v *VaultTransitSigner) call(operation string, body map[string]any, extract func(map[string]any) (string, error)) (string, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	url := fmt.Sprintf("%s/v1/transit/%s/%s", v.addr, operation, v.key)
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(encoded))
	if err != nil {
		return "", err
	}
	request.Header.Set("X-Vault-Token", v.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := v.client.Do(request)
	if err != nil {
		return "", fmt.Errorf("vault transit %s: %w", operation, err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("vault transit %s: status %s body=%s", operation, response.Status, string(payload))
	}
	var decoded struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return "", fmt.Errorf("vault transit %s: decode: %w", operation, err)
	}
	return extract(decoded.Data)
}
