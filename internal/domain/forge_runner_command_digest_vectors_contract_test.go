package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"testing"
)

type forgeRunnerCommandDigestVectorsFixture struct {
	SchemaVersion string                           `json:"schema_version"`
	DigestDomain  string                           `json:"digest_domain"`
	Vectors       []forgeRunnerCommandDigestVector `json:"vectors"`
}

type forgeRunnerCommandDigestVector struct {
	Name          string                          `json:"name"`
	Command       forgeRunnerCommandDigestCommand `json:"command"`
	CommandSHA256 string                          `json:"command_sha256"`
}

type forgeRunnerCommandDigestCommand struct {
	V              uint16                        `json:"v"`
	CommandID      string                        `json:"command_id"`
	LeaseProof     forgeRunnerCommandDigestProof `json:"lease_proof"`
	IdempotencyKey string                        `json:"idempotency_key"`
	WorkspaceRef   string                        `json:"workspace_ref"`
	Argv           []string                      `json:"argv"`
	TimeoutMS      uint64                        `json:"timeout_ms"`
	MaxOutputBytes uint64                        `json:"max_output_bytes"`
}

type forgeRunnerCommandDigestProof struct {
	AttemptID    string `json:"attempt_id"`
	TargetID     string `json:"target_id"`
	Epoch        uint64 `json:"epoch"`
	FencingToken string `json:"fencing_token"`
}

func TestForgeRunnerCommandDigestVectors(t *testing.T) {
	encoded, err := os.ReadFile("testdata/forge-runner-command-digest-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := decodeForgeRunnerCommandDigestVectors(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.SchemaVersion != "forge.runner-command-digest/v1" || fixture.DigestDomain != "forge.runtime.runner-command.v1" || len(fixture.Vectors) != 3 {
		t.Fatalf("unexpected digest vector envelope: %#v", fixture)
	}
	seen := map[string]bool{}
	for _, vector := range fixture.Vectors {
		if vector.Name == "" || seen[vector.Name] {
			t.Fatalf("invalid or duplicate vector name %q", vector.Name)
		}
		seen[vector.Name] = true
		actual, err := forgeRunnerCommandDigest(vector.Command)
		if err != nil {
			t.Fatalf("digest %q: %v", vector.Name, err)
		}
		if actual != vector.CommandSHA256 {
			t.Fatalf("digest %q = %s, want %s", vector.Name, actual, vector.CommandSHA256)
		}
	}
}

func TestForgeRunnerCommandDigestVectorsRejectDrift(t *testing.T) {
	encoded, err := os.ReadFile("testdata/forge-runner-command-digest-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	duplicate := bytes.Replace(encoded, []byte(`"schema_version": "forge.runner-command-digest/v1",`), []byte(`"schema_version": "forge.runner-command-digest/v1", "schema_version": "forge.runner-command-digest/v1",`), 1)
	if rejectDuplicateJSONKeys(duplicate) == nil {
		t.Fatal("duplicate digest vector key was accepted")
	}
	unknown := bytes.Replace(encoded, []byte(`"digest_domain": "forge.runtime.runner-command.v1",`), []byte(`"digest_domain": "forge.runtime.runner-command.v1", "unexpected": true,`), 1)
	if _, err := decodeForgeRunnerCommandDigestVectors(unknown); err == nil {
		t.Fatal("unknown digest vector key was accepted")
	}
}

func decodeForgeRunnerCommandDigestVectors(raw []byte) (forgeRunnerCommandDigestVectorsFixture, error) {
	var fixture forgeRunnerCommandDigestVectorsFixture
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fixture); err != nil {
		return forgeRunnerCommandDigestVectorsFixture{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return forgeRunnerCommandDigestVectorsFixture{}, err
	}
	return fixture, nil
}

func forgeRunnerCommandDigest(command forgeRunnerCommandDigestCommand) (string, error) {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(command); err != nil {
		return "", err
	}
	bytesWithoutLF := bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'})
	digest := sha256.New()
	_, _ = digest.Write([]byte("forge.runtime.runner-command.v1\x00"))
	_, _ = digest.Write(bytesWithoutLF)
	return hex.EncodeToString(digest.Sum(nil)), nil
}
