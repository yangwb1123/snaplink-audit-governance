package forge

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

// This receiver-side contract canonicalizes a caller-declared owner-scoped
// inventory snapshot. It does not authenticate the owner, read a clock,
// persist inventory, make a declaration authoritative, reserve capacity, or
// authorize execution.
//
//go:embed testdata/forge-device-inventory-snapshot-canonical-v1.json
var forgeDeviceInventorySnapshotCanonicalFixture []byte

const (
	forgeDeviceInventorySnapshotCanonicalSchema = "forge.device-inventory-snapshot-canonical/v1"
	forgeDeviceInventorySnapshotCanonicalMode   = "pure_owner_scoped_snapshot_only"
	forgeDeviceInventorySnapshotCanonicalDomain = forgeDeviceInventorySnapshotCanonicalSchema
	forgeDeviceInventorySnapshotMaxRows         = 128
	forgeDeviceInventorySnapshotMaxIDBytes      = 128
	forgeDeviceInventorySnapshotMaxOwnerBytes   = 512
)

type forgeDeviceInventorySnapshotCanonicalAuthority struct {
	IdentityVerified       bool `json:"identity_verified"`
	HeartbeatPersisted     bool `json:"heartbeat_persisted"`
	InventoryAuthoritative bool `json:"inventory_authoritative"`
	ReservationCreated     bool `json:"reservation_created"`
	ExecutionAuthorized    bool `json:"execution_authorized"`
	DispatchPerformed      bool `json:"dispatch_performed"`
}

type forgeDeviceInventorySnapshotCanonicalOwner struct {
	Issuer   string `json:"issuer"`
	Subject  string `json:"subject"`
	TenantID string `json:"tenant_id"`
}

type forgeDeviceInventorySnapshotCanonicalRow struct {
	DeviceID   string                                     `json:"device_id"`
	InstanceID string                                     `json:"instance_id"`
	Owner      forgeDeviceInventorySnapshotCanonicalOwner `json:"owner"`
}

type forgeDeviceInventorySnapshotCanonicalInput struct {
	SnapshotID string                                     `json:"snapshot_id"`
	ObservedAt uint64                                     `json:"observed_at_ms"`
	Owner      forgeDeviceInventorySnapshotCanonicalOwner `json:"owner"`
	Rows       []forgeDeviceInventorySnapshotCanonicalRow `json:"rows"`
}

type forgeDeviceInventorySnapshotCanonicalExpected struct {
	OrderedKeys     []string `json:"ordered_keys"`
	CanonicalSHA256 string   `json:"canonical_sha256"`
	Error           string   `json:"error"`
}

type forgeDeviceInventorySnapshotCanonicalCase struct {
	Name     string                                        `json:"name"`
	Input    forgeDeviceInventorySnapshotCanonicalInput    `json:"input"`
	Expected forgeDeviceInventorySnapshotCanonicalExpected `json:"expected"`
}

type forgeDeviceInventorySnapshotCanonicalFixtureType struct {
	SchemaVersion                   string                                         `json:"schema_version"`
	EvaluationMode                  string                                         `json:"evaluation_mode"`
	OwnerDeclarationUnverified      bool                                           `json:"owner_declaration_unverified"`
	InventoryDeclarationsUnverified bool                                           `json:"inventory_declarations_unverified"`
	Authority                       forgeDeviceInventorySnapshotCanonicalAuthority `json:"authority"`
	Cases                           []forgeDeviceInventorySnapshotCanonicalCase    `json:"cases"`
}

func decodeForgeDeviceInventorySnapshotCanonical(raw []byte, out *forgeDeviceInventorySnapshotCanonicalFixtureType) error {
	if err := rejectForgeDeviceInventorySnapshotCanonicalDuplicateKeys(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	return validateForgeDeviceInventorySnapshotCanonicalFixture(out)
}

func validateForgeDeviceInventorySnapshotCanonicalFixture(fixture *forgeDeviceInventorySnapshotCanonicalFixtureType) error {
	if fixture.SchemaVersion != forgeDeviceInventorySnapshotCanonicalSchema ||
		fixture.EvaluationMode != forgeDeviceInventorySnapshotCanonicalMode ||
		!fixture.OwnerDeclarationUnverified ||
		!fixture.InventoryDeclarationsUnverified ||
		fixture.Authority != (forgeDeviceInventorySnapshotCanonicalAuthority{}) ||
		len(fixture.Cases) != 6 {
		return fmt.Errorf("invalid inventory snapshot canonical envelope")
	}
	wantNames := []string{
		"sorts_by_device_then_instance_without_mutating_input",
		"empty_snapshot_has_stable_digest",
		"foreign_owner_row_rejected",
		"duplicate_composite_row_rejected",
		"invalid_snapshot_id_rejected",
		"zero_observed_time_rejected",
	}
	seen := make(map[string]struct{}, len(fixture.Cases))
	for index, testCase := range fixture.Cases {
		if testCase.Name != wantNames[index] {
			return fmt.Errorf("case ordering drift at %d", index)
		}
		if _, exists := seen[testCase.Name]; exists {
			return fmt.Errorf("duplicate case %q", testCase.Name)
		}
		seen[testCase.Name] = struct{}{}
		if err := validateForgeDeviceInventorySnapshotCanonicalCase(testCase); err != nil {
			return err
		}
	}
	return nil
}

func validateForgeDeviceInventorySnapshotCanonicalCase(testCase forgeDeviceInventorySnapshotCanonicalCase) error {
	var before []forgeDeviceInventorySnapshotCanonicalRow
	if testCase.Input.Rows != nil {
		before = append([]forgeDeviceInventorySnapshotCanonicalRow{}, testCase.Input.Rows...)
	}
	canonical, err := canonicalizeForgeDeviceInventorySnapshot(testCase.Input)
	if testCase.Expected.Error != "" {
		if err == nil || err.Error() != testCase.Expected.Error {
			return fmt.Errorf("case %q returned %v, want %q", testCase.Name, err, testCase.Expected.Error)
		}
		if testCase.Expected.OrderedKeys != nil || testCase.Expected.CanonicalSHA256 != "" {
			return fmt.Errorf("rejected case %q carries accepted fields", testCase.Name)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("case %q rejected: %w", testCase.Name, err)
	}
	if testCase.Expected.CanonicalSHA256 == "" || testCase.Expected.OrderedKeys == nil {
		return fmt.Errorf("accepted case %q is missing canonical expectation", testCase.Name)
	}
	if !reflect.DeepEqual(testCase.Input.Rows, before) {
		return fmt.Errorf("case %q mutated input", testCase.Name)
	}
	if got := forgeDeviceInventorySnapshotCanonicalRowKeys(canonical.Rows); !reflect.DeepEqual(got, testCase.Expected.OrderedKeys) {
		return fmt.Errorf("case %q ordered keys = %#v, want %#v", testCase.Name, got, testCase.Expected.OrderedKeys)
	}
	gotDigest, err := digestForgeDeviceInventorySnapshot(testCase.Input)
	if err != nil {
		return fmt.Errorf("case %q digest rejected: %w", testCase.Name, err)
	}
	if gotDigest != testCase.Expected.CanonicalSHA256 {
		return fmt.Errorf("case %q digest = %s, want %s", testCase.Name, gotDigest, testCase.Expected.CanonicalSHA256)
	}
	return nil
}

func canonicalizeForgeDeviceInventorySnapshot(input forgeDeviceInventorySnapshotCanonicalInput) (forgeDeviceInventorySnapshotCanonicalInput, error) {
	if !validForgeDeviceInventorySnapshotIdentifier(input.SnapshotID) {
		return forgeDeviceInventorySnapshotCanonicalInput{}, errors.New("invalid_snapshot_id")
	}
	if input.ObservedAt == 0 {
		return forgeDeviceInventorySnapshotCanonicalInput{}, errors.New("invalid_observed_at")
	}
	if !validForgeDeviceInventorySnapshotOwner(input.Owner) {
		return forgeDeviceInventorySnapshotCanonicalInput{}, errors.New("invalid_owner")
	}
	if len(input.Rows) > forgeDeviceInventorySnapshotMaxRows {
		return forgeDeviceInventorySnapshotCanonicalInput{}, errors.New("too_many_rows")
	}
	canonical := input
	canonical.Rows = append([]forgeDeviceInventorySnapshotCanonicalRow(nil), input.Rows...)
	for _, row := range canonical.Rows {
		if !validForgeDeviceInventorySnapshotIdentifier(row.DeviceID) || !validForgeDeviceInventorySnapshotIdentifier(row.InstanceID) {
			return forgeDeviceInventorySnapshotCanonicalInput{}, errors.New("invalid_snapshot_id")
		}
		if row.Owner != input.Owner {
			return forgeDeviceInventorySnapshotCanonicalInput{}, errors.New("owner_mismatch")
		}
		if !validForgeDeviceInventorySnapshotOwner(row.Owner) {
			return forgeDeviceInventorySnapshotCanonicalInput{}, errors.New("invalid_owner")
		}
	}
	sort.Slice(canonical.Rows, func(left, right int) bool {
		if canonical.Rows[left].DeviceID != canonical.Rows[right].DeviceID {
			return canonical.Rows[left].DeviceID < canonical.Rows[right].DeviceID
		}
		return canonical.Rows[left].InstanceID < canonical.Rows[right].InstanceID
	})
	for index := 1; index < len(canonical.Rows); index++ {
		if canonical.Rows[index-1].DeviceID == canonical.Rows[index].DeviceID &&
			canonical.Rows[index-1].InstanceID == canonical.Rows[index].InstanceID {
			return forgeDeviceInventorySnapshotCanonicalInput{}, errors.New("duplicate_row")
		}
	}
	return canonical, nil
}

func digestForgeDeviceInventorySnapshot(input forgeDeviceInventorySnapshotCanonicalInput) (string, error) {
	canonical, err := canonicalizeForgeDeviceInventorySnapshot(input)
	if err != nil {
		return "", err
	}
	digest := sha256.New()
	_, _ = io.WriteString(digest, forgeDeviceInventorySnapshotCanonicalDomain)
	_, _ = digest.Write([]byte{0})
	appendForgeDeviceInventorySnapshotField(digest, canonical.SnapshotID)
	appendForgeDeviceInventorySnapshotField(digest, strconv.FormatUint(canonical.ObservedAt, 10))
	appendForgeDeviceInventorySnapshotOwner(digest, canonical.Owner)
	for _, row := range canonical.Rows {
		appendForgeDeviceInventorySnapshotField(digest, row.DeviceID)
		appendForgeDeviceInventorySnapshotField(digest, row.InstanceID)
		appendForgeDeviceInventorySnapshotOwner(digest, row.Owner)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func appendForgeDeviceInventorySnapshotOwner(digest hash.Hash, owner forgeDeviceInventorySnapshotCanonicalOwner) {
	appendForgeDeviceInventorySnapshotField(digest, owner.Issuer)
	appendForgeDeviceInventorySnapshotField(digest, owner.Subject)
	appendForgeDeviceInventorySnapshotField(digest, owner.TenantID)
}

func appendForgeDeviceInventorySnapshotField(digest hash.Hash, value string) {
	_, _ = io.WriteString(digest, strconv.Itoa(len(value)))
	_, _ = digest.Write([]byte{':'})
	_, _ = io.WriteString(digest, value)
	_, _ = digest.Write([]byte{0})
}

func validForgeDeviceInventorySnapshotOwner(owner forgeDeviceInventorySnapshotCanonicalOwner) bool {
	return validForgeDeviceInventorySnapshotOwnerPart(owner.Issuer) &&
		validForgeDeviceInventorySnapshotOwnerPart(owner.Subject) &&
		validForgeDeviceInventorySnapshotOwnerPart(owner.TenantID)
}

func validForgeDeviceInventorySnapshotOwnerPart(value string) bool {
	return value != "" && len(value) <= forgeDeviceInventorySnapshotMaxOwnerBytes &&
		strings.TrimSpace(value) == value &&
		!strings.ContainsFunc(value, unicode.IsControl)
}

func validForgeDeviceInventorySnapshotIdentifier(value string) bool {
	valueBytes := []byte(value)
	if len(valueBytes) == 0 || len(valueBytes) > forgeDeviceInventorySnapshotMaxIDBytes {
		return false
	}
	for index, valueByte := range valueBytes {
		asciiAlphanumeric := valueByte >= 'a' && valueByte <= 'z' ||
			valueByte >= 'A' && valueByte <= 'Z' ||
			valueByte >= '0' && valueByte <= '9'
		if asciiAlphanumeric {
			continue
		}
		if index == 0 || !strings.ContainsRune("._:-", rune(valueByte)) {
			return false
		}
	}
	return true
}

func forgeDeviceInventorySnapshotCanonicalRowKeys(rows []forgeDeviceInventorySnapshotCanonicalRow) []string {
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, row.DeviceID+"/"+row.InstanceID)
	}
	return keys
}

func rejectForgeDeviceInventorySnapshotCanonicalDuplicateKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			seen := map[string]struct{}{}
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok {
					return fmt.Errorf("object key is not string")
				}
				if _, exists := seen[name]; exists {
					return fmt.Errorf("duplicate JSON key %q", name)
				}
				seen[name] = struct{}{}
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		default:
			return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
		}
	}
	if err := walk(); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	return nil
}

func TestForgeDeviceInventorySnapshotCanonicalReceiverContract(t *testing.T) {
	var fixture forgeDeviceInventorySnapshotCanonicalFixtureType
	if err := decodeForgeDeviceInventorySnapshotCanonical(forgeDeviceInventorySnapshotCanonicalFixture, &fixture); err != nil {
		t.Fatalf("decode inventory snapshot canonical fixture: %v", err)
	}
	if got := forgeDeviceInventorySnapshotCanonicalRowKeys(fixture.Cases[0].Input.Rows); !reflect.DeepEqual(got, []string{"device-b/runner-z", "device-a/runner-z", "device-a/runner-a"}) {
		t.Fatalf("fixture input order unexpectedly changed: %#v", got)
	}
}

func TestForgeDeviceInventorySnapshotCanonicalReceiverRejectsMutation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "unknown", mutate: func(root map[string]any) { root["unexpected"] = true }},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var root map[string]any
			if err := json.Unmarshal(forgeDeviceInventorySnapshotCanonicalFixture, &root); err != nil {
				t.Fatal(err)
			}
			testCase.mutate(root)
			raw, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			var fixture forgeDeviceInventorySnapshotCanonicalFixtureType
			if err := decodeForgeDeviceInventorySnapshotCanonical(raw, &fixture); err == nil {
				t.Fatal("mutated inventory snapshot fixture unexpectedly accepted")
			}
		})
	}

	for _, authorityField := range []string{
		"identity_verified",
		"heartbeat_persisted",
		"inventory_authoritative",
		"reservation_created",
		"execution_authorized",
		"dispatch_performed",
	} {
		t.Run("authority_"+authorityField, func(t *testing.T) {
			var root map[string]any
			if err := json.Unmarshal(forgeDeviceInventorySnapshotCanonicalFixture, &root); err != nil {
				t.Fatal(err)
			}
			root["authority"].(map[string]any)[authorityField] = true
			raw, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			var fixture forgeDeviceInventorySnapshotCanonicalFixtureType
			if err := decodeForgeDeviceInventorySnapshotCanonical(raw, &fixture); err == nil {
				t.Fatalf("authority mutation %q unexpectedly accepted", authorityField)
			}
		})
	}

	t.Run("digest_mismatch", func(t *testing.T) {
		var root map[string]any
		if err := json.Unmarshal(forgeDeviceInventorySnapshotCanonicalFixture, &root); err != nil {
			t.Fatal(err)
		}
		cases := root["cases"].([]any)
		cases[0].(map[string]any)["expected"].(map[string]any)["canonical_sha256"] = strings.Repeat("0", 64)
		raw, err := json.Marshal(root)
		if err != nil {
			t.Fatal(err)
		}
		var fixture forgeDeviceInventorySnapshotCanonicalFixtureType
		if err := decodeForgeDeviceInventorySnapshotCanonical(raw, &fixture); err == nil {
			t.Fatal("digest mutation unexpectedly accepted")
		}
	})

	t.Run("duplicate_key", func(t *testing.T) {
		duplicate := bytes.TrimSpace(forgeDeviceInventorySnapshotCanonicalFixture)
		duplicate = append(bytes.TrimSuffix(duplicate, []byte("}")), []byte(`,"schema_version":"forge.device-inventory-snapshot-canonical/v1"}`)...)
		var fixture forgeDeviceInventorySnapshotCanonicalFixtureType
		if err := decodeForgeDeviceInventorySnapshotCanonical(duplicate, &fixture); err == nil {
			t.Fatal("duplicate field unexpectedly accepted")
		}
	})
}
