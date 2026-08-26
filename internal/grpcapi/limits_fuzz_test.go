package grpcapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/snaplink/audit-governance/internal/domain"
)

// FuzzCanonicalJSONCapInvariant exercises the attacker-controlled JSON path
// behind before_json/after_json. Any input accepted by the cap validator must
// be strict single-value JSON whose canonical representation is within the
// shared byte cap; malformed, trailing, and over-cap values remain rejected.
func FuzzCanonicalJSONCapInvariant(f *testing.F) {
	f.Add(`{"v":1}`)
	f.Add(` {"v":"x"} \n`)
	f.Add(`{"v":1} trailing`)
	f.Add(`"<"`)
	f.Fuzz(func(t *testing.T, raw string) {
		if raw == "" {
			return // an empty proto field means an omitted value
		}
		err := checkCanonicalJSON("changed_fields[].after_json", raw)
		if err != nil {
			return
		}

		var decoded any
		decoder := json.NewDecoder(bytes.NewReader([]byte(raw)))
		decoder.UseNumber()
		if err := decoder.Decode(&decoded); err != nil {
			t.Fatalf("accepted input did not decode: %v", err)
		}
		canonical, err := domain.CanonicalJSON(decoded)
		if err != nil {
			t.Fatalf("accepted input did not canonicalize: %v", err)
		}
		if len(canonical) > domain.MaxEnvelopeFieldBytes {
			t.Fatalf("accepted canonical JSON is %d bytes, max %d", len(canonical), domain.MaxEnvelopeFieldBytes)
		}
		if errors.Is(checkCanonicalJSON("changed_fields[].after_json", raw), domain.ErrEnvelopeTooLarge) {
			t.Fatal("accepted value was also classified as over-cap")
		}
	})
}
