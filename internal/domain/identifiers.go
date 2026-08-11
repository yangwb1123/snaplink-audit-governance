package domain

import (
	"fmt"
	"unicode"
)

// ValidKeyComponent is the canonical key-framing charset rule. A composite
// snapshot key component must not contain KeySeparator (0x1F) — it would
// make the key ambiguous with another component pair — nor any other
// control character, whitespace, or '/'/'\\' (URL/log/archive hygiene).
// store.ValidTenantID delegates here; every untrusted identifier at the API
// boundary is validated with this single implementation. Rejection-based:
// non-ASCII letters and punctuation stay valid.
func ValidKeyComponent(name, value string) error {
	for _, r := range value {
		switch {
		case unicode.IsControl(r): // includes 0x1F, NUL, CR, LF, TAB, ...
			return fmt.Errorf("%w: %s must not contain control characters", ErrInvalid, name)
		case unicode.IsSpace(r): // \t \n \v \f \r, ' ', U+0085, U+00A0
			return fmt.Errorf("%w: %s must not contain whitespace", ErrInvalid, name)
		case r == '/' || r == '\\':
			return fmt.Errorf("%w: %s must not contain path separators", ErrInvalid, name)
		}
	}
	return nil
}
