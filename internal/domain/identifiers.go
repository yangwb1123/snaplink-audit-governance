package domain

import (
	"fmt"
	"strings"
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

// ValidStreamComponent is ValidKeyComponent plus the ':' rejection. ':' is a
// stream-frame delimiter (Event.Stream: tenant + ":aggregate:" + type + ":" + id);
// ("a","b:c") and ("a:b","c") would otherwise derive the same stream key and
// silently merge two distinct aggregates into one hash-chained ledger stream.
func ValidStreamComponent(name, value string) error {
	if err := ValidKeyComponent(name, value); err != nil {
		return err
	}
	if strings.ContainsRune(value, ':') {
		return fmt.Errorf("%w: %s must not contain ':'", ErrInvalid, name)
	}
	return nil
}

// ValidTenantIDComponent is ValidKeyComponent plus the ':' rejection for
// tenant identifiers: tenant IDs prefix every Event.Stream() frame and are
// the subject of ':'-delimited dev tokens, so an embedded ':' makes both
// ambiguous (dev-token rebinding defect).
func ValidTenantIDComponent(name, value string) error {
	if err := ValidKeyComponent(name, value); err != nil {
		return err
	}
	if strings.ContainsRune(value, ':') {
		return fmt.Errorf("%w: %s must not contain ':'", ErrInvalid, name)
	}
	return nil
}
