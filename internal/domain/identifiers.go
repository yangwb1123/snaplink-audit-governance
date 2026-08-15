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
// Aggregate components derive archive stream keys, so the FM-1 length bound
// applies.
func ValidStreamComponent(name, value string) error {
	if err := ValidKeyComponent(name, value); err != nil {
		return err
	}
	if strings.ContainsRune(value, ':') {
		return fmt.Errorf("%w: %s must not contain ':'", ErrInvalid, name)
	}
	return ValidArchiveComponentLength(name, value)
}

// ValidArchiveComponentLength is the FM-1 per-component length bound. The
// injective percent-encoding expands non-safe bytes 3×, so a component
// longer than MaxArchiveComponentBytes (85 = POSIX NAME_MAX ÷ 3) can never
// fit a FileStore path component even when fully escaped, and three maximal
// components still fit the S3 1024-byte key budget. Enforced at the API
// boundary so over-limit identifiers are rejected before ingest instead of
// degrading to a permanently unarchivable receipt. Exported so every
// identifier that becomes an archive key component — tenant ids, stream
// components, event ids, and the source id at registration (the source
// branch of Event.Stream()) — is capped by the same single implementation.
func ValidArchiveComponentLength(name, value string) error {
	if len(value) > MaxArchiveComponentBytes {
		return fmt.Errorf("%w: %s is %d bytes, max %d (archive component limit)", ErrInvalid, name, len(value), MaxArchiveComponentBytes)
	}
	return nil
}

// ValidTenantIDComponent is ValidKeyComponent plus the ':' rejection for
// tenant identifiers: tenant IDs prefix every Event.Stream() frame and are
// the subject of ':'-delimited dev tokens, so an embedded ':' makes both
// ambiguous (dev-token rebinding defect). Tenant ids are archive key
// components, so the FM-1 length bound applies.
func ValidTenantIDComponent(name, value string) error {
	if err := ValidKeyComponent(name, value); err != nil {
		return err
	}
	if strings.ContainsRune(value, ':') {
		return fmt.Errorf("%w: %s must not contain ':'", ErrInvalid, name)
	}
	return ValidArchiveComponentLength(name, value)
}
