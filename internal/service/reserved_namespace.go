package service

import (
	"fmt"
	"sort"
	"strings"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/security"
)

// rejectReservedSearchDigestNamespace rejects an event whose top-level payload
// contains a key colliding with the reserved search-digest on-disk namespace
// (protectSensitiveFields writes field+"__search_digest"). Top-level only:
// nested *__search_digest keys remain legal at ingest and are stripped only
// at read/export boundaries by security.StripSearchDigests. The namespace is
// not user-allocatable: rejection is independent of schema contents, whether
// or not the key is listed in AllowedFields, and whether or not AllowedFields
// is populated. Fail-closed: the check runs inside validateEvent, before
// protectSensitiveFields and before any Store.Update write.
func rejectReservedSearchDigestNamespace(payload map[string]any) error {
	for key := range payload {
		if strings.HasSuffix(key, security.SearchDigestSuffix) {
			return fmt.Errorf("%w: payload key %q collides with the reserved search_digest namespace", domain.ErrInvalid, key)
		}
	}
	return nil
}

// firstReservedSearchDigestKey returns the lexicographically first top-level
// payload key ending in the reserved search-digest suffix, and whether one
// exists. Used by verifyContentDigest to attribute a content mismatch to the
// reserved namespace (REQ-3). Deterministic: candidates are sorted before the
// first is picked, so a multi-collision event reports the same key across
// runs (raw map iteration order is random). Top-level only, mirroring the
// ingest-time rejection.
func firstReservedSearchDigestKey(payload map[string]any) (string, bool) {
	var keys []string
	for key := range payload {
		if strings.HasSuffix(key, security.SearchDigestSuffix) {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return "", false
	}
	sort.Strings(keys)
	return keys[0], true
}
