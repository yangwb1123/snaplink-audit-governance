package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// TestValidateEventCapsBoundaryAndClass pins the shared cap contract at the
// leaf: strings at exactly MaxEnvelopeFieldBytes pass, one byte over fails
// with ErrEnvelopeTooLarge (which wraps ErrInvalid), and arity caps count
// the same values the transports measure. This is the base the HTTP and
// gRPC parity tests build on.
func TestValidateEventCapsBoundaryAndClass(t *testing.T) {
	exact := strings.Repeat("e", MaxEnvelopeFieldBytes)
	utf8Exact := strings.Repeat("é", MaxEnvelopeFieldBytes/len("é"))
	utf8Over := utf8Exact + "x"
	over := strings.Repeat("x", MaxEnvelopeFieldBytes+1)

	base := Event{
		EventID: "cap-boundary", SourceSystem: "crm", EventType: "audit.event",
		SchemaID: "audit.event", SchemaVersion: 1,
		OccurredAt: t0(),
		Actor:      Actor{ID: "service"},
		Action:     "write", Outcome: "success",
		DataClassification: "internal", RetentionClass: "standard",
		IdempotencyKey: "cap-boundary-idem",
		Payload:        map[string]any{"value": 1},
	}

	atCap := base
	atCap.Reason = exact
	if err := ValidateEventCaps(atCap); err != nil {
		t.Fatalf("field at exactly the cap rejected: %v", err)
	}
	atCap.Reason = utf8Exact
	if err := ValidateEventCaps(atCap); err != nil {
		t.Fatalf("UTF-8 field at exactly the byte cap rejected: %v", err)
	}

	overCap := base
	overCap.Reason = over
	err := ValidateEventCaps(overCap)
	if !errors.Is(err, ErrInvalid) || !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatalf("err=%v, want both ErrInvalid and ErrEnvelopeTooLarge", err)
	}
	const want = "reason is 8193 bytes, max 8192"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("message %q missing size detail %q", err.Error(), want)
	}
	utf8OverCap := base
	utf8OverCap.Reason = utf8Over
	if err := ValidateEventCaps(utf8OverCap); !errors.Is(err, ErrEnvelopeTooLarge) || !strings.Contains(err.Error(), want) {
		t.Fatalf("UTF-8 one-byte-over error=%v, want byte-cap rejection %q", err, want)
	}
}

// TestValidateEventCapsArity pins the three arity caps and the
// changed_fields canonical-length measurement: a JSON-string value of
// MaxEnvelopeFieldBytes+1 chars canonicalizes to +2 bytes (the quotes) and
// is rejected via the canonical length, exactly like digest derivation
// measures it.
func TestValidateEventCapsArity(t *testing.T) {
	base := Event{
		EventID: "cap-arity", SourceSystem: "crm", EventType: "audit.event",
		SchemaID: "audit.event", SchemaVersion: 1,
		OccurredAt: t0(),
		Actor:      Actor{ID: "service"},
		Action:     "write", Outcome: "success",
		DataClassification: "internal", RetentionClass: "standard",
		IdempotencyKey: "cap-arity-idem",
		Payload:        map[string]any{"value": 1},
	}

	roles := base
	roles.Actor.Roles = make([]string, MaxActorRoles+1)
	if err := ValidateEventCaps(roles); !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatalf("roles over cap: err=%v", err)
	}

	targets := base
	targets.Targets = make([]Target, MaxTargetsPerEvent+1)
	if err := ValidateEventCaps(targets); !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatalf("targets over cap: err=%v", err)
	}

	fields := base
	fields.ChangedFields = make(map[string]FieldChange, MaxChangedFields+1)
	for i := 0; i < MaxChangedFields+1; i++ {
		fields.ChangedFields["field-"+strings.Repeat("k", i)] = FieldChange{}
	}
	if err := ValidateEventCaps(fields); !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatalf("changed_fields over cap: err=%v", err)
	}

	value := base
	value.ChangedFields = map[string]FieldChange{"f": {Before: strings.Repeat("v", MaxEnvelopeFieldBytes+1)}}
	if err := ValidateEventCaps(value); !errors.Is(err, ErrEnvelopeTooLarge) {
		t.Fatalf("changed_fields value over cap: err=%v", err)
	}
	if !strings.Contains(ValidateEventCaps(value).Error(), "before_json") {
		t.Fatalf("changed_fields value rejection must name before_json, got: %v", ValidateEventCaps(value))
	}
}

func t0() (t time.Time) { return time.Unix(1_700_000_010, 0).UTC() }
