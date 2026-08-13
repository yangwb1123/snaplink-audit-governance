package domain

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

// CanonicalJSON emits deterministic JSON for hash calculation. Map keys are
// sorted recursively and time values are encoded in RFC3339Nano UTC form.
// Numbers are representation-independent: json.Number, int64 and float64
// values that are numerically equal produce identical bytes, and integers
// never lose precision (see writeNumber).
func CanonicalJSON(value any) ([]byte, error) {
	var b bytes.Buffer
	if err := writeCanonical(&b, reflect.ValueOf(value)); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func writeCanonical(b *bytes.Buffer, v reflect.Value) error {
	if !v.IsValid() {
		b.WriteString("null")
		return nil
	}
	if v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		if v.IsNil() {
			b.WriteString("null")
			return nil
		}
		return writeCanonical(b, v.Elem())
	}
	if v.CanInterface() {
		switch typed := v.Interface().(type) {
		case time.Time:
			return writeString(b, typed.UTC().Format(time.RFC3339Nano))
		case json.Number:
			// json.Number must never hit the String branch: it would be
			// emitted quoted, making the digest representation-dependent
			// (UseNumber decoders produce json.Number, direct int64/float64
			// producers do not).
			return writeNumber(b, typed)
		}
	}
	switch v.Kind() {
	case reflect.String:
		return writeString(b, v.String())
	case reflect.Bool:
		b.WriteString(strconv.FormatBool(v.Bool()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		b.WriteString(strconv.FormatInt(v.Int(), 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		b.WriteString(strconv.FormatUint(v.Uint(), 10))
	case reflect.Float32, reflect.Float64:
		f := v.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return fmt.Errorf("non-finite number is not valid JSON")
		}
		if f == 0 {
			// Normalize -0 to 0: numerically equal values must canonicalize
			// to identical bytes.
			b.WriteString("0")
			return nil
		}
		// 'f' (not 'g') keeps byte parity with writeNumber's json.Number
		// path, e.g. float64(1e15) and json.Number("1e15") both emit
		// "1000000000000000" instead of "1e+15".
		b.WriteString(strconv.FormatFloat(f, 'f', -1, v.Type().Bits()))
	case reflect.Slice, reflect.Array:
		b.WriteByte('[')
		for i := 0; i < v.Len(); i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := writeCanonical(b, v.Index(i)); err != nil {
				return err
			}
		}
		b.WriteByte(']')
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("canonical maps require string keys")
		}
		keys := v.MapKeys()
		sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
		b.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := writeString(b, key.String()); err != nil {
				return err
			}
			b.WriteByte(':')
			if err := writeCanonical(b, v.MapIndex(key)); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	case reflect.Struct:
		// Direct reflection emission instead of a json.Marshal/Unmarshal
		// round-trip: the round-trip collapsed int64 values > 2^53 through
		// float64 (digest collisions) and turned nested time.Time into
		// zone-preserving strings. The byte shape still matches the old
		// round-trip for unaffected values (keys sorted byte-wise, stdlib
		// omitempty semantics), so digests of unaffected events are stable.
		return writeStruct(b, v)
	default:
		return fmt.Errorf("unsupported canonical type %s", v.Type())
	}
	return nil
}

// writeNumber emits a json.Number. Integer literals are reproduced exactly
// with arbitrary precision (big.Int, normalizing "-0" to "0"); fractional or
// exponent forms are reduced to the shortest 'f'-format decimal of their
// float64 value so that e.g. json.Number("1.5e3"), json.Number("1500.0"),
// json.Number("1500"), int64(1500) and float64(1500) all emit "1500".
// Non-finite or float64-overflowing literals are rejected.
func writeNumber(b *bytes.Buffer, n json.Number) error {
	s := string(n)
	if !strings.ContainsAny(s, ".eE") {
		i, ok := new(big.Int).SetString(s, 10)
		if !ok {
			return fmt.Errorf("invalid number literal %q", s)
		}
		b.WriteString(i.String())
		return nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return fmt.Errorf("number %q is not representable as a finite float64", s)
	}
	if f == 0 {
		b.WriteString("0")
		return nil
	}
	b.WriteString(strconv.FormatFloat(f, 'f', -1, 64))
	return nil
}

// writeStruct emits a struct with the same byte shape encoding/json produces
// after a Marshal/Unmarshal round-trip through map[string]any: keys sorted
// byte-wise, fields omitted per the stdlib omitempty rules, and every nested
// value emitted through writeCanonical (so nested time.Time normalizes to
// RFC3339Nano UTC and numbers stay exact, including any-typed values).
func writeStruct(b *bytes.Buffer, v reflect.Value) error {
	type namedValue struct {
		name  string
		value reflect.Value
	}
	t := v.Type()
	fields := make([]namedValue, 0, v.NumField())
	for i := 0; i < v.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue // unexported: encoding/json ignores these
		}
		name, omit := jsonFieldName(sf)
		if name == "" {
			continue // json:"-"
		}
		fv := v.Field(i)
		if omit && jsonIsEmptyValue(fv) {
			continue
		}
		fields = append(fields, namedValue{name: name, value: fv})
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].name < fields[j].name })
	b.WriteByte('{')
	for i, f := range fields {
		if i > 0 {
			b.WriteByte(',')
		}
		if err := writeString(b, f.name); err != nil {
			return err
		}
		b.WriteByte(':')
		if err := writeCanonical(b, f.value); err != nil {
			return err
		}
	}
	b.WriteByte('}')
	return nil
}

// jsonFieldName mirrors encoding/json's tag handling: the tag name (or the
// field name when the tag has no name), "-" meaning skip, and the omitempty
// option.
func jsonFieldName(sf reflect.StructField) (string, bool) {
	tag := sf.Tag.Get("json")
	if tag == "-" {
		return "", false
	}
	parts := strings.Split(tag, ",")
	name := parts[0]
	if name == "" {
		name = sf.Name
	}
	omit := false
	for _, opt := range parts[1:] {
		if opt == "omitempty" {
			omit = true
		}
	}
	return name, omit
}

// jsonIsEmptyValue mirrors encoding/json's isEmptyValue, which decides
// omitempty omission. Structs are never empty, matching the stdlib.
func jsonIsEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Pointer:
		return v.IsNil()
	}
	return false
}

func writeString(b *bytes.Buffer, value string) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	b.Write(encoded)
	return nil
}

// EventContentDigest derives the digest from the event's content alone,
// ignoring any stored SourceDigest field. It is the canonical content
// derivation; EventDigest is a thin wrapper that short-circuits on a stored
// SourceDigest (dedupe and chain hashing) and otherwise delegates here.
func EventContentDigest(e Event) (string, error) {
	copyEvent := e
	copyEvent.SourceDigest = ""
	copyEvent.ReceivedAt = time.Time{}
	copyEvent.ServerVersion = ""
	copyEvent.Hash = ""
	copyEvent.PrevHash = ""
	copyEvent.Sequence = 0
	copyEvent.StreamID = ""
	data, err := CanonicalJSON(copyEvent)
	if err != nil {
		return "", err
	}
	return HashBytes(data), nil
}

func EventDigest(e Event) (string, error) {
	if e.SourceDigest != "" {
		return e.SourceDigest, nil
	}
	return EventContentDigest(e)
}

// Cursor is an opaque pagination key in the chronological total order
// (OccurredAt, Sequence, EventID). Legacy reports a pre-chronological
// 2-tuple (Sequence, EventID) cursor; callers must reject Legacy cursors
// because per-stream sequence coordinates have no meaning in the
// chronological space.
type Cursor struct {
	OccurredAt time.Time
	Sequence   int64
	EventID    string
	Legacy     bool
}

// EncodeCursor encodes a cursor as a stable opaque string:
//   - chronological: [occurred_at_rfc3339nano_utc, sequence, event_id]
//   - legacy (Legacy=true): [sequence, event_id]  (byte-identical to today)
func EncodeCursor(c Cursor) string {
	var value any
	if c.Legacy {
		value = []any{c.Sequence, c.EventID}
	} else {
		value = []any{c.OccurredAt, c.Sequence, c.EventID}
	}
	data, _ := CanonicalJSON(value)
	return base64.RawURLEncoding.EncodeToString(data)
}

// DecodeCursor parses an opaque cursor. It accepts both the current 3-tuple
// (Legacy=false) and the legacy 2-tuple (Legacy=true); any other payload
// fails closed with ErrInvalid. The 3-tuple's first element must be an
// RFC3339 time (time.Time round-trips through CanonicalJSON's RFC3339Nano
// UTC normalization).
func DecodeCursor(cursor string) (Cursor, error) {
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return Cursor{}, fmt.Errorf("%w: invalid cursor", ErrInvalid)
	}
	var values []json.RawMessage
	if err := json.Unmarshal(data, &values); err != nil {
		return Cursor{}, fmt.Errorf("%w: invalid cursor", ErrInvalid)
	}
	var decoded Cursor
	switch len(values) {
	case 2:
		decoded.Legacy = true
		if err := json.Unmarshal(values[0], &decoded.Sequence); err != nil {
			return Cursor{}, fmt.Errorf("%w: invalid cursor sequence", ErrInvalid)
		}
		if err := json.Unmarshal(values[1], &decoded.EventID); err != nil {
			return Cursor{}, fmt.Errorf("%w: invalid cursor event", ErrInvalid)
		}
	case 3:
		if err := json.Unmarshal(values[0], &decoded.OccurredAt); err != nil {
			return Cursor{}, fmt.Errorf("%w: invalid cursor occurred_at", ErrInvalid)
		}
		if err := json.Unmarshal(values[1], &decoded.Sequence); err != nil {
			return Cursor{}, fmt.Errorf("%w: invalid cursor sequence", ErrInvalid)
		}
		if err := json.Unmarshal(values[2], &decoded.EventID); err != nil {
			return Cursor{}, fmt.Errorf("%w: invalid cursor event", ErrInvalid)
		}
	default:
		return Cursor{}, fmt.Errorf("%w: invalid cursor", ErrInvalid)
	}
	return decoded, nil
}
