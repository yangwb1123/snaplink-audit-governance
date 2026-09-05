package domain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"time"
)

// DigestEncoding identifies the canonicalization that authenticated a stored
// content digest. Legacy is read-only compatibility for records written before
// lossless decimal and typed-nil handling were introduced.
type DigestEncoding uint8

const (
	DigestEncodingNone DigestEncoding = iota
	DigestEncodingLossless
	DigestEncodingLegacy
)

// MatchEventContentDigest accepts either the current canonical digest or the
// exact digest produced by the pre-lossless implementation. It deliberately
// does not expose the legacy bytes as a writer: new records always use
// EventContentDigest.
func MatchEventContentDigest(event Event, expected string) (DigestEncoding, error) {
	current, err := EventContentDigest(event)
	if err != nil {
		return DigestEncodingNone, err
	}
	if current == expected {
		return DigestEncodingLossless, nil
	}
	legacy, err := legacyEventContentDigest(event)
	if err != nil {
		return DigestEncodingNone, err
	}
	if legacy == expected {
		return DigestEncodingLegacy, nil
	}
	return DigestEncodingNone, nil
}

// legacyEventContentDigest is frozen compatibility code. In particular, the
// old implementation marshaled structs and decoded them into float64-backed
// interface values before sorting, and iterated nil maps/slices as empty
// collections. Event digests went through that struct JSON round-trip, so a
// typed-nil payload descendant was already JSON null in the historical event
// representation; matching does not guess ambiguous Go types after reload.
// Do not share this writer with the current canonicalizer.
func legacyEventContentDigest(event Event) (string, error) {
	copyEvent := event
	copyEvent.SourceDigest = ""
	copyEvent.ReceivedAt = time.Time{}
	copyEvent.ServerVersion = ""
	copyEvent.Hash = ""
	copyEvent.PrevHash = ""
	copyEvent.Sequence = 0
	copyEvent.StreamID = ""
	data, err := legacyCanonicalJSON(copyEvent)
	if err != nil {
		return "", err
	}
	return HashBytes(data), nil
}

func legacyCanonicalJSON(value any) ([]byte, error) {
	var b bytes.Buffer
	if err := writeLegacyCanonical(&b, reflect.ValueOf(value)); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func writeLegacyCanonical(b *bytes.Buffer, value reflect.Value) error {
	if !value.IsValid() {
		b.WriteString("null")
		return nil
	}
	if value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			b.WriteString("null")
			return nil
		}
		return writeLegacyCanonical(b, value.Elem())
	}
	if value.CanInterface() {
		if t, ok := value.Interface().(time.Time); ok {
			return writeLegacyString(b, t.UTC().Format(time.RFC3339Nano))
		}
	}
	switch value.Kind() {
	case reflect.String:
		return writeLegacyString(b, value.String())
	case reflect.Bool:
		b.WriteString(strconv.FormatBool(value.Bool()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		b.WriteString(strconv.FormatInt(value.Int(), 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		b.WriteString(strconv.FormatUint(value.Uint(), 10))
	case reflect.Float32, reflect.Float64:
		b.WriteString(strconv.FormatFloat(value.Float(), 'g', -1, value.Type().Bits()))
	case reflect.Slice, reflect.Array:
		b.WriteByte('[')
		for i := 0; i < value.Len(); i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := writeLegacyCanonical(b, value.Index(i)); err != nil {
				return err
			}
		}
		b.WriteByte(']')
	case reflect.Map:
		if value.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("canonical maps require string keys")
		}
		keys := value.MapKeys()
		sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
		b.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := writeLegacyString(b, key.String()); err != nil {
				return err
			}
			b.WriteByte(':')
			if err := writeLegacyCanonical(b, value.MapIndex(key)); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	case reflect.Struct:
		data, err := json.Marshal(value.Interface())
		if err != nil {
			return err
		}
		var decoded any
		if err := json.Unmarshal(data, &decoded); err != nil {
			return err
		}
		return writeLegacyCanonical(b, reflect.ValueOf(decoded))
	default:
		return fmt.Errorf("unsupported canonical type %s", value.Type())
	}
	return nil
}

func writeLegacyString(b *bytes.Buffer, value string) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	b.Write(encoded)
	return nil
}
