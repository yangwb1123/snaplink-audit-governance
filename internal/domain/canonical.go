package domain

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"time"
)

// CanonicalJSON emits deterministic JSON for hash calculation. Map keys are
// sorted recursively and time values are encoded in RFC3339Nano UTC form.
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
		if t, ok := v.Interface().(time.Time); ok {
			return writeString(b, t.UTC().Format(time.RFC3339Nano))
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
		b.WriteString(strconv.FormatFloat(f, 'g', -1, v.Type().Bits()))
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
		data, err := json.Marshal(v.Interface())
		if err != nil {
			return err
		}
		var decoded any
		if err := json.Unmarshal(data, &decoded); err != nil {
			return err
		}
		return writeCanonical(b, reflect.ValueOf(decoded))
	default:
		return fmt.Errorf("unsupported canonical type %s", v.Type())
	}
	return nil
}

func writeString(b *bytes.Buffer, value string) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	b.Write(encoded)
	return nil
}

func EventDigest(e Event) (string, error) {
	if e.SourceDigest != "" {
		return e.SourceDigest, nil
	}
	copyEvent := e
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

func EncodeCursor(sequence int64, eventID string) string {
	data, _ := CanonicalJSON([]any{sequence, eventID})
	return base64.RawURLEncoding.EncodeToString(data)
}

func DecodeCursor(cursor string) (int64, string, error) {
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, "", fmt.Errorf("%w: invalid cursor", ErrInvalid)
	}
	var values []json.RawMessage
	if err := json.Unmarshal(data, &values); err != nil || len(values) != 2 {
		return 0, "", fmt.Errorf("%w: invalid cursor", ErrInvalid)
	}
	var sequence int64
	var eventID string
	if err := json.Unmarshal(values[0], &sequence); err != nil {
		return 0, "", fmt.Errorf("%w: invalid cursor sequence", ErrInvalid)
	}
	if err := json.Unmarshal(values[1], &eventID); err != nil {
		return 0, "", fmt.Errorf("%w: invalid cursor event", ErrInvalid)
	}
	return sequence, eventID, nil
}
