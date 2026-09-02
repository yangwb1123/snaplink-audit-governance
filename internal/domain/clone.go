package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"
)

// ErrEventClone marks an event graph that cannot be cloned safely. Callers
// must fail closed because the source graph may retain shared mutable state.
var ErrEventClone = errors.New("event graph clone failed")

var timeType = reflect.TypeOf(time.Time{})

type cloneVisit struct {
	typ reflect.Type
	ptr uintptr
}

type cloneState struct {
	active map[cloneVisit]int
}

// CloneEvent returns a deep copy of event and every mutable descendant.
// Unsupported values, uninspectable structs, and cycles fail closed with
// ErrEventClone; on failure the returned Event is always the zero value.
func CloneEvent(event Event) (Event, error) {
	cloned, err := cloneValue(reflect.ValueOf(event), "event", &cloneState{active: map[cloneVisit]int{}})
	if err != nil {
		return Event{}, wrapCloneError(err)
	}
	copyEvent, ok := cloned.Interface().(Event)
	if !ok {
		return Event{}, wrapCloneError(fmt.Errorf("unexpected clone type %s", cloned.Type()))
	}
	return copyEvent, nil
}

func wrapCloneError(err error) error {
	if err == nil || errors.Is(err, ErrEventClone) {
		return err
	}
	return fmt.Errorf("%w: %v", ErrEventClone, err)
}

func cloneValue(value reflect.Value, path string, state *cloneState) (reflect.Value, error) {
	if !value.IsValid() {
		return reflect.Value{}, nil
	}
	if value.Type() == timeType {
		copyValue := reflect.New(value.Type()).Elem()
		copyValue.Set(value)
		return copyValue, nil
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		cloned, err := cloneValue(value.Elem(), path, state)
		if err != nil {
			return reflect.Value{}, err
		}
		copyValue := reflect.New(value.Type()).Elem()
		copyValue.Set(cloned)
		return copyValue, nil
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		release, err := enterCloneVisit(value, path, state)
		if err != nil {
			return reflect.Value{}, err
		}
		defer release()
		cloned, err := cloneValue(value.Elem(), path+"*", state)
		if err != nil {
			return reflect.Value{}, err
		}
		copyValue := reflect.New(value.Type().Elem())
		copyValue.Elem().Set(cloned)
		return copyValue, nil
	case reflect.Map:
		if value.Type().Key().Kind() != reflect.String {
			return reflect.Value{}, fmt.Errorf("%s: unsupported map key type %s", path, value.Type().Key())
		}
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		release, err := enterCloneVisit(value, path, state)
		if err != nil {
			return reflect.Value{}, err
		}
		defer release()
		copyValue := reflect.MakeMapWithSize(value.Type(), value.Len())
		iter := value.MapRange()
		for iter.Next() {
			key := iter.Key()
			cloned, err := cloneValue(iter.Value(), fmt.Sprintf("%s[%q]", path, key.String()), state)
			if err != nil {
				return reflect.Value{}, err
			}
			copyValue.SetMapIndex(key, cloned)
		}
		return copyValue, nil
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		release, err := enterCloneVisit(value, path, state)
		if err != nil {
			return reflect.Value{}, err
		}
		defer release()
		copyValue := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			cloned, err := cloneValue(value.Index(i), fmt.Sprintf("%s[%d]", path, i), state)
			if err != nil {
				return reflect.Value{}, err
			}
			copyValue.Index(i).Set(cloned)
		}
		return copyValue, nil
	case reflect.Array:
		copyValue := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			cloned, err := cloneValue(value.Index(i), fmt.Sprintf("%s[%d]", path, i), state)
			if err != nil {
				return reflect.Value{}, err
			}
			copyValue.Index(i).Set(cloned)
		}
		return copyValue, nil
	case reflect.Struct:
		copyValue := reflect.New(value.Type()).Elem()
		for i := 0; i < value.NumField(); i++ {
			fieldType := value.Type().Field(i)
			if fieldType.PkgPath != "" {
				return reflect.Value{}, fmt.Errorf("%s.%s: unsupported struct field %s", path, fieldType.Name, fieldType.Type)
			}
			cloned, err := cloneValue(value.Field(i), path+"."+fieldType.Name, state)
			if err != nil {
				return reflect.Value{}, err
			}
			copyValue.Field(i).Set(cloned)
		}
		return copyValue, nil
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.String:
		copyValue := reflect.New(value.Type()).Elem()
		copyValue.Set(value)
		return copyValue, nil
	case reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return reflect.Value{}, fmt.Errorf("%s: unsupported value kind %s", path, value.Kind())
	default:
		if _, ok := value.Interface().(json.Number); ok {
			copyValue := reflect.New(value.Type()).Elem()
			copyValue.Set(value)
			return copyValue, nil
		}
		return reflect.Value{}, fmt.Errorf("%s: unsupported value kind %s", path, value.Kind())
	}
}

func enterCloneVisit(value reflect.Value, path string, state *cloneState) (func(), error) {
	ptr := value.Pointer()
	if ptr == 0 {
		return func() {}, nil
	}
	visit := cloneVisit{typ: value.Type(), ptr: ptr}
	if state.active[visit] > 0 {
		return nil, fmt.Errorf("%s: cycle detected", path)
	}
	state.active[visit]++
	return func() {
		state.active[visit]--
		if state.active[visit] == 0 {
			delete(state.active, visit)
		}
	}, nil
}
