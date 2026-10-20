package main

import (
	"math"
	"testing"
)

func TestProtoSchemaVersionBoundsBeforeDial(t *testing.T) {
	for _, test := range []struct {
		value int64
		valid bool
	}{
		{math.MinInt64, false}, {-1, false}, {0, false},
		{1, true}, {math.MaxInt32, true},
		{math.MaxInt32 + 1, false}, {math.MaxInt64, false},
	} {
		got, err := protoSchemaVersion(test.value)
		if test.valid {
			if err != nil || int64(got) != test.value {
				t.Fatalf("value=%d got=%d err=%v", test.value, got, err)
			}
		} else if err == nil || got != 0 {
			t.Fatalf("invalid value=%d narrowed to %d err=%v", test.value, got, err)
		}
	}
}
