package runtimeconfig

import (
	"testing"
	"time"
)

func TestS3RetentionDurationMachineWordBounds(t *testing.T) {
	for _, test := range []struct {
		days uint
		want time.Duration
	}{
		{1, 24 * time.Hour},
		{maxArchiveRetentionDays, 36500 * 24 * time.Hour},
		{0, 0},
		{maxArchiveRetentionDays + 1, 0},
		{^uint(0), 0},
	} {
		got, err := s3RetentionDuration(test.days)
		if test.want == 0 {
			if err == nil || got != 0 {
				t.Fatalf("days=%d duration=%v err=%v", test.days, got, err)
			}
		} else if err != nil || got != test.want {
			t.Fatalf("days=%d duration=%v err=%v, want %v", test.days, got, err, test.want)
		}
	}
}
