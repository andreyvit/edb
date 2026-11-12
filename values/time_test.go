package values

import (
	"math"
	"testing"
	"time"
)

func TestTimeRepresentation(t *testing.T) {
	for _, tc := range []struct {
		name string
		time time.Time
		wide bool
	}{
		{"zero", time.Time{}, false},
		{"epoch", time.Unix(0, 0), false},
		{"nanos", time.Unix(1234567890, 123456789), false},
		{"min_nanos", time.Unix(0, math.MinInt64), false},
		{"max_nanos", time.Unix(0, math.MaxInt64), false},
		{"before_min_nanos", time.Unix(0, math.MinInt64).Add(-time.Nanosecond), true},
		{"after_max_nanos", time.Unix(0, math.MaxInt64).Add(time.Nanosecond), true},
		{"reserved_word", time.Unix(0, -1), true},
		{"ancient", time.Date(-12345, 1, 2, 3, 4, 5, 6, time.UTC), true},
		{"future", time.Date(12345, 1, 2, 3, 4, 5, 6, time.UTC), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := Time(tc.time)
			if (v.P != nil) != tc.wide || tc.wide && v.R != 12 {
				t.Fatalf("unexpected representation: %+v", v)
			}
			if got := v.AsTime(); !got.Equal(tc.time) || got.Location() != time.UTC {
				t.Fatalf("round trip: got %v, want %v", got, tc.time)
			}
			if !tc.wide || tc.time.Equal(time.Unix(0, -1)) {
				if n := testing.AllocsPerRun(100, func() { _ = Time(tc.time).AsTime() }); n != 0 {
					t.Fatalf("scalar time conversion: %g allocations", n)
				}
			}
		})
	}
}
