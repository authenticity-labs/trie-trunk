package cache

import (
	"testing"
	"time"
)

func TestCompactReplicationBounds(t *testing.T) {
	for _, tc := range []struct {
		raw    string
		hourly bool
		want   string
	}{
		{"2600100", true, "2026-01-01T00:00:00Z"}, {"2836623", true, "2028-12-31T23:00:00Z"}, {"26001", false, "2026-01-01T00:00:00Z"},
	} {
		got, err := compactTime(tc.raw, tc.hourly)
		if err != nil || got.Format(time.RFC3339) != tc.want {
			t.Fatal(tc, got, err)
		}
	}
	for _, raw := range []string{"2600000", "2636600", "2836700", "2600124", "26x0100", "202600100"} {
		if _, err := compactTime(raw, true); err == nil {
			t.Fatal("accepted invalid UTC bound", raw)
		}
	}
}
