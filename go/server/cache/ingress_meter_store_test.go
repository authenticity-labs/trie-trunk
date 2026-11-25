//go:build trunk_prototype

package cache

import (
	"testing"
)

func TestGeneratedLogIdentityWidthsAndReuse(t *testing.T) {
	l := testLogLayout(t)
	defer l.Close()
	values := make([]Value, len(l.Widths))
	values[l.Time] = Uint(1, 8)
	var scratch [10]byte
	for width := 5; width <= 10; width++ {
		values[l.Event] = Value{}
		if err := l.generateLogIdentity(values, scratch[:width]); err != nil {
			t.Fatal(err)
		}
		id := values[l.Event].Bytes
		if len(id) != width || &id[0] != &scratch[0] || width == 10 && id[0] > 3 {
			t.Fatal("width or buffer reuse", width)
		}
		// A supplied identity is retained even if the next generated width differs.
		if err := l.generateLogIdentity(values, nil); err != nil || &values[l.Event].Bytes[0] != &scratch[0] {
			t.Fatal("supplied identity", err)
		}
	}
}
