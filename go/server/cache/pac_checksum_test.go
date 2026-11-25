package cache

import (
	"testing"
	"time"
)

func TestPACChecksumFollowsStorageEncoding(t *testing.T) {
	l := testLayout(t, false)
	frame := testFrame(t, l, time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC), 1, false)
	for _, compressed := range []bool{false, true} {
		p, err := factorPAC(l, [][]byte{frame}, nil, true)
		if err != nil {
			t.Fatal(err)
		}
		p.Headers = map[string][]byte{"application": []byte("preserved")}
		allocations := 0
		var allocation []byte
		raw, err := marshalStoredPAC(l, p, compressed, func(n int) ([]byte, error) {
			allocations++
			allocation = make([]byte, n)
			return allocation, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if allocations != 1 || &raw[0] != &allocation[0] {
			t.Fatal("PAC payload was copied or allocated more than once")
		}
		bound, err := ParsePAC(l, raw, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, hasChecksum := bound.Headers[pacChecksumHeader]
		if hasChecksum == compressed || string(bound.Headers["application"]) != "preserved" {
			t.Fatal("storage encoding did not determine checksum policy or headers lost")
		}
	}
}
