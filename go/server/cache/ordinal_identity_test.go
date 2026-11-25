package cache

import (
	"testing"
	"time"
)

// Compare event contents across the ingress-to-storage identity boundary.
func sameSeedData(t testing.TB, l *Layout, a, b []byte) bool {
	t.Helper()
	var x, y Frame
	if err := l.Scan(a, &x); err != nil {
		t.Fatal(err)
	}
	if err := l.Scan(b, &y); err != nil {
		t.Fatal(err)
	}
	if eventTime(x.Values[l.Event].Bytes) != eventTime(y.Values[l.Event].Bytes) {
		return false
	}
	for i := range x.Values {
		if i == l.Event || i == l.Record || i == l.Time || i == l.Leaf || i == l.Shared {
			continue
		}
		if !l.equal(i, x.Values[i], y.Values[i]) {
			return false
		}
	}
	return true
}

func TestOrdinalTimeTrieAddressBounds(t *testing.T) {
	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	run := timeRun(at, 1, maxHourRecords)
	directory, err := buildTimeDirectory([]Run{run}, at, at+TimeHourMS, run.End, [16]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	for _, ordinal := range []uint32{0, 1, 65535, 65536, 0xffffff} {
		row, e := directory.resolveReference(collectionAddress{0, ordinal + 1})
		if e != nil || row != run.Start+uint64(ordinal) {
			t.Fatal(ordinal, row, e)
		}
	}
	if _, e := directory.resolveReference(collectionAddress{0, (1 << 24) + 1}); e == nil {
		t.Fatal("accepted record overflow")
	}
}
