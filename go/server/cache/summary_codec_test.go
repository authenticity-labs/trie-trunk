package cache

import (
	"bytes"
	"reflect"
	"testing"
)

func aggregateFixture() Summary {
	return summaryFromGroups(4, 10, []Group{
		{0, 0, Aggregate{Count: 2}},
		{0, 7, Aggregate{Count: 3, DurationCount: 2, Sum: 10, Min: 4, Max: 6}},
		{300000, 2, Aggregate{Count: 1, DurationCount: 1}},
	})
}

func TestFixedSummaryLayout(t *testing.T) {
	s := aggregateFixture()
	raw, e := encodeSummary(s)
	if e != nil {
		t.Fatal(e)
	}
	if string(raw[:8]) != "SUMCOL00" || len(raw) != 116+summaryEncodingSize+3*14+2 {
		t.Fatalf("layout %d", len(raw))
	}
	var got Summary
	if e = decodeSummary(raw, &got); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(got, s) {
		t.Fatal("round trip")
	}
	for n := 0; n < len(raw); n++ {
		if decodeSummary(raw[:n], &got) == nil {
			t.Fatalf("truncation %d", n)
		}
	}
	for _, at := range []int{0, 8, 16, 24, 32, 36, 40, 44, 48, 64, 68} {
		b := bytes.Clone(raw)
		for j := 0; j < 4; j++ {
			b[at+j] = 255
		}
		if decodeSummary(b, &got) == nil {
			t.Fatalf("corruption %d", at)
		}
	}
	if decodeSummary(append(bytes.Clone(raw), 0), &got) == nil {
		t.Fatal("trailing data")
	}
}

func TestFixedRollupLayout(t *testing.T) {
	r := rollup{From: -3600000, To: 0, Groups: []Group{{-3600000, 1, Aggregate{Count: 2, DurationCount: 1, Sum: 20, Min: 20, Max: 20}}}}
	for _, groups := range [][]Group{r.Groups, {}} {
		r.Groups = groups
		raw, e := encodeRollup(r)
		if e != nil {
			t.Fatal(e)
		}
		if len(raw) != 24+42*len(groups) {
			t.Fatal("rollup stride")
		}
		var got rollup
		if e = decodeRollup(raw, &got); e != nil {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(got, r) {
			t.Fatal("rollup round trip")
		}
		for n := 0; n < len(raw); n++ {
			var got rollup
			if decodeRollup(raw[:n], &got) == nil {
				t.Fatalf("accepted truncation %d", n)
			}
		}
	}
}
