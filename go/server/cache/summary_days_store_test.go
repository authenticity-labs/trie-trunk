//go:build trunk_prototype

package cache

import (
	"bytes"
	"reflect"
	"testing"
)

func TestDailySummaryCodec(t *testing.T) {
	sum := aggregateFixture()
	sum.Statistics = []statisticsGroup{
		{Period: 0, Labels: [4]string{"payment", "transactions.v1", "read", "INFO"}, Count: 5, Durations: [][2]uint64{{4, 1}, {6, 1}}},
		{Period: sumBucketMS, Labels: [4]string{"payment", "transactions.v1", "read", "ERROR"}, Count: 1, Errors: 1, Durations: [][2]uint64{{0, 1}}},
	}
	for _, compressed := range []bool{false, true} {
		raw, e := encodeSummaryDay(0, 0, summaryDayMS, []Summary{sum}, compressed)
		if e != nil {
			t.Fatal(e)
		}
		day, from, to, got, e := decodeSummaryDay(raw)
		if e != nil || day != 0 || from != 0 || to != summaryDayMS || !reflect.DeepEqual(got, []Summary{sum}) {
			t.Fatalf("round trip: %v %+v", e, got)
		}
		for n := 0; n < len(raw); n++ {
			if _, _, _, _, e = decodeSummaryDay(raw[:n]); e == nil {
				t.Fatalf("truncation %d", n)
			}
		}
		for _, at := range []int{0, 8, 12, 16, 20, 24, 32, len(raw) - 1} {
			bad := bytes.Clone(raw)
			bad[at] ^= 255
			if _, _, _, _, e = decodeSummaryDay(bad); e == nil {
				t.Fatalf("corruption %d", at)
			}
		}
	}
	if _, e := encodeSummaryDay(0, 0, summaryDayMS, []Summary{{}}, true); e == nil {
		t.Fatal("empty chunk")
	}
	if _, e := encodeSummaryDay(0, 0, summaryDayMS, []Summary{sum, sum}, true); e == nil {
		t.Fatal("duplicate hour")
	}
	// A PAC needs at least one summary member to carry its day coverage.
	if _, e := encodeSummaryDay(0, TimeHourMS, 3*TimeHourMS, nil, true); e == nil {
		t.Fatal("empty summary PAC accepted")
	}
}
