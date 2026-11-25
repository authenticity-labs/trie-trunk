package cache

import (
	"testing"
	"time"
)

func TestSummaryFixedOffsetBoundaries(t *testing.T) {
	for _, h := range []int{-12, -5, 0, 5, 14} {
		for _, stamp := range []string{"2026-01-01T02:00:00Z", "2026-03-08T07:00:00Z", "2026-11-01T06:00:00Z"} {
			ts, _ := time.Parse(time.RFC3339, stamp)
			day := summaryDayOffset(ts.UnixMilli(), h)
			if day > ts.UnixMilli() || ts.UnixMilli() >= day+summaryDayMS || (day+int64(h)*TimeHourMS)%summaryDayMS != 0 {
				t.Fatal(h, stamp, day)
			}
			if summaryDayOffset(day+summaryDayMS, h)-day != summaryDayMS {
				t.Fatal("day is not 24 hours")
			}
			for _, res := range []Resolution{Weekly, Monthly, Yearly} {
				a, z := calendarPeriod(res, ts.UnixMilli(), h)
				if a > day || z <= day || (a+int64(h)*TimeHourMS)%summaryDayMS != 0 {
					t.Fatal("calendar boundary", h, res, a, z)
				}
			}
		}
	}
}
