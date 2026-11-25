package cache

import (
	"math"
	"testing"
	"time"
)

func TestCalendarRollupBoundariesAndTotals(t *testing.T) {
	ms := func(s string) int64 {
		v, e := time.Parse("2006-01-02", s)
		if e != nil {
			t.Fatal(e)
		}
		return v.UnixMilli()
	}
	days := map[int64]summaryDayLocation{}
	// Leap day, absent durations, and unequal counts prove weighted averages.
	for i, date := range []string{"2024-02-28", "2024-02-29", "2024-03-01"} {
		a := ms(date)
		r := CalendarRollup{StatisticsComplete: true, Count: uint64(i + 1), DurationCount: uint64(i + 1), Sum: uint64((i + 1) * 10), Min: 10, Max: 10, Errors: uint64(i)}
		if i == 1 {
			r.DurationCount = 0
			r.Sum = 0
			r.Min = 0
			r.Max = 0
		}
		days[a] = summaryDayLocation{From: a, To: a + summaryDayMS, Rollup: r}
	}
	rows, e := calendarRollups(days, ms("2024-02-28"), ms("2024-03-03"))
	if e != nil {
		t.Fatal(e)
	}
	var months []CalendarRollup
	for _, r := range rows {
		switch r.Resolution {
		case Monthly:
			months = append(months, r)
		case Yearly, Weekly:
			if r.Count != 6 || r.DurationCount != 4 || r.Sum != 40 || r.Min != 10 || r.Max != 10 || r.Errors != 3 {
				t.Fatalf("bad totals %+v", r)
			}
			if r.CoveredTo != ms("2024-03-03") {
				t.Fatal("empty day lost")
			}
		}
	}
	if len(months) != 2 || months[0].Count != 3 || months[1].Count != 3 || months[0].To != ms("2024-03-01") {
		t.Fatal(months)
	}
	a, z := calendarPeriod(Weekly, ms("2021-01-01"))
	if a != ms("2020-12-28") || z != ms("2021-01-04") {
		t.Fatal("ISO week year", a, z)
	}
	// The same instant in a different timezone selects the same UTC rollup.
	instant := time.UnixMilli(ms("2024-03-01")).In(time.FixedZone("UTC-5", -5*3600))
	a, _ = calendarPeriod(Monthly, instant.UnixMilli())
	if a != ms("2024-03-01") {
		t.Fatal("timezone affected storage")
	}
	empty, e := calendarRollups(nil, ms("2024-01-01"), ms("2024-02-01"))
	if e != nil {
		t.Fatal(e)
	}
	for _, r := range empty {
		if r.Count != 0 || !r.StatisticsComplete {
			t.Fatal(r)
		}
	}
	// Totals must fail rather than wrap when crossing days.
	d := days[ms("2024-02-28")]
	d.Rollup.Count = math.MaxUint64
	days[ms("2024-02-28")] = d
	if _, e = calendarRollups(days, ms("2024-02-28"), ms("2024-03-03")); e == nil {
		t.Fatal("overflow accepted")
	}
}

func BenchmarkCalendarRollupsYear(b *testing.B) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	days := map[int64]summaryDayLocation{}
	for i := int64(0); i < 366; i++ {
		at := start + i*summaryDayMS
		days[at] = summaryDayLocation{From: at, To: at + summaryDayMS, Rollup: CalendarRollup{Count: 10000, DurationCount: 10000, Sum: 500000, Min: 1, Max: 100, StatisticsComplete: true}}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, e := calendarRollups(days, start, start+366*summaryDayMS); e != nil {
			b.Fatal(e)
		}
	}
}
