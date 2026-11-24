package cache

import (
	"bytes"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
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

func TestSummaryOffsetInheritanceReloadAndReopen(t *testing.T) {
	l := testLayout(t, false)
	start := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	cfg := Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return start.Add(72 * time.Hour) }}
	writeOffset := func(scope []string, h int) {
		t.Helper()
		p := configurationPath(cfg.Root, scope)
		if e := os.MkdirAll(filepath.Dir(p), 0700); e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(p, []byte(fmt.Sprintf("version=1\n[policy]\nsummary_utc_offset_hours=%d\n", h)), 0600); e != nil {
			t.Fatal(e)
		}
	}
	writeOffset(nil, -5)
	s, e := Open(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { s.Close() }()
	for i := 0; i < 48; i++ {
		if _, e = s.Append(Read, testFrame(t, l, start.Add(time.Duration(i)*time.Hour), uint32(i+1), false)); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	check := func(h int) {
		t.Helper()
		b, e := s.openBranch(Read)
		if e != nil {
			t.Fatal(e)
		}
		if s.policyFor(Read).summaryOffsetHours() != h {
			t.Fatal("inheritance", s.policyFor(Read))
		}
		if len(b.summaryDays) == 0 {
			t.Fatal("no summary days")
		}
		count := 0
		for day, loc := range b.summaryDays {
			if summaryDayOffset(day, h) != day {
				t.Fatal("wrong day", h, day)
			}
			raw, e := readBytes(b.sum, loc.Offset, loc.Offset+loc.Length)
			if e != nil {
				t.Fatal(e)
			}
			d, _, _, sums, e := decodeSummaryDay(raw)
			if e != nil || d != day {
				t.Fatal("PAC", e, d, day)
			}
			count += len(sums)
			if dest := os.Getenv("SUMMARY_OFFSET_FIXTURE"); dest != "" && h == -5 && day == start.UnixMilli()+5*TimeHourMS {
				if e = os.WriteFile(dest, raw, 0600); e != nil {
					t.Fatal(e)
				}
			}
		}
		api := &HTTP{Store: s}
		day := summaryDayOffset(start.UnixMilli()+12*TimeHourMS, h)
		packets := requestSummaryPacks(t, api, []summaryPackQuery{{Source: api.tap(Read), Definition: fmtDigestNative(l.Digest), Day: day}})
		if packets.Code != 200 {
			t.Fatal("offset range", packets.Code, packets.Body.String())
		}
		url := fmt.Sprintf("/%s/summaries?beginTime=%d&endTime=%d", api.tap(Read), day, day+summaryDayMS)
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest("GET", url, nil))
		if response.Code != 200 {
			t.Fatal("offset bounds", response.Code, response.Body.String())
		}
		if h != 0 {
			bad := requestSummaryPacks(t, api, []summaryPackQuery{{Source: api.tap(Read), Definition: fmtDigestNative(l.Digest), Day: start.UnixMilli()}})
			if bad.Code != 400 {
				t.Fatal("accepted wrong day boundary", bad.Code)
			}
		}
		if count != 48 {
			t.Fatal("lost hours", count)
		}
		raw, e := os.ReadFile(filepath.Join(s.dir, s.prefix(Read)+".time"))
		if e != nil || int(int8(raw[47])) != h {
			t.Fatal("timeline offset", e)
		}
	}
	check(-5)
	b := s.branches[Read]
	packs, e := os.ReadFile(b.packs.Name())
	if e != nil {
		t.Fatal(e)
	}
	name := l.Definition.Resource
	resource := []string{cfg.Cache, name.Service, name.Resource + ".v1"}
	writeOffset(resource, 5)
	if e = s.ReloadPolicy(); e != nil {
		t.Fatal(e)
	}
	check(5)
	after, e := os.ReadFile(b.packs.Name())
	if e != nil || !bytes.Equal(packs, after) {
		t.Fatal("offset changed event storage", e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(cfg)
	if e != nil {
		t.Fatal(e)
	}
	check(5)
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	writeOffset(append(resource, "read"), 0)
	s, e = Open(cfg)
	if e != nil {
		t.Fatal(e)
	}
	check(0)
}
