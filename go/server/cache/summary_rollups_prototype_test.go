//go:build trunk_prototype

package cache

import (
	"bytes"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
	"time"
)

func TestCalendarRollupPublicationRecoveryRetention(t *testing.T) {
	l := testLayout(t, false)
	start := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	now := start.Add(3*24*time.Hour + 15*time.Minute)
	cfg := Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }}
	s, e := Open(cfg)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 72; i++ {
		if _, e = s.Append(Read, testFrame(t, l, start.Add(time.Duration(i)*time.Hour), uint32(i+1), false)); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	rows, e := s.CalendarRollups(Read, Monthly, start, start.AddDate(0, 1, 0))
	if e != nil {
		t.Fatal(e)
	}
	if len(rows) != 2 || rows[0].Count != 24 || rows[1].Count != 48 {
		t.Fatalf("monthly %+v", rows)
	}
	before := append([]CalendarRollup(nil), s.branches[Read].calendarRollups...)
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	b, e := s.openBranch(Read)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(before, b.calendarRollups) {
		t.Fatal("restart changed totals")
	}
	// A partial retained day must not retain its expired counts in parent totals.
	s.mu.Lock()
	e = s.prune(Read, start.Add(12*time.Hour))
	s.mu.Unlock()
	if e != nil {
		t.Fatal(e)
	}
	rows, e = s.CalendarRollups(Read, Monthly, start, start.AddDate(0, 1, 0))
	if e != nil {
		t.Fatal(e)
	}
	if len(rows) != 2 || rows[0].Count != 12 || rows[0].CoveredFrom != start.Add(12*time.Hour).UnixMilli() || rows[1].Count != 48 {
		t.Fatalf("retained %+v", rows)
	}
	// Advancing across known empty days still advances coverage, without new events.
	now = now.AddDate(0, 0, 2)
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	rows, e = s.CalendarRollups(Read, Monthly, start, start.AddDate(0, 1, 0))
	if e != nil {
		t.Fatal(e)
	}
	if rows[1].CoveredTo != start.AddDate(0, 0, 5).UnixMilli() || rows[1].Count != 48 {
		t.Fatal("empty-day rollover", rows)
	}
	if _, e = s.CalendarRollups(Debug, Monthly, start, now); e == nil {
		t.Fatal("debug aggregates")
	}
	// Checksummed but inconsistent derived totals must still fail validation.
	b = s.branches[Read]
	raw, e := readBytes(b.sum, 72, b.sumEnd)
	if e != nil {
		t.Fatal(e)
	}
	at := bytes.Index(raw, []byte("SUMROLL0"))
	if at < 0 {
		t.Fatal("no persisted rollups")
	}
	n := int(order.Uint32(raw[at-8:]))
	packet := bytes.Clone(raw[at : at+n])
	packet[24+40] ^= 1
	if e = b.sum.Truncate(int64(72 + at - 8)); e != nil {
		t.Fatal(e)
	}
	if _, e = b.sum.Seek(0, 2); e != nil {
		t.Fatal(e)
	}
	if e = appendBoundedRecord(b.sum, packet, maxSummaryDay+32); e != nil {
		t.Fatal(e)
	}
	if e = s.loadSummaries(b); e == nil {
		t.Fatal("inconsistent rollup accepted")
	}
}

func TestCalendarRollupHTTPSelection(t *testing.T) {
	l := testLayout(t, false)
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	s, e := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return start.Add(25 * time.Hour) }})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if _, e = s.Append(Read, testFrame(t, l, start, 1, false)); e != nil {
		t.Fatal(e)
	}
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	get := func(res, known, query string) (int, ViewResult) {
		q := url.Values{"tap": {"demo/payment/transactions.v1/read"}, "from": {start.Format(time.RFC3339)}, "to": {start.Add(24 * time.Hour).Format(time.RFC3339)}, "include": {"summary"}}
		if query != "" {
			q.Set("query", query)
		}
		r := httptest.NewRequest("GET", "/events?"+q.Encode(), nil)
		r.Header.Set("Accept", "application/pil")
		r.Header.Set("PIL-Version", "2")
		r.Header.Set("PIL-Features", "pac-arrays,daily-summaries")
		r.Header.Set("Summary-Resolution", res)
		r.Header.Set("Summary-Known", known)
		w := httptest.NewRecorder()
		(&HTTP{Store: s}).serveHTTP(w, r)
		if w.Code != 200 {
			return w.Code, ViewResult{}
		}
		return w.Code, decodeView(t, l, w.Body.Bytes()).result
	}
	code, base := get("", "", "")
	if code != 200 || base.Analytics == nil || len(base.Analytics.Rollups) != 0 {
		t.Fatal("default view changed", code)
	}
	code, monthly := get("month", base.SummaryVersion, "")
	if code != 200 || monthly.Analytics == nil || len(monthly.Analytics.Rollups) != 1 || monthly.SummaryVersion == base.SummaryVersion {
		t.Fatal("resolution cache identity", code)
	}
	if monthly.Analytics.Rollups[0].Buckets[0].Count != 1 {
		t.Fatal("incorrect count")
	}
	code, cached := get("month", monthly.SummaryVersion, "")
	if code != 200 || cached.Summary != nil {
		t.Fatal("known summary not reused", code)
	}
	if code, _ = get("decade", "", ""); code != 400 {
		t.Fatal("invalid resolution accepted")
	}
	if code, _ = get("month", "", `{"search":{"text":"timeout","mode":"phrase"}}`); code != 400 {
		t.Fatal("unfiltered totals allowed for text query", code)
	}
}
