package cache

import (
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLiveSummaryAccumulatorCheckpoints(t *testing.T) {
	l := testLayout(t, false)
	hour := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	now := hour.Add(12 * time.Minute)
	s, e := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	appendAt := func(minute int, n uint32) {
		t.Helper()
		if _, e := s.Append(Read, testFrame(t, l, hour.Add(time.Duration(minute)*time.Minute), n, false)); e != nil {
			t.Fatal(e)
		}
		if e := s.Flush(); e != nil {
			t.Fatal(e)
		}
	}
	appendAt(1, 1)
	appendAt(7, 2)
	appendAt(11, 3)
	h := &HTTP{Store: s}
	request := func(checkpoint string) []byte {
		t.Helper()
		w := httptest.NewRecorder()
		path := fmt.Sprintf("/demo/payment/transactions.v1/read/summaries?definition=%s&live=%d%s", fmtDigestNative(l.Digest), hour.UnixMilli(), checkpoint)
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		if w.Header().Get("Trunk-Ingress") == "" {
			t.Fatal("missing ingress meter")
		}
		p, e := pil.OpenNestedPack(w.Body.Bytes())
		if e != nil || p.Count() != 1 {
			t.Fatal(e, p)
		}
		raw, e := p.Get(0)
		if e != nil {
			t.Fatal(e)
		}
		return raw
	}
	layout, e := summaryBucketLayout()
	if e != nil {
		t.Fatal(e)
	}
	raw := request("")
	p, e := pil.ParsePAC(layout, raw, nil)
	if e != nil {
		t.Fatal(e)
	}
	if len(p.Members) != 3 || len(p.Members[0]) != 50 || p.Headers["live-summary"][17] != 2 || order.Uint64(p.Headers["rollup"]) != 3 {
		t.Fatal("bad bucket snapshot", p)
	}
	if dir := os.Getenv("TRUNK_LIVE_SUMMARY_FIXTURE"); dir != "" {
		if e = os.MkdirAll(dir, 0755); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(dir, "initial.pac"), raw, 0644); e != nil {
			t.Fatal(e)
		}
	}
	revision := order.Uint64(p.Headers["live-summary"][18:])
	checkpoint := fmt.Sprintf("&checkpoint=%d,2,%d", hour.UnixMilli(), revision)
	appendAt(12, 4)
	raw = request(checkpoint)
	p, e = pil.ParsePAC(layout, raw, nil)
	if e != nil {
		t.Fatal(e)
	}
	if len(p.Members) != 1 || p.Headers["live-summary"][16] != 2 || order.Uint64(p.Headers["rollup"]) != 4 {
		t.Fatal("current bucket must replace, preserving prefix")
	}
	if dir := os.Getenv("TRUNK_LIVE_SUMMARY_FIXTURE"); dir != "" {
		if e = os.WriteFile(filepath.Join(dir, "delta.pac"), raw, 0644); e != nil {
			t.Fatal(e)
		}
	}
	appendAt(2, 5)
	raw = request(checkpoint)
	p, e = pil.ParsePAC(layout, raw, nil)
	if e != nil {
		t.Fatal(e)
	}
	if len(p.Members) != 3 || p.Headers["live-summary"][16] != 0 || order.Uint64(p.Headers["rollup"]) != 5 {
		t.Fatal("late ingress must invalidate prefix")
	}
	if dir := os.Getenv("TRUNK_LIVE_SUMMARY_FIXTURE"); dir != "" {
		if e = os.WriteFile(filepath.Join(dir, "late.pac"), raw, 0644); e != nil {
			t.Fatal(e)
		}
	}
	// Old snapshots retain immutable aggregate values after later ingress.
	snap := s.resident(Read)
	defer snap.release()
	before := snap.liveSummaries[hour.UnixMilli()].Total.Count
	appendAt(12, 6)
	if snap.liveSummaries[hour.UnixMilli()].Total.Count != before {
		t.Fatal("mutated published summary")
	}
}

func TestLiveSummaryCompletedHourAndEmptyHour(t *testing.T) {
	l, err := summaryBucketLayout()
	if err != nil {
		t.Fatal(err)
	}
	hour := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC).UnixMilli()
	for _, complete := range []int{0, 12} {
		raw, err := encodeLiveSummary(hour, hour+TimeHourMS, liveSummaryHour{}, summaryCheckpoint{record: complete})
		if err != nil {
			t.Fatal(err)
		}
		p, err := pil.ParsePAC(l, raw, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Members) != 12-complete || p.Headers["live-summary"][16] != byte(complete) || order.Uint64(p.Headers["rollup"]) != 0 {
			t.Fatal("invalid empty/completed hour")
		}
	}
}

func TestLiveSummaryRestartAndBranchWithoutDailySummaries(t *testing.T) {
	l := testLayout(t, false)
	hour := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	now := hour.Add(12 * time.Minute)
	config := Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Append(Trace, testFrame(t, l, hour.Add(time.Minute), 1, false)); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := &HTTP{Store: s}
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", fmt.Sprintf("/demo/payment/transactions.v1/trace/summaries?definition=%s&live=%d", fmtDigestNative(l.Digest), hour.UnixMilli()), nil))
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		p, err := pil.OpenNestedPack(w.Body.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		raw, err := p.Get(0)
		if err != nil {
			t.Fatal(err)
		}
		layout, _ := summaryBucketLayout()
		v, err := pil.ParsePAC(layout, raw, nil)
		if err != nil {
			t.Fatal(err)
		}
		if order.Uint64(v.Headers["rollup"]) != 1 {
			t.Fatal("request lost or double-counted recovered ingress")
		}
	}
}
