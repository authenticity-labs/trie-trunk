//go:build trunk_prototype

package cache

import (
	"bytes"
	"encoding/json"
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSummarySparsePACStream(t *testing.T) {
	l := testLayout(t, false)
	start := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	now := start.Add(73*time.Hour + 30*time.Minute)
	s, e := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	for i := 0; i < 4; i++ {
		if _, e = s.Append(Read, testFrame(t, l, start.Add(time.Duration(i)*24*time.Hour), 1, false)); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	h := &HTTP{Store: s}
	q := url.Values{"branch": {"payment/transactions.v1/read"}, "definition": {fmtDigestNative(l.Digest)}}
	for _, i := range []int{2, 0, 2, 3} {
		a := start.Add(time.Duration(i) * 24 * time.Hour).UnixMilli()
		q.Add("range", fmt.Sprintf("%d,%d", a, a+summaryDayMS))
	}
	request := func(q url.Values) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/demo/"+q.Get("branch")+"/summaries?"+withoutBranch(q), nil))
		return w
	}
	w := request(q)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	p, e := pil.ParsePAC(nil, w.Body.Bytes(), nil)
	if e != nil || len(p.Members) != 3 {
		t.Fatal(e, p)
	}
	b, _ := s.openBranch(Read)
	for i, day := range []int{0, 2, 3} {
		raw := p.Members[i]
		d, _, _, _, sealed, e := decodeSummaryPACInfo(raw)
		if e != nil || d != start.Add(time.Duration(day)*24*time.Hour).UnixMilli() || sealed != (day < 3) {
			t.Fatal(d, sealed, e)
		}
		if day < 3 {
			if order.Uint32(raw[4:8]) != 514 {
				t.Fatal("sealed summary must use default Zstd compression")
			}
			plain, err := expandPAC(raw)
			if err != nil {
				t.Fatal(err)
			}
			expected, err := compressPAC(plain, 1)
			if err != nil || !bytes.Equal(raw, expected) {
				t.Fatal("summary must seal at Zstd level 1", err)
			}
			loc := b.summaryDays[d]
			disk, e := readBytes(b.sum, loc.Offset, loc.Offset+loc.Length)
			if e != nil || !bytes.Equal(raw, disk) {
				t.Fatal("stored PAC changed", e)
			}
		}
	}
	if dir := os.Getenv("TRUNK_SUMMARY_FIXTURE"); dir != "" {
		dir = filepath.Join(dir, "sparse")
		if e = os.MkdirAll(dir, 0755); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(dir, "response.pac"), w.Body.Bytes(), 0644); e != nil {
			t.Fatal(e)
		}

		// Export the already-replicated middle day as real durable fixture data.
		middle := b.summaryDays[start.Add(24*time.Hour).UnixMilli()]
		cached, err := readBytes(b.sum, middle.Offset, middle.Offset+middle.Length)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, "cached.pac"), cached, 0644); err != nil {
			t.Fatal(err)
		}

		tq := url.Values{"branch": {"payment/transactions.v1/read"}, "definition": {fmtDigestNative(l.Digest)}, "beginTime": {fmt.Sprint(start.UnixMilli())}, "endTime": {fmt.Sprint(start.Add(73 * time.Hour).UnixMilli())}}
		tw := httptest.NewRecorder()
		h.ServeHTTP(tw, httptest.NewRequest("GET", "/demo/payment/transactions.v1/read/timelines?"+withoutBranch(tq), nil))
		if tw.Code != 200 {
			t.Fatal(tw.Code, tw.Body.String())
		}
		patch, e := b.time.window(start.UnixMilli(), start.Add(73*time.Hour).UnixMilli(), false)
		if e != nil {
			t.Fatal(e)
		}
		for name, data := range map[string][]byte{"timeline.time": patch, "timeline.bin": tw.Body.Bytes(), "definition.rx": l.Bytes} {
			if e = os.WriteFile(filepath.Join(dir, name), data, 0644); e != nil {
				t.Fatal(e)
			}
		}
		meta, e := json.Marshal(w.Header())
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(dir, "headers.json"), meta, 0644); e != nil {
			t.Fatal(e)
		}
	}

	q.Set("beginTime", "0")
	if w = request(q); w.Code != 400 {
		t.Fatal(w.Code)
	}
	q.Del("beginTime")
	q.Set("branch", "payment/transactions.v1/*")
	if w = request(q); w.Code != 404 {
		t.Fatal("mixed branch", w.Code)
	}
}
