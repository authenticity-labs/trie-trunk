package cache

import (
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestHourlyPACWaitsForClosingGrace(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 19, 12, 20, 0, 0, time.UTC)
	now := at.Add(20 * time.Minute)
	s, e := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if _, e = s.Append(Write, testFrame(t, l, at, 1, false)); e != nil {
		t.Fatal(e)
	}
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	if len(s.branches[Write].rows) != 0 {
		t.Fatal("open hour committed")
	}
	now = time.Date(2026, 10, 19, 13, 10, 0, 0, time.UTC)
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	if len(s.branches[Write].rows) != 0 {
		t.Fatal("closing hour committed before grace")
	}
	if _, e = s.Append(Write, testFrame(t, l, at.Add(time.Minute), 2, true)); e != nil {
		t.Fatal(e)
	}
	if os.Getenv("UPDATE_PAC_STORAGE_FIXTURE") == "1" {
		for _, mode := range []string{"native", "live"} {
			url := "/collect?tap=demo/payment/transactions.v1/write&from=2026-10-19T12:00:00Z&to=2026-10-19T13:00:00Z"
			if mode == "native" {
				url += "&format=native"
			}
			req := httptest.NewRequest("GET", url, nil)
			req.Header.Set("Accept", "application/pil")
			req.Header.Set("PIL-Version", "2")
			req.Header.Set("PIL-Features", "native-ranges,native-state-pages,adaptive-integers")
			req.Header.Set("Accept-Encoding", "gzip")
			w := httptest.NewRecorder()
			if mode == "live" {
				benchmarkStandaloneCollect(w, req, &HTTP{Store: s})
			} else {
				(&HTTP{Store: s}).serveHTTP(w, req)
			}
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			raw := w.Body.Bytes()
			if w.Header().Get("Content-Encoding") == "gzip" {
				raw = ungzipTest(t, raw)
			}
			if e := os.WriteFile("../../../testdata/pac-storage/"+mode+"-open.pil", raw, 0644); e != nil {
				t.Fatal(e)
			}
		}
	}
	now = now.Add(5 * time.Minute)
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	if len(s.branches[Write].rows) != 3*IndexRowSize {
		t.Fatal("sealed hour missing late event")
	}
	if s.branches[Write].runs[0].PAC.Compressed {
		t.Fatal("write packs must remain uncompressed")
	}
}
