//go:build trunk_prototype

package cache

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNativeStatePagesHTTP(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	s, err := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return at.Add(4 * time.Hour) }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 1200; i++ {
		s.state.objects[string(testObject(uint32(i)).Bytes)] = uint64(i + 1)
	}
	s.state.sequence = 1200
	r := httptest.NewRequest("GET", "/collect?format=native&tap=demo/payment/transactions.v1/write&from=2026-10-16T12:00:00Z&to=2026-10-16T14:00:00Z", nil)
	r.Header.Set("Accept", "application/pil")
	r.Header.Set("Accept-Encoding", "gzip")
	r.Header.Set("PIL-Version", "2")
	r.Header.Set("PIL-Features", "native-ranges,native-state-pages,adaptive-integers")
	w := httptest.NewRecorder()
	(&HTTP{Store: s}).serveHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	_, chunks := readNativeTest(t, w.Body.Bytes())
	pages, unchanged := 0, 0
	for _, chunk := range chunks {
		if chunk.Kind != "state" {
			continue
		}
		pages++
		var page statePage
		if err := decoder.Unmarshal(chunk.Data, &page); err != nil {
			t.Fatal(err)
		}
		if page.Unchanged {
			unchanged++
		}
	}
	if pages != 2 || unchanged != 0 {
		t.Fatal("merged delivery repeated state", pages, unchanged)
	}
	if dir := os.Getenv("PIL_STATE_FIXTURE_DIR"); dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "ranges.pil"), ungzipTest(t, w.Body.Bytes()), 0644); err != nil {
			t.Fatal(err)
		}
	}
}
