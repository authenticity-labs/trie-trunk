package cache

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func exportViewNative(t *testing.T, dir string, h *HTTP, from, to time.Time) {
	t.Helper()
	r := httptest.NewRequest("GET", "/collect?format=native&tap="+url.QueryEscape(h.tap(Write))+"&from="+url.QueryEscape(from.Format(time.RFC3339))+"&to="+url.QueryEscape(to.Format(time.RFC3339)), nil)
	r.Header.Set("Authorization", "Bearer test")
	r.Header.Set("Accept", "application/pil")
	r.Header.Set("PIL-Version", "2")
	r.Header.Set("PIL-Features", "native-ranges,native-state-pages,adaptive-integers")
	w := httptest.NewRecorder()
	h.serveHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(fmt.Sprint(w.Code), w.Body.String())
	}
	if err := os.WriteFile(filepath.Join(dir, "native.pil"), w.Body.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
	b := h.Store.branches[Write]
	digest := fmtDigestNative(h.Store.layout.Digest)
	get := func(path string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer "+h.Token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatal(path, w.Code, w.Body.String())
		}
		return w
	}
	base := "/" + h.tap(Write)
	q := url.Values{"definition": {digest}, "beginTime": {fmt.Sprint(from.UnixMilli())}, "endTime": {fmt.Sprint(to.UnixMilli())}}
	timeline := get(base + "/timelines?" + q.Encode())
	seeds := get(base + "/packs?" + q.Encode())
	states := get("/" + strings.TrimSuffix(h.tap(Write), "/write") + "/states?definition=" + digest)
	// Fixtures preserve the full native index and the internal local timeline
	// patch; the mock responder slices them using the real request accessors.
	idx, e := os.ReadFile(b.idx.Name())
	if e != nil {
		t.Fatal(e)
	}
	idx = idx[headerSize:]
	tri, e := b.time.window(max(from.UnixMilli(), b.time.first()), min(to.UnixMilli(), b.time.sealed()), true)
	if e != nil {
		t.Fatal(e)
	}
	headers := timeline.Header().Clone()
	headers.Set("Trunk-Generation", fmt.Sprintf("%x", b.generation))
	headers.Set("Trunk-Source", h.tap(Write))
	headers.Set("Definition-Version", digest)
	meta, _ := json.Marshal(headers)
	for name, raw := range map[string][]byte{"native.pac": seeds.Body.Bytes(), "native.indx": idx, "native.time": tri, "native.state": states.Body.Bytes(), "native.headers.json": meta, "definition.rx": h.Store.layout.Bytes} {
		if e = os.WriteFile(filepath.Join(dir, name), raw, 0644); e != nil {
			t.Fatal(e)
		}
	}
}

// A page spanning six sealed hours exercises bounded parallel retrieval and
// restoration of query order after physical row batching in the client.
func TestNativeViewParallelHoursFixture(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	p, err := OpenEndpoint(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return at.Add(7 * time.Hour) }})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for h := 0; h < 6; h++ {
		for n := 0; n < 2; n++ {
			if _, err = p.api.Store.Append(Write, testFrame(t, l, at.Add(time.Duration(h)*time.Hour+time.Duration(n)*10*time.Minute), uint32(h*2+n+1), false)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if dir := os.Getenv("PIL_PARALLEL_FIXTURE_DIR"); dir != "" {
		if err = os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		exportViewNative(t, dir, p.api, at, at.Add(6*time.Hour))
	}
}
