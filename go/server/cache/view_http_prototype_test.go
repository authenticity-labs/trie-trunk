//go:build trunk_prototype

package cache

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPagedViewNegotiationSnapshotAndCache(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	now := at.Add(time.Hour)
	s, e := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	originals := [][]byte{}
	for i := 0; i < 7; i++ {
		f := testFrame(t, l, at.Add(time.Duration(i)*time.Minute), uint32(i+1), false)
		originals = append(originals, f)
		if _, e = s.Append(Write, f); e != nil {
			t.Fatal(e)
		}
	}
	h := &HTTP{Store: s, Token: "test"}
	path := "/events?tap=" + url.QueryEscape(h.tap(Write)) + "&from=2026-09-01T12:00:00Z&to=2026-09-01T14:00:00Z&page=2"
	get := func(path string, headers map[string]string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer test")
		r.Header.Set("Accept", "application/pil")
		r.Header.Set("PIL-Version", "2")
		r.Header.Set("PIL-Features", "pac-arrays,adaptive-integers")
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		h.serveHTTP(w, r)
		return w
	}
	first := get(path, nil)
	if first.Code != 200 {
		t.Fatal(first.Code, first.Body.String())
	}
	d := collectView(t, l, h, first.Body.Bytes())
	if d.definitions != 1 || len(d.frames) != 2 || d.result.Summary.Count != 7 || d.result.Next == "" {
		t.Fatalf("first %#v", d)
	}
	for i, f := range d.frames {
		if !sameSeedData(t, l, f, originals[i]) {
			t.Fatal("changed frame")
		}
	}
	compressed := get(path, map[string]string{"Accept-Encoding": "gzip"})
	if compressed.Code != 200 || compressed.Header().Get("Content-Encoding") != "gzip" {
		t.Fatal("missing whole-response gzip")
	}
	gz, err := gzip.NewReader(bytes.NewReader(compressed.Body.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := io.ReadAll(gz)
	gz.Close()
	if err != nil {
		t.Fatal(err)
	}
	z := collectView(t, l, h, decoded)
	if len(z.frames) != len(d.frames) || z.result.Summary.Count != d.result.Summary.Count {
		t.Fatal("gzip response differs")
	}
	for i := range z.frames {
		if !bytes.Equal(z.frames[i], d.frames[i]) {
			t.Fatal("gzip changed frame")
		}
	}
	if no := get(path, map[string]string{"Accept-Encoding": "gzip;q=0"}); no.Header().Get("Content-Encoding") != "" {
		t.Fatal("ignored gzip rejection")
	}
	known := []string{}
	for _, r := range d.result.Page {
		known = append(known, r.Key)
	}
	warm := get(path, map[string]string{"Definition-Known": fmtDigest(l), "Summary-Known": d.result.SummaryVersion, "Event-Known": strings.Join(known, ",")})
	warmCompressed := get(path, map[string]string{"Accept-Encoding": "gzip", "Definition-Known": fmtDigest(l), "Summary-Known": d.result.SummaryVersion, "Event-Known": strings.Join(known, ",")})
	w := collectView(t, l, h, warm.Body.Bytes())
	if w.definitions != 0 || len(w.frames) != 0 || w.result.Summary != nil || len(w.result.Page) != 2 || !w.result.Page[0].Cached {
		t.Fatal("cache negotiation failed")
	}
	// A newly finalized event must not enter an existing cursor's tail or summary.
	if _, e = s.Append(Write, testFrame(t, l, at.Add(50*time.Minute), 99, false)); e != nil {
		t.Fatal(e)
	}
	now = now.Add(time.Hour)
	seen := 2
	var nextFixture, nextCompressed []byte
	cursor := d.result.Next
	for cursor != "" {
		reply := get(path+"&after="+url.QueryEscape(cursor), map[string]string{"Summary-Known": d.result.SummaryVersion})
		if reply.Code != 200 {
			t.Fatal(reply.Code, reply.Body.String())
		}
		if nextFixture == nil {
			nextFixture = append([]byte(nil), reply.Body.Bytes()...)
			nextCompressed = get(path+"&after="+url.QueryEscape(cursor), map[string]string{"Accept-Encoding": "gzip", "Summary-Known": d.result.SummaryVersion}).Body.Bytes()
		}
		p := collectView(t, l, h, reply.Body.Bytes())
		if p.result.Snapshot != d.result.Snapshot || p.result.Summary != nil {
			t.Fatal("snapshot drift")
		}
		seen += len(p.frames)
		cursor = p.result.Next
	}
	if seen != 7 {
		t.Fatal("page lost or duplicated events", seen)
	}
	previous := collectView(t, l, h, get(path+"&after="+url.QueryEscape(d.result.Cursor), nil).Body.Bytes())
	if previous.result.Snapshot != d.result.Snapshot || previous.result.Summary.Count != 7 {
		t.Fatal("previous page lost snapshot")
	}
	fresh := collectView(t, l, h, get(path, nil).Body.Bytes())
	if fresh.result.Summary.Count != 8 {
		t.Fatal("refresh missed new event")
	}
	bad := get(path+"&after="+url.QueryEscape(d.result.Next+"x"), nil)
	if bad.Code != 409 {
		t.Fatal("tampered cursor", bad.Code)
	}
	changed := get(strings.Replace(path, "14:00:00", "15:00:00", 1)+"&after="+url.QueryEscape(d.result.Next), nil)
	if changed.Code != 409 {
		t.Fatal("changed query", changed.Code)
	}
	unauthorized := get(path, map[string]string{"Authorization": "Bearer wrong"})
	if unauthorized.Code != 401 {
		t.Fatal("authorization", unauthorized.Code)
	}
	if dir := os.Getenv("PIL_VIEW_FIXTURE_DIR"); dir != "" {
		if e = os.MkdirAll(dir, 0755); e != nil {
			t.Fatal(e)
		}
		exportViewNative(t, dir, h, time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC), time.Date(2026, 9, 1, 14, 0, 0, 0, time.UTC))
	}
	// Summary-only requests don't read .packs values (damage a frame after commit).
	b := s.branches[Write]
	if _, e = b.packs.WriteAt([]byte{255, 255, 255, 255}, headerSize); e != nil {
		t.Fatal(e)
	}
	summary := get(path+"&include=summary", nil)
	if summary.Code != 200 {
		t.Fatal(summary.Body.String())
	}
	only := collectView(t, l, h, summary.Body.Bytes())
	if len(only.frames) != 0 || only.result.Summary.Count != 8 {
		t.Fatal("summary hydrated payload")
	}
	if dir := os.Getenv("PIL_VIEW_FIXTURE_DIR"); dir != "" {
		if e = os.MkdirAll(dir, 0755); e != nil {
			t.Fatal(e)
		}
		for name, raw := range map[string][]byte{"cold.pil.gz": compressed.Body.Bytes(), "warm.pil.gz": warmCompressed.Body.Bytes(), "next.pil.gz": nextCompressed, "next.pil": nextFixture, "cold.pil": first.Body.Bytes(), "warm.pil": warm.Body.Bytes(), "definition.rx": l.Bytes} {
			if e = os.WriteFile(filepath.Join(dir, name), raw, 0644); e != nil {
				t.Fatal(e)
			}
		}
	}
}

func TestPagedViewBranchesBoundaryAndGeneration(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	s, e := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return at.Add(time.Hour) }})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	// Repeated UUIDs across branches require a source/row tie-breaker.
	for _, b := range []Branch{Write, Error} {
		for n := 0; n < 3; n++ {
			if _, e = s.Append(b, testFrame(t, l, at.Add(time.Duration(n)*time.Millisecond), uint32(n), false)); e != nil {
				t.Fatal(e)
			}
		}
	}
	h := &HTTP{Store: s}
	path := "/events?from=2026-09-01T12:00:00.0005Z&to=2026-09-01T12:00:00.0025Z&page=1"
	get := func(p string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", p, nil)
		r.Header.Set("Accept", "application/pil")
		r.Header.Set("PIL-Version", "2")
		r.Header.Set("PIL-Features", "pac-arrays")
		w := httptest.NewRecorder()
		h.serveHTTP(w, r)
		return w
	}
	first := get(path)
	if first.Code != 200 {
		t.Fatal(first.Code, first.Body.String())
	}
	d := collectView(t, l, h, first.Body.Bytes())
	if d.result.Summary.Count != 4 || d.result.Summary.Errors[0] != 2 {
		t.Fatalf("fractional time bounds %#v", d.result.Summary)
	}
	seen := map[string]bool{}
	var previous []byte
	next := ""
	for {
		p := path
		if next != "" {
			p += "&after=" + url.QueryEscape(next)
		}
		w := get(p)
		if w.Code != 200 {
			t.Fatal(w.Body.String())
		}
		d := collectView(t, l, h, w.Body.Bytes())
		if len(d.frames) != 1 {
			t.Fatal("page size")
		}
		var f Frame
		if e = l.Scan(d.frames[0], &f); e != nil {
			t.Fatal(e)
		}
		id := f.Values[l.Event].Bytes
		if bytes.Compare(previous, id) > 0 {
			t.Fatal("global ordering")
		}
		previous = append(previous[:0], id...)
		key := d.result.Page[0].Key
		if seen[key] {
			t.Fatal("repeated reference")
		}
		seen[key] = true
		next = d.result.Next
		if next == "" {
			break
		}
	}
	if len(seen) != 4 {
		t.Fatal("lost equal-timestamp events")
	}
	// A physical rewrite cannot silently reinterpret a cursor as a new generation.
	s.branches[Write].generation[0] ^= 1
	if w := get(path + "&after=" + url.QueryEscape(d.result.Next)); w.Code != 409 {
		t.Fatal("generation change", w.Code)
	}
}
