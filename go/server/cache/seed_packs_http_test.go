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

func TestSeedSparsePACStream(t *testing.T) {
	l := testLayout(t, false)
	start := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	now := start.Add(4*time.Hour + 30*time.Minute)
	s, e := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	for _, branch := range []Branch{Read, Write} {
		for i := 0; i < 5; i++ {
			if _, e = s.Append(branch, testFrame(t, l, start.Add(time.Duration(i)*time.Hour+time.Minute), uint32(i+1), false)); e != nil {
				t.Fatal(e)
			}
		}
	}
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	h := &HTTP{Store: s}
	get := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Accept", "application/pil")
		r.Header.Set("PIL-Version", "2")
		r.Header.Set("PIL-Features", "native-ranges,native-state-pages,adaptive-integers")
		h.ServeHTTP(w, r)
		return w
	}
	for _, branch := range []Branch{Read, Write} {
		q := url.Values{"branch": {"payment/transactions.v1/" + branch.Name()}, "definition": {fmtDigestNative(l.Digest)}, "beginTime": {fmt.Sprint(start.Add(4 * time.Hour).UnixMilli())}}
		for _, i := range []int{2, 0, 2} {
			a := start.Add(time.Duration(i) * time.Hour).UnixMilli()
			q.Add("range", fmt.Sprintf("%d,%d", a, a+3600000))
		}
		w := get("/demo/" + q.Get("branch") + "/packs?" + withoutBranch(q))
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		p, e := pil.OpenNestedPack(w.Body.Bytes())
		if e != nil || p.Count() != 3 {
			t.Fatal(p, e)
		}
		b, _ := s.openBranch(branch)
		for i, hour := range []int{0, 2} {
			raw, _ := p.Get(i)
			run := b.runs[hour]
			disk, e := readBytes(b.packs, run.PacksStart-b.packsRetired, run.PacksEnd-b.packsRetired)
			if e != nil || !bytes.Equal(raw, disk) {
				t.Fatal("stored pack was rewritten", e)
			}
		}
		live, _ := p.Get(2)
		pack, e := pil.ParsePAC(l.Layout, live, map[uint32]bool{1: true})
		if e != nil || len(pack.Headers["seed.rows"]) != 6 || order.Uint64(pack.Headers["seed.hour"]) != uint64(start.Add(4*time.Hour).UnixMilli()) {
			t.Fatal("invalid live child", e)
		}
		if dir := os.Getenv("TRUNK_SEED_FIXTURE"); dir != "" && branch == Read {
			if e = os.MkdirAll(dir, 0755); e != nil {
				t.Fatal(e)
			}
			iq := url.Values{"branch": q["branch"], "definition": q["definition"], "beginTime": {fmt.Sprint(start.UnixMilli())}, "endTime": {fmt.Sprint(start.Add(4 * time.Hour).UnixMilli())}}
			iw := httptest.NewRecorder()
			ir := httptest.NewRequest("GET", "/collect?tap=demo/payment/transactions.v1/read&data=none&from="+start.Format(time.RFC3339)+"&to="+start.Add(4*time.Hour).Format(time.RFC3339), nil)
			ir.Header.Set("Accept", "application/pil")
			ir.Header.Set("PIL-Version", "2")
			ir.Header.Set("PIL-Features", "native-ranges,native-state-pages,adaptive-integers")
			h.serveHTTP(iw, ir)
			if iw.Code != 200 {
				t.Fatal(iw.Code, iw.Body.String())
			}
			aq := url.Values{"branch": q["branch"], "definition": q["definition"], "beginTime": {fmt.Sprint(start.UnixMilli())}}
			aw := get("/demo/" + aq.Get("branch") + "/packs?" + withoutBranch(aq))
			if aw.Code != 200 {
				t.Fatal(aw.Code, aw.Body.String())
			}
			tw := get("/demo/" + iq.Get("branch") + "/timelines?" + withoutBranch(iq))
			if tw.Code != 200 {
				t.Fatal(tw.Code, tw.Body.String())
			}
			rawq := url.Values{"definition": {fmtDigestNative(l.Digest)}, "generation": {fmt.Sprintf("%x", b.generation)}}
			for _, run := range b.runs {
				rawq.Add("rows", fmt.Sprintf("%d,%d", run.Start-1, run.End))
			}
			rw := get("/demo/payment/transactions.v1/read/indexes?" + rawq.Encode())
			if rw.Code != 200 {
				t.Fatal(rw.Code, rw.Body.String())
			}
			sw := get("/demo/payment/transactions.v1/states?definition=" + fmtDigestNative(l.Digest))
			if sw.Code != 200 {
				t.Fatal(sw.Code, sw.Body.String())
			}
			registry := get("/demo/registries?kind=inventory")
			taxonomy := get("/demo/registries?kind=taxonomy")
			directory := get("/caches")
			rmeta, _ := json.Marshal(rw.Header())
			tmeta, _ := json.Marshal(tw.Header())
			patch, _ := b.time.window(start.UnixMilli(), start.Add(4*time.Hour).UnixMilli(), true)
			meta, _ := json.Marshal(w.Header())
			imeta, _ := json.Marshal(iw.Header())
			for name, data := range map[string][]byte{"indexes.bin": rw.Body.Bytes(), "leaf.trie": sw.Body.Bytes(), "inventory.pil": registry.Body.Bytes(), "trunk.trie": taxonomy.Body.Bytes(), "caches.pil": directory.Body.Bytes(), "raw-index-headers.json": rmeta, "timeline-headers.json": tmeta, "response.pac": w.Body.Bytes(), "all.pac": aw.Body.Bytes(), "timeline.time": patch, "timeline.bin": tw.Body.Bytes(), "headers.json": meta, "indexes.pil": iw.Body.Bytes(), "index-headers.json": imeta, "definition.rx": l.Bytes} {
				if e = os.WriteFile(filepath.Join(dir, name), data, 0644); e != nil {
					t.Fatal(e)
				}
			}
		}
		q.Set("branch", "payment/transactions.v1/*")
		if get("/demo/"+q.Get("branch")+"/packs?"+withoutBranch(q)).Code != 404 {
			t.Fatal("wildcard accepted")
		}
	}
	// The empty response is still a valid zero-member PAC with a live receipt.
	q := url.Values{"branch": {"payment/transactions.v1/read"}, "beginTime": {fmt.Sprint(now.Add(-time.Minute).UnixMilli())}}
	w := get("/demo/" + q.Get("branch") + "/packs?" + withoutBranch(q))
	p, e := pil.OpenNestedPack(w.Body.Bytes())
	if w.Code != 200 || e != nil || p.Count() != 0 {
		t.Fatal(w.Code, e)
	}
}

func withoutBranch(q url.Values) string {
	copy := url.Values{}
	for k, v := range q {
		if k != "branch" {
			copy[k] = v
		}
	}
	return copy.Encode()
}

// Regression: a resource GET must not run the sealing writer.
func TestSeedGETDoesNotSeal(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	now := at.Add(time.Minute)
	calls := 0
	s, e := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }, sealHook: func(phase string) error {
		if phase == "prepare" {
			calls++
			return fmt.Errorf("diagnostic: GET entered sealing")
		}
		return nil
	}})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if _, e = s.Append(Read, testFrame(t, l, at, 1, false)); e != nil {
		t.Fatal(e)
	}
	now = at.Add(80 * time.Minute)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/demo/payment/transactions.v1/read/packs?beginTime="+fmt.Sprint(at.UnixMilli()), nil)
	r.Header.Set("Accept", "application/pil")
	(&HTTP{Store: s}).ServeHTTP(w, r)
	if calls != 0 {
		t.Fatalf("resource GET invoked sealing %d time(s), HTTP %d: %s", calls, w.Code, w.Body.String())
	}
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}

// Regression: a resident sealed hour must not need to reopen its payload file.
func TestSeedGETReadsResidentHour(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	now := at.Add(time.Minute)
	s, e := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if _, e = s.Append(Read, testFrame(t, l, at, 1, false)); e != nil {
		t.Fatal(e)
	}
	now = at.Add(80 * time.Minute)
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	b := s.branches[Read]
	if b.live.count(1) != 1 {
		t.Fatal("sealed record not resident")
	}
	path := b.packs.Name()
	if e = os.Rename(path, path+".probe"); e != nil {
		t.Fatal(e)
	}
	defer os.Rename(path+".probe", path)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/demo/payment/transactions.v1/read/packs?beginTime="+fmt.Sprint(at.UnixMilli()), nil)
	r.Header.Set("Accept", "application/pil")
	(&HTTP{Store: s}).ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("resident sealed hour required payload reopen, HTTP %d: %s", w.Code, w.Body.String())
	}
}

func TestTaxonomyETag(t *testing.T) {
	s, err := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: testLayout(t, false)})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := &HTTP{Store: s}
	get := func(tag string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/demo/registries?kind=taxonomy", nil)
		r.Header.Set("Accept", "application/pil")
		r.Header.Set("If-None-Match", tag)
		h.ServeHTTP(w, r)
		return w
	}
	first := get("")
	if first.Code != 200 || first.Header().Get("ETag") == "" {
		t.Fatal(first.Code, first.Body.String())
	}
	raw, err := os.ReadFile(filepath.Join(s.config.Root, "trunk.trie"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, first.Body.Bytes()) {
		t.Fatal("taxonomy was filtered or transformed")
	}
	unchanged := get(first.Header().Get("ETag"))
	if unchanged.Code != 304 || unchanged.Body.Len() != 0 {
		t.Fatal(unchanged.Code, unchanged.Body.String())
	}
	if stale := get("\"stale\""); stale.Code != 200 || !bytes.Equal(stale.Body.Bytes(), raw) {
		t.Fatal("stale reader did not receive the whole trie")
	}
}

// Empty hours still need the snapshot boundary even though no child Pack can
// carry it. This exercises the existing handler without a synthetic wire body.
func TestSeedEmptyResponseSnapshotHeaders(t *testing.T) {
	l := testLayout(t, false)
	now := time.Date(2026, 11, 10, 10, 15, 0, 0, time.UTC)
	s, err := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := &HTTP{Store: s}
	q := url.Values{"tap": {"demo/payment/transactions.v1/read"}, "from": {now.Add(-time.Hour).Format(time.RFC3339Nano)}, "to": {"now"}}
	response := httptest.NewRecorder()
	h.seedPacks(response, httptest.NewRequest("GET", "/packs?"+q.Encode(), nil))
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	if response.Header().Get("Trunk-Window-To") != fmt.Sprint(now.UnixMilli()) {
		t.Fatal("missing final time slot")
	}
	if response.Header().Get("Trunk-Generation") == "" || response.Header().Get("Trunk-Generation-Sequence") == "" {
		t.Fatal("missing snapshot identity")
	}
	sealed, err := time.Parse(time.RFC3339Nano, response.Header().Get("Trunk-Sealed-Through"))
	if err != nil {
		t.Fatal(err)
	}
	retained, err := time.Parse(time.RFC3339Nano, response.Header().Get("Trunk-Retained-From"))
	if err != nil {
		t.Fatal(err)
	}
	if retained.After(sealed) || sealed.After(now) || sealed.UnixMilli()%TimeHourMS != 0 {
		t.Fatal("invalid snapshot bounds")
	}
}
