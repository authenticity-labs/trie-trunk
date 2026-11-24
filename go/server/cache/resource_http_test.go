package cache

import (
	"bytes"
	"crypto/subtle"
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Existing operation tests continue to exercise storage/codec behavior directly.
// Resource routing is tested separately below, through the public handler.
func (p *Endpoint) operationHandler(token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
		p.serveHTTP(w, r)
	})
}

func TestCacheResourceRoutes(t *testing.T) {
	l := adaptiveLayout(t, testLayout(t, false).Definition, "16")
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	p, err := OpenEndpoint(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, ObjectIndex: true, Now: func() time.Time { return at.Add(2 * time.Hour) }})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	source := p.api
	branch := l.Definition.Resource.Service + "/" + fmt.Sprintf("%s.v%d/write", l.Definition.Resource.Resource, l.Definition.Resource.Major)
	if _, err := source.Store.Append(Write, testFrame(t, l, at, 1, false)); err != nil {
		t.Fatal(err)
	}
	if err := source.Store.Flush(); err != nil {
		t.Fatal(err)
	}
	request := func(path string, public bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer secret")
		r.Header.Set("Accept", "application/pil")
		r.Header.Set("PIL-Version", "2")
		r.Header.Set("PIL-Features", "native-ranges,adaptive-integers,pac-arrays")
		w := httptest.NewRecorder()
		if public {
			p.Handler("secret").ServeHTTP(w, r)
		} else {
			p.serveHTTP(w, r)
		}
		return w
	}
	bounds := fmt.Sprintf("beginTime=%d&endTime=%d", at.UnixMilli(), at.Add(time.Hour).UnixMilli())
	for _, kind := range []string{"packs", "indexes", "timelines", "summaries"} {
		t.Run(kind, func(t *testing.T) {
			rangeQuery := bounds
			if kind == "summaries" {
				rangeQuery = fmt.Sprintf("beginTime=%d&endTime=%d", at.Truncate(24*time.Hour).UnixMilli(), at.Truncate(24*time.Hour).Add(24*time.Hour).UnixMilli())
			}
			if kind == "indexes" {
				b, _ := source.Store.openBranch(Write)
				rangeQuery = fmt.Sprintf("definition=%x&generation=%x&rows=%d,%d", l.Digest, b.generation, b.runs[0].Start-1, b.runs[0].End)
			}
			w := request("/demo/"+branch+"/"+kind+"?"+rangeQuery, true)
			if w.Code != 200 && w.Code != 204 {
				t.Fatal(w.Code, w.Body.String())
			}
			if kind == "packs" {
				pack, e := pil.OpenNestedPack(w.Body.Bytes())
				if e != nil || pack.Count() != 1 || w.Header().Get("Trunk-Format") != "seed-packs/0" {
					t.Fatal("invalid seed PAC route", e)
				}
			}
		})
	}
	if w := request("/demo/"+branch+"/packs"+fmt.Sprintf("?beginTime=%d", at.UnixMilli()), true); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := request("/demo/"+strings.TrimSuffix(branch, "/write")+"/collections?id=00000000-0000-7000-8000-00000000002a", true); w.Code != 200 {
		t.Fatal("unbounded object history", w.Code, w.Body.String())
	}
	if w := request("/demo/registries?kind=inventory", true); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, kind := range []string{"definitions", "leaves", "registries"} {
		path := "/demo/" + kind
		var expected []byte
		switch kind {
		case "definitions":
			path += fmt.Sprintf("?definition=%x", l.Digest)
			expected = l.Bytes
		case "leaves":
			path = "/demo/" + strings.TrimSuffix(branch, "/write") + "/leaves"
			stat, _ := source.Store.shared.Stat()
			expected = make([]byte, stat.Size())
			_, _ = source.Store.shared.ReadAt(expected, 0)
		case "registries":
			c, e := readCatalog(source.Store.config.Root, source.Store.config.Cache)
			if e != nil {
				t.Fatal(e)
			}
			expected, _ = encoder.Marshal(c)
		}
		w := request(path, true)
		if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), expected) {
			t.Fatalf("%s did not return resource bytes: %d", kind, w.Code)
		}
	}
	b, _ := source.Store.openBranch(Write)
	q := fmt.Sprintf("?definition=%x&generation=%x&rows=%d,%d", l.Digest, b.generation, b.runs[0].Start-1, b.runs[0].End)
	w := request("/demo/"+branch+"/indexes"+q, true)
	expected, _ := io.ReadAll(io.NewSectionReader(b.idx, headerSize+int64((b.runs[0].Start-1-b.baseRows)*IndexRowSize), int64((b.runs[0].End-b.runs[0].Start+1)*IndexRowSize)))
	if !bytes.Equal(w.Body.Bytes(), expected) {
		t.Fatal("index transport changed native bytes")
	}
	if w := request("/demo/"+branch+"/indexes"+fmt.Sprintf("?generation=%x&rows=1,2", b.generation), true); w.Code < 400 {
		t.Fatal("partial index segment accepted")
	}

	for _, path := range []string{"/other/packs?branch=" + branch + "&" + bounds, "/demo/packs?branch=../../read&" + bounds, "/demo/packs?branch=" + branch + "&" + bounds + "&beginTime=0", "/demo/packs?from=0", "/demo/timelines?branch=" + branch + "&beginTime=1&endTime=2", "/demo/indexes?mode=unknown&" + bounds} {
		if w := request(path, true); w.Code < 400 {
			t.Fatal("accepted invalid request", path, w.Code)
		}
	}
	for _, path := range []string{"/demo/definitions", "/demo/registries?kind=inventory", "/demo/payment/transactions.v1/leaves", "/demo/payment/transactions.v1/write/timelines?" + bounds} {
		secured := &HTTP{Store: source.Store, Token: "secret"}
		out := httptest.NewRecorder()
		secured.ServeHTTP(out, httptest.NewRequest("GET", path, nil))
		if out.Code != 401 {
			t.Fatal("raw resource bypassed authentication", path, out.Code)
		}
	}
	for _, path := range []string{"/trunk", "/demo/packs", "/demo/indexes", "/collect", "/events", "/schemas", "/summaries", "/demo/payment/transactions.v1/0/write.packs"} {
		if w := request(path, true); w.Code != 404 {
			t.Fatal("exposed removed route", path, w.Code)
		}
	}
}

func TestRegistryInventoryConditionalValidation(t *testing.T) {
	p, err := OpenEndpoint(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: testLayout(t, false)})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	request := func(tag, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/demo/registries?kind=inventory", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Accept", "application/pil")
		r.Header.Set("If-None-Match", tag)
		w := httptest.NewRecorder()
		p.Handler("secret").ServeHTTP(w, r)
		return w
	}
	first := request("", "secret")
	tag := first.Header().Get("ETag")
	if first.Code != 200 || tag == "" || first.Header().Get("Cache-Control") != "private, no-cache" {
		t.Fatal(first.Code, first.Header(), first.Body.String())
	}
	unchanged := request(tag, "secret")
	if unchanged.Code != 304 || unchanged.Body.Len() != 0 {
		t.Fatal(unchanged.Code, unchanged.Body.String())
	}
	if unauthorized := request(tag, "wrong"); unauthorized.Code != 401 {
		t.Fatal("validator bypassed authorization", unauthorized.Code)
	}
	next := p.api.Store.layout.Definition
	next.Version++
	next.Resource.Minor++
	newer, err := Compile(next)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.install(newer); err != nil {
		t.Fatal(err)
	}
	changed := request(tag, "secret")
	if changed.Code != 200 || changed.Header().Get("ETag") == tag || changed.Body.Len() == 0 {
		t.Fatal("new Definition not discovered", changed.Code, changed.Header())
	}
}
