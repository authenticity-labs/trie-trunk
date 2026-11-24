package cache

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"trie-trunk/go/resourceapi"
)

// ResourcePath recognizes the public cache-scoped data API. Storage filenames
// remain catalog identities, not public routes.
func ResourcePath(path string) bool { return resourceapi.IsPath(path) }

// translateResource selects an existing handler without changing its batching,
// byte framing, or storage semantics. The translated paths are internal only.
func translateResource(r *http.Request, cache string, sources []*HTTP) (*http.Request, error) {
	if !ResourcePath(r.URL.Path) || r.URL.EscapedPath() != r.URL.Path {
		return nil, fmt.Errorf("invalid resource endpoint")
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if parts[0] != cache {
		return nil, fmt.Errorf("unknown cache")
	}
	resource := parts[len(parts)-1]
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("invalid parameters")
	}
	for key, values := range q {
		if len(values) != 1 && !((resource == "summaries" || resource == "packs" || resource == "indexes" || resource == "timelines") && key == "range") && !(resource == "summaries" && key == "checkpoint") {
			return nil, fmt.Errorf("duplicate parameter: %s", key)
		}
		if key == "tap" || key == "from" || key == "to" || key == "begin" || key == "end" || key == "cache" || key == "collection" {
			return nil, fmt.Errorf("use branch, beginTime, and endTime")
		}
	}
	if q.Has("branch") {
		return nil, fmt.Errorf("branch belongs in the resource path")
	}
	branch := ""
	if len(parts) == 5 {
		branch = strings.Join(parts[1:4], "/")
	}
	if len(parts) == 4 {
		branch = strings.Join(parts[1:3], "/") + "/write"
	}

	tap := cache + "/" + branch
	if branch == "" {
		tap = cache + "/*/*/*"
	}
	for _, pair := range [][2]string{{"beginTime", "from"}, {"endTime", "to"}} {
		if !q.Has(pair[0]) {
			continue
		}
		n, e := strconv.ParseInt(q.Get(pair[0]), 10, 64)
		if e != nil || n < 0 || n > 1<<48-1 {
			return nil, fmt.Errorf("%s must be epoch milliseconds", pair[0])
		}
		q.Del(pair[0])
		q.Set(pair[1], time.UnixMilli(n).UTC().Format(time.RFC3339Nano))
	}
	copy := r.Clone(r.Context())
	u := *r.URL
	copy.URL = &u
	u.RawPath = ""
	switch resource {
	case "seeds":
		return nil, fmt.Errorf("PIL ingestion requires a registered Definition")
	case "packs":
		if r.Method != http.MethodGet {
			return nil, fmt.Errorf("packs require GET")
		}
		if branch == "" || strings.Contains(branch, "*") {
			return nil, fmt.Errorf("packs require one exact branch")
		}
		u.Path = "/seed-packs"
		q.Set("tap", tap)
		if !q.Has("to") {
			q.Set("to", "now")
		}
	case "indexes":
		if q.Get("mode") == "search" {
			u.Path = "/events"
			q.Del("mode")
		} else {
			if q.Has("mode") {
				return nil, fmt.Errorf("invalid index mode")
			}
			u.Path = "/collect"
			q.Set("data", "none")
		}
		q.Set("tap", tap)
		if !q.Has("to") {
			q.Set("to", "now")
		}
	case "definitions":
		u.Path = "/definitions"
	case "registries", "leaves":
		return nil, fmt.Errorf("raw resource must be handled directly")
	case "collections":
		if branch == "" || strings.Contains(branch, "*") || !strings.HasSuffix(branch, "/write") {
			return nil, fmt.Errorf("collections require an exact write branch")
		}
		q.Set("collection", strings.TrimSuffix(branch, "/write"))
		q.Set("cache", cache)
		if q.Get("kind") == "state" {
			u.Path = "/objects/state"
		} else if q.Get("kind") == "" {
			u.Path = "/objects/history"
			if !q.Has("from") {
				q.Set("from", time.UnixMilli(0).UTC().Format(time.RFC3339Nano))
			}
			if !q.Has("to") {
				now := time.Now()
				if len(sources) > 0 && sources[0].Store.config.Now != nil {
					now = sources[0].Store.config.Now()
				}
				q.Set("to", now.UTC().Format(time.RFC3339Nano))
			}
		} else {
			return nil, fmt.Errorf("invalid collection kind")
		}
		q.Del("kind")
	case "summaries", "timelines":
		if resource == "summaries" {
			if r.Method != http.MethodGet {
				return nil, fmt.Errorf("summaries require GET")
			}
			if branch == "" || strings.Contains(branch, "*") {
				return nil, fmt.Errorf("summaries require one exact branch")
			}
			if q.Has("range") {
				if _, e := summaryRanges(q); e != nil {
					return nil, e
				}
			}
		}

		var selected *HTTP
		var b Branch
		for _, api := range sources {
			if q.Get("definition") != "" && q.Get("definition") != fmtDigestNative(api.Store.layout.Digest) {
				continue
			}
			branches, e := api.branch(tap)
			if e != nil || len(branches) != 1 {
				continue
			}
			if selected != nil {
				return nil, fmt.Errorf("select a definition for this branch")
			}
			selected, b = api, branches[0]
		}
		if selected == nil {
			return nil, fmt.Errorf("unknown branch")
		}
		ext := "time"
		if resource == "summaries" {
			ext = "summ"
		}
		u.Path = "/" + cache + "/" + selected.Store.binding + "/" + b.Name() + "." + ext
		q.Del("definition")
		if ext == "summ" {
			if !q.Has("range") && !q.Has("live") {
				from, e := time.Parse(time.RFC3339Nano, q.Get("from"))
				if e != nil {
					return nil, fmt.Errorf("invalid time bounds")
				}
				to, e := time.Parse(time.RFC3339Nano, q.Get("to"))
				if !q.Has("to") {
					to = time.UnixMilli(selected.Store.summaryDay(b, selected.Store.config.Now().UnixMilli()) + summaryDayMS)
					e = nil
				}
				if e != nil || !from.Before(to) || selected.Store.summaryDay(b, from.UnixMilli()) != from.UnixMilli() || selected.Store.summaryDay(b, to.UnixMilli()) != to.UnixMilli() {
					return nil, fmt.Errorf("summary range must align to the configured UTC offset")
				}
				q.Del("from")
				q.Del("to")
				q.Set("range", fmt.Sprintf("%d,%d", from.UnixMilli(), to.UnixMilli()))
			}
			break
		}
		for _, pair := range [][2]string{{"from", "begin"}, {"to", "end"}} {
			t, e := time.Parse(time.RFC3339Nano, q.Get(pair[0]))
			if pair[0] == "to" && !q.Has("to") {
				t, e = selected.Store.config.Now(), nil
				t = t.Truncate(time.Hour).Add(time.Hour)
			}
			if e != nil {
				return nil, fmt.Errorf("invalid time bounds")
			}
			unit := time.Hour

			if !t.Equal(t.Truncate(unit)) {
				return nil, fmt.Errorf("range must align to %s", unit)
			}
			v := fmt.Sprintf("%02d%03d", t.Year()-2000, t.YearDay())
			v += fmt.Sprintf("%02d", t.Hour())
			q.Del(pair[0])
			q.Set(pair[1], v)
		}
	}
	u.RawQuery = q.Encode()
	return copy, nil
}

func controlPath(path string) bool {
	return path == "/definitions" || path == "/diagnostics/searches" || path == "/ingest" || path == "/rejections" || path == "/caches" || path == "/configurations" || path == "/storage" || path == "/storage/quotas"
}

func (p *Endpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		http.Error(w, "endpoint closed", http.StatusServiceUnavailable)
		return
	}
	p.commitWork.Add(1)
	p.mu.Unlock()
	defer p.commitWork.Done()
	if p.registeredCatalog(w, r) {
		return
	}

	if ResourcePath(r.URL.Path) {
		snapshot := p.routing.Load()
		if snapshot == nil {
			http.Error(w, "endpoint closed", 503)
			return
		}
		sources := snapshot.sources
		if p.registrationResource(w, r) {
			return
		}
		if r.URL.Path == "/"+p.config.Cache+"/definitions" {
			p.registration(w, r)
			return
		}
		if p.registeredLeavesResource(w, r) {
			return
		}
		if p.registeredReadResource(w, r) {
			return
		}
		var selectErr error
		r, sources, selectErr = p.selectRegisteredBranch(r, sources)
		if selectErr != nil {
			http.Error(w, selectErr.Error(), http.StatusBadRequest)
			return
		}
		if branch := r.Header.Get("Trunk-Branch"); branch != "" {
			w.Header().Set("Trunk-Branch", branch)
		}
		if (&HTTP{sources: sources}).replicationResource(w, r) {
			return
		}
		translated, err := translateResource(r, p.config.Cache, sources)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if translated.URL.Path == "/seed-packs" {
			(&HTTP{sources: sources}).serveHTTP(w, translated)
			return
		}
		if file, ok := branchFilePath(translated.URL.Path); ok {
			(&HTTP{sources: sources}).branchFileHTTP(w, translated, file)
			return
		}
		if r.Header.Get("Trunk-Branch") != "" {
			(&HTTP{Store: sources[0].Store, sources: sources}).serveHTTP(w, translated)
			return
		}
		p.serveHTTP(w, translated)
		return
	}
	if !controlPath(r.URL.Path) {
		http.NotFound(w, r)
		return
	}
	p.serveHTTP(w, r)
}

func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	if h.Token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+h.Token)) != 1 {
		http.Error(w, "unauthorized", 401)
		return
	}
	if ResourcePath(r.URL.Path) {
		sources := h.sources
		if len(sources) == 0 && h.Store != nil {
			sources = []*HTTP{h}
		}
		if len(sources) == 0 {
			http.NotFound(w, r)
			return
		}
		if _, selected := r.Header[http.CanonicalHeaderKey("Trunk-Branch")]; selected {
			var err error
			endpoint := Endpoint{config: sources[0].Store.config}
			r, sources, err = endpoint.selectRegisteredBranch(r, sources)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.Header().Set("Trunk-Branch", r.Header.Get("Trunk-Branch"))
		}
		if (&HTTP{sources: sources}).replicationResource(w, r) {
			return
		}
		translated, err := translateResource(r, sources[0].Store.config.Cache, sources)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if r.Header.Get("Trunk-Branch") != "" {
			(&HTTP{Store: sources[0].Store, sources: sources, Token: h.Token}).serveHTTP(w, translated)
			return
		}
		h.serveHTTP(w, translated)
		return
	}
	if !controlPath(r.URL.Path) {
		http.NotFound(w, r)
		return
	}
	h.serveHTTP(w, r)
}
