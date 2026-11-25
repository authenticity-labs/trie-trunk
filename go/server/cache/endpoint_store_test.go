package cache

import (
	"fmt"
	"mime"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
)

// Exercises the historical Store and object prototypes in their test suites.
// Production clients enter through Endpoint.ServeHTTP.
func (p *Endpoint) serveHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.URL.Path == "/ingest" && p.node != nil {
		http.NotFound(w, r)
		return
	}
	if r.URL.Path == "/definitions" {
		p.registration(w, r)
		return
	}
	if r.URL.Path == "/objects/history" || r.URL.Path == "/objects/state" || strings.HasPrefix(r.URL.Path, "/collections/") {
		p.replayHTTP(w, r)
		return
	}

	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		http.Error(w, "endpoint closed", 503)
		return
	}
	if r.URL.Path == "/ingest" {
		kind, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		// Standard CBOR is independent of PIL logger registration.
		if r.Method == "POST" && r.Header.Get("Definition-Version") == "" && blindContentType(kind) && r.Header.Get("Trunk-Version") == "" && r.Header.Get("Trunk-Digest") == "" && r.Header.Get("Upgrade") == "" {
			l, e := GenericDefinition()
			if e == nil {
				e = p.install(l)
			}
			if e == nil {
				e = p.persistPreferences()
			}
			if e != nil {
				p.mu.Unlock()
				http.Error(w, e.Error(), 409)
				return
			}
		}
	}
	api := p.api
	if r.URL.Path == "/ingest" && r.Header.Get("Definition-Version") == "" && blindContentType(strings.Split(r.Header.Get("Content-Type"), ";")[0]) {
		l, _ := GenericDefinition()
		api = p.apis[fmt.Sprintf("%x", l.Digest)]
	}
	sources := p.sources()
	if r.URL.Path == "/ingest" && (r.Header.Get("Definition-Version") != "" || strings.EqualFold(strings.Split(r.Header.Get("Content-Type"), ";")[0], "application/pil")) {
		api = p.apis[r.Header.Get("Definition-Version")]
		if api == nil {
			for _, candidate := range sources {
				if candidate.Store.providers[r.Header.Get("Definition-Version")] == nil {
					continue
				}
				branches, err := candidate.branch(r.URL.Query().Get("tap"))
				if err != nil || len(branches) != 1 {
					continue
				}
				if api != nil {
					p.mu.Unlock()
					http.Error(w, "ambiguous provider destination", 412)
					return
				}
				api = candidate
			}
		}
	}
	p.mu.Unlock()
	if trunkRequest(r) {
		(&HTTP{sources: sources}).trunkHTTP(w, r)
		return
	}
	if (&HTTP{sources: sources}).trunkFileHTTP(w, r) {
		return
	}
	if r.URL.Path == "/configurations" {
		p.configurationHTTP(w, r, sources)
		return
	}
	if r.URL.Path == "/storage/quotas" {
		p.quotaHTTP(w, r)
		return
	}
	if r.URL.Path == "/storage" {
		(&HTTP{sources: sources, storageRoots: map[string]string{p.config.Cache: filepath.Join(p.config.Root, p.config.Cache)}}).storageHTTP(w, r, p.node)
		return
	}
	if catalogRequest(r) {
		if !catalogGET(w, r) {
			return
		}
		taps := []string{}
		seen := map[string]bool{}
		for _, source := range sources {
			for b := Read; b <= Trace; b++ {
				tap := source.tap(b)
				if !seen[tap] {
					seen[tap] = true
					taps = append(taps, tap)
				}
			}
		}
		sort.Strings(taps)
		w.Header().Set("Content-Type", "application/json")
		writeCatalog(w, r, taps, collectionSources(sources))
		return
	}
	if api == nil && r.Method == http.MethodGet && r.URL.Path == "/rejections" && r.URL.Query().Get("format") == "availability" {
		(&HTTP{}).serveHTTP(w, r)
		return
	}
	if file, ok := branchFilePath(r.URL.Path); ok {
		(&HTTP{sources: sources}).branchFileHTTP(w, r, file)
		return
	}
	if api != nil && (r.URL.Path == "/events" || r.URL.Path == "/collect" || r.URL.Path == "/seed-packs" || r.URL.Path == "/schemas" || r.URL.Path == "/rejections") {
		handler := &HTTP{Store: api.Store, sources: sources}
		handler.serveHTTP(w, r)
		return
	}
	if api == nil {
		if r.URL.Path == "/ingest" && r.Method != "POST" {
			w.Header().Set("Allow", "POST")
			w.WriteHeader(405)
			return
		}
		http.Error(w, "register a PIL definition before writing; blind CBOR registers automatically", 412)
		return
	}
	api.serveHTTP(w, r)
}

func blindContentType(kind string) bool {
	switch strings.ToLower(kind) {
	case "application/cbor", "application/json", "application/jsonl", "application/x-ndjson", "text/plain", "application/syslog":
		return true
	}
	return false
}
