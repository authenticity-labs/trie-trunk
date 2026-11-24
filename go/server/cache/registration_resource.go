package cache

import (
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
)

// Registration resources remain independent requests. They do not depend on an
// open event Store: a registered Definition can exist before any events do.
func (p *Endpoint) registrationResource(w http.ResponseWriter, r *http.Request) bool {
	taxonomy := r.URL.Path == "/"+p.config.Cache+"/registries" && r.URL.Query().Get("kind") == "taxonomy"
	definition := r.URL.Path == "/"+p.config.Cache+"/definitions" && r.URL.Query().Has("file")
	if !taxonomy && !definition {
		return false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "GET or HEAD required", http.StatusMethodNotAllowed)
		return true
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || len(q) != 1 || taxonomy && len(q["kind"]) != 1 || definition && len(q["file"]) != 1 {
		http.Error(w, "invalid registration resource parameters", http.StatusBadRequest)
		return true
	}
	raw, err := taxonomyBytes(p.config.Root)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return true
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Type", "application/octet-stream")
	if taxonomy {
		if metadataUnchanged(w, r, raw) {
			return true
		}
	} else {
		resolved, err := seedNativeDefinitionFile(raw, q.Get("file"))
		if err != nil {
			http.NotFound(w, r)
			return true
		}
		defer resolved.close()
		var artifact struct {
			ETag string `json:"etag"`
			Size uint64 `json:"size"`
		}
		if err = json.Unmarshal(resolved.bytes, &artifact); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return true
		}
		tag := strconv.Quote(artifact.ETag)
		w.Header().Set("Cache-Control", "private, no-cache")
		w.Header().Add("Vary", "Authorization, Accept")
		w.Header().Set("ETag", tag)
		w.Header().Set("Content-Type", "application/pil")
		if match := r.Header.Get("If-Match"); match != "" && match != "*" && match != tag {
			w.WriteHeader(http.StatusPreconditionFailed)
			return true
		}
		if match := r.Header.Get("If-None-Match"); match == tag || match == "*" {
			w.WriteHeader(http.StatusNotModified)
			return true
		}
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.FormatUint(artifact.Size, 10))
			return true
		}
		// Rust authorizes the filename against the snapshot. The same Rust check
		// catches replacement between trie publication and this independent read.
		raw, err = os.ReadFile(filepath.Join(p.config.Root, "definitions", q.Get("file")))
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return true
		}
		if err = seedNativeDefinitionFileMatches(resolved.bytes, raw); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return true
		}
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	if r.Method != http.MethodHead {
		_, _ = w.Write(raw)
	}
	return true
}
