package cache

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// The catalog lists cache/service/resource.vMajor/branch source bindings.
// Keep the old route and response key for clients opened before the rename.
func catalogRequest(r *http.Request) bool {
	return r.URL.Path == "/caches" || r.URL.Path == "/taps"
}

func catalogGET(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.EscapedPath() != r.URL.Path || r.URL.RawQuery != "" {
		http.NotFound(w, r)
		return false
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	accept := strings.ToLower(strings.TrimSpace(r.Header.Get("Accept")))
	if accept != "" && accept != "*/*" && accept != "application/json" && accept != "application/pil" {
		http.Error(w, "unsupported response format", http.StatusNotAcceptable)
		return false
	}
	return true
}

// Replay must be enabled for every retained definition of a resource major.
func collectionSources(apis []*HTTP) []string {
	enabled := map[string]bool{}
	for _, api := range apis {
		s := api.Store
		s.mu.Lock()
		key := api.tap(Write)
		ok := s.config.ObjectIndex && s.objects != nil && s.layout.Object >= 0
		s.mu.Unlock()
		prior, exists := enabled[key]
		enabled[key] = ok && (!exists || prior)
	}
	result := []string{}
	for key, ok := range enabled {
		if ok {
			result = append(result, key)
		}
	}
	sort.Strings(result)
	return result
}

func writeCatalog(w http.ResponseWriter, r *http.Request, sources, collections []string) {
	key := "caches"
	if r.URL.Path == "/taps" {
		key = "taps"
	}
	if sources == nil {
		sources = []string{}
	}
	if r.Header.Get("Accept") == "application/pil" {
		raw, err := encoder.Marshal(map[string][]string{key: sources, "collections": collections})
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/pil")
		if metadataUnchanged(w, r, raw) {
			return
		}
		_, _ = w.Write(raw)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string][]string{key: sources, "collections": collections})
}

// Metadata is private to credentials and must be revalidated before reuse.
func metadataUnchanged(w http.ResponseWriter, r *http.Request, raw []byte) bool {
	w.Header().Set("Cache-Control", "private, no-cache")
	w.Header().Add("Vary", "Authorization, Accept")
	sum := sha256.Sum256(raw)
	tag := fmt.Sprintf("\"%x\"", sum)
	w.Header().Set("ETag", tag)
	if r.Header.Get("If-None-Match") == tag {
		w.WriteHeader(http.StatusNotModified)
		return true
	}
	return false
}
