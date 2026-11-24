// Package resourceapi shares public route recognition between the server and desktop proxy.
package resourceapi

import (
	"path"
	"strings"
)

func IsPath(path string) bool {
	p := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(p) != 2 && len(p) != 4 && len(p) != 5 {
		return false
	}
	for _, part := range p {
		if len(part) == 0 || len(part) > 128 {
			return false
		}
		for i, c := range part {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || i > 0 && (c == '-' || c == '_' || c == '.')) {
				return false
			}
		}
	}
	switch len(p) {
	case 2:
		return p[1] == "registries" || p[1] == "definitions"
	case 4:
		return p[3] == "leaves" || p[3] == "collections"
	case 5:
		return p[4] == "summaries" || p[4] == "timelines" || p[4] == "indexes" || p[4] == "seeds" || p[4] == "packs" || p[4] == "strings"
	}
	return false
}

// IsRequest reports whether a browser request belongs to Trunk's HTTP API.
func IsRequest(name string) bool {
	if IsPath(name) || name == "/diagnostics/searches" {
		return true
	}
	first, _, _ := strings.Cut(strings.TrimPrefix(name, "/"), "/")
	switch first {
	case "trunk", "collections", "caches", "schemas", "events", "collect", "summaries", "objects", "configurations", "storage", "rejections", "definitions", "ingest", "taps":
		return true
	case "assets":
		return false
	}
	switch path.Ext(name) {
	case ".rx", ".tri", ".trie", ".time", ".sum", ".summ", ".idx", ".indx", ".tvy", ".packs":
		return true
	}
	return false
}
