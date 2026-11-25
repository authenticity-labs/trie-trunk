package cache

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"trie-trunk/go/internal/node"
)

// History and invalidation checks share the registered Rust lookup.
func (p *Endpoint) registeredCollection(w http.ResponseWriter, r *http.Request) bool {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if p.node == nil || len(parts) != 4 || parts[3] != "collections" || parts[0] != p.config.Cache {
		return false
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "GET required", 405)
		return true
	}
	q := r.URL.Query()
	state := q.Get("kind") == "state"
	for name, values := range q {
		if len(values) != 1 || !(name == "type" || name == "id" || (state && (name == "kind" || name == "token")) || (!state && (name == "beginTime" || name == "endTime"))) {
			http.Error(w, "Invalid collection parameters", 400)
			return true
		}
	}
	typ := q.Get("type")
	if typ == "" || strings.Contains(typ, "/") {
		http.Error(w, "Select an object type", 400)
		return true
	}
	var from, to uint64 = 0, 1 << 48
	if !state {
		var err error
		from, err = strconv.ParseUint(q.Get("beginTime"), 10, 48)
		if err == nil {
			to, err = strconv.ParseUint(q.Get("endTime"), 10, 48)
		}
		if err != nil || from >= to {
			http.Error(w, "Invalid history window", 400)
			return true
		}
	}
	reply, err := p.node.History(strings.Join(parts[:3], "/")+"/"+typ, q.Get("id"), from, to, state)
	if err != nil {
		status := 503
		if e, ok := err.(*node.Error); ok {
			status = e.Status
		}
		http.Error(w, err.Error(), status)
		return true
	}
	defer reply.Close()
	w.Header().Set("Content-Type", "application/json")
	if state {
		var value struct {
			Token string `json:"token"`
		}
		if err := json.Unmarshal(reply.Bytes(0, 0), &value); err != nil {
			http.Error(w, "Invalid state projection", 500)
			return true
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"token": value.Token, "current": q.Get("token") == value.Token})
	} else {
		_, _ = w.Write(reply.Bytes(0, 0))
	}
	return true
}
