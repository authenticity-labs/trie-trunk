package cache

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
)

// Label revisions bind the full leaf publication and the selected root. The
// host serializes publication; Rust resolves roots, IDs, and footer bytes.
func leafLabelsETag(raw []byte, branch uint16) string {
	return fmt.Sprintf("\"%x-%d\"", sha256.Sum256(raw), branch)
}
func (p *Endpoint) leafLabelsResource(w http.ResponseWriter, r *http.Request) bool {
	q := r.URL.Query()
	for k, v := range q {
		if (k != "labels" && k != "leaf") || len(v) != 1 {
			http.Error(w, "invalid leaf label selector", 400)
			return true
		}
	}
	branch, e := strconv.ParseUint(r.Header.Get("Trunk-Branch"), 10, 16)
	if e != nil || branch == 0 {
		http.Error(w, "select a registered leaf root", 400)
		return true
	}
	source := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/"), "/leaves")
	reader, e := taxonomyReader(p.config.Root)
	if e != nil {
		http.Error(w, e.Error(), 503)
		return true
	}
	path, e := reader.trie.statePath(source, "", uint16(branch))
	if e != nil {
		http.Error(w, e.Error(), 400)
		return true
	}
	if !strings.HasPrefix(path, p.config.Cache+"/") {
		http.NotFound(w, r)
		return true
	}
	if e = prepareRegisteredDefinitions(p.config.Root, p.config.Cache); e != nil {
		http.Error(w, e.Error(), 503)
		return true
	}
	w.Header().Set("Trunk-Branch", strconv.FormatUint(branch, 10))
	w.Header().Set("Cache-Control", "private, no-cache")
	w.Header().Set("Vary", "Trunk-Branch, Authorization, Accept")
	if r.Method == http.MethodGet && q.Get("labels") == "1" && !q.Has("leaf") {
		snapshot, e := p.registeredLeaf(path)
		if e != nil {
			http.Error(w, e.Error(), 404)
			return true
		}
		labels, e := reader.trie.labels(uint16(branch), snapshot.bytes)
		if e != nil {
			http.Error(w, e.Error(), 400)
			return true
		}
		defer labels.close()
		tag := leafLabelsETag(snapshot.bytes, uint16(branch))
		w.Header().Set("ETag", tag)
		if r.Header.Get("If-None-Match") == tag {
			w.WriteHeader(http.StatusNotModified)
			return true
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(labels.bytes)
		return true
	}
	if r.Method != http.MethodPatch || !q.Has("leaf") || q.Has("labels") {
		w.Header().Set("Allow", "GET, PATCH")
		http.Error(w, "invalid leaf label operation", 405)
		return true
	}
	leaf, e := strconv.ParseUint(q.Get("leaf"), 10, 16)
	if e != nil {
		http.Error(w, "invalid leaf", 400)
		return true
	}
	if r.Header.Get("If-Match") == "" {
		http.Error(w, "read labels before editing", 428)
		return true
	}
	label, e := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024))
	if e != nil {
		http.Error(w, "label exceeds limit", 413)
		return true
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		http.Error(w, "endpoint closed", 503)
		return true
	}
	writer := p.registeredLeafWriters[path]
	if writer == nil {
		writer = &registeredLeafWriter{}
		p.registeredLeafWriters[path] = writer
	}
	p.commitWork.Add(1)
	p.mu.Unlock()
	defer p.commitWork.Done()
	writer.mu.Lock()
	defer writer.mu.Unlock()
	unlock, e := lockRegistration(filepath.Dir(filepath.Join(p.config.Root, filepath.FromSlash(path))), true)
	if e != nil {
		http.Error(w, e.Error(), 503)
		return true
	}
	defer unlock()

	snapshot, e := p.registeredLeaf(path)
	if e != nil {
		http.Error(w, e.Error(), 404)
		return true
	}
	tag := leafLabelsETag(snapshot.bytes, uint16(branch))
	if r.Header.Get("If-Match") != tag {
		http.Error(w, "leaf metadata changed; reload before editing", 412)
		return true
	}
	next, e := reader.trie.label(uint16(branch), uint16(leaf), snapshot.bytes, label)
	if e != nil {
		http.Error(w, e.Error(), 400)
		return true
	}
	defer next.close()
	// Atomic metadata publication preserves all node bytes and offsets. Existing
	// readers keep their immutable snapshot; the commit lane resumes this footer.
	if e = replace(filepath.Join(p.config.Root, filepath.FromSlash(path)), next.bytes); e != nil {
		http.Error(w, e.Error(), 500)
		return true
	}
	writer.native.close()
	writer.native = nil
	writer.registry = nil
	if writer.file != nil {
		writer.file.Close()
		writer.file = nil
	}
	p.mu.Lock()
	delete(p.registeredLeaves, path)
	p.mu.Unlock()
	w.Header().Set("ETag", leafLabelsETag(next.bytes, uint16(branch)))
	w.WriteHeader(204)
	return true
}
