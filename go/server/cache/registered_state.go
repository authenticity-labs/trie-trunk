package cache

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Immutable published bytes shared by discovery and state retrieval. Rust
// interprets the root directory; the host owns file I/O and snapshot lifetime.
type registeredLeafSnapshot struct {
	file        os.FileInfo
	bytes       []byte
	definitions []string
}

func (p *Endpoint) registeredLeaf(path string) (*registeredLeafSnapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, fmt.Errorf("endpoint closed")
	}
	previous := p.registeredLeaves[path]
	if p.publishingLeaves[path] {
		if previous != nil {
			return previous, nil
		}
		return nil, os.ErrNotExist
	}
	full := filepath.Join(p.config.Root, filepath.FromSlash(path))
	if previous != nil {
		info, err := os.Stat(full)
		if err != nil {
			return nil, err
		}
		if os.SameFile(previous.file, info) {
			return previous, nil
		}
	}
	// The Rust node atomically replaces a leaf file when it publishes new IDs.
	// Pin the inode used to read these bytes so revalidation survives a rename.
	f, err := os.Open(full)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	reader, err := taxonomyReader(p.config.Root)
	if err != nil {
		return nil, err
	}
	roots, err := reader.trie.leafDefinitions(path, raw)
	if err != nil {
		return nil, err
	}
	snapshot := &registeredLeafSnapshot{file: info, bytes: raw, definitions: roots}
	p.registeredLeaves[path] = snapshot
	return snapshot, nil
}
func (p *Endpoint) registeredLeavesResource(w http.ResponseWriter, r *http.Request) bool {
	if p.registeredReads == nil || !strings.HasSuffix(r.URL.Path, "/leaves") {
		return false
	}
	if r.URL.Query().Has("labels") || r.URL.Query().Has("leaf") {
		return p.leafLabelsResource(w, r)
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "GET required", 405)
		return true
	}
	source := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/"), "/leaves")
	if strings.Split(source, "/")[0] != p.config.Cache {
		http.NotFound(w, r)
		return true
	}
	query := r.URL.Query()
	for key, values := range query {
		if key != "definition" || len(values) != 1 {
			http.Error(w, "invalid state selector", 400)
			return true
		}
	}
	var branch uint64
	if selector := r.Header.Get("Trunk-Branch"); selector != "" {
		var err error
		branch, err = strconv.ParseUint(selector, 10, 16)
		if err != nil || branch == 0 {
			http.Error(w, "invalid state branch", 400)
			return true
		}
	}
	reader, err := taxonomyReader(p.config.Root)
	var path string
	if err == nil {
		path, err = reader.trie.statePath(source, query.Get("definition"), uint16(branch))
	}
	if err != nil {
		http.Error(w, err.Error(), 400)
		return true
	}
	snapshot, err := p.registeredLeaf(path)
	if os.IsNotExist(err) {
		http.NotFound(w, r)
		return true
	}
	if err != nil {
		http.Error(w, err.Error(), 503)
		return true
	}
	if branch != 0 {
		w.Header().Set("Trunk-Branch", strconv.FormatUint(branch, 10))
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-cache")
	if metadataUnchanged(w, r, snapshot.bytes) {
		return true
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(snapshot.bytes)))
	_, _ = (summaryContextWriter{r.Context(), w}).Write(snapshot.bytes)
	return true
}
