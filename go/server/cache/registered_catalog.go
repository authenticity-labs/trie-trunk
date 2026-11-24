package cache

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
)

// Host I/O reports file existence and lengths. Rust resolves the registrations,
// constructs the existing discovery responses, and encodes their PIL payloads.
func (p *Endpoint) registeredCatalog(w http.ResponseWriter, r *http.Request) bool {
	inventory := r.URL.Path == "/"+p.config.Cache+"/registries" && r.URL.RawQuery == "kind=inventory"
	directory := r.URL.Path == "/caches"
	if p.registeredReads == nil || !inventory && !directory {
		return false
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "GET required", 405)
		return true
	}
	if directory && !catalogGET(w, r) {
		return true
	}
	reader, err := taxonomyReader(p.config.Root)
	if err != nil {
		http.Error(w, err.Error(), 503)
		return true
	}
	mode := uint32(1)
	var observed []byte
	if directory {
		mode = 2
		if r.Header.Get("Accept") != "application/pil" {
			mode = 3
		}
	} else {
		candidates, e := reader.trie.catalog(p.config.Cache, nil, 0)
		if e != nil {
			http.Error(w, e.Error(), 503)
			return true
		}
		var catalog struct {
			Files []struct {
				Path        string   `json:"path"`
				BranchID    uint16   `json:"branchId,omitempty"`
				Kind        string   `json:"kind"`
				Definition  string   `json:"definition,omitempty"`
				Definitions []string `json:"definitions,omitempty"`
				Digest      string   `json:"digest,omitempty"`
				Size        uint64   `json:"size"`
				Bootstrap   bool     `json:"bootstrap"`
			} `json:"files"`
		}
		e = json.Unmarshal(candidates.bytes, &catalog)
		candidates.close()
		if e != nil {
			http.Error(w, e.Error(), 500)
			return true
		}
		files := catalog.Files[:0]
		for _, file := range catalog.Files {
			path := filepath.Join(p.config.Root, filepath.FromSlash(file.Path))
			if file.Bootstrap {
				var raw []byte
				if file.Path == "trunk.trie" {
					raw, e = reader.image, nil
				} else if file.Kind == "state" {
					var snapshot *registeredLeafSnapshot
					snapshot, e = p.registeredLeaf(file.Path)
					if e == nil {
						raw = snapshot.bytes
						file.Definitions = snapshot.definitions
					}
				} else {
					raw, e = os.ReadFile(path)
				}
				if os.IsNotExist(e) && file.Kind == "state" {
					continue
				}
				if e != nil {
					http.Error(w, e.Error(), 503)
					return true
				}

				file.Size = uint64(len(raw))
				file.Digest = fmt.Sprintf("%x", sha256.Sum256(raw))
			} else {
				stat, e := os.Stat(path)
				if os.IsNotExist(e) {
					continue
				}
				if e != nil {
					http.Error(w, e.Error(), 503)
					return true
				}
				if !stat.Mode().IsRegular() {
					http.Error(w, "registered artifact is not a file", 503)
					return true
				}
				file.Size = uint64(stat.Size())
			}
			files = append(files, file)
		}
		observed, err = json.Marshal(files)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return true
		}
	}
	body, err := reader.trie.catalog(p.config.Cache, observed, mode)
	if err != nil {
		http.Error(w, err.Error(), 503)
		return true
	}
	defer body.close()
	if mode == 3 {
		w.Header().Set("Content-Type", "application/json")
	} else {
		w.Header().Set("Content-Type", "application/pil")
	}
	if metadataUnchanged(w, r, body.bytes) {
		return true
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body.bytes)))
	_, _ = w.Write(body.bytes)
	return true
}
