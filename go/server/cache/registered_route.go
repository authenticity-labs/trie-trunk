package cache

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"trie-trunk/go/internal/node"
)

// This is a request projection returned by Rust, not a second registry.
type registeredRoute struct {
	Source       string `json:"source"`
	Branch       uint16 `json:"branch"`
	Definition   uint32 `json:"definition"`
	Digest       string `json:"digest"`
	Cache        string `json:"cache"`
	Service      string `json:"service"`
	Resource     string `json:"resource"`
	ResourceType string `json:"resource_type"`
	Level        string `json:"level"`
	Major        uint32 `json:"major"`
	Minor        uint32 `json:"minor"`
	Storage      struct {
		Directory string `json:"directory"`
		Stem      string `json:"stem"`
	} `json:"storage"`
}
type registeredRouteReader struct {
	image  []byte
	etag   string
	trie   *seedNativeTrunk
	mu     sync.Mutex
	files  map[string]bool
	shared bool
}

func taxonomyReader(root string) (*registeredRouteReader, error) {
	raw, err := taxonomyBytes(root)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty taxonomy")
	}
	entry, ok := taxonomyRegistrations.Load(filepath.Join(root, "trunk.trie"))
	if !ok {
		return nil, fmt.Errorf("taxonomy not published")
	}
	registration := entry.(*taxonomyRegistration)
	reader := registration.reader.Load()
	if reader == nil || len(reader.image) != len(raw) || &reader.image[0] != &raw[0] {
		registration.readerMu.Lock()
		// Re-read after locking so an older request cannot replace a newer
		// cached registration snapshot.
		raw, err = taxonomyBytes(root)
		if err != nil || len(raw) == 0 {
			registration.readerMu.Unlock()
			return nil, fmt.Errorf("taxonomy unavailable: %v", err)
		}
		reader = registration.reader.Load()
		if reader == nil || len(reader.image) != len(raw) || &reader.image[0] != &raw[0] {
			var trie *seedNativeTrunk
			var shared bool
			owner, e := node.Registry(root)
			if e == nil && owner != nil {
				trie, e = seedNativeTrunkHandle(owner.Trie())
				registration.mu.Lock()
				raw = bytes.Clone(owner.Bytes(0, 0))
				if registration.owner != nil {
					registration.owner.Close()
				}
				registration.owner = owner
				registration.published.Store(&raw)
				registration.mu.Unlock()
				shared = true
			} else if e == nil {
				trie, e = seedNativeOpenTrunk(raw)
			}
			if e != nil {
				registration.readerMu.Unlock()
				return nil, e
			}
			reader = &registeredRouteReader{image: raw, etag: fmt.Sprintf("\"%x\"", sha256.Sum256(raw)), trie: trie, files: make(map[string]bool), shared: shared}
			registration.reader.Store(reader)
		}
		registration.readerMu.Unlock()
	}
	return reader, nil
}

func (reader *registeredRouteReader) prescribe(root string, id uint32) (*seedNativeDefinition, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	file, err := reader.trie.definitionFile(id)
	if err != nil {
		return nil, err
	}
	if reader.shared {
		reader.files[file] = true
		return reader.trie.prescribe(id, nil)
	}
	if !reader.files[file] {
		raw, err := os.ReadFile(filepath.Join(root, "definitions", file))
		if err != nil {
			return nil, err
		}
		if err = reader.trie.prescribeFile(file, raw); err != nil {
			return nil, err
		}
		reader.files[file] = true
	}
	return reader.trie.prescribe(id, nil)
}

func taxonomyRoute(root string, id uint16) (registeredRoute, error) {
	var route registeredRoute
	reader, err := taxonomyReader(root)
	if err != nil {
		return route, err
	}
	result, err := reader.trie.route(id)
	if err != nil {
		return route, err
	}
	defer result.close()
	err = json.Unmarshal(result.bytes, &route)
	return route, err
}

// Startup prepares the reusable RX files named by this cache's registrations.
// Rust resolves lineage and builds codec plans. Host startup only reads files.
func prepareRegisteredDefinitions(root, cache string) error {
	if _, err := os.Stat(filepath.Join(root, "trunk.trie")); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	reader, err := taxonomyReader(root)
	if err != nil {
		return err
	}
	result, err := reader.trie.routes(cache)
	if err != nil {
		return err
	}
	defer result.close()
	var routes []registeredRoute
	if err = json.Unmarshal(result.bytes, &routes); err != nil {
		return err
	}
	for _, route := range routes {
		definition, err := reader.prescribe(root, route.Definition)
		if err != nil {
			return fmt.Errorf("prepare registered Definition %d: %w", route.Definition, err)
		}
		definition.close()
	}
	return nil
}

// Bind the request before invoking the existing range/stream handlers. Headers
// select an exact trie branch without changing .rx URLs or HTTP operations.
func (p *Endpoint) selectRegisteredBranch(r *http.Request, sources []*HTTP) (*http.Request, []*HTTP, error) {
	values, exists := r.Header[http.CanonicalHeaderKey("Trunk-Branch")]
	if !exists {
		return r, sources, nil
	}
	if len(values) != 1 {
		return nil, nil, fmt.Errorf("one Trunk-Branch is required")
	}
	n, err := strconv.ParseUint(values[0], 10, 16)
	if err != nil || n == 0 {
		return nil, nil, fmt.Errorf("invalid Trunk-Branch")
	}
	route, err := taxonomyRoute(p.config.Root, uint16(n))
	if err != nil {
		return nil, nil, err
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if len(parts) < 4 || route.Cache != p.config.Cache || parts[0] != route.Cache || parts[1] != route.Service || parts[2] != fmt.Sprintf("%s.v%d", route.Resource, route.Major) {
		return nil, nil, fmt.Errorf("Trunk-Branch does not match resource")
	}
	if len(parts) == 5 && parts[3] != route.Level {
		return nil, nil, fmt.Errorf("Trunk-Branch does not match branch")
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, nil, err
	}
	if q.Get("definition") != "" && q.Get("definition") != route.Digest {
		return nil, nil, fmt.Errorf("Trunk-Branch Definition mismatch")
	}
	if given := r.Header.Get("Definition-Version"); given != "" && given != route.Digest {
		return nil, nil, fmt.Errorf("Trunk-Branch Definition mismatch")
	}
	selected := make([]*HTTP, 0, 1)
	for _, source := range sources {
		n := source.Store.layout.Definition.Resource
		if source.Store.config.Cache == route.Cache && n.Service == route.Service && n.Resource == route.Resource && n.Major == route.Major && source.Store.config.ResourceType == route.ResourceType && fmtDigestNative(source.Store.layout.Digest) == route.Digest {
			selected = append(selected, source)
		}
	}
	if len(selected) != 1 {
		return nil, nil, fmt.Errorf("registered branch has %d active storage bindings; expected one", len(selected))
	}
	copy := r.Clone(r.Context())
	u := *r.URL
	copy.URL = &u
	q.Set("definition", route.Digest)
	u.RawQuery = q.Encode()
	return copy, selected, nil
}
