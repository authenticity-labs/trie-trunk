package cache

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

// Paths belong to the filesystem, not to the persisted quota section. The HTTP
// view still assembles the same allocation tree for Snoot's existing calculations.
type quotaConfiguration struct {
	Managed  *bool                     `toml:"managed,omitempty"`
	Limit    *uint64                   `toml:"limit_bytes,omitempty"`
	Recovery *quotaRecovery            `toml:"recovery,omitempty"`
	Groups   []quotaGroupConfiguration `toml:"groups,omitempty"`
}
type quotaGroupConfiguration struct {
	ResourceType    string  `toml:"resource_type"`
	Managed         bool    `toml:"managed"`
	Limit           uint64  `toml:"limit_bytes"`
	RecoveryPercent float64 `toml:"recovery_percent"`
}

// Only visit hierarchy directories, never event files or shared Definitions.
func quotaConfigurationPaths(root string) (map[string][]string, error) {
	paths := map[string][]string{configurationPath(root, nil): nil}
	var visit func([]string) error
	visit = func(scope []string) error {
		if len(scope) == 4 {
			return nil
		}
		dir := filepath.Join(append([]string{root}, scope...)...)
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") || len(scope) == 0 && entry.Name() == "definitions" {
				continue
			}
			next := append(append([]string{}, scope...), entry.Name())
			paths[configurationPath(root, next)] = next
			if err = visit(next); err != nil {
				return err
			}
		}
		return nil
	}
	return paths, visit(nil)
}
func readQuotaPolicy(root string) (quotaPolicy, string, error) {
	p := quotaPolicy{Scopes: []quotaScope{}}
	paths, err := quotaConfigurationPaths(root)
	if err != nil {
		return p, "", err
	}
	names := make([]string, 0, len(paths))
	for path := range paths {
		names = append(names, path)
	}
	sort.Strings(names)
	for _, path := range names {
		raw, e := readConfiguration(path)
		if e != nil {
			return p, "", e
		}
		if len(raw) == 0 {
			continue
		}
		var doc struct {
			Quota *quotaConfiguration `toml:"quota"`
		}
		if e = toml.Unmarshal(raw, &doc); e != nil {
			return p, "", fmt.Errorf("%s: %w", path, e)
		}
		q := doc.Quota
		if q == nil {
			continue
		}
		scope := paths[path]
		if len(scope) == 0 {
			p.Pool = q.Limit
			p.Managed = q.Managed
		} else if q.Limit != nil || q.Recovery != nil || q.Managed != nil {
			p.Scopes = append(p.Scopes, quotaScope{Path: scope, Managed: q.Managed, Limit: q.Limit, Recovery: q.Recovery})
		}
		for _, g := range q.Groups {
			p.Groups = append(p.Groups, quotaGroup{Path: scope, ResourceType: g.ResourceType, Managed: g.Managed, Limit: g.Limit, RecoveryPercent: g.RecoveryPercent})
		}
	}
	return p, quotaRevision(p), nil
}
func quotaRevision(p quotaPolicy) string {
	if p.Pool == nil && p.Managed == nil && len(p.Scopes) == 0 && len(p.Groups) == 0 {
		return "unset"
	}
	raw, _ := json.Marshal(p)
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}
func saveQuotaPolicy(root string, p quotaPolicy) error {
	paths, err := quotaConfigurationPaths(root)
	if err != nil {
		return err
	}
	desired := map[string]*quotaConfiguration{}
	set := func(scope []string) *quotaConfiguration {
		path := configurationPath(root, scope)
		paths[path] = scope
		if desired[path] == nil {
			desired[path] = &quotaConfiguration{}
		}
		return desired[path]
	}
	if p.Pool != nil || p.Managed != nil {
		q := set(nil)
		q.Limit = p.Pool
		q.Managed = p.Managed
	}
	for _, s := range p.Scopes {
		if s.Limit == nil && s.Recovery == nil && s.Managed == nil {
			continue
		}
		q := set(s.Path)
		q.Limit = s.Limit
		q.Recovery = s.Recovery
		q.Managed = s.Managed
	}
	for _, g := range p.Groups {
		q := set(g.Path)
		q.Groups = append(q.Groups, quotaGroupConfiguration{g.ResourceType, g.Managed, g.Limit, g.RecoveryPercent})
	}
	type edit struct {
		path   string
		raw    []byte
		remove bool
	}
	var edits []edit
	// Prepare and parse every file before touching any. Only the quota section is
	// replaced. The configuration endpoint uses the same administrative file lease.
	names := make([]string, 0, len(paths))
	for path := range paths {
		names = append(names, path)
	}
	sort.Strings(names)
	for _, path := range names {
		raw, e := readConfiguration(path)
		if e != nil {
			return e
		}
		document := map[string]any{"version": int64(1)}
		if len(raw) > 0 {
			if e = toml.Unmarshal(raw, &document); e != nil {
				return e
			}
		}
		var old struct {
			Quota *quotaConfiguration `toml:"quota"`
		}
		if len(raw) > 0 {
			if e = toml.Unmarshal(raw, &old); e != nil {
				return e
			}
		}
		q := desired[path]
		if reflect.DeepEqual(old.Quota, q) {
			continue
		}
		delete(document, "quota")
		if q != nil {
			document["quota"] = q
		}
		next, e := toml.Marshal(document)
		if e != nil {
			return e
		}
		if _, _, e = resolveScope(next, path, Policy{}, len(paths[path]) == 4); e != nil {
			return e
		}
		edits = append(edits, edit{path, next, q == nil && len(paths[path]) > 0 && len(document) == 1})
	}
	for _, change := range edits {
		if change.remove {
			if err = os.Remove(change.path); err != nil {
				return err
			}
			if err = syncDir(filepath.Dir(change.path)); err != nil {
				return err
			}
		} else {
			if err = os.MkdirAll(filepath.Dir(change.path), 0700); err != nil {
				return err
			}
			if err = replace(change.path, change.raw); err != nil {
				return err
			}
		}
	}
	return nil
}
