package cache

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"syscall"
)

// Quotas are persisted policy only. Deletion is deliberately not connected here.
// Each explicit allotment reserves capacity from its nearest allocating ancestor.
type quotaRecovery struct {
	Unit  string  `json:"unit" toml:"unit"`
	Value float64 `json:"value" toml:"value"`
}
type quotaScope struct {
	Managed  *bool          `json:"managed,omitempty"`
	Path     []string       `json:"path"`
	Limit    *uint64        `json:"limitBytes,omitempty"`
	Recovery *quotaRecovery `json:"recovery,omitempty"`
}
type quotaGroup struct {
	Path            []string `json:"path"`
	ResourceType    string   `json:"resourceType"`
	Managed         bool     `json:"managed"`
	Limit           uint64   `json:"limitBytes"`
	RecoveryPercent float64  `json:"recoveryPercent"`
}
type quotaPolicy struct {
	Managed *bool        `json:"managed,omitempty"`
	Pool    *uint64      `json:"poolBytes"`
	Scopes  []quotaScope `json:"scopes"`
	Groups  []quotaGroup `json:"groups,omitempty"`
}
type quotaDisk struct {
	Total     uint64 `json:"totalBytes"`
	Available uint64 `json:"availableBytes"`
	Owned     uint64 `json:"ownedBytes"`
	Maximum   uint64 `json:"maximumBytes"`
}
type quotaSettings struct {
	Policy   quotaPolicy `json:"policy"`
	Revision string      `json:"revision"`
	Disk     quotaDisk   `json:"disk"`
	Enforced bool        `json:"enforced"`
}

func scopeWithin(path, parent []string) bool {
	return len(path) >= len(parent) && slices.Equal(path[:len(parent)], parent)
}
func quotaManaged(flag *bool, depth int) bool {
	return depth == 0 || flag == nil || *flag
}
func resourceQuotaType(branch string) string {
	switch branch {
	case "collection":
		return "Collections"
	case "job", "task":
		return "Jobs"
	case "read", "write", "info", "error", "debug", "trace":
		return "Logs"
	default:
		return "Custom"
	}
}
func (p quotaPolicy) sharedQuota(path []string) bool {
	if len(path) != 4 {
		return false
	}
	for _, g := range p.Groups {
		if g.Managed && scopeWithin(path, g.Path) && resourceQuotaType(path[3]) == g.ResourceType {
			return true
		}
	}
	return false
}
func validateQuotas(p quotaPolicy, maximum uint64) error {
	if len(p.Scopes)+len(p.Groups) > 10000 {
		return fmt.Errorf("too many quota scopes")
	}
	rootManaged := true
	if p.Pool != nil && (*p.Pool > maximum || *p.Pool > (1<<53)-1) {
		return fmt.Errorf("Trunk pool exceeds available disk capacity")
	}
	seen := map[string]bool{}
	for _, s := range p.Scopes {
		if len(s.Path) < 1 || len(s.Path) > 4 {
			return fmt.Errorf("invalid quota scope")
		}
		for _, part := range s.Path {
			if part == "" || len(part) > 256 {
				return fmt.Errorf("invalid scope name")
			}
		}
		raw, _ := json.Marshal(s.Path)
		key := string(raw)
		if seen[key] {
			return fmt.Errorf("duplicate quota scope")
		}
		seen[key] = true
		if s.Limit != nil && *s.Limit > (1<<53)-1 {
			return fmt.Errorf("invalid allotment")
		}
		if quotaManaged(s.Managed, len(s.Path)) && s.Limit != nil && p.Pool == nil {
			return fmt.Errorf("set the Trunk pool before assigning limits")
		}
		if s.Recovery != nil {
			r := s.Recovery
			if len(s.Path) != 4 || math.IsNaN(r.Value) || math.IsInf(r.Value, 0) || r.Value < 0 || (r.Unit != "percent" && r.Unit != "bytes") || (r.Unit == "percent" && r.Value > 100) || (r.Unit == "bytes" && (r.Value > (1<<53)-1 || math.Trunc(r.Value) != r.Value)) {
				return fmt.Errorf("invalid leaf recovery setting")
			}
		}
	}
	groups := map[string]bool{}
	for _, g := range p.Groups {
		if len(g.Path) != 3 || !slices.Contains([]string{"Logs", "Collections", "Jobs", "Custom"}, g.ResourceType) || math.IsNaN(g.RecoveryPercent) || math.IsInf(g.RecoveryPercent, 0) || g.RecoveryPercent < 0 || g.RecoveryPercent > 100 || g.Limit > (1<<53)-1 {
			return fmt.Errorf("invalid resource-type quota")
		}
		for _, part := range g.Path {
			if part == "" || len(part) > 256 {
				return fmt.Errorf("invalid quota group scope")
			}
		}
		key := storageKey(g.Path) + ":" + g.ResourceType
		if groups[key] {
			return fmt.Errorf("duplicate resource-type quota")
		}
		groups[key] = true
		if g.Managed && rootManaged && p.Pool == nil {
			return fmt.Errorf("set the Trunk pool before assigning limits")
		}
	}
	pool := maximum
	if p.Pool != nil {
		pool = *p.Pool
	}
	type allocation struct {
		path     []string
		limit    uint64
		group    bool
		recovery *quotaRecovery
	}
	entries := []allocation{{limit: pool}}
	for _, s := range p.Scopes {
		if s.Limit != nil && quotaManaged(s.Managed, len(s.Path)) && !p.sharedQuota(s.Path) {
			entries = append(entries, allocation{path: s.Path, limit: *s.Limit, recovery: s.Recovery})
		}
	}
	for _, g := range p.Groups {
		if g.Managed {
			entries = append(entries, allocation{path: g.Path, limit: g.Limit, group: true})
		}
	}
	assigned := make([]uint64, len(entries))
	for i := 1; i < len(entries); i++ {
		child := entries[i]
		parent := 0
		for j := 1; j < len(entries); j++ {
			candidate := entries[j]
			if i != j && !candidate.group && scopeWithin(child.path, candidate.path) && (len(candidate.path) < len(child.path) || child.group) && len(candidate.path) > len(entries[parent].path) {
				parent = j
			}
		}
		if child.limit > entries[parent].limit-assigned[parent] {
			return fmt.Errorf("child allotments exceed their source pool")
		}
		assigned[parent] += child.limit
		if child.recovery != nil && child.recovery.Unit == "bytes" && child.recovery.Value > float64(child.limit) {
			return fmt.Errorf("recovery amount exceeds the scope limit")
		}
	}

	return nil
}
func quotaCapacity(root string) (quotaDisk, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(root, &stat); err != nil {
		return quotaDisk{}, err
	}
	rootInfo, err := os.Stat(root)
	if err != nil {
		return quotaDisk{}, err
	}
	device := rootInfo.Sys().(*syscall.Stat_t).Dev
	d := quotaDisk{Total: stat.Blocks * uint64(stat.Bsize), Available: stat.Bavail * uint64(stat.Bsize)}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return nil
		}
		if st.Dev != device {
			return fmt.Errorf("quota root spans multiple filesystems; configure each storage facility separately")
		}
		if entry.Type().IsRegular() {
			d.Owned += uint64(st.Blocks) * 512
		}
		return nil
	})
	d.Maximum = min(d.Total, d.Available+d.Owned)
	return d, err
}
func (p *Endpoint) quotaHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" && r.Method != "PUT" {
		w.Header().Set("Allow", "GET, PUT")
		http.Error(w, "method not allowed", 405)
		return
	}
	if r.URL.RawQuery != "" {
		http.Error(w, "no query parameters supported", 400)
		return
	}
	// A file lock serializes publications across endpoints sharing this facility.
	lock, err := os.OpenFile(filepath.Join(p.config.Root, ".configurations.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		http.Error(w, "could not open quota lock", 500)
		return
	}
	defer lock.Close()
	mode := syscall.LOCK_SH
	if r.Method == "PUT" {
		mode = syscall.LOCK_EX
	}
	if err = syscall.Flock(int(lock.Fd()), mode); err != nil {
		http.Error(w, "could not lock quotas", 500)
		return
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	policy, revision, err := readQuotaPolicy(p.config.Root)
	if err != nil {
		http.Error(w, "could not read quota settings", 500)
		return
	}
	disk, err := quotaCapacity(p.config.Root)
	if err != nil {
		http.Error(w, "could not measure disk capacity: "+err.Error(), 500)
		return
	}
	if r.Method == "PUT" {
		if r.Header.Get("If-Match") != `"`+revision+`"` {
			http.Error(w, "quota settings changed; reload before saving", 412)
			return
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20))
		decoder.DisallowUnknownFields()
		var next quotaPolicy
		if err = decoder.Decode(&next); err != nil {
			http.Error(w, "invalid quota settings", 400)
			return
		}
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			http.Error(w, "expected one settings document", 400)
			return
		}
		p.mu.Lock()
		sources := p.sources()
		p.mu.Unlock()
		inventory, inventoryErr := collectStorageInventory(sources, map[string]string{p.config.Cache: filepath.Join(p.config.Root, p.config.Cache)}, false)
		if inventoryErr != nil {
			http.Error(w, "could not verify storage inventory", 500)
			return
		}
		for _, scope := range next.Scopes {
			if inventory.nodes[storageKey(scope.Path)] == nil {
				http.Error(w, "quota scope is not in the physical storage catalog; reload storage", 400)
				return
			}
		}
		for _, group := range next.Groups {
			if len(group.Path) != 3 || inventory.nodes[storageKey(group.Path)] == nil {
				http.Error(w, "quota group is not in the physical storage catalog", 400)
				return
			}
			found := false
			for _, node := range inventory.nodes {
				if len(node.Path) == 4 && scopeWithin(node.Path, group.Path) && resourceQuotaType(node.Path[3]) == group.ResourceType {
					found = true
					break
				}
			}
			if !found {
				http.Error(w, "resource type has no physical branches", 400)
				return
			}
		}
		if err = validateQuotas(next, disk.Maximum); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if next.Scopes == nil {
			next.Scopes = []quotaScope{}
		}
		if err = saveQuotaPolicy(p.config.Root, next); err != nil {
			http.Error(w, "could not save scoped quota settings; reload to verify", 500)
			return
		}
		policy, revision, err = readQuotaPolicy(p.config.Root)
		if err != nil {
			http.Error(w, "could not reread scoped quota settings", 500)
			return
		}
	}
	w.Header().Set("ETag", `"`+revision+`"`)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(quotaSettings{policy, revision, disk, false})
}
