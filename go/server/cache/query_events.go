package cache

import (
	"fmt"
	"os"
	"path/filepath"
)

// EnableQueryEvents installs the built-in model once. Reopening an existing
// node prepares the Definition selected by that node's registry.
func (p *Endpoint) EnableQueryEvents() error {
	source := p.config.Cache + "/trunk-client/queries/query"
	prepare := func() (bool, error) {
		reader, err := taxonomyReader(p.config.Root)
		if err != nil {
			return false, err
		}
		routes, err := reader.routes(p.config.Cache)
		if err != nil {
			return false, err
		}
		var latest *registeredRoute
		for i := range routes {
			r := &routes[i]
			if r.Source == source && (latest == nil || r.Major > latest.Major || r.Major == latest.Major && r.Minor > latest.Minor) {
				latest = r
			}
		}
		if latest == nil {
			return false, nil
		}
		return true, p.node.Prepare(latest.Branch, uint64(p.config.Now().UnixMilli()))
	}
	if _, err := os.Stat(filepath.Join(p.config.Root, "trunk.trie")); err == nil {
		if found, err := prepare(); found || err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	document, err := seedQueryDefinition()
	if err != nil {
		return err
	}
	defer document.close()
	if err = RegisterSeedDefinition(p.config.Root, document.bytes, SeedRegistration{Cache: p.config.Cache, Service: "trunk-client", Resource: "queries", ResourceType: "query", File: "log-query.rx"}); err != nil {
		return err
	}
	if found, err := prepare(); found || err != nil {
		return err
	}
	return fmt.Errorf("query event registration has no route")
}
