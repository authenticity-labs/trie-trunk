package cache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// EnableQueryEvents registers the ordinary telemetry resource once. Definitions
// and event encoding come from the same Rust implementation on every node.
func (p *Endpoint) EnableQueryEvents() error {
	document, err := seedQueryDefinition()
	if err != nil {
		return err
	}
	defer document.close()
	const filename = "log-query.rx"
	path := filepath.Join(p.config.Root, "definitions", filename)
	old, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if !bytes.Equal(old, document.bytes) {
		if err = RegisterSeedDefinition(p.config.Root, document.bytes, SeedRegistration{Cache: p.config.Cache, Service: "trunk-client", Resource: "queries", ResourceType: "query", File: filename}); err != nil {
			return err
		}
	}
	reader, err := taxonomyReader(p.config.Root)
	if err != nil {
		return err
	}

	candidates, err := reader.trie.routes(p.config.Cache)
	if err != nil {
		return err
	}
	defer candidates.close()
	var routes []registeredRoute
	if err = json.Unmarshal(candidates.bytes, &routes); err != nil {
		return err
	}
	for _, route := range routes {
		if route.Service != "trunk-client" || route.Resource != "queries" || route.ResourceType != "query" {
			continue
		}
		return p.node.Prepare(route.Branch, uint64(p.config.Now().UnixMilli()))
	}
	return fmt.Errorf("query event registration has no route")
}
