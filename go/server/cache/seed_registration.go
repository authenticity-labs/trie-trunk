package cache

import (
	"encoding/json"
	"time"
	"trie-trunk/go/internal/node"
)

// SeedRegistration identifies the resource using a reusable Definition. These
// settings are persisted in trunk.trie; they are never appended to the .rx.
type SeedRegistration struct {
	Cache        string                   `json:"cache"`
	Service      string                   `json:"service"`
	Resource     string                   `json:"resource"`
	ResourceType string                   `json:"resource_type"`
	Level        string                   `json:"level"`
	File         string                   `json:"file"`
	Binding      map[string]any           `json:"binding"`
	Selection    *SeedDefinitionSelection `json:"selection,omitempty"`
}
type SeedDefinitionSelection struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Major     uint32 `json:"major"`
	Minor     uint32 `json:"minor"`
}

// RegisterSeedDefinition invokes the same node transaction used by every client.
func RegisterSeedDefinition(root string, rx []byte, target SeedRegistration) error {
	if target.Binding == nil {
		target.Binding = map[string]any{}
	}
	request, err := json.Marshal(target)
	if err != nil {
		return err
	}
	owner, err := node.Open(root, node.Config{}, 0, uint64(time.Now().UnixMilli()))
	if err != nil {
		return err
	}
	defer owner.Close()
	if err = owner.Register(request, rx); err != nil {
		return err
	}
	_, err = taxonomyBytes(root)
	return err
}
