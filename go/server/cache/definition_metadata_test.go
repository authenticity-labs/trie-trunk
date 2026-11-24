package cache

import (
	"os"
	"testing"
	"trie-trunk/go/internal/definitionmeta"
)

func TestDefinitionMetadataRegistryRoundTrip(t *testing.T) {
	raw, e := os.ReadFile("../../../schemas/logs/registry.map")
	if e != nil {
		t.Fatal(e)
	}
	var catalog Catalog
	if e = decoder.Unmarshal(raw, &catalog); e != nil {
		t.Fatal(e)
	}
	if len(catalog.DefinitionResources) != 27 {
		t.Fatal("missing metadata bindings")
	}
	catalog.Definitions["test/resource.v1/0"] = [32]byte{1}
	catalog.Preferred = map[string][32]byte{"test/resource.v1": {1}}
	catalog.DefinitionResources["test"] = definitionmeta.Resources{Metadata: definitionmeta.Location{Offset: 2, Length: 16}}
	raw, e = encoder.Marshal(catalog)
	if e != nil {
		t.Fatal(e)
	}
	var restored Catalog
	if e = decoder.Unmarshal(raw, &restored); e != nil {
		t.Fatal(e)
	}
	if len(restored.DefinitionResources) != 28 || restored.Definitions["test/resource.v1/0"] != ([32]byte{1}) || restored.Preferred["test/resource.v1"] != ([32]byte{1}) {
		t.Fatal("registry metadata lost other bindings")
	}
}
