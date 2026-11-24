// Build the reviewable log Definition bundle. Does not install it into a server.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"os"
	"os/exec"
	"path/filepath"
	meta "trie-trunk/go/internal/definitionmeta"
	"trie-trunk/go/server/cache"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	dir := flag.String("dir", "../schemas/logs", "authoring and output directory")
	rx := flag.String("rx", "rx", "current RX compiler")
	flag.Parse()
	source, e := meta.LoadSource(*dir)
	if e != nil {
		return e
	}
	layouts := map[string]*pil.Layout{}
	defer func() {
		for _, l := range layouts {
			l.Close()
		}
	}()
	compile := func(file, root string) (*pil.Layout, error) {
		out := filepath.Join(*dir, root+".rx")
		if b, e := exec.Command(*rx, "write", filepath.Join(*dir, file), out, "--root", root).CombinedOutput(); e != nil {
			return nil, fmt.Errorf("compile %s: %s: %w", root, b, e)
		}
		view, e := exec.Command(*rx, "read", out, "--pretty").Output()
		if e != nil {
			return nil, e
		}
		var doc map[string]json.RawMessage
		if e = json.Unmarshal(view, &doc); e != nil {
			return nil, e
		}
		var schema map[string]any
		decoder := json.NewDecoder(bytes.NewReader(doc["cddl"]))
		decoder.UseNumber()
		if e = decoder.Decode(&schema); e != nil {
			return nil, e
		}
		// Remove unrelated roots from each RX without inventing an inheritance format.
		types := schema["types"].(map[string]any)
		keep := map[string]any{}
		var walk func(any)
		var visit func(string)
		visit = func(name string) {
			if _, ok := keep[name]; ok {
				return
			}
			v, ok := types[name]
			if !ok {
				return
			}
			keep[name] = v
			walk(v)
		}
		walk = func(v any) {
			switch x := v.(type) {
			case map[string]any:
				if x["type"] == "reference" {
					visit(x["name"].(string))
				}
				for _, v := range x {
					walk(v)
				}
			case []any:
				for _, v := range x {
					walk(v)
				}
			}
		}
		visit(root)
		schema["types"] = keep
		schema["identity"] = root
		author, e := json.MarshalIndent(schema, "", "  ")
		if e != nil {
			return nil, e
		}
		author = append(author, '\n')
		jsonPath := filepath.Join(*dir, root+".cddl.json")
		if e = os.WriteFile(jsonPath, author, 0644); e != nil {
			return nil, e
		}
		if b, e := exec.Command(*rx, "write", jsonPath, out).CombinedOutput(); e != nil {
			return nil, fmt.Errorf("compile JSON %s: %s: %w", root, b, e)
		}
		raw, e := os.ReadFile(out)
		if e != nil {
			return nil, e
		}
		return pil.Define(raw)
	}
	for _, d := range source.Definitions {
		sourceFiles := map[string]string{"trietrunk": "trietrunk.cddl", "opentelemetry": "opentelemetry.cddl", "datadog": "datadog.cddl"}
		if d.File != d.Name+".rx" {
			return fmt.Errorf("Definition file must be its RX container: %s", d.File)
		}
		l, e := compile(sourceFiles[d.Provider], d.Name)
		if e != nil {
			return e
		}
		layouts[d.Name] = l
	}
	field, e := compile("metadata.cddl", "field-meta")
	if e != nil {
		return e
	}
	defer field.Close()
	synonym, e := compile("metadata.cddl", "synonym")
	if e != nil {
		return e
	}
	defer synonym.Close()
	metadata, synonyms, entries, e := meta.Build(source, layouts, field, synonym)
	if e != nil {
		return e
	}
	// This bundle has metadata bindings only. Registration supplies real resource
	// bindings; no service/resource paths are invented to address presentation data.
	catalog := cache.Catalog{Format: cache.Format, Cache: "log-definitions", Tenant: "local", Definitions: map[string][32]byte{}, DefinitionResources: entries}
	registry, e := pil.MarshalObject(catalog)
	if e != nil {
		return e
	}
	for name, data := range map[string][]byte{"definition.meta": metadata, "synonym.map": synonyms, "registry.map": registry} {
		if e = os.WriteFile(filepath.Join(*dir, name), data, 0644); e != nil {
			return e
		}
	}
	review := struct {
		Definitions   map[string]meta.Resources `json:"definitions"`
		MetadataBytes int                       `json:"metadata_bytes"`
		SynonymBytes  int                       `json:"synonym_bytes"`
		RegistryBytes int                       `json:"registry_bytes"`
	}{map[string]meta.Resources{}, len(metadata), len(synonyms), len(registry)}
	for _, d := range source.Definitions {
		review.Definitions[d.Name] = entries[fmt.Sprintf("%x", layouts[d.Name].Digest)]
	}
	summary, _ := json.MarshalIndent(review, "", "  ")
	if e = os.WriteFile(filepath.Join(*dir, "addresses.json"), append(summary, '\n'), 0644); e != nil {
		return e
	}
	fmt.Printf("%d Definitions; definition.meta %d B; synonym.map %d B; registry.map %d B\n", len(layouts), len(metadata), len(synonyms), len(registry))
	return nil
}
