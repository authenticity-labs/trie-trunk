package cache

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"trie-trunk/go/internal/taxonomy"
)

func TestSeedDefinitionRegistrationPublishesBindingOutsideRX(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "trunk.trie")
	t.Cleanup(func() { taxonomyRegistrations.Delete(path) })
	// Preserve other existing footer sections through the same publication path.
	if err := updateRegistration(root, func(tree *taxonomy.Trie) error {
		tree.SetSection(19, []byte("unrelated metadata"))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	document, err := seedNativeDocument([]byte(`[{"header":{"rx-name":"event","rx-major":1},"fields":[
		{"name":"time","type":[1,0],"size":6,"annotations":["@time"]},
		{"name":"id","type":[4,0],"size":10,"annotations":["@event"]},
		{"name":"region","type":[5,0],"size":0}
	]}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer document.close()
	original := bytes.Clone(document.bytes)
	target := SeedRegistration{
		Cache: "demo", Service: "payment", Resource: "transactions",
		ResourceType: "http", Level: "", File: "event.rx",
		Binding: map[string]any{"strategy": "log", "timezone": "UTC", "path": []string{"region"}},
	}
	register := func() []byte {
		t.Helper()
		if err := RegisterSeedDefinition(root, document.bytes, target); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		published, err := taxonomyBytes(root)
		if err != nil || !bytes.Equal(published, raw) {
			t.Fatal("published snapshot differs from committed file", err)
		}
		stored, err := os.ReadFile(filepath.Join(root, "definitions", target.File))
		if err != nil || !bytes.Equal(stored, original) || !bytes.Equal(document.bytes, original) {
			t.Fatal("registration altered Definition", err)
		}
		return raw
	}
	binding := func(raw []byte, branch uint16, timezone string) uint32 {
		t.Helper()
		result, err := seedNativeBinding(raw, branch)
		if err != nil {
			t.Fatal(err)
		}
		defer result.close()
		var resource struct {
			Branch     uint16         `json:"branch"`
			Definition uint32         `json:"definition"`
			Binding    map[string]any `json:"binding"`
		}
		if err = json.Unmarshal(result.bytes, &resource); err != nil {
			t.Fatal(err)
		}
		if resource.Branch != branch || resource.Binding["timezone"] != timezone {
			t.Fatalf("unexpected resource binding: %s", result.bytes)
		}
		return resource.Definition
	}
	before := register()
	definition := binding(before, 1, "UTC")
	prior, err := taxonomy.Load(before)
	if err != nil {
		t.Fatal(err)
	}
	// Reopen from disk rather than the retained registration memtable.
	taxonomyRegistrations.Delete(path)
	target.Binding["timezone"] = "America/New_York"
	after := register()
	next, err := taxonomy.Load(after)
	if err != nil {
		t.Fatal(err)
	}
	if !next.Extends(prior) || next.NodeCount() != prior.NodeCount() || !bytes.Equal(next.Section(19), prior.Section(19)) {
		t.Fatal("binding edit changed taxonomy addressing or unrelated metadata")
	}
	if binding(after, 1, "America/New_York") != definition || binding(before, 1, "UTC") != definition {
		t.Fatal("binding edit changed Definition identity or previous snapshot")
	}
	// One reusable Definition, two independent resource registrations.
	target.Resource = "refunds"
	target.Binding = map[string]any{"timezone": "Europe/Paris"}
	after = register()
	if binding(after, 2, "Europe/Paris") != definition {
		t.Fatal("resource duplicated reusable Definition")
	}
	binding(after, 1, "America/New_York")
	// Invalid input must not publish a registration or install a file.
	target.File = "../outside.rx"
	if err = RegisterSeedDefinition(root, document.bytes, target); err == nil {
		t.Fatal("accepted an external Definition filename")
	}
	current, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(current, after) {
		t.Fatal("failed registration changed committed snapshot", err)
	}
	if fixture := os.Getenv("TRUNK_NATIVE_REGISTRATION_FIXTURE"); fixture != "" {
		if err := os.WriteFile(fixture, current, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSeedLineagePublicationKeepsVersionsAndDefaultSeparate(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "trunk.trie")
	t.Cleanup(func() { taxonomyRegistrations.Delete(path) })
	oldJSON := `{"header":{"rx-name":"http","namespace":"log","rx-major":1,"rx-minor":0},"fields":[{"name":"status","type":[1,0],"size":1}]}`
	newJSON := `{"header":{"rx-name":"http","namespace":"log","rx-major":1,"rx-minor":1},"fields":[{"name":"status","type":[1,0],"size":2}]}`
	old, err := seedNativeDocument([]byte("[" + oldJSON + "]"))
	if err != nil {
		t.Fatal(err)
	}
	defer old.close()
	bundle, err := seedNativeDocument([]byte("[" + newJSON + "," + oldJSON + "]"))
	if err != nil {
		t.Fatal(err)
	}
	defer bundle.close()
	target := SeedRegistration{Cache: "demo", Service: "payment", Resource: "transactions", ResourceType: "http", Level: "", File: "log.http.rx"}
	if err = RegisterSeedDefinition(root, old.bytes, target); err != nil {
		t.Fatal(err)
	}
	if err = RegisterSeedDefinition(root, bundle.bytes, target); err != nil {
		t.Fatal(err)
	}
	stored, err := os.ReadFile(filepath.Join(root, "definitions", target.File))
	if err != nil || !bytes.Equal(stored, bundle.bytes) {
		t.Fatal("lineage bundle not published", err)
	}
	target.Selection = &SeedDefinitionSelection{Namespace: "log", Name: "http", Major: 1, Minor: 0}
	if err = RegisterSeedDefinition(root, bundle.bytes, target); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var ids []uint32
	for _, branch := range []uint16{1, 2} {
		result, err := seedNativeBinding(before, branch)
		if err != nil {
			t.Fatal(err)
		}
		var r struct {
			Definition uint32 `json:"definition"`
		}
		err = json.Unmarshal(result.bytes, &r)
		result.close()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.Definition)
	}
	if ids[0] == ids[1] {
		t.Fatal("versions bound to the same Definition")
	}
	trie, err := seedNativeOpenTrunk(before)
	if err != nil {
		t.Fatal(err)
	}
	defer trie.close()
	for version, id := range ids {
		file, err := trie.definitionFile(id)
		if err != nil || file != target.File {
			t.Fatal("registered file lookup", file, err)
		}
		codec, err := trie.prescribe(id, stored)
		if err != nil {
			t.Fatal(err)
		}
		value, err := codec.ingest([]seedNativeValue{{ordinal: 0, kind: seedUnsigned, low: 300}}, false)
		codec.close()
		if version == 1 {
			if err != nil {
				t.Fatal(err)
			}
			value.close()
		} else if err == nil {
			value.close()
			t.Fatal("old version used default version width")
		}
	}
	// Missing versions and bundles that drop a published version leave both files alone.
	target.Selection.Minor = 9
	if err = RegisterSeedDefinition(root, bundle.bytes, target); err == nil {
		t.Fatal("missing version accepted")
	}
	target.Selection = nil
	if err = RegisterSeedDefinition(root, old.bytes, target); err == nil {
		t.Fatal("dropped published schema")
	}
	after, _ := os.ReadFile(path)
	stored, _ = os.ReadFile(filepath.Join(root, "definitions", target.File))
	if !bytes.Equal(before, after) || !bytes.Equal(stored, bundle.bytes) {
		t.Fatal("rejected registration changed persisted state")
	}
	if fixture := os.Getenv("TRUNK_LINEAGE_FIXTURE"); fixture != "" {
		if err := os.MkdirAll(fixture, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(fixture, "log.http.rx"), bundle.bytes, 0600); err != nil {
			t.Fatal(err)
		}
		for _, member := range []uint32{0, 1} {
			codec, err := seedNativeRegisterMember(bundle.bytes, member)
			if err != nil {
				t.Fatal(err)
			}
			value := uint64(300)
			name := "current.pac"
			if member == 1 {
				value = 200
				name = "previous.pac"
			}
			pill, err := codec.ingest([]seedNativeValue{{ordinal: 0, kind: seedUnsigned, low: value}}, false)
			if err != nil {
				codec.close()
				t.Fatal(err)
			}
			pack, err := codec.pack([][]byte{pill.bytes}, 3_600_000, true, 0, 0)
			pill.close()
			codec.close()
			if err != nil {
				t.Fatal(err)
			}
			err = os.WriteFile(filepath.Join(fixture, name), pack.bytes, 0600)
			pack.close()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}
