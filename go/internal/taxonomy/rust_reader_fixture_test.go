package taxonomy

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// The Rust registration prototype must preserve the production taxonomy format.
// Definition metadata is opaque here; only Rust interprets its PIL payload.
func TestReadRustRegistrationSnapshot(t *testing.T) {
	raw, err := os.ReadFile("../../../rust/trunk/examples/implicit-shapes/trunk.trie")
	if err != nil {
		t.Fatal(err)
	}
	tr, err := Load(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(tr.Section(2)) == 0 {
		t.Fatal("missing shape registry")
	}
	for id := LeafID(1); id <= 6; id++ {
		node, ok := tr.Leaf(id)
		if !ok {
			t.Fatalf("missing branch %d", id)
		}
		if got, ok := tr.BranchID(node); !ok || got != id {
			t.Fatal("unstable branch identity")
		}
	}
	metadata := bytes.Clone(tr.Section(2))
	tr.SetSection(99, []byte("independent footer update"))
	reopened, err := Load(tr.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reopened.Section(2), metadata) {
		t.Fatal("shape registry changed")
	}
}

// Cross-language fixtures use the production writer, including its CRCs,
// commit slots and EOF-relative references. No alternate format writer.
func TestExportRustReaderFixture(t *testing.T) {
	dir := os.Getenv("TRUNK_RUST_FIXTURE_DIR")
	if dir == "" {
		t.Skip("fixture export not requested")
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	tr, err := New("Main")
	if err != nil {
		t.Fatal(err)
	}
	c := add(t, tr, tr.Root(), Cache, "demo", "", "")
	s := add(t, tr, c, Service, "payment", "", "")
	r := add(t, tr, s, Resource, "transactions", "", "")
	v := add(t, tr, r, Version, "1.0", "", "")
	add(t, tr, v, Branch, "", "http", "error")
	add(t, tr, v, Branch, "", "security", "")
	tr.SetSection(1, []byte("registered definitions"))
	before := tr.Bytes()
	if err = os.WriteFile(filepath.Join(dir, "before.trie"), before, 0644); err != nil {
		t.Fatal(err)
	}
	r = add(t, tr, s, Resource, "refunds", "", "")
	v = add(t, tr, r, Version, "1.1", "", "")
	add(t, tr, v, Branch, "", "http", "write")
	c = add(t, tr, tr.Root(), Cache, "研究", "", "")
	add(t, tr, c, Service, "lab", "", "")
	tr.SetSection(1, []byte("updated registration"))
	tr.SetSection(9, []byte{1, 2, 3})
	if err = os.WriteFile(filepath.Join(dir, "after.trie"), tr.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
}
