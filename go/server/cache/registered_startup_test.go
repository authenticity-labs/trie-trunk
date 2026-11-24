package cache

import (
	"bytes"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestEndpointStartupPreparesRegisteredRXInRust(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(func() { taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie")) })
	document, err := seedNativeDocument([]byte(`[{"header":{"rx-name":"event"},"fields":[
		{"name":"time","type":[1,0],"size":6,"annotations":["@time"]},
		{"name":"id","type":[4,0],"size":10,"annotations":["@event"]},
		{"name":"extra","type":[126,122],"size":0,"element-size":0,"annotations":["@fields"]}
	]}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer document.close()
	target := SeedRegistration{Cache: "demo", Service: "payment", Resource: "transactions", ResourceType: "http", Level: "", File: "event.rx"}
	if err = RegisterSeedDefinition(root, document.bytes, target); err != nil {
		t.Fatal(err)
	}
	// Catalog registration no longer supplies executable schemas. Leaving an
	// obsolete file reference here must not trigger the old Definition parser.
	if err = saveCatalog(root, Catalog{Format: Format, Cache: "demo", Tenant: "local",
		Definitions: map[string][32]byte{"payment/transactions.v0/0": {1}},
	}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(root, "trunk.trie"))
	if err != nil {
		t.Fatal(err)
	}
	// Exercise an actual restart, without the registration process's cache.
	taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie"))
	endpoint, err := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "local"})
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(root, "trunk.trie"))
	if err != nil || !bytes.Equal(before, after) {
		endpoint.Close()
		t.Fatal("startup rewrote registered metadata", err)
	}
	request := httptest.NewRequest("GET", "/demo/definitions?file=event.rx", nil)
	response := httptest.NewRecorder()
	endpoint.ServeHTTP(response, request)
	if response.Code != 200 || !bytes.Equal(response.Body.Bytes(), document.bytes) {
		endpoint.Close()
		t.Fatalf("registered RX resource unavailable after startup: %d", response.Code)
	}
	reader, err := taxonomyReader(root)
	if err != nil {
		endpoint.Close()
		t.Fatal(err)
	}
	route, err := taxonomyRoute(root, 1)
	if err != nil {
		endpoint.Close()
		t.Fatal(err)
	}
	definition, err := reader.trie.prescribe(route.Definition, nil)
	if err != nil {
		endpoint.Close()
		t.Fatal("startup did not prepare the registered codec", err)
	}
	definition.close()
	if !reader.shared {
		t.Fatal("Definition file was not prepared")
	}
	if err = endpoint.Close(); err != nil {
		t.Fatal(err)
	}

	// A new process must load the registered RX resource through DOC. A file
	// containing the previous object-shaped schema cannot become a prescription.
	taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie"))
	if err = os.WriteFile(filepath.Join(root, "definitions", "event.rx"), []byte(`{"fields":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if invalid, err := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "local"}); err == nil {
		invalid.Close()
		t.Fatal("accepted a file outside the RX Definition format")
	}
}

func TestEndpointRejectsHostCodecLayout(t *testing.T) {
	if endpoint, err := OpenEndpoint(Config{Root: t.TempDir(), Cache: "demo", Tenant: "local", Layout: &Layout{}}); err == nil {
		endpoint.Close()
		t.Fatal("endpoint accepted an unregistered host codec layout")
	}
}
