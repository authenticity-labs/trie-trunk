package cache

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestRegisteredDiscoveryWithoutEventStore(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(func() { taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie")) })
	document, err := seedNativeDocument([]byte(`[{"header":{"rx-name":"event"},"fields":[{"name":"time","type":[1,0],"size":6,"annotations":["@time"]}]}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer document.close()
	for _, kind := range []string{"http", "security"} {
		if err = RegisterSeedDefinition(root, document.bytes, SeedRegistration{Cache: "demo", Service: "service", Resource: "events", ResourceType: kind, Level: "", File: "event.rx"}); err != nil {
			t.Fatal(err)
		}
	}
	endpoint, err := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "local"})
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	if len(endpoint.sources()) != 0 {
		t.Fatal("discovery must not need an old event Store")
	}
	var inventory []byte
	for _, path := range []string{"/caches", "/demo/registries?kind=inventory"} {
		request := httptest.NewRequest("GET", path, nil)
		request.Header.Set("Accept", "application/pil")
		response := httptest.NewRecorder()
		endpoint.ServeHTTP(response, request)
		if response.Code != 200 || response.Header().Get("Content-Type") != "application/pil" || response.Body.Len() == 0 {
			t.Fatalf("discovery failed: %s %d %s", path, response.Code, response.Body.String())
		}
		if path != "/caches" {
			inventory = bytes.Clone(response.Body.Bytes())
		}
		if folder := os.Getenv("REGISTERED_DISCOVERY_FIXTURE"); folder != "" {
			if err = os.MkdirAll(folder, 0755); err != nil {
				t.Fatal(err)
			}
			file := "caches.pil"
			if path != "/caches" {
				file = "inventory.pil"
			}
			if err = os.WriteFile(filepath.Join(folder, file), response.Body.Bytes(), 0644); err != nil {
				t.Fatal(err)
			}
		}
		request.Header.Set("If-None-Match", response.Header().Get("ETag"))
		unchanged := httptest.NewRecorder()
		endpoint.ServeHTTP(unchanged, request)
		if unchanged.Code != 304 || unchanged.Body.Len() != 0 {
			t.Fatal("discovery did not revalidate")
		}
	}
	request := httptest.NewRequest("GET", "/caches", nil)
	response := httptest.NewRecorder()
	endpoint.ServeHTTP(response, request)
	var directory struct {
		Caches []string `json:"caches"`
	}
	if err = json.Unmarshal(response.Body.Bytes(), &directory); err != nil || len(directory.Caches) != 2 || directory.Caches[0] != "demo/service/events/http" {
		t.Fatal(directory, err)
	}
	reader, err := taxonomyReader(root)
	if err != nil {
		t.Fatal(err)
	}
	if folder := os.Getenv("REGISTERED_DISCOVERY_FIXTURE"); folder != "" {
		for file, raw := range map[string][]byte{"trunk.trie": reader.image, "event.rx": document.bytes} {
			if err = os.WriteFile(filepath.Join(folder, file), raw, 0644); err != nil {
				t.Fatal(err)
			}
		}
	}
	// No event files are fabricated to make registration discoverable.
	if len(inventory) == 0 {
		t.Fatal("missing registry inventory")
	}
}
