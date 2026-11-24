package cache

import (
	"encoding/hex"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
)

func TestRegisteredBranchRoutingUsesRustAndSeparatesResourceTypes(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(func() { taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie")) })
	document, err := seedNativeDocument([]byte(`[{"header":{"rx-name":"event"},"fields":[{"name":"time","type":[1,0],"size":6,"annotations":["@time"]},{"name":"level","type":[1,0],"size":1}]}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer document.close()
	for _, kind := range []string{"http", "security"} {
		if err := RegisterSeedDefinition(root, document.bytes, SeedRegistration{Cache: "demo", Service: "payment", Resource: "transactions", ResourceType: kind, Level: "info", File: "event.rx", Binding: map[string]any{"field.split": "level", "split.info": "9,10,11,12"}}); err != nil {
			t.Fatal(err)
		}
	}
	a, err := taxonomyRoute(root, 1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := taxonomyRoute(root, 2)
	if err != nil {
		t.Fatal(err)
	}
	if a.ResourceType != "http" || b.ResourceType != "security" || a.Digest != b.Digest {
		t.Fatal(a, b)
	}
	raw, _ := hex.DecodeString(a.Digest)
	var digest [32]byte
	copy(digest[:], raw)
	httpSource := &HTTP{Store: &Store{config: Config{Root: root, Cache: "demo", ResourceType: "http"}, layout: &Layout{Layout: &pil.Layout{Digest: digest}, Definition: Definition{Resource: Binding{Major: 1, Service: "payment", Resource: "transactions"}}}}}
	securitySource := &HTTP{Store: &Store{config: Config{Root: root, Cache: "demo", ResourceType: "security"}, layout: &Layout{Layout: &pil.Layout{Digest: digest}, Definition: Definition{Resource: Binding{Major: 1, Service: "payment", Resource: "transactions"}}}}}
	p := &Endpoint{config: Config{Root: root, Cache: "demo"}}
	path := "/demo/payment/transactions.v1/info/packs?beginTime=0&endTime=3600000"
	for _, id := range []uint16{1, 2} {
		request := httptest.NewRequest("GET", path, nil)
		request.Header.Set("Trunk-Branch", strconv.Itoa(int(id)))
		selected, sources, err := p.selectRegisteredBranch(request, []*HTTP{httpSource, securitySource})
		if err != nil {
			t.Fatal(err)
		}
		if len(sources) != 1 || sources[0].Store.config.ResourceType != map[uint16]string{1: "http", 2: "security"}[id] {
			t.Fatal("wrong registered source")
		}
		if selected.URL.Path != request.URL.Path || selected.URL.Query().Get("beginTime") != "0" || selected.URL.Query().Get("endTime") != "3600000" || request.URL.Query().Has("definition") {
			t.Fatal("request modified")
		}
		if selected.URL.Query().Get("definition") != a.Digest {
			t.Fatal("missing exact definition")
		}
	}
	// Sharing a schema does not make a different resource the same source.
	unrelated := *httpSource.Store
	layout := *unrelated.layout
	layout.Definition.Resource.Resource = "other"
	unrelated.layout = &layout
	request := httptest.NewRequest("GET", path, nil)
	request.Header.Set("Trunk-Branch", "1")
	if _, _, err := p.selectRegisteredBranch(request, []*HTTP{{Store: &unrelated}}); err == nil {
		t.Fatal("accepted unrelated storage with the same schema")
	}
	if _, _, err := p.selectRegisteredBranch(request, []*HTTP{httpSource, httpSource}); err == nil {
		t.Fatal("accepted ambiguous active storage")
	}
	p.routing.Store(&routingSnapshot{sources: []*HTTP{httpSource, securitySource}})
	for _, handler := range []interface {
		ServeHTTP(http.ResponseWriter, *http.Request)
	}{p, &HTTP{sources: []*HTTP{httpSource, securitySource}}} {
		for _, selector := range []string{"", "0", "3", "65536"} {
			request := httptest.NewRequest("GET", path, nil)
			request.Header.Set("Trunk-Branch", selector)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != 400 {
				t.Fatalf("invalid branch reached a data handler: %d", recorder.Code)
			}
		}
	}
	entry, _ := taxonomyRegistrations.Load(filepath.Join(root, "trunk.trie"))
	cached := entry.(*taxonomyRegistration).reader.Load()
	if _, err = taxonomyRoute(root, 1); err != nil || entry.(*taxonomyRegistration).reader.Load() != cached {
		t.Fatal("unchanged registry recompiled", err)
	}
	for _, test := range []struct{ path, selector string }{
		{path, "0"}, {path, "65536"}, {path, "garbage"}, {path, "3"},
		{"/other/payment/transactions.v1/info/packs", "1"},
		{"/demo/payment/transactions.v1/revision/packs", "1"},
		{"/demo/payment/transactions.v2/info/packs", "1"},
		{path + "&definition=wrong", "1"},
	} {
		r := httptest.NewRequest("GET", test.path, nil)
		r.Header.Set("Trunk-Branch", test.selector)
		if _, _, err = p.selectRegisteredBranch(r, []*HTTP{httpSource, securitySource}); err == nil {
			t.Fatal("accepted invalid selector", test)
		}
	}
}
