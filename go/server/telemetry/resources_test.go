package telemetry

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
	"trie-trunk/go/internal/node"
	"trie-trunk/go/server/cache"
)

func TestResourceObservationsUseSharedNode(t *testing.T) {
	root := t.TempDir()
	endpoint, err := cache.OpenEndpoint(cache.Config{Root: root, Cache: "test", Tenant: "test", Storage: "browse"})
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	owner, err := node.Open(root, node.Config{}, 0, uint64(time.Now().UnixMilli()))
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	var mu sync.Mutex
	var failures []error
	observer := &Observer{OnError: func(err error) { mu.Lock(); failures = append(failures, err); mu.Unlock() }}
	if err = observer.Open(owner, "test"); err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	handler := observer.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(202); _, _ = w.Write([]byte("body")) }))
	for _, resource := range Resources {
		path := "/test/service/resource/http/" + resource
		if resource == "definitions" || resource == "registries" {
			path = "/test/" + resource
		}
		if resource == "leaves" || resource == "collections" {
			path = "/test/service/resource/" + resource
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("GET", path+"?range=1,2&token=must-not-log", nil))
		if w.Code != 202 || w.Body.String() != "body" {
			t.Fatal("observer altered response", w.Code, w.Body.String())
		}
	}
	observer.Close()
	mu.Lock()
	defer mu.Unlock()
	if len(failures) > 0 {
		t.Fatal(failures)
	}
	// Closing an observer does not close the node used by other transports.
	registry, err := owner.Registry()
	if err != nil {
		t.Fatal(err)
	}
	registry.Close()
}

func TestInactiveObserverPassesThrough(t *testing.T) {
	observer := &Observer{}
	count := 0
	handler := observer.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { count++; w.WriteHeader(204) }))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/test/registries", nil))
	if count != 1 || w.Code != 204 {
		t.Fatal(count, w.Code)
	}
	observer.Close()
}
