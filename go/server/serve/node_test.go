package serve

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
	"trie-trunk/go/server/cache"
)

// Exercise the existing listeners against the shared owner, without the retired
// Store/logger fixtures used by the isolated object prototype tests.
func TestRegisteredNodeTransports(t *testing.T) {
	for _, storage := range []string{"pack", "browse"} {
		t.Run(storage, func(t *testing.T) {
			root := shortDirectory(t)
			fixture := func(name string) []byte {
				t.Helper()
				data, err := os.ReadFile("../../../testdata/http-ingress/" + name)
				if err != nil {
					t.Fatal(err)
				}
				return data
			}
			const hour = int64(1794830400000)
			endpoint, err := cache.OpenEndpoint(cache.Config{Root: root, Cache: "demo", Tenant: "test", Storage: storage, Now: func() time.Time { return time.UnixMilli(hour + 3000) }})
			if err != nil {
				t.Fatal(err)
			}
			defer endpoint.Close()
			serverTLS, clientTLS := credentials(t)
			socket := filepath.Join(root, "node.sock")
			address, stop := start(t, Config{Root: root, Address: "127.0.0.1:0", Socket: socket, TLS: serverTLS, API: endpoint.Handler("secret")})
			defer stop()
			for _, protocol := range []string{"http2", "http3", "socket"} {
				t.Run(protocol, func(t *testing.T) {
					origin := "https://" + address
					var transport http.RoundTripper
					switch protocol {
					case "http2":
						h := &http.Transport{TLSClientConfig: clientTLS.Clone(), ForceAttemptHTTP2: true}
						defer h.CloseIdleConnections()
						transport = h
					case "http3":
						h := &http3.Transport{TLSClientConfig: clientTLS.Clone()}
						defer h.Close()
						transport = h
					case "socket":
						h := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
							return (&net.Dialer{}).DialContext(ctx, "unix", socket)
						}}
						defer h.CloseIdleConnections()
						transport = h
						origin = "http://localhost"
					}
					client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
					request := func(method, path string, body []byte, want int) []byte {
						t.Helper()
						r, err := http.NewRequest(method, origin+path, bytes.NewReader(body))
						if err != nil {
							t.Fatal(err)
						}
						r.Header.Set("Authorization", "Bearer secret")
						if path == "/definitions" {
							r.Header.Set("Resource-Service", "trie-trunk")
							r.Header.Set("Resource-Name", "queries")
							r.Header.Set("Resource-Type", "query")
						} else {
							r.Header.Set("Trunk-Branch", "1")
						}
						r.Header.Set("Content-Type", "application/pil")
						response, err := client.Do(r)
						if err != nil {
							t.Fatal(err)
						}
						defer response.Body.Close()
						raw, err := io.ReadAll(response.Body)
						if err != nil {
							t.Fatal(err)
						}
						if response.StatusCode != want {
							t.Fatalf("%s: %d %s", path, response.StatusCode, raw)
						}
						if path == "/definitions" && response.Header.Get("Trunk-Branch") != "1" {
							t.Fatal("missing registered branch", response.Header)
						}
						if protocol == "http2" && response.ProtoMajor != 2 || protocol == "http3" && response.ProtoMajor != 3 {
							t.Fatal(response.Proto)
						}
						return raw
					}
					request("PUT", "/definitions", fixture("query.rx"), 204)
					request("POST", "/demo/trie-trunk/queries/query/seeds", fixture("0.pil"), 204)
					raw := request("GET", fmt.Sprintf("/demo/trie-trunk/queries/query/packs?beginTime=%d&endTime=%d", hour, hour+3600000), nil, 200)
					if len(raw) == 0 {
						t.Fatal("live Pack missing")
					}
				})
			}
		})
	}
}
