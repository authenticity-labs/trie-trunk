package serve

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"trie-trunk/go/codec"
	"trie-trunk/go/server/cache"
	"trie-trunk/go/server/search"
	"trie-trunk/go/server/store"
)

func credentials(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	pair := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	return &tls.Config{Certificates: []tls.Certificate{pair}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13}, &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS13}
}

func events(t *testing.T) (search.Config, *store.Manager) {
	t.Helper()
	root := shortDirectory(t)
	at := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	definition := codec.Definition{Version: 42, Timezone: "UTC", Aggregations: []string{"count", "duration.count", "duration.sum", "duration.min", "duration.max"}, Fields: codec.Schema{8: {Name: "id", Encoding: "uuidv7"}, 12: {Name: "duration", Unit: "us"}, 15: {Name: "message"}}}
	rev, data, err := definition.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	writer, err := store.Open(store.Config{Root: root, Tap: "test", Now: func() time.Time { return at }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { writer.Close() })
	if err := writer.Install(rev, data); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		id, err := codec.NewUUIDv7(at.Add(time.Duration(i) * time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		record, err := codec.MarshalCompact(codec.Record{8: id, 12: uint64(2234), 15: fmt.Sprintf("event %d", i)}, codec.PIL)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = writer.Append(rev, record); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	return search.Config{Root: root, Tap: "test", Start: at, End: at.Add(time.Hour)}, writer
}

func start(t *testing.T, c Config) (string, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan string, 1)
	done := make(chan error, 1)
	c.Ready = func(addr string) { ready <- addr }
	go func() { done <- Run(ctx, c) }()
	var address string
	select {
	case address = <-ready:
	case err := <-done:
		cancel()
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("listener startup timeout")
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("listener shutdown timeout")
		}
	}
	t.Cleanup(stop)
	return address, stop
}

func TestSocketDoesNotReplaceFilesOrListeners(t *testing.T) {
	dir := filepath.Join(shortDirectory(t), "private")
	name := filepath.Join(dir, "search.sock")
	listener, err := listenUnix(name)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if second, err := listenUnix(name); err == nil {
		second.Close()
		t.Fatal("replaced active listener")
	}
	listener.Close()
	if err := os.WriteFile(name, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := listenUnix(name); err == nil {
		t.Fatal("replaced regular file")
	}
	data, _ := os.ReadFile(name)
	if string(data) != "keep" {
		t.Fatal("changed regular file")
	}
}

func shortDirectory(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "trunk-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestLoopbackHTTPOrigin(t *testing.T) {
	for _, token := range []string{"", "test-token"} {
		t.Run("token="+token, func(t *testing.T) {
			root := shortDirectory(t)
			rx, err := os.ReadFile("../../../testdata/http-ingress/query.rx")
			if err != nil {
				t.Fatal(err)
			}
			if err = cache.RegisterSeedDefinition(root, rx, cache.SeedRegistration{Cache: "demo", Service: "lab", Resource: "events", ResourceType: "query", File: "query.rx"}); err != nil {
				t.Fatal(err)
			}
			endpoint, err := cache.OpenEndpoint(cache.Config{Root: root, Cache: "demo", Tenant: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer endpoint.Close()
			serverTLS, clientTLS := credentials(t)
			web := t.TempDir()
			if err := os.WriteFile(filepath.Join(web, "index.html"), []byte("<title>snoot</title>"), 0644); err != nil {
				t.Fatal(err)
			}
			ready := make(chan string, 1)
			address, stop := start(t, Config{
				Root: root, API: endpoint.Handler(token), Web: web, Address: "127.0.0.1:0", Socket: search.SocketPath(root),
				TLS: serverTLS, Token: token, HTTPAddress: "127.0.0.1:0", HTTPReady: func(a string) { ready <- a },
			})
			origin := <-ready
			tr := &http.Transport{}
			defer tr.CloseIdleConnections()
			client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
			request := func(path, auth, etag string) ([]byte, *http.Response) {
				t.Helper()
				req, _ := http.NewRequest("GET", "http://"+origin+path, nil)
				if etag != "" {
					req.Header.Set("If-None-Match", etag)
				}
				if auth != "" {
					req.Header.Set("Authorization", "Bearer "+auth)
				}
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				if resp.Header.Get("Alt-Svc") != "" {
					t.Fatal("HTTP origin leaked direct QUIC address")
				}
				return body, resp
			}
			body, resp := request("/", "", "")
			if resp.StatusCode != 200 || string(body) != "<title>snoot</title>" {
				t.Fatalf("static: %s %s", resp.Status, body)
			}
			if token != "" {
				for _, auth := range []string{"", "wrong"} {
					_, resp = request("/caches", auth, "")
					if resp.StatusCode != 401 {
						t.Fatalf("missing auth check: %s", resp.Status)
					}
				}
			}
			body, resp = request("/caches", token, "")
			if resp.StatusCode != 200 || !bytes.Contains(body, []byte(`"demo/lab/events/query"`)) {
				t.Fatalf("taps: %s %s", resp.Status, body)
			}
			body, resp = request("/demo/definitions?file=query.rx", token, "")
			etag := resp.Header.Get("ETag")
			if resp.StatusCode != 200 || etag == "" {
				t.Fatal(resp.Status, resp.Header)
			}
			published, err := os.ReadFile(filepath.Join(root, "definitions", "query.rx"))
			if err != nil || !bytes.Equal(body, published) {
				t.Fatal("incorrect registered RX", err)
			}
			body, resp = request("/demo/definitions?file=query.rx", token, etag)
			if resp.StatusCode != 304 || len(body) != 0 {
				t.Fatal("unchanged RX transferred", resp.Status, len(body))
			}
			// The optional HTTP origin must not change TLS client authentication.
			noIdentity := clientTLS.Clone()
			noIdentity.Certificates = nil
			tlsTransport := &http.Transport{TLSClientConfig: noIdentity}
			defer tlsTransport.CloseIdleConnections()
			if r, err := (&http.Client{Transport: tlsTransport, Timeout: 5 * time.Second}).Get("https://" + address + "/"); err == nil {
				r.Body.Close()
				t.Fatal("TLS listener accepted a missing client certificate")
			}
			stop()
			if c, err := net.DialTimeout("tcp", origin, time.Second); err == nil {
				c.Close()
				t.Fatal("HTTP origin still listening after shutdown")
			}
		})
	}
}

func TestHTTPOriginRejectsNonLoopback(t *testing.T) {
	serverTLS, _ := credentials(t)
	for _, address := range []string{":3020", "0.0.0.0:3020", "[::]:3020", "192.168.1.2:3020", "localhost:3020", "127.0.0.1"} {
		err := Run(context.Background(), Config{TLS: serverTLS, Socket: "unused", HTTPAddress: address})
		if err == nil || !strings.Contains(err.Error(), "loopback") {
			t.Fatalf("%q: %v", address, err)
		}
	}
}

func TestSocketRecoversDeadListener(t *testing.T) {
	name := filepath.Join(shortDirectory(t), "private", "search.sock")
	first, err := listenUnix(name)
	if err != nil {
		t.Fatal(err)
	}
	first.SetUnlinkOnClose(false)
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := listenUnix(name)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	connection, err := net.Dial("unix", name)
	if err != nil {
		t.Fatal(err)
	}
	connection.Close()
}
