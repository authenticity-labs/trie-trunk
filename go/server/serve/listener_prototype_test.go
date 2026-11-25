//go:build trunk_prototype

package serve

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/quic-go/quic-go/http3"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"trie-trunk/go/server/search"
)

func TestHTTP2HTTP3AndSocket(t *testing.T) {
	selection, _ := events(t)
	serverTLS, clientTLS := credentials(t)
	socket := search.SocketPath(selection.Root)
	web := t.TempDir()
	if err := os.Mkdir(filepath.Join(web, "assets"), 0755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"index.html": "<!doctype html><title>trie-trunk</title>", "assets/app.js": "document.title='trie-trunk';"} {
		if err := os.WriteFile(filepath.Join(web, name), []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	address, stop := start(t, Config{Root: selection.Root, Web: web, Address: "127.0.0.1:0", Socket: socket, TLS: serverTLS, Token: "test-token"})
	var expected bytes.Buffer
	if err := search.Run(context.Background(), selection, &expected); err != nil {
		t.Fatal(err)
	}
	for _, pretty := range []bool{false, true} {
		selection.Pretty = pretty
		expected.Reset()
		if err := search.Run(context.Background(), selection, &expected); err != nil {
			t.Fatal(err)
		}
		for _, connection := range []search.Connection{{Socket: socket}, {Endpoint: "https://" + address, TLS: clientTLS, Token: "test-token"}} {
			var output bytes.Buffer
			if err := search.Pull(context.Background(), selection, connection, &output); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(output.Bytes(), expected.Bytes()) {
				t.Fatalf("stream differs: %s", output.String())
			}
		}
	}
	selection.Pretty = false
	compact := selection
	compact.Compact = true
	var dictionary bytes.Buffer
	if err := search.Run(context.Background(), compact, &dictionary); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(selection)
	for _, version := range []int{2, 3} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			var transport http.RoundTripper
			if version == 2 {
				tr := &http.Transport{TLSClientConfig: clientTLS, ForceAttemptHTTP2: true}
				transport = tr
				defer tr.CloseIdleConnections()
			} else {
				tr := &http3.Transport{TLSClientConfig: clientTLS}
				transport = tr
				defer tr.Close()
			}
			client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
			tapsRequest, _ := http.NewRequest(http.MethodGet, "https://"+address+"/caches", nil)
			tapsRequest.Header.Set("Authorization", "Bearer test-token")
			tapsResponse, err := client.Do(tapsRequest)
			if err != nil {
				t.Fatal(err)
			}
			tapsBody, err := io.ReadAll(tapsResponse.Body)
			tapsResponse.Body.Close()
			if err != nil || tapsResponse.StatusCode != 200 || tapsResponse.ProtoMajor != version || string(tapsBody) != "{\"caches\":[\"test\"]}\n" {
				t.Fatal(tapsResponse.Status, string(tapsBody), err)
			}
			for _, test := range []struct {
				method string
				path   string
				status int
				body   string
				typeOf string
			}{
				{http.MethodGet, "/", 200, "<!doctype html><title>trie-trunk</title>", "text/html"},
				{http.MethodGet, "/assets/app.js", 200, "document.title='trie-trunk';", "javascript"},
				{http.MethodHead, "/", 200, "", "text/html"},
				{http.MethodGet, "/assets/missing.js", 404, "404 page not found\n", "text/plain"},
				{http.MethodPost, "/", 405, "method not allowed\n", "text/plain"},
			} {
				req, _ := http.NewRequest(test.method, "https://"+address+test.path, nil)
				response, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || response.StatusCode != test.status || response.ProtoMajor != version || string(body) != test.body || !strings.Contains(response.Header.Get("Content-Type"), test.typeOf) {
					t.Fatalf("%s %s: proto=%s status=%d body=%s content-type=%s error=%v", test.method, test.path, response.Proto, response.StatusCode, body, response.Header.Get("Content-Type"), err)
				}
			}
			req, _ := http.NewRequest(http.MethodPost, "https://"+address+"/events", bytes.NewReader(data))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer test-token")
			response, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil || response.StatusCode != 200 || response.ProtoMajor != version || response.Trailer.Get("Trunk-Status") != "ok" {
				t.Fatalf("proto=%s status=%d body=%s error=%v trailer=%v", response.Proto, response.StatusCode, body, err, response.Trailer)
			}
			if version == 2 && !strings.Contains(response.Header.Get("Alt-Svc"), "h3=") {
				t.Fatal("no HTTP/3 advertisement")
			}
			negotiateRequest, _ := http.NewRequest(http.MethodPost, "https://"+address+"/schemas", strings.NewReader(`{"tap":"test"}`))
			negotiateRequest.Header.Set("Content-Type", "application/json")
			negotiateRequest.Header.Set("Authorization", "Bearer test-token")
			negotiateRequest.Header.Set("Accept", "application/json")
			negotiateResponse, err := client.Do(negotiateRequest)
			if err != nil {
				t.Fatal(err)
			}
			var negotiation schemaResponse
			err = json.NewDecoder(negotiateResponse.Body).Decode(&negotiation)
			negotiateResponse.Body.Close()
			if err != nil || negotiateResponse.StatusCode != 200 || negotiateResponse.ProtoMajor != version || len(negotiation.Schemas) != 1 || negotiation.Schemas[0].Dictionary == nil {
				t.Fatal(negotiation, err)
			}
			compact.Negotiation = negotiation.Negotiation
			compactData, err := json.Marshal(compact)
			if err != nil {
				t.Fatal(err)
			}
			compactRequest, _ := http.NewRequest(http.MethodPost, "https://"+address+"/events", bytes.NewReader(compactData))
			compactRequest.Header.Set("Content-Type", "application/json")
			compactRequest.Header.Set("Authorization", "Bearer test-token")
			compactRequest.Header.Set("Accept", search.DictionaryJSONL)
			compactResponse, err := client.Do(compactRequest)
			if err != nil {
				t.Fatal(err)
			}
			compactBody, err := io.ReadAll(compactResponse.Body)
			compactResponse.Body.Close()
			if err != nil || compactResponse.ProtoMajor != version || compactResponse.StatusCode != 200 || compactResponse.Header.Get("Content-Type") != search.DictionaryJSONL || compactResponse.Trailer.Get("Trunk-Status") != "ok" || !bytes.Equal(compactBody, dictionary.Bytes()) {
				t.Fatalf("dictionary stream mismatch: %s %s %v", compactResponse.Status, compactBody, err)
			}
			req, _ = http.NewRequest(http.MethodPost, "https://"+address+"/events", bytes.NewReader(data))
			req.Header.Set("Content-Type", "application/json")
			response, err = client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != 401 {
				t.Fatal(response.Status)
			}
		})
	}
	unauthorized := clientTLS.Clone()
	unauthorized.Certificates = nil
	err := search.Pull(context.Background(), selection, search.Connection{Endpoint: "https://" + address, TLS: unauthorized, Token: "test-token"}, io.Discard)
	if err == nil {
		t.Fatal("missing client certificate accepted")
	}
	stat, err := os.Stat(socket)
	if err != nil || stat.Mode().Perm() != 0600 {
		t.Fatal(stat, err)
	}
	stop()
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Fatal("socket was not removed", err)
	}
}

func TestFollowDisconnectAndShutdown(t *testing.T) {
	selection, _ := events(t)
	tlsConfig, _ := credentials(t)
	socket := search.SocketPath(selection.Root)
	_, stop := start(t, Config{Root: selection.Root, Address: "127.0.0.1:0", Socket: socket, TLS: tlsConfig, MaxQueries: 1})
	selection.Follow = true
	selection.End = time.UnixMilli(1 << 48)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	read, write := io.Pipe()
	defer read.Close()
	done := make(chan error, 1)
	go func() {
		err := search.Pull(ctx, selection, search.Connection{Socket: socket}, write)
		write.CloseWithError(err)
		done <- err
	}()
	decoder := json.NewDecoder(read)
	for i := 0; i < 3; i++ {
		var record map[string]any
		if err := decoder.Decode(&record); err != nil {
			t.Fatal(err)
		}
	}
	selection.Follow = false
	selection.End = selection.Start.Add(time.Hour)
	if err := search.Pull(context.Background(), selection, search.Connection{Socket: socket}, io.Discard); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatal("follower did not hold query slot", err)
	}
	cancel()
	read.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancellation lost")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("follow did not stop")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := search.Pull(context.Background(), selection, search.Connection{Socket: socket}, io.Discard)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("slot leaked", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	selection.Follow = true
	selection.End = time.UnixMilli(1 << 48)
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go func() { done <- search.Pull(ctx, selection, search.Connection{Socket: socket}, io.Discard) }()
	time.Sleep(100 * time.Millisecond)
	stop()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("shutdown reported completed stream")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown left follower running")
	}
}
