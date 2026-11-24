package cache

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
	"trie-trunk/go/internal/node"
)

func TestHTTPAndSocketShareOneNodeAndPublication(t *testing.T) {
	for _, storage := range []string{"pack", "browse"} {
		t.Run(storage, func(t *testing.T) {
			root := t.TempDir()
			t.Cleanup(func() { taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie")) })
			fixture := func(name string) []byte {
				t.Helper()
				b, e := os.ReadFile("../../../testdata/http-ingress/" + name)
				if e != nil {
					t.Fatal(e)
				}
				return b
			}
			if e := RegisterSeedDefinition(root, fixture("query.rx"), SeedRegistration{Cache: "demo", Service: "trie-trunk", Resource: "queries", ResourceType: "query", File: "query.rx"}); e != nil {
				t.Fatal(e)
			}
			const hour = uint64(1794830400000)
			clock := hour + 3000
			endpoint, e := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "test", Storage: storage, Now: func() time.Time { return time.UnixMilli(int64(clock)) }})
			if e != nil {
				t.Fatal(e)
			}
			defer endpoint.Close()
			owner, e := node.Open(root, node.Config{Storage: storage}, 0, clock)
			if e != nil {
				t.Fatal(e)
			}
			defer owner.Close()
			if owner != endpoint.node {
				t.Fatal("transports did not share the node")
			}
			// A short private path also works on macOS's 104-byte Unix address limit.
			sockets, e := os.MkdirTemp("/tmp", "trunk-transport-")
			if e != nil {
				t.Fatal(e)
			}
			defer os.RemoveAll(sockets)
			socket := filepath.Join(sockets, "node.sock")
			service, e := owner.Serve(socket)
			if e != nil {
				t.Fatal(e)
			}
			defer service.Close()
			post := httptest.NewRequest("POST", "/demo/trie-trunk/queries/query/seeds", bytes.NewReader(fixture("0.pil")))
			post.Header.Set("Content-Type", "application/pil")
			post.Header.Set("Trunk-Branch", "1")
			accepted := httptest.NewRecorder()
			endpoint.ServeHTTP(accepted, post)
			if accepted.Code != 204 {
				t.Fatal(accepted.Code, accepted.Body.String())
			}
			query := func() {
				t.Helper()
				stream, e := net.Dial("unix", socket)
				if e != nil {
					t.Fatal(e)
				}
				defer stream.Close()
				stream.SetDeadline(time.Now().Add(5 * time.Second))
				raw, _ := json.Marshal(map[string]any{"op": "query", "branch": 1, "from": hour, "to": hour + 3600000, "query": map[string]any{}})
				var size [4]byte
				binary.LittleEndian.PutUint32(size[:], uint32(len(raw)))
				if _, e = stream.Write(append(size[:], raw...)); e != nil {
					t.Fatal(e)
				}
				if _, e = io.ReadFull(stream, size[:]); e != nil {
					t.Fatal(e)
				}
				length := binary.LittleEndian.Uint32(size[:])
				if length > 8<<20 {
					t.Fatal("unbounded response")
				}
				raw = make([]byte, length)
				if _, e = io.ReadFull(stream, raw); e != nil {
					t.Fatal(e)
				}
				var result struct {
					OK    bool
					Error string
					Value struct{ Rows []json.RawMessage }
				}
				if e = json.Unmarshal(raw, &result); e != nil || !result.OK || len(result.Value.Rows) != 1 {
					t.Fatal(string(raw), e)
				}
			}
			query()
			live, e := owner.Snapshot(1, [][2]uint64{{hour, hour + 3600000}}, false, nil, clock)
			if e != nil {
				t.Fatal(e)
			}
			defer live.Close()
			before := bytes.Clone(live.Bytes(4, 0))
			if len(before) == 0 {
				t.Fatal("live Pack missing")
			}
			update := httptest.NewRequest("PUT", "/definitions", bytes.NewReader(fixture("query.rx")))
			update.Header.Set("Content-Type", "application/pil")
			update.Header.Set("Trunk-Branch", "1")
			updated := httptest.NewRecorder()
			endpoint.ServeHTTP(updated, update)
			if updated.Code != 204 {
				t.Fatal(updated.Code, updated.Body.String())
			}
			reused, e := owner.Snapshot(1, [][2]uint64{{hour, hour + 3600000}}, false, &live.Generation, clock)
			if e != nil {
				t.Fatal(e)
			}
			if reused.Published || reused.Generation != live.Generation || !bytes.Equal(before, reused.Bytes(4, 0)) {
				t.Fatal("unchanged RX invalidated a live publication")
			}
			reused.Close()
			clock = hour + 3600000 + 900000
			if e = endpoint.flushRegistered(clock); e != nil {
				t.Fatal(e)
			}
			if !bytes.Equal(before, live.Bytes(4, 0)) {
				t.Fatal("commit invalidated a reader's live snapshot")
			}
			after, e := owner.Snapshot(1, [][2]uint64{{hour, hour + 3600000}}, false, &live.Generation, clock)
			if e != nil {
				t.Fatal(e)
			}
			defer after.Close()
			if !after.Published || after.Live != 0 || after.Generation == live.Generation {
				t.Fatal("published/live handoff failed")
			}
			query()
			request := httptest.NewRequest("GET", fmt.Sprintf("/demo/trie-trunk/queries/query/packs?beginTime=%d&endTime=%d", hour, hour+3600000), nil)
			request.Header.Set("Trunk-Branch", "1")
			read := httptest.NewRecorder()
			endpoint.ServeHTTP(read, request)
			if read.Code != 200 || read.Body.Len() == 0 {
				t.Fatal(read.Code, read.Body.String())
			}
		})
	}
}
