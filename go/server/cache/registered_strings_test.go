package cache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Set TRUNK_TEST_STRINGS_WASM=1 to run the replica against the same live HTTP handler.
func TestSparseBoxStrings(t *testing.T) {
	for _, mode := range []string{"pack", "browse"} {
		t.Run(mode, func(t *testing.T) {
			const hour = int64(1794830400000)
			const path = "/demo/lab/readings/sample/"
			root := t.TempDir()
			doc, e := seedNativeDocument([]byte(`[{"header":{"rx-name":"sample"},"fields":[{"name":"time","type":[1,0],"size":6,"annotations":["@time"]},{"name":"id","type":[4,0],"size":10,"annotations":["@event"]},{"name":"message","type":[5,0],"size":0}]}]`))
			if e != nil {
				t.Fatal(e)
			}
			defer doc.close()
			d, e := seedNativeRegister(doc.bytes)
			if e != nil {
				t.Fatal(e)
			}
			defer d.close()
			if e = RegisterSeedDefinition(root, doc.bytes, SeedRegistration{Cache: "demo", Service: "lab", Resource: "readings", ResourceType: "sample", File: "sample.rx"}); e != nil {
				t.Fatal(e)
			}
			const training = hour - 30*24*TimeHourMS
			now := time.UnixMilli(training + 100)
			p, e := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "local", Storage: mode, Now: func() time.Time { return now }})
			if e != nil {
				t.Fatal(e)
			}
			defer p.Close()
			// Include rare repeats in a tiny hour and alongside a profitable
			// string. Neither should acquire an ordinal merely from two hits.
			words := [][]string{{}, {}, {"first value"}, {"rare small hour", "rare small hour"}}
			for i := 0; i < 16; i++ {
				words[0] = append(words[0], "first value")
				words[1] = append(words[1], "unselected hour")
				words[2] = append(words[2], "later value")
			}
			words[0] = append(words[0], "rare in busy hour", "rare in busy hour")
			publish := func(at int64, rows []string) {
				t.Helper()
				now = time.UnixMilli(at + 100)
				for i, s := range rows {
					pill, e := d.ingest([]seedNativeValue{{ordinal: 0, kind: seedUnsigned, low: uint64(at) + uint64(i)}, {ordinal: 1, kind: seedBytes, bytes: bytes.Repeat([]byte{byte(i + 1)}, 10)}, {ordinal: 2, kind: seedText, bytes: []byte(s)}}, true)
					if e != nil {
						t.Fatal(e)
					}
					r := httptest.NewRequest("POST", path+"seeds", bytes.NewReader(pill.bytes))
					r.Header.Set("Content-Type", "application/pil")
					r.Header.Set("Trunk-Branch", "1")
					w := httptest.NewRecorder()
					p.ServeHTTP(w, r)
					pill.close()
					if w.Code != 204 {
						t.Fatal(w.Code, w.Body.String())
					}
				}
				now = time.UnixMilli(at + TimeHourMS + 900000)
				if e = p.flushRegistered(uint64(now.UnixMilli())); e != nil {
					t.Fatal(e)
				}
			}
			var history []string
			for i := 0; i < 16; i++ {
				history = append(history, "first value", "unselected hour", "later value")
			}
			history = append(history, "discard one", "discard two", "discard three")
			publish(training, history)
			publish(hour-TimeHourMS, history)
			get := func(op, g string) *httptest.ResponseRecorder {
				r := httptest.NewRequest("GET", path+op, nil)
				r.Header.Set("Trunk-Branch", "1")
				r.Header.Set("Trunk-Generation", g)
				w := httptest.NewRecorder()
				p.ServeHTTP(w, r)
				return w
			}
			early := get(fmt.Sprintf("packs?range=%d,%d", training, training+TimeHourMS), "")
			if early.Code != 200 {
				t.Fatal(early.Code, early.Body.String())
			}
			if resp := get("strings?id=0", early.Header().Get("Trunk-Generation")); resp.Code != 404 {
				t.Fatalf("dictionary before 30 days: %d %s", resp.Code, resp.Body)
			}
			for h, rows := range words {
				publish(hour+int64(h)*TimeHourMS, rows)
			}
			wire := get(fmt.Sprintf("packs?range=%d,%d", hour, hour+TimeHourMS), "")
			if wire.Code != 200 {
				t.Fatal(wire.Code, wire.Body.String())
			}
			g := wire.Header().Get("Trunk-Generation")
			selected := get("strings?id=2&id=0", g)
			if mode == "pack" {
				if w := get("strings?id=0", g); w.Code != 404 {
					t.Fatalf("unprofitable compressed entry %d %s", w.Code, w.Body.String())
				}
			} else if selected.Code != 200 || !bytes.Equal(selected.Body.Bytes(), []byte(`[[2,"later value"],[0,"first value"]]`)) {
				t.Fatalf("strings %d %s", selected.Code, selected.Body)
			}
			for _, c := range []struct {
				q, g   string
				status int
			}{{"strings?id=0", "wrong", 412}, {"strings?id=3", g, 404}, {"strings?id=0&id=0", g, 400}, {"strings", g, 400}} {
				if w := get(c.q, c.g); w.Code != c.status {
					t.Fatal(c, w.Code)
				}
			}
			if os.Getenv("TRUNK_TEST_STRINGS_WASM") == "1" {
				server := httptest.NewServer(http.HandlerFunc(p.ServeHTTP))
				defer server.Close()
				route, e := taxonomyRoute(root, 1)
				if e != nil {
					t.Fatal(e)
				}
				config, _ := json.Marshal([]string{server.URL, root, filepath.Join(root, route.Storage.Directory, route.Storage.Stem+".time"), mode})
				cmd := exec.Command("node", "--import", "../../../../snoot/client/scripts/test-types.mjs", "../../../../snoot/client/scripts/test-box-strings.mjs", string(config))
				if output, e := cmd.CombinedOutput(); e != nil {
					t.Fatalf("replica: %v\n%s", e, output)
				} else {
					t.Log(string(output))
				}
			}
		})
	}
}
