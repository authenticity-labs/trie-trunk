package cache

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"trie-trunk/go/internal/seedstream"
)

func TestRegisteredObjectRevisionTransport(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(func() { taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie")) })
	const clock = 1800000010000
	fixture := func(name string) []byte {
		t.Helper()
		b, err := os.ReadFile("../../../testdata/object-revisions/" + name)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	target := SeedRegistration{Cache: "demo", Service: "lab", Resource: "meters", ResourceType: "meter", File: "meter.rx"}
	for _, v := range []string{"1.2", "1.3"} {
		if err := RegisterSeedDefinition(root, fixture("meter-"+v+".rx"), target); err != nil {
			t.Fatal(err)
		}
	}
	open := func() (*Endpoint, *httptest.Server) {
		t.Helper()
		p, err := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "local", Storage: "pack", Now: func() time.Time { return time.UnixMilli(clock) }})
		if err != nil {
			t.Fatal(err)
		}
		s := httptest.NewServer(p)
		return p, s
	}
	p, s := open()
	defer func() { s.Close(); p.Close() }()
	routes := map[uint32]registeredRoute{}
	rr, err := taxonomyReader(root)
	if err != nil {
		t.Fatal(err)
	}
	list, err := rr.routes("demo")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range list {
		routes[r.Minor] = r
	}
	if len(routes) != 2 {
		t.Fatalf("routes: %+v", routes)
	}
	const path = "/demo/lab/meters/meter/seeds"
	post := func(minor int, rev int, digest string, want int) {
		t.Helper()
		req, err := http.NewRequest("POST", s.URL+path, bytes.NewReader(fixture(fmt.Sprintf("meter-1.%d-r%d.pil", minor, rev))))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/pil")
		req.Header.Set("Trunk-Branch", strconv.Itoa(int(routes[uint32(minor)].Branch)))
		req.Header.Set("Definition-Version", digest)
		res, err := s.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode != want {
			body, _ := io.ReadAll(res.Body)
			t.Fatalf("POST status %d: %s", res.StatusCode, body)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	live, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(s.URL, "http")+
		"/demo/lab/meters/meter/roots?branch="+strconv.Itoa(int(routes[2].Branch)), &websocket.DialOptions{Subprotocols: []string{seedstream.Live}})
	if err != nil {
		t.Fatal(err)
	}
	defer live.CloseNow()
	post(2, 1, routes[3].Digest, 400)
	post(2, 1, routes[2].Digest, 204)
	kind, change, err := live.Read(ctx)
	first := fixture("meter-1.2-r1.pil")
	if err != nil || kind != websocket.MessageBinary || !bytes.Equal(change, append(binary.BigEndian.AppendUint32(nil, uint32(len(first))), first...)) {
		t.Fatalf("Roots subscription delivery: %x %v", change, err)
	}
	live.Close(websocket.StatusNormalClosure, "")
	header := http.Header{}
	header.Set("Trunk-Branch", strconv.Itoa(int(routes[3].Branch)))
	header.Set("Definition-Version", routes[3].Digest)
	ws, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(s.URL, "http")+path, &websocket.DialOptions{HTTPHeader: header, Subprotocols: []string{seedstream.Protocol}})
	if err != nil {
		t.Fatal(err)
	}
	defer ws.CloseNow()
	pil := fixture("meter-1.3-r2.pil")
	if err := ws.Write(ctx, websocket.MessageBinary, append(binary.BigEndian.AppendUint32(nil, uint32(len(pil))), pil...)); err != nil {
		t.Fatal(err)
	}
	kind, ack, err := ws.Read(ctx)
	if err != nil || kind != websocket.MessageBinary || !bytes.Equal(ack, []byte{seedstream.Durable}) {
		t.Fatalf("durable ACK %v %x %v", kind, ack, err)
	}
	ws.Close(websocket.StatusNormalClosure, "")
	s.Close()
	p.Close()
	p, s = open()
	post(2, 3, routes[2].Digest, 204)
	if err := p.flushRegistered(uint64(clock + 2*TimeHourMS)); err != nil {
		t.Fatal(err)
	}
	res, err := s.Client().Get(s.URL + "/demo/lab/meters/collections?type=meter&id=31313131-3131-3131-3131-313131313131&beginTime=0&endTime=1800007200000")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil || res.StatusCode != 200 {
		t.Fatalf("history %d: %s %v", res.StatusCode, body, err)
	}
	var history struct {
		Rows []struct {
			Definition string
			Revision   int
			Values     struct{ Object struct{ Reading int } }
		}
	}
	if err := json.Unmarshal(body, &history); err != nil {
		t.Fatal(err)
	}
	if len(history.Rows) != 3 {
		t.Fatalf("history: %s", body)
	}
	for i, minor := range []uint32{2, 3, 2} {
		row := history.Rows[i]
		if row.Revision != i+1 || row.Values.Object.Reading != []int{-17, 90000, -17}[i] || row.Definition != routes[minor].Digest {
			t.Fatalf("revision %d: %+v", i+1, row)
		}
	}
}
