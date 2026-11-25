//go:build trunk_prototype

package serve

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	pilcodec "github.com/authenticity-labs/pill-packer/go/codec"
	"github.com/quic-go/quic-go/http3"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"
	"trie-trunk/go/codec"
	"trie-trunk/go/logger/pil"
	"trie-trunk/go/server/cache"
)

func TestObjectHistoryHTTP2HTTP3AndSocket(t *testing.T) {
	root := shortDirectory(t)
	l, err := cache.Compile(cache.Definition{PIL: 0, Identity: "customer/accounts", Version: 1 << 20,
		Fields:   []cache.Field{{Name: "event_id", Type: "UUID"}, {Name: "id", Type: "UUID"}, {Name: "state", Type: "STRING"}, {Name: "revision_id", Type: "U16"}},
		Resource: cache.Binding{Format: cache.Format, Service: "customer", Resource: "accounts", Major: 1, Timezone: "UTC", Roles: cache.Roles{Event: "event_id", Object: "id", Revision: "revision_id"}}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := cache.Config{Root: root, Cache: "demo", Tenant: "test", Layout: l, ObjectIndex: true}
	s, err := cache.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	id, err := codec.NewUUIDv7(at)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := l.Encode(nil, []cache.Value{{State: cache.Direct, Bytes: id[:]}, {State: cache.Direct, Bytes: id[:]}, cache.Text("active"), cache.Uint(1, 2)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Append(cache.Write, raw); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	// Sealing retains timestamp + ordinal, rather than ingress UUID entropy.
	storedID := id
	clear(storedID[6:])
	storedID[6], storedID[8] = 0x70, 0x80
	expected := make([]cache.Value, len(l.Definition.Fields))
	expected[l.Event] = cache.Value{State: cache.Direct, Bytes: storedID[:]}
	expected[l.Object] = cache.Value{State: cache.Direct, Bytes: id[:]}
	expected[2], expected[l.Revision] = cache.Text("active"), cache.Uint(1, 2)
	expected[l.Record], expected[l.Leaf] = cache.Uint(0, 3), cache.Uint(0, 2)
	raw, err = l.Encode(nil, expected)
	if err != nil {
		t.Fatal(err)
	}
	p, err := cache.OpenEndpoint(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	serverTLS, clientTLS := credentials(t)
	socket := filepath.Join(root, "history.sock")
	address, stop := start(t, Config{Root: root, Address: "127.0.0.1:0", Socket: socket, TLS: serverTLS, API: p.Handler("secret")})
	defer stop()
	h2 := &http.Transport{TLSClientConfig: clientTLS.Clone(), ForceAttemptHTTP2: true}
	defer h2.CloseIdleConnections()
	h3 := &http3.Transport{TLSClientConfig: clientTLS.Clone()}
	defer h3.Close()
	unix := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer unix.CloseIdleConnections()
	for _, tc := range []struct {
		name, origin string
		transport    http.RoundTripper
		major        int
	}{
		{"http2", "https://" + address, h2, 2}, {"http3", "https://" + address, h3, 3}, {"socket", "http://localhost", unix, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &http.Client{Transport: tc.transport, Timeout: 10 * time.Second}
			query := fmt.Sprintf("id=%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:])
			r, _ := http.NewRequest("GET", tc.origin+"/demo/customer/accounts.v1/collections?"+query+"&beginTime=1788220800000&endTime=1788307200000", nil)
			r.Header.Set("Authorization", "Bearer secret")
			r.Header.Set("Accept", "application/pil")
			r.Header.Set("PIL-Version", "2")
			res, err := client.Do(r)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(res.Body)
			res.Body.Close()
			if err != nil || res.StatusCode != 200 || res.ProtoMajor != tc.major || !bytes.Contains(body, raw) || !bytes.HasSuffix(body, []byte{255, 254}) {
				t.Fatalf("history: %v %v %x", res, err, body)
			}
			token := res.Header.Get("Trunk-State")
			r, _ = http.NewRequest("GET", tc.origin+"/demo/customer/accounts.v1/collections?kind=state&"+query+"&token="+token, nil)
			r.Header.Set("Authorization", "Bearer secret")
			res, err = client.Do(r)
			if err != nil {
				t.Fatal(err)
			}
			var state struct {
				Token   string
				Current bool
			}
			err = json.NewDecoder(res.Body).Decode(&state)
			res.Body.Close()
			if err != nil || res.StatusCode != 200 || state.Token != token || !state.Current {
				t.Fatalf("state: %+v %v", state, err)
			}
		})
	}
}

func TestNativeCacheHTTP2HTTP3AndSocket(t *testing.T) {
	root := shortDirectory(t)
	l, e := cache.Compile(cache.Definition{PIL: 1, AdaptiveIntegers: "32", Identity: "payment/transactions", Version: 1 << 20, Fields: []cache.Field{{Name: "event_id", Type: "UUID"}, {Name: "amount", Type: "U64"}}, Resource: cache.Binding{Format: cache.Format, Service: "payment", Resource: "transactions", Major: 1, Timezone: "UTC", Roles: cache.Roles{Event: "event_id"}}})
	if e != nil {
		t.Fatal(e)
	}
	s, e := cache.Open(cache.Config{Root: root, Cache: "demo", Tenant: "test", Layout: l})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	uuid, e := codec.NewUUIDv7(time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC))
	if e != nil {
		t.Fatal(e)
	}
	frame, e := l.Encode(nil, []cache.Value{{State: cache.Direct, Bytes: uuid[:]}, cache.Uint(9, 8)})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Append(cache.Write, frame); e != nil {
		t.Fatal(e)
	}
	serverTLS, clientTLS := credentials(t)
	socket := filepath.Join(root, "native.sock")
	address, stop := start(t, Config{Root: root, Address: "127.0.0.1:0", Socket: socket, TLS: serverTLS, API: &cache.HTTP{Store: s}})
	defer stop()
	h2 := &http.Transport{TLSClientConfig: clientTLS.Clone(), ForceAttemptHTTP2: true}
	defer h2.CloseIdleConnections()
	h3 := &http3.Transport{TLSClientConfig: clientTLS.Clone()}
	defer h3.Close()
	unix := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer unix.CloseIdleConnections()
	for _, tc := range []struct {
		name, url string
		transport http.RoundTripper
		major     int
	}{{"http2", "https://" + address, h2, 2}, {"http3", "https://" + address, h3, 3}, {"socket", "http://localhost", unix, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			current := time.Now()
			eventID, e := codec.NewUUIDv7(current)
			if e != nil {
				t.Fatal(e)
			}
			recent, e := l.Encode(nil, []cache.Value{{State: cache.Direct, Bytes: eventID[:]}, cache.Uint(13, 8)})
			if e != nil {
				t.Fatal(e)
			}
			req, _ := http.NewRequest("POST", tc.url+"/ingest?tap=demo/payment/transactions.v1/write", bytes.NewReader(recent))
			req.Header.Set("Content-Type", "application/pil")
			req.Header.Set("PIL-Version", "2")
			req.Header.Set("PIL-Features", "adaptive-integers,pac-arrays,native-ranges")
			req.Header.Set("Definition-Version", fmt.Sprintf("%x", l.Digest))
			client := &http.Client{Transport: tc.transport, Timeout: 10 * time.Second}
			res, e := client.Do(req)
			if e != nil {
				t.Fatal(e)
			}
			body, e := io.ReadAll(res.Body)
			res.Body.Close()
			if e != nil || res.StatusCode != 200 || res.ProtoMajor != tc.major || res.Header.Get("Trunk-Acknowledgement") != "durable-staging" {
				t.Fatalf("PIL ingress %v %v %s", res, e, body)
			}
			req, _ = http.NewRequest("GET", tc.url+"/demo/payment/transactions.v1/write/indexes?mode=search&page=50&beginTime="+fmt.Sprint(current.Add(-time.Minute).UnixMilli())+"&endTime="+fmt.Sprint(current.Add(time.Minute).UnixMilli()), nil)
			req.Header.Set("Accept", "application/pil")
			req.Header.Set("PIL-Version", "2")
			req.Header.Set("PIL-Features", "adaptive-integers,pac-arrays,native-ranges")
			res, e = client.Do(req)
			if e != nil {
				t.Fatal(e)
			}
			body, e = io.ReadAll(res.Body)
			res.Body.Close()
			if e != nil || res.StatusCode != 200 || !viewContainsFrame(t, l, body, recent) {
				t.Fatalf("staged collection %v %s", e, body)
			}
			r, _ := http.NewRequest("GET", tc.url+"/demo/payment/transactions.v1/write/packs?beginTime=1788220800000&endTime=1788307200000", nil)
			r.Header.Set("Accept", "application/pil")
			r.Header.Set("Accept-Encoding", "gzip")
			r.Header.Set("PIL-Version", "2")
			r.Header.Set("PIL-Features", "adaptive-integers,pac-arrays,native-ranges")
			response, e := (&http.Client{Transport: tc.transport, Timeout: 10 * time.Second}).Do(r)
			if e != nil {
				t.Fatal(e)
			}
			defer response.Body.Close()
			body, e = io.ReadAll(response.Body)
			if e != nil || response.StatusCode != 200 || response.ProtoMajor != tc.major {
				t.Fatalf("%v %v %s", response, e, body)
			}
			packs, e := pilcodec.ParsePAC(nil, body, nil)
			if e != nil || len(packs.Members) != 1 || response.Header.Get("Trunk-Format") != "seed-packs/0" || response.Header.Get("Content-Encoding") != "" {
				t.Fatal("invalid seed transport", e, response.Header)
			}
			raw := packs.Members[0]
			unpacked, e := pilcodec.ExpandPAC(raw)
			if e != nil {
				t.Fatal(e)
			}
			p, e := pilcodec.ParsePAC(l.Layout, unpacked, map[uint32]bool{1: true, 0x5454524b: true})
			if e != nil || len(p.Members) != 1 {
				t.Fatal("invalid seed member", e)
			}

		})
	}
	t.Run("parallel-branch-streams", func(t *testing.T) {
		failures := make(chan error, 5)
		for _, branch := range []string{"read", "write", "error", "debug", "trace"} {
			go func(branch string) {
				url := "https://" + address + "/demo/payment/transactions.v1/" + branch + "/packs?range=1788220800000,1788307200000"
				response, e := (&http.Client{Transport: h3, Timeout: 10 * time.Second}).Get(url)
				if e != nil {
					failures <- e
					return
				}
				defer response.Body.Close()
				raw, e := io.ReadAll(response.Body)
				if e != nil {
					failures <- e
					return
				}
				if response.StatusCode != 200 || response.ProtoMajor != 3 || response.Header.Get("Trunk-Source") != "demo/payment/transactions.v1/"+branch {
					failures <- fmt.Errorf("branch %s: %d %s", branch, response.StatusCode, string(raw))
					return
				}
				_, e = pilcodec.ParsePAC(nil, raw, nil)
				failures <- e
			}(branch)
		}
		for i := 0; i < 5; i++ {
			if e := <-failures; e != nil {
				t.Error(e)
			}
		}
	})

}

func TestPILLoggerRegistrationTransports(t *testing.T) {
	for _, protocol := range []string{"http2", "http3", "socket"} {
		t.Run(protocol, func(t *testing.T) {
			root := shortDirectory(t)
			p, e := cache.OpenEndpoint(cache.Config{Root: root, Cache: "demo", Tenant: "test"})
			if e != nil {
				t.Fatal(e)
			}
			defer p.Close()
			serverTLS, clientTLS := credentials(t)
			socket := filepath.Join(root, "register.sock")
			address, stop := start(t, Config{Root: root, Address: "127.0.0.1:0", Socket: socket, TLS: serverTLS, API: p.Handler("secret")})
			defer stop()
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
			l, e := cache.Compile(cache.Definition{PIL: 1, AdaptiveIntegers: "16", Identity: "payment/transactions", Version: 1 << 20, Fields: []cache.Field{{Name: "event_id", Type: "UUID"}, {Name: "amount", Type: "U16"}, {Name: "body", Type: "STRING"}}, Resource: cache.Binding{Format: cache.Format, Service: "payment", Resource: "transactions", Major: 1, Timezone: "UTC", Roles: cache.Roles{Event: "event_id"}}})
			if e != nil {
				t.Fatal(e)
			}
			logger, e := pil.Open(context.Background(), pil.Config{URL: origin, Cache: "demo", Token: "secret", Definition: l.Bytes, HTTPClient: &http.Client{Transport: transport}})
			if e != nil {
				t.Fatal(e)
			}
			defer logger.Close()
			id, _ := codec.NewUUIDv7(time.Now())
			values := []pil.Value{{State: pil.Direct, Bytes: id[:]}, cache.Uint(23, 2), cache.Text(string(bytes.Repeat([]byte("request diagnostic; upstream unavailable. "), 128)))}
			raw, _ := l.Encode(nil, values)
			if e = logger.WriteValues(context.Background(), values); e != nil {
				t.Fatal(e)
			}
			request, _ := http.NewRequest("GET", origin+"/demo/payment/transactions.v1/write/indexes?mode=search&page=50&beginTime="+fmt.Sprint(time.Now().Add(-time.Hour).UnixMilli())+"&endTime="+fmt.Sprint(time.Now().Add(time.Hour).UnixMilli()), nil)
			request.Header.Set("Authorization", "Bearer secret")
			request.Header.Set("Accept", "application/pil")
			request.Header.Set("PIL-Version", "2")
			request.Header.Set("PIL-Features", "adaptive-integers,pac-arrays")
			response, e := (&http.Client{Transport: transport}).Do(request)
			if e != nil {
				t.Fatal(e)
			}
			body, e := io.ReadAll(response.Body)
			response.Body.Close()
			// Negotiated HTTP compression is removed at ingress. Collection
			// retains user values and event time inside its live PAC.
			if e != nil || response.StatusCode != 200 || !viewContainsFrame(t, l, body, raw) {
				t.Fatal("HTTP-compressed ingress round trip", e, response.StatusCode)
			}
			if e = logger.Register(context.Background()); e != nil {
				t.Fatal("warm registration", e)
			}
		})
	}
}

// Decode transport controls, retaining PACs for round-trip assertions.
func responsePACs(t *testing.T, l *cache.Layout, raw []byte) []*cache.PAC {
	t.Helper()
	if bytes.HasPrefix(raw, []byte{31, 139}) {
		z, e := gzip.NewReader(bytes.NewReader(raw))
		if e != nil {
			t.Fatal(e)
		}
		raw, e = io.ReadAll(z)
		z.Close()
		if e != nil {
			t.Fatal(e)
		}
	}
	if !bytes.HasPrefix(raw, []byte{'T', 'P', 'I', 'L', 2}) {
		t.Fatal("PIL response preamble")
	}
	r := bytes.NewReader(raw[5:])
	var out []*cache.PAC
	var rangeLeft uint64
	var e error
	for {
		tag := byte(7)
		if rangeLeft == 0 {
			tag, e = r.ReadByte()
			if e != nil {
				t.Fatal(e)
			}
		}
		if tag == 9 {
			var h [12]byte
			if _, e = io.ReadFull(r, h[:]); e != nil {
				t.Fatal(e)
			}
			rangeLeft = binary.LittleEndian.Uint64(h[4:])
			if rangeLeft == 0 || rangeLeft > uint64(r.Len()) {
				t.Fatal("invalid PAC range")
			}
			continue
		}
		if tag == 255 {
			end, e := r.ReadByte()
			if e != nil || end != 254 || r.Len() != 0 {
				t.Fatal("incomplete response")
			}
			return out
		}
		if tag == 7 {
			h := make([]byte, 4)
			if _, e = io.ReadFull(r, h); e != nil {
				t.Fatal(e)
			}
			n := uint64(binary.LittleEndian.Uint32(h))
			if n < uint64(len(h)) || n > cache.PACMaximum {
				t.Fatal("PAC size")
			}
			data := make([]byte, n)
			copy(data, h)
			if _, e = io.ReadFull(r, data[len(h):]); e != nil {
				t.Fatal(e)
			}
			if rangeLeft > 0 {
				if n > rangeLeft {
					t.Fatal("PAC exceeds range")
				}
				rangeLeft -= n
			}
			p, e := cache.ParsePAC(l, data, map[uint32]bool{cache.PACNative: true})
			if e != nil {
				t.Fatal(e)
			}
			out = append(out, p)
			continue
		}
		widths := map[byte][]int{1: {0, 0, 0}, 2: {4, 0, 0}, 4: {4, 0}, 5: {4, 1, 8, 0, 0}, 6: {4}, 8: {0}}[tag]
		if widths == nil {
			t.Fatal("unknown control", tag)
		}
		control := &pilcodec.Layout{Widths: widths}
		if _, e = control.ReadFrame(r, nil); e != nil {
			t.Fatal(e)
		}
	}
}
func viewContainsFrame(t *testing.T, l *cache.Layout, body, want []byte) bool {
	t.Helper()
	for _, p := range responsePACs(t, l, body) {
		for _, m := range p.Members {
			raw, e := cache.ResolvePAC(l, p.Common, m)
			if e != nil {
				t.Fatal(e)
			}
			var got, input cache.Frame
			if e = l.Scan(raw, &got); e != nil {
				t.Fatal(e)
			}
			if e = l.Scan(want, &input); e != nil {
				t.Fatal(e)
			}
			matches := bytes.Equal(got.Values[l.Time].Bytes, input.Values[l.Time].Bytes)
			for i, v := range input.Values {
				if i == l.Event || i == l.Record || i == l.Time || i == l.Leaf {
					continue
				}
				matches = matches && v.State == got.Values[i].State && bytes.Equal(v.Bytes, got.Values[i].Bytes)
			}
			if matches {
				id, ordinal := got.Values[l.Event].Bytes, got.Values[l.Record].Bytes
				if len(id) != 16 || len(ordinal) != 3 || !bytes.Equal(id[:6], input.Values[l.Event].Bytes[:6]) || id[6] != 0x70 || id[8] != 0x80 || id[13] != ordinal[2] || id[14] != ordinal[1] || id[15] != ordinal[0] {
					t.Fatal("invalid replicated ordinal identity")
				}
				return true
			}
		}
	}
	return false
}

func TestSummariesNativeRoute(t *testing.T) {
	root := shortDirectory(t)
	at := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	l, e := cache.Compile(cache.Definition{PIL: 0, Identity: "payment/transactions", Version: 1 << 20, Fields: []cache.Field{{Name: "event_id", Type: "UUID"}, {Name: "duration", Type: "U64"}}, Resource: cache.Binding{Format: cache.Format, Service: "payment", Resource: "transactions", Major: 1, Timezone: "UTC", Roles: cache.Roles{Event: "event_id", Duration: "duration"}, Aggregations: []string{"count", "duration.count", "duration.sum", "duration.min", "duration.max"}}})
	if e != nil {
		t.Fatal(e)
	}
	s, e := cache.Open(cache.Config{Root: root, Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return at.Add(73 * time.Hour) }})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	for i := 0; i < 3; i++ {
		id, e := codec.NewUUIDv7(at.Add(time.Duration(i) * 24 * time.Hour))
		if e != nil {
			t.Fatal(e)
		}
		frame, e := l.Encode(nil, []cache.Value{{State: cache.Direct, Bytes: id[:]}, cache.Uint(100, 8)})
		if e != nil {
			t.Fatal(e)
		}
		if _, e = s.Append(cache.Read, frame); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	serverTLS, clientTLS := credentials(t)
	address, stop := start(t, Config{Root: root, Socket: filepath.Join(root, "summary.sock"), Address: "127.0.0.1:0", TLS: serverTLS, API: &cache.HTTP{Store: s}})
	defer stop()
	transport := &http3.Transport{TLSClientConfig: clientTLS}
	defer transport.Close()
	url := fmt.Sprintf("https://%s/demo/payment/transactions.v1/read/summaries?definition=%x&range=%d,%d&range=%d,%d", address, l.Digest, at.UnixMilli(), at.Add(24*time.Hour).UnixMilli(), at.Add(48*time.Hour).UnixMilli(), at.Add(72*time.Hour).UnixMilli())
	response, e := (&http.Client{Transport: transport, Timeout: 10 * time.Second}).Get(url)
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	raw, e := io.ReadAll(response.Body)
	if e != nil || response.StatusCode != 200 || response.ProtoMajor != 3 {
		t.Fatal(response.StatusCode, response.Proto, string(raw), e)
	}
	pack, e := pilcodec.ParsePAC(nil, raw, nil)
	if e != nil {
		t.Fatal(e)
	}
	if len(pack.Members) != 2 || response.ContentLength != int64(len(raw)) || response.Header.Get("Content-Type") != "application/pil" {
		t.Fatal("invalid summary PAC response", len(pack.Members), response.ContentLength, len(raw), response.Header)
	}
	for i := 0; i < len(pack.Members); i++ {
		child := pack.Members[i]
		if _, e = pilcodec.ExpandPAC(child); e != nil {
			t.Fatal(e)
		}
	}
}
