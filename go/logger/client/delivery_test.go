package logging

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"trie-trunk/go/internal/seedstream"
	"trie-trunk/go/server/cache"
)

func TestRegisteredSocketDelivery(t *testing.T) {
	const hour = 1794830400000
	const path = "/demo/shop/orders/query/"
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	logs, err := Query()
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	rx, err := os.ReadFile("../../../testdata/seed-identity/query.rx")
	if err != nil {
		t.Fatal(err)
	}
	for _, storage := range []string{"pack", "browse"} {
		t.Run(storage, func(t *testing.T) {
			open := func(root string) *cache.Endpoint {
				p, err := cache.OpenEndpoint(cache.Config{Root: root, Cache: "demo", Tenant: "test", Storage: storage, Now: func() time.Time { return time.UnixMilli(hour + 1000) }})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { p.Close() })
				return p
			}
			request := func(p *cache.Endpoint, method, op string, payload []byte, branch string) *httptest.ResponseRecorder {
				r := httptest.NewRequest(method, path+op, bytes.NewReader(payload))
				r.Header.Set("Trunk-Branch", branch)
				r.Header.Set("Content-Type", "application/pil")
				w := httptest.NewRecorder()
				p.ServeHTTP(w, r)
				if w.Code != 204 && w.Code != 200 {
					t.Fatalf("%s: %d %s", op, w.Code, w.Body.String())
				}
				return w
			}
			register := func(p *cache.Endpoint) string {
				r := httptest.NewRequest("PUT", "/definitions", bytes.NewReader(rx))
				r.Header.Set("Content-Type", "application/pil")
				r.Header.Set("Resource-Service", "shop")
				r.Header.Set("Resource-Name", "orders")
				r.Header.Set("Resource-Type", "query")
				w := httptest.NewRecorder()
				p.ServeHTTP(w, r)
				if w.Code != 204 {
					t.Fatal(w.Code, w.Body.String())
				}
				return w.Header().Get("Trunk-Branch")
			}
			root := t.TempDir()
			p := open(root)
			branch := register(p)
			baseline := open(t.TempDir())
			baseBranch := register(baseline)
			server := httptest.NewUnstartedServer(p.Handler("secret"))
			server.EnableHTTP2 = true
			server.StartTLS()
			defer server.Close()
			id, err := strconv.ParseUint(branch, 10, 16)
			if err != nil {
				t.Fatal(err)
			}
			dst := Destination{URL: server.URL + path + "seeds", Branch: uint16(id), TLS: server.Client().Transport.(*http.Transport).TLSClientConfig}
			if d, err := logs.Connect(ctx, dst); err == nil {
				d.Close(ctx)
				t.Fatal("accepted missing token")
			}
			dst.Token = "secret"
			wrong, err := HTTP()
			if err != nil {
				t.Fatal(err)
			}
			defer wrong.Close()
			if d, err := wrong.Connect(ctx, dst); err == nil {
				d.Close(ctx)
				t.Fatal("accepted wrong RX")
			}
			delivery, err := logs.Connect(ctx, dst)
			if err != nil {
				t.Fatal(err)
			}
			defer delivery.Close(ctx)
			opts := delivery.opts
			opts.Subprotocols = []string{seedstream.Live}
			opts.HTTPHeader = opts.HTTPHeader.Clone()
			opts.HTTPHeader.Del("Trunk-Branch") // Browser subscriptions select the branch in the URL.
			feed, _, err := websocket.Dial(ctx, server.URL+path+"roots?branch="+branch, &opts)
			if err != nil {
				t.Fatal(err)
			}
			defer feed.CloseNow()
			var expected []byte
			// Expose both open hours. Enough events to cross the SDK batch boundary.
			for i := 0; i < 65; i++ {
				ms := uint64(hour + i/2)
				if i%2 == 0 {
					ms -= 60000
				}
				event := logs.Event().Time(ms).Operation("execute").Rows(uint32(i)).Status("complete")
				if err := delivery.Write(ctx, event); err != nil {
					t.Fatal(err)
				}
				raw, err := event.Encode()
				if err != nil {
					t.Fatal(err)
				}
				expected = binary.BigEndian.AppendUint32(expected, uint32(len(raw)))
				expected = append(expected, raw...)
				request(baseline, "POST", "seeds", raw, baseBranch)
			}
			if err := delivery.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			for len(expected) > 0 {
				kind, msg, err := feed.Read(ctx)
				if err != nil || kind != websocket.MessageBinary || len(msg) == 0 || !bytes.HasPrefix(expected, msg) {
					t.Fatalf("unexpected Roots batch (%d bytes): %v", len(msg), err)
				}
				expected = expected[len(msg):]
			}
			day := int64(hour / 86400000 * 86400000)
			reads := []string{fmt.Sprintf("packs?beginTime=%d&endTime=%d", int64(hour-3600000), int64(hour+3600000)), fmt.Sprintf("summaries?range=%d,%d", day, day+86400000)}
			check := func(p *cache.Endpoint) {
				for _, op := range reads {
					want := request(baseline, "GET", op, nil, baseBranch)
					got := request(p, "GET", op, nil, branch)
					if want.Body.Len() == 0 || !bytes.Equal(got.Body.Bytes(), want.Body.Bytes()) {
						t.Fatalf("%s differs: socket %d B, POST %d B", op, got.Body.Len(), want.Body.Len())
					}
				}
			}
			check(p)
			// A valid prefix followed by a truncated PIL must not enter either spool.
			raw, err := logs.Event().Time(hour).Operation("malformed batch").Encode()
			if err != nil {
				t.Fatal(err)
			}
			prefix := binary.BigEndian.AppendUint32(nil, uint32(len(raw)))
			prefix = append(prefix, raw...)
			invalidPIL := binary.BigEndian.AppendUint32(nil, uint32(len(raw)+1))
			invalidPIL = append(invalidPIL, raw...)
			invalidPIL = append(invalidPIL, 255)
			for _, tail := range [][]byte{{0, 0, 0, 4, 1}, invalidPIL} {
				conn, _, err := websocket.Dial(ctx, delivery.address, &delivery.opts)
				if err != nil {
					t.Fatal(err)
				}
				if err = conn.Write(ctx, websocket.MessageBinary, append(prefix, tail...)); err != nil {
					t.Fatal(err)
				}
				_, _, err = conn.Read(ctx)
				conn.CloseNow()
				if websocket.CloseStatus(err) != websocket.StatusInvalidFramePayloadData {
					t.Fatalf("malformed batch response: %v", err)
				}
			}
			check(p)
			request(p, "POST", "seeds", raw, branch)
			request(baseline, "POST", "seeds", raw, baseBranch)
			_, msg, err := feed.Read(ctx)
			if err != nil || !bytes.Equal(msg, prefix) {
				t.Fatalf("expected the single durable POST after rejection: %v", err)
			}
			stopped := make(chan struct{})
			go func() { p.Close(); close(stopped) }()
			select {
			case <-stopped:
			case <-ctx.Done():
				t.Fatal("shutdown blocked on idle producer")
			}
			check(open(root))
		})
	}
}

func TestDeliveryLostAcknowledgement(t *testing.T) {
	// A dropped ACK is ambiguous. Retrying must be explicit and preserve identity.
	logs, err := HTTP()
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	var connections atomic.Int32
	messages := make(chan []byte, 2)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Definition-Version", logs.plan.digest)
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{seedstream.Protocol}})
		if err != nil {
			return
		}
		defer c.CloseNow()
		id := connections.Add(1)
		_, msg, err := c.Read(r.Context())
		if err != nil {
			return
		}
		messages <- msg
		if id > 1 {
			c.Write(r.Context(), websocket.MessageBinary, []byte{seedstream.Durable})
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	d, err := logs.Connect(ctx, Destination{URL: server.URL, Branch: 1, TLS: server.Client().Transport.(*http.Transport).TLSClientConfig})
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Write(ctx, logs.Event().Method("GET")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
		t.Fatal("low-volume timer failed")
	case first := <-messages:
		if err = d.Flush(ctx); err == nil {
			t.Fatal("successful flush without ACK")
		}
		if connections.Load() != 1 {
			t.Fatal("automatic replay")
		}
		if err = d.Retry(ctx); err != nil {
			t.Fatal(err)
		}
		if err = d.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if second := <-messages; !bytes.Equal(first, second) {
			t.Fatal("replay changed event bytes")
		}
	}
}

func BenchmarkRegisteredLoggerDelivery(b *testing.B) {
	const path = "/demo/shop/orders/http/seeds"
	for _, mode := range []string{"WebSocket", "POST"} {
		b.Run(mode, func(b *testing.B) {
			rx, err := os.ReadFile("../../../schemas/logs/log.http.rx")
			if err != nil {
				b.Fatal(err)
			}
			ep, err := cache.OpenEndpoint(cache.Config{Root: b.TempDir(), Cache: "demo", Tenant: "bench", Storage: "browse"})
			if err != nil {
				b.Fatal(err)
			}
			defer ep.Close()
			reg := httptest.NewRequest("PUT", "/definitions", bytes.NewReader(rx))
			reg.Header.Set("Content-Type", "application/pil")
			reg.Header.Set("Resource-Service", "shop")
			reg.Header.Set("Resource-Name", "orders")
			reg.Header.Set("Resource-Type", "http")
			out := httptest.NewRecorder()
			ep.ServeHTTP(out, reg)
			if out.Code != 204 {
				b.Fatal(out.Code, out.Body.String())
			}
			branch := out.Header().Get("Trunk-Branch")
			id, err := strconv.ParseUint(branch, 10, 16)
			if err != nil {
				b.Fatal(err)
			}
			server := httptest.NewTLSServer(ep.Handler("bench"))
			defer server.Close()
			logs, err := HTTP()
			if err != nil {
				b.Fatal(err)
			}
			defer logs.Close()
			endpoint := server.URL + path
			var delivery *Delivery[*HTTPEvent]
			if mode == "WebSocket" {
				delivery, err = logs.Connect(b.Context(), Destination{URL: endpoint, Branch: uint16(id), Token: "bench", TLS: server.Client().Transport.(*http.Transport).TLSClientConfig})
				if err != nil {
					b.Fatal(err)
				}
				defer delivery.Close(b.Context())
			}
			// Each timed operation includes encoding 32 fresh identities and waiting for durability.
			for b.Loop() {
				for i := 0; i < batchRecords; i++ {
					event := logs.Event().Method("GET").Status(200)
					if delivery != nil {
						if err := delivery.Write(b.Context(), event); err != nil {
							b.Fatal(err)
						}
						continue
					}
					req, err := event.Request(b.Context(), endpoint)
					if err != nil {
						b.Fatal(err)
					}
					req.Header.Set("Trunk-Branch", branch)
					req.Header.Set("Authorization", "Bearer bench")
					resp, err := server.Client().Do(req)
					if err != nil {
						b.Fatal(err)
					}
					resp.Body.Close()
					if resp.StatusCode != 204 {
						b.Fatal(resp.Status)
					}
				}
				if delivery != nil {
					if err := delivery.Flush(b.Context()); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportMetric(float64(b.N*batchRecords)/b.Elapsed().Seconds(), "PIL/s")
		})
	}
}

func TestDeliveryBackpressure(t *testing.T) {
	logs, err := HTTP()
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	hold, received := make(chan struct{}), make(chan struct{}, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Definition-Version", logs.plan.digest)
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{seedstream.Protocol}})
		if err != nil {
			return
		}
		defer c.CloseNow()
		for {
			_, _, err := c.Read(ctx)
			if err != nil {
				return
			}
			select {
			case received <- struct{}{}:
			default:
			}
			select {
			case <-hold:
			case <-ctx.Done():
				return
			}
			if err = c.Write(ctx, websocket.MessageBinary, []byte{seedstream.Durable}); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	d, err := logs.Connect(ctx, Destination{URL: server.URL, Branch: 1, TLS: server.Client().Transport.(*http.Transport).TLSClientConfig})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < queueRecords; i++ {
		if err := d.Write(ctx, logs.Event().Status(200)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	short, stop := context.WithTimeout(ctx, 30*time.Millisecond)
	if err := d.Write(short, logs.Event()); err != context.DeadlineExceeded {
		t.Fatalf("full queue: %v", err)
	}
	stop()
	short, stop = context.WithTimeout(ctx, 30*time.Millisecond)
	if err := d.Flush(short); err != context.DeadlineExceeded {
		t.Fatalf("missing durable ACK: %v", err)
	}
	stop()
	close(hold)
	if err := d.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
