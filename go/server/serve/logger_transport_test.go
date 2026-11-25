//go:build darwin || linux

package serve

// Opt-in transport comparison. The server and producer use separate processes.
// All three adapters below are benchmark-only and call the same Rust admission.
import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/quic-go/quic-go/http3"
	"trie-trunk/go/codec"
	"trie-trunk/go/internal/node"
	"trie-trunk/go/server/cache"
)

const logBenchClock = 1794830403000
const logBenchLimit = 256 << 10

type logTrial struct {
	Storage                   string
	Address, Protocol         string
	Certificate               []byte
	Producers, Batch, Batches int
}

func ackLogBatch(owner *node.Node, payload []byte) error {
	for len(payload) > 0 {
		if len(payload) < 2 {
			return io.ErrUnexpectedEOF
		}
		size := int(binary.BigEndian.Uint16(payload))
		payload = payload[2:]
		if size == 0 || size > len(payload) {
			return io.ErrUnexpectedEOF
		}
		if err := owner.Ingest(1, payload[:size], logBenchClock); err != nil {
			return err
		}
		payload = payload[size:]
	}
	return nil
}

func logTrialHandler(accept func([]byte) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer bench" {
			w.WriteHeader(401)
			return
		}
		switch r.URL.Query().Get("mode") {
		case "wss":
			c, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer c.CloseNow()
			c.SetReadLimit(logBenchLimit)
			for {
				kind, body, err := c.Read(r.Context())
				if err != nil || kind != websocket.MessageBinary || accept(body) != nil {
					return
				}
				if err := c.Write(r.Context(), websocket.MessageBinary, []byte{1}); err != nil {
					return
				}
			}
		case "h3stream":
			if r.ProtoMajor != 3 {
				w.WriteHeader(505)
				return
			}
			w.WriteHeader(200)
			flusher := w.(http.Flusher)
			flusher.Flush()
			var size [4]byte
			var body []byte
			for {
				if _, err := io.ReadFull(r.Body, size[:]); err != nil {
					return
				}
				n := binary.BigEndian.Uint32(size[:])
				if n == 0 || n > logBenchLimit {
					return
				}
				if cap(body) < int(n) {
					body = make([]byte, n)
				}
				if _, err := io.ReadFull(r.Body, body[:n]); err != nil || accept(body[:n]) != nil {
					return
				}
				if _, err := w.Write([]byte{1}); err != nil {
					return
				}
				flusher.Flush()
			}
		case "h3post":
			if r.ProtoMajor != 3 {
				w.WriteHeader(505)
				return
			}
			body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, logBenchLimit))
			if err == nil {
				err = accept(body)
			}
			if err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			w.WriteHeader(204)
		default:
			http.NotFound(w, r)
		}
	}
}

// Each simulated logger gets its own warm TLS connection and has one outstanding batch.
func (cfg logTrial) sender(ctx context.Context) (func([]byte) error, func(), error) {
	pool := x509.NewCertPool()
	cert, err := x509.ParseCertificate(cfg.Certificate)
	if err != nil {
		return nil, nil, err
	}
	pool.AddCert(cert)
	tlsConfig := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13}
	path := cfg.Address + "/ingest?mode=" + cfg.Protocol
	header := http.Header{"Authorization": {"Bearer bench"}, "Content-Type": {"application/octet-stream"}}
	if cfg.Protocol == "wss" {
		tr := &http.Transport{TLSClientConfig: tlsConfig}
		c, _, err := websocket.Dial(ctx, "wss://"+path, &websocket.DialOptions{HTTPClient: &http.Client{Transport: tr}, HTTPHeader: header})
		if err != nil {
			return nil, nil, err
		}
		return func(body []byte) error {
			if err := c.Write(ctx, websocket.MessageBinary, body); err != nil {
				return err
			}
			kind, ack, err := c.Read(ctx)
			if err == nil && (kind != websocket.MessageBinary || !bytes.Equal(ack, []byte{1})) {
				return fmt.Errorf("invalid WebSocket ACK")
			}
			return err
		}, func() { c.CloseNow(); tr.CloseIdleConnections() }, nil
	}
	tr := &http3.Transport{TLSClientConfig: tlsConfig}
	client := &http.Client{Transport: tr}
	post := func(body io.Reader) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, "POST", "https://"+path, body)
		if err != nil {
			return nil, err
		}
		req.Header = header.Clone()
		return client.Do(req)
	}
	if cfg.Protocol == "h3post" {
		return func(body []byte) error {
			resp, err := post(bytes.NewReader(body))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 204 || resp.ProtoMajor != 3 {
				detail, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("unexpected POST response %s: %s", resp.Status, detail)
			}
			return nil
		}, func() { tr.Close() }, nil
	}
	reader, writer := io.Pipe()
	resp, err := post(reader)
	if err != nil {
		writer.Close()
		tr.Close()
		return nil, nil, err
	}
	if resp.ProtoMajor != 3 || resp.StatusCode != 200 {
		writer.Close()
		resp.Body.Close()
		tr.Close()
		return nil, nil, fmt.Errorf("unexpected stream response %s", resp.Status)
	}
	return func(body []byte) error {
		// Batch size is prefixed before timing, just as PIL lengths are for all adapters.
		if _, err := writer.Write(body); err != nil {
			return err
		}
		var ack [1]byte
		if _, err := io.ReadFull(resp.Body, ack[:]); err != nil {
			return err
		}
		if ack[0] != 1 {
			return fmt.Errorf("invalid HTTP/3 ACK")
		}
		return nil
	}, func() { writer.Close(); resp.Body.Close(); tr.Close() }, nil
}

func loggerCPU() time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		panic(err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

func TestLogTransportProducer(t *testing.T) {
	raw := os.Getenv("TRUNK_LOG_TRIAL")
	if raw == "" {
		t.Skip("benchmark subprocess")
	}
	var cfg logTrial
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	var body []byte
	for i := 0; i < cfg.Batch; i++ {
		pill, err := os.ReadFile(fmt.Sprintf("../../../testdata/http-ingress/%d.pil", i%2))
		if err != nil {
			t.Fatal(err)
		}
		body, err = codec.AppendFrame(body, pill)
		if err != nil {
			t.Fatal(err)
		}
	}
	if cfg.Protocol == "h3stream" {
		body = append(binary.BigEndian.AppendUint32(nil, uint32(len(body))), body...)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	start := make(chan struct{})
	var workers sync.WaitGroup
	samples := make([]time.Duration, cfg.Batches)
	errs := make(chan error, cfg.Producers)
	for i := 0; i < cfg.Producers; i++ {
		send, closeSender, err := cfg.sender(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer closeSender()
		for range 3 {
			if err = send(body); err != nil {
				t.Fatal(err)
			}
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for j := i; j < len(samples); j += cfg.Producers {
				at := time.Now()
				if err := send(body); err != nil {
					errs <- err
					return
				}
				samples[j] = time.Since(at)
			}
		}()
	}
	cpu := loggerCPU()
	at := time.Now()
	close(start)
	workers.Wait()
	elapsed := time.Since(at)
	cost := loggerCPU() - cpu
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	slices.Sort(samples)
	events := cfg.Batches * cfg.Batch
	p99 := samples[(99*len(samples)+99)/100-1]
	fmt.Printf("RESULT %s %s %d %d %d %.2f %.2f %.0f\n", cfg.Storage, cfg.Protocol, cfg.Producers, cfg.Batch, cfg.Batches, float64(cost.Nanoseconds())/1e3/float64(events), float64(p99.Nanoseconds())/1e6, float64(events)/elapsed.Seconds())
}

func TestRemoteLogTransport(t *testing.T) {
	if os.Getenv("TRUNK_LOG_BENCH") != "1" {
		t.Skip("set TRUNK_LOG_BENCH=1 to measure")
	}
	rx, err := os.ReadFile("../../../testdata/http-ingress/query.rx")
	if err != nil {
		t.Fatal(err)
	}
	protocols := []string{"h3post", "wss", "h3stream"}
	for repeat := 0; repeat < 3; repeat++ {
		for _, producers := range []int{1, 8, 64} {
			for _, scenario := range []string{"node1", "sync32", "transport1", "transport32"} {
				batch := 1
				if strings.HasSuffix(scenario, "32") {
					batch = 32
				}
				if producers == 64 && batch == 1 {
					continue
				}
				for i := range protocols {
					protocol := protocols[(i+repeat)%len(protocols)]
					cfg := logTrial{Storage: scenario, Protocol: protocol, Producers: producers, Batch: batch, Batches: 256}
					if strings.HasPrefix(scenario, "transport") {
						cfg.Batches = 4096
					}
					t.Run(fmt.Sprintf("%d/%d/%s/%s", repeat, producers, scenario, protocol), func(t *testing.T) { runLogTrial(t, rx, cfg) })
				}
			}
		}
	}
}

func runLogTrial(t *testing.T, rx []byte, cfg logTrial) {
	t.Helper()
	root := shortDirectory(t)
	owner, err := node.Open(root, node.Config{Storage: "pack"}, 0, logBenchClock)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	err = cache.RegisterSeedDefinition(root, rx, cache.SeedRegistration{Cache: "demo", Service: "bench", Resource: "requests", ResourceType: "query", File: "query.rx"})
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, _ := credentials(t)
	serverTLS.ClientAuth = tls.NoClientCert
	// The sync32 model measures one durable append per network batch. It is a
	// benchmark spool, not a change to the Rust recovery or publication path.
	accept := func(data []byte) error { return ackLogBatch(owner, data) }
	if cfg.Storage == "sync32" {
		spool, err := os.CreateTemp(root, "batch-spool-*")
		if err != nil {
			t.Fatal(err)
		}
		defer spool.Close()
		var mu sync.Mutex
		accept = func(data []byte) error {
			mu.Lock()
			defer mu.Unlock()
			if _, err := spool.Write(data); err != nil {
				return err
			}
			return spool.Sync()
		}
	} else if strings.HasPrefix(cfg.Storage, "transport") {
		accept = func([]byte) error { return nil }
	}
	var count atomic.Int64
	var startCPU atomic.Int64
	doneCPU := make(chan time.Duration, 1)
	observe := func(data []byte) error {
		if err := accept(data); err != nil {
			return err
		}
		switch count.Add(1) {
		case int64(3 * cfg.Producers):
			startCPU.Store(int64(loggerCPU()))
		case int64(3*cfg.Producers + cfg.Batches):
			doneCPU <- loggerCPU() - time.Duration(startCPU.Load())
		}
		return nil
	}
	addr, stop := start(t, Config{Root: root, Socket: filepath.Join(root, "n.sock"), Address: "127.0.0.1:0", Token: "bench", TLS: serverTLS, API: logTrialHandler(observe)})
	defer stop()
	cfg.Address = addr
	cfg.Certificate = serverTLS.Certificates[0].Certificate[0]
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestLogTransportProducer$")
	cmd.Env = append(os.Environ(), "TRUNK_LOG_TRIAL="+string(raw))
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, output)
	}
	select {
	case serverCPU := <-doneCPU:
		t.Logf("%sSERVER_CPU %.2f", output, float64(serverCPU.Nanoseconds())/1e3/float64(cfg.Batch*cfg.Batches))
	default:
		t.Fatal("missing final server admission")
	}
}
