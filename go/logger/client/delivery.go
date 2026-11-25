package logging

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"trie-trunk/go/internal/seedstream"
)

type encodable interface {
	Encode() ([]byte, error)
	logDefinition() string
}

// Destination identifies a registered /seeds endpoint. Branch comes from registration.
type Destination struct {
	URL    string
	Branch uint16
	Token  string
	TLS    *tls.Config
}

const (
	batchRecords = 32
	queueRecords = 256
	queueBytes   = 4 << 20
	batchDelay   = 10 * time.Millisecond
)

// Delivery batches complete PILs over one connection. Producers can share it.
// Acknowledged and queued sequences are protected by mu, including in-flight PILs.
type Delivery[E encodable] struct {
	mu                  sync.Mutex
	address             string
	plan                *LogType
	opts                websocket.DialOptions
	pending             [][]byte
	bytes               int
	acked, flushThrough uint64
	deadline            time.Time
	wake                chan struct{}
	encoders            chan struct{}
	progress            chan struct{}
	closing             bool
	failure             error
	done                chan struct{}
}

func (l *TypedLog[E]) Connect(ctx context.Context, dst Destination) (*Delivery[E], error) {
	return connect[E](ctx, dst, l.plan)
}

func (l *LogType) Connect(ctx context.Context, dst Destination) (*Delivery[*LogEvent], error) {
	return connect[*LogEvent](ctx, dst, l)
}

func connect[E encodable](ctx context.Context, dst Destination, plan *LogType) (*Delivery[E], error) {
	u, err := url.Parse(dst.URL)
	if err != nil {
		return nil, err
	}
	if (u.Scheme != "https" && u.Scheme != "wss") || u.Host == "" || u.User != nil || dst.Branch == 0 {
		return nil, errors.New("registered HTTPS or WSS seeds URL and branch required")
	}
	u.Scheme = "wss"
	header := http.Header{}
	header.Set("Trunk-Branch", strconv.Itoa(int(dst.Branch)))
	if dst.Token != "" {
		header.Set("Authorization", "Bearer "+dst.Token)
	}
	// RFC 6455 uses an HTTP/1.1 upgrade, including when the origin also offers h2.
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment, Protocols: &http.Protocols{}, TLSHandshakeTimeout: 10 * time.Second}
	transport.Protocols.SetHTTP1(true)
	if dst.TLS != nil {
		transport.TLSClientConfig = dst.TLS.Clone()
		transport.TLSClientConfig.NextProtos = []string{"http/1.1"}
	}
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	d := &Delivery[E]{address: u.String(), plan: plan, opts: websocket.DialOptions{HTTPClient: client, HTTPHeader: header, Subprotocols: []string{seedstream.Protocol}}, wake: make(chan struct{}, 1), encoders: make(chan struct{}, 8)}
	err = d.start(ctx)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// start runs before publication or with mu held after the previous worker stopped.
func (d *Delivery[E]) start(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, d.address, &d.opts)
	if err != nil {
		if resp != nil && resp.Body != nil {
			defer resp.Body.Close()
			detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			if len(detail) > 0 {
				return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(detail)))
			}
		}
		return err
	}
	if conn.Subprotocol() != seedstream.Protocol {
		conn.CloseNow()
		return errors.New("server did not negotiate PIL batch delivery")
	}
	if !d.plan.matches(resp.Header.Get("Definition-Version"), resp.Header.Get("Definition-Revision")) {
		conn.CloseNow()
		return errors.New("registered branch does not match the logger Definition")
	}
	conn.SetReadLimit(1)
	d.failure = nil
	d.done = make(chan struct{})
	go d.run(conn)
	return nil
}

func (d *Delivery[E]) signal() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}
func (d *Delivery[E]) notify() {
	if d.progress != nil {
		close(d.progress)
		d.progress = nil
	}
}

// wait releases mu while waiting for capacity, a durable acknowledgement or an error.
func (d *Delivery[E]) wait(ctx context.Context) error {
	if d.progress == nil {
		d.progress = make(chan struct{})
	}
	progress := d.progress
	d.mu.Unlock()
	select {
	case <-progress:
	case <-ctx.Done():
	}
	d.mu.Lock()
	return ctx.Err()
}

// Write admits an encoded event to the bounded producer queue. Flush waits for disk.
func (d *Delivery[E]) Write(ctx context.Context, event E) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Block before encoding when many producers are waiting on a full queue.
	select {
	case d.encoders <- struct{}{}:
		defer func() { <-d.encoders }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if event.logDefinition() != d.plan.digest {
		return errors.New("event uses another logger Definition")
	}
	pill, err := event.Encode()
	if err != nil {
		return err
	}
	if len(pill) == 0 || len(pill) > seedstream.MaxPill {
		return errors.New("PIL exceeds ingress limits")
	}
	size := len(pill) + 4
	d.mu.Lock()
	defer d.mu.Unlock()
	for {
		if d.failure != nil {
			return d.failure
		}
		if d.closing {
			return errors.New("logger delivery is closed")
		}
		if len(d.pending) < queueRecords && d.bytes+size <= queueBytes {
			break
		}
		if err := d.wait(ctx); err != nil {
			return err
		}
	}
	if len(d.pending) == 0 {
		d.deadline = time.Now().Add(batchDelay)
		d.signal()
	}
	d.pending = append(d.pending, pill)
	d.bytes += size
	if len(d.pending) == batchRecords || d.bytes >= seedstream.MaxMessage {
		d.signal()
	}
	return nil
}

func (d *Delivery[E]) Flush(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	target := d.acked + uint64(len(d.pending))
	d.flushThrough = max(target, d.flushThrough)
	d.signal()
	for d.acked < target {
		if d.failure != nil {
			return d.failure
		}
		if err := d.wait(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Retry explicitly resends the unacknowledged queue. If an ACK was lost, its
// batch may already be durable and replay can create duplicates.
func (d *Delivery[E]) Retry(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failure == nil {
		return errors.New("delivery has no failed batch to retry")
	}
	return d.start(ctx)
}

// Close stops admission and drains the queue. If ctx expires, draining continues.
func (d *Delivery[E]) Close(ctx context.Context) error {
	d.mu.Lock()
	d.closing = true
	d.notify()
	d.signal()
	defer d.mu.Unlock()
	for {
		done := d.done
		d.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			d.mu.Lock()
			return ctx.Err()
		}
		d.mu.Lock()
		if d.done == done {
			return d.failure
		}
	}
}

func (d *Delivery[E]) run(conn *websocket.Conn) {
	var wire []byte
	var err error
	defer func() {
		conn.CloseNow()
		d.mu.Lock()
		defer d.mu.Unlock()
		if err != nil {
			d.failure = fmt.Errorf("logger delivery unacknowledged (%d queued): %w", len(d.pending), err)
		}
		close(d.done)
		d.notify()
	}()
	for {
		d.mu.Lock()
		n := min(len(d.pending), batchRecords)
		if n == 0 {
			closing := d.closing
			d.mu.Unlock()
			if closing {
				return
			}
			<-d.wake
			continue
		}
		delay := time.Until(d.deadline)
		if n < batchRecords && !d.closing && d.flushThrough <= d.acked && delay > 0 {
			d.mu.Unlock()
			timer := time.NewTimer(delay)
			select {
			case <-d.wake:
				timer.Stop()
			case <-timer.C:
			}
			continue
		}
		wire = wire[:0]
		for _, pill := range d.pending[:n] {
			wire = binary.BigEndian.AppendUint32(wire, uint32(len(pill)))
			wire = append(wire, pill...)
		}
		d.mu.Unlock()
		err = sendBatch(conn, wire)
		if err != nil {
			return
		}
		d.mu.Lock()
		clear(d.pending[:n])
		if n == len(d.pending) {
			d.pending = d.pending[:0]
		} else {
			d.pending = d.pending[n:]
		}
		d.bytes -= len(wire)
		d.acked += uint64(n)
		d.notify()
		d.mu.Unlock()
	}
}

func sendBatch(conn *websocket.Conn, wire []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageBinary, wire); err != nil {
		return err
	}
	kind, ack, err := conn.Read(ctx)
	if err != nil {
		return err
	}
	if kind != websocket.MessageBinary || len(ack) != 1 || ack[0] != seedstream.Durable {
		return errors.New("invalid durable acknowledgement")
	}
	return nil
}
