package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
	"trie-trunk/go/internal/node"
	"trie-trunk/go/internal/seedstream"
)

func offersStream(header http.Header, protocol string) bool {
	for _, value := range header.Values("Sec-WebSocket-Protocol") {
		for token := range strings.SplitSeq(value, ",") {
			if strings.TrimSpace(token) == protocol {
				return true
			}
		}
	}
	return false
}

// ServeHTTP holds the node lease for the lifetime of the upgraded request.
func (p *Endpoint) streamSeeds(w http.ResponseWriter, r *http.Request, route registeredRoute) {
	if !offersStream(r.Header, seedstream.Protocol) {
		http.Error(w, "expected "+seedstream.Protocol, http.StatusBadRequest)
		return
	}
	w.Header().Set("Definition-Version", route.Digest)
	w.Header().Set("Definition-Revision", fmt.Sprintf("%d.%d", route.Major, route.Minor))
	// Pass the selected token to the handshake after case-sensitive negotiation.
	r = r.Clone(r.Context())
	r.Header.Set("Sec-WebSocket-Protocol", seedstream.Protocol)
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{seedstream.Protocol}})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(p.streams, cancel)
	defer stop()
	conn.SetReadLimit(seedstream.MaxMessage)
	for {
		kind, body, err := conn.Reader(ctx)
		if err != nil {
			return
		}
		if kind != websocket.MessageBinary {
			conn.Close(websocket.StatusUnsupportedData, "binary PIL batch required")
			return
		}
		// Once a frame starts, bound the time it can pin the native staging buffer.
		deadline := time.AfterFunc(30*time.Second, cancel)
		err = p.admitMessage(body, route.Branch)
		deadline.Stop()
		if err != nil {
			code := websocket.StatusInternalError
			message := "ingress batch was not acknowledged"
			var failure *node.Error
			if errors.As(err, &failure) && failure.Status == 400 {
				code = websocket.StatusInvalidFramePayloadData
				message = "invalid registered PIL batch"
			}
			conn.Close(code, message)
			return
		}
		ack, done := context.WithTimeout(ctx, 10*time.Second)
		err = conn.Write(ack, websocket.MessageBinary, []byte{seedstream.Durable})
		done()
		if err != nil {
			return
		}
	}
}

func (p *Endpoint) admitMessage(body io.Reader, branch uint16) error {
	buffer := p.readMemory.buffer()
	defer buffer.close()
	// Allocate only while receiving a message. Idle producers hold no staging lease.
	size := 0
	for {
		target, err := buffer.resizeLimit(max(4096, cap(buffer.bytes), size+1), seedstream.MaxMessage+1)
		if err != nil {
			return err
		}
		n, err := body.Read(target[size:])
		size += n
		if size > seedstream.MaxMessage {
			return errors.New("ingress batch too large")
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return p.node.IngestBatch(branch, buffer.bytes[:size], uint64(p.config.Now().UnixMilli()))
}

// Subscribe before writing the handshake so the first acknowledged write after
// Dial returns cannot fall into a registration gap.
func (p *Endpoint) subscribe(w http.ResponseWriter, r *http.Request, route registeredRoute) {
	if !offersStream(r.Header, seedstream.Live) {
		http.Error(w, "expected "+seedstream.Live, http.StatusBadRequest)
		return
	}
	sub, ready, err := p.node.Subscribe(route.Branch, route.Digest, route.Source)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer sub.Close()
	defer ready.Close()
	w.Header().Set("Definition-Version", route.Digest)
	w.Header().Set("Definition-Revision", fmt.Sprintf("%d.%d", route.Major, route.Minor))
	r = r.Clone(r.Context())
	r.Header.Set("Sec-WebSocket-Protocol", seedstream.Live)
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{seedstream.Live}})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(p.streams, cancel)
	defer stop()
	ctx = conn.CloseRead(ctx)
	interrupt := context.AfterFunc(ctx, func() { ready.Close() })
	defer interrupt()
	var wake [8]byte
	for {
		n, err := ready.Read(wake[:])
		if err != nil {
			if ctx.Err() == nil {
				conn.Close(websocket.StatusGoingAway, "subscription ended; refresh branch and reconnect")
			}
			return
		}
		for range n {
			message, err := sub.Message()
			if err != nil {
				return
			}
			write, done := context.WithTimeout(ctx, 10*time.Second)
			err = conn.Write(write, websocket.MessageBinary, message.Bytes(0, 0))
			done()
			message.Close()
			if err != nil {
				return
			}
		}
	}
}
