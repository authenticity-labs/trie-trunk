// Package telemetry captures transport observations for the shared Rust node.
package telemetry

import (
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"trie-trunk/go/internal/node"
	"trie-trunk/go/resourceapi"
)

var Resources = []string{"definitions", "registries", "timelines", "seeds", "packs", "summaries", "indexes", "leaves", "collections"}

type Observer struct {
	current    atomic.Pointer[node.Logs]
	lifecycle  sync.Mutex
	deliveries sync.WaitGroup
	OnError    func(error)
}

func (o *Observer) Open(owner *node.Node, cache string) error {
	logs, err := owner.Logs(cache, Resources)
	if err != nil {
		return err
	}
	o.current.Store(logs)
	return nil
}
func (o *Observer) Close() {
	o.lifecycle.Lock()
	logs := o.current.Swap(nil)
	o.lifecycle.Unlock()
	o.deliveries.Wait()
	if logs != nil {
		logs.Close()
	}
}

// Transport supplies request timings; Rust authors PILs and admits them through
// the same durable node operation as external producers. Response completion
// does not wait for the observation's durability acknowledgement.
func (o *Observer) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logs := o.current.Load()
		kind := r.URL.Path[strings.LastIndexByte(r.URL.Path, '/')+1:]
		if logs == nil || r.Method != http.MethodGet || !resourceapi.IsPath(r.URL.Path) || !slices.Contains(Resources, kind) {
			next.ServeHTTP(w, r)
			return
		}
		started := time.Now()
		out := &response{ResponseWriter: w}
		next.ServeHTTP(out, r)
		completed := time.Now()
		selected := r.URL.Query()
		for key := range selected {
			switch key {
			case "beginTime", "endTime", "range", "rows", "days", "hours", "kind":
			default:
				selected.Del(key)
			}
		}
		status := out.status
		if status == 0 {
			status = http.StatusOK
		}
		event := node.Observation{Resource: kind, Time: uint64(started.UnixMilli()), Duration: uint64(completed.Sub(started).Microseconds()), Method: r.Method, Route: r.URL.Path, Status: uint16(status), Bytes: out.bytes, Protocol: r.Proto, Selection: selected.Encode()}
		o.lifecycle.Lock()
		if o.current.Load() != logs {
			o.lifecycle.Unlock()
			return
		}
		o.deliveries.Add(1)
		o.lifecycle.Unlock()
		go func() {
			defer o.deliveries.Done()
			if err := logs.Write(event); err != nil && o.OnError != nil {
				o.OnError(fmt.Errorf("server log %s: %w", kind, err))
			}
		}()
	})
}

type response struct {
	http.ResponseWriter
	status int
	bytes  uint64
}

func (w *response) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *response) WriteHeader(status int) {
	if status >= 200 && w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}
func (w *response) Write(raw []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	n, err := w.ResponseWriter.Write(raw)
	w.bytes += uint64(n)
	return n, err
}
func (w *response) ReadFrom(r io.Reader) (int64, error) { return io.Copy(struct{ io.Writer }{w}, r) }
