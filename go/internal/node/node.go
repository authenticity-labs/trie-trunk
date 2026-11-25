// Package node adapts transports to the shared Rust Trunk owner. It contains no
// database lifecycle, schema interpretation, or publication logic.
package node

/*
#cgo LDFLAGS: -L${SRCDIR}/../../../rust/target/release -ltrie_trunk_search
#cgo darwin LDFLAGS: -framework Security -framework CoreFoundation -liconv -lm
#cgo linux LDFLAGS: -ldl -lpthread -lm -lrt -lutil
#include "node.h"
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

type Result struct{ ptr unsafe.Pointer }

type Error struct {
	Message string
	Status  int
}

func (e *Error) Error() string { return e.Message }

// Trie shares already-prescribed plans. Its returned handle is released through
// the Seed reader ABI, not Result.Close.
func (r *Result) Trie() unsafe.Pointer { return C.trunk_node_trie(r.ptr) }
func (n *Node) Memory() unsafe.Pointer { return C.trunk_node_memory(n.ptr) }

func span(b []byte) C.NodeSlice {
	return C.NodeSlice{data: (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(b))), len: C.size_t(len(b))}
}
func borrowed(s C.NodeSlice) []byte { return unsafe.Slice((*byte)(unsafe.Pointer(s.data)), int(s.len)) }
func result(ptr unsafe.Pointer) (*Result, error) {
	r := &Result{ptr: ptr}
	if message := string(borrowed(C.trunk_node_error(ptr))); message != "" {
		status := int(C.trunk_node_status(ptr))
		r.Close()
		return nil, &Error{Message: message, Status: status}
	}
	return r, nil
}
func (r *Result) Close() {
	if r != nil && r.ptr != nil {
		C.trunk_node_free(r.ptr)
		r.ptr = nil
	}
}

// Bytes borrows the result allocation. Callers retain the result until the last
// write finishes; this method never converts Pack bytes through JSON.
func (r *Result) Bytes(kind uint32, index int) []byte {
	return borrowed(C.trunk_node_bytes(r.ptr, C.uint32_t(kind), C.size_t(index)))
}
func (r *Result) Number(index int) uint64 { return uint64(C.trunk_node_number(r.ptr, C.size_t(index))) }

type Config struct {
	Name    string `json:"name"`
	Storage string `json:"storage"`
}
type Node struct {
	*Result
	root   string
	config Config
	refs   int
}

var owners = struct {
	sync.Mutex
	values  map[string]*Node
	aliases map[string]string
}{values: map[string]*Node{}, aliases: map[string]string{}}

// Open returns the same owner to every transport in this process. Rust also
// enforces exclusive ownership across processes.
func Open(root string, config Config, memory uint64, clock uint64) (*Node, error) {
	alias, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	root, err = filepath.EvalSymlinks(alias)
	if err != nil {
		return nil, err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	owners.Lock()
	defer owners.Unlock()
	owners.aliases[alias] = root
	if n := owners.values[root]; n != nil {
		if config.Storage != "" && n.config.Storage != config.Storage {
			return nil, fmt.Errorf("Trunk is already configured for %s storage", n.config.Storage)
		}
		n.refs++
		return n, nil
	}
	raw, e := os.ReadFile(filepath.Join(root, ".trunk/node.json"))
	if e == nil {
		var saved Config
		if e = json.Unmarshal(raw, &saved); e != nil {
			return nil, e
		}
		if config.Storage == "" {
			config.Storage = saved.Storage
		}
		if config.Name == "" {
			config.Name = saved.Name
		}
	} else if !os.IsNotExist(e) {
		return nil, e
	}

	if config.Storage == "" {
		config.Storage = "pack"
	}
	if config.Name == "" {
		config.Name = "Trunk"
	}
	if memory == 0 {
		memory = 1 << 30
	}
	raw, err = json.Marshal(config)
	if err != nil {
		return nil, err
	}
	path := []byte(root)
	r, err := result(C.trunk_node_open(span(path), span(raw), C.size_t(memory), C.uint64_t(clock)))
	runtime.KeepAlive(raw)
	runtime.KeepAlive(path)
	if err != nil {
		return nil, err
	}
	n := &Node{Result: r, root: root, config: config, refs: 1}
	owners.values[root] = n
	return n, nil
}
func (n *Node) Close() {
	if n == nil {
		return
	}
	owners.Lock()
	defer owners.Unlock()
	n.refs--
	if n.refs == 0 {
		delete(owners.values, n.root)
		for alias, root := range owners.aliases {
			if root == n.root {
				delete(owners.aliases, alias)
			}
		}
		n.Result.Close()
	}
}
func (n *Node) Ingest(branch uint16, pill []byte, clock uint64) error {
	r, e := result(C.trunk_node_ingest(n.ptr, C.uint16_t(branch), span(pill), C.uint64_t(clock)))
	runtime.KeepAlive(pill)
	if r != nil {
		r.Close()
	}
	return e
}
func (n *Node) IngestBatch(branch uint16, wire []byte, clock uint64) error {
	r, e := result(C.trunk_node_ingest_batch(n.ptr, C.uint16_t(branch), span(wire), C.uint64_t(clock)))
	runtime.KeepAlive(wire)
	if r != nil {
		r.Close()
	}
	return e
}
func (n *Node) Commit(clock uint64) error {
	r, e := result(C.trunk_node_commit(n.ptr, C.uint64_t(clock)))
	if r != nil {
		r.Close()
	}
	return e
}

type Observation struct {
	Resource  string `json:"resource"`
	Time      uint64 `json:"time"`
	Duration  uint64 `json:"duration"`
	Method    string `json:"method"`
	Route     string `json:"route"`
	Status    uint16 `json:"status"`
	Bytes     uint64 `json:"bytes"`
	Protocol  string `json:"protocol"`
	Selection string `json:"selection"`
}
type Logs struct{ *Result }

func (n *Node) Logs(cache string, resources []string) (*Logs, error) {
	name := []byte(cache)
	raw, err := json.Marshal(resources)
	if err != nil {
		return nil, err
	}
	r, err := result(C.trunk_node_logs(n.ptr, span(name), span(raw)))
	runtime.KeepAlive(name)
	runtime.KeepAlive(raw)
	if err != nil {
		return nil, err
	}
	return &Logs{r}, nil
}
func (l *Logs) Write(observation Observation) error {
	raw, err := json.Marshal(observation)
	if err != nil {
		return err
	}
	r, err := result(C.trunk_node_log(l.ptr, span(raw)))
	runtime.KeepAlive(raw)
	if r != nil {
		r.Close()
	}
	return err
}
func (n *Node) Prepare(branch uint16, clock uint64) error {
	r, e := result(C.trunk_node_prepare(n.ptr, C.uint16_t(branch), C.uint64_t(clock)))
	if r != nil {
		r.Close()
	}
	return e
}
func (n *Node) Register(request, rx []byte) error {
	r, e := result(C.trunk_node_register(n.ptr, span(request), span(rx)))
	runtime.KeepAlive(request)
	runtime.KeepAlive(rx)
	if r != nil {
		r.Close()
	}
	return e
}
func (n *Node) History(source, id string, from, to uint64, state bool) (*Result, error) {
	src, key := []byte(source), []byte(id)
	reply, err := result(C.trunk_node_history(n.ptr, span(src), span(key), C.uint64_t(from), C.uint64_t(to), C.bool(state)))
	runtime.KeepAlive(src)
	runtime.KeepAlive(key)
	return reply, err
}
func (n *Node) TimeBounds(branch uint16) (*Result, error) {
	return result(C.trunk_node_time_bounds(n.ptr, C.uint16_t(branch)))
}
func (n *Node) StorageUsage(branch uint16) ([][3]uint64, error) {
	r, err := result(C.trunk_node_storage_usage(n.ptr, C.uint16_t(branch)))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var counts [][3]uint64
	err = json.Unmarshal(r.Bytes(0, 0), &counts)
	return counts, err
}
func (n *Node) Registry() (*Result, error) { return result(C.trunk_node_registry(n.ptr)) }

type RegistrationTarget struct {
	Action     string `json:"action,omitempty"`
	Generation string `json:"generation,omitempty"`
	Discard    bool   `json:"discard,omitempty"`

	Split    string  `json:"split,omitempty"`
	Cache    string  `json:"cache"`
	Service  string  `json:"service,omitempty"`
	Resource string  `json:"resource,omitempty"`
	Type     string  `json:"resource_type,omitempty"`
	Branch   *uint16 `json:"branch,omitempty"`
}
type Registered struct {
	Bytes      []byte          `json:"bytes,omitempty"`
	Generation string          `json:"generation,omitempty"`
	Model      json.RawMessage `json:"model,omitempty"`

	Branch uint16 `json:"branch"`
	File   string `json:"file"`
}

func (n *Node) Registration(target RegistrationTarget, rx []byte) (Registered, error) {
	var registered Registered
	raw, err := json.Marshal(target)
	if err != nil {
		return registered, err
	}
	r, err := result(C.trunk_node_registration(n.ptr, span(raw), span(rx)))
	runtime.KeepAlive(raw)
	runtime.KeepAlive(rx)
	if err != nil {
		return registered, err
	}
	defer r.Close()
	err = json.Unmarshal(r.Bytes(0, 0), &registered)
	return registered, err
}
func (n *Node) Caches() ([]string, error) {
	r, err := result(C.trunk_node_caches(n.ptr))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var names []string
	err = json.Unmarshal(r.Bytes(0, 0), &names)
	return names, err
}

func (n *Node) Postgres(address, password, cert, key string, offsets map[string]int) (*Result, error) {
	zones, err := json.Marshal(offsets)
	if err != nil {
		return nil, err
	}
	a, p := []byte(address), []byte(password)
	c, k := []byte(cert), []byte(key)
	r, err := result(C.trunk_node_postgres(n.ptr, span(a), span(p), span(c), span(k), span(zones)))
	runtime.KeepAlive(zones)
	runtime.KeepAlive(c)
	runtime.KeepAlive(k)
	runtime.KeepAlive(a)
	runtime.KeepAlive(p)
	return r, err
}
func (n *Node) Serve(path string) (*Result, error) {
	raw := []byte(path)
	r, e := result(C.trunk_node_serve(n.ptr, span(raw)))
	runtime.KeepAlive(raw)
	return r, e
}

type Snapshot struct {
	*Result
	Generation, Length, SummaryLength, IndexLength uint64
	Live, Summaries                                int
	Published                                      bool
}

type SnapshotKind uint8

const (
	Packs SnapshotKind = iota
	Summaries
	Timeline
)

func (n *Node) Snapshot(branch uint16, windows [][2]uint64, kind SnapshotKind, known *uint64, clock uint64) (*Snapshot, error) {
	if windows == nil {
		windows = [][2]uint64{}
	}
	raw, e := json.Marshal(windows)
	if e != nil {
		return nil, e
	}
	var generation uint64
	if known != nil {
		generation = *known
	}
	r, e := result(C.trunk_node_snapshot(n.ptr, C.uint16_t(branch), span(raw), C.uint8_t(kind), C.uint64_t(generation), C.bool(known != nil), C.uint64_t(clock)))
	runtime.KeepAlive(raw)
	if e != nil {
		return nil, e
	}
	i := C.trunk_node_snapshot_info(r.ptr)
	return &Snapshot{Result: r, Generation: uint64(i.generation), Length: uint64(i.length), SummaryLength: uint64(i.summary_length), IndexLength: uint64(i.index_length), Live: int(i.live), Summaries: int(i.summaries), Published: bool(i.published)}, nil
}
func (s *Snapshot) File(kind uint32) (*os.File, error) {
	fd := int(C.trunk_node_file(s.ptr, C.uint32_t(kind)))
	if fd == -1 {
		return nil, nil
	}
	if fd < 0 {
		return nil, fmt.Errorf("could not pin published file descriptor")
	}
	return os.NewFile(uintptr(fd), "Trunk publication"), nil
}

// Registry returns an active owner's current immutable registry. No disk read
// or registry copy occurs here. A nil result means no node currently owns root.
func acquire(root string) (*Node, error) {
	root, e := filepath.Abs(root)
	if e != nil {
		return nil, e
	}
	owners.Lock()
	defer owners.Unlock()
	if resolved, ok := owners.aliases[root]; ok {
		root = resolved
	}
	if n := owners.values[root]; n != nil {
		n.refs++
		return n, nil
	}
	return nil, nil
}
func Registry(root string) (*Result, error) {
	n, e := acquire(root)
	if e != nil || n == nil {
		return nil, e
	}
	defer n.Close()
	return n.Registry()
}
func Register(root string, request, rx []byte) error {
	n, e := Open(root, Config{}, 0, uint64(time.Now().UnixMilli()))
	if e != nil {
		return e
	}
	defer n.Close()
	return n.Register(request, rx)
}

// Publish lets existing metadata editors participate in the node transaction.
// It refuses a stale input snapshot rather than overwriting a concurrent commit.
func Publish(root string, before, after []byte) (bool, error) {
	n, e := acquire(root)
	if e != nil || n == nil {
		return false, e
	}
	defer n.Close()
	r, e := result(C.trunk_node_publish(n.ptr, span(before), span(after)))
	runtime.KeepAlive(before)
	runtime.KeepAlive(after)
	if r != nil {
		r.Close()
	}
	return true, e
}

func (n *Node) Configuration() Config { return n.config }
func Command(args []string, input, output, diagnostics *os.File) error {
	raw, e := json.Marshal(args)
	if e != nil {
		return e
	}
	r, e := result(C.trunk_node_command(span(raw), C.int32_t(input.Fd()), C.int32_t(output.Fd()), C.int32_t(diagnostics.Fd()), C.bool(false)))
	runtime.KeepAlive(raw)
	runtime.KeepAlive(input)
	runtime.KeepAlive(output)
	runtime.KeepAlive(diagnostics)
	if r != nil {
		r.Close()
	}
	return e
}

// Subscribe returns a pollable descriptor. Each byte makes one Message available.
func (n *Node) Subscribe(branch uint16, digest, source string) (*Result, net.Conn, error) {
	sub, err := result(C.trunk_node_subscribe(n.ptr, C.uint16_t(branch), span([]byte(digest)), span([]byte(source))))
	if err != nil {
		return nil, nil, err
	}
	fd := C.trunk_node_ready(sub.ptr)
	if fd < 0 {
		sub.Close()
		return nil, nil, fmt.Errorf("open subscription descriptor")
	}
	file := os.NewFile(uintptr(fd), "roots")
	ready, err := net.FileConn(file)
	file.Close()
	if err != nil {
		sub.Close()
		return nil, nil, err
	}
	return sub, ready, nil
}
func (r *Result) Message() (*Result, error) { return result(C.trunk_node_message(r.ptr)) }
