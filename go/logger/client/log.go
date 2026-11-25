package logging

/*
#cgo LDFLAGS: -L${SRCDIR}/../../../rust/target/release -ltrie_trunk_search
#cgo darwin LDFLAGS: -framework Security -framework CoreFoundation -liconv -lm
#cgo linux LDFLAGS: -ldl -lpthread -lm -lrt -lutil
#include "../../server/cache/seed_native.h"
*/
import "C"

//go:generate cargo run --quiet --locked --manifest-path ../../../rust/seeds/Cargo.toml --example go_log_builders -- log_builders.gen.go
//go:generate gofmt -w log_builders.gen.go

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/fxamacker/cbor/v2"
)

// Style selects the input vocabulary of a registered log type.
type Style uint8

const (
	TrieTrunk Style = iota
	OpenTelemetry
)

// LogOption describes one setter offered by the selected RX and style.
// Field is the canonical stored name. Unit describes the setter's input.
type LogOption struct{ Name, Field, Kind, Unit string }

// LogType is safe to share across producers. Each Event belongs to its caller.
type LogType struct {
	mu      sync.RWMutex
	handle  unsafe.Pointer
	options []LogOption
	custom  bool
	digest  string
	rx      []byte
}

// TypedLog shares a prepared Definition. Event returns its style-specific builder.
type TypedLog[E encodable] struct {
	plan *LogType
	wrap func(logRecord) E
}

func typedLog[E encodable](name string, style Style, wrap func(logRecord) E) (*TypedLog[E], error) {
	plan, err := Log(name, style)
	if err != nil {
		return nil, err
	}
	return &TypedLog[E]{plan: plan, wrap: wrap}, nil
}

func (l *TypedLog[E]) Event() E { return l.wrap(logRecord{l.plan.Event()}) }
func (l *TypedLog[E]) Close()   { l.plan.Close() }

type logRecord struct{ seed *LogEvent }

func (r logRecord) Encode() ([]byte, error) { return r.seed.Encode() }
func (r logRecord) Request(ctx context.Context, url string) (*http.Request, error) {
	return r.seed.Request(ctx, url)
}

func logSpan(bytes []byte) C.TTSeedSlice {
	return C.TTSeedSlice{data: (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(bytes))), len: C.size_t(len(bytes))}
}

func logResult(handle unsafe.Pointer) ([]byte, error) {
	defer C.trunk_seed_output_free(handle)
	if failure := C.trunk_seed_output_error(handle); failure.len != 0 {
		return nil, errors.New(C.GoStringN((*C.char)(unsafe.Pointer(failure.data)), C.int(failure.len)))
	}
	bytes := C.trunk_seed_output_bytes(handle)
	return C.GoBytes(unsafe.Pointer(bytes.data), C.int(bytes.len)), nil
}

// Log selects a built-in Trunk log type by name.
func Log(name string, style Style) (*LogType, error) {
	b := []byte(name)
	rx, err := logResult(C.trunk_log_definition(logSpan(b)))
	runtime.KeepAlive(b)
	if err != nil {
		return nil, err
	}
	return OpenLog(rx, style)
}

// OpenLog binds a DOC-authored log RX once. Close it after its producers finish.
func OpenLog(rx []byte, style Style) (*LogType, error) {
	var handle unsafe.Pointer
	raw, err := logResult(C.trunk_log_open(logSpan(rx), C.uint8_t(style), &handle))
	runtime.KeepAlive(rx)
	if err != nil {
		return nil, err
	}
	var descriptor struct {
		Options []LogOption
		Custom  bool
		Digest  string
	}
	if err = json.Unmarshal(raw, &descriptor); err != nil {
		C.trunk_log_close(handle)
		return nil, err
	}
	return &LogType{handle: handle, options: descriptor.Options, custom: descriptor.Custom, digest: descriptor.Digest, rx: bytes.Clone(rx)}, nil
}

// Registration stamps draft RX templates. Verify that the same template at the
// server's published revision produces exactly its registered structural hash.
func (l *LogType) matches(digest, revision string) bool {
	if digest == "" {
		return false
	}
	if digest == l.digest {
		return true
	}
	a, b, ok := strings.Cut(revision, ".")
	if !ok {
		return false
	}
	major, e := strconv.ParseUint(a, 10, 32)
	minor, f := strconv.ParseUint(b, 10, 32)
	if e != nil || f != nil || major == 0 {
		return false
	}
	raw, err := logResult(C.trunk_log_release_digest(logSpan(l.rx), C.uint32_t(major), C.uint32_t(minor)))
	runtime.KeepAlive(l)
	return err == nil && hex.EncodeToString(raw) == digest
}
func (r logRecord) logDefinition() string { return r.seed.log.digest }
func (e *LogEvent) logDefinition() string { return e.log.digest }

func (l *LogType) Options() []LogOption { return slices.Clone(l.options) }
func (l *LogType) CustomFields() bool   { return l.custom }

func (l *LogType) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	C.trunk_log_close(l.handle)
	l.handle = nil
}

// LogEvent retains capture time and entropy, including across repeated encodes.
type LogEvent struct {
	log             *LogType
	captured        uint64
	id              [10]byte
	options, custom [][2]any
	err             error
}

func (l *LogType) Event() *LogEvent {
	e := &LogEvent{log: l, captured: uint64(time.Now().UnixNano()), options: [][2]any{}, custom: [][2]any{}}
	_, e.err = rand.Read(e.id[:])
	return e
}

// Set assigns a named option in this log's style.
func (e *LogEvent) Set(name string, value any) *LogEvent {
	e.options = append(e.options, [2]any{name, value})
	return e
}

// Field adds an application attribute to the Definition's @fields collection.
func (e *LogEvent) Field(name string, value any) *LogEvent {
	e.custom = append(e.custom, [2]any{name, value})
	return e
}

// Encode returns one complete PIL for the registered branch's POST /seeds body.
func (e *LogEvent) Encode() ([]byte, error) {
	if e.err != nil {
		return nil, e.err
	}
	raw, err := cbor.Marshal([2]any{e.options, e.custom})
	if err != nil {
		return nil, err
	}
	l := e.log
	l.mu.RLock()
	defer l.mu.RUnlock()
	id := e.id
	result := C.trunk_log_encode(l.handle, C.uint64_t(e.captured), logSpan(id[:]), logSpan(raw))
	runtime.KeepAlive(id)
	runtime.KeepAlive(raw)
	return logResult(result)
}

// Request builds a replayable POST for the selected branch's /seeds URL.
// Use the application's HTTP client for authentication, deadlines and transport.
func (e *LogEvent) Request(ctx context.Context, seedsURL string) (*http.Request, error) {
	pill, err := e.Encode()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, seedsURL, bytes.NewReader(pill))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/pil")
	return req, nil
}
