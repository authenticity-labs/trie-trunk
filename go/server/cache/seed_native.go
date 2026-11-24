package cache

/*
#include <stdlib.h>
#include "seed_native.h"
*/
import "C"

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"unsafe"
)

// Native adapters only: Rust owns Definition compilation and all PIL/PAC bytes.
// These handles are explicitly released by their storage owner.
type seedNativeDefinition struct{ ptr unsafe.Pointer }
type seedNativeTime struct{ ptr unsafe.Pointer }
type seedNativeTimeLeaf struct{ Hour, Start, End, Packs, Beds, Count uint64 }
type seedNativeTimeSelection struct {
	ptr    unsafe.Pointer
	leaves []seedNativeTimeLeaf
}
type seedNativeOutput struct {
	ptr   unsafe.Pointer
	bytes []byte
}

func seedNativeSlice(raw []byte) C.TTSeedSlice {
	return C.TTSeedSlice{data: (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(raw))), len: C.size_t(len(raw))}
}
func seedNativeText(raw C.TTSeedSlice) string {
	return string(unsafe.Slice((*byte)(unsafe.Pointer(raw.data)), int(raw.len)))
}
func seedNativeResult(ptr unsafe.Pointer) (*seedNativeOutput, error) {
	if ptr == nil {
		return nil, errors.New("native Seed operation returned no result")
	}
	if e := seedNativeText(C.trunk_seed_output_error(ptr)); e != "" {
		C.trunk_seed_output_free(ptr)
		return nil, errors.New(e)
	}
	span := C.trunk_seed_output_bytes(ptr)
	return &seedNativeOutput{ptr: ptr, bytes: unsafe.Slice((*byte)(unsafe.Pointer(span.data)), int(span.len))}, nil
}
func (o *seedNativeOutput) close() {
	if o != nil && o.ptr != nil {
		C.trunk_seed_output_free(o.ptr)
		o.ptr, o.bytes = nil, nil
	}
}
func (o *seedNativeOutput) adopt(memory *nativeMemory) (*nativeBuffer, error) {
	if o == nil || o.ptr == nil || memory == nil || memory.ptr == nil {
		return nil, errors.New("released native Seed output or pool")
	}
	ptr := C.trunk_seed_output_adopt(o.ptr, memory.ptr)
	if ptr == nil {
		return nil, fmt.Errorf("%w: native Seed output budget exhausted", ErrStagingFull)
	}
	buffer := &nativeBuffer{ptr: ptr, bytes: o.bytes, frozen: true}
	o.close()
	return buffer, nil
}
func seedNativeDocument(jsonView []byte) (*seedNativeOutput, error) {
	result := C.trunk_seed_document(seedNativeSlice(jsonView))
	runtime.KeepAlive(jsonView)
	return seedNativeResult(result)
}

func seedNativeOpenTime(raw []byte) (*seedNativeTime, error) {
	ptr := C.trunk_seed_time_open(seedNativeSlice(raw))
	runtime.KeepAlive(raw)
	if err := seedNativeText(C.trunk_seed_time_error(ptr)); err != "" {
		C.trunk_seed_time_free(ptr)
		return nil, errors.New(err)
	}
	return &seedNativeTime{ptr: ptr}, nil
}

func (t *seedNativeTime) selectRange(from, to uint64) (*seedNativeTimeSelection, error) {
	result := C.trunk_seed_time_select(t.ptr, C.uint64_t(from), C.uint64_t(to))
	runtime.KeepAlive(t)
	if err := seedNativeText(C.trunk_seed_time_selection_error(result)); err != "" {
		C.trunk_seed_time_selection_free(result)
		return nil, errors.New(err)
	}
	leaves := C.trunk_seed_time_leaves(result)
	return &seedNativeTimeSelection{ptr: result, leaves: unsafe.Slice((*seedNativeTimeLeaf)(unsafe.Pointer(leaves.data)), int(leaves.len))}, nil
}

func (s *seedNativeTimeSelection) close() {
	if s != nil && s.ptr != nil {
		C.trunk_seed_time_selection_free(s.ptr)
		s.ptr, s.leaves = nil, nil
	}
}

func (t *seedNativeTime) info() (*seedNativeOutput, error) {
	result := C.trunk_seed_time_info(t.ptr)
	runtime.KeepAlive(t)
	return seedNativeResult(result)
}

func (t *seedNativeTime) bytes() []byte {
	span := C.trunk_seed_time_bytes(t.ptr)
	return unsafe.Slice((*byte)(unsafe.Pointer(span.data)), int(span.len))
}

func (t *seedNativeTime) spans(from, to uint64) (*seedNativeOutput, error) {
	result := C.trunk_seed_time_spans(t.ptr, C.uint64_t(from), C.uint64_t(to))
	runtime.KeepAlive(t)
	return seedNativeResult(result)
}

func (t *seedNativeTime) indexSpans(rows []string, length uint64) (*seedNativeOutput, error) {
	raw, err := json.Marshal(rows)
	if err != nil {
		return nil, err
	}
	result := C.trunk_seed_time_index_spans(t.ptr, seedNativeSlice(raw), C.uint64_t(length))
	runtime.KeepAlive(t)
	runtime.KeepAlive(raw)
	return seedNativeResult(result)
}

func (t *seedNativeTime) close() {
	if t != nil && t.ptr != nil {
		C.trunk_seed_time_free(t.ptr)
		t.ptr = nil
	}
}

func seedCompressPAC(raw []byte, algorithm uint32, level int) ([]byte, error) {
	if level < 0 || uint64(level) > uint64(^uint32(0)) {
		return nil, errors.New("invalid PAC compression level")
	}
	ptr := C.trunk_seed_compress(seedNativeSlice(raw), C.uint32_t(algorithm), C.uint32_t(level))
	runtime.KeepAlive(raw)
	result, err := seedNativeResult(ptr)
	if err != nil {
		return nil, err
	}
	defer result.close()
	// This existing convenience API returns host-owned bytes. The storage writer
	// uses nativeMemory.sealPAC instead, retaining the codec allocation directly.
	return append([]byte(nil), result.bytes...), nil
}

func seedExpandPAC(raw []byte) ([]byte, error) {
	expansion := C.trunk_seed_expand(seedNativeSlice(raw))
	runtime.KeepAlive(raw)
	if expansion.status == 0 {
		return raw, nil
	}
	result, err := seedNativeResult(expansion.output)
	if err != nil {
		return nil, err
	}
	defer result.close()
	return append([]byte(nil), result.bytes...), nil
}
func seedNativeRegistration(trie, rx, target []byte) (*seedNativeOutput, error) {
	result := C.trunk_register_resource(seedNativeSlice(trie), seedNativeSlice(rx), seedNativeSlice(target))
	runtime.KeepAlive(trie)
	runtime.KeepAlive(rx)
	runtime.KeepAlive(target)
	return seedNativeResult(result)
}
func seedNativeBinding(trie []byte, branch uint16) (*seedNativeOutput, error) {
	result := C.trunk_resource_binding(seedNativeSlice(trie), C.uint16_t(branch))
	runtime.KeepAlive(trie)
	return seedNativeResult(result)
}
func seedNativeReplacement(before, after []byte) error {
	ptr := C.trunk_definition_replacement(seedNativeSlice(before), seedNativeSlice(after))
	runtime.KeepAlive(before)
	runtime.KeepAlive(after)
	result, err := seedNativeResult(ptr)
	if err == nil {
		result.close()
	}
	return err
}
func seedNativeDefinitionFile(trie []byte, file string) (*seedNativeOutput, error) {
	name := []byte(file)
	ptr := C.trunk_definition_file(seedNativeSlice(trie), seedNativeSlice(name))
	runtime.KeepAlive(trie)
	runtime.KeepAlive(name)
	return seedNativeResult(ptr)
}
func seedNativeDefinitionFileMatches(artifact, raw []byte) error {
	ptr := C.trunk_definition_file_matches(seedNativeSlice(artifact), seedNativeSlice(raw))
	runtime.KeepAlive(artifact)
	runtime.KeepAlive(raw)
	result, err := seedNativeResult(ptr)
	if err == nil {
		result.close()
	}
	return err
}
func seedNativeRegister(raw []byte) (*seedNativeDefinition, error) {
	return seedNativeRegisterMember(raw, 0)
}

type seedNativeTrunk struct{ ptr unsafe.Pointer }

func seedNativeOpenTrunk(raw []byte) (*seedNativeTrunk, error) {
	p := C.trunk_seed_trie_open(seedNativeSlice(raw))
	runtime.KeepAlive(raw)
	return seedNativeTrunkHandle(p)
}
func seedNativeTrunkHandle(p unsafe.Pointer) (*seedNativeTrunk, error) {
	if p == nil {
		return nil, errors.New("native trie returned no result")
	}
	message := C.trunk_seed_trie_error(p)
	if message.len != 0 {
		err := errors.New(seedNativeText(message))
		C.trunk_seed_trie_free(p)
		return nil, err
	}
	trie := &seedNativeTrunk{p}
	runtime.SetFinalizer(trie, (*seedNativeTrunk).close)
	return trie, nil
}
func (t *seedNativeTrunk) close() {
	if t != nil && t.ptr != nil {
		runtime.SetFinalizer(t, nil)
		C.trunk_seed_trie_free(t.ptr)
		t.ptr = nil
	}
}
func (t *seedNativeTrunk) definitionFile(id uint32) (string, error) {
	result, err := seedNativeResult(C.trunk_seed_trie_definition_file(t.ptr, C.uint32_t(id)))
	runtime.KeepAlive(t)
	if err != nil {
		return "", err
	}
	defer result.close()
	return string(result.bytes), nil
}
func (t *seedNativeTrunk) routes(cache string) (*seedNativeOutput, error) {
	name := []byte(cache)
	result := C.trunk_seed_trie_routes(t.ptr, seedNativeSlice(name))
	runtime.KeepAlive(name)
	runtime.KeepAlive(t)
	return seedNativeResult(result)
}
func (t *seedNativeTrunk) leafDefinitions(path string, raw []byte) ([]string, error) {
	name := []byte(path)
	ptr := C.trunk_seed_trie_leaves(t.ptr, seedNativeSlice(name), seedNativeSlice(raw))
	runtime.KeepAlive(name)
	runtime.KeepAlive(t)
	runtime.KeepAlive(raw)
	result, err := seedNativeResult(ptr)
	if err != nil {
		return nil, err
	}
	defer result.close()
	var roots []string
	err = json.Unmarshal(result.bytes, &roots)
	return roots, err
}
func (t *seedNativeTrunk) catalog(cache string, files []byte, mode uint32) (*seedNativeOutput, error) {
	name := []byte(cache)
	result := C.trunk_seed_trie_catalog(t.ptr, seedNativeSlice(name), seedNativeSlice(files), C.uint32_t(mode))
	runtime.KeepAlive(name)
	runtime.KeepAlive(files)
	runtime.KeepAlive(t)
	return seedNativeResult(result)
}
func (t *seedNativeTrunk) prescribeFile(file string, raw []byte) error {
	name := []byte(file)
	ptr := C.trunk_seed_trie_prescribe_file(t.ptr, seedNativeSlice(name), seedNativeSlice(raw))
	runtime.KeepAlive(name)
	runtime.KeepAlive(raw)
	runtime.KeepAlive(t)
	result, err := seedNativeResult(ptr)
	if result != nil {
		result.close()
	}
	return err
}
func (t *seedNativeTrunk) prescribe(id uint32, raw []byte) (*seedNativeDefinition, error) {
	p := C.trunk_seed_trie_prescribe(t.ptr, C.uint32_t(id), seedNativeSlice(raw))
	runtime.KeepAlive(raw)
	runtime.KeepAlive(t)
	if p == nil {
		return nil, errors.New("native prescription returned no result")
	}
	message := C.trunk_seed_register_error(p)
	if message.len != 0 {
		err := errors.New(seedNativeText(message))
		C.trunk_seed_register_free(p)
		return nil, err
	}
	return &seedNativeDefinition{p}, nil
}
func seedNativeRegisterMember(raw []byte, member uint32) (*seedNativeDefinition, error) {
	p := C.trunk_seed_register_member(seedNativeSlice(raw), C.size_t(member))
	runtime.KeepAlive(raw)
	if p == nil {
		return nil, errors.New("native Seed registration returned no result")
	}
	if e := seedNativeText(C.trunk_seed_register_error(p)); e != "" {
		C.trunk_seed_register_free(p)
		return nil, errors.New(e)
	}
	return &seedNativeDefinition{p}, nil
}
func (d *seedNativeDefinition) close() {
	if d != nil && d.ptr != nil {
		C.trunk_seed_register_free(d.ptr)
		d.ptr = nil
	}
}

type seedNativeValue struct {
	ordinal   int
	kind      uint32
	low, high uint64
	real      float64
	bytes     []byte
}

const (
	seedAbsent uint32 = iota
	seedNull
	seedFieldBytes
	seedUnsigned
	seedSigned
	seedFloat
	seedBoolean
	seedText
	seedBytes
	seedStructured
)

func (d *seedNativeDefinition) ingest(values []seedNativeValue, concise bool) (*seedNativeOutput, error) {
	if d == nil || d.ptr == nil {
		return nil, errors.New("released Seed Definition")
	}
	if uint64(len(values)) > uint64(^uint(0)>>1)/uint64(C.sizeof_TTSeedValue) {
		return nil, errors.New("Seed value count overflow")
	}
	var arena unsafe.Pointer
	if len(values) != 0 {
		arena = C.calloc(C.size_t(len(values)), C.sizeof_TTSeedValue)
		if arena == nil {
			return nil, errors.New("Seed input allocation failed")
		}
		defer C.free(arena)
	}
	fields := unsafe.Slice((*C.TTSeedValue)(arena), len(values))
	var pins runtime.Pinner
	defer pins.Unpin()
	for i, value := range values {
		if value.ordinal < 0 {
			return nil, errors.New("negative Seed field ordinal")
		}
		if len(value.bytes) > 0 {
			pins.Pin(unsafe.SliceData(value.bytes))
		}
		fields[i] = C.TTSeedValue{ordinal: C.size_t(value.ordinal), kind: C.uint32_t(value.kind), low: C.uint64_t(value.low), high: C.uint64_t(value.high), real: C.double(value.real), bytes: seedNativeSlice(value.bytes)}
	}
	result := C.trunk_seed_ingest(d.ptr, (*C.TTSeedValue)(arena), C.size_t(len(values)), C.bool(concise))
	runtime.KeepAlive(values)
	return seedNativeResult(result)
}
func (d *seedNativeDefinition) pack(members [][]byte, hour uint64, sealed bool, algorithm, level uint32) (*seedNativeOutput, error) {
	if d == nil || d.ptr == nil {
		return nil, errors.New("released Seed Definition")
	}
	if uint64(len(members)) > uint64(^uint(0)>>1)/uint64(C.sizeof_TTSeedSlice) {
		return nil, errors.New("Seed member count overflow")
	}
	var arena unsafe.Pointer
	if len(members) != 0 {
		arena = C.calloc(C.size_t(len(members)), C.sizeof_TTSeedSlice)
		if arena == nil {
			return nil, errors.New("Seed member allocation failed")
		}
		defer C.free(arena)
	}
	spans := unsafe.Slice((*C.TTSeedSlice)(arena), len(members))
	var pins runtime.Pinner
	defer pins.Unpin()
	for i, member := range members {
		if len(member) > 0 {
			pins.Pin(unsafe.SliceData(member))
		}
		spans[i] = seedNativeSlice(member)
	}
	result := C.trunk_seed_pack(d.ptr, (*C.TTSeedSlice)(arena), C.size_t(len(members)), C.uint64_t(hour), C.bool(sealed), C.uint32_t(algorithm), C.uint32_t(level))
	runtime.KeepAlive(members)
	return seedNativeResult(result)
}

// Used with a time-trie BOF address. PAC controls, including the length width,
// are interpreted by Rust. No assumptions about a four-byte length at BOF.
func seedNativeLength(prefix []byte) (uint64, error) {
	var size C.uint64_t
	status := C.trunk_seed_envelope(seedNativeSlice(prefix), &size)
	runtime.KeepAlive(prefix)
	switch status {
	case 0:
		return uint64(size), nil
	case 1:
		return 0, io.ErrUnexpectedEOF
	default:
		return 0, fmt.Errorf("invalid Seed Pack envelope")
	}
}

type seedStrings struct{ ptr unsafe.Pointer }

func openSeedStrings(footer []byte, length uint64) (*seedStrings, seedPublication, error) {
	var info C.TTSeedBoxPublication
	p := C.trunk_seed_strings_open(seedNativeSlice(footer), C.uint64_t(length), &info)
	runtime.KeepAlive(footer)
	if p == nil {
		return nil, seedPublication{}, errors.New("invalid BOX strings footer")
	}
	return &seedStrings{p}, seedPublication{uint64(info.length), uint64(info.count), length - uint64(len(footer))}, nil
}
func (s *seedStrings) close() {
	if s != nil {
		C.trunk_seed_strings_free(s.ptr)
	}
}
func (s *seedStrings) entries(ids []uint32) (*seedNativeOutput, error) {
	if s == nil || len(ids) == 0 {
		return nil, errors.New("unknown BOX string ordinal")
	}
	p := C.trunk_seed_strings(s.ptr, (*C.uint32_t)(unsafe.Pointer(&ids[0])), C.size_t(len(ids)))
	runtime.KeepAlive(ids)
	return seedNativeResult(p)
}

type seedPublication struct{ length, count, bodyEnd uint64 }

func seedPublicationFooter(footer []byte, length uint64) (seedPublication, error) {
	var publication C.TTSeedBoxPublication
	status := C.trunk_seed_packs_publication(seedNativeSlice(footer), C.uint64_t(length), &publication)
	runtime.KeepAlive(footer)
	if status != 0 || uint64(len(footer)) > length {
		return seedPublication{}, fmt.Errorf("invalid Seed publication metadata")
	}
	return seedPublication{uint64(publication.length), uint64(publication.count), length - uint64(len(footer))}, nil
}
func seedSummaryFooter(footer []byte, length uint64) (*seedNativeSummaryIndex, error) {
	ptr := C.trunk_seed_summary_index(seedNativeSlice(footer), C.uint64_t(length))
	runtime.KeepAlive(footer)
	if message := seedNativeText(C.trunk_seed_summary_index_error(ptr)); message != "" {
		C.trunk_seed_summary_index_free(ptr)
		return nil, errors.New(message)
	}
	return &seedNativeSummaryIndex{ptr}, nil
}

// I/O only: Rust locates the footer and interprets its Seed metadata. Reuse the
// existing native pool; no scan, decompression, or retained metadata allocation.
func seedNativePublication(file io.ReaderAt, length uint64, buffer *nativeBuffer) (seedPublication, error) {
	var result seedPublication
	if length < 9 || length > uint64(^uint64(0)>>1) {
		return result, fmt.Errorf("invalid origin Box extent")
	}
	tailSize := min(length, 27)
	tail, err := buffer.resize(int(tailSize))
	if err != nil {
		return result, err
	}
	if _, err = file.ReadAt(tail, int64(length-tailSize)); err != nil {
		return result, err
	}
	var offset C.uint64_t
	status := C.trunk_seed_box_footer(seedNativeSlice(tail), C.uint64_t(length), &offset)
	runtime.KeepAlive(tail)
	if status != 0 || uint64(offset) > length {
		return result, fmt.Errorf("invalid origin Box footer")
	}
	footSize := length - uint64(offset)
	if footSize > uint64(^uint(0)>>1) {
		return result, fmt.Errorf("origin Box footer exceeds host range")
	}
	footer, err := buffer.resize(int(footSize))
	if err != nil {
		return result, err
	}
	if _, err = file.ReadAt(footer, int64(offset)); err != nil {
		return result, err
	}
	return seedPublicationFooter(footer, length)
}

// Rust resolves the registered branch; Go only routes the resulting operation.
func (t *seedNativeTrunk) route(id uint16) (*seedNativeOutput, error) {
	result := C.trunk_seed_trie_route(t.ptr, C.uint16_t(id))
	runtime.KeepAlive(t)
	return seedNativeResult(result)
}

func (t *seedNativeTrunk) statePath(source, definition string, branch uint16) (string, error) {
	src, def := []byte(source), []byte(definition)
	ptr := C.trunk_seed_trie_state_path(t.ptr, seedNativeSlice(src), seedNativeSlice(def), C.uint16_t(branch))
	runtime.KeepAlive(src)
	runtime.KeepAlive(def)
	runtime.KeepAlive(t)
	result, err := seedNativeResult(ptr)
	if err != nil {
		return "", err
	}
	defer result.close()
	return string(result.bytes), nil
}

// A type-scoped writer is used only while holding its commit lane. Node arrays
// are produced by the Rust leaves API; the host never inspects their contents.
type seedNativeLeafWriter struct{ ptr unsafe.Pointer }

func (t *seedNativeTrunk) leafWriter(branch uint16, raw []byte) (*seedNativeLeafWriter, error) {
	ptr := C.trunk_seed_leaf_writer(t.ptr, C.uint16_t(branch), seedNativeSlice(raw))
	runtime.KeepAlive(t)
	runtime.KeepAlive(raw)
	if err := seedNativeText(C.trunk_seed_leaf_writer_error(ptr)); err != "" {
		C.trunk_seed_leaf_writer_free(ptr)
		return nil, errors.New(err)
	}
	return &seedNativeLeafWriter{ptr: ptr}, nil
}
func (l *seedNativeLeafWriter) close() {
	if l != nil && l.ptr != nil {
		C.trunk_seed_leaf_writer_free(l.ptr)
		l.ptr = nil
	}
}
func (l *seedNativeLeafWriter) prepare(branch uint16, first int, nodes []byte) (int64, []byte, error) {
	if first < 0 {
		return 0, nil, errors.New("invalid leaf node ordinal")
	}
	result, err := seedNativeResult(C.trunk_seed_leaf_prepare(l.ptr, C.uint16_t(branch), C.size_t(first), seedNativeSlice(nodes)))
	runtime.KeepAlive(nodes)
	if err != nil {
		return 0, nil, err
	}
	result.close()
	var offset C.size_t
	tail := C.trunk_seed_leaf_tail(l.ptr, &offset)
	if uint64(offset) > uint64(^uint64(0)>>1) {
		l.finish(false)
		return 0, nil, errors.New("leaf file exceeds filesystem addressing")
	}
	return int64(offset), unsafe.Slice((*byte)(unsafe.Pointer(tail.data)), int(tail.len)), nil
}
func (l *seedNativeLeafWriter) finish(written bool) error {
	result, err := seedNativeResult(C.trunk_seed_leaf_finish(l.ptr, C.bool(written)))
	if result != nil {
		result.close()
	}
	return err
}

type seedNativeSummaryIndex struct{ ptr unsafe.Pointer }
type seedSummarySpan struct{ Day, Start, End uint64 }

func seedNativeSummaryFooter(file io.ReaderAt, length uint64, buffer *nativeBuffer) (*seedNativeSummaryIndex, error) {
	if length < 9 || length > uint64(^uint64(0)>>1) {
		return nil, fmt.Errorf("invalid summary Box extent")
	}
	n := min(length, 27)
	tail, err := buffer.resize(int(n))
	if err != nil {
		return nil, err
	}
	if _, err = file.ReadAt(tail, int64(length-n)); err != nil {
		return nil, err
	}
	var offset C.uint64_t
	status := C.trunk_seed_box_footer(seedNativeSlice(tail), C.uint64_t(length), &offset)
	runtime.KeepAlive(tail)
	if status != 0 || uint64(offset) > length || length-uint64(offset) > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("invalid summary Box footer")
	}
	footer, err := buffer.resize(int(length - uint64(offset)))
	if err != nil {
		return nil, err
	}
	if _, err = file.ReadAt(footer, int64(offset)); err != nil {
		return nil, err
	}
	ptr := C.trunk_seed_summary_index(seedNativeSlice(footer), C.uint64_t(length))
	runtime.KeepAlive(footer)
	if message := seedNativeText(C.trunk_seed_summary_index_error(ptr)); message != "" {
		C.trunk_seed_summary_index_free(ptr)
		return nil, errors.New(message)
	}
	return &seedNativeSummaryIndex{ptr}, nil
}
func (s *seedNativeSummaryIndex) close() {
	if s != nil && s.ptr != nil {
		C.trunk_seed_summary_index_free(s.ptr)
		s.ptr = nil
	}
}
func (s *seedNativeSummaryIndex) spans(t *seedNativeTime, ranges []summaryRange) ([]seedSummarySpan, error) {
	values := make([][2]uint64, len(ranges))
	for i, r := range ranges {
		values[i] = [2]uint64{uint64(r.from), uint64(r.to)}
	}
	raw, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}
	result, err := seedNativeResult(C.trunk_seed_summary_spans(s.ptr, t.ptr, seedNativeSlice(raw)))
	runtime.KeepAlive(raw)
	if err != nil {
		return nil, err
	}
	defer result.close()
	var spans []seedSummarySpan
	err = json.Unmarshal(result.bytes, &spans)
	return spans, err
}

func seedNativePrepareSummaries(trie, rx, target, previous []byte) (tree, summaries []byte, err error) {
	result, err := seedNativeResult(C.trunk_prepare_summaries(seedNativeSlice(trie), seedNativeSlice(rx), seedNativeSlice(target), seedNativeSlice(previous)))
	runtime.KeepAlive(trie)
	runtime.KeepAlive(rx)
	runtime.KeepAlive(target)
	runtime.KeepAlive(previous)
	if err != nil {
		return nil, nil, err
	}
	defer result.close()
	// Rust returns opaque bytes, not a host-side Definition representation.
	var value struct {
		Trie []byte
		RX   []byte
	}
	if err = json.Unmarshal(result.bytes, &value); err != nil {
		return nil, nil, err
	}
	return value.Trie, value.RX, nil
}

func (t *seedNativeTrunk) labels(branch uint16, raw []byte) (*seedNativeOutput, error) {
	ptr := C.trunk_seed_leaf_labels(t.ptr, C.uint16_t(branch), seedNativeSlice(raw))
	runtime.KeepAlive(t)
	runtime.KeepAlive(raw)
	return seedNativeResult(ptr)
}
func (t *seedNativeTrunk) label(branch, leaf uint16, raw, label []byte) (*seedNativeOutput, error) {
	ptr := C.trunk_seed_leaf_label(t.ptr, C.uint16_t(branch), seedNativeSlice(raw), C.uint16_t(leaf), seedNativeSlice(label))
	runtime.KeepAlive(t)
	runtime.KeepAlive(raw)
	runtime.KeepAlive(label)
	return seedNativeResult(ptr)
}
