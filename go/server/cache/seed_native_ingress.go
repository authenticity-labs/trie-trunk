package cache

/*
#include "seed_native.h"
*/
import "C"

import (
	"encoding/json"
	"errors"
	"os"
	"runtime"
	"unsafe"
)

type seedNativeIngress struct{ ptr unsafe.Pointer }

func (i *seedNativeIngress) inspect(branch uint16, pill []byte, now uint64) (uint64, error) {
	out, err := seedNativeResult(C.trunk_seed_ingress_inspect(i.ptr, C.uint16_t(branch), seedNativeSlice(pill), C.uint64_t(now)))
	runtime.KeepAlive(i)
	runtime.KeepAlive(pill)
	if err != nil {
		return 0, err
	}
	defer out.close()
	var timestamp uint64
	err = json.Unmarshal(out.bytes, &timestamp)
	return timestamp, err
}
func (i *seedNativeIngress) persist(branch uint16, pill []byte, now uint64, file *os.File) error {
	out, err := seedNativeResult(C.trunk_seed_ingress_persist(i.ptr, C.uint16_t(branch), seedNativeSlice(pill), C.uint64_t(now), C.int32_t(file.Fd())))
	runtime.KeepAlive(i)
	runtime.KeepAlive(pill)
	runtime.KeepAlive(file)
	if out != nil {
		out.close()
	}
	return err
}

type seedStorage struct {
	Suffix      string `json:"suffix"`
	Alternative string `json:"alternative"`
	Packed      bool   `json:"packed"`
}

func storagePolicy(name string) (seedStorage, error) {
	bytes := []byte(name)
	raw := C.trunk_seed_storage(seedNativeSlice(bytes))
	runtime.KeepAlive(bytes)
	out, err := seedNativeResult(raw)
	var policy seedStorage
	if err != nil {
		return policy, err
	}
	defer out.close()
	err = json.Unmarshal(out.bytes, &policy)
	return policy, err
}

func (t *seedNativeTrunk) ingressEnabled(branch uint16) (bool, error) {
	result := C.trunk_seed_ingress_enabled(t.ptr, C.uint16_t(branch))
	runtime.KeepAlive(t)
	out, err := seedNativeResult(result)
	if err != nil {
		return false, err
	}
	defer out.close()
	var enabled bool
	err = json.Unmarshal(out.bytes, &enabled)
	return enabled, err
}

func (t *seedNativeTrunk) ingress(branch uint16, maximum, pillMaximum int, through uint64) (*seedNativeIngress, error) {
	ptr := C.trunk_seed_ingress_open(t.ptr, C.uint16_t(branch), C.size_t(maximum), C.size_t(pillMaximum), C.uint64_t(through))
	runtime.KeepAlive(t)
	if message := seedNativeText(C.trunk_seed_ingress_error(ptr)); message != "" {
		C.trunk_seed_ingress_free(ptr)
		return nil, errors.New(message)
	}
	return &seedNativeIngress{ptr}, nil
}
func (i *seedNativeIngress) close() {
	if i != nil && i.ptr != nil {
		C.trunk_seed_ingress_free(i.ptr)
		i.ptr = nil
	}
}
func (i *seedNativeIngress) load(branch uint16, pill []byte, now uint64, recover bool) (*uint64, error) {
	result := C.trunk_seed_ingress_load(i.ptr, C.uint16_t(branch), seedNativeSlice(pill), C.uint64_t(now), C.bool(recover))
	runtime.KeepAlive(i)
	runtime.KeepAlive(pill)
	out, err := seedNativeResult(result)
	if err != nil {
		return nil, err
	}
	defer out.close()
	var value *uint64
	err = json.Unmarshal(out.bytes, &value)
	return value, err
}
func (i *seedNativeIngress) snapshot(hour uint64) (*seedNativeOutput, error) {
	out := C.trunk_seed_ingress_snapshot(i.ptr, C.uint64_t(hour))
	runtime.KeepAlive(i)
	return seedNativeResult(out)
}
func (i *seedNativeIngress) hours() ([]uint64, error) {
	raw := C.trunk_seed_ingress_hours(i.ptr)
	runtime.KeepAlive(i)
	out, err := seedNativeResult(raw)
	if err != nil {
		return nil, err
	}
	defer out.close()
	var hours []uint64
	err = json.Unmarshal(out.bytes, &hours)
	return hours, err
}

type seedCommits struct {
	Head    uint64   `json:"head"`
	Through uint64   `json:"through"`
	Hours   []uint64 `json:"hours"`
}

func (i *seedNativeIngress) commits(now uint64) (seedCommits, error) {
	raw := C.trunk_seed_ingress_commits(i.ptr, C.uint64_t(now))
	runtime.KeepAlive(i)
	out, err := seedNativeResult(raw)
	var plan seedCommits
	if err != nil {
		return plan, err
	}
	defer out.close()
	err = json.Unmarshal(out.bytes, &plan)
	return plan, err
}
func seedSpoolPrefix(branch uint16, pill []byte) (*seedNativeOutput, error) {
	out := C.trunk_seed_spool_prefix(C.uint16_t(branch), seedNativeSlice(pill))
	runtime.KeepAlive(pill)
	return seedNativeResult(out)
}
func seedSpoolRead(raw []byte, maximum int) (branch uint16, length int, complete bool, err error) {
	var id C.uint16_t
	var size C.size_t
	status := C.trunk_seed_spool_read(seedNativeSlice(raw), C.size_t(maximum), &id, &size)
	runtime.KeepAlive(raw)
	if status < 0 {
		err = errors.New("invalid ingress recovery record")
		return
	}
	return uint16(id), int(size), status == 0, nil
}

type seedNativeSeal struct{ ptr unsafe.Pointer }

func (i *seedNativeIngress) seal(hour, now uint64) (*seedNativeSeal, error) {
	ptr := C.trunk_seed_ingress_seal(i.ptr, C.uint64_t(hour), C.uint64_t(now))
	runtime.KeepAlive(i)
	if message := seedNativeText(C.trunk_seed_ingress_seal_error(ptr)); message != "" {
		C.trunk_seed_ingress_seal_free(ptr)
		return nil, errors.New(message)
	}
	return &seedNativeSeal{ptr}, nil
}
func (s *seedNativeSeal) bytes(index bool) []byte {
	span := C.trunk_seed_ingress_seal_bytes(s.ptr, C.bool(index))
	return unsafe.Slice((*byte)(unsafe.Pointer(span.data)), int(span.len))
}
func (s *seedNativeSeal) close() {
	if s != nil && s.ptr != nil {
		C.trunk_seed_ingress_seal_free(s.ptr)
		s.ptr = nil
	}
}
func (i *seedNativeIngress) published(hour, through uint64) error {
	ptr := C.trunk_seed_ingress_published(i.ptr, C.uint64_t(hour), C.uint64_t(through))
	runtime.KeepAlive(i)
	out, err := seedNativeResult(ptr)
	if out != nil {
		out.close()
	}
	return err
}

func (t *seedNativeTrunk) ingressBootstrap(branch uint16, now uint64, generation []byte, kind uint32) (*seedNativeOutput, error) {
	ptr := C.trunk_seed_ingress_bootstrap(t.ptr, C.uint16_t(branch), C.uint64_t(now), seedNativeSlice(generation), C.uint32_t(kind))
	runtime.KeepAlive(t)
	runtime.KeepAlive(generation)
	return seedNativeResult(ptr)
}

type seedNativeCommit struct {
	ptr                                      unsafe.Pointer
	offset, length, beds, index, hour, count uint64
}

func publicationResult(ptr unsafe.Pointer) (*seedNativeCommit, error) {
	if message := seedNativeText(C.trunk_seed_ingress_prepare_error(ptr)); message != "" {
		C.trunk_seed_ingress_prepare_free(ptr)
		return nil, errors.New(message)
	}
	var info C.TTSeedPublicationPlan
	C.trunk_seed_ingress_prepare_bytes(ptr, 0, &info)
	return &seedNativeCommit{ptr, uint64(info.offset), uint64(info.length), uint64(info.beds), uint64(info.index), uint64(info.hour), uint64(info.count)}, nil
}
func (s *seedNativeSeal) prepare(time, footer []byte, length uint64, packed bool) (*seedNativeCommit, error) {
	ptr := C.trunk_seed_ingress_prepare(s.ptr, seedNativeSlice(time), seedNativeSlice(footer), C.uint64_t(length), C.bool(packed))
	runtime.KeepAlive(s)
	runtime.KeepAlive(time)
	runtime.KeepAlive(footer)
	return publicationResult(ptr)
}
func (p *seedNativeCommit) bytes(kind uint32) []byte {
	span := C.trunk_seed_ingress_prepare_bytes(p.ptr, C.uint32_t(kind), nil)
	return unsafe.Slice((*byte)(unsafe.Pointer(span.data)), int(span.len))
}
func (p *seedNativeCommit) checkpoint() (*seedNativeOutput, error) {
	ptr := C.trunk_seed_ingress_checkpoint(p.ptr)
	runtime.KeepAlive(p)
	return seedNativeResult(ptr)
}
func (p *seedNativeCommit) publish(prefix string, packed bool, budget uint64) error {
	path := []byte(prefix)
	out, err := seedNativeResult(C.trunk_seed_ingress_publish(p.ptr, seedNativeSlice(path), C.bool(packed), C.size_t(budget)))
	runtime.KeepAlive(p)
	runtime.KeepAlive(path)
	if out != nil {
		out.close()
	}
	return err
}
func (p *seedSummaryWrite) publish(prefix string, budget uint64) error {
	path := []byte(prefix)
	out, err := seedNativeResult(C.trunk_seed_summary_publish(p.ptr, seedNativeSlice(path), C.size_t(budget)))
	runtime.KeepAlive(p)
	runtime.KeepAlive(path)
	if out != nil {
		out.close()
	}
	return err
}
func (p *seedNativeCommit) close() {
	if p != nil && p.ptr != nil {
		C.trunk_seed_ingress_prepare_free(p.ptr)
		p.ptr = nil
	}
}
func seedCheckpointOpen(raw []byte) (*seedNativeCommit, error) {
	ptr := C.trunk_seed_ingress_checkpoint_open(seedNativeSlice(raw))
	runtime.KeepAlive(raw)
	return publicationResult(ptr)
}

func (i *seedNativeIngress) complete(timeline []byte, now uint64) (*seedNativeOutput, error) {
	ptr := C.trunk_seed_ingress_complete(i.ptr, seedNativeSlice(timeline), C.uint64_t(now))
	runtime.KeepAlive(i)
	runtime.KeepAlive(timeline)
	return seedNativeResult(ptr)
}

func seedQueryDefinition() (*seedNativeOutput, error) {
	return seedNativeResult(C.trunk_seed_query_definition())
}

type seedSummaryWrite struct {
	ptr             unsafe.Pointer
	Trie, Rx, Bytes []byte
	File            string
	Offset, Length  uint64
}

func (p *seedSummaryWrite) close() {
	if p != nil && p.ptr != nil {
		C.trunk_seed_summary_free(p.ptr)
		p.ptr = nil
		p.Trie, p.Rx, p.Bytes = nil, nil, nil
	}
}
func summaryResult(ptr unsafe.Pointer) (*seedSummaryWrite, error) {
	if message := seedNativeText(C.trunk_seed_summary_error(ptr)); message != "" {
		C.trunk_seed_summary_free(ptr)
		return nil, errors.New(message)
	}
	var offset, length C.uint64_t
	span := C.trunk_seed_summary_bytes(ptr, 0, &offset, &length)
	bytes := func(kind uint32) []byte {
		v := C.trunk_seed_summary_bytes(ptr, C.uint32_t(kind), nil, nil)
		return unsafe.Slice((*byte)(unsafe.Pointer(v.data)), int(v.len))
	}
	return &seedSummaryWrite{ptr: ptr, Bytes: unsafe.Slice((*byte)(unsafe.Pointer(span.data)), int(span.len)), Trie: bytes(1), Rx: bytes(2), File: string(bytes(3)), Offset: uint64(offset), Length: uint64(length)}, nil
}

func (i *seedNativeIngress) summaryStart(t *seedNativeTrunk, timeline, rx, footer []byte, length uint64) (*seedSummaryStart, error) {
	out, err := seedNativeResult(C.trunk_seed_ingress_summary_start(i.ptr, t.ptr, seedNativeSlice(timeline), seedNativeSlice(rx), seedNativeSlice(footer), C.uint64_t(length)))
	runtime.KeepAlive(i)
	runtime.KeepAlive(t)
	runtime.KeepAlive(timeline)
	runtime.KeepAlive(rx)
	runtime.KeepAlive(footer)
	if err != nil {
		return nil, err
	}
	defer out.close()
	if len(out.bytes) == 0 {
		return nil, nil
	}
	var v seedSummaryStart
	err = json.Unmarshal(out.bytes, &v)
	return &v, err
}
func (i *seedNativeIngress) summaryRestore(pack []byte) error {
	out, err := seedNativeResult(C.trunk_seed_ingress_summary_restore(i.ptr, seedNativeSlice(pack)))
	runtime.KeepAlive(i)
	runtime.KeepAlive(pack)
	if out != nil {
		out.close()
	}
	return err
}
func (i *seedNativeIngress) summarySnapshot(trie []byte, day uint64) ([]byte, error) {
	out, err := seedNativeResult(C.trunk_seed_ingress_summary_snapshot(i.ptr, seedNativeSlice(trie), C.uint64_t(day)))
	runtime.KeepAlive(i)
	runtime.KeepAlive(trie)
	if err != nil {
		return nil, err
	}
	defer out.close()
	// Own the response after the native output is released. No schema or data
	// conversion crosses this boundary when the registry context is unchanged.
	return append([]byte(nil), out.bytes...), nil
}
func (i *seedNativeIngress) summary(trie []byte, day, through uint64, footer []byte, length uint64, seal bool) (*seedSummaryWrite, []byte, error) {
	ptr := C.trunk_seed_ingress_summary(i.ptr, seedNativeSlice(trie), C.uint64_t(day), C.uint64_t(through), seedNativeSlice(footer), C.uint64_t(length), C.bool(seal))
	runtime.KeepAlive(i)
	runtime.KeepAlive(trie)
	runtime.KeepAlive(footer)
	result, err := summaryResult(ptr)
	if err != nil {
		return nil, nil, err
	}
	var checkpoint []byte
	if seal {
		v, e := seedNativeResult(C.trunk_seed_summary_checkpoint(ptr))
		runtime.KeepAlive(result)
		if e != nil {
			result.close()
			return nil, nil, e
		}
		checkpoint = append([]byte(nil), v.bytes...)
		v.close()
	}
	return result, checkpoint, nil
}

func (i *seedNativeIngress) summaryPublished(day uint64) error {
	out, err := seedNativeResult(C.trunk_seed_ingress_summary_published(i.ptr, C.uint64_t(day)))
	runtime.KeepAlive(i)
	if out != nil {
		out.close()
	}
	return err
}
func seedSummaryCheckpoint(raw []byte) (*seedSummaryWrite, error) {
	ptr := C.trunk_seed_summary_restore(seedNativeSlice(raw))
	runtime.KeepAlive(raw)
	return summaryResult(ptr)
}

type seedSummaryStart struct {
	First     uint64
	Bootstrap []byte
}
