package cache

/*
#include "collection_native.h"
*/
import "C"

import (
	"errors"
	"os"
	"runtime"
	"runtime/cgo"
	"sync"
	"syscall"
	"unsafe"
)

const collectionMaxRevision = 1<<24 - 1

type objectBinding struct {
	Minor      uint32
	Generation [16]byte
}

func (k objectBinding) native() (out C.TTBinding) {
	out.minor = C.uint32_t(k.Minor)
	*(*[16]byte)(unsafe.Pointer(&out.generation)) = k.Generation
	return
}

type collectionAddress struct{ Hour, Record uint32 }

func (a collectionAddress) packed() uint64 { return uint64(a.Hour)<<32 | uint64(a.Record) }
func unpackCollection(v uint64) collectionAddress {
	return collectionAddress{uint32(v >> 32), uint32(v)}
}

type collectionPosting struct {
	Object   [16]byte
	Revision uint64
	Address  collectionAddress
}
type collectionSource struct {
	Width   byte
	Through uint64
}
type collectionReadError struct{ error }
type objectIndex struct {
	file    *os.File
	native  *C.TTCollection
	sources map[objectBinding]*collectionSource
	through map[objectBinding]uint64
	fault   func(string) error
}

func collectionError(e *C.TTCollectionError) error {
	if e.message[0] == 0 {
		return nil
	}
	if e.os_error != 0 {
		return &os.PathError{Op: "collection index", Err: syscall.Errno(e.os_error)}
	}
	return errors.New(C.GoString(&e.message[0]))
}
func loadObjectIndex(f *os.File) (*objectIndex, error) {
	var e C.TTCollectionError
	i := &objectIndex{file: f, sources: map[objectBinding]*collectionSource{}, through: map[objectBinding]uint64{}}
	fd := f.Fd()
	if fd == ^uintptr(0) {
		return nil, os.ErrClosed
	}
	i.native = C.tt_collection_open(C.int32_t(fd), &e)
	runtime.KeepAlive(f)
	if err := collectionError(&e); err != nil {
		return nil, err
	}
	runtime.SetFinalizer(i, (*objectIndex).release)
	h := cgo.NewHandle(i)
	defer h.Delete()
	C.tt_collection_sources(i.native, C.uintptr_t(h), (*[0]byte)(C.goCollectionSource))
	return i, nil
}

//export goCollectionSource
func goCollectionSource(ctx C.uintptr_t, minor C.uint32_t, id *C.uint8_t, width C.uint8_t, through C.uint64_t) {
	i := cgo.Handle(ctx).Value().(*objectIndex)
	k := objectBinding{uint32(minor), *(*[16]byte)(unsafe.Pointer(id))}
	i.sources[k] = &collectionSource{byte(width), uint64(through)}
	i.through[k] = uint64(through)
}
func (i *objectIndex) release() {
	if i.native != nil {
		C.tt_collection_close(i.native)
		i.native = nil
	}
}
func (i *objectIndex) close() error                  { i.release(); return i.file.Close() }
func (i *objectIndex) replaceWith(next *objectIndex) { i.release(); *i = *next; next.native = nil }
func (i *objectIndex) append(k objectBinding, width byte, from, through uint64, entries []collectionPosting) error {
	return i.write(k, width, from, through, entries, false)
}
func (i *objectIndex) mergeRetained(k objectBinding, width byte, through uint64, entries []collectionPosting) error {
	return i.write(k, width, 0, through, entries, true)
}

//export goCollectionHook
func goCollectionHook(ctx C.uintptr_t, after C.bool) C.bool {
	stage := "before-root"
	if after {
		stage = "after-root"
	}
	return C.bool(cgo.Handle(ctx).Value().(func(string) error)(stage) == nil)
}
func (i *objectIndex) write(k objectBinding, width byte, from, through uint64, entries []collectionPosting, merge bool) error {
	posts := make([]C.TTPosting, len(entries))
	for n, p := range entries {
		if p.Revision > collectionMaxRevision {
			return errors.New("collection revision exceeds U24")
		}
		dst := &posts[n]
		*(*[16]byte)(unsafe.Pointer(&dst.object)) = p.Object
		dst.revision = C.uint32_t(p.Revision)
		dst.hour = C.uint32_t(p.Address.Hour)
		dst.record = C.uint32_t(p.Address.Record)
	}
	var e C.TTCollectionError
	var h cgo.Handle
	var hook *[0]byte
	if i.fault != nil {
		h = cgo.NewHandle(i.fault)
		defer h.Delete()
		hook = (*[0]byte)(C.goCollectionHook)
	}
	C.tt_collection_append(i.native, k.native(), C.uint8_t(width), C.uint64_t(from), C.uint64_t(through), C.bool(merge), unsafe.SliceData(posts), C.size_t(len(posts)), C.uintptr_t(h), hook, &e)
	if err := collectionError(&e); err != nil {
		return err
	}
	i.sources[k] = &collectionSource{width, through}
	i.through[k] = through
	return nil
}
func (i *objectIndex) point(id []byte, k objectBinding, revision uint64, latest bool) (uint64, collectionAddress, bool, error) {
	if len(id) != 16 || revision > collectionMaxRevision {
		return 0, collectionAddress{}, false, nil
	}
	var key C.TTLookupKey
	*(*[16]byte)(unsafe.Pointer(&key.id)) = [16]byte(id)
	result := C.tt_collection_get(i.native, k.native(), key, C.uint32_t(revision), C.bool(latest))
	runtime.KeepAlive(i)
	if result.error != nil {
		defer C.tt_collection_error_free(result.error)
		return 0, collectionAddress{}, false, &collectionReadError{collectionError(result.error)}
	}
	return uint64(result.revision), unpackCollection(uint64(result.address)), result.address != 0, nil
}
func (i *objectIndex) lookup(id []byte, k objectBinding, rev uint64) (collectionAddress, bool, error) {
	_, a, ok, e := i.point(id, k, rev, false)
	return a, ok, e
}
func (i *objectIndex) latest(id []byte, k objectBinding) (uint64, collectionAddress, bool, error) {
	return i.point(id, k, 0, true)
}

type collectionView struct{ native *C.TTCollectionSnapshot }

func (i *objectIndex) pin() *collectionView {
	v := &collectionView{C.tt_collection_pin(i.native)}
	runtime.KeepAlive(i)
	return v
}
func releaseCollection(v *collectionView) { C.tt_collection_release(v.native) }

type collectionVisitor struct {
	fn  func([16]byte, uint64, collectionAddress) error
	err error
}

//export goCollectionVisit
func goCollectionVisit(ctx C.uintptr_t, id *C.uint8_t, r C.uint32_t, v C.uint64_t) C.bool {
	c := cgo.Handle(ctx).Value().(*collectionVisitor)
	if c.err != nil {
		return false
	}
	c.err = c.fn(*(*[16]byte)(unsafe.Pointer(id)), uint64(r), unpackCollection(uint64(v)))
	return C.bool(c.err == nil)
}

var historyBuffers = sync.Pool{New: func() any { return new(C.TTHistoryBatch) }}

func readHistory(idx *C.TTCollection, view *C.TTCollectionSnapshot, id []byte, k objectBinding, fn func(uint64, collectionAddress) error) error {
	if len(id) != 16 {
		return errors.New("invalid object UUID")
	}
	var key C.TTLookupKey
	*(*[16]byte)(unsafe.Pointer(&key.id)) = [16]byte(id)
	batch := historyBuffers.Get().(*C.TTHistoryBatch)
	var cursor *C.TTHistoryCursor
	defer func() {
		if cursor != nil {
			C.tt_history_close(cursor)
		}
		historyBuffers.Put(batch)
	}()
	for {
		out := C.tt_history_page(idx, view, k.native(), key, cursor, batch)
		cursor = out.cursor
		idx, view = nil, nil // A continuation owns the pinned snapshot.
		if out.error != nil {
			defer C.tt_collection_error_free(out.error)
			return collectionError(out.error)
		}
		for _, row := range batch.rows[:out.len] {
			if err := fn(uint64(row.revision), collectionAddress{uint32(row.hour), uint32(row.record)}); err != nil {
				return err
			}
		}
		if cursor == nil {
			return nil
		}
	}
}
func (v *collectionView) visit(id []byte, k objectBinding, fn func([16]byte, uint64, collectionAddress) error) error {
	if id != nil {
		if len(id) != 16 {
			return errors.New("invalid object UUID")
		}
		key := [16]byte(id)
		return readHistory(nil, v.native, id, k, func(r uint64, a collectionAddress) error { return fn(key, r, a) })
	}
	c := &collectionVisitor{fn: fn}
	h := cgo.NewHandle(c)
	defer h.Delete()
	var e C.TTCollectionError
	C.tt_collection_each(v.native, k.native(), C.uintptr_t(h), (*[0]byte)(C.goCollectionVisit), &e)
	if c.err != nil {
		return c.err
	}
	return collectionError(&e)
}
func (i *objectIndex) visit(id []byte, k objectBinding, fn func(uint64, collectionAddress)) error {
	err := readHistory(i.native, nil, id, k, func(r uint64, a collectionAddress) error { fn(r, a); return nil })
	runtime.KeepAlive(i)
	return err
}
func (i *objectIndex) each(k objectBinding, fn func([16]byte, uint64, collectionAddress) error) error {
	v := i.pin()
	defer releaseCollection(v)
	return v.visit(nil, k, fn)
}

func (i *objectIndex) pages(k objectBinding) (int, int, int) {
	var counts [3]C.size_t
	C.tt_collection_pages(i.native, k.native(), &counts[0])
	runtime.KeepAlive(i)
	return int(counts[0]), int(counts[1]), int(counts[2])
}
