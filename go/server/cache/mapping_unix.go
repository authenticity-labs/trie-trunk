//go:build darwin || linux

package cache

import (
	"fmt"
	"golang.org/x/sys/unix"
	"sync/atomic"
)

type residentMapping struct {
	refs atomic.Int64
	raw  []byte
}

func (m *residentMapping) release() {
	if m != nil && m.refs.Add(-1) == 0 {
		_ = unix.Munmap(m.raw)
	}
}

func (b *branchStore) mapIndex() error {
	stat, e := b.idx.Stat()
	if e != nil {
		return e
	}
	size := stat.Size()
	if size < headerSize || int64(int(size)) != size {
		return fmt.Errorf("index mapping too large")
	}
	mapped, e := unix.Mmap(int(b.idx.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if e != nil {
		return e
	}
	if e = b.unmapIndex(); e != nil {
		unix.Munmap(mapped)
		return e
	}
	b.readerIndex = &residentMapping{raw: mapped}
	b.readerIndex.refs.Store(1)
	b.mapping = mapped
	b.rows = mapped[headerSize:]
	return nil
}
func (b *branchStore) unmapIndex() error {
	if b.mapping == nil {
		return nil
	}
	b.readerIndex.release()
	b.readerIndex = nil
	var e error
	b.mapping = nil
	b.rows = nil
	return e
}

// A separate mapping owns its lifetime. Borrowing b.rows across an unlocked
// callback is unsafe: an append remaps it and retention unmaps the old branch.
func (b *branchStore) snapshotIndex() ([]byte, func(), error) {
	if len(b.rows) == 0 {
		return nil, func() {}, nil
	}
	mapped, err := unix.Mmap(int(b.idx.Fd()), 0, headerSize+len(b.rows), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}
	return mapped[headerSize:], func() { _ = unix.Munmap(mapped) }, nil
}
