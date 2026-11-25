package cache

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// One lane per type and major, shared by splits and minor revisions. It retains
// the Rust writer between commits; normal reads retain immutable snapshots.
type registeredLeafWriter struct {
	mu       sync.Mutex
	registry *registeredRouteReader
	native   *seedNativeLeafWriter
	file     *os.File
}

func (w *registeredLeafWriter) close() {
	w.native.close()
	w.native = nil
	w.registry = nil
	if w.file != nil {
		w.file.Close()
		w.file = nil
	}
}

// Enroll in endpoint shutdown before waiting on the per-file writer.
func (p *Endpoint) acquireLeafWriter(path string) (*registeredLeafWriter, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("endpoint closed")
	}
	writer := p.registeredLeafWriters[path]
	if writer == nil {
		writer = &registeredLeafWriter{}
		p.registeredLeafWriters[path] = writer
	}
	p.commitWork.Add(1)
	p.mu.Unlock()
	writer.mu.Lock()
	return writer, nil
}

func (p *Endpoint) releaseLeafWriter(writer *registeredLeafWriter) {
	writer.mu.Unlock()
	p.commitWork.Done()
}

// commitLeafBatch is the state publication step of an hourly commit. The
// caller supplies the node batch produced by Rust, after packing that hour.
// It must complete before publishing Pack/index references to those nodes.
func (p *Endpoint) commitLeafBatch(branch uint16, hour time.Time, first int, nodes []byte) error {
	if hour.UnixMilli()%TimeHourMS != 0 {
		return fmt.Errorf("commit requires an hour boundary")
	}
	if p.config.Now().Before(hour.Add(time.Hour + 15*time.Minute)) {
		return fmt.Errorf("hour still accepts stragglers")
	}
	route, err := taxonomyRoute(p.config.Root, branch)
	if err != nil {
		return err
	}
	if route.Cache != p.config.Cache {
		return fmt.Errorf("leaf commit outside endpoint cache")
	}
	reader, err := taxonomyReader(p.config.Root)
	if err != nil {
		return err
	}
	path, err := reader.trie.statePath(route.Source, route.Digest, branch)
	if err != nil {
		return err
	}
	writer, err := p.acquireLeafWriter(path)
	if err != nil {
		return err
	}
	defer p.releaseLeafWriter(writer)
	// Never hold the endpoint publication lock while waiting for a permit.
	release := p.config.CommitScheduler.acquire()
	defer release()
	full := filepath.Join(p.config.Root, filepath.FromSlash(path))
	if err = os.MkdirAll(filepath.Dir(full), 0700); err != nil {
		return err
	}
	unlock, err := lockRegistration(filepath.Dir(full), true)
	if err != nil {
		return err
	}
	defer unlock()
	if writer.file != nil {
		info, e := writer.file.Stat()
		if e != nil {
			return e
		}
		current, e := os.Stat(full)
		if e != nil {
			return e
		}
		if !os.SameFile(info, current) {
			writer.close()
		}
	}
	if writer.registry != reader {
		// Only a new registration snapshot needs recompilation/resume. The
		// scoped Rust writer rejects unrelated types and frozen version roots.
		if err = prepareRegisteredDefinitions(p.config.Root, p.config.Cache); err != nil {
			return err
		}
		snapshot, e := p.registeredLeaf(path)
		if e != nil && !os.IsNotExist(e) {
			return e
		}
		var raw []byte
		if snapshot != nil {
			raw = snapshot.bytes
		}
		next, e := reader.trie.leafWriter(branch, raw)
		if e != nil {
			return e
		}
		writer.native.close()
		writer.native = next
		writer.registry = reader
	}
	offset, tail, err := writer.native.prepare(branch, first, nodes)
	if err != nil {
		return err
	}
	accepted := false
	defer func() {
		if !accepted {
			_ = writer.native.finish(false)
		}
	}()
	// Readers without a previous publication must not discover a partially
	// written initial file. Existing readers retain their original snapshot.
	p.mu.Lock()
	p.publishingLeaves[path] = true
	p.mu.Unlock()

	if writer.file == nil {
		writer.file, err = os.OpenFile(full, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
	}
	// Same append-only node region and replaceable footer as the existing
	// writer. The new root and its directory are one tail write, followed by
	// the existing durability boundary. No old node prefix is rewritten.
	n, err := writer.file.WriteAt(tail, offset)
	if err != nil {
		return err
	}
	if n != len(tail) {
		return io.ErrShortWrite
	}
	if err = writer.file.Truncate(offset + int64(len(tail))); err != nil {
		return err
	}
	if err = writer.file.Sync(); err != nil {
		return err
	}
	if err = writer.native.finish(true); err != nil {
		return err
	}
	accepted = true
	p.mu.Lock()
	delete(p.registeredLeaves, path)
	delete(p.publishingLeaves, path)
	p.mu.Unlock()
	p.config.CommitScheduler.observeWindow(hour.Add(time.Hour), p.config.Now())
	return nil
}
