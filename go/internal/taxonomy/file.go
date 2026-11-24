package taxonomy

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type writable interface {
	WriteAt([]byte, int64) (int, error)
	Sync() error
	Truncate(int64) error
}

func writeAt(f writable, b []byte, at int64) error {
	n, err := f.WriteAt(b, at)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return err
}
func publish(f writable, c commit) error {
	if err := writeAt(f, encodeCommit(c), int64(8+c.slot*slotBytes)); err != nil {
		return err
	}
	return f.Sync()
}

// Re-tail publication preserves the current directory before reusing its space.
// Both commit slots belong to this resource header, not to a journal. Readers
// retain immutable snapshots; this function is called only by registration.
func retail(f writable, before []byte, next *Trie) ([]byte, error) {
	old, err := Load(before)
	if err != nil {
		return nil, err
	}
	return retailKnown(f, before, old, next)
}
func retailKnown(f writable, before []byte, old, next *Trie) ([]byte, error) {
	if !next.Extends(old) {
		return nil, fmt.Errorf("immutable taxonomy changed")
	}
	c, err := selectCommit(before)
	if err != nil {
		return nil, err
	}
	if len(next.nodes) == len(old.nodes) && next.directoryBytes == old.directoryBytes && next.sameSections(old) {
		return append([]byte(nil), before[:c.end]...), nil
	}
	if c.generation > ^uint64(0)-2 {
		return nil, fmt.Errorf("taxonomy commit generation exhausted")
	}
	raw := next.Bytes()
	oldNodesEnd := headerBytes + int(c.count)*nodeBytes
	newNodesEnd := headerBytes + (len(next.nodes)-1)*nodeBytes
	oldFooterStart := old.footerStart(int(c.end))
	footer := before[oldFooterStart:c.end]
	// This copy is beyond both the current file and the complete new image. New
	// nodes and the new canonical footer cannot overlap the recovery directory.
	safe := max(len(before), len(raw))
	if err = writeAt(f, footer, int64(safe)); err != nil {
		return nil, err
	}
	if err = f.Sync(); err != nil {
		return nil, err
	}
	backup := commit{c.generation + 1, c.count, c.leaves, uint64(safe + len(footer)), 1 - c.slot}
	if err = publish(f, backup); err != nil {
		return nil, err
	}
	// Existing nodes are never written. The directory copy is now authoritative.
	if err = writeAt(f, raw[oldNodesEnd:newNodesEnd], int64(oldNodesEnd)); err != nil {
		return nil, err
	}
	if err = writeAt(f, raw[newNodesEnd:], int64(newNodesEnd)); err != nil {
		return nil, err
	}
	if err = f.Sync(); err != nil {
		return nil, err
	}
	final := commit{c.generation + 2, uint32(len(next.nodes) - 1), uint32(len(next.leaves) - 1), uint64(len(raw)), c.slot}
	if err = publish(f, final); err != nil {
		return nil, err
	}
	// The authoritative commit bounds all reads even if a crash interrupts the
	// truncation. The obsolete copy lies outside the committed resource image.
	if err = f.Truncate(int64(len(raw))); err != nil {
		return nil, err
	}
	if err = f.Sync(); err != nil {
		return nil, err
	}
	copy(raw[:headerBytes], before[:headerBytes])
	copy(raw[8+backup.slot*slotBytes:], encodeCommit(backup))
	copy(raw[8+final.slot*slotBytes:], encodeCommit(final))
	return raw, nil
}

// Save publishes one registration. New files use an atomic initial rename;
// subsequent registrations append nodes in place and re-tail the directory.
func Save(path string, t *Trie) ([]byte, error) {
	before, err := os.ReadFile(path)
	if err == nil {
		f, err := os.OpenFile(path, os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		raw, err := retail(f, before, t)
		closeErr := f.Close()
		if err != nil {
			return nil, err
		}
		return raw, closeErr
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	raw := t.Bytes()
	if _, err = Load(raw); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".trunk.trie-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())
	if err = writeAt(f, raw, 0); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return nil, err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	err = dir.Sync()
	closeErr = dir.Close()
	if err != nil {
		return nil, err
	}
	return raw, closeErr
}

// SavePrepared uses the registration owner's committed memtable and snapshot.
// The owner serializes registration for this file; searches never acquire it.
func SavePrepared(path string, before []byte, old, next *Trie) ([]byte, error) {
	if old == nil {
		return Save(path, next)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	raw, err := retailKnown(f, before, old, next)
	closeErr := f.Close()
	if err != nil {
		return nil, err
	}
	return raw, closeErr
}
