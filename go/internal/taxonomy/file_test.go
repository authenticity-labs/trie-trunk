package taxonomy

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

var interrupted = errors.New("power loss")

type crashFile struct {
	raw, durable []byte
	remaining    int
	calls        int
}

func (f *crashFile) WriteAt(b []byte, at int64) (int, error) {
	f.calls++
	n := len(b)
	if f.remaining >= 0 && n > f.remaining {
		n = f.remaining
	}
	end := int(at) + n
	if end > len(f.raw) {
		f.raw = append(f.raw, make([]byte, end-len(f.raw))...)
	}
	copy(f.raw[int(at):end], b[:n])
	if f.remaining >= 0 {
		f.remaining -= n
		if f.remaining == 0 {
			return n, interrupted
		}
	}
	return n, nil
}
func (f *crashFile) Sync() error            { f.durable = append([]byte(nil), f.raw...); return nil }
func (f *crashFile) Truncate(n int64) error { f.raw = f.raw[:n]; return nil }
func TestInterruptedRetailAtEveryWrittenByte(t *testing.T) {
	old, s, branch := fixture(t)
	before := old.Bytes()
	next, err := Load(before)
	if err != nil {
		t.Fatal(err)
	}
	r := add(t, next, s, Resource, "New Resource With A Longer Label", "", "")
	v := add(t, next, r, Version, "2", "", "")
	add(t, next, v, Branch, "", "Jobs", "Debug")
	total := next.Size() + len(before) + slotBytes*2
	for cut := 0; cut <= total; cut++ {
		f := &crashFile{raw: append([]byte(nil), before...), durable: append([]byte(nil), before...), remaining: cut}
		_, err := retail(f, before, next)
		if err != nil && !errors.Is(err, interrupted) {
			t.Fatal(cut, err)
		}
		// Check both persisted unsynced writes and the last explicitly durable image.
		for _, disk := range [][]byte{f.raw, f.durable} {
			loaded, e := Load(disk)
			if e != nil {
				t.Fatalf("cut %d cannot recover: %v", cut, e)
			}
			if !loaded.Extends(old) {
				t.Fatal("lost existing nodes or labels", cut)
			}
			if loaded.NodeCount() != old.NodeCount() && loaded.NodeCount() != next.NodeCount() {
				t.Fatal("partially published registration", cut)
			}
			if got, _ := loaded.Leaf(1); got != branch {
				t.Fatal("leaf changed", cut)
			}
			retry := &crashFile{raw: append([]byte(nil), disk...), remaining: -1}
			image, e := retail(retry, disk, next)
			if e != nil {
				t.Fatal("retry", cut, e)
			}
			recovered, e := Load(image)
			if e != nil || !bytes.Equal(recovered.Bytes(), next.Bytes()) {
				t.Fatal("retry changed identities", cut, e)
			}
		}
	}
}

func BenchmarkRegistration(b *testing.B) {
	old, service, _ := fixture(b)
	for i := 0; i < 10000; i++ {
		add(b, old, service, Resource, fmt.Sprintf("resource-%d", i), "", "")
	}
	before := old.Bytes()
	next := old.Clone()
	add(b, next, service, Resource, "new-resource", "", "")
	b.ReportMetric(float64(len(before)), "file-bytes")
	b.ReportMetric(float64(old.directoryBytes), "directory-bytes")
	path := filepath.Join(b.TempDir(), "trunk.trie")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		if err := os.WriteFile(path, before, 0600); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := SavePrepared(path, before, old, next); err != nil {
			b.Fatal(err)
		}
	}
}
