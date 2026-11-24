package cache

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"unsafe"
)

func TestSeedNativeIngressPackControls(t *testing.T) {
	// DOC receives its existing JSON view and authors/signs the Definition in
	// Rust. Event data enters only through the native typed ingress adapter.
	document, err := seedNativeDocument([]byte(`[{"header":{"rx-name":"native-server"},"fields":[
		{"name":"time","type":[1,0],"size":6,"annotations":["@time"]},
		{"name":"id","type":[4,0],"size":10,"annotations":["@event"]},
		{"name":"duration","type":[1,0],"size":4},
		{"name":"message","type":[5,0],"size":0}
	]}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer document.close()
	definition, err := seedNativeRegister(document.bytes)
	if err != nil {
		t.Fatal(err)
	}
	defer definition.close()
	const hour = uint64(1762732800000)
	var members [][]byte
	for i := range 3 {
		pill, err := definition.ingest([]seedNativeValue{
			{ordinal: 0, kind: seedUnsigned, low: hour + uint64(i)},
			{ordinal: 1, kind: seedBytes, bytes: bytes.Repeat([]byte{byte(i + 1)}, 10)},
			{ordinal: 2, kind: seedUnsigned, low: uint64(10000 + i)},
			{ordinal: 3, kind: seedText, bytes: []byte("received")},
		}, true)
		if err != nil {
			t.Fatal(err)
		}
		defer pill.close()
		members = append(members, pill.bytes)
	}
	var fixture bytes.Buffer
	for _, input := range []struct {
		name      string
		sealed    bool
		algorithm uint32
	}{
		{"live", false, 0}, {"plain", true, 0}, {"zstd", true, 1}, {"gzip", true, 2}, {"brotli", true, 3},
	} {
		pack, err := definition.pack(members, hour, input.sealed, input.algorithm, 1)
		if err != nil {
			t.Fatal(input.name, err)
		}
		if n, err := seedNativeLength(pack.bytes); err != nil || n != uint64(len(pack.bytes)) {
			t.Fatalf("%s length %d: %v", input.name, n, err)
		}
		if _, err := seedNativeLength(pack.bytes[:1]); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal("fragmented control", err)
		}
		if input.name == "plain" || input.name == "zstd" {
			fixture.Write(pack.bytes)
		}
		// The unchanged time trie supplies BOF, including address zero. Sparse
		// retrieval must not need the next hour or interpret size as uint32.
		file, err := os.CreateTemp(t.TempDir(), "pack-*")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = file.Write(pack.bytes); err != nil {
			t.Fatal(err)
		}
		run := timeRun(int64(hour), 0, 3)
		run.PacksStart = 0
		timeline, err := buildTimeDirectory([]Run{run}, int64(hour), int64(hour)+TimeHourMS, 4, [16]byte{1})
		if err != nil {
			t.Fatal(err)
		}
		leaf, ok := timeline.leaf(int64(hour))
		if !ok {
			t.Fatal("missing hour")
		}
		start, end, err := storedSegmentRange(file, 0, leaf.Packs)
		if err != nil || start != 0 || end != uint64(len(pack.bytes)) {
			t.Fatalf("time-trie Pack range: %d %d %v", start, end, err)
		}
		file.Close()
		tooSmall := newNativeMemory(1)
		before := unsafe.SliceData(pack.bytes)
		if buffer, err := pack.adopt(tooSmall); err == nil {
			buffer.close()
			t.Fatal("ignored native budget")
		}
		tooSmall.close()
		if before != unsafe.SliceData(pack.bytes) {
			t.Fatal("failed adoption changed output")
		}
		memory := newNativeMemory(1 << 20)
		buffer, err := pack.adopt(memory)
		if err != nil {
			t.Fatal(err)
		}
		if before != unsafe.SliceData(buffer.bytes) || pack.ptr != nil {
			t.Fatal("codec output copied or retained twice")
		}
		if memory.stats().Used < uint64(len(buffer.bytes)) {
			t.Fatal("native output not budgeted")
		}
		arena := nativeArena{ptr: newArena()}
		if err := arena.adopt(buffer); err != nil {
			t.Fatal(err)
		}
		buffer.close()
		if memory.stats().Used == 0 {
			t.Fatal("published allocation released early")
		}
		arena.close()
		if memory.stats().Used != 0 {
			t.Fatal("published allocation leaked")
		}
		memory.close()
	}
	if result, err := definition.ingest([]seedNativeValue{{ordinal: 2, kind: seedText, bytes: []byte("incorrect")}}, false); err == nil {
		result.close()
		t.Fatal("setter type accepted")
	}
	if result, err := definition.pack(members, hour, false, 1, 1); err == nil {
		result.close()
		t.Fatal("unsealed compression accepted")
	}
	if output := os.Getenv("TRUNK_NATIVE_SEED_FIXTURE"); output != "" {
		if err := os.MkdirAll(output, 0755); err != nil {
			t.Fatal(err)
		}
		large, err := definition.ingest([]seedNativeValue{
			{ordinal: 0, kind: seedUnsigned, low: hour},
			{ordinal: 1, kind: seedBytes, bytes: bytes.Repeat([]byte{9}, 10)},
			{ordinal: 2, kind: seedUnsigned, low: 10001},
			{ordinal: 3, kind: seedText, bytes: bytes.Repeat([]byte("received "), 40_000)},
		}, false)
		if err != nil {
			t.Fatal(err)
		}
		defer large.close()
		compressed, err := definition.pack([][]byte{large.bytes}, hour, true, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer compressed.close()
		for name, raw := range map[string][]byte{"definition.rx": document.bytes, "seeds.pacs": fixture.Bytes(), "limited.pac": compressed.bytes} {
			if err := os.WriteFile(filepath.Join(output, name), raw, 0644); err != nil {
				t.Fatal(err)
			}
		}
	}
}
