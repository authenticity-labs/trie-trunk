package cache

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

func TestPACBoundaryWalkEmptyBlankAndYear(t *testing.T) {
	from := time.Date(2025, 12, 31, 22, 0, 0, 0, time.UTC).UnixMilli()
	runs := []Run{timeRun(from+TimeHourMS, 0, 1), timeRun(from+5*TimeHourMS, 2, 1)}
	pack, err := os.ReadFile("../../../testdata/http-ingress/live.pac")
	if err != nil {
		t.Fatal(err)
	}
	size := uint64(len(pack))
	for i := range runs {
		runs[i].PacksStart = headerSize + uint64(i)*size
		runs[i].PacksEnd = runs[i].PacksStart + size
	}
	tree, err := buildTimeDirectory(runs, from, from+8*TimeHourMS, 4, [16]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.CreateTemp(t.TempDir(), "packs")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	raw := make([]byte, headerSize+size*2)
	for _, r := range runs {
		copy(raw[r.PacksStart:], pack)
	}
	if _, err = f.Write(raw); err != nil {
		t.Fatal(err)
	}
	for h, want := range []uint64{headerSize, headerSize, headerSize + size, headerSize + size, headerSize + size, headerSize + size, headerSize + 2*size, headerSize + 2*size, headerSize + 2*size} {
		got, err := tree.packsBoundary(f, 0, from+int64(h)*TimeHourMS)
		if err != nil || got != want {
			t.Fatalf("hour %d: %d %v", h, got, err)
		}
	}
	// Empty sealed siblings need no occupancy byte and preserve PAC boundaries.
	got, err := tree.packsBoundary(f, 0, from+4*TimeHourMS)
	if err != nil || got != headerSize+size {
		t.Fatal(got, err)
	}
	if _, err = tree.window(from+2*TimeHourMS, from+9*TimeHourMS, false); err == nil {
		t.Fatal("unpublished hour certified as empty")
	}
	if _, err = tree.packsBoundary(f, 0, from+9*TimeHourMS); err == nil {
		t.Fatal("future boundary accepted")
	}
	// Logical byte addresses retain their origin after physical prefix removal.
	for i := range runs {
		runs[i].PacksStart += 4096
		runs[i].PacksEnd += 4096
	}
	tree, err = buildTimeDirectory(runs, from, from+8*TimeHourMS, 4, [16]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	got, err = tree.packsBoundary(f, 4096, from+4*TimeHourMS)
	if err != nil || got != 4096+headerSize+size {
		t.Fatal(got, err)
	}
}

func TestPACRangeShortReadAndCancellation(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "packs")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	f.Write([]byte{1, 2, 3})
	if err = writePACRange(context.Background(), io.Discard, 0, f, 0, 4); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = writePACRange(ctx, io.Discard, 0, f, 0, 3); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func BenchmarkPACRangeDelivery(b *testing.B) {
	const hours = 256
	const events = 256
	l := testLayout(b, false)
	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	now := at.Add((hours + 2) * time.Hour)
	s, err := Open(Config{Root: b.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	off := false
	s.policy.SearchIndex = &off
	s.mu.Lock()
	locked := true
	defer func() {
		if locked {
			s.mu.Unlock()
		}
	}()
	branch, err := s.openBranch(Error)
	if err != nil {
		b.Fatal(err)
	}
	for h := 0; h < hours; h++ {
		batch := make([]pending, events)
		for n := 0; n < events; n++ {
			stamp := at.Add(time.Duration(h)*time.Hour + time.Duration(n)*time.Millisecond)
			ordinal := uint32(h*events + n + 1)
			raw := testFrame(b, l, stamp, ordinal, false)
			id, e := eventID(uuid(stamp.UnixMilli(), ordinal))
			if e != nil {
				b.Fatal(e)
			}
			var frame Frame
			if err = l.Scan(raw, &frame); err != nil {
				b.Fatal(err)
			}
			if err = s.accumulate(branch, id, frame.Values); err != nil {
				b.Fatal(err)
			}
			batch[n] = pending{id, raw}
			branch.pendingBytes += len(raw)
		}
		if err = s.commitBatch(branch, batch); err != nil {
			b.Fatal(err)
		}
	}
	branch.finalizedBefore = at.Add(hours * time.Hour).UnixMilli()
	if err = s.publishTime(branch); err != nil {
		b.Fatal(err)
	}
	s.mu.Unlock()
	locked = false
	req := CollectionRequest{Branch: Error, At: at, Until: at.Add(hours * time.Hour)}
	for _, mode := range []string{"per-pack", "contiguous"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(branch.packsEnd - headerSize))
			for n := 0; n < b.N; n++ {
				var sendRange func(*os.File, uint64, uint64) error
				if mode == "contiguous" {
					sendRange = func(f *os.File, start, end uint64) error {
						return writePACRange(context.Background(), io.Discard, 0, f, start, end)
					}
				}
				err := s.collectDelivery(context.Background(), req, func(Manifest) error { return nil }, func(NativeChunk) error { return nil }, sendRange)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
