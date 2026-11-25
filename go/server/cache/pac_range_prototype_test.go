//go:build trunk_prototype

package cache

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestPACRangeStreamingDoesNotInspectInteriorPacks(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	s, err := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return at.Add(24 * time.Hour) }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, h := range []int{0, 2, 4} {
		if _, err = s.Append(Write, testFrame(t, l, at.Add(time.Duration(h)*time.Hour), uint32(h+1), false)); err != nil {
			t.Fatal(err)
		}
	}
	req := CollectionRequest{Branch: Write, At: at, Until: at.Add(6 * time.Hour)}
	var manifest Manifest
	var packs []NativeChunk
	if err = s.Collect(context.Background(), req, func(m Manifest) error { manifest = m; return nil }, func(c NativeChunk) error {
		if c.Kind == "pac" {
			packs = append(packs, c)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(packs) != 3 {
		t.Fatal(len(packs))
	}
	b := s.branches[Write]
	middle := b.runs[1]
	// Deliberately unreadable interior length: transfer must not parse it. The
	// receiver's sealed checksum rejects corruption; the sender only copies bytes.
	if _, err = b.packs.WriteAt([]byte{0, 0, 0, 0}, int64(middle.PacksStart-b.packsRetired)); err != nil {
		t.Fatal(err)
	}
	packs[1].Data[0] = 0
	packs[1].Data[1] = 0
	packs[1].Data[2] = 0
	packs[1].Data[3] = 0
	var joined bytes.Buffer
	for _, p := range packs {
		joined.Write(p.Data)
	}
	ranges := 0
	run := func(have Coverage) []byte {
		t.Helper()
		req.Have = have
		var got bytes.Buffer
		err = s.collectDelivery(context.Background(), req, func(Manifest) error { return nil }, func(c NativeChunk) error {
			if c.Kind == "pac" {
				t.Fatal("per-pack callback")
			}
			return nil
		}, func(f *os.File, start, end uint64) error {
			ranges++
			_, e := io.Copy(&got, io.NewSectionReader(f, int64(start), int64(end-start)))
			return e
		})
		if err != nil {
			t.Fatal(err)
		}
		return got.Bytes()
	}
	if got := run(Coverage{}); ranges != 1 || !bytes.Equal(got, joined.Bytes()) {
		t.Fatal("range not copied verbatim", ranges)
	}
	// Covering the middle hour splits the physical response into two ranges.
	m := manifest.Blocks[1]
	ranges = 0
	have := Coverage{Definition: manifest.Definition, Generation: manifest.Generation, Index: []Span{{m.Start, m.End}}, MemberRanges: []Span{{m.PAC.Placement.MemberStart, m.PAC.Placement.MemberStart + m.PAC.Placement.MemberBytes}}}
	if got := run(have); ranges != 2 || !bytes.Equal(got, append(packs[0].Data, packs[2].Data...)) {
		t.Fatal("covered PAC was resent", ranges)
	}
	// Restore the pack for the end-to-end HTTP parser.
	if _, err = b.packs.WriteAt(order.AppendUint32(nil, uint32(middle.PacksEnd-middle.PacksStart)), int64(middle.PacksStart-b.packsRetired)); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/collect?tap=demo/payment/transactions.v1/write&from="+at.Format(time.RFC3339)+"&to="+at.Add(6*time.Hour).Format(time.RFC3339), nil)
	r.Header.Set("Accept", "application/pil")
	r.Header.Set("PIL-Version", "2")
	r.Header.Set("PIL-Features", "native-ranges,adaptive-integers")
	w := httptest.NewRecorder()
	(&HTTP{Store: s}).nativeCollect(w, r)
	ms, chunks := readNativeTest(t, w.Body.Bytes())
	if len(ms) != 1 || len(ms[0].Blocks) != 3 {
		t.Fatal("sealed windows not merged", len(ms))
	}
	n := 0
	for _, c := range chunks {
		if c.Kind == "pac" {
			n++
		}
	}
	if n != 3 {
		t.Fatal(n)
	}
	if out := os.Getenv("PIL_RANGE_FIXTURE"); out != "" {
		if err = os.WriteFile(out, w.Body.Bytes(), 0644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPACRangePinsFileThroughRetention(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	now := at.Add(2 * time.Hour)
	s, err := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.policy.Retention.Write = "1h"
	if _, err = s.Append(Write, testFrame(t, l, at, 1, false)); err != nil {
		t.Fatal(err)
	}
	req := CollectionRequest{Branch: Write, At: at, Until: at.Add(time.Hour)}
	var expected []byte
	if err = s.Collect(context.Background(), req, func(Manifest) error { return nil }, func(c NativeChunk) error {
		if c.Kind == "pac" {
			expected = append(expected, c.Data...)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	err = s.collectDelivery(context.Background(), req, func(Manifest) error {
		now = at.Add(6 * time.Hour)
		if _, e := s.Append(Write, testFrame(t, l, now, 2, false)); e != nil {
			return e
		}
		return s.ApplyRetention()
	}, func(NativeChunk) error { return nil }, func(f *os.File, start, end uint64) error {
		_, e := io.Copy(&got, io.NewSectionReader(f, int64(start), int64(end-start)))
		return e
	})
	if err != nil || len(expected) == 0 || !bytes.Equal(got.Bytes(), expected) {
		t.Fatal("pinned range changed", err)
	}
}
