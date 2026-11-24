package cache

import (
	"bytes"
	"testing"
	"time"
)

// Accepted output must retain native chunk reuse and immutable published
// prefixes even after the input lease is returned and later requests reuse it.
func TestIngressNormalizationNativeOwnership(t *testing.T) {
	l, raw := ingressFixture(t, 78)
	defer l.Close()
	now := time.Date(2026, 10, 24, 12, 30, 0, 0, time.UTC)
	cfg := Config{Root: t.TempDir(), Cache: "native", Tenant: "test", Layout: l, Now: func() time.Time { return now }}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	original := bytes.Clone(raw)
	var first []byte
	var firstCopy []byte
	for i := 0; i < 128; i++ {
		in := nativeIngress{memory: s.memory}
		frame, err := in.read(l, bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		if n, err := s.appendNativeBatch(Read, []nativeInput{frame}); err != nil || n != 1 {
			t.Fatalf("%d %v", n, err)
		}
		if !bytes.Equal(frame.raw, original) {
			t.Fatal("input bytes mutated")
		}
		in.close()
		pending := s.branches[Read].pendingAll()
		if i == 0 {
			first = pending[0].frame
			firstCopy = bytes.Clone(first)
		}
		if !bytes.Equal(first, firstCopy) {
			t.Fatal("published prefix overwritten")
		}
		var f Frame
		if err = l.Scan(pending[i].frame, &f); err != nil {
			t.Fatal(err)
		}
		if unsigned(f.Values[l.Record].Bytes) != uint64(i) || !bytes.Equal(f.Values[5].Bytes, bytes.Repeat([]byte("x"), 78)) {
			t.Fatal("normalized fields changed")
		}
	}
	if !bytes.Equal(raw, original) {
		t.Fatal("producer bytes mutated")
	}
	branch := s.branches[Read]
	allocations := 0
	for _, a := range branch.memory {
		allocations += len(a.adopted)
	}
	if allocations > 8 {
		t.Fatalf("lost chunk reuse: %d allocations for 128 records", allocations)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	recovered := s.branches[Read].pendingAll()
	if len(recovered) != 128 || !bytes.Equal(recovered[0].frame, firstCopy) {
		t.Fatal("durable bytes differ from native publication")
	}
}
