//go:build trunk_prototype

package cache

import (
	"context"
	"testing"
	"time"
)

func TestNativeHistorySurvivesStoreClose(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	p, err := OpenEndpoint(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, ObjectIndex: true, Now: func() time.Time { return at.Add(time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	raw := testFrame(t, l, at, 1, false)
	if _, err = p.api.Store.Append(Write, raw); err != nil {
		t.Fatal(err)
	}
	h, err := p.snapshotHistory(l.Definition.Identity, 1, testObject(42), at, at.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer h.close()
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	if err = h.visitBorrowed(context.Background(), func(_ EventReference, b []byte) error {
		if &b[0] != &h.locations[0].raw[0] {
			t.Fatal("borrowed history copied payload")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	n := 0
	if err = h.visit(context.Background(), func(_ EventReference, b []byte) error {
		n++
		if !sameSeedData(t, l, b, raw) {
			t.Fatal("snapshot changed after close")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("rows=%d", n)
	}
}
