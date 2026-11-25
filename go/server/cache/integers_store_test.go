//go:build trunk_prototype

package cache

import (
	"testing"
	"time"
)

func TestAdaptiveIdentityAcrossDefinitionMinors(t *testing.T) {
	d := testLayout(t, false).Definition
	root := t.TempDir()
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	for minor := uint32(0); minor < 2; minor++ {
		d.Resource.Minor = minor
		d.Version = 1<<20 | uint64(minor)
		if minor == 1 {
			d.PIL = 1
			d.AdaptiveIntegers = "32"
		}
		l, e := Compile(d)
		if e != nil {
			t.Fatal(e)
		}
		s, e := Open(Config{Root: root, Cache: "demo", Tenant: "test", Layout: l, ObjectIndex: true})
		if e != nil {
			t.Fatal(e)
		}
		if _, e = s.Append(Write, testFrame(t, l, at.Add(time.Duration(minor)*time.Second), minor+1, false)); e != nil {
			t.Fatal(e)
		}
		found, e := s.ObjectLocations(testObject(42))
		if e != nil || len(found) != int(minor+1) {
			t.Fatalf("cross-minor identities: %v %v", found, e)
		}
		if e = s.Close(); e != nil {
			t.Fatal(e)
		}
	}
}
