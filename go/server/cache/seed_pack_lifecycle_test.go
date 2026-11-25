//go:build trunk_prototype

package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSeedPackRotationLateArrivalsAndSeal(t *testing.T) {
	l := testLayout(t, false)
	hour := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	now := hour.Add(59 * time.Minute)
	c := Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }}
	s, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	first := testFrame(t, l, now, 1, false)
	if _, err = s.Append(Read, first); err != nil {
		t.Fatal(err)
	}
	b := s.branches[Read]
	original, err := os.Stat(s.swapPath(b))
	if err != nil {
		t.Fatal(err)
	}
	now = hour.Add(time.Hour)
	if err = s.Flush(); err != nil {
		t.Fatal(err)
	}
	pack := filepath.Join(s.dir, "read.pack")
	rotated, err := os.Stat(pack)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(original, rotated) {
		t.Fatal("rotation copied/rebuilt ingress instead of renaming")
	}
	fresh, err := os.Stat(s.swapPath(b))
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(original, fresh) {
		t.Fatal("new ingress still targets prior hour")
	}
	priorSize := rotated.Size()
	if _, err = s.Append(Read, testFrame(t, l, now.Add(time.Second), 2, false)); err != nil {
		t.Fatal(err)
	}
	rotated, _ = os.Stat(pack)
	if rotated.Size() != priorSize {
		t.Fatal("new-hour event appended to pack")
	}
	fresh, _ = os.Stat(s.swapPath(b))
	freshSize := fresh.Size()
	now = hour.Add(74 * time.Minute)
	late := testFrame(t, l, hour.Add(30*time.Minute), 3, true)
	if _, err = s.Append(Read, late); err != nil {
		t.Fatal(err)
	}
	rotated, _ = os.Stat(pack)
	if rotated.Size() <= priorSize {
		t.Fatal("late event not appended to pack")
	}
	fresh, _ = os.Stat(s.swapPath(b))
	if fresh.Size() != freshSize {
		t.Fatal("late event appended to new seed")
	}
	if err = s.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(b.runs) != 0 {
		t.Fatal("sealed before the 15-minute grace period")
	}
	// Restart during grace preserves both files and their separate hour ownership.
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(c)
	if err != nil {
		t.Fatal(err)
	}
	b = s.branches[Read]
	now = hour.Add(75 * time.Minute)
	if err = s.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(b.runs) != 1 || b.live.count(0) != 1 {
		t.Fatalf("sealed/live hours: %d/%d", len(b.runs), b.live.count(0))
	}
	if _, err = os.Stat(pack); !os.IsNotExist(err) {
		t.Fatalf("pack survived durable seal: %v", err)
	}
	if _, err = os.Stat(filepath.Join(s.dir, "read.journal")); !os.IsNotExist(err) {
		t.Fatal("journal created")
	}
	got := collect(t, s, Read, hour, hour.Add(time.Hour))
	if len(got) != 2 || !sameSeedData(t, l, got[0], late) || !sameSeedData(t, l, got[1], first) {
		t.Fatal("late arrivals lost or unordered")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(c)
	if err != nil {
		t.Fatal(err)
	}
	got = collect(t, s, Read, hour, hour.Add(time.Hour))
	if len(got) != 2 {
		t.Fatal("recovery duplicated or lost sealed records")
	}
}

func TestSeedPackInterruptedFreshSeed(t *testing.T) {
	for _, size := range []int64{0, 20, headerSize, headerSize + 8} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			l := testLayout(t, false)
			hour := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
			now := hour.Add(time.Minute)
			c := Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }}
			s, err := Open(c)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.Append(Read, testFrame(t, l, now, 1, false)); err != nil {
				t.Fatal(err)
			}
			now = hour.Add(time.Hour)
			if err = s.Flush(); err != nil {
				t.Fatal(err)
			}
			path := s.swapPath(s.branches[Read])
			if err = releaseCrashedTestStore(s); err != nil {
				t.Fatal(err)
			}
			if err = os.Truncate(path, size); err != nil {
				t.Fatal(err)
			}
			s, err = Open(c)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if s.branches[Read].live.count(0) != 1 {
				t.Fatal("rotated ingress lost")
			}
			if _, err = s.Append(Read, testFrame(t, l, now, 2, false)); err != nil {
				t.Fatal(err)
			}
			now = hour.Add(75 * time.Minute)
			if err = s.Flush(); err != nil {
				t.Fatal(err)
			}
			if len(collect(t, s, Read, hour, hour.Add(time.Hour))) != 1 || s.branches[Read].live.count(0) != 1 {
				t.Fatal("hour ownership changed during recovery")
			}
		})
	}
}
