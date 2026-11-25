package cache

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// A real SIGKILL, with no deferred Close, tests the acknowledged append boundary.
func TestStagingSIGKILLRecovery(t *testing.T) {
	root := os.Getenv("TRUNK_STAGE_KILL_ROOT")
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	l := testLayout(t, false)
	if root != "" {
		s, e := Open(Config{Root: root, Cache: "demo", Tenant: "test", Layout: l, ObjectIndex: true, Now: func() time.Time { return at.Add(time.Minute) }})
		if e != nil {
			t.Fatal(e)
		}
		if _, e = s.Append(Write, testFrame(t, l, at, 1, false)); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(root, "ready"), []byte("durable"), 0600); e != nil {
			t.Fatal(e)
		}
		select {}
	}
	root = t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestStagingSIGKILLRecovery$", "-test.timeout=30s")
	cmd.Env = append(os.Environ(), "TRUNK_STAGE_KILL_ROOT="+root)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { cmd.Process.Kill() }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, e := os.Stat(filepath.Join(root, "ready")); e == nil {
			break
		}
		if time.Now().After(deadline) {
			cmd.Process.Kill()
			cmd.Wait()
			t.Fatal("child startup", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if e := cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	if e := cmd.Wait(); e == nil {
		t.Fatal("child not killed")
	}
	s, e := Open(Config{Root: root, Cache: "demo", Tenant: "test", Layout: l, ObjectIndex: true, Now: func() time.Time { return at.Add(time.Minute) }})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if s.branches[Write].live.count(0) != 1 {
		t.Fatal("acknowledged event lost")
	}
	n := 0
	if e = s.History(context.Background(), testObject(42), at, at.Add(time.Hour), func(row uint64, raw []byte) error {
		n++
		if row != ProvisionalRow || !sameSeedData(t, l, raw, testFrame(t, l, at, 1, false)) {
			t.Fatal("recovered live history")
		}
		return nil
	}); e != nil || n != 1 {
		t.Fatal("acknowledged history missing after kill", n, e)
	}
	if len(s.branches[Write].rows) != 0 {
		t.Fatal("recovery finalized young event")
	}
	if got := func() [][]byte {
		var frames [][]byte
		e = s.SearchLive(context.Background(), Query{Branch: Write, From: at, To: at.Add(time.Hour)}, func(raw []byte) error { frames = append(frames, append([]byte(nil), raw...)); return nil })
		if e != nil {
			t.Fatal(e)
		}
		return frames
	}(); len(got) != 1 || !sameSeedData(t, l, got[0], testFrame(t, l, at, 1, false)) {
		t.Fatal("recovery payload")
	}
}

func TestStagingTornTailAndCorruption(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			l := testLayout(t, false)
			at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
			c := Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return at.Add(time.Minute) }}
			s, e := Open(c)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = s.Append(Write, testFrame(t, l, at, 1, false)); e != nil {
				t.Fatal(e)
			}
			b := s.branches[Write]
			path := s.swapPath(b)
			if e = releaseCrashedTestStore(s); e != nil {
				t.Fatal(e)
			}
			f, e := os.OpenFile(path, os.O_RDWR, 0600)
			if e != nil {
				t.Fatal(e)
			}
			if corrupt {
				_, e = f.WriteAt([]byte{255}, headerSize+16+8)
			} else {
				_, e = f.Seek(0, 2)
				if e == nil {
					_, e = f.Write([]byte{1, 2, 3})
				}
			}
			f.Close()
			if e != nil {
				t.Fatal(e)
			}
			s, e = Open(c)
			if corrupt {
				if e == nil {
					s.Close()
					t.Fatal("accepted corrupt full record")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			if s.branches[Write].live.count(0) != 1 {
				t.Fatal("lost durable prefix")
			}
		})
	}
}

func TestStagingIdlePromotionAndClockRollback(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	// Update the test clock under the same mutex as production maintenance.
	now := at.Add(time.Minute)
	s, e := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if _, e = s.Append(Write, testFrame(t, l, at, 1, false)); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, 5*time.Millisecond) }()
	s.mu.Lock()
	now = at.Add(2 * time.Hour)
	s.mu.Unlock()
	deadline := time.Now().Add(3 * time.Second)
	for {
		s.mu.Lock()
		rows := len(s.branches[Write].rows)
		s.mu.Unlock()
		if rows == 2*s.row.Width {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("idle tail never promoted")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	cutoff := s.branches[Write].finalizedBefore
	now = at.Add(time.Minute)
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	if s.branches[Write].finalizedBefore != cutoff {
		t.Fatal("clock rollback reopened finalized time")
	}
}

// Simulate loss before finalization while terminating the newly added indexing
// workers too. Closing file descriptors alone is not equivalent to process death.
func releaseCrashedTestStore(s *Store) error {
	s.mu.Lock()
	s.closed = true
	if s.searchCancel != nil {
		s.searchCancel()
	}
	touched := s.searchTouched
	s.mu.Unlock()
	s.finishSearch(touched)
	return s.release()
}
