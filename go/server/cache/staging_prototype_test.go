//go:build trunk_prototype

package cache

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStagingHoldbackLiveSearchAndLateAudit(t *testing.T) {
	l := testLayout(t, false)
	now := time.Date(2026, 10, 17, 12, 15, 0, 0, time.UTC)
	at := now.Add(-time.Minute)
	c := Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }, BatchRecords: 1}
	s, e := Open(c)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { s.Close() }()
	first := testFrame(t, l, at, 1, false)
	earlier := testFrame(t, l, at.Add(-time.Second), 2, true)
	for _, raw := range [][]byte{first, earlier} {
		if _, e = s.Append(Write, raw); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	b := s.branches[Write]
	if len(b.rows) != 0 || b.packsEnd != headerSize {
		t.Fatal("young records finalized")
	}
	q := Query{Branch: Write, From: at.Add(-time.Hour), To: now}
	var live [][]byte
	if e = s.SearchLive(context.Background(), q, func(raw []byte) error { live = append(live, append([]byte(nil), raw...)); return nil }); e != nil {
		t.Fatal(e)
	}
	if len(live) != 2 || !sameSeedData(t, l, live[0], earlier) {
		t.Fatal("staged search missing/out of order")
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(c)
	if e != nil {
		t.Fatal(e)
	}
	if len(s.branches[Write].rows) != 0 || s.branches[Write].live.count(0) != 2 {
		t.Fatal("close bypassed holdback or recovery lost events")
	}
	now = now.Add(80 * time.Minute)
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	got := collect(t, s, Write, q.From, now)
	if len(got) != 2 || !sameSeedData(t, l, got[0], earlier) || !sameSeedData(t, l, got[1], first) {
		t.Fatal("final storage not sorted")
	}
	if dup, e := s.Append(Write, first); e == nil || dup {
		t.Fatal("retry", dup, e)
	}
	rejected := testFrame(t, l, at.Add(-2*time.Second), 3, false)
	_, e = s.Append(Write, rejected)
	var late *LateEventError
	if !errors.As(e, &late) {
		t.Fatalf("expected rejection: %v", e)
	}
	n := 0
	if e = s.Rejections(func(r Rejection) error {
		n++
		if (!bytes.Equal(r.PIL, rejected) && !bytes.Equal(r.PIL, first)) || r.Reason != "arrival_after_finalization" {
			t.Fatal("lost audit payload")
		}
		return nil
	}); e != nil || n != 2 {
		t.Fatal(n, e)
	}
	// The empty closed interval and audit survive restart too.
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(c)
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.Append(Write, testFrame(t, l, time.UnixMilli(s.branches[Write].finalizedBefore-1), 4, false))
	if !errors.As(e, &late) {
		t.Fatal("closed empty interval reopened", e)
	}
}

func TestStagingRecoveryAfterCommitBeforeCleanup(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	c := Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return at.Add(2 * time.Hour) }, BatchRecords: 1}
	s, e := Open(c)
	if e != nil {
		t.Fatal(e)
	}
	raw := testFrame(t, l, at, 1, false)
	if _, e = s.Append(Write, raw); e != nil {
		t.Fatal(e)
	}
	b := s.branches[Write]
	if _, e = s.Append(Write, testFrame(t, l, at, 2, true)); e != nil {
		t.Fatal(e)
	}
	s.mu.Lock()
	e = s.commitBatch(b, b.pendingAll())
	s.mu.Unlock()
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(s.dir, ".seed-interrupted"), []byte("unfinished replacement"), 0600); e != nil {
		t.Fatal(e)
	}
	// Simulate death after the permanent commit, before atomic swap cleanup.
	if e = releaseCrashedTestStore(s); e != nil {
		t.Fatal(e)
	}
	s, e = Open(c)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if s.branches[Write].live.count(0) != 0 {
		t.Fatal("complete PAC was replayed despite matching index")
	}
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(s.dir, ".seed-interrupted")); !os.IsNotExist(e) {
		t.Fatal("orphan replacement not cleaned", e)
	}
	if got := collect(t, s, Write, at, at.Add(time.Hour)); len(got) != 2 {
		t.Fatal("duplicate after crash")
	}
}

func TestStagingRetentionKeepsYoungTail(t *testing.T) {
	l := testLayout(t, false)
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	c := Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }}
	s, e := Open(c)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { s.Close() }()
	old := testFrame(t, l, now.Add(-48*time.Hour), 1, false)
	young := testFrame(t, l, now.Add(-time.Minute), 2, false)
	if _, e = s.AppendBatch(Write, [][]byte{young, old}); e != nil {
		t.Fatal(e)
	}
	s.mu.Lock()
	e = s.prune(Write, now.Add(-24*time.Hour))
	s.mu.Unlock()
	if e != nil {
		t.Fatal(e)
	}
	if len(s.branches[Write].rows) != 0 || s.branches[Write].live.count(0) != 1 {
		t.Fatal("retention lost young tail")
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(c)
	if e != nil {
		t.Fatal(e)
	}
	now = now.Add(time.Hour)
	got := collect(t, s, Write, now.Add(-2*time.Hour), now)
	if len(got) != 1 || !sameSeedData(t, l, got[0], young) {
		t.Fatal("retention/restart lost pending event")
	}
}
