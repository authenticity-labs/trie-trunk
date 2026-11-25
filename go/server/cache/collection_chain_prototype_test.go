//go:build trunk_prototype

package cache

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestObjectIndexChainBoundaryRecoveryAndPinnedReaders(t *testing.T) {
	i := testCollectionIndex(t)
	k := objectBinding{}
	id := collectionTestID(2)
	if e := i.append(k, 4, 0, 735, collectionPosts(id, 0, 734)); e != nil {
		t.Fatal(e)
	}
	old := i.pin()
	defer releaseCollection(old)
	for end := 735; end <= 2300; end += 313 {
		start := int(i.through[k] - 1)
		if e := i.append(k, 4, i.through[k], uint64(end+1), collectionPosts(id, start, end)); e != nil {
			t.Fatal(e)
		}
	}
	total := int(i.through[k] - 1)
	if _, overflow, _ := i.pages(k); overflow < 2 {
		t.Fatal("no continuation chain")
	}
	verify := func(v *collectionView, want int) {
		t.Helper()
		count := 0
		if e := v.visit(id[:], k, func(_ [16]byte, r uint64, a collectionAddress) error {
			if r != uint64(count) || a != (collectionAddress{17, uint32(count + 1)}) {
				t.Fatalf("revision %d count %d address %v", r, count, a)
			}
			count++
			return nil
		}); e != nil {
			t.Fatal(e)
		}
		if count != want {
			t.Fatal(count, want)
		}
	}
	verify(old, 734)
	current := i.pin()
	defer releaseCollection(current)
	verify(current, total)
	reopened, e := loadObjectIndex(i.file)
	if e != nil {
		t.Fatal(e)
	}
	loaded := reopened.pin()
	defer releaseCollection(loaded)
	verify(loaded, total)
	for _, r := range []int{0, 732, 733, 734, 1465, total - 1} {
		a, ok, e := reopened.lookup(id[:], k, uint64(r))
		if e != nil || !ok || a.Record != uint32(r+1) {
			t.Fatal(r, a, ok, e)
		}
	}

}

func TestObjectIndexChainedSplitAndWiden(t *testing.T) {
	i := testCollectionIndex(t)
	k := objectBinding{}
	id := collectionTestID(400)
	if e := i.mergeRetained(k, 4, 10001, collectionPosts(id, 0, 1500)); e != nil {
		t.Fatal(e)
	}
	old := i.pin()
	defer releaseCollection(old)
	// Insert neighbors around the linked UUID, forcing directory splits.
	for _, n := range []int{399, 401, 200, 600} {
		if e := i.mergeRetained(k, 4, 10001, collectionPosts(collectionTestID(n), 0, 20)); e != nil {
			t.Fatal(e)
		}
	}
	wide := collectionAddress{1 << 24, 1 << 24}
	if e := i.mergeRetained(k, 4, 10002, []collectionPosting{{id, 1500, wide}}); e != nil {
		t.Fatal(e)
	}
	reopened, e := loadObjectIndex(i.file)
	if e != nil {
		t.Fatal(e)
	}
	for r := 0; r < 1500; r++ {
		a, ok, e := reopened.lookup(id[:], k, uint64(r))
		if e != nil || !ok || a.Record != uint32(r+1) {
			t.Fatal(r, a, ok, e)
		}
	}
	if a, ok, e := reopened.lookup(id[:], k, 1500); e != nil || !ok || a != wide {
		t.Fatal(a, ok, e)
	}
	if e := old.visit(id[:], k, func(_ [16]byte, r uint64, a collectionAddress) error {
		if r >= 1500 || a.Hour != 17 {
			t.Fatal("old chain changed")
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
}

func TestObjectIndexChainRandomGapsAndRetention(t *testing.T) {
	i := testCollectionIndex(t)
	k := objectBinding{}
	id := collectionTestID(3)
	rng := rand.New(rand.NewSource(18))
	revs := rng.Perm(2400)
	for start := 0; start < len(revs); start += 100 {
		var posts []collectionPosting
		for _, r := range revs[start:min(start+100, len(revs))] {
			rev := uint64(65536 + r*3)
			posts = append(posts, collectionPosting{id, rev, collectionAddress{4, uint32(r + 1)}})
		}
		if e := i.mergeRetained(k, 4, 10000, posts); e != nil {
			t.Fatal(start, e)
		}
	}
	j, e := loadObjectIndex(i.file)
	if e != nil {
		t.Fatal(e)
	}
	for r := 0; r < 2400; r++ {
		a, ok, e := j.lookup(id[:], k, uint64(65536+r*3))
		if e != nil || !ok || a.Record != uint32(r+1) {
			t.Fatal(r, a, ok, e)
		}
	}
	if _, ok, e := j.lookup(id[:], k, 65537); e != nil || ok {
		t.Fatal("gap", ok, e)
	}
	// The same streaming path used by retention rebuilds links after removing the head.
	next := testCollectionIndex(t)
	var batch []collectionPosting
	e = j.each(k, func(id [16]byte, r uint64, a collectionAddress) error {
		if r >= 70000 {
			batch = append(batch, collectionPosting{id, r, a})
		}
		if len(batch) == 100 {
			e := next.mergeRetained(k, 4, 10000, batch)
			batch = nil
			return e
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	if len(batch) > 0 {
		if e = next.mergeRetained(k, 4, 10000, batch); e != nil {
			t.Fatal(e)
		}
	}
	next, e = loadObjectIndex(next.file)
	if e != nil {
		t.Fatal(e)
	}
	if _, ok, e := next.lookup(id[:], k, 65536); e != nil || ok {
		t.Fatal(ok, e)
	}
	if _, ok, e := next.lookup(id[:], k, 72733); e != nil || !ok {
		t.Fatal(ok, e)
	}
}

func TestObjectIndexChainPublicationFailure(t *testing.T) {
	for _, stage := range []string{"before-root", "after-root"} {
		t.Run(stage, func(t *testing.T) {
			i := testCollectionIndex(t)
			k := objectBinding{}
			id := collectionTestID(1)
			if e := i.append(k, 4, 0, 735, collectionPosts(id, 0, 734)); e != nil {
				t.Fatal(e)
			}
			i.fault = func(at string) error {
				if at == stage {
					return fmt.Errorf("injected")
				}
				return nil
			}
			if e := i.append(k, 4, 735, 736, collectionPosts(id, 734, 735)); e == nil {
				t.Fatal("fault missed")
			}
			if _, ok, e := i.lookup(id[:], k, 734); e != nil || ok {
				t.Fatal("unpublished", ok, e)
			}
			j, e := loadObjectIndex(i.file)
			if e != nil {
				t.Fatal(e)
			}
			if _, ok, e := j.lookup(id[:], k, 734); e != nil || ok != (stage == "after-root") {
				t.Fatal(ok, e)
			}
			for r := uint64(732); r <= 733; r++ {
				if _, ok, e := j.lookup(id[:], k, r); e != nil || !ok {
					t.Fatal("boundary lost", r, ok, e)
				}
			}
		})
	}
}

func TestObjectIndexRevisionU24AndNoEmptyGapPages(t *testing.T) {
	i := testCollectionIndex(t)
	k := objectBinding{}
	id := collectionTestID(1)
	posts := []collectionPosting{{id, 0, collectionAddress{0, 1}}, {id, collectionMaxRevision, collectionAddress{0, 2}}}
	if e := i.append(k, 4, 0, 3, posts); e != nil {
		t.Fatal(e)
	}
	if _, overflow, _ := i.pages(k); overflow != 1 {
		t.Fatal("reserved pages for revision gap")
	}
	if _, ok, e := i.lookup(id[:], k, collectionMaxRevision); e != nil || !ok {
		t.Fatal(ok, e)
	}
	if e := i.append(k, 4, 3, 4, []collectionPosting{{id, collectionMaxRevision + 1, collectionAddress{0, 3}}}); e == nil {
		t.Fatal("overflow accepted")
	}
}

func TestObjectIndexChainedReadersDuringPublication(t *testing.T) {
	i := testCollectionIndex(t)
	k := objectBinding{}
	id := collectionTestID(1)
	if e := i.append(k, 4, 0, 1501, collectionPosts(id, 0, 1500)); e != nil {
		t.Fatal(e)
	}
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	i.fault = func(stage string) error {
		if stage == "before-root" {
			close(entered)
			<-resume
		}
		return nil
	}
	go func() { done <- i.append(k, 4, 1501, 1502, collectionPosts(id, 1500, 1501)) }()
	<-entered
	read := make(chan error, 1)
	go func() {
		r, _, ok, e := i.latest(id[:], k)
		if e == nil && (!ok || r != 1499) {
			e = fmt.Errorf("reader saw unpublished tail %d", r)
		}
		read <- e
	}()
	select {
	case e := <-read:
		if e != nil {
			t.Error(e)
		}
	case <-time.After(time.Second):
		t.Error("reader waited for collection writer")
	}
	close(resume)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if r, _, ok, e := i.latest(id[:], k); e != nil || !ok || r != 1500 {
		t.Fatal(r, ok, e)
	}
}

func BenchmarkCollectionChainAccess(b *testing.B) {
	for _, count := range []int{734, 735, 5000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			i := testCollectionIndex(b)
			k := objectBinding{}
			id := collectionTestID(1)
			if e := i.append(k, 4, 0, uint64(count+1), collectionPosts(id, 0, count)); e != nil {
				b.Fatal(e)
			}
			b.ResetTimer()
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				if r, _, ok, e := i.latest(id[:], k); e != nil || !ok || r != uint64(count-1) {
					b.Fatal(r, ok, e)
				}
			}
		})
	}
}

func TestObjectIndexWideningInsideLinkedHistory(t *testing.T) {
	i := testCollectionIndex(t)
	k := objectBinding{}
	id := collectionTestID(1)
	missing := map[uint64]bool{100: true, 800: true, 1450: true}
	var posts []collectionPosting
	for _, post := range collectionPosts(id, 0, 2000) {
		if !missing[post.Revision] {
			posts = append(posts, post)
		}
	}
	if e := i.mergeRetained(k, 4, 3000, posts); e != nil {
		t.Fatal(e)
	}
	old := i.pin()
	defer releaseCollection(old)
	wide := collectionAddress{1 << 24, 1 << 24}
	for _, rev := range []uint64{800, 100, 1450} {
		if e := i.mergeRetained(k, 4, 3000, []collectionPosting{{id, rev, wide}}); e != nil {
			t.Fatal(rev, e)
		}
	}
	j, e := loadObjectIndex(i.file)
	if e != nil {
		t.Fatal(e)
	}
	count := 0
	e = j.visit(id[:], k, func(r uint64, a collectionAddress) {
		want := collectionAddress{17, uint32(r + 1)}
		if missing[r] {
			want = wide
		}
		if r != uint64(count) || a != want {
			t.Errorf("rev %d count %d got %+v want %+v", r, count, a, want)
		}
		count++
	})
	if e != nil || count != 2000 {
		t.Fatal(count, e)
	}
	e = old.visit(id[:], k, func(_ [16]byte, r uint64, a collectionAddress) error {
		if missing[r] {
			t.Error("old chain acquired new revision", r)
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
}

func TestObjectIndexHistoryBatchRemainsPinnedDuringPublication(t *testing.T) {
	i := testCollectionIndex(t)
	k := objectBinding{}
	id := collectionTestID(57)
	const count = 2100
	if err := i.append(k, 4, 0, count, collectionPosts(id, 0, count)); err != nil {
		t.Fatal(err)
	}
	view := i.pin()
	defer releaseCollection(view)
	stop := errors.New("stop reading history")
	calls := 0
	if err := view.visit(id[:], k, func(_ [16]byte, r uint64, a collectionAddress) error {
		calls++
		if calls == 2 {
			return stop
		}
		return nil
	}); err != stop || calls != 2 {
		t.Fatalf("early stop: %d calls, %v", calls, err)
	}
	seen := 0
	err := i.visit(id[:], k, func(r uint64, a collectionAddress) {
		if r != uint64(seen) || a.Record != uint32(seen+1) {
			t.Fatalf("unexpected revision %d at %v", r, a)
		}
		if seen == 0 {
			// Re-enter while the outer reader owns its buffer. Then publish a new tail.
			if e := i.visit(id[:], k, func(uint64, collectionAddress) {}); e != nil {
				t.Fatal(e)
			}
			if e := i.append(k, 4, count, count+1, collectionPosts(id, count, count+1)); e != nil {
				t.Fatal(e)
			}
		}
		seen++
	})
	if err != nil || seen != count {
		t.Fatalf("pinned history: %d, %v", seen, err)
	}
	if rev, _, ok, e := i.latest(id[:], k); e != nil || !ok || rev != count {
		t.Fatal(rev, ok, e)
	}
}
