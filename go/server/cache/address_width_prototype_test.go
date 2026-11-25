//go:build trunk_prototype

package cache

import (
	"testing"
)

func TestObjectIndexWideningSplitsWithoutMovingPinnedReaders(t *testing.T) {
	i := testCollectionIndex(t)
	k := objectBinding{}
	var posts []collectionPosting
	// With the view pointer reserved, a narrow parent and extension hold seven revisions.
	const narrowRevisions = 7
	for object := 0; object < 60; object++ {
		for revision := 0; revision < narrowRevisions; revision++ {
			posts = append(posts, collectionPosting{collectionTestID(object), uint64(revision), collectionAddress{0, uint32(object*9 + revision + 1)}})
		}
	}
	if err := i.mergeRetained(k, 2, 1000, posts); err != nil {
		t.Fatal(err)
	}
	old := i.pin()
	defer releaseCollection(old)
	if leaves, _, _ := i.pages(k); leaves != 1 {
		t.Fatal("expected one nearly full narrow page")
	}
	id := collectionTestID(30)
	wide := collectionAddress{65536, 65535}
	if err := i.mergeRetained(k, 2, 1001, []collectionPosting{{id, 9, wide}}); err != nil {
		t.Fatal(err)
	}
	if leaves, _, _ := i.pages(k); leaves <= 1 {
		t.Fatal("widening did not split the full page")
	}
	if err := old.visit(id[:], k, func(_ [16]byte, r uint64, a collectionAddress) error {
		if r >= narrowRevisions {
			t.Fatal("pinned page changed", r)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	reopened, err := loadObjectIndex(i.file)
	if err != nil {
		t.Fatal(err)
	}
	for _, post := range append(posts, collectionPosting{id, 9, wide}) {
		got, ok, err := reopened.lookup(post.Object[:], k, post.Revision)
		if err != nil || !ok || got != post.Address {
			t.Fatal("reference lost across widened publication", post, got, ok, err)
		}
	}
}
