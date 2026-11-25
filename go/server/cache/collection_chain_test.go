package cache

func collectionPosts(id [16]byte, start, end int) []collectionPosting {
	out := make([]collectionPosting, 0, end-start)
	for r := start; r < end; r++ {
		out = append(out, collectionPosting{id, uint64(r), collectionAddress{17, uint32(r + 1)}})
	}
	return out
}
