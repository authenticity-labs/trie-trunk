package cache

func collectionTestID(n int) (id [16]byte) { order.PutUint64(id[8:], uint64(n+1)); return }
