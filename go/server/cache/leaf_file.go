package cache

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// Node frames retain their byte offsets and reference ordinals. A zero-length
// frame marks the derived statistics footer; it can never be a node frame.
var leafStatsMarker = [8]byte{0, 0, 0, 0, 'S', 'T', 'A', 'T'}

const leafStatsMagic = "TTSTAT00"
const leafStatsTrailer = 24

// readLeafFile recovers incomplete tail writes, never complete corrupt nodes.
// An interrupted statistics publication loses only recomputable statistics.
func readLeafFile(f *os.File, visit func([]byte) error) (end int64, stats []byte, err error) {
	end = headerSize
	var h [8]byte
	for {
		n, e := f.ReadAt(h[:], end)
		if e == io.EOF {
			if n != 0 {
				e = f.Truncate(end)
			} else {
				e = nil
			}
			return end, nil, e
		}
		if e != nil {
			return end, nil, e
		}
		size := int(order.Uint32(h[:4]))
		if size == 0 {
			if h != leafStatsMarker {
				return end, nil, fmt.Errorf("invalid leaf statistics marker")
			}
			info, e := f.Stat()
			if e != nil {
				return end, nil, e
			}
			remaining := info.Size() - end
			if remaining < 8+leafStatsTrailer {
				return end, nil, f.Truncate(end)
			}
			var tail [leafStatsTrailer]byte
			if _, e = f.ReadAt(tail[:], info.Size()-leafStatsTrailer); e != nil {
				return end, nil, e
			}
			if string(tail[16:]) != leafStatsMagic {
				return end, nil, f.Truncate(end)
			}
			length := int64(order.Uint32(tail[8:12]))
			if order.Uint64(tail[:8]) != uint64(end) || length <= 0 || length > MaxFrame || remaining != 8+length+leafStatsTrailer {
				return end, nil, fmt.Errorf("invalid leaf statistics bounds")
			}
			stats = make([]byte, length)
			if _, e = f.ReadAt(stats, end+8); e != nil {
				return end, nil, e
			}
			if crc32.Checksum(stats, checksum) != order.Uint32(tail[12:16]) {
				return end, nil, fmt.Errorf("leaf statistics checksum mismatch")
			}
			return end, stats, nil
		}
		if size > MaxFrame {
			return end, nil, fmt.Errorf("invalid leaf node frame size")
		}
		raw := make([]byte, size)
		if _, e = f.ReadAt(raw, end+8); e != nil {
			if e == io.EOF {
				e = f.Truncate(end)
			}
			return end, nil, e
		}
		if crc32.Checksum(raw, checksum) != order.Uint32(h[4:]) {
			return end, nil, fmt.Errorf("leaf node checksum mismatch")
		}
		if e = visit(raw); e != nil {
			return end, nil, e
		}
		end += 8 + int64(size)
	}
}

func leafStatisticsFooter(end int64, raw []byte) ([]byte, error) {
	if end < headerSize || len(raw) == 0 || len(raw) > MaxFrame {
		return nil, fmt.Errorf("invalid leaf statistics size")
	}
	out := make([]byte, 8+len(raw)+leafStatsTrailer)
	copy(out, leafStatsMarker[:])
	copy(out[8:], raw)
	tail := out[8+len(raw):]
	order.PutUint64(tail, uint64(end))
	order.PutUint32(tail[8:], uint32(len(raw)))
	order.PutUint32(tail[12:], crc32.Checksum(raw, checksum))
	copy(tail[16:], leafStatsMagic)
	return out, nil
}

func appendLeafNodes(f *os.File, trie *sharedTrie, raw []byte) error {
	// Reclaim only derived footer bytes. Existing node frames never move.
	if trie.footer {
		if err := f.Truncate(trie.fileEnd); err != nil {
			return err
		}
		trie.footer = false
	}
	if err := appendRecord(f, raw); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	trie.fileEnd += 8 + int64(len(raw))
	return nil
}

func validateLeafStatistics(raw []byte, nodes int) error {
	if len(raw) == 0 {
		return nil
	}
	var stats TrieStatistics
	if err := decoder.Unmarshal(raw, &stats); err != nil {
		return err
	}
	if stats.Nodes != nodes || stats.Capacity != 65536 {
		return fmt.Errorf("leaf statistics node binding mismatch")
	}
	return nil
}
