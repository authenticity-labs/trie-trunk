// Package taxonomy stores fixed-width, immutable parent-linked nodes and an
// EOF-relative name directory. Only terminal branches receive compact leaf IDs.
package taxonomy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"maps"
	"math"
	"slices"
	"unicode/utf8"
)

type NodeID uint32

// ValueRef is the distance from the committed EOF to a directory entry's start.
type ValueRef uint32
type LeafID uint16
type Kind uint32

const (
	Trunk Kind = iota + 1
	Cache
	Service
	Resource
	Version
	Branch
)
const magic = "TTTAX000"
const tailMagic = "TTDIR000"
const slotBytes = 32
const headerBytes = 8 + 2*slotBytes
const nodeBytes = 20
const trailerBytes = 16

var order = binary.LittleEndian
var checksum = crc32.MakeTable(crc32.Castagnoli)

type Node struct {
	Kind              Kind
	Parent            NodeID
	Name, Type, Level ValueRef
}
type edge struct {
	parent           NodeID
	kind             Kind
	name, typ, level ValueRef
}
type Trie struct {
	nodes          []Node
	labels         map[ValueRef]string
	names          map[string]ValueRef
	values         []string // insertion order; serialized in reverse order before the trailer
	directoryBytes uint64
	sections       map[uint32][]byte
	children       map[edge]NodeID
	leaves         []NodeID
	leafIDs        map[NodeID]LeafID
}

func empty() *Trie {
	return &Trie{nodes: []Node{{}}, labels: map[ValueRef]string{}, names: map[string]ValueRef{}, sections: map[uint32][]byte{}, children: map[edge]NodeID{}, leaves: []NodeID{0}, leafIDs: map[NodeID]LeafID{}}
}

// Clone isolates a pending registration while readers retain the committed tree.
func (t *Trie) Clone() *Trie {
	return &Trie{nodes: append([]Node(nil), t.nodes...), labels: maps.Clone(t.labels), names: maps.Clone(t.names), values: append([]string(nil), t.values...), directoryBytes: t.directoryBytes, sections: maps.Clone(t.sections), children: maps.Clone(t.children), leaves: append([]NodeID(nil), t.leaves...), leafIDs: maps.Clone(t.leafIDs)}
}
func New(name string) (*Trie, error) {
	t := empty()
	_, err := t.Add(0, Trunk, name, "", "")
	return t, err
}
func (t *Trie) Root() NodeID {
	if len(t.nodes) > 1 {
		return 1
	}
	return 0
}
func (t *Trie) NodeCount() int { return len(t.nodes) - 1 }
func (t *Trie) Size() int {
	return headerBytes + (len(t.nodes)-1)*nodeBytes + int(t.directoryBytes) + trailerBytes + t.sectionSize()
}
func (t *Trie) Node(id NodeID) (Node, bool) {
	if id == 0 || uint64(id) >= uint64(len(t.nodes)) {
		return Node{}, false
	}
	return t.nodes[id], true
}
func (t *Trie) Label(ref ValueRef) (string, bool) { s, ok := t.labels[ref]; return s, ok }
func (t *Trie) Leaf(id LeafID) (NodeID, bool) {
	if id == 0 || int(id) >= len(t.leaves) {
		return 0, false
	}
	return t.leaves[id], true
}
func (t *Trie) BranchID(node NodeID) (LeafID, bool) { id, ok := t.leafIDs[node]; return id, ok }
func (t *Trie) Find(parent NodeID, kind Kind, name, typ, level string) (NodeID, bool) {
	refs := [3]ValueRef{}
	for i, s := range []string{name, typ, level} {
		if s != "" {
			var ok bool
			refs[i], ok = t.names[s]
			if !ok {
				return 0, false
			}
		}
	}
	id, ok := t.children[edge{parent, kind, refs[0], refs[1], refs[2]}]
	return id, ok
}
func (t *Trie) Path(id NodeID) ([]NodeID, error) {
	var path []NodeID
	for id != 0 {
		n, ok := t.Node(id)
		if !ok {
			return nil, fmt.Errorf("unknown taxonomy node")
		}
		path = append(path, id)
		id = n.Parent
	}
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}
	return path, nil
}
func validLabel(s string) bool { return s != "" && utf8.ValidString(s) }
func (t *Trie) validate(parent NodeID, kind Kind, name, typ, level string) error {
	if kind < Trunk || kind > Branch {
		return fmt.Errorf("invalid taxonomy kind")
	}
	if kind == Trunk {
		if parent != 0 || len(t.nodes) != 1 {
			return fmt.Errorf("taxonomy has one trunk")
		}
	} else {
		p, ok := t.Node(parent)
		if !ok || p.Kind+1 != kind {
			return fmt.Errorf("invalid taxonomy parent")
		}
	}
	if kind == Branch {
		if name != "" || !validLabel(typ) || (level != "" && !validLabel(level)) {
			return fmt.Errorf("invalid branch labels")
		}
	} else if !validLabel(name) || typ != "" || level != "" {
		return fmt.Errorf("invalid taxonomy label")
	}
	return nil
}
func (t *Trie) intern(s string) ValueRef {
	if s == "" {
		return 0
	}
	if ref, ok := t.names[s]; ok {
		return ref
	}
	t.directoryBytes += uint64(len(s)) + 4
	ref := ValueRef(t.directoryBytes + trailerBytes)
	t.values = append(t.values, s)
	t.names[s] = ref
	t.labels[ref] = s
	return ref
}
func (t *Trie) insert(n Node) NodeID {
	id := NodeID(len(t.nodes))
	t.nodes = append(t.nodes, n)
	t.children[edge{n.Parent, n.Kind, n.Name, n.Type, n.Level}] = id
	if n.Kind == Branch {
		t.leafIDs[id] = LeafID(len(t.leaves))
		t.leaves = append(t.leaves, id)
	}
	return id
}
func (t *Trie) Add(parent NodeID, kind Kind, name, typ, level string) (NodeID, error) {
	if id, ok := t.Find(parent, kind, name, typ, level); ok {
		return id, nil
	}
	if err := t.validate(parent, kind, name, typ, level); err != nil {
		return 0, err
	}
	if uint64(len(t.nodes)) > math.MaxUint32 {
		return 0, fmt.Errorf("taxonomy node capacity exceeded")
	}
	if kind == Branch && len(t.leaves) > math.MaxUint16 {
		return 0, fmt.Errorf("taxonomy branch capacity exceeded")
	}
	size := t.directoryBytes + trailerBytes
	needed := map[string]bool{}
	for _, s := range []string{name, typ, level} {
		if s != "" && !needed[s] {
			if _, ok := t.names[s]; !ok {
				size += uint64(len(s)) + 4
			}
			needed[s] = true
		}
	}
	if size > math.MaxUint32 {
		return 0, fmt.Errorf("taxonomy directory exceeds 32-bit references")
	}
	return t.insert(Node{kind, parent, t.intern(name), t.intern(typ), t.intern(level)}), nil
}

type commit struct {
	generation    uint64
	count, leaves uint32
	end           uint64
	slot          int
}

func encodeCommit(c commit) []byte {
	b := make([]byte, slotBytes)
	order.PutUint64(b, c.generation)
	order.PutUint32(b[8:], c.count)
	order.PutUint32(b[12:], c.leaves)
	order.PutUint64(b[16:], c.end)
	order.PutUint32(b[24:], crc32.Checksum(b[:24], checksum))
	return b
}
func selectCommit(raw []byte) (commit, error) {
	if len(raw) < headerBytes || string(raw[:8]) != magic {
		return commit{}, fmt.Errorf("invalid taxonomy header")
	}
	var best commit
	for i := 0; i < 2; i++ {
		b := raw[8+i*slotBytes : 8+(i+1)*slotBytes]
		c := commit{order.Uint64(b), order.Uint32(b[8:]), order.Uint32(b[12:]), order.Uint64(b[16:]), i}
		if c.generation == 0 || c.end > uint64(len(raw)) || c.end < uint64(headerBytes+trailerBytes) || order.Uint32(b[24:]) != crc32.Checksum(b[:24], checksum) || order.Uint32(b[28:]) != 0 {
			continue
		}
		if c.generation > best.generation {
			best = c
		}
	}
	if best.generation == 0 {
		return commit{}, fmt.Errorf("missing taxonomy commit")
	}
	return best, nil
}
func putNode(b []byte, n Node) {
	for i, v := range []uint32{uint32(n.Kind), uint32(n.Parent), uint32(n.Name), uint32(n.Type), uint32(n.Level)} {
		order.PutUint32(b[i*4:], v)
	}
}

// Bytes returns a complete canonical snapshot. Old node records and directory
// references are unchanged when nodes are appended; only the directory moves.
func (t *Trie) Bytes() []byte {
	raw := make([]byte, t.Size())
	copy(raw, magic)
	for i, n := range t.nodes[1:] {
		putNode(raw[headerBytes+i*nodeBytes:], n)
	}
	copy(raw[headerBytes+(len(t.nodes)-1)*nodeBytes:], t.sectionBytes())
	end := len(raw) - trailerBytes
	for _, s := range t.values {
		order.PutUint32(raw[end-4:], uint32(len(s)))
		end -= len(s) + 4
		copy(raw[end:], s)
	}
	order.PutUint64(raw[len(raw)-trailerBytes:], t.directoryBytes)
	copy(raw[len(raw)-8:], tailMagic)
	copy(raw[8:], encodeCommit(commit{1, uint32(len(t.nodes) - 1), uint32(len(t.leaves) - 1), uint64(len(raw)), 0}))
	return raw
}
func Load(raw []byte) (*Trie, error) {
	c, err := selectCommit(raw)
	if err != nil {
		return nil, err
	}
	end := int(c.end)
	if string(raw[end-8:end]) != tailMagic {
		return nil, fmt.Errorf("invalid taxonomy trailer")
	}
	size := order.Uint64(raw[end-trailerBytes:])
	nodesEnd := uint64(headerBytes) + uint64(c.count)*nodeBytes
	if size+trailerBytes > math.MaxUint32 || size > uint64(end-trailerBytes) || nodesEnd > uint64(end-trailerBytes)-size || c.leaves > math.MaxUint16 {
		return nil, fmt.Errorf("invalid taxonomy bounds")
	}
	t := empty()
	start := end - trailerBytes - int(size)
	if err = t.loadSections(raw, int(nodesEnd), start); err != nil {
		return nil, err
	}
	for at := end - trailerBytes; at > start; {
		if at-start < 4 {
			return nil, fmt.Errorf("invalid taxonomy directory")
		}
		length := uint64(order.Uint32(raw[at-4:]))
		if length > uint64(at-start-4) {
			return nil, fmt.Errorf("invalid taxonomy label length")
		}
		at -= int(length) + 4
		s := string(raw[at : at+int(length)])
		if !validLabel(s) {
			return nil, fmt.Errorf("invalid taxonomy label")
		}
		if _, ok := t.names[s]; ok {
			return nil, fmt.Errorf("duplicate taxonomy label")
		}
		if t.intern(s) != ValueRef(end-at) {
			return nil, fmt.Errorf("invalid taxonomy directory reference")
		}
	}
	label := func(ref ValueRef) (string, bool) {
		if ref == 0 {
			return "", true
		}
		return t.Label(ref)
	}
	for i := uint32(0); i < c.count; i++ {
		b := raw[headerBytes+int(i)*nodeBytes:]
		n := Node{Kind(order.Uint32(b)), NodeID(order.Uint32(b[4:])), ValueRef(order.Uint32(b[8:])), ValueRef(order.Uint32(b[12:])), ValueRef(order.Uint32(b[16:]))}
		name, a := label(n.Name)
		typ, d := label(n.Type)
		level, e := label(n.Level)
		if !a || !d || !e {
			return nil, fmt.Errorf("unknown taxonomy directory reference")
		}
		if err = t.validate(n.Parent, n.Kind, name, typ, level); err != nil {
			return nil, err
		}
		if _, ok := t.children[edge{n.Parent, n.Kind, n.Name, n.Type, n.Level}]; ok {
			return nil, fmt.Errorf("duplicate taxonomy node")
		}
		if n.Kind == Branch && len(t.leaves) > math.MaxUint16 {
			return nil, fmt.Errorf("taxonomy branch capacity exceeded")
		}
		t.insert(n)
	}
	if t.Root() == 0 || uint32(len(t.leaves)-1) != c.leaves {
		return nil, fmt.Errorf("invalid taxonomy root or branch count")
	}
	return t, nil
}

// Extends checks identity preservation before an owner publishes a replacement.
func (t *Trie) Extends(old *Trie) bool {
	if len(t.nodes) < len(old.nodes) {
		return false
	}
	for i, n := range old.nodes {
		if t.nodes[i] != n {
			return false
		}
	}
	for ref, s := range old.labels {
		if t.labels[ref] != s {
			return false
		}
	}
	return true
}

// Sections are independent of node and name addressing. Values are immutable
// snapshots. Callers replace a section on a cloned trie before publication.
func (t *Trie) Section(id uint32) []byte         { return t.sections[id] }
func (t *Trie) SetSection(id uint32, raw []byte) { t.sections[id] = bytes.Clone(raw) }
func (t *Trie) sameSections(old *Trie) bool {
	return maps.EqualFunc(t.sections, old.sections, bytes.Equal)
}

const sectionTailBytes = 16
const sectionEntryBytes = 24
const sectionMagic = "TTSECT00"

func (t *Trie) sectionSize() int {
	size := sectionTailBytes + len(t.sections)*sectionEntryBytes
	for _, b := range t.sections {
		size += len(b)
	}
	return size
}
func (t *Trie) sectionBytes() []byte {
	ids := make([]uint32, 0, len(t.sections))
	size := sectionTailBytes
	for id, b := range t.sections {
		ids = append(ids, id)
		size += len(b) + sectionEntryBytes
	}
	slices.Sort(ids)
	raw := make([]byte, size)
	at := 0
	dir := size - sectionTailBytes - len(ids)*sectionEntryBytes
	for i, id := range ids {
		b := t.sections[id]
		copy(raw[at:], b)
		entry := raw[dir+i*sectionEntryBytes:]
		order.PutUint32(entry, id)
		order.PutUint32(entry[4:], crc32.Checksum(b, checksum))
		// Distances from the section directory end, not file positions or node IDs.
		order.PutUint64(entry[8:], uint64(size-at))
		order.PutUint64(entry[16:], uint64(len(b)))
		at += len(b)
	}
	order.PutUint64(raw[size-sectionTailBytes:], uint64(len(ids)))
	copy(raw[size-8:], sectionMagic)
	return raw
}
func (t *Trie) loadSections(raw []byte, nodesEnd, end int) error {
	if end-nodesEnd < sectionTailBytes || string(raw[end-8:end]) != sectionMagic {
		return fmt.Errorf("invalid taxonomy sections")
	}
	count := order.Uint64(raw[end-sectionTailBytes:])
	available := uint64(end - nodesEnd - sectionTailBytes)
	if count > available/sectionEntryBytes {
		return fmt.Errorf("invalid taxonomy section directory")
	}
	dir := end - sectionTailBytes - int(count)*sectionEntryBytes
	priorEnd := nodesEnd
	var priorID uint32
	for i := uint64(0); i < count; i++ {
		b := raw[dir+int(i)*sectionEntryBytes:]
		id := order.Uint32(b)
		distance := order.Uint64(b[8:])
		length := order.Uint64(b[16:])
		if distance > uint64(end-nodesEnd) || distance < uint64(end-dir) || length > distance-uint64(end-dir) || i > 0 && id <= priorID {
			return fmt.Errorf("invalid taxonomy section bounds")
		}
		start := end - int(distance)
		if start < priorEnd || i > 0 && start != priorEnd {
			return fmt.Errorf("overlapping taxonomy sections")
		}
		value := raw[start : start+int(length)]
		if crc32.Checksum(value, checksum) != order.Uint32(b[4:]) {
			return fmt.Errorf("taxonomy section checksum mismatch")
		}
		t.sections[id] = bytes.Clone(value)
		priorEnd = start + int(length)
		priorID = id
	}
	if count > 0 && priorEnd != dir {
		return fmt.Errorf("invalid taxonomy section extent")
	}
	return nil
}

// footerStart locates the movable footer, including all extension sections.
func (t *Trie) footerStart(end int) int {
	return end - trailerBytes - int(t.directoryBytes) - t.sectionSize()
}
