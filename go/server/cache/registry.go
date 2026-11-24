package cache

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"trie-trunk/go/internal/node"
	"trie-trunk/go/internal/taxonomy"
)

// Registrations occupy an extensible footer section. Taxonomy nodes and their
// stable IDs never contain offsets into this mutable section.
const registrationsSection uint32 = 1

func readCatalog(root, cache string) (Catalog, error) {
	lock, err := lockRegistration(root, false)
	if err != nil {
		return Catalog{}, err
	}
	defer lock()
	raw, err := os.ReadFile(filepath.Join(root, "trunk.trie"))
	if err != nil {
		return Catalog{}, err
	}
	tree, err := taxonomy.Load(raw)
	if err != nil {
		return Catalog{}, err
	}
	all, err := catalogs(tree)
	if err != nil {
		return Catalog{}, err
	}
	c, ok := all[cache]
	if !ok {
		return Catalog{}, os.ErrNotExist
	}
	return c, nil
}
func catalogs(tree *taxonomy.Trie) (map[string]Catalog, error) {
	all := map[string]Catalog{}
	if raw := tree.Section(registrationsSection); len(raw) > 0 {
		if err := decoder.Unmarshal(raw, &all); err != nil {
			return nil, err
		}
	}
	return all, nil
}
func setCatalog(tree *taxonomy.Trie, c Catalog) error {
	all, err := catalogs(tree)
	if err != nil {
		return err
	}
	all[c.Cache] = c
	raw, err := encoder.Marshal(all)
	if err != nil {
		return err
	}
	tree.SetSection(registrationsSection, raw)
	return nil
}
func updateRegistration(root string, edit func(*taxonomy.Trie) error) error {
	return updateRegistrationSnapshot(root, func(tree *taxonomy.Trie) (*taxonomy.Trie, error) {
		return tree, edit(tree)
	})
}

// Native registration supplies a completed snapshot. Publication retains the
// existing lease, immutable node checks, recovery footer, and reader cache.
func updateRegistrationSnapshot(root string, edit func(*taxonomy.Trie) (*taxonomy.Trie, error)) error {
	path := filepath.Join(root, "trunk.trie")
	state := taxonomyState(path)
	state.mu.Lock()
	defer state.mu.Unlock()
	unlock, err := lockRegistration(root, true)
	if err != nil {
		return err
	}
	defer unlock()
	raw, err := os.ReadFile(path)
	if err == nil {
		if state.tree == nil || !bytes.Equal(raw, state.image) {
			state.tree, err = taxonomy.Load(raw)
			if err != nil {
				return err
			}
			state.image = raw
		}
	} else if os.IsNotExist(err) {
		state.tree = nil
		state.image = nil
	} else {
		return err
	}

	var next *taxonomy.Trie
	if state.tree == nil {
		var err error
		next, err = taxonomy.New("Trunk")
		if err != nil {
			return err
		}
	} else {
		next = state.tree.Clone()
	}
	next, err = edit(next)
	if err != nil {
		return err
	}
	handled, err := node.Publish(root, state.image, next.Bytes())
	if handled && err == nil {
		raw = next.Bytes()
	} else if !handled && err == nil {
		raw, err = taxonomy.SavePrepared(path, state.image, state.tree, next)
	}
	if err != nil {
		state.tree = nil
		state.image = nil
		return err
	}
	state.tree = next
	state.image = raw
	state.published.Store(&raw)
	return nil
}
func saveCatalog(root string, c Catalog) error {
	return updateRegistration(root, func(tree *taxonomy.Trie) error { return setCatalog(tree, c) })
}
func definitionPath(root string, digest [32]byte) string {
	return filepath.Join(root, "definitions", fmt.Sprintf("%x.rx", digest))
}
func installDefinition(root string, l *Layout) error {
	dir := filepath.Join(root, "definitions")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	return ensureMap(definitionPath(root, l.Digest), l.Bytes)
}

// Only registration/publication uses this directory lease. It adds no file and
// is never taken by event ingestion, search, or published-snapshot readers.
func lockRegistration(root string, write bool) (func(), error) {
	f, err := os.Open(root)
	if err != nil {
		return nil, err
	}
	mode := syscall.LOCK_SH
	if write {
		mode = syscall.LOCK_EX
	}
	if err = syscall.Flock(int(f.Fd()), mode); err != nil {
		f.Close()
		return nil, err
	}
	return func() { syscall.Flock(int(f.Fd()), syscall.LOCK_UN); f.Close() }, nil
}
