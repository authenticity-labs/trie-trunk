package cache

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"trie-trunk/go/internal/node"
	"trie-trunk/go/internal/taxonomy"
)

// Registration is rare and serialized per trunk. Searches never acquire this
// mutex. No existing taxonomy record or data-file address is rewritten.
type taxonomyRegistration struct {
	owner     *node.Result
	readerMu  sync.Mutex
	reader    atomic.Pointer[registeredRouteReader]
	mu        sync.Mutex
	published atomic.Pointer[[]byte]
	tree      *taxonomy.Trie
	image     []byte
}

var taxonomyRegistrations sync.Map

func taxonomyState(path string) *taxonomyRegistration {
	if value, ok := taxonomyRegistrations.Load(path); ok {
		return value.(*taxonomyRegistration)
	}
	state := &taxonomyRegistration{}
	runtime.SetFinalizer(state, func(s *taxonomyRegistration) {
		if s.owner != nil {
			s.owner.Close()
		}
	})
	value, _ := taxonomyRegistrations.LoadOrStore(path, state)
	return value.(*taxonomyRegistration)
}

func taxonomyBytes(root string) ([]byte, error) {
	path := filepath.Join(root, "trunk.trie")
	state := taxonomyState(path)
	if owner, err := node.Registry(root); err != nil {
		return nil, err
	} else if owner != nil {
		state.mu.Lock()
		defer state.mu.Unlock()
		// Retain the old Arc until comparison, so allocator address reuse cannot
		// mistake a new registry for the previous publication.
		current := owner.Bytes(0, 0)
		if state.owner != nil && len(current) > 0 && &state.owner.Bytes(0, 0)[0] == &current[0] {
			owner.Close()
			return *state.published.Load(), nil
		}
		raw := bytes.Clone(current)
		if state.owner != nil {
			state.owner.Close()
		}
		state.owner = owner
		state.published.Store(&raw)
		return raw, nil
	}
	if raw := state.published.Load(); raw != nil {
		return *raw, nil
	}
	// A restarted read node has no in-process registration publication yet.
	// Load the published image once; Rust opens and interprets it in the reader.
	state.mu.Lock()
	defer state.mu.Unlock()
	if raw := state.published.Load(); raw != nil {
		return *raw, nil
	}
	unlock, err := lockRegistration(root, false)
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	state.published.Store(&raw)
	return raw, nil
}

func (s *Store) registerTaxonomy() error {
	return updateRegistration(s.config.Root, func(tree *taxonomy.Trie) error {
		var err error

		kind := s.config.ResourceType
		if stored := s.catalog.ResourceTypes[s.binding]; stored != "" {
			if kind != "" && kind != stored {
				return fmt.Errorf("immutable resource type conflict")
			}
			kind = stored
		}
		// Unclassified registrations retain the generic strategy name. HTTP/Jobs/etc.
		// must be provided explicitly; never infer taxonomy from field names.
		if kind == "" {
			kind = s.layout.Definition.Resource.Strategy
			if kind == "" {
				kind = "log"
			}
		}
		if !name(kind) {
			return fmt.Errorf("invalid resource type")
		}
		n := s.layout.Definition.Resource
		parent := tree.Root()
		for _, part := range []struct {
			k taxonomy.Kind
			s string
		}{{taxonomy.Cache, s.config.Cache}, {taxonomy.Service, n.Service}, {taxonomy.Resource, n.Resource}, {taxonomy.Version, fmt.Sprintf("%d.%d", n.Major, n.Minor)}} {
			parent, err = tree.Add(parent, part.k, part.s, "", "")
			if err != nil {
				return err
			}
		}
		branches := map[Branch]uint16{}
		for b := Read; b <= Trace; b++ {
			at, e := tree.Add(parent, taxonomy.Branch, "", kind, b.Name())
			if e != nil {
				return e
			}
			id, ok := tree.BranchID(at)
			if !ok {
				return fmt.Errorf("missing taxonomy branch ID")
			}
			branches[b] = uint16(id)
		}
		if s.catalog.ResourceTypes == nil {
			s.catalog.ResourceTypes = map[string]string{}
		}
		s.catalog.ResourceTypes[s.binding] = kind
		if err = setCatalog(tree, s.catalog); err != nil {
			return err
		}

		s.taxonomyBranches = branches
		s.config.ResourceType = kind
		return nil
	})
}
