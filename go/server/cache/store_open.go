package cache

// Open, validate and recover canonical files before publishing the store.
import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// openStore takes one writer lease for this cache and recovers committed storage.
// Definitions are immutable; format experiments regenerate disposable test data.
func openStore(c Config) (s *Store, err error) {
	if !name(c.Cache) || c.Tenant == "" || c.Root == "" || c.Layout == nil {
		return nil, fmt.Errorf("cache, tenant, root and definition required")
	}
	l, e := ParseDefinition(c.Layout.Bytes)
	if e != nil {
		return nil, e
	}
	if c.ObjectIndex && l.Object < 0 {
		return nil, fmt.Errorf("object index requires an object identity role")
	}
	if c.BatchRecords <= 0 {
		c.BatchRecords = 1024
	}
	if c.BatchRecords > 65536 {
		return nil, fmt.Errorf("batch record limit exceeds journal capacity")
	}
	if c.BatchBytes <= 0 {
		c.BatchBytes = 4 << 20
	}
	if c.BatchBytes > MaxFrame*4 {
		return nil, fmt.Errorf("batch too large")
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.LiveIndexRecords <= 0 {
		c.LiveIndexRecords = 1_000_000
	}
	if c.NativeMemoryBytes == 0 {
		c.NativeMemoryBytes = 1 << 30
	}
	if c.CommitScheduler == nil {
		c.CommitScheduler = defaultCommitScheduler
	}
	s = &Store{mu: &sync.Mutex{}, config: c, requestedObjectIndex: c.ObjectIndex, layout: l, row: l.RowLayout(), trie: newTrie(), branches: map[Branch]*branchStore{}}
	s.ingress = trunkIngress(c.Root)
	s.location = time.UTC
	s.root = filepath.Join(c.Root, c.Cache)
	n := l.Definition.Resource
	s.collection = filepath.Join(s.root, n.Service, fmt.Sprintf("%s.v%d", n.Resource, n.Major))
	s.dir = filepath.Join(s.collection, fmt.Sprint(n.Minor))
	if err = os.MkdirAll(s.dir, 0700); err != nil {
		return nil, err
	}
	if c.group != nil {
		s.mu = &c.group.mu
	} else {
		s.lock, err = lockRoot(s.root)
		if err != nil {
			return nil, err
		}
	}
	cleanup := s
	defer func() {
		if err != nil {
			cleanup.release()
		}
	}()
	// Version-zero fixtures are recreated when their storage names change.
	// Never open an empty signature trie beside an old one: index references
	// would silently lose their meaning.
	if _, e := os.Stat(filepath.Join(s.dir, "resource.tri")); e == nil {
		return nil, fmt.Errorf("obsolete version-zero resource.tri in %s; recreate the cache using leaf.trie", s.dir)
	} else if !os.IsNotExist(e) {
		return nil, e
	}
	if err = s.removeSwapTemps(); err != nil {
		return nil, err
	}
	if err = initializeTrunkConfig(c.Root); err != nil {
		return nil, err
	}
	s.memory = newNativeMemory(c.NativeMemoryBytes)
	catalog := Catalog{Format: Format, Cache: c.Cache, Tenant: c.Tenant, Definitions: map[string][32]byte{}}
	if stored, e := readCatalog(c.Root, c.Cache); e == nil {
		catalog = stored
		if catalog.Format != Format || catalog.Cache != c.Cache || catalog.Tenant != c.Tenant {
			return nil, fmt.Errorf("catalog ownership mismatch")
		}
		if catalog.Definitions == nil {
			return nil, fmt.Errorf("invalid catalog")
		}
	} else if !os.IsNotExist(e) {
		return nil, e
	}
	if err = s.loadPolicy(); err != nil {
		return nil, err
	}
	if err = s.loadCapacity(); err != nil {
		return nil, err
	}
	if err = s.loadTransforms(); err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%s/%s.v%d/%d", n.Service, n.Resource, n.Major, n.Minor)
	s.binding = key
	if digest, ok := catalog.Definitions[key]; ok && digest != l.Digest {
		return nil, fmt.Errorf("immutable definition conflict")
	}
	for binding := range catalog.Definitions {
		p := splitBinding(binding)
		if p == nil {
			return nil, fmt.Errorf("invalid catalog binding")
		}
		if p.domain != n.Service || p.collection != n.Resource || p.major != n.Major || p.minor == n.Minor {
			continue
		}
		raw, e := os.ReadFile(definitionPath(c.Root, catalog.Definitions[binding]))
		if e != nil {
			return nil, e
		}
		prior, e := ParseDefinition(raw)
		if e != nil {
			return nil, e
		}
		for i, field := range []int{l.Object, l.Revision} {
			old := prior.Object
			if i == 1 {
				old = prior.Revision
			}
			if (field < 0) != (old < 0) || field >= 0 && l.Definition.Fields[field].Type != prior.Definition.Fields[old].Type {
				return nil, fmt.Errorf("identity type change requires a new object major")
			}
		}
	}
	if err = installDefinition(c.Root, l); err != nil {
		return nil, err
	}
	catalog.Definitions[key] = l.Digest
	if catalog.Generations == nil {
		catalog.Generations = map[string]map[Branch][16]byte{}
	}
	s.catalog = catalog
	if err = s.registerTaxonomy(); err != nil {
		return nil, err
	}

	s.shared, err = openFile(filepath.Join(s.dir, "leaf.trie"), "NUTSTRI6", l.Digest, [16]byte{}, 0)
	if err != nil {
		return nil, err
	}
	s.trieSizes = map[[32]byte]int{}
	rememberTrie := func() error {
		raw, e := encoder.Marshal(s.trie.nodes)
		if e != nil {
			return e
		}
		s.trieSizes[sha256.Sum256(raw)] = len(s.trie.nodes)
		return nil
	}
	if err = rememberTrie(); err != nil {
		return nil, err
	}
	var statistics []byte
	s.trie.fileEnd, statistics, err = readLeafFile(s.shared, func(raw []byte) error {
		var nodes []sharedNode
		if e := decoder.Unmarshal(raw, &nodes); e != nil {
			return e
		}
		for _, node := range nodes {
			if e := s.trie.add(node, l); e != nil {
				return e
			}
		}
		return rememberTrie()
	})
	if err != nil {
		return nil, err
	}
	if err = validateLeafStatistics(statistics, len(s.trie.nodes)); err != nil {
		return nil, err
	}
	s.trie.footer = len(statistics) != 0
	s.trie.persisted = len(s.trie.nodes)
	if err = s.openObjects(); err != nil {
		return nil, err
	}
	if err = s.openState(); err != nil {
		return nil, err
	}
	for branch := Read; branch <= Trace; branch++ {
		exists := false
		for _, ext := range []string{".indx", ".packs", ".seeds", ".pack"} {
			prefix := s.prefix(branch)
			if ext == ".seeds" || ext == ".pack" {
				prefix = branch.Prefix()
			}
			if _, e = os.Stat(filepath.Join(s.dir, prefix+ext)); e == nil {
				exists = true
			} else if !os.IsNotExist(e) {
				return nil, e
			}
		}
		if exists {
			if _, err = s.openBranch(branch); err != nil {
				return nil, err
			}
		} else if _, required := s.catalog.Generations[s.binding][branch]; required || s.catalog.RetiredRows[s.binding][branch] > 0 {
			return nil, fmt.Errorf("catalog-selected branch is missing")
		}
	}
	if err = s.publishStates(); err != nil {
		return nil, err
	}
	if b := s.branches[Write]; b != nil {
		if err = s.indexObjects(b); err != nil {
			return nil, err
		}
	}
	return s, nil
}
func (s *Store) openBranch(branch Branch) (b *branchStore, err error) {
	if !branch.Valid() {
		return nil, fmt.Errorf("invalid branch")
	}
	if b = s.branches[branch]; b != nil {
		return b, nil
	}
	b = &branchStore{branch: branch, layout: s.layout, packsEnd: headerSize, sumEnd: headerSize}
	base := filepath.Join(s.dir, s.prefix(branch))
	raw, e := os.ReadFile(base + ".packs")
	if e == nil {
		if len(raw) < headerSize {
			return nil, fmt.Errorf("short branch header")
		}
		copy(b.generation[:], raw[40:56])
	} else if os.IsNotExist(e) {
		if _, e = rand.Read(b.generation[:]); e != nil {
			return nil, e
		}
	} else {
		return nil, e
	}
	if s.catalog.RetiredRows[s.binding][branch] > 0 {
		for _, ext := range []string{".indx", ".packs", ".time"} {
			if _, e := os.Stat(base + ext); e != nil {
				return nil, fmt.Errorf("missing retained branch: %w", e)
			}
		}
	}
	cleanup := b
	defer func() {
		if err != nil {
			cleanup.close()
		}
	}()
	b.live = newNativeLive(s.config.LiveIndexRecords)
	if b.idx, err = openFile(base+".indx", "NUTSIDX6", s.layout.Digest, b.generation, s.row.Width); err != nil {
		return nil, err
	}
	if b.packs, err = openFile(base+".packs", "PILPOD00", s.layout.Digest, b.generation, 0); err != nil {
		return nil, err
	}
	if branch.Valid() && s.layout.aggregates != nil {
		if b.sum, err = openFile(base+".summ", "NUTSSUM6", s.layout.Digest, b.generation, 0); err != nil {
			return nil, err
		}
	}
	b.packsRetired, err = retiredBytes(b.packs)
	if err != nil {
		return nil, err
	}
	b.packsEnd += b.packsRetired
	if err = s.openStaging(b); err != nil {
		return nil, err
	}
	if err = s.recoverSealed(b); err != nil {
		return nil, err
	}
	if err = s.loadSummaries(b); err != nil {
		return nil, err
	}

	if err = b.mapIndex(); err != nil {
		return nil, err
	}
	if err = s.warmHot(b); err != nil {
		return nil, err
	}
	if err = s.openSwap(b); err != nil {
		return nil, err
	}
	if raw, e := os.ReadFile(filepath.Join(s.dir, s.prefix(branch)+".time")); e == nil && len(raw) >= TimeTrieHeader && int(int8(raw[47])) != s.policyFor(branch).UTCOffsetHours() {
		b.summaryDays = map[int64]summaryDayLocation{}
		b.summaryPackedThrough = -1
		b.calendarRollups = nil
		if err = s.packSummaryDays(b); err != nil {
			return nil, err
		}
	}

	if b.summaryPackedThrough > b.finalizedBefore {
		return nil, fmt.Errorf("calendar coverage exceeds finalized data")
	}
	if err = s.publishTime(b); err != nil {
		return nil, err
	}
	if err = b.openResidentFile(); err != nil {
		return nil, err
	}
	s.branches[branch] = b
	s.publishResident(b)
	return b, syncDir(s.dir)
}

// Append validates and durably stages an already encoded PIL event. Identical
// retries are deduplicated, including IDs in historical overlapping runs.
