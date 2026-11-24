package cache

// Store ownership and lifecycle. Writers and publication remain serialized by mu.
import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"trie-trunk/go/internal/definitionmeta"
	"trie-trunk/go/internal/seedtransform"
)

type Config struct {
	sealHook            func(string) error // deterministic publication/failure probes in tests
	indexCapacity       uint64             // test-only capacity boundary; production uses all U32 slots
	group               *storeGroup
	Root, Cache, Tenant string
	Storage             string // pack or browse; interpreted by the shared Rust storage policy.
	ResourceType        string // Explicit taxonomy classification, independent of the Definition.
	Layout              *Layout
	ObjectIndex         bool
	BatchRecords        int
	BatchBytes          int
	CommitScheduler     *CommitScheduler // nil shares the process-wide single worker
	LiveIndexRecords    int              // maximum resident/staged identities per branch; default 1 million
	NativeMemoryBytes   uint64           // native payload budget per definition; default 1 GiB
	Now                 func() time.Time
}
type Catalog struct {
	ResourceTypes       map[string]string            `pil:",omitempty"`
	RetiredRows         map[string]map[Branch]uint64 `pil:",omitempty"`
	Format              uint32
	Cache, Tenant       string
	Definitions         map[string][32]byte
	Generations         map[string]map[Branch][16]byte
	GenerationSequences map[string]map[Branch]uint64 `pil:",omitempty"`
	Preferred           map[string][32]byte          `pil:",omitempty"`
	// Exact Definition digest -> PAC ranges in definition.meta and synonym.map.
	// Optional presentation/translation resources do not change event storage.
	DefinitionResources map[string]definitionmeta.Resources `pil:",omitempty"`
}
type pending struct {
	id    [16]byte
	frame []byte
}
type branchStore struct {
	readerIndex                      *residentMapping
	readerSummary                    *residentFile
	readerFile                       *residentFile
	sealedMemory                     map[int64]*nativeArena // immutable PACs only
	ingressPages                     map[int64]*ingressPage
	ingressTail                      []pending
	memory                           map[int64]*nativeArena
	baseRows, packsRetired, memberEnd uint64
	blockBase                        uint32
	branch                           Branch
	layout                           *Layout
	generation                       [16]byte
	idx, packs, sum                   *os.File
	swap                             *os.File
	seedHour                         int64
	stagingPacks                            map[int64]*os.File
	ingressDirty                     map[*os.File]bool
	sealingBefore                    int64
	finalizedBefore                  int64
	rows                             []byte
	mapping                          []byte
	runs                             []Run
	time                             timeDirectory
	timeAnchor                       *TimeAnchor
	timeDirty                        bool
	live                             *nativeLive
	liveViews                        map[string]liveViewSnapshot
	pendingVersion                   uint64
	provisionalRecords               map[int64]uint32
	pendingBytes                     int
	packsEnd, sumEnd                  uint64
	summaries                        []Summary
	accumulators                     summaryRing
	summaryDays                      map[int64]summaryDayLocation
	summaryPackedThrough             int64
	calendarRollups                  []CalendarRollup
}
type Store struct {
	ingress            *ingressMeter
	identityScratch    [10]byte // writer-owned, rebound to native output before publication
	taxonomyBranches   map[Branch]uint16
	publishedStates    atomic.Pointer[[]byte]
	published          [5]atomic.Pointer[residentSnapshot]
	summaryExports     map[string][]byte
	summaryExportOrder []string
	summaryExportBytes int
	sealing            chan struct{} // writer-lock protected; maintenance waits without holding mu
	memory             *nativeMemory
	capacityClosed     bool
	transforms         *TransformConfig
	providers          map[string]*seedtransform.Plan
	searchContext      context.Context
	searchCancel       context.CancelFunc
	searchWG           sync.WaitGroup
	searchTouched      bool
	searchOnce         sync.Once
	searchGates        [5]searchSerial
	closeDone          chan struct{}

	mu                    *sync.Mutex
	config                Config
	requestedObjectIndex  bool
	layout                *Layout
	row                   RowLayout
	root, dir, collection string
	lock, shared          *os.File
	audit                 *os.File
	trie                  sharedTrie
	trieSizes             map[[32]byte]int
	branches              map[Branch]*branchStore
	location              *time.Location
	scratch               Frame
	closed                bool
	failed                error
	objects               *objectIndex
	state                 *objectState
	policy                Policy
	branchPolicies        map[Branch]Policy
	catalog               Catalog
	binding               string
}

func (s *Store) check() error {
	if s.closed {
		return fmt.Errorf("cache closed")
	}
	if s.state != nil && s.state.failed != nil {
		return s.state.failed
	}
	return s.failed
}
func (s *Store) fail(e error) error {
	if e != nil {
		s.failed = e
	}
	return e
}
func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.waitSealing()
	if e := s.check(); e != nil {
		return e
	}
	for branch := Read; branch <= Trace; branch++ {
		if b := s.branches[branch]; b != nil {
			if e := s.flush(b); e != nil {
				return s.fail(e)
			}
		}
	}
	return nil
}
func (b *branchStore) close() error {
	b.readerSummary.release()
	b.readerSummary = nil
	b.readerFile.release()
	b.readerFile = nil
	for _, a := range b.sealedMemory {
		a.close()
	}
	b.sealedMemory = nil
	for _, v := range b.liveViews {
		if v.release != nil {
			v.release()
		}
	}
	for _, a := range b.memory {
		a.close()
	}
	b.memory = nil
	b.live.close()
	b.live = nil
	b.liveViews = nil
	e := b.unmapIndex()
	for _, f := range b.stagingPacks {
		e = errors.Join(e, f.Close())
	}
	b.stagingPacks = nil
	for _, f := range []*os.File{b.idx, b.packs, b.sum, b.swap} {
		if f != nil {
			e = errors.Join(e, f.Close())
		}
	}
	return e
}
func (s *Store) release() error {
	for _, p := range s.providers {
		p.Close()
		p.Source.Close()
	}
	s.providers = nil
	for i := range s.published {
		if old := s.published[i].Swap(nil); old != nil {
			old.release()
		}
	}
	defer s.memory.close()
	var e error
	for _, b := range s.branches {
		e = errors.Join(e, b.close())
	}
	if s.shared != nil {
		e = errors.Join(e, s.shared.Close())
	}
	if s.audit != nil {
		e = errors.Join(e, s.audit.Close())
	}
	if s.config.group == nil && s.objects != nil && s.objects.file != nil {
		e = errors.Join(e, s.objects.file.Close())
	}
	if s.config.group == nil && s.state != nil {
		e = errors.Join(e, s.state.file.Close())
	}
	if s.lock != nil {
		e = errors.Join(e, s.lock.Close())
	}
	return e
}
func (s *Store) Close() error {
	s.mu.Lock()
	s.waitSealing()
	if s.closed {
		done := s.closeDone
		s.mu.Unlock()
		if done != nil {
			<-done
		}
		return nil
	}
	s.closeDone = make(chan struct{})
	e := s.failed
	if e == nil {
		for branch := Read; branch <= Trace; branch++ {
			if b := s.branches[branch]; b != nil {
				if e = s.flush(b); e != nil {
					break
				}
			}
		}
	}
	if e == nil {
		e = s.publishStatistics()
	}
	s.closed = true
	if s.searchCancel != nil {
		s.searchCancel()
	}
	touched := s.searchTouched
	s.mu.Unlock()
	s.finishSearch(touched)
	s.mu.Lock()
	e = errors.Join(e, s.release())
	s.mu.Unlock()
	close(s.closeDone)
	return e
}
func (s *Store) Generation(branch Branch) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b := s.branches[branch]; b != nil {
		return hex.EncodeToString(b.generation[:])
	}
	return ""
}

// Called with mu held. Only maintenance waits; reads continue against the
// published disk prefix plus the native pending set during a seal.
func (s *Store) waitSealing() {
	for s.sealing != nil {
		done := s.sealing
		s.mu.Unlock()
		<-done
		s.mu.Lock()
	}
}
