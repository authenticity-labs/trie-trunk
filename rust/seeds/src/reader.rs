//! Positional storage adapter for the registered Pack reader. The storage owner
//! pins a publication and supplies Time-selected addresses; no Box directory,
//! relocation, checksum scan, or host decoding is introduced here.
mod index;
pub(crate) use leaf_filter::IndexWindow;
mod leaf_filter;
pub(crate) mod paged;
use crate::{
    Cursor, Error, MemoryBudget, PackSource, Query, SeedDefinition, SeedPackSnapshot, error,
};
pub use index::{AddressSpace, IndexSelection};
use std::{
    io,
    sync::{Arc, Mutex, Weak},
};

/// Environment I/O only. Implementations may use the existing pooled handles
/// and read windows. A selection's published byte ranges must remain immutable
/// until its reader, cursors, and results have released them.
pub trait Storage: Send + Sync {
    fn read_at(&self, file: u64, offset: u64, target: &mut [u8]) -> io::Result<usize>;
}

/// Logical addresses from the pinned Time snapshot, in the planner's chosen
/// traversal order. `length` bounds the published file, including its footer.
pub struct Selection {
    pub file: u64,
    pub length: u64,
    pub addresses: Vec<u64>,
}

#[derive(Clone)]
pub struct Reader(Arc<State>, Option<std::ops::Range<u64>>, Option<Arc<Vec<std::ops::Range<u64>>>>);
struct State {
    storage: Arc<dyn Storage>,
    scope: crate::registration::Packs,
    selection: Selection,
    maximum: usize,
    budget: MemoryBudget,
    // Weak owners share already-resident Packs across queries without making
    // this selection a payload cache. Distinct Packs can load independently.
    cache: Vec<Mutex<Weak<SeedPackSnapshot>>>,
    pins: Mutex<Vec<Arc<SeedPackSnapshot>>>,
    index: Option<index::Indexes>,
    metadata: Vec<Mutex<Option<Arc<Metadata>>>>,
    views: Vec<Mutex<Weak<crate::ReadPack>>>,
    metadata_size: std::sync::atomic::AtomicUsize,
    started: std::sync::atomic::AtomicBool,
}
/// Complete Pack metadata for one immutable publication. The owner includes
/// directory, commons, application headers and extension bytes, but no members.
/// A selected Reader retains it across queries; clones share its one admission.
pub struct Metadata {
    plan: Arc<pil_codec::api::PackMetadata>,
    definition: Arc<SeedDefinition>,
    lineage: Option<Arc<std::collections::HashMap<usize, Arc<SeedDefinition>>>>,
    storage: Option<Arc<dyn crate::StorageBinding>>,
    memory: Arc<crate::MemoryLease>,
    compressed: bool,
}
/// A pre-admitted metadata handoff. Dropping it before publication releases
/// admission. A queued write never keeps an abandoned query selection alive.
pub(crate) struct Retention {
    state: Weak<State>,
    ordinal: usize,
    metadata: Arc<Metadata>,
    pack: Weak<SeedPackSnapshot>,
}
impl Retention {
    pub(crate) fn publish(self) -> Result<(), Error> {
        let Some(state) = self.state.upgrade() else { return Ok(()); };
        let mut slot = state.metadata[self.ordinal].lock()
            .map_err(|_| error("Pack metadata cache poisoned"))?;
        let mut payload = state.cache[self.ordinal].lock()
            .map_err(|_| error("Pack reader cache poisoned"))?;
        if slot.is_none() {
            state.metadata_size.fetch_add(self.metadata.retained_bytes(), std::sync::atomic::Ordering::Relaxed);
            *slot = Some(self.metadata);
        }
        *payload = self.pack;
        Ok(())
    }
}
impl Metadata {
    /// A common value borrows the resident metadata and performs no member I/O.
    /// Absent here means no common value, not that every member omits the field.
    pub fn common<'k>(
        &self,
        key: impl Into<crate::FieldKey<'k>>,
    ) -> Result<pil_codec::api::FieldBytes<'_>, Error> {
        self.plan.common(key)
    }
    pub fn header(&self, name: &str) -> Result<Option<&[u8]>, Error> {
        self.plan.header(name)
    }
    pub fn retained_bytes(&self) -> usize {
        self.plan.retained_bytes()
    }
    pub fn len(&self) -> usize {
        self.plan.len()
    }
    pub fn is_empty(&self) -> bool {
        self.plan.is_empty()
    }
    pub fn definition(&self) -> &Arc<SeedDefinition> {
        &self.definition
    }
}
impl crate::registration::Packs {
    pub fn reader(
        &self,
        storage: Arc<dyn Storage>,
        selection: Selection,
        maximum: usize,
        budget: MemoryBudget,
    ) -> Result<Reader, Error> {
        if maximum == 0 || selection.addresses.iter().any(|a| *a >= selection.length) {
            return Err(error("invalid published Pack read selection"));
        }
        let mut cache = Vec::new();
        cache
            .try_reserve_exact(selection.addresses.len())
            .map_err(|_| error("reader selection allocation failed"))?;
        cache.resize_with(selection.addresses.len(), || Mutex::new(Weak::new()));
        Ok(Reader(
            Arc::new(State {
                storage,
                scope: self.reader_scope(),
                selection,
                maximum,
                budget,
                metadata: (0..cache.len()).map(|_| Mutex::new(None)).collect(),
                views: (0..cache.len()).map(|_| Mutex::new(Weak::new())).collect(),
                cache,
                pins: Mutex::new(Vec::new()),
                index: None,
                metadata_size: std::sync::atomic::AtomicUsize::new(0),
                started: std::sync::atomic::AtomicBool::new(false),
            }),
            None,
            None,
        ))
    }
}
impl Reader {
    /// Retain complete metadata for a published Pack, shared by all queries of
    /// this selection. Plain Packs read metadata only. A compressed container
    /// must first expand; its member allocation is released after preparation.
    pub fn metadata(&self, ordinal: usize) -> Result<Arc<Metadata>, Error> {
        self.acquire_metadata(ordinal).map(|(metadata, _)| metadata)
    }
    /// Prepare at most `count` selected Pack metadata regions. Return the next
    /// ordinal so environment schedulers can yield between bounded batches.
    /// Time-empty Packs are skipped. Completed entries survive a later error;
    /// retry from `first` reuses them without another read or allocation.
    pub fn prepare(&self, first: usize, count: usize) -> Result<usize, Error> {
        if first > self.len() || count == 0 {
            return Err(error("invalid metadata preparation range"));
        }
        let end = first.saturating_add(count).min(self.len());
        let mut selected = self.clone();
        for ordinal in first..end {
            if self.0.metadata[ordinal]
                .lock()
                .map_err(|_| error("Pack metadata cache poisoned"))?
                .is_some()
            {
                continue;
            }
            if match selected.row_selection(ordinal)? {
                crate::RowSelection::All => false,
                crate::RowSelection::Range(rows) => rows.is_empty(),
                crate::RowSelection::Ranges(rows) => rows.is_empty(),
            } {
                continue;
            }
            self.metadata(ordinal)?;
        }
        Ok(end)
    }
    /// Total currently retained metadata for this selected publication, not
    /// payload windows, query output, or the entire runtime's resident memory.
    pub fn metadata_bytes(&self) -> Result<usize, Error> {
        Ok(self
            .0
            .metadata_size
            .load(std::sync::atomic::Ordering::Relaxed))
    }
    fn acquire_metadata(
        &self,
        ordinal: usize,
    ) -> Result<(Arc<Metadata>, Option<Arc<SeedPackSnapshot>>), Error> {
        let state = &self.0;
        state
            .started
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let slot = state
            .metadata
            .get(ordinal)
            .ok_or_else(|| error("Pack ordinal outside reader selection"))?;
        let mut slot = slot
            .lock()
            .map_err(|_| error("Pack metadata cache poisoned"))?;
        if let Some(metadata) = &*slot {
            return Ok((metadata.clone(), None));
        }
        let address = state.selection.addresses[ordinal];
        let available = state.selection.length - address;
        let mut prefix = [0; 27];
        let count = available.min(prefix.len() as u64) as usize;
        read_exact(
            state.storage.as_ref(),
            state.selection.file,
            address,
            &mut prefix[..count],
        )?;
        let compressed = pil_codec::pac_envelope::Control::read(prefix[0])?.compressed;
        let resident = state.cache[ordinal]
            .lock()
            .map_err(|_| error("Pack reader cache poisoned"))?
            .upgrade();
        let resident = match resident {
            Some(pack) => Some(pack),
            None if compressed => Some(self.pack(ordinal)?),
            None => None,
        };
        let source = resident
            .as_ref()
            .map(|p| p.stored_bytes())
            .unwrap_or(&prefix[..count]);
        let envelope = pil_codec::pac_envelope::Plain::read_prefix(source)?;
        if envelope.size > state.maximum as u64 || !compressed && envelope.size > available {
            return Err(error("Pack exceeds published read bounds"));
        }
        let length = usize::try_from(envelope.header_length).map_err(|_| Error::Overflow)?;
        let lease = state.budget.reserve(length)?;
        let mut header = Vec::new();
        header
            .try_reserve_exact(length)
            .map_err(|_| error("Pack metadata allocation failed"))?;
        header.resize(length, 0);
        let copied = length.min(source.len());
        header[..copied].copy_from_slice(&source[..copied]);
        if copied < length {
            read_exact(
                state.storage.as_ref(),
                state.selection.file,
                address.checked_add(copied as u64).ok_or(Error::Overflow)?,
                &mut header[copied..],
            )?;
        }
        let metadata = self.compile_metadata(
            ordinal,
            header,
            lease,
            compressed,
            resident.as_deref(),
            resident.as_ref().and_then(|pack| pack.storage.clone()),
        )?;
        state.metadata_size.fetch_add(
            metadata.retained_bytes(),
            std::sync::atomic::Ordering::Relaxed,
        );
        *slot = Some(metadata.clone());
        Ok((metadata, resident))
    }
    fn compile_metadata(
        &self,
        ordinal: usize,
        header: Vec<u8>,
        lease: crate::MemoryLease,
        compressed: bool,
        resident: Option<&SeedPackSnapshot>,
        binding: Option<Arc<dyn crate::StorageBinding>>,
    ) -> Result<Arc<Metadata>, Error> {
        let state = &self.0;
        let address = state.selection.addresses[ordinal];
        let branch = crate::registration::pack_registration(&header)?;
        let definition = state.scope.definition(branch)?.clone();
        let mut lineage = std::collections::HashMap::new();
        let rx = match resident.map(|pack| pack.pack.prescription()) {
            Some(rx) if !rx.bindings().is_empty() => Arc::new(rx.clone()),
            _ => definition.rx.clone(),
        };
        let plan = Arc::new(pil_codec::api::PackMetadata::registered(
            rx, header, |reference| {
                let branch = u16::try_from(reference).map_err(|_| Error::Overflow)?;
                let selected = state.scope.definition(branch)?.clone();
                let rx = match resident.and_then(|pack| pack.lineage.as_deref()) {
                    Some(prior) => prior.values().find(|d| d.same_as(&selected))
                        .ok_or_else(|| error("Ready Pack lineage differs from reader scope"))?.rx.clone(),
                    None => selected.rx.clone(),
                };
                lineage.insert(rx.source_identity(), selected);
                Ok(rx)
            },
        )?);
        let lineage = (!lineage.is_empty()).then(|| Arc::new(lineage));
        if !plan.is_sealed() {
            return Err(error("unsealed Packs must use resident storage"));
        }
        let binding = if binding.is_some() {
            binding
        } else {
            state.bind_index(ordinal, address, &definition, plan.len(), plan.version_directory())?

        };
        Ok(Arc::new(Metadata {
            plan,
            definition,
            lineage,
            storage: binding,
            memory: Arc::new(lease),
            compressed,
        }))
    }
    /// Retain metadata from a collected, sealed Pack already published at this
    /// selection's address. The storage owner guarantees these exact plain bytes
    /// were written there. Only metadata is copied; member ownership is released.
    /// Existing metadata is immutable and is never replaced by this handoff.
    pub fn retain(&self, ordinal: usize, pack: &Arc<SeedPackSnapshot>) -> Result<(), Error> {
        self.stage_retention(ordinal, pack)?.publish()
    }
    /// Pin complete resident Packs independently of durable publication. Index
    /// restoration is identical to disk reads; cursors share these allocations.
    pub fn ready(&self, owner: &crate::replication::Readiness, source: &str) -> Result<(), Error> {
        for (ordinal, address) in self.0.selection.addresses.iter().enumerate() {
            let Some(pack)=owner.get(source,*address)? else {continue};
            let mut retained=self.stage_retention(ordinal,&pack)?;
            let pack = if Arc::ptr_eq(pack.definition(), &retained.metadata.definition) {
                pack
            } else {
                // Registry refresh changes Arc identities, not immutable field
                // layouts. Rebind once per reader, retaining the same payload
                // and the old codec's prescription-to-record mapping.
                let definitions=self.0.scope.reader_definitions();
                let lineage=pack.lineage.as_ref().map(|prior| {
                    prior.iter().map(|(&key, old)| {
                        definitions.iter().find(|d| d.same_as(old)).cloned()
                            .map(|d|(key,d)).ok_or_else(||error("Ready Pack lineage differs from reader scope"))
                    }).collect::<Result<std::collections::HashMap<_,_>,Error>>().map(Arc::new)
                }).transpose()?;
                Arc::new(SeedPackSnapshot {
                    definition:retained.metadata.definition.clone(),lineage,pack:pack.pack.clone(),
                    byte_length:pack.byte_length,allocation:pack.allocation,storage:pack.storage.clone(),memory:pack.memory.clone(),
                    write_checked:std::sync::atomic::AtomicBool::new(pack.write_checked.load(std::sync::atomic::Ordering::Relaxed)),
                })
            };
            let bound=match &retained.metadata.storage {
                Some(binding)=>pack.with_storage(binding.clone())?,None=>pack,
            };
            retained.pack=Arc::downgrade(&bound);
            retained.publish()?;
            self.0.pins.lock().map_err(|_|error("reader pins poisoned"))?.push(bound);
        }
        Ok(())
    }
    pub(crate) fn placement(&self, ordinal: usize) -> Result<(u64, u64), Error> {
        Ok((self.0.selection.file, *self.0.selection.addresses.get(ordinal)
            .ok_or_else(|| error("Pack ordinal outside reader selection"))?))
    }
    pub(crate) fn stage_retention(&self, ordinal: usize, pack: &Arc<SeedPackSnapshot>) -> Result<Retention, Error> {
        let state = &self.0;
        state.started.store(true, std::sync::atomic::Ordering::Relaxed);
        self.placement(ordinal)?;
        let existing = state.metadata[ordinal].lock()
            .map_err(|_| error("Pack metadata cache poisoned"))?.clone();
        if let Some(metadata) = existing {
            if !metadata.definition.same_as(pack.definition()) {
                return Err(error("collected Pack prescription differs from reader scope"));
            }
            return Ok(Retention { state: Arc::downgrade(state), ordinal,
                metadata, pack: Arc::downgrade(pack) });
        }
        let bytes = pack.stored_bytes();
        let envelope = pil_codec::pac_envelope::Plain::read_prefix(bytes)?;
        if envelope.size != bytes.len() as u64
            || envelope.size > state.maximum as u64
            || envelope.size > state.selection.length - state.selection.addresses[ordinal]
        {
            return Err(error("Pack exceeds published read bounds"));
        }
        let length = usize::try_from(envelope.header_length).map_err(|_| Error::Overflow)?;
        let lease = state.budget.reserve(length)?;
        let mut header = Vec::new();
        header
            .try_reserve_exact(length)
            .map_err(|_| error("Pack metadata allocation failed"))?;
        header.extend_from_slice(&bytes[..length]);
        let metadata = self.compile_metadata(ordinal, header, lease, false, Some(pack), None)?;
        if !metadata.definition.same_as(pack.definition()) {
            return Err(error(
                "collected Pack prescription differs from reader scope",
            ));
        }
        Ok(Retention { state: Arc::downgrade(state), ordinal,
            metadata, pack: Arc::downgrade(pack) })
    }
    /// Narrow a time-ordered hour index with binary probes before reading Packs.
    /// The planner supplies [from, to); result ordinals remain Pack ordinals.
    pub fn range(&self, from: u64, to: u64) -> Result<Self, Error> {
        if self.0.index.is_none() || from >= to {
            return Err(error(
                "time range requires an indexed reader and increasing bounds",
            ));
        }
        let range = match &self.1 {
            Some(prior) => from.max(prior.start)..to.min(prior.end),
            None => from..to,
        };
        Ok(Self(self.0.clone(), Some(range), self.2.clone()))
    }
    /// Select disjoint time periods using the existing hour index. Planning
    /// normalizes overlapping periods once; Packs and metadata remain shared.
    pub fn ranges(&self, mut ranges: Vec<std::ops::Range<u64>>) -> Result<Self, Error> {
        if self.0.index.is_none() || ranges.iter().any(|r| r.start >= r.end) {
            return Err(error("time periods require an indexed reader and increasing bounds"));
        }
        ranges.sort_unstable_by_key(|r| r.start);
        let mut selected: Vec<std::ops::Range<u64>> = Vec::with_capacity(ranges.len());
        for range in ranges {
            if let Some(last) = selected.last_mut().filter(|last| range.start <= last.end) {
                last.end = last.end.max(range.end);
            } else { selected.push(range); }
        }
        if let Some(prior) = &self.2 {
            let mut intersection = Vec::new();
            let mut at = 0;
            for range in selected {
                while at < prior.len() && prior[at].end <= range.start { at += 1; }
                for p in &prior[at..] {
                    if p.start >= range.end { break; }
                    intersection.push(range.start.max(p.start)..range.end.min(p.end));
                }
            }
            selected = intersection;
        }
        Ok(Self(self.0.clone(), self.1.clone(), Some(Arc::new(selected))))
    }
    pub fn query(&self, query: Query) -> Result<Cursor, Error> {
        self.0
            .started
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Cursor::from_source(self.clone(), query)
    }
    /// One cursor over the planner's ordered file selections. Limits, sorting,
    /// cancellation and scratch admission remain global to that cursor.
    pub fn query_many(readers: Vec<Self>, query: Query) -> Result<Cursor, Error> {
        if readers.len() == 1 { return readers[0].query(query); }
        for reader in &readers { reader.0.started.store(true, std::sync::atomic::Ordering::Relaxed); }
        Self::query_sources(readers.into_iter().map(|reader|Box::new(reader) as Box<dyn PackSource>).collect(),query)
    }
    /// One global cursor over file selections and immutable resident Packs.
    /// Only ownership handles are retained; payloads and plans are shared.
    pub fn query_sources(readers:Vec<Box<dyn PackSource>>,query:Query)->Result<Cursor,Error> {
        let mut ends=Vec::with_capacity(readers.len());let mut length=0usize;
        for reader in &readers {length=length.checked_add(reader.len()).ok_or(Error::Overflow)?;ends.push(length);}
        Cursor::from_source(Readers{readers,ends,length,current:0},query)
    }

    pub fn pack(&self, ordinal: usize) -> Result<Arc<SeedPackSnapshot>, Error> {
        let state = &self.0;
        state
            .started
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let slot = state
            .cache
            .get(ordinal)
            .ok_or_else(|| error("Pack ordinal outside reader selection"))?;
        let mut slot = slot
            .lock()
            .map_err(|_| error("Pack reader cache poisoned"))?;
        if let Some(pack) = slot.upgrade() {
            return Ok(pack);
        }
        let address = state.selection.addresses[ordinal];
        // The codec reads all controls to obtain the stored length: three
        // widening integers, two flag bytes and the control byte (27 max).
        // Short final files use only their available bytes.
        let available = state.selection.length - address;
        let mut prefix = [0; 27];
        let prefix_length = available.min(prefix.len() as u64) as usize;
        read_exact(
            state.storage.as_ref(),
            state.selection.file,
            address,
            &mut prefix[..prefix_length],
        )?;
        let length = pil_codec::pac_envelope::stored_length(&prefix[..prefix_length])?;
        if length > available || length > state.maximum as u64 {
            return Err(error("Pack exceeds published read bounds"));
        }
        let length = usize::try_from(length).map_err(|_| Error::Overflow)?;
        let mut lease = state.budget.reserve(length)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| error("Pack read allocation failed"))?;
        bytes.resize(length, 0);
        let copied = length.min(prefix_length);
        bytes[..copied].copy_from_slice(&prefix[..copied]);
        read_exact(
            state.storage.as_ref(),
            state.selection.file,
            address.checked_add(copied as u64).ok_or(Error::Overflow)?,
            &mut bytes[copied..],
        )?;
        let compressed = pil_codec::pac_envelope::Control::read(prefix[0])?.compressed;
        // Admission precedes decompression. The codec can geometrically grow
        // its vector and Brotli writes in 64 KiB chunks. Reserve that upper
        // bound, then return unused capacity after opening. Codec workspace,
        // Definition plans and index bindings are outside this payload budget.
        let mut maximum = state.maximum;
        let expansion = if compressed {
            // The configured per-Pack ceiling is not an allocation request.
            // Bound this decode by available admission, too. In particular a
            // tiny compressed Pack must work when the ceiling's worst-case
            // allocation would exceed the whole node budget.
            let available = state.budget.limit().saturating_sub(state.budget.used());
            maximum = maximum.min((available / 2).saturating_sub(65536));
            if maximum == 0 {
                return Err(error("memory budget exhausted"));
            }
            let capacity = maximum
                .checked_add(65536)
                .and_then(|n| n.checked_mul(2))
                .ok_or(Error::Overflow)?;
            Some(state.budget.reserve(capacity)?)
        } else {
            None
        };
        let mut pack = state
            .scope
            .open_pack(Arc::new(bytes), 0..length, maximum)?;
        if !pack.is_sealed() {
            return Err(error("unsealed Packs must use resident storage"));
        }
        if let Some(expanded) = expansion {
            lease = expanded;
        }
        lease.shrink(pack.retained_bytes())?;
        let opened = Arc::get_mut(&mut pack).ok_or_else(|| error("new Pack unexpectedly shared"))?;
        opened.memory = Some(Arc::new(lease));
        // Index and query plans use the logical registration. Member decoding
        // retains the physical prescription and verified strings on the PAC.
        if opened.pack.prescription().bindings().first().is_some_and(|binding|
            matches!(binding.value, pil_codec::api::Parameter::BoxStrings { .. })) {
            let branch = crate::registration::pack_registration(opened.stored_bytes())?;
            opened.definition = state.scope.definition(branch)?.clone();
        }
        if let Some(binding) = state.bind_index(ordinal, address, pack.definition(), pack.len(), pack.pack.version_directory())? {
            pack = pack.with_storage(binding)?;
        }
        *slot = Arc::downgrade(&pack);
        Ok(pack)
    }
}
/// Catalog metadata is per file selection, not per row or per Pack. Sequential
/// scans reuse the current selection; top-K reloads find their source by search.
struct Readers {
    readers: Vec<Box<dyn PackSource>>,
    ends: Vec<usize>,
    length: usize,
    current: usize,
}
impl Readers {
    fn locate(&mut self, ordinal: usize) -> Result<(&mut Box<dyn PackSource>, usize), Error> {
        if ordinal >= self.length { return Err(error("Pack ordinal outside reader selection")); }
        let start = if self.current == 0 { 0 } else { self.ends[self.current - 1] };
        if ordinal < start || ordinal >= self.ends[self.current] {
            self.current = self.ends.partition_point(|end| *end <= ordinal);
        }
        let start = if self.current == 0 { 0 } else { self.ends[self.current - 1] };
        Ok((&mut self.readers[self.current], ordinal - start))
    }
}
impl PackSource for Readers {
    fn len(&self) -> usize { self.length }
    fn definitions(&self) -> Vec<Arc<SeedDefinition>> {
        let mut seen = std::collections::HashSet::new();
        self.readers.iter().flat_map(|r|r.definitions())
            .filter(|definition| seen.insert(Arc::as_ptr(definition))).collect()
    }
    fn load(&mut self, ordinal: usize) -> Result<Arc<SeedPackSnapshot>, Error> {
        let (reader, local) = self.locate(ordinal)?;
        reader.load(local)
    }
    fn read(&mut self, ordinal: usize) -> Result<Arc<crate::ReadPack>, Error> {
        let (reader, local) = self.locate(ordinal)?;
        reader.read(local)
    }
    fn rows(&mut self, ordinal: usize) -> Result<Option<std::ops::Range<usize>>, Error> {
        let (reader, local) = self.locate(ordinal)?;
        reader.rows(local)
    }
    fn filter_rows(&mut self,ordinal:usize,query:&mut crate::SourceFilter<'_>,cancel:&std::sync::atomic::AtomicBool)->Result<(crate::RowSelection,bool),Error>{
        let (reader,local)=self.locate(ordinal)?;
        reader.filter_rows(local,query,cancel)
    }
    fn row_selection(&mut self, ordinal: usize) -> Result<crate::RowSelection, Error> {
        let (reader, local) = self.locate(ordinal)?;
        reader.row_selection(local)
    }
}
impl PackSource for Reader {
    fn len(&self) -> usize {
        self.0.selection.addresses.len()
    }
    fn definitions(&self) -> Vec<Arc<SeedDefinition>> {
        self.0.scope.reader_definitions()
    }
    fn load(&mut self, ordinal: usize) -> Result<Arc<SeedPackSnapshot>, Error> {
        self.pack(ordinal)
    }
    fn read(&mut self, ordinal: usize) -> Result<Arc<crate::ReadPack>, Error> {
        let state = &self.0;
        let slot = state
            .views
            .get(ordinal)
            .ok_or_else(|| error("Pack ordinal outside reader selection"))?;
        let mut slot = slot
            .lock()
            .map_err(|_| error("Pack reader cache poisoned"))?;
        if let Some(pack) = slot.upgrade() {
            return Ok(pack);
        }
        let (metadata, resident) = self.acquire_metadata(ordinal)?;
        let resident = match resident {
            Some(pack) => Some(pack),
            None => state.cache[ordinal]
                .lock()
                .map_err(|_| error("Pack reader cache poisoned"))?
                .upgrade(),
        };
        let pack = if let Some(pack) = resident {
            crate::ReadPack::resident(pack)
        } else if metadata.compressed {
            crate::ReadPack::resident(self.pack(ordinal)?)
        } else {
            Arc::new(crate::ReadPack {
                definition: metadata.definition.clone(),
                lineage: metadata.lineage.clone(),
                storage: metadata.storage.clone(),
                memory: Some(metadata.memory.clone()),
                allocation: Arc::as_ptr(&metadata.plan) as usize,
                pack: crate::query::pack::Data::Paged(Arc::new(paged::PagedPack {
                    metadata: metadata.plan.clone(),
                    storage: state.storage.clone(),
                    budget: state.budget.clone(),
                    file: state.selection.file,
                    address: state.selection.addresses[ordinal],
                    window: Mutex::new(Weak::new()),
                })),
            })
        };
        *slot = Arc::downgrade(&pack);
        Ok(pack)
    }
    fn rows(&mut self, ordinal: usize) -> Result<Option<std::ops::Range<usize>>, Error> {
        self.row_range(ordinal)
    }
    fn filter_rows(&mut self,ordinal:usize,query:&mut crate::SourceFilter<'_>,cancel:&std::sync::atomic::AtomicBool)->Result<(crate::RowSelection,bool),Error>{
        #[cfg(feature = "planner-lab")]
        let (bounds, leaves) = match query.access {
            crate::planner_lab::Access::Scan => (None, false),
            crate::planner_lab::Access::Time => (query.time, false),
            crate::planner_lab::Access::Leaves => (None, true),
            crate::planner_lab::Access::TimeAndLeaves => (query.time, true),
        };
        #[cfg(not(feature = "planner-lab"))]
        let (bounds, leaves) = (query.time, true);
        if let Some(index)=&self.0.index {
            if let Some(bounds) = bounds {
                if query.window.is_some() {
                    // The saved index window already owns the intersected row ranges.
                    return index.prune(self, ordinal, query, cancel, true);
                }
                let range = bounds.range();
                if range.is_empty() {
                    return Ok((crate::RowSelection::Range(0..0), true));
                }
                let slot = index.published_hour(ordinal, self.0.selection.addresses[ordinal])?;
                if range.end <= slot.hour || range.start >= slot.hour.saturating_add(crate::TIME_HOUR_MS) {
                    return Ok((crate::RowSelection::Range(0..0), true));
                }
                let narrowed = self.range(range.start, range.end)?;
                if matches!(bounds, crate::query::TimeBounds::Exact(_)) {
                    return Ok((narrowed.selected_rows(ordinal)?, true));
                }
                if leaves { return index.prune(&narrowed, ordinal, query, cancel, true); }
                return Ok((narrowed.selected_rows(ordinal)?, false));
            }
            if leaves && query.filtered() { return index.prune(self,ordinal,query,cancel,false); }
        }
        Ok((self.selected_rows(ordinal)?,false))
    }
    fn row_selection(&mut self, ordinal: usize) -> Result<crate::RowSelection, Error> {
        self.selected_rows(ordinal)
    }
}

impl Reader {
    fn row_range(&self, ordinal: usize) -> Result<Option<std::ops::Range<usize>>, Error> {
        if self.2.is_some() { return Err(error("disjoint periods require row_selection")); }
        let Some(range) = &self.1 else {
            return Ok(None);
        };
        if range.start >= range.end {
            return Ok(Some(0..0));
        }
        self.0
            .index
            .as_ref()
            .unwrap()
            .range(
                self.0.storage.clone(),
                self.0.budget.clone(),
                ordinal,
                range,
            )
            .map(Some)
    }
    fn selected_rows(&self, ordinal: usize) -> Result<crate::RowSelection, Error> {
        let Some(ranges) = &self.2 else {
            return Ok(match self.row_range(ordinal)? {
                Some(rows) => crate::RowSelection::Range(rows), None => crate::RowSelection::All,
            });
        };
        Ok(crate::RowSelection::Ranges(self.0.index.as_ref().unwrap().ranges(
            self.0.storage.clone(), self.0.budget.clone(), ordinal, ranges, self.1.as_ref(),
        )?))
    }
}
fn read_exact(
    storage: &dyn Storage,
    file: u64,
    mut at: u64,
    mut target: &mut [u8],
) -> Result<(), Error> {
    let mut interrupts = 0;
    while !target.is_empty() {
        match storage.read_at(file, at, target) {
            Ok(0) => return Err(error("incomplete published Pack read")),
            Ok(n) if n <= target.len() => {
                at = at.checked_add(n as u64).ok_or(Error::Overflow)?;
                target = &mut target[n..];
                interrupts = 0;
            }
            Ok(_) => return Err(error("invalid host read count")),
            Err(e) if e.kind() == io::ErrorKind::Interrupted && interrupts < 64 => {
                interrupts += 1;
            }
            Err(e) => return Err(error(e.to_string())),
        }
    }
    Ok(())
}
#[cfg(any(unix, windows))]
impl Storage for crate::writer::NativeFiles {
    fn read_at(&self, file: u64, offset: u64, target: &mut [u8]) -> io::Result<usize> {
        let file = self
            .0
            .get(&file)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown storage handle"))?;
        #[cfg(unix)]
        {
            std::os::unix::fs::FileExt::read_at(file, target, offset)
        }
        #[cfg(windows)]
        {
            std::os::windows::fs::FileExt::seek_read(file, target, offset)
        }
    }
}

impl State {
    fn bind_index(&self, ordinal: usize, address: u64, definition: &Arc<SeedDefinition>, count: usize,
        versions: Option<&pil_codec::pac::versions::Directory>,
    ) -> Result<Option<Arc<dyn crate::StorageBinding>>, Error> {
        let Some(index) = &self.index else { return Ok(None); };
        if let Some(versions) = versions {
            let mut bindings = std::collections::BTreeMap::new();
            for range in versions.ranges() {
                if let std::collections::btree_map::Entry::Vacant(entry) = bindings.entry(range.definition) {
                    let branch = u16::try_from(range.definition).map_err(|_| Error::Overflow)?;
                    let definition = self.scope.definition(branch)?;
                    entry.insert(index.bind(self.storage.clone(), self.budget.clone(), ordinal, address, definition, count)?);
                }
            }
            Ok(Some(Arc::new(crate::storage::VersionBindings { versions: Arc::new(versions.clone()), bindings })))
        } else {
            Ok(Some(index.bind(self.storage.clone(), self.budget.clone(), ordinal, address, definition, count)?))
        }
    }
}
