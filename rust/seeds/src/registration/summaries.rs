//! Registered summary accumulation. Version changes select source plans, not
//! new period rows. The caller's commit watermark controls completion.
use super::{BranchId, Trunk};
use crate::{Document, Error, Seed, SeedDefinition, error, summaries::*};
use std::{
    collections::HashMap,
    ops::Deref,
    sync::{Arc, Mutex, OnceLock},
};

/// Immutable source bindings plus the shared summary library. Old handles stay
/// usable while an updated annotation snapshot is registered on the node.
#[derive(Clone)]
pub struct Summaries {
    branch: BranchId,
    versions: HashMap<BranchId, Arc<SeedDefinition>>,
    definitions: Arc<Mutex<SummaryDefinitions>>,
    plan: SummaryPlan,
}
impl Summaries {
    pub(super) fn new(trunk: &Trunk, branch: BranchId) -> Result<Self, Error> {
        let route = trunk.trie.route(branch).map_err(Error::Definition)?;
        let source = trunk.definition(route.definition)?;
        let definitions = trunk
            .summary_definitions
            .lock()
            .map_err(|_| error("summary registry poisoned"))?
            .entry((source.namespace.clone(), source.name.clone()))
            .or_insert_with(|| Arc::new(Mutex::new(SummaryDefinitions::new())))
            .clone();
        let plan = definitions
            .lock()
            .map_err(|_| error("summary lineage poisoned"))?
            .prescribe(source, SUMMARY_DAY_ROWS)?;
        let mut versions = HashMap::new();
        for branch in trunk
            .trie
            .storage_versions(branch)
            .map_err(Error::Definition)?
        {
            let route = trunk.trie.route(branch).map_err(Error::Definition)?;
            if let Ok(source) = trunk.definition(route.definition) {
                versions.insert(branch, source);
            }
        }
        Ok(Self {
            branch,
            versions,
            definitions,
            plan,
        })
    }
    pub fn restore(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let mut definitions = SummaryDefinitions::read(bytes)?;
        self.plan = definitions.prescribe(self.versions[&self.branch].clone(), SUMMARY_DAY_ROWS)?;
        self.definitions = Arc::new(Mutex::new(definitions));
        Ok(())
    }
    pub fn plan(&self) -> &SummaryPlan {
        &self.plan
    }
    /// Persist the shared DOC-authored class library alongside registration.
    /// Daily PAC readers use the event RX supplied by their taxonomy scope.
    pub fn definitions(&self) -> Result<Document, Error> {
        self.definitions
            .lock()
            .map_err(|_| error("summary lineage poisoned"))?
            .document()?
            .ok_or_else(|| error("summary lineage is empty"))
    }
    fn prepare(&self, branch: BranchId) -> Result<SummaryPlan, Error> {
        if branch == self.branch {
            return Ok(self.plan.clone());
        }
        let source = self
            .versions
            .get(&branch)
            .ok_or_else(|| error("summary branch is outside the loaded lineage"))?;
        self.definitions
            .lock()
            .map_err(|_| error("summary lineage poisoned"))?
            .prescribe(source.clone(), SUMMARY_DAY_ROWS)
    }
    pub fn accumulator_day(
        &self,
        time: u64,
        calendar: SummaryCalendar,
    ) -> Result<SummaryDay, Error> {
        let range = calendar.day(time)?;
        Ok(SummaryDay {
            scope: self.clone(),
            first: range.start,
            end: range.end,
            through: range.start,
            sources: Vec::new(),
            by_definition: HashMap::new(),
            current: None,
            sealed: false,
            snapshot: SnapshotCache::default(),
        })
    }
}
impl Deref for Summaries {
    type Target = SummaryPlan;
    fn deref(&self) -> &SummaryPlan {
        &self.plan
    }
}

/// At most one accumulator per active source binding and summary revision.
/// No event builders or payloads are retained. Refresh happens at registration,
/// while uninterrupted ingress keeps the existing single-source fast path.
#[derive(Clone)]
pub struct SummaryDay {
    scope: Summaries,
    first: u64,
    end: u64,
    through: u64,
    sources: Vec<SummaryAccumulator>,
    by_definition: HashMap<[u32; 2], usize>,
    current: Option<(BranchId, usize)>,
    sealed: bool,
    snapshot: SnapshotCache,
}

// One optional immutable snapshot per day, with a shared process/WASM budget.
// Cloned accumulators share bytes, but independently invalidate on mutation.
struct Snapshot {
    input: Vec<u8>,
    next: Arc<[u8]>,
    file: String,
    revisions: usize,
    document: Document,
    bytes: Vec<u8>,
    _memory: crate::memory::MemoryLease,
}
#[derive(Default)]
struct SnapshotCache(Mutex<Option<Arc<Snapshot>>>);
impl Clone for SnapshotCache {
    fn clone(&self) -> Self { Self(Mutex::new(self.0.lock().ok().and_then(|v| v.clone()))) }
}
impl SnapshotCache {
    fn clear(&mut self) { *self.0.get_mut().unwrap_or_else(|e| e.into_inner()) = None; }
}
fn snapshot_budget() -> &'static crate::MemoryBudget {
    static BUDGET: OnceLock<crate::MemoryBudget> = OnceLock::new();
    BUDGET.get_or_init(|| crate::MemoryBudget::new(32 * 1024 * 1024))
}

impl SummaryDay {
    pub fn first(&self) -> u64 {
        self.first
    }
    pub fn end(&self) -> u64 {
        self.end
    }
    pub fn complete_through(&self) -> u64 {
        self.through
    }
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }
    pub fn active_definitions(&self) -> usize {
        self.sources.len()
    }

    /// Apply a newly registered annotation/source snapshot without discarding
    /// prior period contributions. Rollback can reuse an earlier snapshot.
    pub fn refresh(&mut self, summaries: &Summaries) -> Result<&mut Self, Error> {
        if self.sealed || !Arc::ptr_eq(&self.scope.definitions, &summaries.definitions) {
            return Err(error("summary refresh requires the same open lineage"));
        }
        self.snapshot.clear();
        self.scope = summaries.clone();
        self.current = None;
        Ok(self)
    }
    fn writable(&self, time: u64) -> Result<(), Error> {
        if self.sealed {
            return Err(error("summary Pack is sealed"));
        }
        if time < self.through || time >= self.end {
            return Err(error("event outside open summary periods"));
        }
        Ok(())
    }
    /// Ingest using the registered source's typed setters, returning the Seed
    /// to the existing event Pack path. Rollback reuses its prior accumulator.
    pub fn ingest<'v, T>(
        &mut self,
        branch: BranchId,
        time: u64,
        event: T,
        transform: impl FnOnce(T, &mut Seed<'v>) -> Result<(), Error>,
    ) -> Result<Seed<'v>, Error> {
        self.with_source(branch, time, |source| source.ingest(time, event, transform))
    }

    /// Observe full PIL ingress through its registered decoder. The accumulator
    /// checks prescription identity and reads only annotated fields; it neither
    /// rebuilds the event nor retains its input bytes.
    pub fn observe(&mut self, branch: BranchId, time: u64, row: &pil_codec::api::PillView<'_>) -> Result<(), Error> {
        self.with_source(branch, time, |source| source.observe(time, row))
    }

    fn with_source<T>(
        &mut self,
        branch: BranchId,
        time: u64,
        apply: impl FnOnce(&mut SummaryAccumulator) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.writable(time)?;
        self.snapshot.clear();
        if let Some((current, index)) = self.current {
            if current == branch {
                return apply(&mut self.sources[index]);
            }
        }
        let binding = self
            .scope
            .versions
            .get(&branch)
            .ok_or_else(|| error("summary branch is outside the loaded lineage"))?;
        // Republishing the same registered context may create a fresh Arc.
        // Its observations still belong to the existing accumulator.
        let definition = binding.registration.ok_or_else(|| error("summary source is not registered"))?;
        if let Some(&index) = self.by_definition.get(&definition) {
            self.current = Some((branch, index));
            return apply(&mut self.sources[index]);
        }
        let plan = self.scope.prepare(branch)?;
        let mut source = plan.accumulator(self.first)?;
        source.complete(self.through)?;
        let result = apply(&mut source)?;
        self.by_definition.insert(definition, self.sources.len());
        self.current = Some((branch, self.sources.len()));
        self.sources.push(source);
        Ok(result)
    }
    /// Advance only after the corresponding event commit closes its straggler
    /// window. Check once before changing any participating accumulator.
    pub fn complete(&mut self, through: u64) -> Result<&mut Self, Error> {
        if self.sealed
            || through < self.through
            || through > self.end
            || through % SUMMARY_PERIOD_MS != 0
        {
            return Err(error("invalid summary completion watermark"));
        }
        if through != self.through { self.snapshot.clear(); }
        for source in &mut self.sources {
            source.complete(through)?;
        }
        self.through = through;
        Ok(self)
    }
    /// Reuse only a snapshot whose registry context has already been published.
    /// Mutations clear this cache; a different registry or summary lineage must
    /// take the registration path before these bytes can be served.
    pub fn published_snapshot(&self, registry: &[u8], file: &str) -> Result<Option<Vec<u8>>, Error> {
        if self.sealed { return Ok(None); }
        let definitions = self.scope.definitions.lock().map_err(|_| error("summary lineage poisoned"))?;
        let cached = self.snapshot.0.lock().map_err(|_| error("summary snapshot poisoned"))?;
        Ok(cached.as_ref().filter(|s| s.file == file && s.revisions == definitions.len()
            && s.next.as_ref() == registry).map(|s| s.bytes.clone()))
    }
    /// Resident query snapshot. It retains all 288 positions but is unsealed,
    /// so it cannot be persisted as a Box child or a complete-day receipt.
    pub fn snapshot_registered(&self, trie: &trie_trunk_taxonomy::Trunk, file: &str)
        -> Result<(trie_trunk_taxonomy::Trunk, Document, Vec<u8>), Error>
    {
        if self.sealed { return Err(error("summary day is sealed")); }
        let mut definitions = self.scope.definitions.lock().map_err(|_| error("summary lineage poisoned"))?;
        let mut cached = self.snapshot.0.lock().map_err(|_| error("summary snapshot poisoned"))?;
        if let Some(snapshot) = cached.as_ref().filter(|s| s.file == file && s.revisions == definitions.len()
            && (s.input == trie.bytes() || s.next.as_ref() == trie.bytes())) {
            return Ok((trie_trunk_taxonomy::Trunk::open(snapshot.next.clone()).map_err(Error::Definition)?,
                snapshot.document.clone(), snapshot.bytes.clone()));
        }
        *cached = None;
        let plans = self.sources.iter().map(|s| s.plan().clone()).collect::<Vec<_>>();
        let projection = match plans.len() {
            0 => SummaryProjection::Single(self.scope.plan.clone()),
            1 => SummaryProjection::Single(plans[0].clone()),
            _ => SummaryProjection::Mixed(MixedSummaryPlan::with_definitions(&plans, &mut definitions)?),
        };
        let document = definitions.document()?.ok_or_else(|| error("missing summary lineage"))?;
        let next = trie.register_summaries(self.scope.branch, file, &document).map_err(Error::Definition)?
            .register_summary_context(self.scope.branch, self.context(false)?).map_err(Error::Definition)?;
        let mut pack = projection.layout().fill()?;
        for ordinal in 0..projection.layout().rows() {
            match &projection {
                SummaryProjection::Single(plan) => {
                    let empty = plan.bucket();
                    let bucket = if let Some(source) = self.sources.first() { source.bucket(ordinal)? } else { &empty };
                    pack.load(plan.seed(bucket)?)?;
                }
                SummaryProjection::Mixed(plan) => {
                    let mut bucket = plan.bucket();
                    for source in &self.sources { plan.merge(&mut bucket, source.plan(), source.bucket(ordinal)?)?; }
                    plan.load(&mut pack, &bucket)?;
                }
                SummaryProjection::Stored { .. } => unreachable!(),
            }
        }
        let bytes = pack.snapshot()?;
        // Retention is optional. Budget exhaustion must never reject a query.
        let cost = bytes.len().saturating_add(trie.bytes().len()).saturating_add(next.bytes().len())
            .saturating_add(document.bytes()?.len().saturating_mul(4)).saturating_add(4096);
        if let Ok(memory) = snapshot_budget().reserve(cost) {
            *cached = Some(Arc::new(Snapshot { input: trie.bytes().to_vec(), next: next.bytes().into(),
                file: file.into(), revisions: definitions.len(), document: document.clone(), bytes: bytes.clone(), _memory: memory }));
        }
        Ok((next, document, bytes))
    }
    /// One row per period even across deployments and rollbacks. A single
    /// participating version (or a silent day) uses the ordinary Full path.
    pub fn seal(&mut self) -> Result<(SummaryProjection, SummaryPack), Error> {
        self.seal_using(|_, _| Ok(()))
    }
    /// Prepare the separate .rx resource and the next registry snapshot before
    /// publishing the sealed PAC. Existing schemas retain their registry IDs.
    pub fn seal_registered(&mut self, trie: &trie_trunk_taxonomy::Trunk, file: &str)
        -> Result<(trie_trunk_taxonomy::Trunk, Document, SummaryProjection, SummaryPack), Error>
    {
        let branch = self.scope.branch;
        let context = self.context(true)?;
        let mut publication = None;
        let (projection, pack) = self.seal_using(|definitions, _pack| {
            let document = definitions.document()?.ok_or_else(|| error("missing summary lineage"))?;
            let next = trie.register_summaries(branch, file, &document).map_err(Error::Definition)?
                .register_summary_context(branch, context).map_err(Error::Definition)?;
            publication = Some((next, document));
            Ok(())
        })?;
        let (next, document) = publication.ok_or_else(|| error("missing summary registration"))?;
        Ok((next, document, projection, pack))
    }
    fn context(&self, sealed: bool) -> Result<trie_trunk_taxonomy::registration::SummaryContext, Error> {
        let sources = if self.sources.is_empty() { vec![self.scope.plan.source_context()?] }
            else { self.sources.iter().map(|source| source.plan().source_context()).collect::<Result<_, _>>()? };
        Ok(trie_trunk_taxonomy::registration::SummaryContext { first: self.first, end: self.end, sources, sealed })
    }
    fn seal_using(&mut self, prepare: impl FnOnce(&SummaryDefinitions, &mut SummaryPackBuilder<'_>) -> Result<(), Error>)
        -> Result<(SummaryProjection, SummaryPack), Error>
    {
        if self.sealed || self.through != self.end {
            return Err(error("summary day is sealed or incomplete"));
        }
        let result = match self.sources.len() {
            0 => {
                let mut source = self.scope.plan.accumulator(self.first)?;
                source.complete(self.end)?;
                (
                    SummaryProjection::Single(self.scope.plan.clone()),
                    source.seal_using(|pack| prepare(&*self.scope.definitions.lock().map_err(|_| error("summary lineage poisoned"))?, pack))?,
                )
            }
            1 => {
                let source = &mut self.sources[0];
                let plan = source.plan().clone();
                (SummaryProjection::Single(plan), source.seal_using(|pack| prepare(&*self.scope.definitions.lock().map_err(|_| error("summary lineage poisoned"))?, pack))?)
            }
            _ => {
                let mut definitions = self
                    .scope
                    .definitions
                    .lock()
                    .map_err(|_| error("summary lineage poisoned"))?;
                let (plan, pack) = SummaryAccumulator::seal_mixed_using(
                    &mut self.sources,
                    &mut definitions,
                    prepare,
                )?;
                (SummaryProjection::Mixed(plan), pack)
            }
        };
        self.snapshot.clear();
        self.sealed = true;
        self.sources = Vec::new();
        self.by_definition = HashMap::new();
        self.current = None;
        Ok(result)
    }
}

/// Summary ownership for one registered ingestion lane. Recovery feeds sealed
/// event Packs followed by the existing PIL spool; no second event log exists.
pub struct SummaryIngress {
    pub(super) scope: Summaries,
    calendar: SummaryCalendar,
    days: std::collections::BTreeMap<u64, SummaryDay>,
}
impl SummaryIngress {
    pub fn new(scope: Summaries, calendar: SummaryCalendar) -> Self {
        Self { scope, calendar, days: Default::default() }
    }
    pub fn observe(&mut self, branch: BranchId, time: u64, row: &pil_codec::api::PillView<'_>) -> Result<(), Error> {
        let first = self.calendar.day(time)?.start;
        if !self.days.contains_key(&first) {
            self.days.insert(first, self.scope.accumulator_day(time, self.calendar)?);
        }
        self.days.get_mut(&first).unwrap().observe(branch, time, row)
    }
    pub fn day(&mut self, first: u64) -> Result<&mut SummaryDay, Error> {
        if self.calendar.day(first)?.start != first { return Err(error("summary day is not calendar aligned")); }
        if !self.days.contains_key(&first) {
            self.days.insert(first, self.scope.accumulator_day(first, self.calendar)?);
        }
        Ok(self.days.get_mut(&first).unwrap())
    }
    pub fn published(&mut self, first: u64) { self.days.remove(&first); }
}

impl SummaryIngress {
    pub fn restore_pack(&mut self, scope: &super::Packs, bytes: Vec<u8>) -> Result<(), Error> {
        let length = bytes.len();
        let pack = scope.open_pack(Arc::new(bytes), 0..length, 256 << 20)?;
        self.restore_snapshot(scope,&pack)
    }
    pub fn restore_snapshot(&mut self, scope:&super::Packs, pack:&crate::SeedPackSnapshot)->Result<(),Error>{
        let storage=pack.storage.as_ref().map(crate::storage::BindingReader::new);
        let mut reader = pack.pack.reader();
        let mut bindings = HashMap::new();
        for ordinal in 0..pack.len() {
            let definition = pack.definition_at(ordinal)?;
            let key = Arc::as_ptr(definition) as usize;
            let (branch, fields) = if let Some(binding) = bindings.get(&key) { binding } else {
                let branch = scope.definitions.iter().find_map(|(&id, b)| b.definition.same_as(definition).then_some(id))
                    .ok_or_else(|| error("summary recovery Definition is not registered"))?;
                bindings.entry(key).or_insert((branch, crate::IndexFields::from_definition(definition)?))
            };
            let row = reader.get(ordinal)?;
            let row=crate::storage::BoundRow{row,ordinal,storage:storage.as_ref()};
            let time=fields.bound_time(&row)?;
            self.day(self.calendar.day(time)?.start)?.with_source(*branch,time,|s|s.observe_bound(time,&row))?;
        }
        Ok(())
    }
}
