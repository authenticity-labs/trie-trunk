//! Prepared Seed handlers resolved exclusively through the registered trie.
//! No duplicated registry, Definition translation, or record layout planning.
use crate::{Cursor, Error, Packs as PackStore, Query, SeedDefinition, SeedPackSnapshot, doc};
use std::{
    collections::HashMap,
    ops::{Deref, Range},
    sync::Arc,
};
use trie_trunk_taxonomy::{BranchId, Trunk as Taxonomy};
mod summaries;
mod storage;
mod request;
pub use request::{Request, Registered};
pub use storage::Storage;
pub mod ingress;
pub use ingress::{Ingress, IngressLimits, SealedHour};
pub use summaries::{Summaries, SummaryDay, SummaryIngress};
pub use crate::summaries::SummaryProjection;

pub const DEFINITION_HEADER: &str = "seed.definition";

/// A trunk-local Branch ID binds the complete Pack to one registered lineage
/// member. Pill Packer carries this header without interpreting it.
pub fn pack_registration(bytes: &[u8]) -> Result<BranchId, Error> {
    let raw = pil_codec::pac::application_header(bytes, DEFINITION_HEADER)?
        .ok_or_else(|| Error::Definition("Seed Pack has no registered Definition reference".into()))?;
    let raw: [u8; 2] = raw.try_into()
        .map_err(|_| Error::Definition("Seed Pack Definition reference must be two bytes".into()))?;
    let branch = BranchId::from_le_bytes(raw);
    if branch == 0 { return Err(Error::Definition("Invalid Seed Pack Definition reference".into())); }
    Ok(branch)
}

fn registered_pack(
    owner: Arc<Vec<u8>>, range: Range<usize>, maximum: usize,
    mut definition: impl FnMut(BranchId) -> Result<Arc<SeedDefinition>, Error>,
) -> Result<Arc<SeedPackSnapshot>, Error> {
    let raw = owner.get(range.clone()).ok_or(Error::Incomplete)?;
    if raw.len() > maximum { return Err(Error::Definition("Seed Pack exceeds receive limit".into())); }
    #[cfg(feature = "compression")]
    let (owner, range) = {
        let mut expanded = Vec::new();
        if pil_codec::compression::unseal_with_limit(raw, maximum, &mut expanded)
            .map_err(|e| Error::Definition(e.to_string()))? {
            let length = expanded.len();
            (Arc::new(expanded), 0..length)
        } else { (owner, range) }
    };
    let branch = pack_registration(&owner[range.clone()])?;
    let base = definition(branch)?;
    if pil_codec::pac::flags(&owner[range.clone()])? & pil_codec::pac::MULTI_DEFINITION == 0 {
        return base.open_range(owner, range);
    }
    let mut lineage = HashMap::new();
    let byte_length = range.len();
    let allocation = owner.as_ptr() as usize;
    let pack = pil_codec::api::OwnedPack::shared_registered_range(base.rx.clone(), owner, range, |reference| {
        let branch = BranchId::try_from(reference).map_err(|_| Error::Overflow)?;
        let selected = definition(branch)?;
        if selected.version.0 != base.version.0 {
            return Err(Error::Definition("Seed Pack cannot mix major storage generations".into()));
        }
        lineage.insert(selected.rx.source_identity(), selected.clone());
        Ok(selected.rx.clone())
    })?;
    Ok(Arc::new(SeedPackSnapshot { definition: base, lineage: Some(Arc::new(lineage)), pack: Arc::new(pack),
        byte_length, allocation, storage: None, memory: None,
        write_checked: std::sync::atomic::AtomicBool::new(false) }))
}

pub struct Trunk {
    trie: Taxonomy,
    codecs: HashMap<u32, Arc<SeedDefinition>>,
    descriptions: HashMap<u32, serde_json::Value>,
    commits: Option<crate::writer::CommitScheduler>,
    summary_definitions: std::sync::Mutex<HashMap<(String, String), Arc<std::sync::Mutex<crate::summaries::SummaryDefinitions>>>>,
}
impl Trunk {
    pub fn open(bytes: impl Into<Arc<[u8]>>) -> Result<Self, Error> {
        let trie = Taxonomy::open(bytes).map_err(Error::Definition)?;
        trie.definitions().map_err(Error::Definition)?;
        Ok(Self {
            trie,
            codecs: HashMap::new(),
            descriptions: HashMap::new(),
            commits: None,
            summary_definitions: std::sync::Mutex::new(HashMap::new()),
        })
    }
    pub fn trie(&self) -> Trie<'_> {
        Trie(self)
    }

    /// Source inspection and import preparation. Registration remains explicit.
    pub fn ports(&self) -> crate::ports::Ports {
        crate::ports::Ports
    }

    /// Bind one commit lane to the node, shared across all branch tools. A
    /// read-only Trunk allocates no writer. Environment I/O remains an adapter.
    pub fn writer(mut self, memory: crate::MemoryBudget, limits: crate::writer::QueueLimits) -> Result<Self, Error> {
        if self.commits.is_some() { return Err(Error::Definition("Trunk writer is already configured".into())); }
        self.commits = Some(crate::writer::CommitScheduler::new(memory, limits).map_err(|e| Error::Definition(e.to_string()))?);
        Ok(self)
    }
    pub fn commits(&mut self) -> Result<&mut crate::writer::CommitScheduler, Error> {
        self.commits.as_mut().ok_or_else(|| Error::Definition("Trunk has no writer configured".into()))
    }

    pub fn open_pack(&self, owner: Arc<Vec<u8>>, range: Range<usize>, maximum: usize) -> Result<Arc<SeedPackSnapshot>, Error> {
        registered_pack(owner, range, maximum, |branch| {
            let route = self.trie.route(branch).map_err(Error::Definition)?;
            self.definition(route.definition)
        })
    }

    /// `.rx` bytes were received independently. Resolve by registered identity
    /// and version, never by an assumed default or stale PAC member ordinal.
    /// The caller performs transport integrity checks before publication.
    pub fn prescribe(&mut self, id: u32, bytes: &[u8]) -> Result<Arc<SeedDefinition>, Error> {
        if let Some(codec) = self.codecs.get(&id) {
            if codec.prescription().bytes() == bytes { return Ok(codec.clone()); }
        }
        let document = doc::read(bytes)?;
        let (codec, description) = self.compile(id, &document)?;
        self.descriptions.insert(id, description);
        self.codecs.insert(id, codec.clone());
        Ok(codec)
    }

    /// Prepare every registered member of one independently received lineage
    /// file. DOC reads the bundle once; publication follows successful planning
    /// of all members. Host code supplies file bytes without decoding them.
    pub fn prescribe_file(&mut self, file: &str, bytes: &[u8]) -> Result<(), Error> {
        let registry = self.trie.definitions().map_err(Error::Definition)?;
        let ids = registry.definitions().iter().enumerate()
            .filter(|(_, definition)| definition.file == file)
            .map(|(id, _)| u32::try_from(id).map_err(|_| Error::Overflow))
            .collect::<Result<Vec<_>, _>>()?;
        if ids.is_empty() {
            return Err(Error::Definition("Unregistered Definition file".into()));
        }
        if ids.iter().all(|id| self.codecs.get(id).is_some_and(|codec| codec.prescription().bytes() == bytes)) {
            return Ok(());
        }
        let document = doc::read(bytes)?;
        let mut prepared = Vec::with_capacity(ids.len());
        for id in ids.into_iter().filter(|id| !self.codecs.get(id).is_some_and(|codec| codec.prescription().bytes() == bytes)) {
            let (codec, description) = self.compile(id, &document)?;
            prepared.push((id, codec, description));
        }
        for (id, codec, description) in prepared {
            self.codecs.insert(id, codec);
            self.descriptions.insert(id, description);
        }
        Ok(())
    }

    /// Load the independent summary .rx lineage at startup, before handing
    /// out any summary plans. Event definitions are registered separately.
    pub fn prescribe_summaries(&mut self, branch: BranchId, bytes: &[u8]) -> Result<(), Error> {
        self.restore_summaries(branch, crate::summaries::SummaryDefinitions::read(bytes)?)
    }

    /// Recover the published summary lineage from its resident Box footer.
    /// This does not read historical daily Packs or require a second .rx file.
    pub fn prescribe_summary_box(&mut self, branch: BranchId, reader: &crate::summaries::SummaryBoxReader<'_>) -> Result<(), Error> {
        self.restore_summaries(branch, reader.definitions()?)
    }

    fn restore_summaries(&mut self, branch: BranchId, mut definitions: crate::summaries::SummaryDefinitions) -> Result<(), Error> {
        let route = self.trie.route(branch).map_err(Error::Definition)?;
        let source = self.definition(route.definition)?;
        definitions.prescribe(source.clone(), crate::summaries::SUMMARY_DAY_ROWS)?;
        let mut registry = self.summary_definitions.lock().map_err(|_| crate::error("summary registry poisoned"))?;
        let key = (source.namespace.clone(), source.name.clone());
        if registry.contains_key(&key) {
            return Err(crate::error("restore summary definitions before preparing summary plans"));
        }
        registry.insert(key, Arc::new(std::sync::Mutex::new(definitions)));
        Ok(())
    }

    fn compile(&self, id: u32, document: &doc::Document) -> Result<(Arc<SeedDefinition>, serde_json::Value), Error> {
        let member = self
            .trie
            .definitions()
            .map_err(Error::Definition)?
            .member(id, document)
            .map_err(Error::Definition)?;
        let registry = self.trie.definitions().map_err(Error::Definition)?;
        let file = &registry.definitions()[id as usize].file;
        let summary = registry.resources().iter().flat_map(|r| &r.summaries)
            .any(|reference| registry.definitions()[*reference as usize].file == *file);
        let mut codec = if summary { SeedDefinition::prescribe_summary_member(document, member)? }
            else { SeedDefinition::prescribe_member(document, member)? };
        let history = registry.annotations(id).map_err(Error::Definition)?;
        let revision = history.iter().rposition(|annotations| *annotations == codec.annotations)
            .ok_or_else(|| crate::error("Definition annotations are not registered"))?;
        Arc::get_mut(&mut codec).expect("newly prescribed Definition").registration =
            Some([id, u32::try_from(revision).map_err(|_| Error::Overflow)?]);
        let fields = codec.query_fields().into_iter().map(|name| {
            let ordinal = codec.query_ordinal(&name).expect("prepared query field");
            let f = &codec.prescription().definition().fields()[ordinal];
            let alias = name != f.name;
            let mut field = serde_json::json!({"name": name, "type": f.kind.to_string(),
                "values": f.values, "annotations": document.definitions()[member].fields[ordinal].annotations});
            if alias { field["source"] = serde_json::json!(f.name); }
            if let Some(metadata) = codec.metadata(ordinal) {
                let metadata = metadata.view(&name,&f.name).map(|v|metadata.presentation(v)).unwrap_or_else(|| metadata.clone());
                field["metadata"] = serde_json::to_value(metadata).expect("field metadata serializes");
            }
            field
        }).collect::<Vec<_>>();
        Ok((codec, serde_json::json!({"fields":fields})))
    }
    pub fn describe(&self, id: u32) -> Result<&serde_json::Value, Error> {
        self.descriptions
            .get(&id)
            .ok_or_else(|| Error::Definition("Registered Definition has not been loaded".into()))
    }
    pub fn definition(&self, id: u32) -> Result<Arc<SeedDefinition>, Error> {
        self.codecs
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::Definition("Registered Definition has not been loaded".into()))
    }
    /// Resolve historical application semantics without compiling another
    /// codec. The returned handle can be retained by a query/summary plan.
    pub fn definition_at(&self, id: u32, revision: usize) -> Result<Arc<SeedDefinition>, Error> {
        let annotations = self.trie.definitions().map_err(Error::Definition)?
            .annotations(id).map_err(Error::Definition)?
            .get(revision).ok_or_else(|| crate::error("unknown Definition annotation revision"))?;
        self.definition(id)?.annotated(annotations, [id, u32::try_from(revision).map_err(|_| Error::Overflow)?])
    }
}

/// Borrow the authoritative taxonomy and its prepared codecs together.
/// Traversal still uses Taxonomy; this adds no serialized or client-side model.
pub struct Trie<'a>(&'a Trunk);
/// A pinned registry publication. Readers keep their prepared Definitions while
/// the node publishes a new registry; cloning never copies plans or RX bytes.
#[derive(Clone)]
pub struct Snapshot(Arc<Trunk>);
impl Trunk {
    pub fn snapshot(self: &Arc<Self>) -> Snapshot { Snapshot(self.clone()) }
}
impl Snapshot {
    pub fn branches(&self) -> Branches<'_> { self.0.trie().branches() }
    pub fn registered(&self) -> &Trunk { &self.0 }
}
impl Deref for Snapshot {
    type Target = Taxonomy;
    fn deref(&self) -> &Taxonomy { &self.0.trie }
}
impl Deref for Trie<'_> {
    type Target = Taxonomy;
    fn deref(&self) -> &Taxonomy {
        &self.0.trie
    }
}
impl<'a> Trie<'a> {
    pub fn branches(&self) -> Branches<'a> {
        Branches {
            trunk: self.0,
            selected: None,
        }
    }
}

pub struct Branches<'a> {
    trunk: &'a Trunk,
    selected: Option<Vec<BranchId>>,
}
impl<'a> Branches<'a> {
    /// Prepare the annotated daily summary shape through the same registered
    /// Definition used for this branch's events. No client-side schema model.
    pub fn summaries(self) -> Result<Summaries, Error> {
        let selected = self.selected.as_deref().unwrap_or_default();
        if selected.len() != 1 {
            return Err(Error::Definition("Select one registered branch for its summaries".into()));
        }
        Summaries::new(self.trunk, selected[0])
    }

    /// Select the type's state file and this version's local field ordinals.
    pub fn leaves(self) -> Result<Leaves<'a>, Error> {
        let selected = self.selected.as_deref().unwrap_or_default();
        if selected.len() != 1 { return Err(Error::Definition("Select one registered branch for its leaves".into())); }
        let route = self.trunk.trie.route(selected[0]).map_err(Error::Definition)?;
        let mut versions = HashMap::new();
        for resource in self.trunk.trie.definitions().map_err(Error::Definition)?.resources() {
            let candidate = self.trunk.trie.route(resource.branch).map_err(Error::Definition)?;
            if (candidate.cache.as_str(), candidate.service.as_str(), candidate.resource.as_str(), candidate.resource_type.as_str())
                == (route.cache.as_str(), route.service.as_str(), route.resource.as_str(), route.resource_type.as_str()) && candidate.major == route.major {
                versions.insert(resource.branch, (candidate.definition, candidate.digest));
            }
        }
        Ok(Leaves { trunk: self.trunk, selected: route.definition, versions })
    }

    pub fn time(self, bytes: Arc<Vec<u8>>) -> Result<crate::storage::TimeTrie, Error> {
        self.check_time_branch()?;
        crate::storage::TimeTrie::open(bytes)
    }
    pub fn time_replica(self, bytes: Arc<Vec<u8>>, received: Vec<std::ops::Range<u64>>) -> Result<crate::storage::TimeTrie, Error> {
        self.check_time_branch()?;
        crate::storage::TimeTrie::open_replica(bytes, received)
    }
    fn check_time_branch(&self) -> Result<(), Error> {
        let selected = self.selected.as_deref().unwrap_or_default();
        if selected.len() != 1 {
            return Err(Error::Definition("Select one registered branch for its time trie".into()));
        }
        self.trunk.trie.route(selected[0]).map_err(Error::Definition)?;
        Ok(())
    }
    /// Select registered storage generations. Each selected major includes its
    /// retained minors; logical discovery supplies all majors when unrestricted.
    /// An explicit empty selection stays empty.
    pub fn select(mut self, branches: impl IntoIterator<Item = BranchId>) -> Self {
        self.selected = Some(branches.into_iter().collect());
        self
    }
    pub fn packs(self) -> Result<Packs, Error> {
        let mut definitions = HashMap::new();
        let branches = match self.selected {
            Some(branches) => branches,
            None => self.trunk.trie.definitions().map_err(Error::Definition)?
                .resources().iter().map(|resource| resource.branch).collect(),
        };
        for selected in branches {
            if definitions.contains_key(&selected) { continue; }
            let versions = self.trunk.trie.storage_versions(selected).map_err(Error::Definition)?;
            let family = *versions.iter().min().ok_or_else(|| Error::Definition("Empty registered generation".into()))?;
            for branch in versions {
                let route = self.trunk.trie.route(branch).map_err(Error::Definition)?;
                if route.base==Some(trie_trunk_taxonomy::base::Base::Object){continue;}
                definitions.insert(branch, PrescriptionBinding {
                    family, definition: self.trunk.definition(route.definition)?,
                });
            }
        }
        Ok(Packs {
            definitions: Arc::new(definitions),
            strings: Default::default(),
            inner: PackStore::new(),
        })
    }
    /// Browsable storage uses the same registered codecs and query coordinator.
    /// Storage role does not depend on whether this node is a server or client.
    pub fn beds(self) -> Result<Beds, Error> {
        self.packs()
    }
}

/// State access is scoped by type and major. Minor versions and HTTP splits
/// share roots; distinct majors can remain active independently.
pub struct Leaves<'a> {
    trunk: &'a Trunk,
    selected: u32,
    versions: HashMap<BranchId, (u32, String)>,
}
impl Leaves<'_> {
    fn file<'b>(&self, bytes: &'b [u8], verify_write: bool) -> Result<crate::LeafFile<'b>, Error> {
        let file = crate::LeafFile::open(bytes, verify_write)?;
        for root in &file.roots {
            let digest: String = root.definition.iter().map(|b| format!("{b:02x}")).collect();
            if !self.versions.values().any(|(_, d)| d == &digest) {
                return Err(Error::Definition("Leaf root is not registered to this type".into()));
            }
        }
        Ok(file)
    }
    pub fn directory<'b>(&self, bytes: &'b [u8]) -> Result<crate::LeafFile<'b>, Error> { self.file(bytes, false) }
    pub fn nodes(&self) -> Result<crate::LeafTrie, Error> { Ok(crate::LeafTrie::new(self.trunk.definition(self.selected)?)) }
    pub fn read(&self, bytes: &[u8]) -> Result<Arc<crate::LeafTrie>, Error> {
        Ok(Arc::new(self.file(bytes, false)?.trie(self.trunk.definition(self.selected)?)?))
    }
    /// A registered revision may have no state nodes yet. Its empty trie
    /// resolves leaf zero; a nonzero identity still requires a stored node.
    pub fn read_roots(&self, bytes:&[u8], scope:&Packs)->Result<Vec<Arc<crate::LeafTrie>>,Error>{
        let file=self.file(bytes,false)?;
        let mut roots:Vec<Arc<crate::LeafTrie>>=Vec::new();
        for binding in scope.definitions.values() {
            let definition=&binding.definition;
            if roots.iter().any(|r|r.matches_definition(definition)){continue}
            let trie=if file.roots.iter().any(|r|r.definition==definition.digest){
                file.trie(definition.clone())?
            }else{crate::LeafTrie::new(definition.clone())};
            roots.push(Arc::new(trie));
        }
        Ok(roots)
    }
    pub fn labels(&self, bytes: &[u8]) -> Result<std::collections::BTreeMap<u16, String>, Error> {
        Ok(self.file(bytes, false)?.root(&self.trunk.definition(self.selected)?.digest)?.labels.clone())
    }
    pub fn label(&self, bytes: &[u8], leaf: u16, label: Option<&str>) -> Result<Vec<u8>, Error> {
        self.file(bytes, false)?.label(&self.trunk.definition(self.selected)?.digest, leaf, label)
    }
    pub fn definitions(&self, bytes: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        Ok(self.file(bytes, false)?.roots.into_iter().map(|root| root.definition).collect())
    }
    /// Integrity verification for received bytes before their storage write.
    pub fn inspect(&self, bytes: &[u8]) -> Result<crate::LeafFileLayout, Error> {
        let file = self.file(bytes, true)?;
        let definition = self.trunk.definition(self.selected)?;
        file.trie(definition.clone())?;
        file.layout(&definition.digest)
    }
    pub fn create(&self) -> Result<LeafWriter, Error> {
        Ok(LeafWriter { definitions: self.prepared()?, inner: crate::LeafFileBuilder::new() })
    }
    fn prepared(&self) -> Result<HashMap<BranchId, Arc<SeedDefinition>>, Error> {
        self.versions.iter().map(|(&branch, (id, _))| Ok((branch, self.trunk.definition(*id)?))).collect()
    }
    pub fn resume(&self, bytes: &[u8]) -> Result<LeafWriter, Error> {
        self.file(bytes, false)?;
        let definitions = self.prepared()?;
        let inner = crate::LeafFileBuilder::resume(bytes, |digest| resolve_leaf(&definitions, digest))?;
        Ok(LeafWriter { definitions, inner })
    }
    /// Rebuild a lost footer explicitly. Statistics from a lost footer must be
    /// recomputed by their owner; intact root and node bytes are preserved.
    pub fn recover(&self, bytes: &[u8]) -> Result<LeafWriter, Error> {
        let definitions = self.prepared()?;
        let inner = crate::LeafFileBuilder::recover(bytes, |digest| resolve_leaf(&definitions, digest))?;
        Ok(LeafWriter { definitions, inner })
    }
}
fn resolve_leaf(definitions: &HashMap<BranchId, Arc<SeedDefinition>>, digest: &[u8]) -> Result<Arc<SeedDefinition>, Error> {
    definitions.values().find(|d| d.digest == digest).cloned()
        .ok_or_else(|| Error::Definition("Leaf root is not registered to this type".into()))
}
/// Returned only by the trunk's leaves scope. Disk I/O and publication remain
/// owned by the existing storage host, not the codec or Box.
pub struct LeafWriter {
    definitions: HashMap<BranchId, Arc<SeedDefinition>>,
    inner: crate::LeafFileBuilder,
}
impl LeafWriter {
    fn definition(&self, branch: BranchId) -> Result<Arc<SeedDefinition>, Error> {
        self.definitions.get(&branch).cloned().ok_or_else(|| Error::Definition("Branch does not belong to this leaf type".into()))
    }
    pub fn register(&mut self, branch: BranchId) -> Result<(), Error> { self.inner.register(self.definition(branch)?) }
    pub fn freeze(&mut self, branch: BranchId) -> Result<(), Error> { self.inner.freeze(self.definition(branch)?.as_ref()) }
    pub fn append(&mut self, trie: &crate::LeafTrie) -> Result<(), Error> {
        resolve_leaf(&self.definitions, trie.definition_digest())?;
        self.inner.append(trie)
    }
    pub fn statistics(&mut self, branch: BranchId, value: ciborium::Value) -> Result<(), Error> {
        self.inner.statistics(self.definition(branch)?.as_ref(), value)
    }
    pub fn nodes(&self, branch: BranchId) -> Result<crate::LeafTrie, Error> {
        Ok(self.inner.nodes(self.definition(branch)?))
    }
    pub fn prepare(&self, branch: BranchId, trie: crate::LeafTrie) -> Result<crate::LeafCommit, Error> {
        if !trie.matches_definition(&self.definition(branch)?) {
            return Err(Error::Definition("Leaf commit does not match its registered Definition".into()));
        }
        self.inner.prepare(trie)
    }
    pub fn accept(&mut self, commit: crate::LeafCommit) -> Result<(), Error> { self.inner.accept(commit) }
    pub fn publish(&self) -> Result<Vec<u8>, Error> { self.inner.publish() }
}

/// Registered event query scope. Keeps prepared codecs and the existing Packs
/// coordinator alive independently of the registry snapshot that selected them.
struct PrescriptionBinding {
    family: BranchId,
    definition: Arc<SeedDefinition>,
}
pub struct Packs {
    definitions: Arc<HashMap<BranchId, PrescriptionBinding>>,
    strings: Arc<std::collections::BTreeMap<usize,Arc<str>>>,
    inner: PackStore,
}
pub type Beds = Packs;
impl Packs {
    /// Trunk supplies registration and Time selection; Seed owns reconstruction.
    pub fn reconstruct(&self, length: u64, count: u64) -> Result<crate::replication::Replica, Error> {
        PackStore::reconstruct(length, count)
    }
    /// Create the Box used by this event datastore. Time-trie addresses remain
    /// authoritative; no second directory or private file prefix is emitted.
    pub fn create(&self, headers: &[u8]) -> Result<crate::packs::Writer<'static>, Error> {
        crate::packs::Writer::new(headers)
    }

    /// Resume from the published footer only. Commits return a replacement
    /// tail for the host's existing write/truncate/sync sequence; historical
    /// Packs remain on disk and are neither read nor copied here.
    pub fn resume(&self, footer: Vec<u8>, length: u64) -> Result<crate::packs::Writer<'static>, Error> {
        crate::packs::Writer::resume(footer, length)
    }

    /// Bind dictionary-aware readers to a pinned origin footer.
    pub fn box_footer(&mut self, bytes:Vec<u8>, length:u64) -> Result<(),Error> {
        let owner = pil_codec::r#box::Box::open_footer_owned(bytes,length)?;
        if owner.has_common_strings() { self.bind_strings(|rx|owner.prescription(rx)); }
        Ok(())
    }
    fn bind_strings(&mut self, bind:impl Fn(&pil_codec::api::Prescription)->pil_codec::api::Prescription) {
        self.definitions = Arc::new(self.definitions.iter().map(|(&id,b)| {
            let definition = b.definition.with_prescription(Arc::new(bind(&b.definition.rx)));
            (id, PrescriptionBinding {family:b.family,definition})
        }).collect());
    }
    pub fn strings(&mut self, entries:Vec<(usize,String)>) -> Result<(),Error> {
        let mut additions = std::collections::BTreeMap::new();
        for (id,text) in entries {
            if let Some(old) = self.strings.get(&id).or_else(||additions.get(&id)) {
                if old.as_ref() != text { return Err(crate::error("conflicting BOX string ordinal")); }
            } else { additions.insert(id,Arc::<str>::from(text)); }
        }
        if !additions.is_empty() {
            Arc::make_mut(&mut self.strings).append(&mut additions);
            let entries = self.strings.clone();
            self.bind_strings(|rx| rx.with_box_entries(entries.clone()));
        }
        Ok(())
    }
    pub fn missing_strings(&self, plain:&[u8]) -> Result<Vec<usize>,Error> {
        let mut ids = pil_codec::api::required_strings(plain)?;
        ids.retain(|i|!self.strings.contains_key(i));
        Ok(ids)
    }
    pub fn reader_scope(&self) -> Self {
        Self { definitions: self.definitions.clone(), strings:self.strings.clone(), inner: PackStore::new() }
    }
    pub fn fill(&self, branch: BranchId, count: usize) -> Result<crate::SeedPack<'_>, Error> {
        let mut pack = self.definition(branch)?.fill(count)?;
        pack.header(DEFINITION_HEADER, branch.to_le_bytes().to_vec())?;
        pack.register(branch as u64, self.definition(branch)?.prescription())?;
        let family = self.definitions[&branch].family;
        for (&other, binding) in self.definitions.iter() {
            if other != branch && binding.family == family {
                pack.register(other as u64, binding.definition.prescription())?;
            }
        }
        Ok(pack)
    }

    pub fn open_pack(&self, owner: Arc<Vec<u8>>, range: Range<usize>, maximum: usize) -> Result<Arc<SeedPackSnapshot>, Error> {
        registered_pack(owner, range, maximum, |branch| self.definition(branch).cloned())
    }

    pub fn load_registered(&mut self, owner: Arc<Vec<u8>>, range: Range<usize>, maximum: usize) -> Result<&mut Self, Error> {
        let pack = self.open_pack(owner, range, maximum)?;
        self.inner.pack(pack);
        Ok(self)
    }
    pub fn definition(&self, branch: BranchId) -> Result<&Arc<SeedDefinition>, Error> {
        self.definitions
            .get(&branch)
            .map(|entry| &entry.definition)
            .ok_or_else(|| Error::Definition("Branch is outside the selected PACKS scope".into()))
    }
    /// Existing storage supplies the owning buffer and exact Pack range.
    /// Uncompressed child bytes remain borrowed; decompression retains its
    /// existing bounded allocation. No I/O or replacement directory is added.
    pub fn load(
        &mut self,
        branch: BranchId,
        owner: Arc<Vec<u8>>,
        range: Range<usize>,
        maximum: usize,
    ) -> Result<&mut Self, Error> {
        self.definition(branch)?;
        let pack = self.open_pack(owner, range, maximum)?;
        if pack_registration(pack.stored_bytes())? != branch {
            return Err(Error::Definition("Pack Definition reference differs from the selected branch".into()));
        }
        self.inner.pack(pack);
        Ok(self)
    }
    /// Accept an already opened/index-bound Pack without copying or reopening.
    pub fn pack(
        &mut self,
        branch: BranchId,
        pack: Arc<SeedPackSnapshot>,
    ) -> Result<&mut Self, Error> {
        if !Arc::ptr_eq(self.definition(branch)?, pack.definition()) {
            return Err(Error::Definition(
                "Pack must use the branch's registered prescription".into(),
            ));
        }
        if pack_registration(pack.stored_bytes())? != branch {
            return Err(Error::Definition("Pack Definition reference differs from the selected branch".into()));
        }
        self.inner.pack(pack);
        Ok(self)
    }
    /// Select stored children using addresses supplied by the time trie. The
    /// existing Box reader owns framing; a failed selection publishes no Packs.
    pub fn load_box(
        &mut self,
        branch: BranchId,
        owner: Arc<Vec<u8>>,
        addresses: impl IntoIterator<Item = u64>,
    ) -> Result<&mut Self, Error> {
        self.definition(branch)?;
        let family = self.definitions[&branch].family;
        let container = pil_codec::r#box::Box::open(owner.as_slice())?;
        let mut scope = self.reader_scope();
        if container.has_common_strings() {
            scope.bind_strings(|rx| container.prescription(rx));
        }
        let mut selected = PackStore::new();
        for address in addresses {
            let bytes = container.get_at(address)?;
            let start = usize::try_from(address).map_err(|_| Error::Overflow)?;
            let end = start.checked_add(bytes.len()).ok_or(Error::Overflow)?;
            let pack = scope.open_pack(owner.clone(), start..end, usize::MAX)?;
            let reference = pack_registration(pack.stored_bytes())?;
            if self.definitions[&reference].family != family {
                return Err(Error::Definition("Box child belongs to a different resource branch".into()));
            }
            selected.pack(pack);
        }
        self.inner.append(selected);
        Ok(self)
    }
    pub(crate) fn reader_definitions(&self) -> Vec<Arc<SeedDefinition>> {
        self.definitions.values().map(|p| p.definition.clone()).collect()
    }
    pub fn query(&self, query: Query) -> Result<Cursor, Error> {
        self.inner.query(query)
    }
}

pub mod query_events;
