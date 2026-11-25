//! Seed semantics and query execution over the Rust Pill Packer codec.
//! No PIL/PAC framing, masks, offsets, or Definition parsing are implemented here.
pub mod integrity;
#[cfg(any(feature = "registration", feature = "query-language"))]
pub mod metadata;
#[cfg(feature = "query-language")]
pub mod language;
#[cfg(any(feature = "registration", feature = "query-language"))]
pub mod relational;
#[cfg(feature = "planner-lab")]
pub mod planner_lab;
mod query;
mod memory;
pub use memory::{MemoryBudget, MemoryLease};
pub mod replication;
pub mod seed;
pub mod pack;
pub mod packs;
/// Environment adapter contract, not a separate datastore API.
#[doc(hidden)]
pub mod writer;
/// Environment read adapter; public datastore access remains under Packs.
#[cfg(feature = "registration")]
#[doc(hidden)]
pub mod reader;
#[cfg(feature = "registration")]
pub mod registration;
#[cfg(feature = "registration")]
pub mod ports;
/// Transport decoding shared by native and WASM adapters.
#[cfg(any(feature = "registration", feature = "query-language"))]
#[doc(hidden)]
pub mod request;
#[cfg(feature = "registration")]
pub use registration as trunk;
mod storage;
mod store;
pub mod summaries;
pub mod stalks;
pub mod comparison;
pub mod cast;
mod value;
#[cfg(feature = "compression")]
pub use pil_codec::api::Compression;
pub use pil_codec::api::{FieldKey, PackOptions};
use pil_codec::api::{OwnedPack, Pack, Pill, Prescription};
pub use pil_codec::doc::{self, Document, Field};
pub use pil_codec::{EncodingMode, Error};
pub use query::{
    Batch, BatchReader, Compare, Cursor, Direction, OwnedBatchReader, PackScan, Predicate, Query,
    SeedView, QuerySlice, PackSource, ReadPack, RowSelection, SourceFilter,
};
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};
pub use storage::{TimeHeader, TimeLeaf, TimeRange, TimeTrie, TIME_HEADER_BYTES, TIME_HOUR_MS, LeafCommit, LeafFile, LeafRoot, LeafFileLayout, LeafFileBlock, HourIndex, HourIndexBuilder, IndexFields, LeafIndex, LeafTrie, StorageBinding, StorageCursor};
pub use store::{Packs, Beds};
pub use value::{Cell, Object};

/// A Seed uses the same typed setters and assembly paths as a Pill.
pub type Seed<'a> = Pill<'a>;

/// Application annotations remain attached to their authored field ordinals.
/// Register once; share the executable prescription across Packs and readers.
pub struct SeedDefinition {
    plans: query::Plans,
    summary_plan: std::sync::OnceLock<Result<summaries::Prepared, String>>,
    #[cfg(any(feature = "registration", feature = "query-language"))]
    semantics: Vec<(usize, &'static metadata::Profile)>,
    change_key: Option<String>,
    rx: Arc<Prescription>,
    annotations: Vec<Vec<String>>,
    enum_kinds: Vec<Option<pil_codec::Scalar>>,
    identity: Option<seed::identity::Identity>,
    summary_fields: Vec<summaries::SummaryField>,
    summary_analytics: Vec<summaries::SummaryAnalytics>,
    digest: Vec<u8>,
    version: (u32, u32),
    name: String,
    namespace: String,
    registration: Option<[u32; 2]>,
}
impl SeedDefinition {
    /// Registry snapshots may prescribe the same immutable Definition again.
    /// A pointer match is sufficient, but not required, for reader handoff.
    pub(crate) fn same_as(&self, other: &Self) -> bool {
        std::ptr::eq(self, other) || (self.digest == other.digest
            && self.version == other.version && self.registration == other.registration
            && self.annotations == other.annotations)
    }
    /// Shared presentation metadata; does not participate in encoding or validation.
    #[cfg(any(feature = "registration", feature = "query-language"))]
    pub fn metadata(&self, ordinal: usize) -> Option<&'static metadata::Profile> {
        self.semantics.iter().find(|(i, _)| *i == ordinal).map(|(_, profile)| *profile)
    }
    pub fn prescribe(document: &Document) -> Result<Arc<Self>, Error> {
        Self::prescribe_member(document, 0)
    }
    pub fn prescribe_member(document: &Document, member: usize) -> Result<Arc<Self>, Error> {
        Self::prepare_member(document, member, false)
    }
    pub(crate) fn prescribe_summary_member(document: &Document, member: usize) -> Result<Arc<Self>, Error> {
        Self::prepare_member(document, member, true)
    }
    fn prepare_member(document: &Document, member: usize, summary: bool) -> Result<Arc<Self>, Error> {
        let rx = Arc::new(document.prescribe_member(member)?);
        // Stored class-object annotations describe aggregates, not source
        // fields to feed recursively into another summary accumulator.
        let summary_fields = if summary { Vec::new() } else { summaries::classify(document, member)? };
        let summary_analytics = if summary { Vec::new() } else { summaries::analytics(document, member)? };
        let annotations: Vec<Vec<String>> = document.definitions()[member]
            .fields
            .iter()
            .map(|f| f.annotations.clone())
            .collect();
        let header = &document.definitions()[member].header;
        use pil_codec::{Scalar,authoring_language::language as lang};
        for field in &document.definitions()[member].fields {
            for tag in &field.annotations {
                if (tag == "@timestamp" || tag.starts_with("@timestamp("))
                    && (tag != "@timestamp(ms)"
                        || !matches!(field.base_kind(), [lang::INT, 0] | [lang::UINT, 0]))
                {
                    return Err(error(format!("{}: use @timestamp(ms) on an integer field", field.name)));
                }
            }
        }
        let enum_kinds=document.definitions()[member].fields.iter().map(|field|match field.base_kind() {
            [lang::ENUM,lang::UINT]=>Some(Scalar::Uint(16)),[lang::ENUM,lang::INT]=>Some(Scalar::Int(16)),_=>None,
        }).collect();
        let identity = seed::identity::Identity::new(&rx, &annotations);
        Ok(Arc::new(Self {
            plans: query::Plans::default(),
            summary_plan: Default::default(),
            #[cfg(any(feature = "registration", feature = "query-language"))]
            semantics: metadata::bindings(&rx, &annotations),
            change_key: None, rx, annotations, enum_kinds, identity, summary_fields, summary_analytics, digest: header.rx_hash.clone(),
            version: (header.rx_major, header.rx_minor),
            name: header.rx_name.clone(), namespace: header.namespace.clone(),
            registration: None,
        }))
    }
    pub fn read(bytes: &[u8]) -> Result<Arc<Self>, Error> {
        Self::prescribe(&doc::read(bytes)?)
    }
    pub fn version(&self) -> (u32,u32) {self.version}
    pub fn digest(&self) -> &[u8] {
        &self.digest
    }
    pub fn prescription(&self) -> &Arc<Prescription> {
        &self.rx
    }
    /// Bind application-known array lengths once. The authored RX and its
    /// digest remain unchanged; query plans belong to this executable shape.
    pub fn arrays(&self, lengths: &[(usize, usize)]) -> Result<Arc<Self>, Error> {
        let rx = Arc::new(self.rx.arrays(lengths)?);
        Ok(self.with_prescription(rx))
    }
    /// Bind generic RX parameters once for this immutable executable shape.
    pub fn parameters(&self, bindings:&[pil_codec::api::Binding])->Result<Arc<Self>,Error>{
        Ok(self.with_prescription(Arc::new(self.rx.parameters(bindings)?)))
    }
    fn with_prescription(&self, rx: Arc<Prescription>) -> Arc<Self> {
        Arc::new(Self {
            plans: query::Plans::default(),
            summary_plan: Default::default(),
            #[cfg(any(feature = "registration", feature = "query-language"))]
            semantics: self.semantics.clone(),
            identity: seed::identity::Identity::new(&rx, &self.annotations),
            change_key: self.change_key.clone(), rx,
            annotations: self.annotations.clone(), enum_kinds: self.enum_kinds.clone(),
            summary_fields: self.summary_fields.clone(), summary_analytics: self.summary_analytics.clone(),
            digest: self.digest.clone(), version: self.version,
            name: self.name.clone(), namespace: self.namespace.clone(),
            registration: self.registration,
        })
    }
    fn pack_definition(self: &Arc<Self>, pack: &OwnedPack) -> Arc<Self> {
        if self.rx.binding_bytes() == pack.prescription().binding_bytes() {
            self.clone()
        } else {
            self.with_prescription(Arc::new(pack.prescription().clone()))
        }
    }
    pub fn annotations(&self, ordinal: usize) -> Option<&[String]> {
        self.annotations.get(ordinal).map(Vec::as_slice)
    }
    #[cfg(feature = "registration")]
    pub(crate) fn annotated(self: &Arc<Self>, annotations: &[Vec<String>], reference: [u32; 2]) -> Result<Arc<Self>, Error> {
        if self.annotations == annotations && self.registration == Some(reference) { return Ok(self.clone()); }
        if self.annotations.len() != annotations.len() {
            return Err(error("annotation snapshot does not match its Definition"));
        }
        // Interpret application annotations once. Reuse the existing codec;
        // annotations do not alter its wire layout or require re-registration.
        let document = doc::read(self.rx.bytes())?;
        let member = document.member(&self.namespace, &self.name, self.version.0, self.version.1)?;
        let mut definitions = document.definitions().to_vec();
        for (field, values) in definitions[member].fields.iter_mut().zip(annotations) {
            field.annotations = values.clone();
        }
        let document = Document::from_definitions(definitions);
        Ok(Arc::new(Self {
            plans: query::Plans::default(),
            summary_plan: Default::default(),
            semantics: metadata::bindings(&self.rx, annotations),
            change_key: self.change_key.clone(), rx: self.rx.clone(),
            annotations: annotations.to_vec(),
            enum_kinds: self.enum_kinds.clone(),
            identity: seed::identity::Identity::new(&self.rx, annotations),
            summary_fields: summaries::classify(&document, member)?,
            summary_analytics: summaries::analytics(&document, member)?,
            digest: self.digest.clone(), version: self.version,
            name: self.name.clone(), namespace: self.namespace.clone(),
            registration: Some(reference),
        }))
    }
    pub fn summary_fields(&self) -> &[summaries::SummaryField] {
        &self.summary_fields
    }
    pub fn summary_analytics(&self) -> &[summaries::SummaryAnalytics] {
        &self.summary_analytics
    }
    pub fn summaries(self: &Arc<Self>, rows: usize) -> Result<summaries::SummaryPlan, Error> {
        summaries::SummaryPlan::new(self.clone(), rows)
    }

    pub fn seed(&self) -> Seed<'_> {
        self.rx.dispense()
    }
    pub fn seed_owned(self: &Arc<Self>) -> Seed<'static> {
        self.rx.clone().dispense_owned()
    }
    /// The ingress adapter writes typed values through the existing builder.
    /// Transforms/identity policy belong to that adapter, never to Pill Packer.
    pub fn ingest<'a, T>(
        &'a self,
        event: T,
        transform: impl FnOnce(T, &mut Seed<'a>) -> Result<(), Error>,
    ) -> Result<Seed<'a>, Error> {
        let mut seed = self.seed();
        transform(event, &mut seed)?;
        Ok(seed)
    }
    pub fn fill(&self, count: usize) -> Result<SeedPack<'_>, Error> {
        Ok(SeedPack {
            inner: self.rx.fill(count)?,
        })
    }
    /// Open published or recovery bytes without changing the stored lifecycle.
    pub fn open(self: &Arc<Self>, bytes: Vec<u8>) -> Result<Arc<SeedPackSnapshot>, Error> {
        #[cfg(feature = "compression")]
        let bytes = expand_owned(bytes)?;
        let byte_length = bytes.len();
        let allocation = bytes.as_ptr() as usize;
        let pack = Arc::new(OwnedPack::new(self.rx.clone(), bytes)?);
        Ok(Arc::new(SeedPackSnapshot {
            definition: self.pack_definition(&pack),
            lineage: None,
            pack,
            byte_length,
            allocation,
            storage: None,
            memory: None,
            write_checked: std::sync::atomic::AtomicBool::new(false),
        }))
    }
    /// Open stored bytes. Integrity is checked before publication, never by
    /// rereading the complete payload on a query open.
    pub fn open_stored(self: &Arc<Self>, bytes: Vec<u8>) -> Result<Arc<SeedPackSnapshot>, Error> {
        self.open(bytes)
    }
    pub fn open_stored_range(
        self: &Arc<Self>,
        owner: Arc<Vec<u8>>,
        range: std::ops::Range<usize>,
    ) -> Result<Arc<SeedPackSnapshot>, Error> {
        self.open_range(owner, range)
    }
    /// Storage receivers bound both encoded and expanded bytes. A compressed
    /// Pack's inner length is checked by the shared decompressor, not this layer.
    pub fn open_stored_range_limited(
        self: &Arc<Self>,
        owner: Arc<Vec<u8>>,
        range: std::ops::Range<usize>,
        maximum: usize,
    ) -> Result<Arc<SeedPackSnapshot>, Error> {
        let raw = owner.get(range.clone()).ok_or(Error::Incomplete)?;
        if raw.len() > maximum {
            return Err(error("Seed Pack exceeds receive limit"));
        }
        #[cfg(feature = "compression")]
        {
            let mut expanded = Vec::new();
            if pil_codec::compression::unseal_with_limit(raw, maximum, &mut expanded)
                .map_err(|e| error(e.to_string()))?
            {
                return self.open(expanded);
            }
        }
        self.open_range(owner, range)
    }
    pub fn open_range(
        self: &Arc<Self>,
        owner: Arc<Vec<u8>>,
        range: std::ops::Range<usize>,
    ) -> Result<Arc<SeedPackSnapshot>, Error> {
        #[cfg(feature = "compression")]
        {
            let raw = owner
                .get(range.clone())
                .ok_or_else(|| error("invalid Pack range"))?;
            let mut expanded = Vec::new();
            if pil_codec::compression::unseal(raw, &mut expanded)
                .map_err(|e| error(e.to_string()))?
            {
                return self.open(expanded);
            }
        }
        let byte_length = range
            .end
            .checked_sub(range.start)
            .ok_or_else(|| error("invalid Pack range"))?;
        let allocation = owner.as_ptr() as usize;
        let pack = Arc::new(OwnedPack::shared_range(self.rx.clone(), owner, range)?);
        Ok(Arc::new(SeedPackSnapshot {
            definition: self.pack_definition(&pack),
            lineage: None,
            pack,
            byte_length,
            allocation,
            storage: None,
            memory: None,
            write_checked: std::sync::atomic::AtomicBool::new(false),
        }))
    }
}

/// Delegation preserves capped appends and uncapped sleeve encoding unchanged.
/// Published snapshots provide Pack-local filtering; global limits belong to
/// the source coordinator.
pub struct SeedPack<'a> {
    inner: Pack<'a>,
}
impl<'a> Deref for SeedPack<'a> {
    type Target = Pack<'a>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl DerefMut for SeedPack<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
impl SeedPack<'_> {
    /// Bind a time-trie slot using the existing Seed application header.
    /// This is an epoch-millisecond hour boundary, not a member ordinal.
    pub fn hour(&mut self, epoch_ms: u64) -> Result<&mut Self, Error> {
        if epoch_ms % 3_600_000 != 0 {
            return Err(error("Seed Pack hour must be a UTC hour boundary"));
        }
        self.inner
            .header("seed.hour", epoch_ms.to_le_bytes().to_vec())?;
        Ok(self)
    }
    pub fn into_bytes(self) -> Result<Vec<u8>, Error> {
        Ok(self.inner.into_bytes()?.into_owned())
    }
}

/// An immutable published view; each cursor allocates its own traversal state.
pub struct SeedPackSnapshot {
    definition: Arc<SeedDefinition>,
    lineage: Option<Arc<std::collections::HashMap<usize, Arc<SeedDefinition>>>>,
    pack: Arc<OwnedPack>,
    byte_length: usize,
    allocation: usize,
    storage: Option<Arc<dyn StorageBinding>>,
    memory: Option<Arc<MemoryLease>>,
    write_checked: std::sync::atomic::AtomicBool,
}
impl SeedPackSnapshot {
    pub(crate) fn with_memory(&self, memory: Arc<MemoryLease>) -> Arc<Self> {
        Arc::new(Self { definition:self.definition.clone(),lineage:self.lineage.clone(),pack:self.pack.clone(),
            byte_length:self.byte_length,allocation:self.allocation,storage:self.storage.clone(),memory:Some(memory),
            write_checked:std::sync::atomic::AtomicBool::new(self.write_checked.load(std::sync::atomic::Ordering::Relaxed)) })
    }
    /// Read the supplied fields of one indexed revision, preserving explicit nulls.
    pub fn stored_object(&self, ordinal: usize) -> Result<Object, Error> {
        self.supplied_object(ordinal, None)
    }
    /// Resolve the active shape and expand its event identity for Object History.
    pub fn revision_object(&self, ordinal: usize) -> Result<(&SeedDefinition, Object), Error> {
        let definition = self.definition_at(ordinal)?;
        Ok((definition, self.supplied_object(ordinal, definition.identity)?))
    }
    fn supplied_object(&self, ordinal: usize, identity: Option<seed::identity::Identity>) -> Result<Object, Error> {
        let mut reader = self.pack.reader();
        let pill = reader.get(ordinal)?;
        let mut object = Vec::new();
        for (i, field) in pill.prescription().definition().fields().iter().enumerate() {
            let value = match identity {
                Some(id) if id.id == i => id.read(|field| Ok(pill.native_value(field)?))?,
                _ => pill.native_value(i)?,
            };
            if let Cell::Value(data) = value::own(value)? {
                object.push((field.name.clone(), data));
            }
        }
        Ok(object)
    }
    pub fn definitions(&self) -> impl Iterator<Item = &Arc<SeedDefinition>> {
        self.lineage.iter().flat_map(|m| m.values())
            .chain(self.lineage.is_none().then_some(&self.definition))
    }
    /// Resolve the registered Definition for a particular record ordinal.
    pub fn definition_at(&self, ordinal: usize) -> Result<&Arc<SeedDefinition>, Error> {
        if ordinal >= self.len() { return Err(error("Pack record ordinal outside range")); }
        match &self.lineage {
            None => Ok(&self.definition),
            Some(m) => m.get(&(self.pack.prescription_at(ordinal)?.source_identity()))
                .ok_or_else(|| error("missing registered row Definition")),
        }
    }

    pub(crate) fn checked_for_write(&self) {
        self.write_checked.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    pub(crate) fn before_write(&self) -> Result<(), Error> {
        if !self.write_checked.load(std::sync::atomic::Ordering::Relaxed) {
            integrity::before_write(self.stored_bytes())?;
            self.checked_for_write();
        }
        Ok(())
    }
    pub(crate) fn allocation_bytes(&self) -> &[u8] { self.pack.allocation_bytes() }
    pub fn is_sealed(&self) -> bool {
        self.pack.is_sealed()
    }
    pub fn hour(&self) -> Result<Option<u64>, Error> {
        let Some(raw) = self.pack.headers().get("seed.hour") else {
            return Ok(None);
        };
        let bytes: [u8; 8] = raw
            .as_slice()
            .try_into()
            .map_err(|_| error("Invalid Seed Pack hour"))?;
        let hour = u64::from_le_bytes(bytes);
        if hour % 3_600_000 != 0 {
            return Err(error("Seed Pack hour must be a UTC hour boundary"));
        }
        Ok(Some(hour))
    }
    pub fn headers(&self) -> &std::collections::BTreeMap<String, Vec<u8>> {
        self.pack.headers()
    }
    pub fn len(&self) -> usize {
        self.pack.len()
    }
    pub fn is_empty(&self) -> bool {
        self.pack.is_empty()
    }
    pub fn definition(&self) -> &Arc<SeedDefinition> {
        &self.definition
    }
    pub fn byte_length(&self) -> usize {
        self.byte_length
    }
    pub fn stored_bytes(&self) -> &[u8] {
        self.pack.stored_bytes()
    }
    pub fn retained_bytes(&self) -> usize {
        self.pack.retained_bytes()
    }
}

/// Query sources selected by PACKS/BEDS routing. Container I/O and replication
/// are separate from this execution catalog; entries retain immutable Packs.
#[derive(Default, Clone)]
pub struct Sources {
    packs: Vec<Arc<SeedPackSnapshot>>,
}
impl Sources {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn pack(&mut self, pack: Arc<SeedPackSnapshot>) -> &mut Self {
        self.packs.push(pack);
        self
    }
    pub fn query(&self, query: Query) -> Result<Cursor, Error> {
        Cursor::new(self.packs.clone(), query)
    }
}

impl PackSource for Sources {
 fn len(&self)->usize {self.packs.len()}
 fn definitions(&self)->Vec<Arc<SeedDefinition>> {
  let mut seen=std::collections::HashSet::new();
  self.packs.iter().flat_map(|p|p.definitions()).filter(|d|seen.insert(Arc::as_ptr(d))).cloned().collect()
 }
 fn load(&mut self,ordinal:usize)->Result<Arc<SeedPackSnapshot>,Error> {
  self.packs.get(ordinal).cloned().ok_or_else(||error("Pack outside resident selection"))
 }
}

fn error(message: impl Into<String>) -> Error {
    Error::Definition(message.into())
}

#[cfg(feature = "compression")]
fn expand_owned(bytes: Vec<u8>) -> Result<Vec<u8>, Error> {
    let mut expanded = Vec::new();
    if pil_codec::compression::unseal(&bytes, &mut expanded).map_err(|e| error(e.to_string()))? {
        Ok(expanded)
    } else {
        Ok(bytes)
    }
}

pub(crate) use storage::LeafFileBuilder;
