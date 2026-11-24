//! Inferred structural-shape DAG, stored in the authoritative trie footer.
//! This is registration metadata, not a second Definition or a record codec.
use crate::base::Base;
use crate::{BranchId, Error, Kind, NodeId, Trunk, edit::Edit};
use pil_codec::{
    authoring_language::{language as lang, wire},
    doc::Document,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};

pub const SHAPES_SECTION: u32 = 2;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Attribute {
    pub name: String,
    pub label: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Shape {
    /// Interned field identities in canonical index order, never .rx order.
    pub fields: Vec<u32>,
    /// Closest registered strict subsets; traversing these edges finds all bases.
    pub bases: Vec<u32>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Definition {
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub base: Option<Base>,
    pub digest: Vec<u8>,
    pub name: String,
    pub namespace: String,
    pub major: u32,
    pub minor: u32,
    pub file: String,
    pub member: u32,
    pub shape: u32,
    /// Canonical shape-field position -> original Definition ordinal.
    pub ordinals: Vec<u32>,
}
impl Definition {
    pub fn qualified_name(&self) -> String {
        if self.namespace.is_empty() {
            self.name.clone()
        } else {
            format!("{}.{}", self.namespace, self.name)
        }
    }
}
/// Exact resource selection projected from the authoritative taxonomy.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Route {
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub base: Option<Base>,
    pub branch: BranchId,
    pub definition: u32,
    pub digest: String,
    pub cache: String,
    pub service: String,
    pub resource: String,
    pub resource_type: String,
    pub level: String,
    pub major: u32,
    pub minor: u32,
}
/// Physical storage generation, separate from the logical taxonomy source.
/// Minor revisions share a generation; major revisions have independent files.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoragePaths {
    pub directory: String,
    pub stem: String,
}
impl Route {
    fn generation(&self) -> String {
        if self.major == 0 { "0".into() } else { format!("v{}", self.major) }
    }
    pub fn pods(&self) -> Result<Option<String>, Error> {
        if self.base != Some(Base::Object) {return Ok(None);}
        Ok(Some(format!("{}/{}{}.pods", self.storage()?.directory,self.resource_type,if self.major == 0 { ".0" } else { "" })))
    }
    pub fn event_storage(&self) -> Result<StoragePaths,Error> {
        if self.base == Some(Base::Object) {return Err("Object registrations use PODS".into());}
        self.storage()
    }
    pub fn leaf_path(&self) -> Result<String, Error> {
        Ok(format!("{}/{}.{}.leaf", self.storage()?.directory, self.resource_type, self.generation()))
    }
    pub fn source(&self) -> Result<String, Error> {
        let storage = self.storage()?;
        let branch = if self.level.is_empty() { self.resource_type.clone() }
            else { format!("{}.{}", self.resource_type, self.level) };
        Ok(format!("{}/{branch}", storage.directory))
    }
    pub fn storage(&self) -> Result<StoragePaths, Error> {
        for component in [&self.cache, &self.service, &self.resource, &self.resource_type, &self.level] {
            if component == "." || component == ".." || component.contains(['/', '\\', '\0']) {
                return Err("Taxonomy label cannot address a storage file".into());
            }
        }
        let stem = if self.level.is_empty() { self.resource_type.clone() }
            else { format!("{}.{}", self.resource_type, self.level) };
        Ok(StoragePaths { directory: format!("{}/{}/{}", self.cache, self.service, self.resource), stem: format!("{stem}.{}", self.generation()) })
    }
}
/// A working copy belongs to a placement. The token identifies its disposable storage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Draft {
    pub generation: String,
    pub basis: Option<String>,
    pub file: String,
}
impl Draft {
    pub fn validate(&self, source: &str) -> Result<(), Error> {
        let valid_hex = |s: &str, size| s.len() == size && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        if source.split('/').count() != 4 || source.split('/').any(|p| p.is_empty() || p.starts_with('.') || p.contains(['\\','\0']) || p.chars().any(char::is_control))
            || !valid_hex(&self.generation,32) || !valid_file(&self.file)
            || self.basis.as_ref().is_some_and(|b| !valid_hex(b,64)) {
            return Err("Invalid draft placement or generation".into());
        }
        Ok(())
    }
}
/// Selection is scoped to a supplied .rx bundle. No selection means member 0.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Selection {
    #[serde(default)]
    pub namespace: String,
    pub name: String,
    pub major: u32,
    pub minor: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Resource {
    pub branch: BranchId,
    pub definition: u32,
    /// Shared summary class library attached to this event branch.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub summaries: Vec<u32>,
    /// Application-owned resource configuration. It belongs to registration,
    /// never to the reusable .rx document or its layout/digest.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub binding: BTreeMap<String, serde_json::Value>,
}
/// Revision of the separately retrievable .rx resource. Unlike a Definition's
/// structural digest, this identity includes application annotations.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Artifact {
    pub etag: String,
    pub size: u64,
}
/// Interpretation of summary days owned by one physical branch dataset.
/// These are source/annotation selections, not Pack addresses or row schemas.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SummaryContext {
    pub first: u64,
    pub end: u64,
    pub sources: Vec<[u32; 2]>,
    pub sealed: bool,
}
impl Artifact {
    fn from_bytes(bytes: &[u8]) -> Self {
        Self { etag: format!("{:x}", Sha256::digest(bytes)), size: bytes.len() as u64 }
    }
    pub fn matches(&self, bytes: &[u8]) -> bool {
        self.size == bytes.len() as u64 && *self == Self::from_bytes(bytes)
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Registry {
    /// One structural fingerprint per distinct named field, independent of annotations.
    fields: Vec<Vec<u8>>,
    attributes: Vec<Attribute>,
    shapes: Vec<Shape>,
    definitions: Vec<Definition>,
    resources: Vec<Resource>,
    #[serde(default,skip_serializing_if="BTreeMap::is_empty")]
    pub drafts: BTreeMap<String, Draft>,
    artifacts: BTreeMap<String, Artifact>,
    /// Field annotations are application configuration, not structural RX
    /// identity. Retain published snapshots once per event Definition so
    /// stored analytic layouts can resolve their original interpretation.
    annotations: BTreeMap<u32, Vec<Vec<Vec<String>>>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    summary_contexts: BTreeMap<BranchId, Vec<SummaryContext>>,
}
/// A resource type is supplied explicitly; it is not guessed from field names.
pub struct Registration<'a> {
    pub cache: &'a str,
    pub service: &'a str,
    pub resource: &'a str,
    pub resource_type: &'a str,
    pub level: &'a str,
    /// Relative to the existing definitions directory.
    pub file: &'a str,
}
fn pil<T: Serialize>(value: &T) -> Result<Vec<u8>, Error> {
    let data = ciborium::Value::serialized(value).map_err(|e| e.to_string())?;
    pil_codec::object::encode_value(&data).map_err(|e| e.to_string())
}
fn subset(a: &[u32], b: &[u32]) -> bool {
    a.iter().all(|id| b.binary_search(id).is_ok())
}
impl Registry {
    pub fn annotations(&self, definition: u32) -> Result<&[Vec<Vec<String>>], Error> {
        self.annotations.get(&definition).map(Vec::as_slice)
            .ok_or_else(|| "Unknown Definition annotation history".into())
    }
    /// One independent HTTP resource per referenced file, including bundles
    /// containing multiple registered Definition versions.
    pub fn artifacts(&self) -> &BTreeMap<String, Artifact> {
        &self.artifacts
    }
    pub fn artifact(&self, file: &str) -> Result<&Artifact, Error> {
        self.artifacts.get(file).ok_or_else(|| "Unregistered Definition file".into())
    }
    pub fn shapes(&self) -> &[Shape] {
        &self.shapes
    }
    pub fn definitions(&self) -> &[Definition] {
        &self.definitions
    }
    pub fn resources(&self) -> &[Resource] {
        &self.resources
    }
    /// Discover registrations under an actual taxonomy node. Namespaces never
    /// create nodes or confer an inherited binding on unregistered branches.
    pub fn resources_at(&self, trunk: &Trunk, node: NodeId) -> Result<Vec<&Resource>, Error> {
        trunk.node(node)?;
        let mut pending = vec![node];
        let mut branches = HashSet::new();
        while let Some(id) = pending.pop() {
            if let Some(branch) = trunk.node(id)?.branch_id {
                branches.insert(branch);
            }
            pending.extend_from_slice(trunk.children(id)?);
        }
        Ok(self
            .resources
            .iter()
            .filter(|r| branches.contains(&r.branch))
            .collect())
    }
    pub fn lineage(&self, namespace: &str, name: &str) -> Vec<(u32, &Definition)> {
        let mut result: Vec<_> = self
            .definitions
            .iter()
            .enumerate()
            .filter(|(_, d)| d.namespace == namespace && d.name == name)
            .map(|(id, d)| (id as u32, d))
            .collect();
        result.sort_by_key(|(id, d)| (d.major, d.minor, *id));
        result
    }
    /// Stored data binds an exact schema, never the bundle's current default.
    /// Resolve by identity/version when a later publication moves its ordinal.
    pub fn prescribe(
        &self,
        id: u32,
        document: &Document,
    ) -> Result<pil_codec::api::Prescription, Error> {
        document.prescribe_member(self.member(id, document)?).map_err(|e| e.to_string())
    }
    /// Resolve an exact registered schema without compiling it. Seed semantics
    /// and RX compilation consume this same selection on native and WASM.
    pub fn member(&self, id: u32, document: &Document) -> Result<usize, Error> {
        let registered = self
            .definitions
            .get(id as usize)
            .ok_or("Unknown Definition")?;
        let member = document
            .member(
                &registered.namespace,
                &registered.name,
                registered.major,
                registered.minor,
            )
            .map_err(|e| e.to_string())?;
        if document.definitions()[member].header.rx_hash != registered.digest {
            return Err("Registered Definition digest does not match bundle member".into());
        }
        Ok(member)
    }
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }
    pub fn attributes(&self) -> &[Attribute] { &self.attributes }

    pub fn read(trunk: &Trunk) -> Result<Self, Error> {
        let Some(bytes) = trunk.section(SHAPES_SECTION) else {
            return Ok(Self::default());
        };
        let value = pil_codec::object::decode_value(bytes).map_err(|e| e.to_string())?;
        let registry: Self = value.deserialized().map_err(|e| e.to_string())?;
        registry.check(trunk)?;
        Ok(registry)
    }
    fn check(&self, trunk: &Trunk) -> Result<(), Error> {
        for (source, draft) in &self.drafts { draft.validate(source)?; }

        if self.artifacts.iter().any(|(file, a)| !valid_file(file) || a.size == 0
            || a.etag.len() != 64 || !a.etag.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))) {
            return Err("Invalid Definition file revision".into());
        }
        if self.definitions.iter().any(|d| !self.artifacts.contains_key(&d.file)) {
            return Err("Missing registered Definition file".into());
        }
        if self.attributes.len() != self.fields.len() || self.attributes.iter().any(|a|a.name.is_empty()) {
            return Err("Invalid registered attributes".into());
        }
        let mut unique = HashSet::new();
        if self
            .fields
            .iter()
            .any(|f| f.len() != 32 || !unique.insert(f))
        {
            return Err("Invalid shape field identities".into());
        }
        let mut shape_keys = HashSet::new();
        for shape in &self.shapes {
            if shape
                .fields
                .iter()
                .any(|&f| f as usize >= self.fields.len())
                || shape.fields.windows(2).any(|w| w[0] >= w[1])
                || !shape_keys.insert(&shape.fields)
            {
                return Err("Invalid shape fields".into());
            }
            let mut seen = HashSet::new();
            for &id in &shape.bases {
                let base = self.shapes.get(id as usize).ok_or("Unknown base shape")?;
                if base.fields.len() >= shape.fields.len()
                    || !subset(&base.fields, &shape.fields)
                    || !seen.insert(id)
                {
                    return Err("Invalid shape DAG edge".into());
                }
            }
        }
        let mut digests = HashSet::new();
        for d in &self.definitions {
            let shape = self
                .shapes
                .get(d.shape as usize)
                .ok_or("Unknown Definition shape")?;
            let mut ordinals = d.ordinals.clone();
            ordinals.sort_unstable();
            if d.digest.len() != 32
                || !digests.insert(&d.digest)
                || ordinals != (0..shape.fields.len() as u32).collect::<Vec<_>>()
                || d.name.is_empty()
                || !valid_file(&d.file)
            {
                return Err("Invalid Definition registration".into());
            }
        }
        if self.annotations.len() != self.definitions.len() {
            return Err("Incomplete Definition annotation history".into());
        }
        for (&id, history) in &self.annotations {
            let definition = self.definitions.get(id as usize).ok_or("Unknown annotation Definition")?;
            if history.is_empty() || history.iter().any(|fields| fields.len() != definition.ordinals.len()) {
                return Err("Invalid Definition annotation history".into());
            }
        }
        let mut branches = HashSet::new();
        for r in &self.resources {
            trunk.branch(r.branch)?;
            if r.definition as usize >= self.definitions.len() || !branches.insert(r.branch) {
                return Err("Invalid resource registration".into());
            }
            let mut summaries = HashSet::new();
            if r.summaries.iter().any(|id| *id as usize >= self.definitions.len() || !summaries.insert(id)) {
                return Err("Invalid summary Definition registration".into());
            }
        }
        for (&owner, spans) in &self.summary_contexts {
            let branch = trunk.branch(owner)?;
            let parent = trunk.node(branch.parent)?.parent;
            if !self.resources.iter().any(|r| r.branch == owner) { return Err("Unknown summary owner".into()); }
            for (index, span) in spans.iter().enumerate() {
                if span.first >= span.end || span.sources.is_empty() || index > 0 && spans[index-1].end > span.first {
                    return Err("Invalid summary context intervals".into());
                }
                let mut unique = HashSet::new();
                for &[definition, revision] in &span.sources {
                    if !unique.insert([definition, revision]) || self.annotations(definition)?.get(revision as usize).is_none() {
                        return Err("Invalid summary annotation selection".into());
                    }
                    let mut belongs = false;
                    for resource in self.resources.iter().filter(|r| r.definition == definition) {
                        let candidate = trunk.branch(resource.branch)?;
                        belongs |= trunk.node(candidate.parent)?.parent == parent
                            && candidate.resource_type == branch.resource_type && candidate.level == branch.level;
                    }
                    if !belongs { return Err("Summary source is outside the owning branch".into()); }
                }
            }
        }
        // A missing edge would silently omit a valid base. Recompute only on
        // opening registration metadata, never during event traversal.
        let mut expected = self.clone();
        expected.relate();
        if expected.shapes != self.shapes {
            return Err("Incomplete shape DAG".into());
        }
        Ok(())
    }
    fn relate(&mut self) {
        for child in 0..self.shapes.len() {
            let fields = &self.shapes[child].fields;
            let candidates: Vec<_> = (0..self.shapes.len())
                .filter(|&base| {
                    self.shapes[base].fields.len() < fields.len()
                        && subset(&self.shapes[base].fields, fields)
                })
                .collect();
            self.shapes[child].bases = candidates
                .iter()
                .copied()
                .filter(|&base| {
                    !candidates.iter().any(|&middle| {
                        self.shapes[base].fields.len() < self.shapes[middle].fields.len()
                            && subset(&self.shapes[base].fields, &self.shapes[middle].fields)
                    })
                })
                .map(|n| n as u32)
                .collect();
        }
    }
    pub fn roots(&self) -> Vec<u32> {
        self.shapes
            .iter()
            .enumerate()
            .filter(|(_, s)| s.bases.is_empty())
            .map(|(i, _)| i as u32)
            .collect()
    }
    pub fn bases(&self, shape: u32) -> Result<Vec<u32>, Error> {
        let mut visited = HashSet::new();
        let mut todo = self
            .shapes
            .get(shape as usize)
            .ok_or("Unknown shape")?
            .bases
            .clone();
        while let Some(n) = todo.pop() {
            if visited.insert(n) {
                todo.extend_from_slice(&self.shapes[n as usize].bases);
            }
        }
        let mut bases: Vec<_> = visited.into_iter().collect();
        bases.sort_unstable();
        Ok(bases)
    }
    /// Base ordinal -> concrete ordinal. Only exact field shapes match; no cast,
    /// unit inference, required-field guarantee, or event validation occurs.
    pub fn projection(&self, base: u32, concrete: u32) -> Result<Vec<u32>, Error> {
        let base = self
            .definitions
            .get(base as usize)
            .ok_or("Unknown base Definition")?;
        let concrete = self
            .definitions
            .get(concrete as usize)
            .ok_or("Unknown concrete Definition")?;
        let a = &self.shapes[base.shape as usize].fields;
        let b = &self.shapes[concrete.shape as usize].fields;
        let mut map = vec![0; a.len()];
        for (position, field) in a.iter().enumerate() {
            let target = b
                .binary_search(field)
                .map_err(|_| "Definitions do not share the requested shape")?;
            map[base.ordinals[position] as usize] = concrete.ordinals[target];
        }
        Ok(map)
    }
    fn insert(&mut self, document: &Document, file: &str, selected: usize) -> Result<u32, Error> {
        let bytes = document.bytes().map_err(|e| e.to_string())?;
        let definitions = document.definitions();
        // Replacing a lineage bundle cannot strand schemas already referenced
        // by this registry. Annotations may change without changing identity.
        for old in self.definitions.iter().filter(|d| d.file == file) {
            if !definitions.iter().any(|d| d.header.rx_hash == old.digest) {
                return Err(format!("Definition bundle {file} would remove registered schema {}.{} {}.{}", old.namespace, old.name, old.major, old.minor));
            }
        }
        let mut atoms: HashMap<_, _> = self
            .fields
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, f)| (f, i as u32))
            .collect();
        let mut root = None;
        for (member, d) in definitions.iter().enumerate() {
            let mut ordered = Vec::new();
            for (ordinal, field) in d.fields.iter().enumerate() {
                let signature = fingerprint(field, definitions)?;
                let id = if let Some(&id) = atoms.get(&signature) {
                    id
                } else {
                    let id =
                        u32::try_from(self.fields.len()).map_err(|_| "Field index capacity")?;
                    atoms.insert(signature.clone(), id);
                    self.fields.push(signature);
                    self.attributes.push(Attribute { name: field.name.clone(), label: attribute_label(field) });
                    id
                };
                ordered.push((id, ordinal as u32));
            }
            ordered.sort_unstable();
            let fields: Vec<_> = ordered.iter().map(|p| p.0).collect();
            let ordinals: Vec<_> = ordered.iter().map(|p| p.1).collect();
            let shape = if let Some(i) = self.shapes.iter().position(|s| s.fields == fields) {
                i as u32
            } else {
                let i = u32::try_from(self.shapes.len()).map_err(|_| "Shape index capacity")?;
                self.shapes.push(Shape {
                    fields,
                    bases: vec![],
                });
                i
            };
            let base = Base::read(&d.fields)?;
            let h = &d.header;
            if h.rx_hash.len() != 32 {
                return Err("Definition is not signed".into());
            }
            let id = if let Some(i) = self.definitions.iter().position(|r| r.digest == h.rx_hash) {
                let old = &mut self.definitions[i];
                if old.shape != shape
                    || old.ordinals != ordinals
                    || old.name != h.rx_name
                    || old.namespace != h.namespace
                    || old.major != h.rx_major
                    || old.minor != h.rx_minor
                {
                    return Err("Conflicting Definition identity".into());
                }
                if old.base.is_some() && old.base != base {return Err("A registered base cannot be reassigned".into());}
                old.base = base;
                if old.file == file {
                    old.member = member as u32;
                }
                i as u32
            } else {
                let i = u32::try_from(self.definitions.len())
                    .map_err(|_| "Definition index capacity")?;
                self.definitions.push(Definition {
                    base,
                    digest: h.rx_hash.clone(),
                    name: h.rx_name.clone(),
                    namespace: h.namespace.clone(),
                    major: h.rx_major,
                    minor: h.rx_minor,
                    file: file.into(),
                    member: member as u32,
                    shape,
                    ordinals,
                });
                i
            };
            let annotations = d.fields.iter().map(|field| field.annotations.clone()).collect::<Vec<_>>();
            let history = self.annotations.entry(id).or_default();
            if history.last() != Some(&annotations) {
                history.push(annotations);
            }
            if member == selected {
                root = Some(id);
            }
        }
        self.artifacts.insert(file.into(), Artifact::from_bytes(bytes));
        self.relate();
        root.ok_or_else(|| "Definition ordinal outside bundle".into())
    }
}

pub fn attribute_label(field: &lang::Field) -> String {
    fn scalar(kind: u8, size: u16) -> String {
        let name = match kind {
            lang::UINT=>"uint",lang::INT=>"int",lang::FLOAT=>"float",lang::BYTES=>"bytes",
            lang::TSTR=>"tstr",lang::BOOL=>"bool",lang::NULL=>"null",lang::ANY=>"any",
            lang::OBJECT=>"object",lang::RECORD=>"record",lang::CHOICE=>"choice",_=>"none",
        };
        if size==0 {name.into()} else {format!("{name}({size})")}
    }
    let [a,b]=field.base_kind();
    match a {
        lang::ARRAY=>format!("array<{}>",scalar(b,field.element_size.unwrap_or(0))),
        lang::ENUM=>format!("enum<{}>",scalar(b,0)),
        lang::OBJECT=>"object".into(),
        _ if b!=0=>format!("map<{}, {}>",scalar(a,field.key_size.unwrap_or(0)),scalar(b,field.value_size.unwrap_or(0))),
        _=>scalar(a,field.size),
    }
}
fn valid_file(file: &str) -> bool {
    !file.is_empty() && file.ends_with(".rx") && !file.contains(['/', '\\', '\0']) && file != ".."
}
impl Trunk {
    fn summary_owner(&self, branch: BranchId) -> Result<BranchId, Error> {
        self.storage_versions(branch)?.into_iter().min().ok_or_else(|| "Unknown summary branch".into())
    }
    pub fn summary_context(&self, branch: BranchId, first: u64) -> Result<&SummaryContext, Error> {
        let owner = self.summary_owner(branch)?;
        let spans = self.definitions()?.summary_contexts.get(&owner)
            .ok_or("Summary context is not published for this branch")?;
        let at = spans.partition_point(|span| span.first <= first);
        at.checked_sub(1).and_then(|i| spans.get(i)).filter(|span| first < span.end)
            .ok_or_else(|| "Summary context is not published for this day".into())
    }
    /// Publish the same historical source selection used by the daily Box
    /// layout. Adjacent equal selections coalesce; completed days cannot change.
    pub fn register_summary_context(&self, branch: BranchId, context: SummaryContext) -> Result<Self, Error> {
        if context.first >= context.end || context.sources.is_empty() {
            return Err("Invalid summary context".into());
        }
        let family = self.storage_versions(branch)?;
        let owner = *family.iter().min().ok_or("Unknown summary branch")?;
        let registry = self.definitions()?;
        let mut unique = HashSet::new();
        for &[definition, revision] in &context.sources {
            if !unique.insert([definition, revision]) || !registry.resources.iter().any(|r| family.contains(&r.branch) && r.definition == definition)
                || registry.annotations(definition)?.get(revision as usize).is_none() {
                return Err("Summary source is outside the registered branch history".into());
            }
        }
        if registry.summary_contexts.get(&owner).is_some_and(|spans| spans.iter().any(|span| span.first <= context.first && span.end >= context.end
            && span.sources == context.sources && span.sealed == context.sealed)) { return Self::open(self.bytes.clone()); }
        let mut registry = registry.clone();
        let spans = registry.summary_contexts.entry(owner).or_default();
        let mut next = Vec::with_capacity(spans.len() + 2);
        for span in spans.iter() {
            if span.first >= context.end || span.end <= context.first { next.push(span.clone()); continue; }
            if span.sealed && (span.sources != context.sources || !context.sealed) {
                return Err("Cannot replace sealed summary context".into());
            }
            if span.first < context.first { next.push(SummaryContext { end: context.first, ..span.clone() }); }
            if span.end > context.end { next.push(SummaryContext { first: context.end, ..span.clone() }); }
        }
        next.push(context);
        next.sort_unstable_by_key(|span| span.first);
        spans.clear();
        for span in next {
            if let Some(last) = spans.last_mut().filter(|last| last.end == span.first && last.sources == span.sources && last.sealed == span.sealed) {
                last.end = span.end;
            } else { spans.push(span); }
        }
        let mut edit = Edit::from(self);
        edit.sections.insert(SHAPES_SECTION, pil(&registry)?);
        edit.finish()
    }
    /// Register the shared summary class library on an existing event branch.
    /// The caller writes the separate .rx resource before publishing this trie.
    pub fn register_summaries(&self, branch: BranchId, file: &str, document: &Document) -> Result<Trunk, Error> {
        if !valid_file(file) { return Err("Expected a Definition filename within definitions".into()); }
        let mut registry = self.definitions()?.clone();
        let position = registry.resources.iter().position(|r| r.branch == branch)
            .ok_or("Summary registration requires an existing branch")?;
        let root = &document.definitions().first().ok_or("Empty summary Definition")?.header;
        let previous = registry.resources[position].summaries.clone();
        if previous.iter().any(|id| {
            let old = &registry.definitions[*id as usize];
            old.name != root.rx_name || old.namespace != root.namespace || old.file != file
        }) { return Err("Summary registration must retain its lineage and resource".into()); }
        registry.insert(document, file, 0)?;
        let ids = registry.lineage(&root.namespace, &root.rx_name).into_iter()
            .filter(|(_, d)| d.file == file).map(|(id, _)| id).collect::<Vec<_>>();
        if previous.iter().any(|id| !ids.contains(id)) { return Err("Summary registration would remove a published revision".into()); }
        registry.resources[position].summaries = ids;
        let mut edit = Edit::from(self);
        edit.sections.insert(SHAPES_SECTION, pil(&registry)?);
        edit.finish()
    }
    /// Native and WASM readers use the same registry attached to this immutable
    /// trie image. Decode the footer once, not once per Definition lookup.
    pub fn definitions(&self) -> Result<&Registry, Error> {
        self.registry.get_or_init(|| Registry::read(self)).as_ref().map_err(Clone::clone)
    }
    pub fn definition(&self, id: u32) -> Result<&Definition, Error> {
        self.definitions()?.definitions.get(id as usize).ok_or_else(|| "Unknown Definition".into())
    }
    pub fn with_draft(&self, source:&str, draft:Option<Draft>) -> Result<Self, Error> {
        let mut registry=self.definitions()?.clone();
        let mut edit=Edit::from(self);
        if let Some(draft)=draft {
            let parts: Vec<_>=source.split('/').collect();
            draft.validate(source)?;
            let cache=edit.node(self.root(),Kind::Cache,parts[0],"","")?;
            let service=edit.node(cache,Kind::Service,parts[1],"","")?;
            edit.node(service,Kind::Resource,parts[2],"","")?;
            registry.drafts.insert(source.to_owned(),draft);
        } else { registry.drafts.remove(source); }
        edit.sections.insert(SHAPES_SECTION,pil(&registry)?);
        edit.finish()
    }
    pub fn definition_file(&self, file: &str) -> Result<&Artifact, Error> {
        self.definitions()?.artifact(file)
    }
    pub fn prescribe_definition(&self, id: u32, document: &Document) -> Result<pil_codec::api::Prescription, Error> {
        self.definitions()?.prescribe(id, document)
    }
    /// Prepare an immutable registered snapshot. The caller installs the authored
    /// .rx and publishes the snapshot through its existing registration lease.
    pub fn registration(
        &self,
        target: Registration<'_>,
        document: &Document,
    ) -> Result<(Trunk, Resource), Error> {
        self.register_resource(target, document, None, None)
    }

    /// Install or update resource-specific metadata in the existing registration
    /// section. Node records and branch IDs are unaffected by metadata edits.
    pub fn registration_with_binding(
        &self,
        target: Registration<'_>,
        document: &Document,
        binding: BTreeMap<String, serde_json::Value>,
    ) -> Result<(Trunk, Resource), Error> {
        self.register_resource(target, document, Some(binding), None)
    }
    pub fn registration_selected(
        &self,
        target: Registration<'_>,
        document: &Document,
        binding: Option<BTreeMap<String, serde_json::Value>>,
        selection: Option<&Selection>,
    ) -> Result<(Trunk, Resource), Error> {
        self.register_resource(target, document, binding, selection)
    }

    fn register_resource(
        &self,
        target: Registration<'_>,
        document: &Document,
        binding: Option<BTreeMap<String, serde_json::Value>>,
        selection: Option<&Selection>,
    ) -> Result<(Trunk, Resource), Error> {
        if !valid_file(target.file) {
            return Err("Expected a Definition filename within definitions".into());
        }
        let mut registry = Registry::read(self)?;
        let proposed = Route {
            base: None, branch: 0, definition: 0, digest: String::new(),
            cache: target.cache.into(), service: target.service.into(), resource: target.resource.into(),
            resource_type: target.resource_type.into(), level: target.level.into(), major: 0, minor: 0,
        };
        let source = proposed.source()?;
        // Distinct type/split pairs must not alias through their logical name.
        for resource in registry.resources() {
            let route = self.route(resource.branch)?;
            if route.source()? == source &&
                (route.resource_type != proposed.resource_type || route.level != proposed.level) {
                return Err("Resource type and branch alias an existing storage filename".into());
            }
        }
        let member = match selection {
            None => 0,
            Some(s) => document
                .member(&s.namespace, &s.name, s.major, s.minor)
                .map_err(|e| e.to_string())?,
        };
        let base = Base::read(&document.definitions()[member].fields)?;
        for resource in registry.resources() {
            let route = self.route(resource.branch)?;
            if route.source()? == source && (base == Some(Base::Object)) != (route.base == Some(Base::Object)) {
                return Err("A registered type must preserve its storage family".into());
            }
        }
        let prior_binding = registry.resources().iter().find_map(|resource| {
            let route = self.route(resource.branch).ok()?;
            (route.cache == target.cache && route.service == target.service &&
                route.resource == target.resource && route.resource_type == target.resource_type &&
                route.level == target.level).then_some(&resource.binding)
        });
        let effective_binding = binding.clone().or_else(|| prior_binding.cloned());
        validate_split(target.level, effective_binding.as_ref(), &document.definitions()[member].fields)?;
        let definition = registry.insert(document, target.file, member)?;
        let header = &document.definitions()[member].header;
        let mut edit = Edit::from(self);
        let cache = edit.node(self.root(), Kind::Cache, target.cache, "", "")?;
        let service = edit.node(cache, Kind::Service, target.service, "", "")?;
        let resource = edit.node(service, Kind::Resource, target.resource, "", "")?;
        let version = edit.node(
            resource,
            Kind::Version,
            &format!("{}.{}", header.rx_major, header.rx_minor),
            "",
            "",
        )?;
        let node = edit.node(
            version,
            Kind::Branch,
            "",
            target.resource_type,
            target.level,
        )?;
        let branch = edit.branch_id(node)?;
        let registration =
            if let Some(old) = registry.resources.iter_mut().find(|r| r.branch == branch) {
                if old.definition != definition {
                    return Err("Resource type is already bound to a different Definition".into());
                }
                if let Some(binding) = binding {
                    old.binding = binding;
                }
                old.clone()
            } else {
                let resource = Resource {
                    branch,
                    definition,
                    summaries: Vec::new(),
                    binding: effective_binding.unwrap_or_default(),
                };
                registry.resources.push(resource.clone());
                resource
            };
        edit.sections.insert(SHAPES_SECTION, pil(&registry)?);
        Ok((edit.finish()?, registration))
    }
}

/// Validate the selected partition against its finite registration mapping.
fn validate_split(
    value: &str,
    binding: Option<&BTreeMap<String, serde_json::Value>>,
    fields: &[lang::Field],
) -> Result<(), Error> {
    let plan = binding.map(|b| crate::splits::Splits::compile(b, fields)).transpose()?.flatten();
    if value.is_empty() { return Ok(()); }
    match plan {
        Some(plan) if plan.contains(value) => Ok(()),
        Some(_) => Err("Split value is outside the configured groups".into()),
        None => Err("Split requires a configured field".into()),
    }
}

fn fingerprint(field: &lang::Field, definitions: &[wire::Definition]) -> Result<Vec<u8>, Error> {
    let canonical = named(field, definitions, &mut vec![], 0)?;
    Ok(Sha256::digest(pil(&canonical)?).to_vec())
}
fn named(
    field: &lang::Field,
    definitions: &[wire::Definition],
    stack: &mut Vec<usize>,
    depth: usize,
) -> Result<serde_json::Value, Error> {
    Ok(
        serde_json::json!({"name":field.name,"descriptor":descriptor(&field.descriptor,definitions,stack,depth)?}),
    )
}
// Field order belongs to the executable Definition, not structural identity.
// Canonicalize only named fields; enum literals and choice alternatives retain
// their declared interpretation. No source Definition is reordered.
fn named_shape(
    fields: &[lang::Field],
    definitions: &[wire::Definition],
    stack: &mut Vec<usize>,
    depth: usize,
) -> Result<serde_json::Value, Error> {
    let mut ordered: Vec<_> = fields.iter().collect();
    ordered.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    Ok(serde_json::Value::Array(
        ordered
            .into_iter()
            .map(|f| named(f, definitions, stack, depth))
            .collect::<Result<Vec<_>, _>>()?,
    ))
}
fn referenced(
    ordinal: usize,
    definitions: &[wire::Definition],
    stack: &mut Vec<usize>,
    depth: usize,
) -> Result<serde_json::Value, Error> {
    if stack.contains(&ordinal) {
        return Err("Recursive Definition reference".into());
    }
    let d = definitions
        .get(ordinal)
        .ok_or("Object ordinal outside Definition bundle")?;
    stack.push(ordinal);
    let fields = named_shape(&d.fields, definitions, stack, depth + 1)?;
    stack.pop();
    Ok(fields)
}
fn descriptor(
    t: &lang::TypeDescriptor,
    definitions: &[wire::Definition],
    stack: &mut Vec<usize>,
    depth: usize,
) -> Result<serde_json::Value, Error> {
    if depth > 64 {
        return Err("Shape nesting limit".into());
    }
    let mut value = serde_json::to_value(t).map_err(|e| e.to_string())?;
    let kind = t.base_kind();
    if kind[0] == lang::OBJECT {
        value["type"] = serde_json::json!([t.kind[0], 0]);
        value["values"] = referenced(kind[1] as usize, definitions, stack, depth)?;
    } else if kind[1] == lang::OBJECT {
        let ordinal = t
            .values
            .first()
            .and_then(|v| v.as_u64())
            .ok_or("Missing object ordinal")?;
        value["values"] = referenced(
            usize::try_from(ordinal).map_err(|_| "Object ordinal overflow")?,
            definitions,
            stack,
            depth,
        )?;
    } else if kind == [lang::RECORD, 0] {
        let fields = t
            .values
            .iter()
            .map(|f| serde_json::from_value(f.clone()).map_err(|e| e.to_string()))
            .collect::<Result<Vec<lang::Field>, Error>>()?;
        value["values"] = named_shape(&fields, definitions, stack, depth + 1)?;
    } else if kind == [lang::CHOICE, 0] {
        value["values"] = serde_json::Value::Array(
            lang::choice_types(t)?
                .iter()
                .map(|t| descriptor(t, definitions, stack, depth + 1))
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    Ok(value)
}

impl Trunk {
    /// Registered versions of one cache/service/resource/type/branch. Starting
    /// from an older leaf selects the same resource scope as its newer leaves.
    pub fn branch_versions(&self, branch: BranchId) -> Result<Vec<BranchId>, Error> {
        self.route(branch)?;
        let leaf = self.branch(branch)?;
        let resource = self.node(leaf.parent)?.parent;
        self.resolve(resource, &[leaf.resource_type.to_owned()])?.into_iter()
            .filter_map(|id| match self.branch(id) {
                Ok(candidate) if candidate.level == leaf.level => Some(Ok(id)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            }).collect()
    }

    /// Minor revisions admitted to one physical storage generation. Logical
    /// discovery still uses branch_versions and includes every retained major.
    pub fn storage_versions(&self, branch: BranchId) -> Result<Vec<BranchId>, Error> {
        let major = self.route(branch)?.major;
        self.branch_versions(branch)?.into_iter().filter_map(|id| match self.route(id) {
            Ok(route) if route.major == major => Some(Ok(id)),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        }).collect()
    }

    pub fn route(&self, branch: BranchId) -> Result<Route, Error> {
        let registry = self.definitions()?;
        // Branch IDs already have a compact direct-address domain. Cache only
        // positions in the immutable registry, never a second binding model.
        let indexes = self.registrations_by_branch.get_or_init(|| {
            let mut indexes = vec![u32::MAX; self.leaves.len()];
            for (index, resource) in registry.resources().iter().enumerate() {
                indexes[resource.branch as usize] = index as u32;
            }
            indexes
        });
        let resource = indexes.get(branch as usize)
            .and_then(|&index| registry.resources().get(index as usize))
            .ok_or("Branch has no Definition registration")?;
        let definition = self.definition(resource.definition)?;
        let leaf = self.branch(branch)?;
        let mut cache = None; let mut service = None; let mut name = None;
        for id in self.path(leaf.id)? {
            let node = self.node(id)?;
            match node.kind {
                Kind::Cache => cache = Some(node.name.to_owned()),
                Kind::Service => service = Some(node.name.to_owned()),
                Kind::Resource => name = Some(node.name.to_owned()),
                _ => {},
            }
        }
        Ok(Route { base:definition.base, branch, definition: resource.definition,
            digest: definition.digest.iter().map(|b| format!("{b:02x}")).collect(),
            cache: cache.ok_or("Missing branch cache")?, service: service.ok_or("Missing branch service")?,
            resource: name.ok_or("Missing branch resource")?, resource_type: leaf.resource_type.to_owned(),
            level: leaf.level.to_owned(), major: definition.major, minor: definition.minor })
    }
}
