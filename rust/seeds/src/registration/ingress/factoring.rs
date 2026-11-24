//! State is interned at seal. Spools and open PACs retain the submitted PIL.
use super::*;
use crate::{
    LeafTrie, SeedDefinition,
    registration::{LeafWriter, Leaves, Snapshot},
};
use pil_codec::api::{FieldBytes, NativeValue, Pill, PillView};
use std::{borrow::Cow, sync::Arc};

pub struct StatePaths(BTreeMap<u16, Vec<u16>>);

impl StatePaths {
    pub fn registered(trie: &Snapshot, branch: u16, scope: &Packs) -> Result<Self, Error> {
        let registry = trie.definitions().map_err(error)?;
        let mut paths = BTreeMap::new();
        for id in trie.storage_versions(branch).map_err(error)? {
            let def = scope.definition(id)?;
            let path = registry
                .resources()
                .iter()
                .find(|r| r.branch == id)
                .and_then(|r| r.binding.get("path"))
                .map(|p| Self::compile(def, p))
                .transpose()?
                .unwrap_or_default();
            paths.insert(id, path);
        }
        if paths.values().all(Vec::is_empty) {
            paths.clear();
        }
        Ok(Self(paths))
    }
    pub(crate) fn compile(
        def: &SeedDefinition,
        path: &serde_json::Value,
    ) -> Result<Vec<u16>, Error> {
        let index = IndexFields::from_definition(def)?;
        let mut ordinals = Vec::new();
        for name in path
            .as_array()
            .ok_or_else(|| error("state path must be a field-name array"))?
        {
            let field = name
                .as_str()
                .and_then(|name| def.rx.field_ordinal(name))
                .ok_or_else(|| error("unregistered state path field"))?;
            let ordinal = u16::try_from(field).map_err(|_| Error::Overflow)?;
            if index.contains(field) || ordinals.contains(&ordinal) {
                return Err(error("repeated or reserved state path field"));
            }
            ordinals.push(ordinal);
        }
        Ok(ordinals)
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

struct Root {
    branch: u16,
    path: Vec<u16>,
    trie: LeafTrie,
    definition: Arc<SeedDefinition>,
    edges: HashMap<(u16, u16), HashMap<Arc<[u8]>, u16>>,
    initial: usize,
    frozen: bool,
    registered: bool,
    used: bool,
}

// FieldBytes may be an adaptive integer or dictionary ordinal in a source PIL.
// Read those through the codec before externalizing the direct field value.
fn direct<'a>(row: &'a PillView<'_>, i: usize, bytes: &'a [u8]) -> Result<Cow<'a, [u8]>, Error> {
    use pil_codec::{Scalar::*, Type, Value};
    let f = &row.prescription().definition().fields()[i];
    if !f.values.is_empty() {
        if let NativeValue::Scalar(Value::String(s)) = row.native_value(i)? {
            return Ok(Cow::Borrowed(s.as_bytes()));
        }
    }
    if let Type::Scalar(k @ (U16 | U32 | U64 | I16 | I32 | I64)) = f.kind {
        if bytes.len() != k.width().unwrap() {
            let v = match row.native_value(i)? {
                NativeValue::Scalar(Value::U64(v)) => v,
                NativeValue::Scalar(Value::U32(v)) => v.into(),
                NativeValue::Scalar(Value::U16(v)) => v.into(),
                NativeValue::Scalar(Value::I64(v)) => v as u64,
                NativeValue::Scalar(Value::I32(v)) => v as u64,
                NativeValue::Scalar(Value::I16(v)) => v as u64,
                _ => return Err(error("invalid adaptive state integer")),
            };
            return Ok(Cow::Owned(v.to_le_bytes()[..k.width().unwrap()].to_vec()));
        }
    }
    Ok(Cow::Borrowed(bytes))
}

impl Root {
    fn store(
        &mut self,
        pill: Pill<'_>,
        out: &mut SeedPack<'_>,
        supplied: u16,
        room: &mut usize,
    ) -> Result<u16, Error> {
        self.used = true;
        if self.path.is_empty() {
            out.load(pill)?;
            return Ok(supplied);
        }
        let raw = pill.bytes()?;
        let mut decoder = self.definition.rx.decoder();
        let row = decoder.read(&raw)?;
        let mut parent = 0;
        let mut depth = 0;
        for &field in &self.path {
            let FieldBytes::Bytes(raw) = row.field(field as usize)? else {
                break;
            };
            let value = direct(&row, field as usize, raw)?;
            let edges = self.edges.entry((parent, field)).or_default();
            parent = if let Some(&id) = edges.get(value.as_ref()) {
                id
            } else if self.frozen
                || self.trie.len() == u16::MAX as usize
                || value.len().saturating_add(256) > *room
            {
                break;
            } else {
                // Reserve room for the node frame and its directory entry as well as its value.
                *room -= value.len() + 256;
                let id = self.trie.append(parent, field, value.into_owned())?;
                let (_, _, bytes, _) = self.trie.edges().next_back().unwrap();
                edges.insert(bytes, id);
                id
            };
            depth += 1;
        }
        if supplied != 0
            && (supplied as usize > self.trie.len() || (depth != 0 && supplied != parent))
        {
            return Err(error(
                "supplied state leaf conflicts with registered state path",
            ));
        }
        if depth == 0 {
            out.load(pill)?;
            return Ok(supplied);
        }
        let mut values = Vec::with_capacity(self.definition.rx.definition().fields().len());
        for i in 0..self.definition.rx.definition().fields().len() {
            let field = if self.path[..depth].contains(&(i as u16)) {
                None
            } else {
                match row.field(i)? {
                    FieldBytes::Absent => None,
                    FieldBytes::Null => Some(None),
                    FieldBytes::Bytes(b) => Some(Some(direct(&row, i, b)?)),
                }
            };
            values.push(field);
        }
        let fields: Vec<_> = values
            .iter()
            .map(|f| match f {
                None => FieldBytes::Absent,
                Some(None) => FieldBytes::Null,
                Some(Some(b)) => FieldBytes::Bytes(b),
            })
            .collect();
        out.load(self.definition.rx.bind(Fill::Fields(&fields))?)?;
        Ok(parent)
    }
}

pub(super) struct Forest {
    writer: LeafWriter,
    roots: Vec<Root>,
    branches: HashMap<u16, usize>,
    room: usize,
}
impl Forest {
    pub(super) fn store(
        &mut self,
        branch: u16,
        pill: Pill<'_>,
        out: &mut SeedPack<'_>,
        supplied: u16,
    ) -> Result<u16, Error> {
        if let Some(&i) = self.branches.get(&branch) {
            self.roots[i].store(pill, out, supplied, &mut self.room)
        } else {
            out.load(pill)?;
            Ok(supplied)
        }
    }
    fn new(
        paths: &StatePaths,
        scope: &Packs,
        leaves: &Leaves<'_>,
        bytes: &[u8],
        maximum: usize,
    ) -> Result<Self, Error> {
        let dir = leaves.directory(bytes)?;
        let writer = leaves.resume(bytes)?;
        let mut roots: Vec<Root> = Vec::new();
        let mut branches = HashMap::new();
        for (&branch, path) in &paths.0 {
            let definition = scope.definition(branch)?.clone();
            let trie = writer.nodes(branch)?;
            let prior = dir.roots.iter().find(|r| r.definition == definition.digest);
            let index = roots.len();
            let edges = trie.edges().fold(
                HashMap::<_, HashMap<_, _>>::new(),
                |mut e, (p, f, b, id)| {
                    e.entry((p, f)).or_default().insert(b, id);
                    e
                },
            );
            roots.push(Root {
                branch,
                path: path.clone(),
                initial: trie.len(),
                trie,
                edges,
                frozen: prior.is_some_and(|r| r.frozen)
                    || (prior.is_none()
                        && dir.roots.last().is_some_and(|r| {
                            writer
                                .definitions
                                .values()
                                .any(|d| d.digest == r.definition && d.version > definition.version)
                        })),
                registered: prior.is_some(),
                used: false,
                definition,
            });
            branches.insert(branch, index);
        }
        Ok(Self {
            writer,
            roots,
            branches,
            room: maximum.saturating_sub(bytes.len()),
        })
    }
    fn finish(mut self) -> Result<Option<Vec<u8>>, Error> {
        self.roots
            .sort_by_key(|r| (!r.registered, r.definition.version));
        let mut changed = false;
        for root in self.roots {
            if root.trie.len() == root.initial
                && (root.registered || root.frozen || !root.used || self.room < 256)
            {
                continue;
            }
            if !root.registered {
                self.room = self.room.saturating_sub(256);
            }
            self.writer.register(root.branch)?;
            self.writer.append(&root.trie)?;
            changed = true;
        }
        changed.then(|| self.writer.publish()).transpose()
    }
}
#[cfg(feature = "compression")]
impl OwnedIngress {
    pub fn publish_factored(
        &mut self,
        hour: u64,
        now: u64,
        time: &[u8],
        footer: &[u8],
        length: u64,
        compression: Option<crate::Compression>,
        paths: &StatePaths,
        leaves: &Leaves<'_>,
        bytes: &[u8],
        maximum: usize,
    ) -> Result<(publication::Publication, Option<Vec<u8>>), Error> {
        let mut forest = Forest::new(paths, &self.scope, leaves, bytes, maximum)?;
        let publication = self.publish_using(
            hour,
            now,
            time,
            footer,
            length,
            compression,
            Some(&mut forest),
        )?;
        Ok((publication, forest.finish()?))
    }
}
