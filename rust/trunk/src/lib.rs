//! Read the published trunk.trie directly. Indexes contain addresses, never a
//! second taxonomy or copies of its labels. The server owns registration.
use std::{
    collections::{HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    ops::Range,
    sync::Arc,
};
pub type NodeId = u32;
pub type BranchId = u16;
pub type Error = String;
const HEADER: usize = 72;
const NODE: usize = 20;
fn u32_at(b: &[u8], p: usize) -> Result<u32, Error> {
    Ok(u32::from_le_bytes(
        b.get(p..p + 4)
            .ok_or("Truncated trunk trie")?
            .try_into()
            .unwrap(),
    ))
}
fn u64_at(b: &[u8], p: usize) -> Result<u64, Error> {
    Ok(u64::from_le_bytes(
        b.get(p..p + 8)
            .ok_or("Truncated trunk trie")?
            .try_into()
            .unwrap(),
    ))
}
fn size(n: u64) -> Result<usize, Error> {
    usize::try_from(n).map_err(|_| "Trunk trie exceeds address space".into())
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum Kind {
    Trunk = 1,
    Cache = 2,
    Service = 3,
    Resource = 4,
    Version = 5,
    Branch = 6,
}
impl TryFrom<u32> for Kind {
    type Error = Error;
    fn try_from(n: u32) -> Result<Self, Error> {
        match n {
            1 => Ok(Self::Trunk),
            2 => Ok(Self::Cache),
            3 => Ok(Self::Service),
            4 => Ok(Self::Resource),
            5 => Ok(Self::Version),
            6 => Ok(Self::Branch),
            _ => Err("Invalid taxonomy kind".into()),
        }
    }
}
#[derive(Debug)]
pub struct Node<'a> {
    pub id: NodeId,
    pub kind: Kind,
    pub parent: NodeId,
    pub name: &'a str,
    pub resource_type: &'a str,
    pub level: &'a str,
    pub branch_id: Option<BranchId>,
}
pub struct Trunk {
    bytes: Arc<[u8]>,
    end: usize,
    count: u32,
    generation: u64,
    labels: HashMap<u32, Range<usize>>,
    label_index: HashMap<u64, Vec<u32>>,
    edges: HashMap<(NodeId, u32, [u32; 3]), NodeId>,
    sections: HashMap<u32, Range<usize>>,
    children_start: Vec<usize>,
    children: Vec<NodeId>,
    leaves: Vec<NodeId>,
    leaf_ids: Vec<BranchId>,
    #[cfg(feature = "registration")]
    registry: std::sync::OnceLock<Result<registration::Registry, Error>>,
    #[cfg(feature = "registration")]
    registrations_by_branch: std::sync::OnceLock<Vec<u32>>,
}
fn label_hash(value: &str) -> u64 {
    let mut h = DefaultHasher::new();
    value.hash(&mut h);
    h.finish()
}
impl Trunk {
    pub fn open(bytes: impl Into<Arc<[u8]>>) -> Result<Self, Error> {
        let bytes = bytes.into();
        let b = &bytes;
        if b.len() < HEADER || &b[..8] != b"TTTAX000" {
            return Err("Invalid trunk trie header".into());
        }
        let mut best = None;
        for p in [8, 40] {
            let generation = u64_at(b, p)?;
            let end = u64_at(b, p + 16)?;
            if generation == 0
                || end > b.len() as u64
                || end < 88
                || u32_at(b, p + 28)? != 0
                || crc32c::crc32c(&b[p..p + 24]) != u32_at(b, p + 24)?
            {
                continue;
            }
            if best.is_none_or(|(g, _, _, _)| generation > g) {
                best = Some((
                    generation,
                    size(end)?,
                    u32_at(b, p + 8)?,
                    u32_at(b, p + 12)?,
                ));
            }
        }
        let (generation, end, count, leaf_count) = best.ok_or("Missing trunk trie commit")?;
        if &b[end - 8..end] != b"TTDIR000" || leaf_count > 65535 || count == 0 {
            return Err("Invalid trunk trie trailer/count".into());
        }
        let directory = size(u64_at(b, end - 16)?)?;
        let names = end
            .checked_sub(16)
            .and_then(|n| n.checked_sub(directory))
            .ok_or("Invalid name directory")?;
        let nodes_end = (count as usize)
            .checked_mul(NODE)
            .and_then(|n| HEADER.checked_add(n))
            .ok_or("Node count overflow")?;
        if directory > u32::MAX as usize - 16
            || nodes_end > names
            || names - nodes_end < 16
            || &b[names - 8..names] != b"TTSECT00"
        {
            return Err("Invalid trunk trie sections".into());
        }
        let section_count = size(u64_at(b, names - 16)?)?;
        if section_count > (names - nodes_end - 16) / 24 {
            return Err("Invalid section count".into());
        }
        let dir = names - 16 - section_count * 24;
        let mut sections = HashMap::new();
        let mut prior_end = nodes_end;
        let mut prior_id = None;
        for i in 0..section_count {
            let p = dir + i * 24;
            let id = u32_at(b, p)?;
            let distance = size(u64_at(b, p + 8)?)?;
            let len = size(u64_at(b, p + 16)?)?;
            let start = names
                .checked_sub(distance)
                .ok_or("Invalid section address")?;
            let stop = start.checked_add(len).ok_or("Section overflow")?;
            if start != prior_end
                || stop > dir
                || prior_id.is_some_and(|v| id <= v)
                || crc32c::crc32c(b.get(start..stop).ok_or("Invalid section range")?)
                    != u32_at(b, p + 4)?
            {
                return Err("Invalid section bounds/checksum".into());
            }
            sections.insert(id, start..stop);
            prior_end = stop;
            prior_id = Some(id);
        }
        if prior_end != dir {
            return Err("Invalid section extent".into());
        }
        let mut labels = HashMap::new();
        let mut label_index: HashMap<u64, Vec<u32>> = HashMap::new();
        let mut unique = HashSet::new();
        let mut at = end - 16;
        while at > names {
            if at - names < 4 {
                return Err("Truncated name directory".into());
            }
            let n = u32_at(b, at - 4)? as usize;
            if n == 0 || n > at - names - 4 {
                return Err("Invalid label length".into());
            }
            let start = at - 4 - n;
            let name = std::str::from_utf8(&b[start..at - 4])
                .map_err(|_| "Invalid UTF-8 taxonomy label")?;
            if !unique.insert(name) {
                return Err("Duplicate taxonomy label".into());
            }
            let reference = (end - start) as u32;
            label_index
                .entry(label_hash(name))
                .or_default()
                .push(reference);
            labels.insert(reference, start..at - 4);
            at = start;
        }
        let mut result = Self {
            bytes: bytes.clone(),
            end,
            count,
            generation,
            labels,
            label_index,
            edges: HashMap::new(),
            sections,
            children_start: vec![0; count as usize + 2],
            children: vec![0; count as usize],
            leaves: vec![0],
            leaf_ids: vec![0; count as usize + 1],
            #[cfg(feature = "registration")]
            registry: std::sync::OnceLock::new(),
            #[cfg(feature = "registration")]
            registrations_by_branch: std::sync::OnceLock::new(),
        };
        for id in 1..=count {
            let p = result.position(id)?;
            let parent = u32_at(b, p + 4)?;
            let kind = Kind::try_from(u32_at(b, p)?)?;
            if id == 1 {
                if parent != 0 || kind != Kind::Trunk {
                    return Err("Invalid taxonomy root".into());
                }
            } else if parent == 0
                || parent >= id
                || u32_at(b, result.position(parent)?)? + 1 != kind as u32
            {
                return Err("Invalid taxonomy parent".into());
            }
            let refs = [u32_at(b, p + 8)?, u32_at(b, p + 12)?, u32_at(b, p + 16)?];
            for r in refs {
                if r != 0 && !result.labels.contains_key(&r) {
                    return Err("Unknown label reference".into());
                }
            }
            if kind == Kind::Branch {
                if refs[0] != 0 || refs[1] == 0 || result.leaves.len() > 65535 {
                    return Err("Invalid branch".into());
                }
                result.leaf_ids[id as usize] = result.leaves.len() as u16;
                result.leaves.push(id);
            } else if refs[0] == 0 || refs[1] != 0 || refs[2] != 0 {
                return Err("Invalid node label roles".into());
            }
            if result
                .edges
                .insert((parent, kind as u32, refs), id)
                .is_some()
            {
                return Err("Duplicate taxonomy edge".into());
            }
            result.children_start[parent as usize + 1] += 1;
        }
        if result.leaves.len() - 1 != leaf_count as usize {
            return Err("Branch count mismatch".into());
        }
        for i in 1..result.children_start.len() {
            result.children_start[i] += result.children_start[i - 1];
        }
        let mut cursor = result.children_start.clone();
        for id in 1..=count {
            let parent = u32_at(b, result.position(id)? + 4)? as usize;
            result.children[cursor[parent]] = id;
            cursor[parent] += 1;
        }
        Ok(result)
    }
    fn position(&self, id: NodeId) -> Result<usize, Error> {
        if id == 0 || id > self.count {
            Err("Unknown taxonomy node".into())
        } else {
            Ok(HEADER + (id as usize - 1) * NODE)
        }
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes[..self.end]
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn root(&self) -> NodeId {
        1
    }
    pub fn len(&self) -> usize {
        self.count as usize
    }
    pub fn is_empty(&self) -> bool {
        false
    }
    pub fn label(&self, reference: u32) -> Result<&str, Error> {
        if reference == 0 {
            return Ok("");
        }
        let range = self
            .labels
            .get(&reference)
            .ok_or("Unknown label reference")?;
        Ok(std::str::from_utf8(&self.bytes[range.clone()]).expect("validated label"))
    }
    pub fn node(&self, id: NodeId) -> Result<Node<'_>, Error> {
        let p = self.position(id)?;
        Ok(Node {
            id,
            kind: Kind::try_from(u32_at(&self.bytes, p)?)?,
            parent: u32_at(&self.bytes, p + 4)?,
            name: self.label(u32_at(&self.bytes, p + 8)?)?,
            resource_type: self.label(u32_at(&self.bytes, p + 12)?)?,
            level: self.label(u32_at(&self.bytes, p + 16)?)?,
            branch_id: match self.leaf_ids[id as usize] {
                0 => None,
                n => Some(n),
            },
        })
    }
    pub fn children(&self, id: NodeId) -> Result<&[NodeId], Error> {
        self.position(id)?;
        Ok(&self.children[self.children_start[id as usize]..self.children_start[id as usize + 1]])
    }
    pub fn branch(&self, id: BranchId) -> Result<Node<'_>, Error> {
        self.node(
            *self
                .leaves
                .get(id as usize)
                .filter(|&&n| n != 0)
                .ok_or("Unknown branch ID")?,
        )
    }
    pub fn find(
        &self,
        parent: NodeId,
        kind: Kind,
        name: &str,
        resource_type: &str,
        level: &str,
    ) -> Result<Option<NodeId>, Error> {
        self.position(parent)?;
        let mut refs = [0; 3];
        for (i, value) in [name, resource_type, level].iter().enumerate() {
            if value.is_empty() {
                continue;
            }
            let Some(reference) = self
                .label_index
                .get(&label_hash(value))
                .and_then(|rs| rs.iter().find(|&&r| self.label(r).ok() == Some(*value)))
            else {
                return Ok(None);
            };
            refs[i] = *reference;
        }
        Ok(self.edges.get(&(parent, kind as u32, refs)).copied())
    }

    pub fn path(&self, mut id: NodeId) -> Result<Vec<NodeId>, Error> {
        let mut ids = Vec::with_capacity(6);
        while id != 0 {
            let n = self.node(id)?;
            ids.push(id);
            id = n.parent;
        }
        ids.reverse();
        Ok(ids)
    }
    /// Resolve cache.service.resource.type using selected ancestors for omitted
    /// qualifiers. Type is always explicit; versions/levels retain branch IDs.
    pub fn resolve(&self, scope: NodeId, qualifiers: &[String]) -> Result<Vec<BranchId>, Error> {
        if qualifiers.is_empty() || qualifiers.len() > 4 || qualifiers.iter().any(String::is_empty)
        {
            return Err("Expected cache.service.resource.type with an explicit type".into());
        }
        let mut names = [""; 4];
        for id in self.path(scope)? {
            let node = self.node(id)?;
            match node.kind {
                Kind::Cache => names[0] = node.name,
                Kind::Service => names[1] = node.name,
                Kind::Resource => names[2] = node.name,
                _ => {}
            }
        }
        for (slot, value) in names[4 - qualifiers.len()..].iter_mut().zip(qualifiers) {
            *slot = value;
        }
        if names.iter().any(|v| v.is_empty()) {
            return Err(
                "Select the missing taxonomy ancestors or qualify cache.service.resource.type"
                    .into(),
            );
        }
        let mut parent = self.root();
        for (kind, name) in [Kind::Cache, Kind::Service, Kind::Resource]
            .into_iter()
            .zip(&names[..3])
        {
            parent = self
                .find(parent, kind, name, "", "")?
                .ok_or_else(|| format!("Unknown taxonomy source {}", names.join(".")))?;
        }
        let mut branches = Vec::new();
        for &version in self.children(parent)? {
            for &branch in self.children(version)? {
                let node = self.node(branch)?;
                if node.resource_type == names[3] {
                    branches.push(node.branch_id.expect("branch node"));
                }
            }
        }
        if branches.is_empty() {
            return Err(format!("Unknown resource type {}", names[3]));
        }
        Ok(branches)
    }
    pub fn section(&self, id: u32) -> Option<&[u8]> {
        self.sections.get(&id).map(|r| &self.bytes[r.clone()])
    }
    pub fn section_ids(&self) -> Vec<u32> {
        let mut ids: Vec<_> = self.sections.keys().copied().collect();
        ids.sort_unstable();
        ids
    }
    pub fn extends(&self, old: &Self) -> bool {
        self.count >= old.count
            && self.bytes[HEADER..HEADER + old.count as usize * NODE]
                == old.bytes[HEADER..HEADER + old.count as usize * NODE]
            && old
                .labels
                .keys()
                .all(|&r| self.label(r).ok() == old.label(r).ok())
    }
    /// Resource choices are version-independent. Resolve from trie nodes,
    /// never from storage filenames or a reconstructed client hierarchy.
    pub fn source_options(&self, scope: &[String], index: usize) -> Result<Vec<String>, Error> {
        if index > 2 {
            return Err("Unknown source level".into());
        }
        let mut parents = vec![self.root()];
        for depth in 0..=index {
            let mut next = Vec::new();
            let mut options = std::collections::BTreeSet::new();
            for parent in parents {
                for &id in self.children(parent)? {
                    let n = self.node(id)?;
                    if depth == index {
                        options.insert(n.name.to_string());
                    } else if scope
                        .get(depth)
                        .is_none_or(|s| s.is_empty() || s == "*" || s == n.name)
                    {
                        next.push(id);
                    }
                }
            }
            if depth == index {
                return Ok(options.into_iter().collect());
            }
            parents = next;
        }
        Ok(vec![])
    }
}
#[cfg(target_arch = "wasm32")]
mod wasm;

#[cfg(feature = "registration")]
mod edit;
#[cfg(feature = "registration")]
pub mod registration;
#[cfg(feature = "registration")]
pub mod base;
#[cfg(feature = "registration")]
pub mod catalog;
#[cfg(feature = "registration")]
pub mod splits;

mod api;
