//! Snapshot editing for registration. Publication/locking remains the caller's
//! responsibility; this never modifies a live file or rewrites old node records.
use super::*;
use std::collections::BTreeMap;

pub(crate) struct Edit<'a> {
    base: Option<&'a Trunk>,
    nodes: Vec<[u32; 5]>,
    names: HashMap<String, u32>,
    added_names: Vec<String>,
    old_directory: &'a [u8],
    directory_len: usize,
    edges: HashMap<(NodeId, u32, [u32; 3]), NodeId>,
    pub sections: BTreeMap<u32, Vec<u8>>,
    leaves: u32,
}
impl Trunk {
    /// Rename a service in a new snapshot, preserving node and Branch IDs.
    /// The publisher must move its storage directory under the same exclusive
    /// ownership used for registration before exposing the new snapshot.
    pub fn rename_service(&self, id: NodeId, name: &str) -> Result<Self, Error> {
        let node = self.node(id)?;
        if node.kind != Kind::Service || name.is_empty() || name == "." || name == ".."
            || name.contains(['/', '\\', '\0']) {
            return Err("Expected a service and a single nonempty path component".into());
        }
        if node.name == name { return Trunk::open(self.bytes.clone()); }
        if self.find(node.parent, Kind::Service, name, "", "")?.is_some() {
            return Err("Service name already exists".into());
        }
        let mut edit = Edit::from(self);
        let label = edit.intern(name)?;
        edit.nodes[id as usize - 1][2] = label;
        edit.finish()
    }

    pub fn new(name: &str) -> Result<Self, Error> {
        let mut edit = Edit::empty();
        edit.node(0, Kind::Trunk, name, "", "")?;
        edit.finish()
    }
}
impl<'a> Edit<'a> {
    fn empty() -> Self {
        Self {
            base: None,
            nodes: vec![],
            names: HashMap::new(),
            added_names: vec![],
            old_directory: &[],
            directory_len: 0,
            edges: HashMap::new(),
            sections: BTreeMap::new(),
            leaves: 0,
        }
    }
    pub fn from(trunk: &'a Trunk) -> Self {
        let n = u64_at(trunk.bytes(), trunk.end - 16).expect("validated directory") as usize;
        Self {
            base: Some(trunk),
            nodes: (1..=trunk.count)
                .map(|id| {
                    let p = trunk.position(id).unwrap();
                    std::array::from_fn(|i| u32_at(trunk.bytes(), p + i * 4).unwrap())
                })
                .collect(),
            names: trunk
                .labels
                .keys()
                .map(|&r| (trunk.label(r).unwrap().into(), r))
                .collect(),
            added_names: vec![],
            old_directory: &trunk.bytes()[trunk.end - 16 - n..trunk.end - 16],
            directory_len: n,
            edges: trunk.edges.clone(),
            sections: trunk
                .section_ids()
                .into_iter()
                .map(|id| (id, trunk.section(id).unwrap().to_vec()))
                .collect(),
            leaves: trunk.leaves.len() as u32 - 1,
        }
    }
    fn intern(&mut self, name: &str) -> Result<u32, Error> {
        if name.is_empty() {
            return Ok(0);
        }
        if let Some(&r) = self.names.get(name) {
            return Ok(r);
        }
        self.directory_len = self
            .directory_len
            .checked_add(name.len())
            .and_then(|n| n.checked_add(4))
            .ok_or("Name directory overflow")?;
        let r = u32::try_from(
            self.directory_len
                .checked_add(16)
                .ok_or("Name directory overflow")?,
        )
        .map_err(|_| "Name directory overflow")?;
        self.names.insert(name.into(), r);
        self.added_names.push(name.into());
        Ok(r)
    }
    pub fn node(
        &mut self,
        parent: NodeId,
        kind: Kind,
        name: &str,
        typ: &str,
        level: &str,
    ) -> Result<NodeId, Error> {
        if kind == Kind::Trunk {
            if parent != 0 || !self.nodes.is_empty() {
                return Err("Taxonomy has one root".into());
            }
        } else if parent == 0
            || self
                .nodes
                .get(parent as usize - 1)
                .is_none_or(|n| n[0] + 1 != kind as u32)
        {
            return Err("Invalid taxonomy parent".into());
        }
        if (kind == Kind::Branch && (!name.is_empty() || typ.is_empty()))
            || (kind != Kind::Branch && (name.is_empty() || !typ.is_empty() || !level.is_empty()))
        {
            return Err("Invalid taxonomy label roles".into());
        }
        let refs = [self.intern(name)?, self.intern(typ)?, self.intern(level)?];
        let edge = (parent, kind as u32, refs);
        if let Some(&id) = self.edges.get(&edge) {
            return Ok(id);
        }
        let id = u32::try_from(self.nodes.len() + 1).map_err(|_| "Taxonomy node limit")?;
        if kind == Kind::Branch {
            if self.leaves == u16::MAX as u32 {
                return Err("Branch ID limit".into());
            }
            self.leaves += 1;
        }
        self.nodes
            .push([kind as u32, parent, refs[0], refs[1], refs[2]]);
        self.edges.insert(edge, id);
        Ok(id)
    }
    pub fn branch_id(&self, node: NodeId) -> Result<BranchId, Error> {
        if node == 0
            || self
                .nodes
                .get(node as usize - 1)
                .is_none_or(|n| n[0] != Kind::Branch as u32)
        {
            return Err("Not a branch node".into());
        }
        Ok(self.nodes[..node as usize]
            .iter()
            .filter(|n| n[0] == Kind::Branch as u32)
            .count() as u16)
    }
    pub fn finish(self) -> Result<Trunk, Error> {
        let mut bytes = vec![0; HEADER];
        bytes[..8].copy_from_slice(b"TTTAX000");
        for node in &self.nodes {
            for v in node {
                bytes.extend_from_slice(&v.to_le_bytes());
            }
        }
        let sections_start = bytes.len();
        let mut entries = Vec::new();
        for (&id, payload) in &self.sections {
            entries.push((id, crc32c::crc32c(payload), bytes.len(), payload.len()));
            bytes.extend_from_slice(payload);
        }
        let section_end = bytes
            .len()
            .checked_add(
                entries
                    .len()
                    .checked_mul(24)
                    .ok_or("Section directory overflow")?,
            )
            .and_then(|n| n.checked_add(16))
            .ok_or("Section directory overflow")?;
        for (id, crc, start, len) in &entries {
            bytes.extend_from_slice(&id.to_le_bytes());
            bytes.extend_from_slice(&crc.to_le_bytes());
            bytes.extend_from_slice(&((section_end - start) as u64).to_le_bytes());
            bytes.extend_from_slice(&(*len as u64).to_le_bytes());
        }
        bytes.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        bytes.extend_from_slice(b"TTSECT00");
        debug_assert_eq!(bytes.len(), section_end);
        debug_assert!(section_end >= sections_start);
        for name in self.added_names.iter().rev() {
            bytes.extend_from_slice(name.as_bytes());
            bytes.extend_from_slice(&(name.len() as u32).to_le_bytes());
        }
        bytes.extend_from_slice(self.old_directory);
        bytes.extend_from_slice(&(self.directory_len as u64).to_le_bytes());
        bytes.extend_from_slice(b"TTDIR000");
        if let Some(old) = self.base {
            if old.bytes()[HEADER..] == bytes[HEADER..] {
                return Trunk::open(old.bytes.clone());
            }
        }
        let generation = self
            .base
            .map_or(Some(1), |t| t.generation.checked_add(1))
            .ok_or("Generation overflow")?;
        let end = bytes.len() as u64;
        bytes[8..16].copy_from_slice(&generation.to_le_bytes());
        bytes[16..20].copy_from_slice(&(self.nodes.len() as u32).to_le_bytes());
        bytes[20..24].copy_from_slice(&self.leaves.to_le_bytes());
        bytes[24..32].copy_from_slice(&end.to_le_bytes());
        let crc = crc32c::crc32c(&bytes[8..32]);
        bytes[32..36].copy_from_slice(&crc.to_le_bytes());
        Trunk::open(bytes)
    }
}
