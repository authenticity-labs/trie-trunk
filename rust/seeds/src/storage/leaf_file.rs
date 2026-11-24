//! Contiguous version roots. Each root declares its Definition once; the
//! replaceable footer indexes immutable node frames without changing addresses.
use super::leaf::{LeafFileBlock, LeafFileLayout, LeafTrie};
use crate::{Error, SeedDefinition, error};
use ciborium::Value as Data;
use std::{collections::{HashSet, BTreeMap}, sync::Arc};

const HEADER: usize = 72;
const MARKER: &[u8; 8] = b"\0\0\0\0STAT";
const TRAILER: usize = 24;

#[derive(Clone)]
pub struct LeafRoot {
    pub offset: usize,
    pub definition: Vec<u8>,
    pub frozen: bool,
    pub statistics: Data,
    pub labels: BTreeMap<u16, String>,
    pub blocks: Vec<LeafFileBlock>,
}
impl LeafRoot {
    pub fn node_count(&self) -> usize {
        1 + self.blocks.iter().map(|b| b.count).sum::<usize>()
    }
}

/// Borrows the file. Opening the directory never decodes another root's nodes.
pub struct LeafFile<'a> {
    bytes: &'a [u8],
    pub nodes_end: usize,
    pub roots: Vec<LeafRoot>,
}
impl<'a> LeafFile<'a> {
    pub(crate) fn open(bytes: &'a [u8], verify_write: bool) -> Result<Self, Error> {
        if bytes.len() < HEADER + 8 + TRAILER
            || &bytes[..8] != b"NUTSTRI6"
            || bytes[8..HEADER].iter().any(|&b| b != 0)
        {
            return Err(error("invalid leaf forest header"));
        }
        let trailer_at = bytes.len() - TRAILER;
        let trailer = &bytes[trailer_at..];
        let nodes_end = usize::try_from(u64::from_le_bytes(trailer[..8].try_into().unwrap()))
            .map_err(|_| Error::Overflow)?;
        let length = u32::from_le_bytes(trailer[8..12].try_into().unwrap()) as usize;
        let payload_at = nodes_end.checked_add(8).ok_or(Error::Overflow)?;
        if &trailer[16..] != b"TTSTAT00"
            || nodes_end < HEADER
            || bytes.get(nodes_end..payload_at) != Some(MARKER.as_slice())
            || length == 0
            || payload_at.checked_add(length) != Some(trailer_at)
        {
            return Err(error("invalid leaf footer bounds"));
        }
        let payload = &bytes[payload_at..trailer_at];
        if verify_write
            && crc32c::crc32c(payload) != u32::from_le_bytes(trailer[12..16].try_into().unwrap())
        {
            return Err(error("leaf footer checksum mismatch"));
        }
        let Data::Map(properties) = pil_codec::object::decode_value(payload)? else {
            return Err(error("invalid leaf footer"));
        };
        let mut root_values = None;
        let mut label_values = None;
        for (key, value) in properties {
            match key.as_text() {
                Some("roots") if root_values.is_none() => root_values = Some(value),
                Some("labels") if label_values.is_none() => label_values = Some(value),
                _ => return Err(error("unknown or repeated leaf footer property")),
            }
        }
        let Some(Data::Array(entries)) = root_values else {
            return Err(error("missing leaf roots"));
        };
        let mut roots = Vec::with_capacity(entries.len());
        let mut definitions = HashSet::new();
        let mut end = HEADER;
        for entry in entries {
            let Data::Array(parts) = entry else {
                return Err(error("invalid leaf root"));
            };
            let [offset, Data::Bool(frozen), Data::Array(blocks), statistics] = parts.as_slice()
            else {
                return Err(error("invalid leaf root properties"));
            };
            let offset = number(offset)?;
            if offset != end {
                return Err(error("noncontiguous leaf version root"));
            }
            let (payload, root_end) = frame(bytes, offset, nodes_end, verify_write)?;
            let definition = root_definition(payload)?;
            end = root_end;
            if definition.len() != 32 || !definitions.insert(definition.clone()) {
                return Err(error("invalid or repeated leaf Definition"));
            }
            let mut root = LeafRoot {
                offset,
                definition: definition.clone(),
                frozen: *frozen,
                statistics: statistics.clone(),
                labels: BTreeMap::new(),
                blocks: Vec::with_capacity(blocks.len()),
            };
            let mut first = 1usize;
            for value in blocks {
                let Data::Array(parts) = value else {
                    return Err(error("invalid leaf block descriptor"));
                };
                let [start, count, local, length] = parts.as_slice() else {
                    return Err(error("invalid leaf block properties"));
                };
                let (start, count, local, length) = (
                    number(start)?,
                    number(count)?,
                    number(local)?,
                    number(length)?,
                );
                let block_end = local.checked_add(length).ok_or(Error::Overflow)?;
                if start != first
                    || count == 0
                    || first.checked_add(count).is_none_or(|n| n > 65536)
                    || local < HEADER + 8
                    || block_end > nodes_end
                    || local.checked_sub(8) != Some(end)
                    || length == 0
                    || length > u32::MAX as usize
                {
                    return Err(error("invalid leaf block bounds"));
                }
                let frame = &bytes[local - 8..local];
                if u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize != length {
                    return Err(error("leaf block length mismatch"));
                }
                // Integrity is checked before publication, never during reads.
                if verify_write
                    && crc32c::crc32c(&bytes[local..block_end])
                        != u32::from_le_bytes(frame[4..].try_into().unwrap())
                {
                    return Err(error("leaf block checksum mismatch"));
                }
                end = block_end;
                root.blocks.push(LeafFileBlock {
                    start,
                    count,
                    local,
                    length,
                });
                first += count;
            }
            roots.push(root);
        }
        if let Some(labels) = label_values {
            let Data::Array(entries) = labels else { return Err(error("invalid leaf labels")); };
            let mut seen = HashSet::new();
            for entry in entries {
                let Data::Array(parts) = entry else { return Err(error("invalid leaf labels")); };
                let [Data::Bytes(digest), Data::Map(labels)] = parts.as_slice() else { return Err(error("invalid leaf labels")); };
                if !seen.insert(digest.clone()) { return Err(error("repeated leaf label root")); }
                let root = roots.iter_mut().find(|r| r.definition == *digest).ok_or_else(|| error("unknown leaf label root"))?;
                for (id, label) in labels {
                    let id = u16::try_from(number(id)?).map_err(|_| Error::Overflow)?;
                    let Data::Text(label) = label else { return Err(error("invalid leaf label")); };
                    check_label(root, id, label)?;
                    if root.labels.insert(id, label.clone()).is_some() { return Err(error("repeated leaf label")); }
                }
            }
        }
        if roots.iter().rev().skip(1).any(|r| !r.frozen) {
            return Err(error("only the final leaf root may remain active"));
        }
        if end != nodes_end {
            return Err(error("unreferenced leaf bytes"));
        }
        Ok(Self {
            bytes,
            nodes_end,
            roots,
        })
    }
    /// Rebuild the directory from intact framed roots and node batches. This
    /// is an explicit recovery operation; normal opens use the footer only.
    pub(crate) fn recover(bytes: &'a [u8]) -> Result<Self, Error> {
        if bytes.len() < HEADER
            || &bytes[..8] != b"NUTSTRI6"
            || bytes[8..HEADER].iter().any(|&b| b != 0)
        {
            return Err(error("invalid leaf forest header"));
        }
        let mut roots: Vec<LeafRoot> = Vec::new();
        let mut definitions = HashSet::new();
        let mut end = HEADER;
        while end < bytes.len() {
            if bytes.get(end..end + 8) == Some(MARKER.as_slice()) {
                break;
            }
            // An incomplete final write is not published. Corruption of a
            // complete frame is an error, rather than silent data loss.
            if bytes.len() - end < 8 {
                break;
            }
            let length = u32::from_le_bytes(bytes[end..end + 4].try_into().unwrap()) as usize;
            if length > bytes.len() - end - 8 {
                break;
            }
            let (payload, next) = frame(bytes, end, bytes.len(), true)?;
            match pil_codec::object::decode_value(payload)? {
                Data::Map(_) => {
                    let definition = root_definition(payload)?;
                    if !definitions.insert(definition.clone()) {
                        return Err(error("repeated leaf Definition"));
                    }
                    if let Some(previous) = roots.last_mut() {
                        previous.frozen = true;
                    }
                    roots.push(LeafRoot {
                        offset: end,
                        definition,
                        frozen: false,
                        statistics: Data::Null,
                        labels: BTreeMap::new(),
                        blocks: Vec::new(),
                    });
                }
                Data::Array(nodes) => {
                    let root = roots
                        .last_mut()
                        .ok_or_else(|| error("leaf batch has no root"))?;
                    let start = root.node_count();
                    if nodes.is_empty() || start.checked_add(nodes.len()).is_none_or(|n| n > 65536)
                    {
                        return Err(error("invalid leaf batch count"));
                    }
                    root.blocks.push(LeafFileBlock {
                        start,
                        count: nodes.len(),
                        local: end + 8,
                        length,
                    });
                }
                _ => return Err(error("invalid leaf frame")),
            }
            end = next;
        }
        Ok(Self {
            bytes,
            nodes_end: end,
            roots,
        })
    }
    pub fn root(&self, definition: &[u8]) -> Result<&LeafRoot, Error> {
        self.roots
            .iter()
            .find(|r| r.definition == definition)
            .ok_or_else(|| error("leaf trie Definition mismatch: missing version root"))
    }
    pub fn trie(&self, definition: Arc<SeedDefinition>) -> Result<LeafTrie, Error> {
        let root = self.root(&definition.digest)?;
        let mut trie = LeafTrie::new(definition);
        for block in &root.blocks {
            trie.append_pil(
                block.start,
                &self.bytes[block.local..block.local + block.length],
            )?;
            if trie.len() + 1 != block.start + block.count {
                return Err(error("leaf block node count mismatch"));
            }
        }
        Ok(trie)
    }
    /// Labels are shared presentation metadata. Node identities and values stay immutable.
    pub fn label(mut self, definition: &[u8], leaf: u16, label: Option<&str>) -> Result<Vec<u8>, Error> {
        let root = self.roots.iter_mut().find(|r| r.definition == definition)
            .ok_or_else(|| error("unknown leaf label root"))?;
        if leaf as usize >= root.node_count() { return Err(error("leaf label outside root")); }
        if let Some(label) = label { check_label(root, leaf, label)?; root.labels.insert(leaf, label.to_owned()); }
        else { root.labels.remove(&leaf); }
        let tail = footer(self.nodes_end, self.roots.iter())?;
        let mut bytes = Vec::with_capacity(self.nodes_end + tail.len());
        bytes.extend_from_slice(&self.bytes[..self.nodes_end]); bytes.extend_from_slice(&tail);
        Ok(bytes)
    }
    pub fn layout(&self, definition: &[u8]) -> Result<LeafFileLayout, Error> {
        let root = self.root(definition)?;
        Ok(LeafFileLayout {
            nodes_end: self.nodes_end,
            node_count: root.node_count(),
            blocks: root.blocks.clone(),
        })
    }
}
fn check_label(root: &LeafRoot, leaf: u16, label: &str) -> Result<(), Error> {
    if leaf as usize >= root.node_count() || label.trim().is_empty() || label.len() > 1024 || label.chars().any(char::is_control) {
        return Err(error("invalid leaf label"));
    }
    Ok(())
}
fn frame(bytes: &[u8], offset: usize, limit: usize, verify: bool) -> Result<(&[u8], usize), Error> {
    let local = offset.checked_add(8).ok_or(Error::Overflow)?;
    let header = bytes.get(offset..local).ok_or(Error::Incomplete)?;
    let length = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
    let end = local.checked_add(length).ok_or(Error::Overflow)?;
    if end > limit || length == 0 {
        return Err(error("invalid leaf frame bounds"));
    }
    let payload = bytes.get(local..end).ok_or(Error::Incomplete)?;
    if verify && crc32c::crc32c(payload) != u32::from_le_bytes(header[4..].try_into().unwrap()) {
        return Err(error("leaf frame checksum mismatch"));
    }
    Ok((payload, end))
}
fn root_definition(payload: &[u8]) -> Result<Vec<u8>, Error> {
    match pil_codec::object::decode_value(payload)? {
        Data::Map(properties) if properties.len() == 1 => match &properties[0] {
            (Data::Text(key), Data::Bytes(digest)) if key == "definition" && digest.len() == 32 => {
                Ok(digest.clone())
            }
            _ => Err(error("invalid leaf root declaration")),
        },
        _ => Err(error("invalid leaf root declaration")),
    }
}
fn append_frame(out: &mut Vec<u8>, payload: &[u8]) -> Result<(), Error> {
    let length = u32::try_from(payload.len()).map_err(|_| Error::Overflow)?;
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(payload).to_le_bytes());
    out.extend_from_slice(payload);
    Ok(())
}
fn number(value: &Data) -> Result<usize, Error> {
    match value {
        Data::Integer(n) => usize::try_from(i128::from(*n)).map_err(|_| Error::Overflow),
        _ => Err(error("invalid leaf directory integer")),
    }
}

/// Publishes node additions plus one footer. The owner controls disk writes and
/// retires an old root when its replacement becomes active.
pub struct LeafFileBuilder {
    nodes: Vec<u8>,
    roots: Vec<(LeafRoot, LeafTrie)>,
    identity: Arc<()>,
    revision: u64,
}
impl Default for LeafFileBuilder {
    fn default() -> Self {
        Self::new()
    }
}
impl LeafFileBuilder {
    pub fn new() -> Self {
        let mut nodes = vec![0; HEADER];
        nodes[..8].copy_from_slice(b"NUTSTRI6");
        Self {
            nodes,
            roots: vec![],
            identity: Arc::new(()),
            revision: 0,
        }
    }
    pub fn resume(
        bytes: &[u8],
        resolve: impl FnMut(&[u8]) -> Result<Arc<SeedDefinition>, Error>,
    ) -> Result<Self, Error> {
        let file = LeafFile::open(bytes, false)?;
        Self::from_file(file, resolve)
    }
    pub(crate) fn recover(
        bytes: &[u8],
        resolve: impl FnMut(&[u8]) -> Result<Arc<SeedDefinition>, Error>,
    ) -> Result<Self, Error> {
        Self::from_file(LeafFile::recover(bytes)?, resolve)
    }
    fn from_file(
        file: LeafFile<'_>,
        mut resolve: impl FnMut(&[u8]) -> Result<Arc<SeedDefinition>, Error>,
    ) -> Result<Self, Error> {
        let mut roots = Vec::with_capacity(file.roots.len());
        for root in &file.roots {
            let definition = resolve(&root.definition)?;
            if definition.digest != root.definition {
                return Err(error("leaf root resolved to wrong Definition"));
            }
            roots.push((root.clone(), file.trie(definition)?));
        }
        Ok(Self {
            nodes: file.bytes[..file.nodes_end].to_vec(),
            roots,
            identity: Arc::new(()),
            revision: 0,
        })
    }
    pub fn register(&mut self, definition: Arc<SeedDefinition>) -> Result<(), Error> {
        if definition.digest.len() != 32 {
            return Err(error("unsigned leaf Definition"));
        }
        if self
            .roots
            .iter()
            .any(|(r, _)| r.definition == definition.digest)
        {
            return Ok(());
        }
        let declaration = pil_codec::object::encode_value(&Data::Map(vec![(
            "definition".into(),
            Data::Bytes(definition.digest.clone()),
        )]))?;
        let offset = self.nodes.len();
        append_frame(&mut self.nodes, &declaration)?;
        if let Some((previous, _)) = self.roots.last_mut() {
            previous.frozen = true;
        }
        self.revision = self.revision.wrapping_add(1);
        self.roots.push((
            LeafRoot {
                offset,
                definition: definition.digest.clone(),
                frozen: false,
                statistics: Data::Null,
                        labels: BTreeMap::new(),
                blocks: vec![],
            },
            LeafTrie::new(definition),
        ));
        Ok(())
    }
    pub fn freeze(&mut self, definition: &SeedDefinition) -> Result<(), Error> {
        let (root, _) = self
            .roots
            .iter_mut()
            .find(|(r, _)| r.definition == definition.digest)
            .ok_or_else(|| error("unknown leaf root"))?;
        root.frozen = true;
        self.revision = self.revision.wrapping_add(1);
        Ok(())
    }
    pub fn append(&mut self, trie: &LeafTrie) -> Result<(), Error> {
        let (root, previous) = self
            .roots
            .iter_mut()
            .find(|(r, _)| r.definition == trie.definition.digest)
            .ok_or_else(|| error("register leaf root before appending"))?;
        if !trie.nodes.starts_with(&previous.nodes) {
            return Err(error("immutable leaf nodes changed"));
        }
        if trie.len() == previous.len() {
            return Ok(());
        }
        if root.frozen {
            return Err(error("leaf version root is frozen"));
        }
        let raw = trie.encode_pil(previous.len() + 1)?;
        let local = self.nodes.len().checked_add(8).ok_or(Error::Overflow)?;
        let block = LeafFileBlock {
            start: previous.len() + 1,
            count: trie.len() - previous.len(),
            local,
            length: raw.len(),
        };
        append_frame(&mut self.nodes, &raw)?;
        root.blocks.push(block);
        *previous = trie.clone();
        self.revision = self.revision.wrapping_add(1);
        Ok(())
    }
    pub fn statistics(&mut self, definition: &SeedDefinition, value: Data) -> Result<(), Error> {
        let (root, _) = self
            .roots
            .iter_mut()
            .find(|(r, _)| r.definition == definition.digest)
            .ok_or_else(|| error("unknown leaf root"))?;
        root.statistics = value;
        self.revision = self.revision.wrapping_add(1);
        Ok(())
    }
    /// Prepare only the replaceable tail. No existing node bytes are copied,
    /// changed, or made visible until the storage owner acknowledges the write.
    pub(crate) fn prepare(&self, trie: LeafTrie) -> Result<LeafCommit, Error> {
        let definition = &trie.definition;
        if definition.digest.len() != 32 {
            return Err(error("unsigned leaf Definition"));
        }
        let existing = self
            .roots
            .iter()
            .position(|(r, _)| r.definition == definition.digest);
        let mut roots: Vec<LeafRoot> = self.roots.iter().map(|(r, _)| r.clone()).collect();
        let mut added = Vec::new();
        let previous_count;
        let root_index;
        if let Some(index) = existing {
            if index + 1 != roots.len() || roots[index].frozen {
                return Err(error("leaf version root is frozen"));
            }
            let previous = &self.roots[index].1;
            if !trie.nodes.starts_with(&previous.nodes) {
                return Err(error("immutable leaf nodes changed"));
            }
            previous_count = previous.len();
            root_index = index;
        } else {
            // A version handoff is append-only; a new root cannot reopen an
            // older version after a later one has started committing.
            if let Some((_, previous)) = self.roots.last() {
                if definition.version <= previous.definition.version {
                    return Err(error("leaf handoff must advance the Definition version"));
                }
            }
            let declaration = pil_codec::object::encode_value(&Data::Map(vec![(
                "definition".into(),
                Data::Bytes(definition.digest.clone()),
            )]))?;
            append_frame(&mut added, &declaration)?;
            if let Some(previous) = roots.last_mut() {
                previous.frozen = true;
            }
            root_index = roots.len();
            roots.push(LeafRoot {
                offset: self.nodes.len(),
                definition: definition.digest.clone(),
                frozen: false,
                statistics: Data::Null,
                        labels: BTreeMap::new(),
                blocks: vec![],
            });
            previous_count = 0;
        }
        if trie.len() > previous_count {
            let raw = trie.encode_pil(previous_count + 1)?;
            let local = self
                .nodes
                .len()
                .checked_add(added.len())
                .and_then(|n| n.checked_add(8))
                .ok_or(Error::Overflow)?;
            append_frame(&mut added, &raw)?;
            roots[root_index].blocks.push(LeafFileBlock {
                start: previous_count + 1,
                count: trie.len() - previous_count,
                local,
                length: raw.len(),
            });
        }
        let nodes_end = self
            .nodes
            .len()
            .checked_add(added.len())
            .ok_or(Error::Overflow)?;
        let footer = footer(nodes_end, roots.iter())?;
        let offset = if self.roots.is_empty() {
            0
        } else {
            self.nodes.len()
        };
        let mut tail = Vec::with_capacity(self.nodes.len() - offset + added.len() + footer.len());
        tail.extend_from_slice(&self.nodes[offset..]);
        tail.extend_from_slice(&added);
        let node_tail_length = tail.len();
        tail.extend_from_slice(&footer);
        Ok(LeafCommit {
            offset,
            tail,
            node_tail_length,
            identity: self.identity.clone(),
            revision: self.revision,
            roots,
            trie,
        })
    }

    pub(crate) fn accept(&mut self, commit: LeafCommit) -> Result<(), Error> {
        if !Arc::ptr_eq(&self.identity, &commit.identity) || self.revision != commit.revision {
            return Err(error("stale leaf commit"));
        }
        self.revision = self.revision.wrapping_add(1);
        let skip = self.nodes.len() - commit.offset;
        self.nodes
            .extend_from_slice(&commit.tail[skip..commit.node_tail_length]);
        let index = commit.roots.len() - 1;
        for ((root, _), next) in self.roots.iter_mut().zip(&commit.roots) {
            *root = next.clone();
        }
        if index == self.roots.len() {
            self.roots.push((commit.roots[index].clone(), commit.trie));
        } else {
            self.roots[index].1 = commit.trie;
        }
        Ok(())
    }

    pub(crate) fn nodes(&self, definition: Arc<SeedDefinition>) -> LeafTrie {
        self.roots
            .iter()
            .find(|(root, _)| root.definition == definition.digest)
            .map(|(_, trie)| trie.clone())
            .unwrap_or_else(|| LeafTrie::new(definition))
    }

    pub fn publish(&self) -> Result<Vec<u8>, Error> {
        let footer = footer(self.nodes.len(), self.roots.iter().map(|(root, _)| root))?;
        let mut out = Vec::with_capacity(self.nodes.len() + footer.len());
        out.extend_from_slice(&self.nodes);
        out.extend_from_slice(&footer);
        Ok(out)
    }
}

/// A staged root handoff and its node/footer tail. Dropping it leaves the
/// published writer unchanged. The owner must durably write before accepting.
pub struct LeafCommit {
    offset: usize,
    tail: Vec<u8>,
    node_tail_length: usize,
    identity: Arc<()>,
    revision: u64,
    roots: Vec<LeafRoot>,
    trie: LeafTrie,
}
impl LeafCommit {
    pub fn offset(&self) -> usize {
        self.offset
    }
    pub fn bytes(&self) -> &[u8] {
        &self.tail
    }
    pub fn file_length(&self) -> usize {
        self.offset + self.tail.len()
    }
}

fn footer<'a>(
    nodes_end: usize,
    roots: impl Iterator<Item = &'a LeafRoot>,
) -> Result<Vec<u8>, Error> {
    let roots: Vec<_> = roots.collect();
    let labels = roots.iter().filter(|r| !r.labels.is_empty()).map(|r| Data::Array(vec![Data::Bytes(r.definition.clone()), Data::Map(r.labels.iter().map(|(&id, label)| (Data::Integer(u64::from(id).into()), Data::Text(label.clone()))).collect())])).collect::<Vec<_>>();
    let roots = roots.into_iter()
        .map(|root| {
            Data::Array(vec![
                Data::Integer((root.offset as u64).into()),
                Data::Bool(root.frozen),
                Data::Array(
                    root.blocks
                        .iter()
                        .map(|b| {
                            Data::Array(
                                [b.start, b.count, b.local, b.length]
                                    .into_iter()
                                    .map(|n| Data::Integer((n as u64).into()))
                                    .collect(),
                            )
                        })
                        .collect(),
                ),
                root.statistics.clone(),
            ])
        })
        .collect();
    let mut properties = vec![("roots".into(), Data::Array(roots))];
    if !labels.is_empty() { properties.push(("labels".into(), Data::Array(labels))); }
    let payload = pil_codec::object::encode_value(&Data::Map(properties))?;
    let length = u32::try_from(payload.len()).map_err(|_| Error::Overflow)?;
    let mut out = Vec::with_capacity(8 + payload.len() + TRAILER);
    out.extend_from_slice(MARKER);
    out.extend_from_slice(&payload);
    out.extend_from_slice(&(nodes_end as u64).to_le_bytes());
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
    out.extend_from_slice(b"TTSTAT00");
    Ok(out)
}
