//! Immutable state paths addressed by the existing two-byte leaf identity.
//! PIL value interpretation stays in Pill Packer; this module owns only paths.
use crate::{Error, HourIndex, SeedDefinition, StorageBinding, error};
use ciborium::Value as Data;
use pil_codec::api::NativeValue;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct LeafFileBlock {
    pub start: usize,
    pub count: usize,
    pub local: usize,
    pub length: usize,
}
#[derive(Debug)]
pub struct LeafFileLayout {
    pub nodes_end: usize,
    pub node_count: usize,
    pub blocks: Vec<LeafFileBlock>,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct Node {
    parent: u16,
    field: u16,
    bytes: Arc<[u8]>,
}

#[derive(Clone)]
pub struct LeafTrie {
    pub(super) definition: Arc<SeedDefinition>,
    fields: std::collections::HashSet<u16>,
    // Identity zero is the empty root, represented without a stored node.
    pub(super) nodes: Vec<Node>,
}

impl LeafTrie {
    pub(crate) fn definition_digest(&self) -> &[u8] { &self.definition.digest }

    pub fn new(definition: Arc<SeedDefinition>) -> Self {
        Self {
            definition,
            nodes: Vec::new(),
            fields: Default::default(),
        }
    }

    pub(crate) fn has_field(&self, field: usize) -> bool { u16::try_from(field).is_ok_and(|f|self.fields.contains(&f)) }
    pub(crate) fn edges(&self) -> impl DoubleEndedIterator<Item = (u16, u16, Arc<[u8]>, u16)> + '_ {
        self.nodes.iter().enumerate().map(|(i,n)| (n.parent, n.field, n.bytes.clone(), (i+1) as u16))
    }
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    pub fn matches_definition(&self, definition: &Arc<SeedDefinition>) -> bool {
        Arc::ptr_eq(&self.definition, definition)
            || self.definition.digest == definition.digest
    }

    /// Restore the next registered node. Bytes are direct field values, not
    /// adaptive slices or masks copied from a Pill. Parent-before-child order
    /// gives stable identities and prevents cycles without a separate graph.
    pub fn append(&mut self, parent: u16, field: u16, bytes: Vec<u8>) -> Result<u16, Error> {
        if self.nodes.len() == u16::MAX as usize || parent as usize > self.nodes.len() {
            return Err(error("invalid state parent"));
        }
        if self.find(parent, field as usize)?.is_some() {
            return Err(error("state path repeats field"));
        }
        // Structural addressing is checked here. Type validation remains
        // selective in read_field(), or explicit in validate().
        if field as usize >= self.definition.prescription().definition().fields().len() {
            return Err(error("state field outside Definition"));
        }
        self.fields.insert(field);
        self.nodes.push(Node {
            parent,
            field,
            bytes: bytes.into(),
        });
        Ok(self.nodes.len() as u16)
    }

    /// Existing node batches are PIL arrays of {"0": parent, "1": field, "2": bytes}.
    /// The shared object codec reads that representation; no PIL layout is
    /// interpreted here. The initial batch includes the empty root at zero.
    pub fn append_pil(&mut self, first: usize, bytes: &[u8]) -> Result<(), Error> {
        let Data::Array(nodes) = pil_codec::object::decode_value(bytes)? else {
            return Err(error("state batch must be an array"));
        };
        if first == 0 && nodes.is_empty() {
            return Err(error("missing state root"));
        }
        if (first == 0 && !self.nodes.is_empty()) || (first != 0 && first != self.nodes.len() + 1) {
            return Err(error("state batch is not contiguous"));
        }
        let start = self.nodes.len();
        let result = (|| {
            for (i, value) in nodes.into_iter().enumerate() {
                let Data::Map(entries) = value else {
                    return Err(error("invalid state node"));
                };
                let (mut parent, mut field, mut bytes) = (None, None, None);
                for (key, value) in entries {
                    match key.as_text() {
                        Some("0") if parent.is_none() => parent = Some(node_uint(value)?),
                        Some("1") if field.is_none() => field = Some(node_uint(value)?),
                        Some("2") if bytes.is_none() => {
                            let Data::Bytes(raw) = value else {
                                return Err(error("invalid state bytes"));
                            };
                            bytes = Some(raw);
                        }
                        _ => return Err(error("invalid state node property")),
                    }
                }
                let (Some(parent), Some(field), Some(bytes)) = (parent, field, bytes) else {
                    return Err(error("incomplete state node"));
                };
                if first == 0 && i == 0 {
                    if parent != 0 || field != 0 || !bytes.is_empty() {
                        return Err(error("invalid state root"));
                    }
                } else {
                    self.append(parent, field, bytes)?;
                }
            }
            Ok(())
        })();
        if result.is_err() {
            self.nodes.truncate(start);
            self.fields=self.nodes.iter().map(|n|n.field).collect();
        }
        result
    }

    pub fn encode_pil(&self, first: usize) -> Result<Vec<u8>, Error> {
        if first > self.nodes.len() + 1 {
            return Err(error("state batch starts beyond trie"));
        }
        let mut nodes = Vec::with_capacity(self.nodes.len() + 1 - first);
        let node = |parent: u16, field: u16, bytes: &[u8]| {
            Data::Map(vec![
                ("0".into(), parent.into()),
                ("1".into(), field.into()),
                ("2".into(), Data::Bytes(bytes.to_vec())),
            ])
        };
        if first == 0 {
            nodes.push(node(0, 0, &[]));
        }
        for n in &self.nodes[first.max(1) - 1..] {
            nodes.push(node(n.parent, n.field, &n.bytes));
        }
        pil_codec::object::encode_value(&Data::Array(nodes))
    }

    fn find(&self, mut leaf: u16, field: usize) -> Result<Option<&Node>, Error> {
        while leaf != 0 {
            let node = self
                .nodes
                .get(leaf as usize - 1)
                .ok_or_else(|| error("unknown state leaf"))?;
            if node.field as usize == field {
                return Ok(Some(node));
            }
            leaf = node.parent;
        }
        Ok(None)
    }

    pub(crate) fn covers(&self,leaf:u16,field:usize)->Result<bool,Error>{Ok(self.find(leaf,field)?.is_some())}
    pub fn value(&self, leaf: u16, field: usize) -> Result<Option<NativeValue<'_>>, Error> {
        self.find(leaf, field)?
            .map(|node| {
                self.definition
                    .prescription()
                    .read_field(field, &node.bytes)
            })
            .transpose()
    }

    pub fn validate(&self) -> Result<(), Error> {
        for node in &self.nodes {
            self.definition
                .prescription()
                .read_field(node.field as usize, &node.bytes)?;
        }
        Ok(())
    }
}

fn node_uint(value: Data) -> Result<u16, Error> {
    value
        .as_integer()
        .and_then(|v| u16::try_from(v).ok())
        .ok_or_else(|| error("invalid state ordinal"))
}

/// One shared trie snapshot and one hour's index; no per-record object cache.
pub struct LeafIndex {
    index: HourIndex,
    trie: Arc<LeafTrie>,
}
impl LeafIndex {
    pub fn new(index: HourIndex, trie: Arc<LeafTrie>) -> Self {
        Self { index, trie }
    }
}
impl StorageBinding for LeafIndex {
    fn check_definition(&self, definition: &Arc<SeedDefinition>) -> Result<(), Error> {
        if !self.trie.matches_definition(definition) { return Err(error("leaf root uses a different Definition version")); }
        Ok(())
    }

    fn row_count(&self) -> usize {
        self.index.row_count()
    }

    fn value(&self, row: usize, field: usize) -> Result<Option<NativeValue<'_>>, Error> {
        if let Some(value) = self.index.value(row, field)? {
            return Ok(Some(value));
        }
        let (_, leaf) = self.index.time_leaf(row)?;
        self.trie.value(leaf, field)
    }

    fn requires_absent(&self, row: usize, field: usize) -> Result<bool, Error> {
        // Called only after value() found a restored value. Do not traverse
        // the path a second time to establish its source.
        Ok(self.index.value(row, field)?.is_none())
    }
}
