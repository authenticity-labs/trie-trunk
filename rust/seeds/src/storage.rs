//! Logical fields restored from a Pack's immutable index/state snapshot.
//! Row identity is the existing member position, never an encoded Seed field.
pub(crate) mod index;
mod leaf;
mod time;
pub use index::{HourIndex, HourIndexBuilder, IndexFields};
pub use leaf::{LeafFileBlock, LeafFileLayout, LeafIndex, LeafTrie};
pub use time::{TimeHeader, TimeLeaf, TimeRange, TimeTrie, TIME_HEADER_BYTES, TIME_HOUR_MS};

use crate::{Error, SeedPackSnapshot, error};
use pil_codec::api::{NativeValue, PillView};
use std::sync::Arc;

/// Supplied by the storage owner after index dependencies are available.
/// Values use the registered Definition's ordinals and declared types. `None`
/// delegates to the Pill; `Some(Absent)` and `Some(Null)` remain distinct.
/// Implementations retain their index/state owners and must be immutable.
pub trait StorageBinding: Send + Sync {
    /// Optional per-reader state. Shared immutable bindings use the default;
    /// file-backed bindings supply independent read windows without row locks.
    fn cursor(&self) -> Option<Box<dyn StorageCursor>> { None }

    /// Check once when binding a Pack, not once per accessed field.
    fn check_definition(&self, _definition: &Arc<crate::SeedDefinition>) -> Result<(), Error> { Ok(()) }

    fn row_count(&self) -> usize;
    fn value(&self, row: usize, field: usize) -> Result<Option<NativeValue<'_>>, Error>;
    /// Trie-restored values cannot coexist with a local assignment. Checked
    /// only when the field is accessed, just like other addressing checks.
    fn requires_absent(&self, _row: usize, _field: usize) -> Result<bool, Error> {
        Ok(false)
    }
}

/// Reader-local storage access. Values may borrow immutable Leaves data, while
/// the cursor's index window is independent of every other query/view reader.
pub trait StorageCursor: Send {
    fn value(&self, row: usize, field: usize) -> Result<Option<NativeValue<'_>>, Error>;
    fn requires_absent(&self, _row: usize, _field: usize) -> Result<bool, Error> { Ok(false) }
}
pub(crate) enum BindingReader {
    Shared(Arc<dyn StorageBinding>),
    Cursor(Box<dyn StorageCursor>),
}
impl BindingReader {
    pub(crate) fn new(binding: &Arc<dyn StorageBinding>) -> Self {
        match binding.cursor() { Some(cursor) => Self::Cursor(cursor), None => Self::Shared(binding.clone()) }
    }
    pub(crate) fn value(&self, row: usize, field: usize) -> Result<Option<NativeValue<'_>>, Error> {
        match self { Self::Shared(b) => b.value(row,field), Self::Cursor(c) => c.value(row,field) }
    }
    fn requires_absent(&self, row: usize, field: usize) -> Result<bool, Error> {
        match self { Self::Shared(b) => b.requires_absent(row,field), Self::Cursor(c) => c.requires_absent(row,field) }
    }
}

impl SeedPackSnapshot {
    /// Attach an index snapshot without copying or re-encoding the Pack.
    pub fn with_storage(
        self: &Arc<Self>,
        storage: Arc<dyn StorageBinding>,
    ) -> Result<Arc<Self>, Error> {
        for definition in self.definitions() { storage.check_definition(definition)?; }
        if storage.row_count() != self.len() {
            return Err(error("storage row count differs from Pack member count"));
        }
        Ok(Arc::new(Self {
            definition: self.definition.clone(),
            lineage: self.lineage.clone(),
            pack: self.pack.clone(),
            byte_length: self.byte_length,
            allocation: self.allocation,
            storage: Some(storage),
            memory: self.memory.clone(),
            write_checked: std::sync::atomic::AtomicBool::new(self.write_checked.load(std::sync::atomic::Ordering::Relaxed)),
        }))
    }
}

pub(crate) struct BoundRow<'a> {
    pub row: PillView<'a>,
    pub ordinal: usize,
    pub storage: Option<&'a BindingReader>,
}
impl BoundRow<'_> {
    pub fn native_value(&self, field: usize) -> Result<NativeValue<'_>, Error> {
        if let Some(storage) = self.storage {
            if let Some(value) = storage.value(self.ordinal, field)? {
                if storage.requires_absent(self.ordinal, field)? && self.row.is_present(field)? {
                    return Err(error("payload shadows shared reference"));
                }
                return Ok(value);
            }
        }
        self.row.native_value(field)
    }
    pub fn is_present(&self, field: usize) -> Result<bool, Error> {
        if let Some(storage) = self.storage {
            if let Some(value) = storage.value(self.ordinal, field)? {
                if storage.requires_absent(self.ordinal, field)? && self.row.is_present(field)? {
                    return Err(error("payload shadows shared reference"));
                }
                return Ok(!matches!(
                    value,
                    NativeValue::Scalar(pil_codec::Value::Absent)
                ));
            }
        }
        self.row.is_present(field)
    }
}

mod leaf_file;
pub use leaf_file::{LeafCommit, LeafFile, LeafRoot};
pub(crate) use leaf_file::LeafFileBuilder;

/// Exception-only dispatch over the existing immutable index bindings. The
/// row ordinal stays global within the hour; only its Definition plan changes.
pub(crate) struct VersionBindings {
    pub versions: Arc<pil_codec::pac::versions::Directory>,
    pub bindings: std::collections::BTreeMap<u64, Arc<dyn StorageBinding>>,
}
impl StorageBinding for VersionBindings {
    fn check_definition(&self, definition: &Arc<crate::SeedDefinition>) -> Result<(), Error> {
        if self.bindings.values().any(|b| b.check_definition(definition).is_ok()) { Ok(()) }
        else { Err(error("index has no binding for the record Definition")) }
    }
    fn row_count(&self) -> usize { self.versions.count() as usize }
    fn value(&self, row: usize, field: usize) -> Result<Option<NativeValue<'_>>, Error> {
        let range = self.versions.range(row as u64).ok_or_else(|| error("index row outside Pack"))?;
        self.bindings[&range.definition].value(row, field)
    }
    fn requires_absent(&self, row: usize, field: usize) -> Result<bool, Error> {
        let range = self.versions.range(row as u64).ok_or_else(|| error("index row outside Pack"))?;
        self.bindings[&range.definition].requires_absent(row, field)
    }
    fn cursor(&self) -> Option<Box<dyn StorageCursor>> {
        Some(Box::new(VersionCursor { versions: self.versions.clone(),
            bindings: self.bindings.iter().map(|(id, b)| (*id, BindingReader::new(b))).collect() }))
    }
}
struct VersionCursor {
    versions: Arc<pil_codec::pac::versions::Directory>,
    bindings: std::collections::BTreeMap<u64, BindingReader>,
}
impl StorageCursor for VersionCursor {
    fn value(&self, row: usize, field: usize) -> Result<Option<NativeValue<'_>>, Error> {
        let range = self.versions.range(row as u64).ok_or_else(|| error("index row outside Pack"))?;
        self.bindings[&range.definition].value(row, field)
    }
    fn requires_absent(&self, row: usize, field: usize) -> Result<bool, Error> {
        let range = self.versions.range(row as u64).ok_or_else(|| error("index row outside Pack"))?;
        self.bindings[&range.definition].requires_absent(row, field)
    }
}
