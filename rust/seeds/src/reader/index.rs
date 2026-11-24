//! Reuse the existing six-byte hour index and registered Leaves roots. One
//! immutable plan is shared; each reader allocates its own small index window.
use super::{Reader, Storage, read_exact};
use crate::{
    Error, HourIndex, IndexFields, LeafTrie, MemoryBudget, MemoryLease, SeedDefinition,
    StorageBinding, StorageCursor, TimeTrie, error,
};
use pil_codec::api::NativeValue;
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    sync::Arc,
};

#[derive(Clone, Copy)]
pub enum AddressSpace {
    Packs,
    Beds,
}
/// Existing storage coordinates, supplied from the same published snapshot as
/// the Reader. Hours correspond to its selected Pack addresses, not row data.
pub struct IndexSelection {
    pub file: u64,
    pub length: u64,
    pub time: Arc<TimeTrie>,
    pub hours: Vec<u64>,
    pub space: AddressSpace,
    pub leaves: Vec<Arc<LeafTrie>>,
}
pub(super) struct Indexes {
    pub(super) selection: IndexSelection,
    pub(super) plans: HashMap<usize, Fields>,
}
pub(super) struct Fields {
    pub(super) definition: Arc<SeedDefinition>,
    fields: IndexFields,
    pub(super) leaves: Option<Arc<LeafTrie>>,
}
impl Reader {
    /// Configure the published index/root snapshot before executing any reads.
    /// No payloads or index rows are read while preparing this binding.
    pub fn index(&mut self, selection: IndexSelection) -> Result<&mut Self, Error> {
        let state = Arc::get_mut(&mut self.0)
            .ok_or_else(|| error("configure reader indexes before sharing the reader"))?;
        if state.started.load(std::sync::atomic::Ordering::Relaxed) || state.index.is_some() {
            return Err(error("configure reader indexes before execution"));
        }
        if selection.hours.len() != state.selection.addresses.len() {
            return Err(error("index hours differ from Pack selection"));
        }
        let definitions = state.scope.reader_definitions();
        for root in &selection.leaves {
            if !definitions.iter().any(|d| root.matches_definition(d)) {
                return Err(error("leaf root is outside the reader Definition scope"));
            }
        }
        let mut plans = HashMap::new();
        for definition in definitions {
            let key = Arc::as_ptr(&definition) as usize;
            if plans.contains_key(&key) {
                continue;
            }
            let mut roots = selection
                .leaves
                .iter()
                .filter(|root| root.matches_definition(&definition));
            let leaves = roots.next().cloned();
            if roots.next().is_some() {
                return Err(error("repeated leaf root for Definition"));
            }
            let fields = IndexFields::from_definition(&definition)?;
            plans.insert(
                key,
                Fields {
                    definition,
                    fields,
                    leaves,
                },
            );
        }
        state.index = Some(Indexes { selection, plans });
        Ok(self)
    }
}
impl Indexes {
    pub(super) fn published_hour(&self, ordinal: usize, address: u64) -> Result<crate::TimeLeaf, Error> {
        let hour = self.selection.hours[ordinal];
        let leaf = self.selection.time.leaf(hour)?
            .ok_or_else(|| error("index binding requires a published Time leaf"))?;
        let expected = match self.selection.space {
            AddressSpace::Packs => leaf.packs,
            AddressSpace::Beds => leaf.beds,
        };
        if leaf.hour != hour || expected != address || leaf.count == 0 {
            return Err(error("Pack address/hour differs from Time"));
        }
        Ok(leaf)
    }

    pub(super) fn bind(
        &self,
        storage: Arc<dyn Storage>,
        budget: MemoryBudget,
        ordinal: usize,
        address: u64,
        definition: &Arc<SeedDefinition>,
        count: usize,
    ) -> Result<Arc<dyn StorageBinding>, Error> {
        let leaf = self.published_hour(ordinal, address)?;
        if leaf.count != count as u64 {
            return Err(error("Pack count/hour differs from Time"));
        }
        let fields = self
            .plans
            .get(&(Arc::as_ptr(definition) as usize))
            .ok_or_else(|| error("unregistered index Definition"))?;
        if fields.fields.has_leaf() && fields.leaves.is_none() {
            return Err(error("missing leaf root for Pack Definition version"));
        }
        let rows = self.rows(storage, budget, &leaf)?;
        Ok(Arc::new(DiskIndex(Arc::new(IndexPlan {
            rows,
            definition: fields.definition.clone(),
            fields: fields.fields,
            leaves: fields.leaves.clone(),
        }))))
    }
    pub(super) fn rows(
        &self,
        storage: Arc<dyn Storage>,
        budget: MemoryBudget,
        leaf: &crate::TimeLeaf,
    ) -> Result<IndexRows, Error> {
        let start = leaf.start.checked_mul(6).ok_or(Error::Overflow)?;
        let end = leaf.end.checked_mul(6).ok_or(Error::Overflow)?;
        if end > self.selection.length || end <= start {
            return Err(error("index hour exceeds published file"));
        }
        let count = usize::try_from(leaf.count).map_err(|_| Error::Overflow)?;
        let mut header = [0; 6];
        read_exact(storage.as_ref(), self.selection.file, start, &mut header)?;
        HourIndex::check_header(&header, leaf.hour)?;
        Ok(IndexRows {
            storage,
            budget,
            file: self.selection.file,
            start: start + 6,
            count,
            hour: leaf.hour,
        })
    }
    pub(super) fn range(
        &self,
        storage: Arc<dyn Storage>,
        budget: MemoryBudget,
        ordinal: usize,
        range: &std::ops::Range<u64>,
    ) -> Result<std::ops::Range<usize>, Error> {
        let hour = self.selection.hours[ordinal];
        if range.end <= hour || range.start >= hour.saturating_add(crate::TIME_HOUR_MS) {
            return Ok(0..0);
        }
        let leaf = self
            .selection
            .time
            .leaf(hour)?
            .ok_or_else(|| error("range requires a published Time leaf"))?;
        if leaf.hour != hour {
            return Err(error("invalid index hour"));
        }
        let count = usize::try_from(leaf.count).map_err(|_| Error::Overflow)?;
        if count == 0 {
            return Ok(0..0);
        }
        if range.start <= hour && range.end >= hour + crate::TIME_HOUR_MS {
            return Ok(0..count);
        }
        let reader = WindowReader::new(self.rows(storage, budget, &leaf)?, 4096);
        let start = if range.start <= hour {
            0
        } else {
            reader.lower_bound(range.start, 0)?
        };
        let end = if range.end >= hour + crate::TIME_HOUR_MS {
            count
        } else {
            reader.lower_bound(range.end, start)?
        };
        Ok(start..end)
    }
    pub(super) fn ranges(
        &self, storage: Arc<dyn Storage>, budget: MemoryBudget, ordinal: usize,
        ranges: &[std::ops::Range<u64>], bound: Option<&std::ops::Range<u64>>,
    ) -> Result<Vec<std::ops::Range<usize>>, Error> {
        let hour = *self.selection.hours.get(ordinal).ok_or_else(|| error("Pack ordinal outside index"))?;
        let from = hour.max(bound.map_or(0, |r| r.start));
        let to = hour.saturating_add(crate::TIME_HOUR_MS).min(bound.map_or(u64::MAX, |r| r.end));
        if from >= to { return Ok(Vec::new()); }
        let first = ranges.partition_point(|r| r.end <= from);
        let end = first + ranges[first..].partition_point(|r| r.start < to);
        if first == end { return Ok(Vec::new()); }
        let leaf = self.selection.time.leaf(hour)?.ok_or_else(|| error("range requires a published Time leaf"))?;
        if leaf.hour != hour { return Err(error("invalid index hour")); }
        let count = usize::try_from(leaf.count).map_err(|_| Error::Overflow)?;
        if count == 0 { return Ok(Vec::new()); }
        if ranges[first].start <= hour && ranges[first].end >= hour + crate::TIME_HOUR_MS
            && from == hour && to == hour + crate::TIME_HOUR_MS { return Ok(vec![0..count]); }
        // All binary probes for this hour reuse one bounded index window.
        let reader = WindowReader::new(self.rows(storage, budget, &leaf)?, 4096);
        let mut result: Vec<std::ops::Range<usize>> = Vec::with_capacity(end-first);
        let mut low = 0;
        for range in &ranges[first..end] {
            let a = range.start.max(from); let z = range.end.min(to);
            let start = if a <= hour { 0 } else { reader.lower_bound(a, low)? };
            let end = if z >= hour + crate::TIME_HOUR_MS { count } else { reader.lower_bound(z, start)? };
            low = end;
            if start == end { continue; }
            if let Some(last) = result.last_mut().filter(|last| last.end == start) { last.end = end; }
            else { result.push(start..end); }
        }
        Ok(result)
    }
}
#[derive(Clone)]
pub(super) struct IndexRows {
    storage: Arc<dyn Storage>,
    budget: MemoryBudget,
    file: u64,
    start: u64,
    count: usize,
    hour: u64,
}
struct IndexPlan {
    rows: IndexRows,
    definition: Arc<SeedDefinition>,
    fields: IndexFields,
    leaves: Option<Arc<LeafTrie>>,
}
struct DiskIndex(Arc<IndexPlan>);
impl IndexRows {
    fn address(&self, row: usize) -> Result<u64, Error> {
        if row >= self.count {
            return Err(error("index row outside hour"));
        }
        self.start
            .checked_add((row as u64).checked_mul(6).ok_or(Error::Overflow)?)
            .ok_or(Error::Overflow)
    }
}
impl IndexPlan {
    fn supplies(&self, field:usize)->bool {
        self.fields.contains(field) || self.leaves.as_ref().is_some_and(|r| r.has_field(field))
    }
    fn value(&self, time: u64, leaf: u16, field: usize) -> Result<Option<NativeValue<'_>>, Error> {
        if let Some(value) = self.fields.value(time, leaf, field) {
            return Ok(Some(value));
        }
        self.leaves
            .as_ref()
            .map_or(Ok(None), |root| root.value(leaf, field))
    }
}
impl StorageBinding for DiskIndex {
    fn check_definition(&self, definition: &Arc<SeedDefinition>) -> Result<(), Error> {
        if !Arc::ptr_eq(definition, &self.0.definition) {
            return Err(error("index uses a different registered Definition"));
        }
        Ok(())
    }
    fn row_count(&self) -> usize {
        self.0.rows.count
    }
    fn cursor(&self) -> Option<Box<dyn StorageCursor>> {
        Some(Box::new(IndexCursor {
            plan: self.0.clone(),
            reader: WindowReader::new(self.0.rows.clone(), 16384),
        }))
    }
    fn value(&self, row: usize, field: usize) -> Result<Option<NativeValue<'_>>, Error> {
        if !self.0.supplies(field) {
            return Ok(None);
        }
        let mut bytes = [0; 6];
        read_exact(
            self.0.rows.storage.as_ref(),
            self.0.rows.file,
            self.0.rows.address(row)?,
            &mut bytes,
        )?;
        let (time, leaf) = HourIndex::decode_row(&bytes, self.0.rows.hour)?;
        self.0.value(time, leaf, field)
    }
    fn requires_absent(&self, _row: usize, field: usize) -> Result<bool, Error> {
        Ok(!self.0.fields.contains(field))
    }
}
struct Window {
    bytes: Vec<u8>,
    first: usize,
    rows: usize,
    _memory: MemoryLease,
}
struct IndexCursor {
    plan: Arc<IndexPlan>,
    reader: WindowReader,
}
pub(crate) struct WindowReader {
    plan: IndexRows,
    window: RefCell<Option<Window>>,
    last: Cell<Option<(usize, u64, u16)>>,
    window_rows: usize,
}
impl WindowReader {
    pub(super) fn new(plan: IndexRows, bytes: usize) -> Self {
        Self {
            plan,
            window: RefCell::new(None),
            last: Cell::new(None),
            window_rows: bytes / 6,
        }
    }
    fn lower_bound(&self, time: u64, mut low: usize) -> Result<usize, Error> {
        let mut high = self.plan.count;
        while low < high {
            let middle = low + (high - low) / 2;
            if self.row(middle)?.0 < time {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(low)
    }
    pub(super) fn row(&self, row: usize) -> Result<(u64, u16), Error> {
        if let Some((_, time, leaf)) = self.last.get().filter(|(prior, _, _)| *prior == row) {
            return Ok((time, leaf));
        }
        self.plan.address(row)?;
        let mut window = self.window.borrow_mut();
        let rows = self.plan.count.min(self.window_rows);
        if window.is_none() {
            let length = rows * 6;
            let memory = self.plan.budget.reserve(length)?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(length)
                .map_err(|_| error("index window allocation failed"))?;
            bytes.resize(length, 0);
            *window = Some(Window {
                bytes,
                first: 0,
                rows: 0,
                _memory: memory,
            });
        }
        let window = window.as_mut().unwrap();
        if row < window.first || row - window.first >= window.rows {
            let first = row / rows * rows;
            let count = rows.min(self.plan.count - first);
            // Failed reads never turn partially replaced bytes into a valid window.
            window.rows = 0;
            read_exact(
                self.plan.storage.as_ref(),
                self.plan.file,
                self.plan.address(first)?,
                &mut window.bytes[..count * 6],
            )?;
            window.first = first;
            window.rows = count;
        }
        let at = (row - window.first) * 6;
        let (time, leaf) = HourIndex::decode_row(&window.bytes[at..at + 6], self.plan.hour)?;
        self.last.set(Some((row, time, leaf)));
        Ok((time, leaf))
    }

    /// Extend a decision over consecutive equal leaves in the window loaded
    /// by row(start). Do not read ahead into another index page.
    pub(super) fn same_leaf_end(&self, start: usize, end: usize, leaf: u16) -> Result<usize, Error> {
        let window = self.window.borrow();
        let window = window.as_ref().ok_or_else(|| error("missing index window"))?;
        let stop = end.min(window.first + window.rows);
        let mut next = start + 1;
        let bytes = &window.bytes[(next - window.first) * 6..(stop - window.first) * 6];
        for raw in bytes.chunks_exact(6) {
            if HourIndex::decode_row(raw, self.plan.hour)?.1 != leaf {
                break;
            }
            next += 1;
        }
        Ok(next)
    }

}
impl StorageCursor for IndexCursor {
    fn value(&self, row: usize, field: usize) -> Result<Option<NativeValue<'_>>, Error> {
        if !self.plan.supplies(field) {
            return Ok(None);
        }
        let (time, leaf) = self.reader.row(row)?;
        self.plan.value(time, leaf, field)
    }
    fn requires_absent(&self, _row: usize, field: usize) -> Result<bool, Error> {
        Ok(!self.plan.fields.contains(field))
    }
}
