//! Match an ordered response to its pinned Time selection. No hour, address,
//! or storage coordinates are added to Pills or PACs for this operation.
use super::{Replica, ReplicaBatch};
use crate::{Error, SeedPackSnapshot, TimeLeaf, TimeTrie, TIME_HOUR_MS, error};
use std::{ops::Range, sync::Arc};

/// One response's published boundaries. Transport adapters supply these values;
/// they do not interpret Time leaves or derive local placement.
pub struct Window {
    pub generation: [u8; 16],
    pub retained: u64,
    pub sealed: u64,
}

/// Retains the original Time allocation and a small range selection. Each Pack
/// consumes one nonempty hour; empty hours consume no response bytes.
pub struct Collection {
    replica: Replica,
    time: Arc<TimeTrie>,
    ranges: Vec<Range<u64>>,
    range: usize,
    hour: u64,
    next: Option<TimeLeaf>,
    accepted: u64,
    finished: bool,
}
impl Replica {
    pub fn collect(&self, time: Arc<TimeTrie>, ranges: Vec<Range<u64>>, window: Window) -> Result<Collection, Error> {
        let header = time.header();
        if window.generation != header.generation || window.retained > window.sealed
            || window.retained % TIME_HOUR_MS != 0 || window.sealed % TIME_HOUR_MS != 0 {
            return Err(error("collection window does not match Time generation/bounds"));
        }
        // Match HTTP selection semantics: intersecting hours, sorted disjoint
        // windows. Adjacent/overlapping hour coverage is visited only once.
        let mut selected: Vec<Range<u64>> = Vec::with_capacity(ranges.len());
        let mut prior_end = 0;
        for range in ranges {
            if range.start >= range.end || range.start < prior_end {
                return Err(error("collection ranges must be ordered and disjoint"));
            }
            prior_end = range.end;
            let first = (range.start / TIME_HOUR_MS * TIME_HOUR_MS).max(window.retained);
            let end = range.end.checked_add(TIME_HOUR_MS - 1).ok_or(Error::Overflow)?
                / TIME_HOUR_MS * TIME_HOUR_MS;
            let end = end.min(window.sealed);
            if first >= end { continue; }
            if first < header.first || end > header.sealed {
                return Err(error("refresh Time before collecting newly published hours"));
            }
            if let Some(last) = selected.last_mut().filter(|last| last.end >= first) {
                last.end = last.end.max(end);
            } else { selected.push(first..end); }
        }
        let hour = selected.first().map_or(0, |range| range.start);
        Ok(Collection { replica: self.clone(), time, ranges: selected, range: 0, hour,
            next: None, accepted: 0, finished: false })
    }
}
impl Collection {
    pub fn peek(&mut self)->Result<Option<TimeLeaf>,Error>{self.next()}
    fn next(&mut self) -> Result<Option<TimeLeaf>, Error> {
        if let Some(next) = self.next { return Ok(Some(next)); }
        while let Some(range) = self.ranges.get(self.range) {
            if self.hour >= range.end {
                self.range += 1;
                self.hour = self.ranges.get(self.range).map_or(0, |range| range.start);
                continue;
            }
            let leaf = self.time.leaf(self.hour)?
                .ok_or_else(|| error("collection requires the missing Time leaf"))?;
            if leaf.count != 0 { self.next = Some(leaf); return Ok(Some(leaf)); }
            self.hour += TIME_HOUR_MS;
        }
        Ok(None)
    }
    /// A failed admission leaves this Pack selected for retry after backpressure.
    /// Successful attachment advances once; completion still requires queue I/O
    /// and the storage owner's existing metadata publication.
    pub fn load(&mut self, batch: &mut ReplicaBatch, pack: Arc<SeedPackSnapshot>) -> Result<TimeLeaf, Error> {
        self.load_with(batch, pack, |batch, leaf, pack| batch.pack(leaf.beds, pack).map(|_| ()))
    }
    /// Match a received Pack before the storage owner has acquired its file
    /// lease. This advances transport selection only, never durable coverage.
    /// The owner must retain the Pack until admission to its bounded write queue.
    pub fn accept(&mut self, pack: &SeedPackSnapshot) -> Result<TimeLeaf, Error> {
        let (leaf, accepted) = self.check(pack)?;
        self.advance(accepted);
        Ok(leaf)
    }
    /// Match Time placement and retain the selected Pack metadata only after
    /// the batch's owner publishes its durable coverage.
    #[cfg(feature = "registration")]
    pub fn load_selected(&mut self, batch: &mut ReplicaBatch, pack: Arc<SeedPackSnapshot>,
        reader: &crate::reader::Reader, ordinal: usize) -> Result<TimeLeaf, Error> {
        self.load_with(batch, pack, |batch, leaf, pack| {
            if reader.placement(ordinal)?.1 != leaf.beds {
                return Err(error("selected metadata address differs from Time"));
            }
            batch.pack_selected(reader, ordinal, pack).map(|_| ())
        })
    }
    fn load_with(&mut self, batch: &mut ReplicaBatch, pack: Arc<SeedPackSnapshot>,
        attach: impl FnOnce(&mut ReplicaBatch, &TimeLeaf, Arc<SeedPackSnapshot>) -> Result<(), Error>) -> Result<TimeLeaf, Error> {
        if !batch.matches(&self.replica) { return Err(error("collection batch belongs to another replica snapshot")); }
        let (leaf, accepted) = self.check(&pack)?;
        attach(batch, &leaf, pack)?;
        self.advance(accepted);
        Ok(leaf)
    }
    fn check(&mut self, pack: &SeedPackSnapshot) -> Result<(TimeLeaf, u64), Error> {
        if self.finished { return Err(error("collection is finished")); }
        let leaf = self.next()?.ok_or_else(|| error("unexpected Pack after selected Time hours"))?;
        if !pack.is_sealed() { return Err(error("unsealed Packs remain resident outside sealed collection")); }
        if pack.len() as u64 != leaf.count { return Err(error("received Pack count differs from Time")); }
        let accepted = self.accepted.checked_add(1).ok_or(Error::Overflow)?;
        if accepted > self.replica.count() as u64 { return Err(error("collection exceeds published Pack count")); }
        Ok((leaf, accepted))
    }
    fn advance(&mut self, accepted: u64) {
        self.accepted = accepted;
        self.hour += TIME_HOUR_MS;
        self.next = None;
    }
    pub fn finish(&mut self) -> Result<u64, Error> {
        if self.next()?.is_some() { return Err(error("incomplete collection: missing selected Pack")); }
        self.finished = true;
        Ok(self.accepted)
    }
}
