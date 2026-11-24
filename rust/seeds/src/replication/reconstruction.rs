//! Event storage reconstruction. The owner selects missing ranges through Time
//! and pins the publication generation. No local relocation or Box directory.
use super::ReceivedPack;
use crate::writer::{Buffer, FileId, WriteBatch, WriteCursor};
use crate::{Error, MemoryBudget, error, integrity};
use pil_codec::r#box::Box as Container;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

/// The published, open, directory-less event Box envelope. Event storage uses
/// no Box application metadata here; Pack metadata remains untouched. Length
/// and count are taken from the same origin snapshot as the supplied addresses.
#[derive(Clone)]
pub struct Replica {
    container: Arc<Container<'static>>,
    footer: Arc<Vec<u8>>,
    length: u64,
}
impl Replica {
    /// Reuse collection's planned Time layout, reading only the current header
    /// and selected leaves through the host adapter. A changed header requests
    /// replanning; no completion patch is emitted from the stale layout.
    pub fn publication_with(&self, time: &crate::TimeTrie, generation: [u8; 16],
        placements: &[crate::TimeLeaf],
        mut read: impl FnMut(u64, &mut [u8]) -> Result<(), Error>,
    ) -> Result<Option<Vec<(usize, u8)>>, Error> {
        let mut header = [0; crate::TIME_HEADER_BYTES];
        read(0, &mut header)?;
        if header != time.bytes()[..crate::TIME_HEADER_BYTES] { return Ok(None); }
        let mut patches = self.publication(time, generation, placements)?;
        let mut leaf = [0; 33]; // Four uint64 coordinates and the availability byte.
        for (placement, patch) in placements.iter().zip(&mut patches) {
            let (start, expected) = time.collected_span(*placement)?;
            let current = &mut leaf[..expected.len()];
            read(start as u64, current)?;
            let flags = current.len() - 1;
            if current[..flags] != expected[..flags] {
                return Err(error("Seed Pack and committed Time/index disagree"));
            }
            patch.1 = crate::TimeTrie::collected_flags(current[flags])?;
        }
        Ok(Some(patches))
    }
    /// Validate received placements against the transaction's current Time
    /// snapshot. Returns opaque completion-byte patches; the host applies them
    /// only to its detached metadata transaction, after all checks succeed.
    /// No Pack payload or full calendar is copied by this operation.
    pub fn publication(&self, time: &crate::TimeTrie, generation: [u8; 16],
        placements: &[crate::TimeLeaf]) -> Result<Vec<(usize, u8)>, Error> {
        if time.header().generation != generation {
            return Err(error("Seed publication generation differs from Time"));
        }
        if placements.len() > self.count() {
            return Err(error("publication exceeds origin Pack count"));
        }
        let body_end = self.length - self.footer.len() as u64;
        let mut prior = None;
        placements.iter().map(|&placement| {
            if prior.is_some_and(|hour| placement.hour <= hour) {
                return Err(error("publication hours must be ordered and unique"));
            }
            if placement.beds >= body_end {
                return Err(error("Seed Pack address exceeds replica member extent"));
            }
            prior = Some(placement.hour);
            time.collected(placement)
        }).collect()
    }
    pub(crate) fn new(length: u64, count: u64) -> Result<Self, Error> {
        let footer = Container::external_footer(length, count, &[], false)?;
        let container = Container::open_footer_owned(footer.clone(), length)?;
        Ok(Self {
            container: Arc::new(container),
            footer: Arc::new(footer),
            length,
        })
    }
    pub fn length(&self) -> u64 {
        self.length
    }
    pub fn count(&self) -> usize {
        self.container.len()
    }
    pub fn footer(&self) -> &[u8] {
        &self.footer
    }
    pub fn batch(
        &self,
        file: FileId,
        budget: &MemoryBudget,
        maximum_operations: usize,
    ) -> ReplicaBatch {
        ReplicaBatch {
            snapshot: self.clone(),
            file,
            budget: budget.clone(),
            writes: WriteBatch::new(maximum_operations),
            owners: HashMap::new(),
            ranges: BTreeMap::new(),
            footer_added: false,
        }
    }
}

/// A bounded batch of already selected missing PACs. The owner must exclude
/// published immutable ranges and hold its writer lease through completion.
/// Readiness is not durable-coverage publication; the owner publishes that only
/// after dependencies, these writes, and its metadata transaction succeed.
pub struct ReplicaBatch {
    snapshot: Replica,
    file: FileId,
    budget: MemoryBudget,
    writes: WriteBatch,
    owners: HashMap<usize, Buffer>,
    ranges: BTreeMap<u64, u64>,
    footer_added: bool,
}
impl ReplicaBatch {
    /// Write a selected Pack and publish its metadata residency at the same
    /// commit boundary. Admission happens before any write is queued; failed
    /// writes/publication cannot make this metadata visible to queries.
    #[cfg(feature = "registration")]
    pub fn pack_selected(&mut self, reader: &crate::reader::Reader, ordinal: usize,
        pack: Arc<crate::SeedPackSnapshot>) -> Result<&mut Self, Error> {
        let (file, address) = reader.placement(ordinal)?;
        if file != self.file { return Err(error("selected Pack belongs to another file")); }
        let retained = reader.stage_retention(ordinal, &pack)?;
        self.pack(address, pack)?;
        self.writes.retain_metadata(retained);
        Ok(self)
    }
    pub(super) fn matches(&self, replica: &Replica) -> bool {
        Arc::ptr_eq(&self.snapshot.container, &replica.container)
    }
    /// Retain a received/opened Pack's existing allocation. Multiple Pack views
    /// of one network chunk share one admission charge and immutable owner.
    pub fn pack(&mut self, address: u64, pack: Arc<crate::SeedPackSnapshot>) -> Result<&mut Self, Error> {
        let bytes = pack.stored_bytes();
        self.check_range(address, bytes)?;
        let end = address.checked_add(bytes.len() as u64).ok_or(Error::Overflow)?;
        let key = pack.allocation_bytes().as_ptr() as usize;
        let start = bytes.as_ptr() as usize - key;
        let range = start..start + bytes.len();
        let buffer = match self.owners.get(&key) {
            Some(buffer) => buffer.clone(),
            None => Buffer::admit_pack(pack.clone(), &self.budget)?,
        };
        pack.before_write()?;
        self.writes.write(self.file, address, buffer.clone(), range).map_err(|e| error(e.to_string()))?;
        self.owners.entry(key).or_insert(buffer);
        self.ranges.insert(address, end);
        Ok(self)
    }
    pub fn load(&mut self, address: u64, received: ReceivedPack) -> Result<&mut Self, Error> {
        let bytes = received.bytes();
        self.check_range(address, bytes)?;
        let end = address.checked_add(bytes.len() as u64).ok_or(Error::Overflow)?;
        let key = received.owner.as_ptr() as usize;
        let buffer = match self.owners.get(&key) {
            Some(buffer) => buffer.clone(),
            None => Buffer::admit(received.owner.clone(), &self.budget)?,
        };
        integrity::before_write(bytes)?;
        self.writes.write(self.file, address, buffer.clone(), received.range).map_err(|e| error(e.to_string()))?;
        self.owners.entry(key).or_insert(buffer);
        self.ranges.insert(address, end);
        Ok(self)
    }
    fn check_range(&self, address: u64, bytes: &[u8]) -> Result<(), Error> {
        if self.footer_added {
            return Err(error("load Packs before staging the footer"));
        }
        if self.ranges.len() >= self.snapshot.count() {
            return Err(error("batch exceeds origin Pack count"));
        }
        let flags = pil_codec::pac::flags(bytes)?;
        if flags & pil_codec::pac::COMPRESSED != 0 {
            return Err(error("open compressed transport before writing BEDS"));
        }
        if flags & (pil_codec::pac::SEALED | pil_codec::pac::COMPRESSED) == 0 {
            return Err(error(
                "unsealed Packs remain resident, outside replicated event storage",
            ));
        }
        let extent = self.snapshot.container.range_at(address, bytes)?;
        if extent.end - extent.start != bytes.len() as u64 {
            return Err(error("received Pack extent differs from its envelope"));
        }
        if self
            .ranges
            .range(..=address)
            .next_back()
            .is_some_and(|(_, end)| *end > address)
            || self
                .ranges
                .range(address..)
                .next()
                .is_some_and(|(start, _)| *start < extent.end)
        {
            return Err(error("overlapping Pack writes in replica batch"));
        }
        Ok(())
    }
    /// Stage a new origin envelope only when the published snapshot changed.
    /// Gap fills against the same snapshot simply omit this method. Payloads
    /// cross their barrier before the footer, then the footer crosses its own.
    pub fn footer(&mut self) -> Result<&mut Self, Error> {
        if self.footer_added {
            return Ok(self);
        }
        let buffer = Buffer::admit(self.snapshot.footer.clone(), &self.budget)?;
        self.writes.barrier().map_err(|e| error(e.to_string()))?;
        self.writes
            .write(
                self.file,
                self.snapshot.length - self.snapshot.footer.len() as u64,
                buffer,
                0..self.snapshot.footer.len(),
            )
            .map_err(|e| error(e.to_string()))?;
        self.footer_added = true;
        Ok(self)
    }
    pub fn start(self) -> Result<WriteCursor, Error> {
        // A new footer ends exactly at the published logical EOF. Positional
        // write grows the file without a separate resize; generation replacement
        // or shrink is a separate owner operation, never implicit on gap fill.
        self.writes.start().map_err(|e| error(e.to_string()))
    }
}
