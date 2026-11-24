//! Bounded Pack payload windows. The codec owns all metadata interpretation;
//! this adapter owns positional I/O, publication lifetime, and admission.
use super::{Storage, read_exact};
use crate::{Error, MemoryBudget, MemoryLease, error};
use pil_codec::api::{PackMemberReader, PackMetadata, PillView};
use std::{
    ops::Range,
    sync::{Arc, Mutex, Weak},
};

pub(crate) struct PagedPack {
    pub metadata: Arc<PackMetadata>,
    pub storage: Arc<dyn Storage>,
    pub budget: MemoryBudget,
    pub file: u64,
    pub address: u64,
    pub window: Mutex<Weak<Window>>,
}
impl PagedPack {
    pub fn reader(self: &Arc<Self>) -> PagedReader {
        PagedReader {
            decoder: self.metadata.reader(),
            pack: self.clone(),
            window: None,
            end: self.metadata.byte_length(),
        }
    }
    fn window(&self, span: &Range<usize>, end: usize) -> Result<Arc<Window>, Error> {
        let mut cached = self
            .window
            .lock()
            .map_err(|_| error("Pack window cache poisoned"))?;
        if let Some(window) = cached.upgrade() {
            if window.contains(span) {
                return Ok(window);
            }
        }
        let end = span
            .start
            .saturating_add(64 * 1024)
            .min(end)
            .max(span.end);
        let length = end - span.start;
        let memory = self.budget.reserve(length)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| error("Pack window allocation failed"))?;
        bytes.resize(length, 0);
        let at = self
            .address
            .checked_add(span.start as u64)
            .ok_or(Error::Overflow)?;
        read_exact(self.storage.as_ref(), self.file, at, &mut bytes)?;
        let window = Arc::new(Window {
            extent: span.start..end,
            bytes,
            _memory: memory,
        });
        *cached = Arc::downgrade(&window);
        Ok(window)
    }
}
// Only active readers retain payload windows. The shared slot is weak: it
// deduplicates concurrent reads without turning each Pack into a resident cache.
pub(crate) struct Window {
    extent: Range<usize>,
    bytes: Vec<u8>,
    _memory: MemoryLease,
}
impl Window {
    fn contains(&self, span: &Range<usize>) -> bool {
        span.start >= self.extent.start && span.end <= self.extent.end
    }
}
pub(crate) struct PagedReader {
    decoder: PackMemberReader,
    pack: Arc<PagedPack>,
    window: Option<Arc<Window>>,
    end: usize,
}
impl PagedReader {
    pub fn through(&mut self, last:usize)->Result<(),Error> {
        self.end=self.pack.metadata.member(last)?.end;
        Ok(())
    }
    pub fn get(&mut self, ordinal: usize) -> Result<PillView<'_>, Error> {
        let span = self.pack.metadata.member(ordinal)?;
        if span.is_empty() {
            return self.decoder.read(ordinal, &[]);
        }
        if self
            .window
            .as_ref()
            .is_none_or(|window| !window.contains(&span))
        {
            // Never keep stale bytes after a failed replacement, or require
            // double admission merely to move this reader's current window.
            self.window = None;
            self.window = Some(self.pack.window(&span,self.end)?);
        }
        let window = self.window.as_ref().unwrap();
        self.decoder.read(
            ordinal,
            &window.bytes[span.start - window.extent.start..span.end - window.extent.start],
        )
    }
}
