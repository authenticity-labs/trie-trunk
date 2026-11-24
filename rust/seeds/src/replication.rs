//! Incremental transport assembly for adjacent PACs. Pill Packer reads every
//! envelope; this layer does not implement flags, integer widths, or directories.
//! Time-trie routing and durable publication belong to the replication owner.
use crate::{Error, SeedDefinition, SeedPackSnapshot, error};
use std::{ops::Range, sync::Arc};
mod reconstruction;
pub use reconstruction::{Replica, ReplicaBatch};
mod collection;
pub use collection::{Collection, Window};
mod readiness;
pub use readiness::{Readiness, State as ReadState};

pub struct ReceivedPack {
    owner: Arc<Vec<u8>>,
    range: Range<usize>,
}
#[cfg(feature="registration")]
pub struct VerifiedPack(ReceivedPack);
#[cfg(feature="registration")]
impl VerifiedPack {
    pub fn missing(&self, scope:&crate::registration::Packs) -> Result<Vec<usize>,Error> {
        scope.missing_strings(self.0.bytes())
    }
    pub fn resolve(self, scope:&crate::registration::Packs, maximum:usize) -> Result<Arc<SeedPackSnapshot>,Error> {
        let pack=self.0.open_registered(scope,maximum)?;
        pack.checked_for_write();
        Ok(pack)
    }
}
impl ReceivedPack {
    /// Verify incoming plain storage before opening. Compressed transport uses
    /// its existing decompressor integrity policy, without a second CRC scan.
    pub fn open_stored_for_write(self, definition: &Arc<SeedDefinition>, maximum: usize) -> Result<Arc<SeedPackSnapshot>, Error> {
        crate::integrity::before_write(self.bytes())?;
        let pack = self.open_stored_limited(definition, maximum)?;
        pack.checked_for_write();
        Ok(pack)
    }
    #[cfg(feature = "registration")]
    pub fn open_registered_for_write(self, scope: &crate::registration::Packs, maximum: usize) -> Result<Arc<SeedPackSnapshot>, Error> {
        crate::integrity::before_write(self.bytes())?;
        let pack = self.open_registered(scope, maximum)?;
        pack.checked_for_write();
        Ok(pack)
    }
    /// Verify the transport form once and expand once. A missing dictionary can
    /// pause decoding while this same uncompressed allocation stays admitted.
    #[cfg(feature="registration")]
    pub fn plain_for_write(self, maximum:usize) -> Result<VerifiedPack,Error> {
        crate::integrity::before_write(self.bytes())?;
        #[cfg(feature="compression")]
        {
            let mut out=Vec::new();
            if pil_codec::compression::unseal_with_limit(self.bytes(),maximum,&mut out)
                .map_err(|e| error(e.to_string()))? {
                let range=0..out.len();
                return Ok(VerifiedPack(Self{owner:Arc::new(out),range}));
            }
        }
        Ok(VerifiedPack(self))
    }
    /// One bounded, complete transport unit without copying its allocation.
    pub fn bounded(owner: Arc<Vec<u8>>, range: Range<usize>, maximum: usize) -> Result<Self, Error> {
        let raw = owner.get(range.clone()).ok_or(Error::Incomplete)?;
        if pil_codec::pac::stored_size(raw, maximum)? != raw.len() {
            return Err(error("received Pack extent differs from its envelope"));
        }
        Ok(Self { owner, range })
    }
    pub fn bytes(&self) -> &[u8] {
        &self.owner[self.range.clone()]
    }
    #[cfg(feature = "registration")]
    pub fn open_registered(
        self,
        scope: &crate::registration::Packs,
        maximum: usize,
    ) -> Result<Arc<SeedPackSnapshot>, Error> {
        scope.open_pack(self.owner, self.range, maximum)
    }
    pub fn open(self, definition: &Arc<SeedDefinition>) -> Result<Arc<SeedPackSnapshot>, Error> {
        definition.open_range(self.owner, self.range)
    }
    pub fn open_stored_limited(
        self,
        definition: &Arc<SeedDefinition>,
        maximum: usize,
    ) -> Result<Arc<SeedPackSnapshot>, Error> {
        definition.open_stored_range_limited(self.owner, self.range, maximum)
    }
    pub fn open_stored(
        self,
        definition: &Arc<SeedDefinition>,
    ) -> Result<Arc<SeedPackSnapshot>, Error> {
        definition.open_stored_range(self.owner, self.range)
    }
}

/// Complete PACs borrow their received chunk through a shared owner. Only a PAC
/// split across chunks is assembled, directly into its final owned allocation.
/// Acceptance means a complete envelope, not successful storage publication.
pub struct PackStream {
    maximum: usize,
    pending: Vec<u8>,
    expected: Option<usize>,
    count: u64,
    failed: bool,
}
impl PackStream {
    pub fn new(maximum: usize) -> Self {
        Self {
            maximum,
            pending: Vec::new(),
            expected: None,
            count: 0,
            failed: false,
        }
    }
    pub fn count(&self) -> u64 {
        self.count
    }
    fn length(&self, prefix: &[u8]) -> Result<Option<usize>, Error> {
        match pil_codec::pac_envelope::stored_length(prefix) {
            Ok(n) => {
                let n = usize::try_from(n).map_err(|_| Error::Overflow)?;
                if n > self.maximum {
                    return Err(error("Seed Pack exceeds receive limit"));
                }
                Ok(Some(n))
            }
            Err(Error::Incomplete) => Ok(None),
            Err(e) => Err(e),
        }
    }
    pub fn load(
        &mut self,
        chunk: Arc<Vec<u8>>,
        mut accept: impl FnMut(ReceivedPack) -> Result<(), Error>,
    ) -> Result<(), Error> {
        if self.failed {
            return Err(error("Seed stream has failed"));
        }
        let result = self.load_inner(chunk, 0, false, &mut accept).map(|_| ());
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn accept(
        &mut self,
        pack: ReceivedPack,
        accept: &mut impl FnMut(ReceivedPack) -> Result<(), Error>,
    ) -> Result<(), Error> {
        pil_codec::pac::stored_size(pack.bytes(), self.maximum)?;
        let count = self.count.checked_add(1).ok_or(Error::Overflow)?;
        accept(pack)?;
        self.count = count;
        Ok(())
    }
    /// Consume at most one Pack, retaining the network allocation across
    /// calls. The owner can wait for admission before decoding the next Pack.
    pub fn step(&mut self, chunk: Arc<Vec<u8>>, at: usize) -> Result<(usize, Option<ReceivedPack>), Error> {
        if self.failed || at > chunk.len() { return Err(error("invalid Seed stream position")); }
        let mut received=None;
        match self.load_inner(chunk,at,true,&mut |pack|{received=Some(pack);Ok(())}) {
            Ok(end)=>Ok((end,received)),Err(e)=>{self.failed=true;Err(e)}
        }
    }
    fn load_inner(
        &mut self,
        chunk: Arc<Vec<u8>>,
        mut at: usize,
        one: bool,
        accept: &mut impl FnMut(ReceivedPack) -> Result<(), Error>,
    ) -> Result<usize, Error> {
        while at < chunk.len() {
            if self.pending.is_empty() {
                if let Some(length) = self.length(&chunk[at..])? {
                    if length <= chunk.len() - at {
                        let end = at + length;
                        self.accept(
                            ReceivedPack {
                                owner: chunk.clone(),
                                range: at..end,
                            },
                            accept,
                        )?;
                        at = end;
                        if one { return Ok(at); }
                        continue;
                    }
                    self.pending
                        .try_reserve_exact(length)
                        .map_err(|_| Error::Overflow)?;
                    self.expected = Some(length);
                }
            }
            if let Some(length) = self.expected {
                let n = (length - self.pending.len()).min(chunk.len() - at);
                self.pending.extend_from_slice(&chunk[at..at + n]);
                at += n;
                if self.pending.len() == length {
                    let bytes = std::mem::take(&mut self.pending);
                    self.expected = None;
                    self.accept(
                        ReceivedPack {
                            owner: Arc::new(bytes),
                            range: 0..length,
                        },
                        accept,
                    )?;
                    if one { return Ok(at); }
                }
            } else {
                // Only a fragmented envelope reaches this path. Its length is
                // discovered by the codec, before reserving the member payload.
                if self.pending.len() == self.maximum {
                    return Err(error("Seed Pack exceeds receive limit"));
                }
                self.pending.push(chunk[at]);
                at += 1;
                if let Some(length) = self.length(&self.pending)? {
                    self.pending
                        .try_reserve_exact(length - self.pending.len())
                        .map_err(|_| Error::Overflow)?;
                    self.expected = Some(length);
                    if self.pending.len() == length {
                        let bytes = std::mem::take(&mut self.pending);
                        self.expected = None;
                        self.accept(
                            ReceivedPack {
                                owner: Arc::new(bytes),
                                range: 0..length,
                            },
                            accept,
                        )?;
                        if one { return Ok(at); }
                    }
                }
            }
        }
        Ok(at)
    }
    /// EOF completes the transport, not any hour's replication receipt. The
    /// owner publishes receipts only after its existing dependency/durability checks.
    pub fn finish(self) -> Result<u64, Error> {
        if self.failed {
            return Err(error("Seed stream has failed"));
        }
        if !self.pending.is_empty() {
            return Err(error("Truncated Seed Pack"));
        }
        Ok(self.count)
    }
}
