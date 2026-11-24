//! Query ownership is independent of serialized-byte export. Resident Packs
//! retain their existing owner; file readers retain metadata and a pinned source.
use crate::{Error, MemoryLease, SeedDefinition, SeedPackSnapshot, StorageBinding};
use pil_codec::api::{OwnedPackReader, PillView};
use std::sync::Arc;

pub struct ReadPack {
    pub(crate) definition: Arc<SeedDefinition>,
    pub(crate) lineage: Option<Arc<std::collections::HashMap<usize, Arc<SeedDefinition>>>>,
    pub(crate) storage: Option<Arc<dyn StorageBinding>>,
    pub(crate) memory: Option<Arc<MemoryLease>>,
    pub(crate) allocation: usize,
    pub(crate) pack: Data,
}
pub(crate) enum Data {
    Resident(Arc<SeedPackSnapshot>),
    #[cfg(feature = "registration")]
    Paged(Arc<crate::reader::paged::PagedPack>),
}
pub(crate) enum ReadCursor {
    Resident(OwnedPackReader),
    #[cfg(feature = "registration")]
    Paged(crate::reader::paged::PagedReader),
}
impl ReadPack {
    pub(crate) fn definition_at(&self, ordinal: usize) -> Result<&Arc<SeedDefinition>, Error> {
        match &self.lineage {
            None => Ok(&self.definition),
            Some(m) => {
                let rx = match &self.pack {
                    Data::Resident(p) => p.pack.prescription_at(ordinal)?,
                    #[cfg(feature = "registration")]
                    Data::Paged(p) => p.metadata.prescription_at(ordinal)?,
                };
                m.get(&(rx.source_identity())).ok_or_else(|| crate::error("missing row Definition"))
            }
        }
    }

    pub fn resident(pack: Arc<SeedPackSnapshot>) -> Arc<Self> {
        Arc::new(Self {
            definition: pack.definition.clone(),
            lineage: pack.lineage.clone(),
            storage: pack.storage.clone(),
            memory: pack.memory.clone(),
            allocation: pack.allocation,
            pack: Data::Resident(pack),
        })
    }
    pub(crate) fn len(&self) -> usize {
        match &self.pack {
            Data::Resident(p) => p.len(),
            #[cfg(feature = "registration")]
            Data::Paged(p) => p.metadata.len(),
        }
    }
    pub(crate) fn retained_bytes(&self) -> usize {
        match &self.pack {
            Data::Resident(p) => p.retained_bytes(),
            #[cfg(feature = "registration")]
            Data::Paged(p) => p.metadata.retained_bytes(),
        }
    }
}
impl Data {
    pub(crate) fn reader(&self) -> ReadCursor {
        match self {
            Self::Resident(p) => ReadCursor::Resident(p.pack.reader()),
            #[cfg(feature = "registration")]
            Self::Paged(p) => ReadCursor::Paged(p.reader()),
        }
    }
}
impl ReadCursor {
    pub(crate) fn get(&mut self, ordinal: usize) -> Result<PillView<'_>, Error> {
        match self {
            Self::Resident(r) => r.get(ordinal),
            #[cfg(feature = "registration")]
            Self::Paged(r) => r.get(ordinal),
        }
    }
}
