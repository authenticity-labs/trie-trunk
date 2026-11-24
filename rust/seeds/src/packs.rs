//! Event container traversal, query coordination, and sparse reconstruction.
//! Both `.packs` and `.beds` use these operations. `.seeds` remains the ingress
//! recovery spool and is not renamed or assigned Box traversal by this module.
pub use crate::{Packs, Beds, Cursor, Query, QuerySlice};
pub use crate::query::pruning::{CandidatePeriods, PruningDecision, PruningEvidence};
pub use crate::replication::{Replica, ReplicaBatch};
pub use crate::replication::{Collection, Window};
mod origin;
pub use origin::{Publication, Writer};

/// Locate the existing Box footer from a bounded tail read. Native adapters
/// need not load member bytes to select a summary day or resume a writer.
pub fn footer_range(tail: &[u8], length: u64) -> Result<std::ops::Range<u64>, crate::Error> {
    pil_codec::r#box::Box::footer_range(tail, length)
}

#[cfg(feature = "registration")]
pub use crate::reader::{Reader, Selection, AddressSpace, IndexSelection};
