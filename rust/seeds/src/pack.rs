//! One Seed Pack: event access over Pill Packer's PAC implementation.
//! This is distinct from a collection of Packs or Box traversal.
pub use pil_codec::pac_envelope::stored_length;
/// Maximum bytes needed to decode the PAC controls and stored length.
pub const MAX_PREFIX: usize = 27;
pub use crate::{SeedPack as Pack, SeedPackSnapshot as Snapshot, PackScan};
pub use crate::{Batch, BatchReader, OwnedBatchReader, Predicate, Query};
