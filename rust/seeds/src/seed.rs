//! Individual event access over Pill Packer's PIL codec. Seed annotations and
//! restored event fields belong here; PIL representation remains codec-owned.
//!
//! `pack` handles one PAC. `packs` handles event containers and their queries.
pub use crate::{Seed, SeedDefinition as Definition, SeedView as View, Cell, Object};
pub use crate::{pack, packs, stalks};
pub use crate::Beds;
pub(crate) mod identity;
