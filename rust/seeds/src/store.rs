//! PACKS: Packed Array Container Kompressed Storage.
//! BEDS: Browsable Event Datastore.
//! Both storage roles share query coordination on any node.

use crate::{Cursor, Error, Query, SeedDefinition, SeedPackSnapshot, Sources};
use pil_codec::r#box::Box as PackBox;
use std::sync::Arc;

fn box_definition(container: &PackBox<'_>, definition: Arc<SeedDefinition>) -> Arc<SeedDefinition> {
    if container.has_common_strings() {
        definition.with_prescription(Arc::new(container.prescription(&definition.rx)))
    } else {
        definition
    }
}

// PACKS/BEDS own query coordination. Their persistence and replication callers
// supply Definition bindings; neither generic Box nor PAC owns a trunk registry.
macro_rules! store {
    ($name:ident) => {
        #[derive(Default)]
        pub struct $name {
            sources: Sources,
        }
        impl $name {
            pub fn new() -> Self {
                Self::default()
            }
            /// Reconstruct the event Box envelope from origin publication
            /// scalars. The caller supplies selected addresses independently.
            pub fn reconstruct(length: u64, count: u64) -> Result<crate::replication::Replica, Error> {
                crate::replication::Replica::new(length, count)
            }
            /// PACKS/BEDS addresses are supplied by the time trie. Keep them
            /// there instead of serializing a second directory in the Box.
            pub fn create_box(headers: &[u8]) -> Result<PackBox<'static>, Error> {
                PackBox::new(
                    headers,
                    pil_codec::r#box::BoxOptions {
                        external_addresses: true,
                        ..Default::default()
                    },
                )
            }
            pub fn pack(&mut self, pack: Arc<SeedPackSnapshot>) -> &mut Self {
                self.sources.pack(pack);
                self
            }
            /// Bind sealed child Packs in place, with one shared Box allocation.
            /// No child bytes are copied. Registry lookup remains caller-owned.
            pub fn open_box(
                bytes: Arc<Vec<u8>>,
                mut definition: impl FnMut(usize) -> Result<Arc<SeedDefinition>, Error>,
            ) -> Result<Self, Error> {
                #[cfg(feature = "compression")]
                let bytes = {
                    let mut expanded = Vec::new();
                    if PackBox::expand(&bytes, &mut expanded)
                        .map_err(|e| crate::error(e.to_string()))?
                    {
                        Arc::new(expanded)
                    } else {
                        bytes
                    }
                };
                let container = PackBox::open(bytes.as_slice())?;
                let mut result = Self::new();
                for ordinal in 0..container.len() {
                    let child = container.get(ordinal)?;
                    // This is a borrowed child supplied by the Box API, not a
                    // second directory parser. Obtain its range in the owner.
                    let start = child.as_ptr() as usize - bytes.as_ptr() as usize;
                    result.pack(
                        box_definition(&container, definition(ordinal)?)
                            .open_stored_range(bytes.clone(), start..start + child.len())?,
                    );
                }
                Ok(result)
            }
            pub fn query(&self, query: Query) -> Result<Cursor, Error> {
                self.sources.query(query)
            }
            /// Consume this selected BEDS/PACKS working set. Sequential queries
            /// release each Pack as conversion advances; independent owners
            /// (such as another reader or the node cache) retain their leases.
            pub fn stalks(self, query:Query, budget:crate::MemoryBudget, row_limit:usize)->Result<crate::stalks::Growth,Error> {
                Ok(Cursor::consuming(self.sources.packs,query)?.memory_budget(budget.clone())?.stalks(budget,row_limit))
            }
            /// The existing time-trie lookup supplies each Pack's BOF address.
            /// The PAC codec reads the control byte and length; no subsequent
            /// hour, Box directory, or scan of preceding Packs is required.
            pub fn open_box_addresses(
                bytes: Arc<Vec<u8>>,
                addresses: impl IntoIterator<Item = u64>,
                mut definition: impl FnMut(u64) -> Result<Arc<SeedDefinition>, Error>,
            ) -> Result<Self, Error> {
                let container = PackBox::open(bytes.as_slice())?;
                let mut result = Self::new();
                for address in addresses {
                    let child = container.get_at(address)?;
                    let start = usize::try_from(address).map_err(|_| Error::Overflow)?;
                    let end = start.checked_add(child.len()).ok_or(Error::Overflow)?;
                    result.pack(box_definition(&container, definition(address)?).open_stored_range(bytes.clone(), start..end)?);
                }
                Ok(result)
            }
            /// Query selected fixed summary Packs through the same Seed reader
            /// and predicates as events. The caller maps time to Pack ordinals.
            pub fn open_summary_box(
                bytes: Arc<Vec<u8>>,
                packs: std::ops::Range<usize>,
            ) -> Result<Self, Error> {
                let container = crate::summaries::SummaryBoxReader::open(&bytes)?;
                if packs.start > packs.end || packs.end > container.len() {
                    return Err(crate::error("summary Pack range is outside the Box"));
                }
                let mut result = Self::new();
                for ordinal in packs {
                    let layout = container.layout_at(ordinal)?;
                    result.pack(
                        layout.open_range(bytes.clone(), container.pack_range(ordinal)?)?,
                    );
                }
                Ok(result)
            }
        }
    };
}
store!(Packs);
/// Browsable storage uses the same traversal and query implementation.
pub type Beds = Packs;

impl Packs {
    pub(crate) fn append(&mut self, mut other: Self) {
        self.sources.packs.append(&mut other.sources.packs);
    }
}
