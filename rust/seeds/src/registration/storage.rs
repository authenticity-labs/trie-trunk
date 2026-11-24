//! Storage representation is node configuration, independent of its clients.
use crate::{Compression, TimeLeaf, reader::AddressSpace};

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Storage {
    Browse,
    Pack,
}

impl Storage {
    pub fn alternative(self) -> Self {
        match self {
            Self::Browse => Self::Pack,
            Self::Pack => Self::Browse,
        }
    }
    pub fn suffix(self) -> &'static str {
        match self {
            Self::Browse => ".beds",
            Self::Pack => ".packs",
        }
    }

    pub fn address(self, leaf: &TimeLeaf) -> u64 {
        match self {
            Self::Browse => leaf.beds,
            Self::Pack => leaf.packs,
        }
    }

    pub fn space(self) -> AddressSpace {
        match self {
            Self::Browse => AddressSpace::Beds,
            Self::Pack => AddressSpace::Packs,
        }
    }

    pub fn compression(self) -> Option<Compression> {
        match self {
            Self::Browse => None,
            Self::Pack => Some(Compression {
                algorithm: pil_codec::compression::Algorithm::Zstd,
                level: 1,
                only_if_smaller: true,
            }),
        }
    }
}
