//! Origin publication metadata. Generic Box framing remains in Pill Packer.
use crate::{Error, error};
use pil_codec::r#box::{Box as Container, BoxInput, BoxOptions, Commit};

const BEDS_LENGTH: &str = "seed.beds.length";

/// Scalars from the same published origin footer as the stored Pack count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Publication {
    pub length: u64,
    pub count: u64,
}
impl Publication {
    pub fn read(footer: &[u8], origin_length: u64) -> Result<Self, Error> {
        let container = Container::open_footer(footer, origin_length)?;
        Self::from_container(&container)
    }
    pub fn from_container(container: &Container<'_>) -> Result<Self, Error> {
        if !container.options().external_addresses {
            return Err(error("event storage requires Time addressing"));
        }
        let raw = container
            .headers()
            .get(BEDS_LENGTH)
            .ok_or_else(|| error("origin Box has no published BEDS extent"))?;
        let length = u64::from_le_bytes(
            raw.as_slice()
                .try_into()
                .map_err(|_| error("invalid published BEDS extent"))?,
        );
        let count = container.len() as u64;
        // Check scalar consistency using the existing serializer, not Time or
        // member reads. The replica envelope is open and has no origin metadata.
        Container::external_footer(length, count, &[], false)?;
        Ok(Self { length, count })
    }
}

/// Seed's append writer maintains both stored and uncompressed addresses.
/// Resume holds only the footer; commits never reread historical members.
pub struct Writer<'a> {
    container: Container<'a>,
    beds_end: u64,
    #[cfg(feature = "compression")]
    compression: Option<crate::Compression>,
}
impl<'a> Writer<'a> {
    pub fn new(headers: &[u8]) -> Result<Self, Error> {
        let container = Container::new(
            headers,
            BoxOptions {
                external_addresses: true,
                ..Default::default()
            },
        )?;
        if container.headers().contains_key(BEDS_LENGTH) {
            return Err(error("published BEDS extent is maintained by Seed"));
        }
        Ok(Self {
            container,
            beds_end: 0,
            #[cfg(feature = "compression")]
            compression: None,
        })
    }
    pub fn resume(footer: Vec<u8>, length: u64) -> Result<Self, Error> {
        let container = Container::open_footer_owned(footer, length)?;
        if container.is_sealed() {
            return Err(error("cannot resume sealed event storage"));
        }
        let publication = Publication::from_container(&container)?;
        let replica_footer =
            Container::external_footer(publication.length, publication.count, &[], false)?;
        let beds_end = publication.length - replica_footer.len() as u64;
        Ok(Self {
            container,
            beds_end,
            #[cfg(feature = "compression")]
            compression: None,
        })
    }
    #[cfg(feature = "compression")]
    pub fn compression(&mut self, compression: crate::Compression) -> Result<&mut Self, Error> {
        if self.container.is_sealed() {
            return Err(error("event storage is sealed"));
        }
        compression
            .algorithm
            .validate_level(compression.level)
            .map_err(|e| error(e.to_string()))?;
        self.compression = Some(compression);
        Ok(self)
    }
    /// Supply a complete sealed plain Pack. Capture its length before optional
    /// compression; compressed input cannot provide the uncompressed address.
    pub fn pack(&mut self, pack: impl BoxInput<'a>) -> Result<&mut Self, Error> {
        if self.container.is_sealed() {
            return Err(error("event storage is sealed"));
        }
        let raw = pack.into_bytes()?;
        let header = pil_codec::pac_envelope::Plain::read_prefix(&raw)?;
        if header.size != raw.len() as u64
            || pil_codec::pac::flags(&raw)? & pil_codec::pac::SEALED == 0
        {
            return Err(error(
                "origin insertion requires a complete sealed plain Pack",
            ));
        }
        let beds_end = self
            .beds_end
            .checked_add(header.size)
            .ok_or(Error::Overflow)?;
        let count = (self.container.len() as u64)
            .checked_add(1)
            .ok_or(Error::Overflow)?;
        // Establish representability before changing the writer's membership.
        Container::external_footer_from_payload(beds_end, count, &[], false)?;
        #[cfg(feature = "compression")]
        let raw = if let Some(compression) = &self.compression {
            let mut stored = Vec::new();
            pil_codec::compression::seal(
                &raw,
                compression.algorithm,
                compression.level,
                compression.only_if_smaller,
                &mut stored,
            )
            .map_err(|e| error(e.to_string()))?;
            std::borrow::Cow::Owned(stored)
        } else {
            raw
        };
        match raw {
            std::borrow::Cow::Borrowed(bytes) => {
                self.container.pack(bytes)?;
            }
            std::borrow::Cow::Owned(bytes) => {
                self.container.pack(bytes)?;
            }
        }
        self.beds_end = beds_end;
        Ok(self)
    }
    #[cfg(feature="compression")]
    #[cfg(test)]
    fn filling(&mut self, pack:crate::SeedPack<'_>) -> Result<(),Error> {
        self.container.common_strings()?;
        self.append(pack)
    }
    #[cfg(feature="compression")]
    fn append(&mut self, pack:crate::SeedPack<'_>) -> Result<(),Error> {
        let mut end = self.beds_end;
        pack.inner.seal_into_with(&mut self.container, |raw| {
            end = end.checked_add(raw.len() as u64).ok_or(Error::Overflow)?;
            Self::stored(raw, self.compression.as_ref())
        })?;
        self.beds_end = end;
        Ok(())
    }
    #[cfg(feature="compression")]
    pub(crate) fn selected(&mut self, pack:crate::SeedPack<'_>,
        candidates:&std::collections::HashSet<Box<str>>) -> Result<(),Error> {
        if candidates.is_empty() && !self.container.has_common_strings() {
            return self.append(pack);
        }
        let length = pack.inner.seal_with_strings(&mut self.container, candidates,
            |raw| Self::stored(raw,self.compression.as_ref()))?;
        self.beds_end = self.beds_end.checked_add(length as u64).ok_or(Error::Overflow)?;
        Ok(())
    }
    #[cfg(feature="compression")]
    fn stored(mut raw:Vec<u8>, compression:Option<&crate::Compression>) -> Result<Vec<u8>,Error> {
            crate::integrity::stamp(&mut raw)?;
            if let Some(c) = compression {
                let mut stored = Vec::new();
                pil_codec::compression::seal(&raw,c.algorithm,c.level,c.only_if_smaller,&mut stored)
                    .map_err(|e| error(e.to_string()))?;
                Ok(stored)
            } else { Ok(raw) }
    }
    pub fn next_address(&self) -> u64 {
        self.container.next_address()
    }
    pub fn next_beds_address(&self) -> u64 {
        self.beds_end
    }
    pub fn len(&self) -> usize {
        self.container.len()
    }
    pub fn is_empty(&self) -> bool {
        self.container.is_empty()
    }
    pub fn bytes(&self) -> Result<&[u8], Error> {
        self.container.bytes()
    }
    pub fn publication(&self) -> Result<Publication, Error> {
        let (length, _) =
            Container::external_footer_from_payload(self.beds_end, self.len() as u64, &[], false)?;
        Ok(Publication {
            length,
            count: self.len() as u64,
        })
    }
    fn metadata(&mut self) -> Result<(), Error> {
        let value = self.publication()?.length.to_le_bytes().to_vec();
        if self.container.headers().get(BEDS_LENGTH) != Some(&value) {
            self.container.header(BEDS_LENGTH, value)?;
        }
        Ok(())
    }
    pub fn commit(&mut self) -> Result<Commit<'_>, Error> {
        self.metadata()?;
        self.container.commit()
    }
    pub fn seal(&mut self) -> Result<Commit<'_>, Error> {
        self.metadata()?;
        self.container.seal()
    }
}

#[cfg(all(test, feature = "registration", feature = "compression"))]
mod sealing;
