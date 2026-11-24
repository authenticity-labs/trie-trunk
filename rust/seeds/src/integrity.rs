//! Stored plain Seed Packs retain the existing `crc32c` application header.
//! It covers the complete PAC with its four checksum bytes treated as zero.
//! Verification is a pre-write operation, not part of opening or querying a Pack.
use crate::{Error, error};
use std::ops::Range;

pub const HEADER: &str = "crc32c";

fn checksum_range(bytes: &[u8]) -> Result<Range<usize>, Error> {
    let value = pil_codec::pac::application_header(bytes, HEADER)?
        .ok_or_else(|| error("stored uncompressed Seed Pack requires crc32c"))?;
    if value.len() != 4 {
        return Err(error("invalid Seed Pack crc32c header"));
    }
    let start = value.as_ptr() as usize - bytes.as_ptr() as usize;
    Ok(start..start + 4)
}
fn checksum(bytes: &[u8], field: Range<usize>) -> u32 {
    let crc = crc32c::crc32c(&bytes[..field.start]);
    let crc = crc32c::crc32c_append(crc, &[0; 4]);
    crc32c::crc32c_append(crc, &bytes[field.end..])
}

/// The writer reserved the four header bytes before sealing. Patch only those
/// bytes; member offsets, commons and encoded values remain unchanged.
pub fn stamp(bytes: &mut [u8]) -> Result<(), Error> {
    let field = checksum_range(bytes)?;
    let crc = checksum(bytes, field.clone());
    bytes[field].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}
pub fn verify(bytes: &[u8]) -> Result<(), Error> {
    let field = checksum_range(bytes)?;
    let stored = u32::from_le_bytes(bytes[field.clone()].try_into().unwrap());
    if stored != checksum(bytes, field) {
        return Err(error("Seed Pack crc32c mismatch"));
    }
    Ok(())
}

/// Call before publishing received bytes to storage. Live/recovery Packs are
/// unsealed. Compressed Packs retain their compression integrity policy;
/// sealed plain storage requires the header. Readers do not call this.
pub fn before_write(bytes: &[u8]) -> Result<(), Error> {
    let flags = pil_codec::pac::flags(bytes)?;
    if flags & pil_codec::pac::COMPRESSED == 0 && flags & pil_codec::pac::SEALED != 0 {
        verify(bytes)?;
    }
    Ok(())
}
