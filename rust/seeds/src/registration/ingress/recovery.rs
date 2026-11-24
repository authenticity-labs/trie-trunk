//! Recovery framing around complete PILs. These bytes belong to the ingress
//! spool, never to PIL, PAC, BOX, or the HTTP request body.
use crate::{Error, error};

pub const PREFIX: usize = 10;

/// Existing length/CRC framing plus the registered Definition reference that
/// the HTTP request supplied. The full PIL remains byte-for-byte unchanged.
pub fn prefix(branch: u16, pill: &[u8]) -> Result<[u8; PREFIX], Error> {
    if branch == 0 {
        return Err(error("invalid recovery Definition"));
    }
    framing(branch, pill)
}

/// A zero reference identifies an internal commit checkpoint, never a Pill.
pub fn checkpoint(payload: &[u8]) -> Result<[u8; PREFIX], Error> {
    framing(0, payload)
}

/// Opaque adapter checkpoint encoded through PIL's existing object codec.
pub fn checkpoint_bytes(payload: &[u8]) -> Result<Vec<u8>, Error> {
    let body = pil_codec::object::encode_value(&ciborium::Value::Bytes(payload.to_vec()))?;
    let mut bytes = checkpoint(&body)?.to_vec();
    bytes.extend_from_slice(&body);
    Ok(bytes)
}
pub fn restore_bytes(bytes: &[u8], maximum: usize) -> Result<Vec<u8>, Error> {
    let (kind, body) = read(bytes, maximum)?;
    if kind != 0 { return Err(error("expected publication checkpoint")); }
    match pil_codec::object::decode_value(body)? {
        ciborium::Value::Bytes(bytes) => Ok(bytes),
        _ => Err(error("invalid publication checkpoint")),
    }
}

fn framing(branch: u16, pill: &[u8]) -> Result<[u8; PREFIX], Error> {
    if pill.is_empty() {
        return Err(error("invalid recovery record"));
    }
    let length = u32::try_from(pill.len()).map_err(|_| Error::Overflow)?;
    let mut out = [0; PREFIX];
    out[..4].copy_from_slice(&length.to_le_bytes());
    out[8..].copy_from_slice(&branch.to_le_bytes());
    let crc = crc32c::crc32c_append(crc32c::crc32c(&out[8..]), pill);
    out[4..8].copy_from_slice(&crc.to_le_bytes());
    Ok(out)
}

pub fn length(prefix: &[u8], maximum: usize) -> Result<usize, Error> {
    let prefix = prefix.get(..PREFIX).ok_or(Error::Incomplete)?;
    let length = u32::from_le_bytes(prefix[..4].try_into().unwrap()) as usize;
    if length == 0 || length > maximum {
        return Err(error("invalid recovery record length"));
    }
    length.checked_add(PREFIX).ok_or(Error::Overflow)
}

/// Recovery checks the checksum before admitting a complete record. A partial
/// trailing record is distinguishable from corruption in a complete record.
pub fn read(raw: &[u8], maximum: usize) -> Result<(u16, &[u8]), Error> {
    let length = length(raw, maximum)?;
    let raw = raw.get(..length).ok_or(Error::Incomplete)?;
    let branch = u16::from_le_bytes(raw[8..PREFIX].try_into().unwrap());
    let pill = &raw[PREFIX..];
    if framing(branch, pill)?[..PREFIX] != raw[..PREFIX] {
        return Err(error("recovery checksum mismatch"));
    }
    Ok((branch, pill))
}
