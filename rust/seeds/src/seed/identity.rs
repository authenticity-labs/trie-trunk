//! Compact event identity is a Seed semantic, never a PIL layout change.
use crate::{Error, error};
use pil_codec::{Scalar, Type, Value, api::{NativeValue, Prescription}};

/// Canonical UUID text used by query literals. Decode once at binding time.
pub(crate) fn parse(text: &str) -> Result<[u8; 16], Error> {
    let input = text.as_bytes();
    if input.len() != 36 || [8, 13, 18, 23].iter().any(|&i| input[i] != b'-') {
        return Err(error("expected a hyphenated UUID"));
    }
    let mut out = [0; 16];
    let mut at = 0;
    for (i, slot) in out.iter_mut().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) { at += 1; }
        let digit = |b: u8| (b as char).to_digit(16).ok_or_else(|| error("invalid UUID digit"));
        *slot = ((digit(input[at])? << 4) | digit(input[at + 1])?) as u8;
        at += 2;
    }
    Ok(out)
}

pub(crate) fn millis(uuid: &[u8]) -> Option<u64> {
    if uuid.len() != 16 || uuid[6] >> 4 != 7 || uuid[8] >> 6 != 2 { return None; }
    Some(uuid[..6].iter().fold(0, |ms, b| ms << 8 | u64::from(*b)))
}

/// Resolve annotations once per prescription, including reordered revisions.
#[derive(Clone, Copy)]
pub(crate) struct Identity {
    pub id: usize,
    time: usize,
    units_per_ms: u64,
}
impl Identity {
    pub fn new(rx: &Prescription, annotations: &[Vec<String>]) -> Option<Self> {
        let id = annotations.iter().position(|a| a.iter().any(|a| matches!(a.as_str(),"@event"|"@log")))?;
        if rx.definition().fields()[id].kind != Type::Scalar(Scalar::FixedBytes(10)) {
            return None;
        }
        let (time, units_per_ms) = annotations.iter().enumerate().find_map(|(i, a)| {
            a.iter().find_map(|a| Some((i, match a.as_str() {
                "@time" | "@time(ms)" => 1,
                "@time(us)" => 1_000,
                "@time(ns)" => 1_000_000,
                _ => return None,
            })))
        })?;
        Some(Self { id, time, units_per_ms })
    }

    pub fn read<'a>(&self, read: impl Fn(usize) -> Result<NativeValue<'a>, Error>) -> Result<NativeValue<'a>, Error> {
        let id = match read(self.id)? {
            v @ NativeValue::Scalar(Value::Absent | Value::Null) => return Ok(v),
            NativeValue::Scalar(Value::Bytes(v)) if v.len() == 10 => v,
            _ => return Err(error("event identity requires ten bytes")),
        };
        let time = match read(self.time)? {
            NativeValue::Scalar(Value::U8(v)) => v as u128,
            NativeValue::Scalar(Value::U16(v)) => v as u128,
            NativeValue::Scalar(Value::U24(v) | Value::U32(v)) => v as u128,
            NativeValue::Scalar(Value::U64(v)) => v as u128,
            NativeValue::Scalar(Value::U128(v)) => v,
            NativeValue::Scalar(Value::I64(v) | Value::TimestampMs(v) | Value::TimestampUs(v) | Value::TimestampNs(v)) if v >= 0 => v as u128,
            _ => return Err(error("event identity requires a nonnegative timestamp")),
        };
        let millis = time / self.units_per_ms as u128;
        if millis >= 1 << 48 { return Err(error("event timestamp exceeds UUIDv7 range")); }
        let mut bytes = [0; 16];
        bytes[..6].copy_from_slice(&(millis as u64).to_be_bytes()[2..]);
        bytes[6..].copy_from_slice(id);
        if self.units_per_ms != 1 {
            let fraction = ((time % self.units_per_ms as u128) * 4096 / self.units_per_ms as u128) as u16;
            bytes[6..8].copy_from_slice(&fraction.to_be_bytes());
        }
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Ok(NativeValue::Scalar(Value::Uuid(bytes)))
    }
}
