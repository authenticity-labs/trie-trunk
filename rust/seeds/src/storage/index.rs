//! Existing branch.indx hour segments: six-byte header followed by six-byte rows.
//! The event ordinal is its row position; no ordinal is added to the Seed.
use crate::{Error, SeedDefinition, StorageBinding, error};
use pil_codec::{Scalar, Type, Value, api::NativeValue};
use std::{ops::Range, sync::Arc};
const ROW_BYTES: usize = 6;
const HOUR_MS: u64 = 3_600_000;

#[derive(Clone, Copy, Debug)]
pub struct IndexFields {
    time: usize,
    leaf: Option<usize>,
}
impl IndexFields {
    pub(crate) fn read_time(self, row: &pil_codec::api::PillView<'_>) -> Result<u64, Error> {
        unsigned(row.native_value(self.time)?)
            .ok_or_else(|| error("ingress requires a present millisecond timestamp"))
    }
    #[cfg(feature="registration")]
    pub(crate) fn bound_time(self, row:&crate::storage::BoundRow<'_>)->Result<u64,Error>{
        unsigned(row.native_value(self.time)?).ok_or_else(||error("missing summary recovery timestamp"))
    }
    pub(crate) fn read_leaf(self, row: &pil_codec::api::PillView<'_>) -> Result<u16, Error> {
        match self.leaf {
            None => Ok(0),
            Some(ordinal) => match row.native_value(ordinal)? {
                NativeValue::Scalar(Value::Absent | Value::Null) => Ok(0),
                value => unsigned(value).and_then(|n| u16::try_from(n).ok())
                    .ok_or_else(|| error("invalid state leaf reference")),
            },
        }
    }
    pub(crate) fn has_leaf(self) -> bool {
        self.leaf.is_some()
    }
    pub(crate) fn contains(self, field: usize) -> bool {
        field == self.time || Some(field) == self.leaf
    }
    pub(crate) fn value(self, time: u64, leaf: u16, field: usize) -> Option<NativeValue<'static>> {
        if field == self.time {
            Some(NativeValue::Scalar(Value::U64(time)))
        } else if Some(field) == self.leaf {
            Some(NativeValue::Scalar(Value::U16(leaf)))
        } else {
            None
        }
    }
    /// Compile application annotations once, outside Pill Packer.
    pub fn from_definition(definition: &SeedDefinition) -> Result<Self, Error> {
        let mut time = None;
        let mut leaf = None;
        for (i, field) in definition
            .prescription()
            .definition()
            .fields()
            .iter()
            .enumerate()
        {
            for annotation in definition.annotations(i).unwrap_or_default() {
                match annotation.as_str() {
                    "@time" | "@time(ms)" => {
                        if time.replace(i).is_some() {
                            return Err(error("duplicate time binding"));
                        }
                        if !matches!(field.kind, Type::Scalar(Scalar::Uint(6) | Scalar::U64)) {
                            return Err(error(
                                "millisecond index requires uint(6) or uint(8) time",
                            ));
                        }
                    }
                    "@time(us)" | "@time(ns)" => {
                        return Err(error("sub-millisecond time indexes are not implemented"));
                    }
                    "@leaf" => {
                        if leaf.replace(i).is_some() {
                            return Err(error("duplicate leaf binding"));
                        }
                        if field.kind != Type::Scalar(Scalar::U16) {
                            return Err(error("state leaf index requires uint(2)"));
                        }
                    }
                    _ => (),
                }
            }
        }
        Ok(Self {
            time: time.ok_or_else(|| error("missing time binding"))?,
            leaf,
        })
    }
}

fn unsigned(value: NativeValue<'_>) -> Option<u64> {
    match value {
        NativeValue::Scalar(Value::U64(n)) => Some(n),
        NativeValue::Scalar(Value::U16(n)) => Some(u64::from(n)),
        NativeValue::Scalar(Value::U128(n)) => u64::try_from(n).ok(),
        // Nonstandard declared widths use the codec's lossless native value.
        NativeValue::Composite(ciborium::Value::Integer(n)) => u64::try_from(n).ok(),
        _ => None,
    }
}

/// An immutable shared range, suitable for sparse replication and reader pools.
pub struct HourIndex {
    bytes: Arc<Vec<u8>>,
    range: Range<usize>,
    hour: u64,
    fields: IndexFields,
}
impl HourIndex {
    pub const ROW_BYTES: usize = ROW_BYTES;
    pub fn open(
        bytes: Arc<Vec<u8>>,
        range: Range<usize>,
        hour: u64,
        fields: IndexFields,
    ) -> Result<Self, Error> {
        if hour % HOUR_MS != 0 || hour >= 1 << 48 {
            return Err(error("invalid index hour"));
        }
        let data = bytes.get(range.clone()).ok_or(Error::Incomplete)?;
        if data.len() < ROW_BYTES || data.len() % ROW_BYTES != 0 {
            return Err(error("invalid hour index length"));
        }
        Self::check_header(&data[..ROW_BYTES], hour)?;
        Ok(Self {
            bytes,
            range,
            hour,
            fields,
        })
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes[self.range.clone()]
    }
    pub fn time_leaf(&self, row: usize) -> Result<(u64, u16), Error> {
        if row >= self.row_count() {
            return Err(error("index row outside hour"));
        }
        let offset = (row + 1) * ROW_BYTES;
        let raw = &self.bytes()[offset..offset + ROW_BYTES];
        Self::decode_row(raw, self.hour)
    }
    pub(crate) fn check_header(raw: &[u8], hour: u64) -> Result<(), Error> {
        if raw.len() != ROW_BYTES || hour % HOUR_MS != 0 || hour >= 1 << 48 {
            return Err(error("invalid index hour"));
        }
        let high = u32::from_le_bytes(raw[..4].try_into().unwrap()) as u64;
        if high != hour >> 32 || raw[4..6] != [0, 0] {
            return Err(error("invalid index hour prefix"));
        }
        Ok(())
    }
    pub(crate) fn decode_row(raw: &[u8], hour: u64) -> Result<(u64, u16), Error> {
        let raw: &[u8; ROW_BYTES] = raw.try_into().map_err(|_| Error::Incomplete)?;
        let mut time =
            (hour & !0xffff_ffff) | u32::from_le_bytes(raw[..4].try_into().unwrap()) as u64;
        if time < hour {
            time += 1 << 32;
        }
        if time >= hour + HOUR_MS || time >= 1 << 48 {
            return Err(error("index time outside hour"));
        }
        Ok((time, u16::from_le_bytes(raw[4..].try_into().unwrap())))
    }
}
impl StorageBinding for HourIndex {
    fn row_count(&self) -> usize {
        self.range.len() / ROW_BYTES - 1
    }
    fn value(&self, row: usize, field: usize) -> Result<Option<NativeValue<'_>>, Error> {
        if !self.fields.contains(field) {
            return Ok(None);
        }
        let (time, leaf) = self.time_leaf(row)?;
        Ok(self.fields.value(time, leaf, field))
    }
}

/// Write the existing index bytes alongside typed Seed ingress.
pub struct HourIndexBuilder {
    bytes: Vec<u8>,
    hour: u64,
    fields: IndexFields,
}
impl HourIndexBuilder {
    pub fn new(hour: u64, expected_rows: usize, fields: IndexFields) -> Result<Self, Error> {
        if hour % HOUR_MS != 0 || hour >= 1 << 48 {
            return Err(error("invalid index hour"));
        }
        let capacity = expected_rows
            .checked_add(1)
            .and_then(|n| n.checked_mul(ROW_BYTES))
            .ok_or(Error::Overflow)?;
        let mut bytes = Vec::new();
        bytes.try_reserve(capacity).map_err(|_| Error::Overflow)?;
        bytes.extend_from_slice(&((hour >> 32) as u32).to_le_bytes());
        bytes.extend_from_slice(&[0, 0]);
        Ok(Self {
            bytes,
            hour,
            fields,
        })
    }
    pub fn append(&mut self, time: u64, leaf: u16) -> Result<(), Error> {
        if time < self.hour || time >= self.hour + HOUR_MS || time >= 1 << 48 {
            return Err(error("index time outside hour"));
        }
        self.bytes.extend_from_slice(&(time as u32).to_le_bytes());
        self.bytes.extend_from_slice(&leaf.to_le_bytes());
        Ok(())
    }
    pub fn finish(self) -> HourIndex {
        let end = self.bytes.len();
        HourIndex {
            bytes: Arc::new(self.bytes),
            range: 0..end,
            hour: self.hour,
            fields: self.fields,
        }
    }
}
