//! Borrowed RX decoding. Scratch belongs to the traversal, never to every record.
use super::{
    FieldBytes, FieldKey, Pack, Prescription, fixed_payload_field, invalid, raw_field_bytes,
};
use crate::{ABSENT, Definition, EncodingMode, Error, FrameIndex, NULL};
use super::selective::Cursor;

pub struct Decoder<'rx> {
    rx: &'rx Prescription,
    local: Cursor,
    common: FrameIndex,
}
pub struct PackReader<'pack, 'data> {
    pack: &'pack Pack<'data>,
    decoder: Decoder<'pack>,
    cursor: PackCursor,
}
/// A decoded addressing view. Field payloads remain in their original buffer.
/// Its lifetime prevents replacing decoder scratch while the view is in use.
pub struct PillView<'a> {
    rx: &'a Prescription,
    access: Access<'a>,
    common: Option<(&'a [u8], &'a [[u32; 2]])>,
}
enum Access<'a> {
    Uncached { raw: &'a [u8], mask: &'a [u8], prefix: usize, mode: EncodingMode },
    VariableFast {
        raw: &'a [u8],
        cursor: &'a Cursor,
        mask: &'a [u8],
    },
    VariableConcise {
        raw: &'a [u8],
        cursor: &'a Cursor,
        mask: &'a [u8],
    },
    FixedSparse {
        payload: &'a [u8],
        mask: &'a [u8],
    },
    FixedFull {
        payload: &'a [u8],
    },
}
impl Prescription {
    pub fn decoder(&self) -> Decoder<'_> {
        Decoder {
            rx: self,
            local: Cursor::default(),
            common: FrameIndex::default(),
        }
    }
}
impl<'data> Pack<'data> {
    pub fn reader(&self) -> PackReader<'_, 'data> {
        PackReader {
            pack: self,
            decoder: self.rx.decoder(),
            cursor: PackCursor::default(),
        }
    }
}
fn prepare<'a>(
    def: &Definition,
    raw: &'a [u8],
    mask: &'a [u8],
    prefix: usize,
    mode: EncodingMode,
    scratch: &'a mut Cursor,
) -> Result<Access<'a>, Error> {
    match mode {
        EncodingMode::FixedFull | EncodingMode::FixedSparse => {
            let n = def
                .fixed_bytes
                .ok_or(invalid("fixed mode requires fixed field extents"))?;
            let end = prefix
                .checked_add(n)
                .filter(|n| *n < NULL as usize)
                .ok_or(Error::Overflow)?;
            let payload = raw.get(prefix..end).ok_or(Error::Incomplete)?;
            if end != raw.len() {
                return Err(invalid("trailing PIL data"));
            }
            if mode == EncodingMode::FixedFull {
                Ok(Access::FixedFull { payload })
            } else {
                if mask.len() != mode.mask_len(def.fields().len()) {
                    return Err(Error::Incomplete);
                }
                Ok(Access::FixedSparse { payload, mask })
            }
        }
        EncodingMode::VariableFast | EncodingMode::VariableConcise => {
            scratch.reset(def, raw, mask, prefix)?;
            if mode == EncodingMode::VariableFast {
                Ok(Access::VariableFast { raw, cursor: scratch, mask })
            } else {
                Ok(Access::VariableConcise { raw, cursor: scratch, mask })
            }
        }
    }
}
impl Decoder<'_> {
    /// Prepare a borrowed view; only accessed addressing ranges are checked.
    /// Use the view's explicit validate() for a complete record check.
    pub fn read<'a>(&'a mut self, bytes: &'a [u8]) -> Result<PillView<'a>, Error> {
        let mut prefix = 0;
        let (mode, mask) =
            crate::mode::read(bytes, self.rx.definition().fields().len(), &mut prefix)?;
        let access = prepare(
            self.rx.definition(),
            bytes,
            mask,
            prefix,
            mode,
            &mut self.local,
        )?;
        Ok(PillView {
            rx: self.rx,
            access,
            common: None,
        })
    }
    pub(super) fn read_pill<'a>(
        &'a mut self,
        pill: &'a super::Pill<'_>,
    ) -> Result<PillView<'a>, Error> {
        let (raw, packed) = pill.member_parts()?;
        let (mode, mask, prefix) = if let Some((mask, prefix)) = packed {
            (EncodingMode::read(mask)?, mask, prefix)
        } else {
            let mut prefix = 0;
            let (mode, mask) =
                crate::mode::read(raw, self.rx.definition().fields().len(), &mut prefix)?;
            (mode, mask, prefix)
        };
        let common = if pill.common.is_empty() {
            None
        } else {
            if self
                .common
                .scan_structural(self.rx.definition(), &pill.common, pill.common.len())?
                != pill.common.len()
            {
                return Err(invalid("PAC common extent"));
            }
            Some((pill.common.as_ref(), self.common.spans()))
        };
        let access = prepare(
            self.rx.definition(),
            raw,
            mask,
            prefix,
            mode,
            &mut self.local,
        )?;
        Ok(PillView {
            rx: self.rx,
            access,
            common,
        })
    }
}
impl PackReader<'_, '_> {
    pub fn get(&mut self, ordinal: usize) -> Result<PillView<'_>, Error> {
        if ordinal >= self.pack.len() {
            return Err(invalid("PAC ordinal outside pack"));
        }
        if let Some(stored) = &self.pack.sealed {
            self.cursor.read(self.decoder.rx, &stored.base, ordinal)
        } else {
            self.decoder.read(self.pack.raw_member(ordinal)?)
        }
    }
}
#[inline]
fn indexed_field<'a>(
    def: &Definition,
    raw: &'a [u8],
    span: [u32; 2],
    i: usize,
) -> Result<FieldBytes<'a>, Error> {
    match span {
        [ABSENT, _] => Ok(FieldBytes::Absent),
        [NULL, _] => Ok(FieldBytes::Null),
        [s, e] => {
            let bytes = raw.get(s as usize..e as usize).ok_or(Error::Incomplete)?;
            Ok(FieldBytes::Bytes(if def.boolean_count == 0 {
                bytes
            } else {
                raw_field_bytes(def, i, bytes)
            }))
        }
    }
}
impl PillView<'_> {
    #[inline(always)]
    pub fn mode(&self) -> EncodingMode {
        match self.access {
            Access::Uncached { mode, .. } => mode,
            Access::VariableFast { .. } => EncodingMode::VariableFast,
            Access::VariableConcise { .. } => EncodingMode::VariableConcise,
            Access::FixedSparse { .. } => EncodingMode::FixedSparse,
            Access::FixedFull { .. } => EncodingMode::FixedFull,
        }
    }
    #[inline(always)]
    pub fn get<'k>(&self, key: impl Into<FieldKey<'k>>) -> Result<FieldBytes<'_>, Error> {
        let i = match key.into() {
            FieldKey::Position(i) => i,
            FieldKey::Name(n) => match self.rx.positions.get(n) {
                Some(&i) => i,
                None => return Ok(FieldBytes::Absent),
            },
        };
        self.source::<false>(i).map(|(value, _)| value)
    }
    // Raw access does not need the encoding mode. Specialize it away; only
    // materialization needs to distinguish adaptive and common-value sources.
    #[inline(always)]
    fn source<const MODE: bool>(&self, i: usize) -> Result<(FieldBytes<'_>, EncodingMode), Error> {
        let def = self.rx.definition();
        if i >= def.fields().len() {
            return Ok((
                FieldBytes::Absent,
                if MODE {
                    self.mode()
                } else {
                    EncodingMode::VariableFast
                },
            ));
        }
        let local = match &self.access {
            Access::Uncached { raw, mask, prefix, mode } => {
                let span = Cursor::locate(def, raw, mask, *prefix, *mode, i)?;
                indexed_field(def, raw, span, i)?
            },
            Access::FixedFull { payload } => fixed_payload_field(def, payload, 0, i)?,
            Access::FixedSparse { payload, mask } => match crate::mode::masked_state(mask, i) {
                0 => FieldBytes::Absent,
                1 => FieldBytes::Null,
                2 => fixed_payload_field(def, payload, 0, i)?,
                _ => return Err(invalid("OFFSET field in fixed mode")),
            },
            Access::VariableFast { raw, cursor, mask }
            | Access::VariableConcise { raw, cursor, mask } => {
                let span = cursor.field(def, raw, mask, self.mode(), i)?;
                indexed_field(def, raw, span, i)?
            },
        };
        if local == FieldBytes::Absent {
            if let Some((raw, spans)) = self.common {
                return Ok((
                    indexed_field(def, raw, spans[i], i)?,
                    if MODE {
                        EncodingMode::read(raw)?
                    } else {
                        EncodingMode::VariableFast
                    },
                ));
            }
        }
        Ok((
            local,
            if MODE {
                self.mode()
            } else {
                EncodingMode::VariableFast
            },
        ))
    }
    pub fn field<'k>(&self, key: impl Into<FieldKey<'k>>) -> Result<FieldBytes<'_>, Error> {
        self.get(key)
    }
}

impl PillView<'_> {
    /// Explicit whole-record structural and value validation. Application
    /// annotation handlers remain outside ordinary field access.
    pub fn validate(&self) -> Result<&Self, Error> {
        let mut index = FrameIndex::default();
        let (raw, mask, prefix) = match &self.access {
            Access::Uncached { raw, mask, prefix, .. } => (*raw, *mask, *prefix),
            Access::VariableFast { raw, mask, cursor } | Access::VariableConcise { raw, mask, cursor } =>
                (*raw, *mask, cursor.prefix),
            Access::FixedSparse { payload, mask } => (*payload, *mask, 0),
            Access::FixedFull { payload } => (*payload, &[0xc0][..], 0),
        };
        if index.scan_mask(self.rx.definition(), raw, mask, prefix, raw.len(), true)? != raw.len() {
            return Err(invalid("trailing PIL data"));
        }
        self.rx.document.validate_native(&index, raw)?;
        if let Some((raw, common)) = self.common {
            if common.iter().zip(index.spans()).any(|(c,m)|c[0]!=ABSENT && m[0]!=ABSENT) {
                return Err(invalid("PAC common override"));
            }
            if index.scan(self.rx.definition(), raw, raw.len())? != raw.len() { return Err(invalid("PAC common extent")); }
            self.rx.document.validate_native(&index, raw)?;
        }
        Ok(self)
    }
    /// Materialize present values using the already located fields.
    pub fn crush(
        &self,
    ) -> Result<std::collections::BTreeMap<String, Option<serde_json::Value>>, Error> {
        if let Access::Uncached { raw, .. } = self.access {
            return self.rx.decoder().read(raw)?.crush();
        }
        let mut result = std::collections::BTreeMap::new();
        for (i, field) in self.rx.definition().fields().iter().enumerate() {
            if let Some(value) = self.json_field(i)? {
                result.insert(field.name.clone(), Some(value));
            }
        }
        Ok(result)
    }
    /// Unknowns are omitted, duplicates evaluated once, and selected absent fields return None.
    pub fn crush_fields<'k, K: Clone + Into<FieldKey<'k>>>(
        &self,
        fields: &[K],
    ) -> Result<std::collections::BTreeMap<String, Option<serde_json::Value>>, Error> {
        if let Access::Uncached { raw, .. } = self.access {
            return self.rx.decoder().read(raw)?.crush_fields(fields);
        }
        let mut result = std::collections::BTreeMap::new();
        for key in fields {
            let i = match key.clone().into() {
                FieldKey::Position(i) => i,
                FieldKey::Name(n) => match self.rx.positions.get(n) {
                    Some(&i) => i,
                    None => continue,
                },
            };
            let Some(field) = self.rx.definition().fields().get(i) else {
                continue;
            };
            if result.contains_key(&field.name) {
                continue;
            }
            result.insert(field.name.clone(), self.json_field(i)?);
        }
        Ok(result)
    }
    fn field_state(&self, i: usize) -> u8 {
        let state = match self.access {
            Access::FixedFull { .. } => 2,
            Access::FixedSparse { mask, .. }
            | Access::VariableFast { mask, .. }
            | Access::VariableConcise { mask, .. }
            | Access::Uncached { mask, .. } => crate::mode::masked_state(mask, i),
        };
        if state == 0 {
            if let Some((raw, _)) = self.common {
                return crate::mode::state(raw, i);
            }
        }
        state
    }
    fn json_field(&self, i: usize) -> Result<Option<serde_json::Value>, Error> {
        let (field_bytes, mode) = self.source::<true>(i)?;
        let bytes = match field_bytes {
            FieldBytes::Absent => return Ok(None),
            FieldBytes::Null => return Ok(Some(serde_json::Value::Null)),
            FieldBytes::Bytes(bytes) => bytes,
        };
        if let Some(value) = self.rx.document.project_field(i, bytes) {
            return value.map(Some);
        }
        let def = self.rx.definition();
        let field = &def.fields()[i];
        let value = if def.boolean_bits[i].is_some() {
            crate::Value::Bool(bytes[0] != 0)
        } else if !field.values.is_empty() && self.field_state(i) == 2 {
            if bytes.len() > 8 {
                return Err(invalid("mapped reference width"));
            }
            let mut n = [0; 8];
            n[..bytes.len()].copy_from_slice(bytes);
            crate::Value::Mapped(u64::from_le_bytes(n))
        } else {
            match field.kind {
                crate::Type::Scalar(_) => def.integer_value(i, mode, bytes)?,
                crate::Type::Array(kind) => crate::Value::Array(crate::Array::Encoded(
                    crate::ArrayView::parse(kind, bytes)?,
                )),
            }
        };
        crate::json_view::value_json(value, &field.values)
            .map(Some)
            .map_err(|e| Error::Definition(e.to_string()))
    }
}

/// Immutable encoded bytes and their Prescription. Share this owner across
/// readers; addressing caches live in OwnedPillReader, never in the owner.
pub struct OwnedPill {
    rx: std::sync::Arc<Prescription>,
    bytes: Vec<u8>,
    mode: EncodingMode,
    prefix: usize,
}
impl OwnedPill {
    pub fn new(rx: std::sync::Arc<Prescription>, bytes: Vec<u8>) -> Result<Self, Error> {
        let mut prefix = 0;
        let (mode, mask) = crate::mode::read(&bytes, rx.definition().fields().len(), &mut prefix)?;
        let mut scratch = Cursor::default();
        if mode.fixed() {
            prepare(rx.definition(), &bytes, mask, prefix, mode, &mut scratch)?;
        } else {
            scratch.start(rx.definition(), &bytes, mask, prefix)?;
        }
        Ok(Self { rx, bytes, mode, prefix })
    }
    /// A scratch-free selective view. Hold a reader when accessing many fields
    /// or repeatedly reading the same Pill, to reuse its addressing cache.
    pub fn view(&self) -> PillView<'_> {
        let access = match self.mode {
            EncodingMode::FixedFull => Access::FixedFull { payload: &self.bytes[self.prefix..] },
            EncodingMode::FixedSparse => Access::FixedSparse {
                payload: &self.bytes[self.prefix..], mask: &self.bytes[..self.prefix],
            },
            mode => Access::Uncached {
                raw: &self.bytes, mask: &self.bytes[..self.prefix], prefix: self.prefix, mode,
            },
        };
        PillView { rx: &self.rx, access, common: None }
    }
    pub fn reader(self: &std::sync::Arc<Self>) -> Result<OwnedPillReader, Error> {
        OwnedPillReader::shared(self.clone())
    }
    pub fn prescription(&self) -> &Prescription { &self.rx }
    pub fn into_bytes(self) -> Vec<u8> { self.bytes }
}
/// One traversal's lazy addressing state, with shared immutable backing bytes.
/// Creating another reader shares bytes and the compiled Definition, not caches.
pub struct OwnedPillReader {
    pill: std::sync::Arc<OwnedPill>,
    index: Cursor,
}
impl OwnedPillReader {
    pub fn new(rx: std::sync::Arc<Prescription>, bytes: Vec<u8>) -> Result<Self, Error> {
        Self::shared(std::sync::Arc::new(OwnedPill::new(rx, bytes)?))
    }
    pub fn shared(pill: std::sync::Arc<OwnedPill>) -> Result<Self, Error> {
        let mut index = Cursor::default();
        prepare(pill.rx.definition(), &pill.bytes, &pill.bytes[..pill.prefix], pill.prefix, pill.mode, &mut index)?;
        Ok(Self { pill, index })
    }
    pub fn view(&self) -> PillView<'_> {
        let p=&self.pill;
        let access = match p.mode {
            EncodingMode::FixedFull => Access::FixedFull { payload: &p.bytes[p.prefix..] },
            EncodingMode::FixedSparse => Access::FixedSparse { payload: &p.bytes[p.prefix..], mask: &p.bytes[..p.prefix] },
            EncodingMode::VariableFast => Access::VariableFast { raw: &p.bytes, cursor: &self.index, mask: &p.bytes[..p.prefix] },
            EncodingMode::VariableConcise => Access::VariableConcise { raw: &p.bytes, cursor: &self.index, mask: &p.bytes[..p.prefix] },
        };
        PillView { rx: &p.rx, access, common: None }
    }
    pub fn prescription(&self) -> &Prescription { &self.pill.rx }
    pub fn owner(&self) -> &std::sync::Arc<OwnedPill> { &self.pill }
}
/// Host adapters receive interpreted values, never PIL masks or offsets.
/// Composite values currently use the existing registered composite decoder.
pub enum NativeValue<'a> {
    Scalar(crate::Value<'a>),
    Composite(ciborium::Value),
}
impl PillView<'_> {
    /// Presence lookup uses the prepared field states without decoding a value.
    pub fn is_present(&self, i: usize) -> Result<bool, Error> {
        if i >= self.rx.definition().fields().len() {
            return Err(invalid("field ordinal"));
        }
        Ok(self.field_state(i) != 0)
    }
    pub fn native_value(&self, i: usize) -> Result<NativeValue<'_>, Error> {
        use crate::Value;
        let (raw, mode) = self.source::<true>(i)?;
        let bytes = match raw {
            FieldBytes::Absent => return Ok(NativeValue::Scalar(Value::Absent)),
            FieldBytes::Null => return Ok(NativeValue::Scalar(Value::Null)),
            FieldBytes::Bytes(b) => b,
        };
        if let Some(value) = self.rx.document.native_field(i, bytes) {
            return value.map(NativeValue::Composite);
        }
        let def = self.rx.definition();
        let f = &def.fields()[i];
        let value = if def.boolean_bits[i].is_some() {
            Value::Bool(bytes[0] != 0)
        } else if !f.values.is_empty() && self.field_state(i) == 2 {
            if bytes.len() > 8 {
                return Err(invalid("mapped reference width"));
            }
            let mut n = [0; 8];
            n[..bytes.len()].copy_from_slice(bytes);
            let i = usize::try_from(u64::from_le_bytes(n)).map_err(|_| Error::Overflow)?;
            Value::String(
                f.values
                    .get(i)
                    .ok_or(invalid("invalid dictionary reference"))?,
            )
        } else {
            match f.kind {
                crate::Type::Scalar(_) => def.integer_value(i, mode, bytes)?,
                crate::Type::Array(k) => {
                    Value::Array(crate::Array::Encoded(crate::ArrayView::parse(k, bytes)?))
                }
            }
        };
        Ok(NativeValue::Scalar(value))
    }
}

impl Prescription {
    /// Select a host primitive accessor once, without exposing record layout.
    pub fn native_scalar_kind(&self, i: usize) -> Option<crate::Scalar> {
        let field = self.definition().fields().get(i)?;
        if !field.values.is_empty() || self.document.has_native_projection(i) {
            return None;
        }
        match field.kind {
            crate::Type::Scalar(s) => Some(s),
            _ => None,
        }
    }
}

/// Immutable PAC bytes and its checked opening index, shared between readers.
pub struct OwnedPack {
    rx: std::sync::Arc<Prescription>,
    base: super::Base<'static>,
}
/// A reader owns its traversal cursor and shares the immutable PAC opening plan.
pub struct OwnedPackReader {
    pack: std::sync::Arc<OwnedPack>,
    cursor: PackCursor,
}
#[derive(Default)]
struct PackCursor {
    index: Cursor,
    mask: Vec<u8>,
    ordinal: Option<usize>,
    prefix: usize,
}
impl PackCursor {
    fn read<'a>(&'a mut self, rx: &'a Prescription, base: &'a super::Base<'_>, ordinal: usize) -> Result<PillView<'a>, Error> {
        let [s,e] = base.index.member(ordinal).ok_or(invalid("PAC ordinal outside pack"))?;
        let shared = base.index.is_common_only();
        let [s,e] = if shared { base.index.common() } else { [s,e] };
        let raw = &base.bytes[s as usize..e as usize];
        let cache_ordinal = if shared { 0 } else { ordinal };
        let expected = base.index.member_mode();
        let mode = match expected {
            0 => EncodingMode::VariableFast,
            1 => EncodingMode::VariableConcise,
            2 => EncodingMode::FixedSparse,
            3 => EncodingMode::FixedFull,
            _ => return Err(invalid("PAC member mode")),
        };
        if self.ordinal != Some(cache_ordinal) {
            self.ordinal = None;
            let (actual, mask, prefix) = if expected == 3 && !shared {
                (EncodingMode::FixedFull, &[0xc0][..], 0)
            } else if let Some(presence) = base.index.presence() {
                let prefix = presence.mask_into(raw, &mut self.mask)?;
                (EncodingMode::read(&self.mask)?, self.mask.as_slice(), prefix)
            } else {
                let mut prefix = 0;
                let (mode, mask) = crate::mode::read(raw, rx.definition().fields().len(), &mut prefix)?;
                (mode, mask, prefix)
            };
            if actual != mode { return Err(invalid("PAC member mode mismatch")); }
            prepare(rx.definition(), raw, mask, prefix, mode, &mut self.index)?;
            self.prefix = prefix;
            self.ordinal = Some(cache_ordinal);
        }
        let mask = if expected == 3 && !shared { &[0xc0][..] }
            else if base.index.presence().is_some() { &self.mask }
            else { &raw[..self.prefix] };
        let access = match mode {
            EncodingMode::FixedFull => Access::FixedFull { payload: &raw[self.prefix..] },
            EncodingMode::FixedSparse => Access::FixedSparse { payload: &raw[self.prefix..], mask },
            EncodingMode::VariableFast => Access::VariableFast { raw, cursor: &self.index, mask },
            EncodingMode::VariableConcise => Access::VariableConcise { raw, cursor: &self.index, mask },
        };
        let [s,e] = base.index.common();
        let common = (s != e && !shared).then(|| (&base.bytes[s as usize..e as usize], base.index.common_spans()));
        Ok(PillView { rx, access, common })
    }
}
impl OwnedPack {
    pub fn new(rx: std::sync::Arc<Prescription>, bytes: Vec<u8>) -> Result<Self, Error> {
        let pack = rx.read_pack(std::borrow::Cow::Owned(bytes))?;
        let base = pack.base;
        let base = super::Base { bytes: std::borrow::Cow::Owned(base.bytes.into_owned()), index: base.index };

        Ok(Self { rx, base })
    }
    pub fn len(&self) -> usize { self.base.index.member_count() }
    pub fn is_empty(&self) -> bool { self.len() == 0 }
    pub fn reader(self: &std::sync::Arc<Self>) -> OwnedPackReader {
        OwnedPackReader { pack: self.clone(), cursor: PackCursor::default() }
    }
    pub fn prescription(&self) -> &Prescription { &self.rx }
}
impl OwnedPackReader {
    pub fn new(rx: std::sync::Arc<Prescription>, bytes: Vec<u8>) -> Result<Self, Error> {
        Ok(std::sync::Arc::new(OwnedPack::new(rx, bytes)?).reader())
    }
    pub fn get(&mut self, ordinal: usize) -> Result<PillView<'_>, Error> {
        self.cursor.read(&self.pack.rx, &self.pack.base, ordinal)
    }
    pub fn len(&self) -> usize { self.pack.len() }
    pub fn is_empty(&self) -> bool { self.pack.is_empty() }
    pub fn prescription(&self) -> &Prescription { &self.pack.rx }
    pub fn owner(&self) -> &std::sync::Arc<OwnedPack> { &self.pack }
}
