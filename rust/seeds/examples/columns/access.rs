//! Experimental borrowed columns. RX resolves every field and decodes its
//! value; this adapter only performs checked strided addressing. No wire parser.
use data_seeds::{doc, EncodingMode, Error};
use pil_codec::{api::{FieldBytes, NativeValue, Pack, PillView, Prescription}, authoring_language::language as lang, pac::PacIndex};

pub struct Path {
    pub prescriptions: Vec<Prescription>,
    pub ordinals: Vec<usize>,
}
impl Path {
    pub fn new(rx: &Prescription, names: &[&str]) -> Result<Self, Error> {
        if names.is_empty() { return Err(Error::Invalid("empty column path")); }
        let doc = doc::read(rx.bytes())?;
        let mut member = 0;
        let mut prescriptions = Vec::new();
        let mut ordinals = Vec::new();
        for (depth, name) in names.iter().enumerate() {
            let definition = &doc.definitions()[member];
            let ordinal = definition.fields.iter().position(|f| f.name == *name).ok_or(Error::Invalid("unknown column field"))?;
            let field = &definition.fields[ordinal];
            prescriptions.push(doc.prescribe_member(member)?);
            ordinals.push(ordinal);
            if depth + 1 < names.len() {
                let [kind, reference] = field.base_kind();
                if kind != lang::OBJECT { return Err(Error::Invalid("column path requires fixed objects")); }
                member = reference as usize;
            }
        }
        // This PoC targets numeric aggregate slots. Do not treat dictionary
        // references, bitpacked booleans, or composite payloads as scalars.
        match prescriptions.last().unwrap().native_scalar_kind(*ordinals.last().unwrap()) {
            Some(pil_codec::Scalar::F64) => {},
            _ => return Err(Error::Invalid("prototype column requires float64")),
        }
        Ok(Self { prescriptions, ordinals })
    }
    pub fn offset(&self, row: &PillView<'_>, owner: &[u8]) -> Result<(usize, usize), Error> {
        fn visit(path: &Path, depth: usize, row: &PillView<'_>, owner: &[u8]) -> Result<(usize, usize), Error> {
            if row.mode() != EncodingMode::FixedFull { return Err(Error::Invalid("prototype column requires Fixed Full")); }
            let FieldBytes::Bytes(raw) = row.field(path.ordinals[depth])? else { return Err(Error::Invalid("column field has no value")); };
            if depth + 1 == path.ordinals.len() {
                let start = (raw.as_ptr() as usize).checked_sub(owner.as_ptr() as usize).ok_or(Error::Incomplete)?;
                let end = start.checked_add(raw.len()).ok_or(Error::Overflow)?;
                if owner.get(start..end).is_none() { return Err(Error::Incomplete); }
                Ok((start, raw.len()))
            } else {
                let mut decoder = path.prescriptions[depth + 1].decoder();
                visit(path, depth + 1, &decoder.read(raw)?, owner)
            }
        }
        visit(self, 0, row, owner)
    }
    pub fn value(&self, raw: &[u8]) -> Result<f64, Error> {
        let NativeValue::Scalar(pil_codec::Value::F64(value)) = self.prescriptions.last().unwrap().read_field(*self.ordinals.last().unwrap(), raw)? else {
            return Err(Error::Invalid("column scalar mismatch"));
        };
        Ok(value)
    }
}

/// O(1) descriptor; lifetime pins the original bytes. The slice is strided,
/// not a native &[f64], so no alignment or host-endianness assumption is made.
pub struct Column<'a> {
    bytes: &'a [u8],
    stride: usize,
    count: usize,
    width: usize,
    path: &'a Path,
}
impl<'a> Column<'a> {
    pub fn bind(pack: &Pack<'_>, bytes: &'a [u8], path: &'a Path) -> Result<Self, Error> {
        if !pack.options().fixed || pack.options().common_values || !pack.options().common.is_empty() || pack.is_empty() {
            return Err(Error::Invalid("prototype column requires unfactored fixed members"));
        }
        // Use RX's directory/addressing machinery. Never infer a stride from
        // field widths, nor use the standalone Pill size as its packed size.
        let mut index = PacIndex::default();
        index.scan_structural(path.prescriptions[0].definition(), bytes, &[], bytes.len())?;
        if index.member_count() != pack.len() { return Err(Error::Invalid("column Pack mismatch")); }
        let [first, end] = index.member(0).ok_or(Error::Incomplete)?;
        let stride = end.checked_sub(first).ok_or(Error::Incomplete)?;
        let last = index.member(pack.len() - 1).ok_or(Error::Incomplete)?;
        let expected = stride.checked_mul(pack.len() - 1).and_then(|n| first.checked_add(n)).ok_or(Error::Overflow)?;
        if stride == 0 || last != [expected, expected.checked_add(stride).ok_or(Error::Overflow)?] { return Err(Error::Invalid("nonuniform column members")); }
        let mut cursor = pack.reader();
        let (offset, width) = path.offset(&cursor.get(0)?, bytes)?;
        if offset < first || offset.checked_add(width).is_none_or(|n| n > end) { return Err(Error::Invalid("column outside first member")); }
        let limit = offset.checked_add(stride.checked_mul(pack.len() - 1).ok_or(Error::Overflow)?).and_then(|n| n.checked_add(width)).ok_or(Error::Overflow)?;
        let bytes = bytes.get(offset..limit).ok_or(Error::Incomplete)?;
        Ok(Self { bytes, stride, count: pack.len(), width, path })
    }
    pub fn len(&self) -> usize { self.count }
    pub fn stride(&self) -> usize { self.stride }
    pub fn get(&self, ordinal: usize) -> Result<f64, Error> {
        if ordinal >= self.count { return Err(Error::Invalid("column ordinal outside Pack")); }
        // Construction established that the complete selected span exists.
        let start = ordinal * self.stride;
        self.path.value(&self.bytes[start..start + self.width])
    }
    pub fn sum(&self) -> Result<f64, Error> {
        (0..self.count).try_fold(0.0, |sum, i| Ok(sum + self.get(i)?))
    }
}
