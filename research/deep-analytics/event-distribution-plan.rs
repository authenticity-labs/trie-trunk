//! Fixed summary objects selected by #summ(class). Aggregation and projection
//! belong to Seeds; all stored fields and nested objects use the Rust RX codec.
use super::{SummaryClass, SummaryField, SummaryLayout};
use crate::{Document, Error, Field, Object, Seed, SeedDefinition, doc, error};
use ciborium::Value as Data;
use pil_codec::{
    Scalar, Value,
    api::{NativeValue, PillView, Prescription},
};
use std::{cmp::Ordering, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Number {
    Unsigned(u128),
    Signed(i128),
    Float(f64),
}
impl Number {
    fn zero(s: Scalar) -> Self {
        match s {
            Scalar::I8 | Scalar::I16 | Scalar::I32 | Scalar::I64 | Scalar::Int(_) => {
                Self::Signed(0)
            }
            Scalar::F32 | Scalar::F64 => Self::Float(0.0),
            _ => Self::Unsigned(0),
        }
    }
    fn native(v: NativeValue<'_>) -> Result<Self, Error> {
        let NativeValue::Scalar(v) = v else {
            return Err(error("summary expects a numeric scalar"));
        };
        Ok(match v {
            Value::U8(v) => Self::Unsigned(v.into()),
            Value::U16(v) => Self::Unsigned(v.into()),
            Value::U24(v) | Value::U32(v) => Self::Unsigned(v.into()),
            Value::U64(v) => Self::Unsigned(v.into()),
            Value::U128(v) => Self::Unsigned(v),
            Value::I8(v) => Self::Signed(v.into()),
            Value::I16(v) => Self::Signed(v.into()),
            Value::I32(v) => Self::Signed(v.into()),
            Value::I64(v) => Self::Signed(v.into()),
            Value::I128(v) => Self::Signed(v),
            Value::F32(v) => Self::Float(v.into()),
            Value::F64(v) => Self::Float(v),
            _ => return Err(error("summary expects a numeric scalar")),
        })
    }
    fn from_data(v: &Data, scalar: Scalar) -> Result<Self, Error> {
        Ok(match Self::zero(scalar) {
            Self::Unsigned(_) => Self::Unsigned(pil_codec::unsigned_native(v)?),
            Self::Signed(_) => Self::Signed(pil_codec::signed_native(v)?),
            Self::Float(_) => Self::Float(data_float(v)?),
        })
    }
    fn data(self) -> Data {
        match self {
            Self::Unsigned(v) => pil_codec::native_unsigned(v),
            Self::Signed(v) => pil_codec::native_signed(v),
            Self::Float(v) => Data::Float(v),
        }
    }
    fn finite(self) -> bool {
        !matches!(self,Self::Float(v) if !v.is_finite())
    }
    fn compare(self, other: Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Unsigned(a), Self::Unsigned(b)) => Some(a.cmp(&b)),
            (Self::Signed(a), Self::Signed(b)) => Some(a.cmp(&b)),
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(&b),
            _ => None,
        }
    }
}
#[derive(Clone, Debug)]
struct Numeric {
    min: Number,
    max: Number,
    sum: Number,
    compensation: f64,
    nonfinite: u64,
    overflow: bool,
    histogram: Vec<u64>,
}
impl Numeric {
    fn new(s: Scalar, bins: usize) -> Self {
        let z = Number::zero(s);
        Self {
            min: z,
            max: z,
            sum: z,
            compensation: 0.0,
            nonfinite: 0,
            overflow: false,
            histogram: vec![0; bins],
        }
    }
    fn add(&mut self, value: Number) {
        if self.overflow {
            return;
        }
        match (self.sum, value) {
            (Number::Unsigned(a), Number::Unsigned(b)) => match a.checked_add(b) {
                Some(v) => self.sum = Number::Unsigned(v),
                None => self.overflow = true,
            },
            (Number::Signed(a), Number::Signed(b)) => match a.checked_add(b) {
                Some(v) => self.sum = Number::Signed(v),
                None => self.overflow = true,
            },
            (Number::Float(a), Number::Float(b)) => {
                let next = a + b;
                self.compensation += if a.abs() >= b.abs() {
                    (a - next) + b
                } else {
                    (b - next) + a
                };
                self.sum = Number::Float(next);
                self.overflow = !next.is_finite()
                    || !self.compensation.is_finite()
                    || !(next + self.compensation).is_finite();
            }
            _ => self.overflow = true,
        }
    }
}
#[derive(Clone, Debug)]
enum Statistics {
    Numeric(Numeric),
    Boolean(u64),
    Categories(Vec<u64>),
}
#[derive(Clone, Debug)]
struct FieldState {
    count: u64,
    nulls: u64,
    stats: Statistics,
}
#[derive(Clone)]
struct Binding {
    field: SummaryField,
    scalar: Option<Scalar>,
    rx: Arc<Prescription>,
    histogram: Vec<Number>,
}

/// Mutable, bounded state for one period. Its shape is bound to its plan;
/// invalid input poisons the bucket instead of publishing partial statistics.
#[derive(Clone)]
pub struct SummaryBucket {
    identity: Arc<()>,
    count: u64,
    fields: Vec<FieldState>,
    valid: bool,
}
impl SummaryBucket {
    pub fn count(&self) -> u64 {
        self.count
    }
}

/// One generated fixed RX shape and its compiled source bindings. Reuse it for
/// every five-minute bucket in the daily Pack and all subsequent daily Packs.
#[derive(Clone)]
pub struct SummaryPlan {
    source: Arc<SeedDefinition>,
    bindings: Arc<[Binding]>,
    identity: Arc<()>,
    document: Document,
    fields_rx: Arc<Prescription>,
    layout: Arc<SummaryLayout>,
}
impl SummaryPlan {
    pub fn new(source: Arc<SeedDefinition>, rows: usize) -> Result<Self, Error> {
        Self::with_profile(source, rows, super::SummaryProfile::new())
    }
    pub fn with_profile(
        source: Arc<SeedDefinition>,
        rows: usize,
        profile: super::SummaryProfile,
    ) -> Result<Self, Error> {
        for name in profile.histograms.keys() {
            if !source
                .summary_fields()
                .iter()
                .any(|f| f.name == *name && f.class == SummaryClass::Numeric)
            {
                return Err(error(format!("{name}: histogram requires #summ(numeric)")));
            }
        }
        if source.summary_fields().is_empty() {
            return Err(error("Definition has no #summ fields"));
        }
        let mut fields_doc = doc::new("summary-fields");
        let mut bindings = Vec::new();
        for f in source.summary_fields() {
            let scalar = source.prescription().native_scalar_kind(f.ordinal);
            let histogram = profile
                .boundaries(&f.name)
                .unwrap_or(&[])
                .iter()
                .map(|v| {
                    Number::from_data(
                        v,
                        scalar.ok_or_else(|| error("histogram requires a numeric field"))?,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            // Float conversion must not collapse distinct authored boundaries.
            if histogram.iter().any(|n| !n.finite())
                || histogram
                    .windows(2)
                    .any(|w| w[0].compare(w[1]) != Some(Ordering::Less))
            {
                return Err(error(
                    "histogram boundaries lose order at the source type's precision",
                ));
            }
            let mut children = vec![uint("count"), uint("null_count")];
            match f.class {
                SummaryClass::Numeric | SummaryClass::Temporal => {
                    let s = scalar.ok_or_else(|| error("summary scalar is unavailable"))?;
                    let (kind, width) = number_type(s)?;
                    children.extend([
                        Field::new("min").r#type(kind).size(width),
                        Field::new("max").r#type(kind).size(width),
                    ]);
                    if f.class == SummaryClass::Numeric {
                        children.push(Field::new("sum").r#type(kind).size(if kind == "float" {
                            8
                        } else {
                            16
                        }));
                        children.push(uint("nonfinite_count"));
                        children.push(Field::new("overflow").r#type("bool"));
                        if kind == "float" {
                            children.push(Field::new("compensation").r#type("float").size(8));
                        }
                    }
                }
                SummaryClass::Boolean => children.push(uint("true_count")),
                SummaryClass::Categorical | SummaryClass::Ordinal => {
                    let count = u16::try_from(f.values.len()).map_err(|_| {
                        error("summary category set exceeds the fixed array capacity")
                    })?;
                    children.push(
                        Field::new("values")
                            .array("uint")
                            .element_size(8)
                            .size(count),
                    );
                    let literal = match f.values[0] {
                        Data::Text(_) => "tstr",
                        Data::Bool(_) => "bool",
                        _ => {
                            if f.values
                                .iter()
                                .any(|v| v.as_integer().is_some_and(|n| i128::from(n) < 0))
                            {
                                "int"
                            } else {
                                "uint"
                            }
                        }
                    };
                    let labels = f
                        .values
                        .iter()
                        .cloned()
                        .map(pil_codec::json_view::data_json)
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| error(e.to_string()))?;
                    children.push(Field::new("mode").enumeration(literal, labels));
                }
            }
            if !histogram.is_empty() {
                children.push(
                    Field::new("histogram")
                        .array("uint")
                        .element_size(8)
                        .size((histogram.len() + 1) as u16),
                );
            }
            let name = format!("summary-{:?}-{}", f.class, f.ordinal);
            let mut subtype = doc::new(&name);
            for field in children {
                subtype = subtype.field(field);
            }
            let subtype = subtype.sign()?;
            let rx = Arc::new(subtype.prescribe()?);
            fields_doc = fields_doc
                .compound(subtype.bytes()?)
                .field(Field::new(&f.name).object(name));
            bindings.push(Binding {
                field: f.clone(),
                scalar,
                rx,
                histogram,
            });
        }
        let fields_doc = fields_doc.sign()?;
        let fields_rx = Arc::new(fields_doc.prescribe()?);
        let document = doc::new(format!(
            "{}-summaries",
            source.prescription().definition().identity()
        ))
        .compound(fields_doc.bytes()?)
        .field(uint("count"))
        .field(Field::new("fields").object("summary-fields"))
        .sign()?;
        let layout = SummaryLayout::with_profile(&document, rows, profile)?;
        Ok(Self {
            source,
            bindings: bindings.into(),
            identity: Arc::new(()),
            document,
            fields_rx,
            layout,
        })
    }
    pub fn document(&self) -> &Document {
        &self.document
    }
    pub fn from_box(
        source: Arc<SeedDefinition>,
        reader: &super::SummaryBoxReader<'_>,
    ) -> Result<Self, Error> {
        let plan = Self::with_profile(
            source,
            reader.layout().rows(),
            reader.layout().profile().clone(),
        )?;
        plan.check_box(reader)?;
        Ok(plan)
    }
    fn check_box(&self, reader: &super::SummaryBoxReader<'_>) -> Result<(), Error> {
        if self.layout.rows() != reader.layout().rows()
            || self.layout.profile() != reader.layout().profile()
            || self.layout.definition().prescription().bytes()
                != reader.layout().definition().prescription().bytes()
        {
            return Err(error("summary Box uses another preparation plan"));
        }
        Ok(())
    }
    /// Selected summary periods can be reduced for an arbitrary window
    /// without loading event Packs. Returned state can answer multiple perc(n)
    /// calls; each call reuses the same merged distribution.
    pub fn rollup_box(
        &self,
        reader: &super::SummaryBoxReader<'_>,
        first: u64,
        window: &super::SummaryWindow,
    ) -> Result<SummaryBucket, Error> {
        self.check_box(reader)?;
        let rows = self.layout.rows();
        let selected = window.rows(
            first,
            reader.len().checked_mul(rows).ok_or(Error::Overflow)?,
        )?;
        if selected.len() as u64 != window.period_count() {
            return Err(error("incomplete summary coverage for rollup"));
        }
        let mut merged = self.bucket();
        if selected.is_empty() {
            return Ok(merged);
        }
        for i in selected.start / rows..selected.end.div_ceil(rows) {
            let at = first
                .checked_add(
                    u64::try_from(i.checked_mul(rows).ok_or(Error::Overflow)?)
                        .map_err(|_| Error::Overflow)?
                        .checked_mul(super::SUMMARY_PERIOD_MS)
                        .ok_or(Error::Overflow)?,
                )
                .ok_or(Error::Overflow)?;
            self.rollup(&mut merged, &reader.pack(i)?, at, window)?;
        }
        Ok(merged)
    }
    pub fn preview_box(
        &self,
        reader: &super::SummaryBoxReader<'_>,
        first: u64,
        window: &super::SummaryWindow,
        predicate: &crate::Predicate,
    ) -> Result<super::SummaryWindowResult, Error> {
        self.check_box(reader)?;
        let rows = self.layout.rows();
        let selected = window.rows(
            first,
            reader.len().checked_mul(rows).ok_or(Error::Overflow)?,
        )?;
        let preview = self.preview(predicate);
        let mut scan = preview.window(window.requested())?;
        if !selected.is_empty() {
            for i in selected.start / rows..selected.end.div_ceil(rows) {
                let pack = reader.pack(i)?;
                let mut cursor = pack.reader();
                let at = first
                    .checked_add(
                        u64::try_from(i.checked_mul(rows).ok_or(Error::Overflow)?)
                            .map_err(|_| Error::Overflow)?
                            .checked_mul(super::SUMMARY_PERIOD_MS)
                            .ok_or(Error::Overflow)?,
                    )
                    .ok_or(Error::Overflow)?;
                for ordinal in window.rows(at, rows)? {
                    let time = at
                        .checked_add(ordinal as u64 * super::SUMMARY_PERIOD_MS)
                        .ok_or(Error::Overflow)?;
                    scan.observe(time, &cursor.get(ordinal)?, true)?;
                }
            }
        }
        scan.finish()
    }
    pub fn layout(&self) -> &Arc<SummaryLayout> {
        &self.layout
    }
    pub fn fields(&self) -> impl Iterator<Item = &SummaryField> {
        self.bindings.iter().map(|b| &b.field)
    }
    /// Combine the complete five-minute periods overlapping a requested
    /// window. Calendar placement comes from the caller's storage binding.
    /// This uses the existing summary reader and never opens event Packs.
    pub fn rollup(
        &self,
        target: &mut SummaryBucket,
        pack: &pil_codec::api::Pack<'_>,
        first: u64,
        window: &super::SummaryWindow,
    ) -> Result<usize, Error> {
        self.check(target)?;
        let rows = window.rows(first, pack.len())?;
        let count = rows.len();
        let mut reader = pack.reader();
        for row in rows {
            let bucket = self.restore(&reader.get(row)?)?;
            self.merge(target, &bucket)?;
        }
        Ok(count)
    }
    pub fn bucket(&self) -> SummaryBucket {
        SummaryBucket {
            identity: self.identity.clone(),
            count: 0,
            valid: true,
            fields: self
                .bindings
                .iter()
                .map(|b| FieldState {
                    count: 0,
                    nulls: 0,
                    stats: match b.field.class {
                        SummaryClass::Numeric | SummaryClass::Temporal => {
                            Statistics::Numeric(Numeric::new(
                                b.scalar.unwrap(),
                                if b.histogram.is_empty() {
                                    0
                                } else {
                                    b.histogram.len() + 1
                                },
                            ))
                        }
                        SummaryClass::Boolean => Statistics::Boolean(0),
                        _ => Statistics::Categories(vec![0; b.field.values.len()]),
                    },
                })
                .collect(),
        }
    }
    fn check(&self, b: &SummaryBucket) -> Result<(), Error> {
        if !Arc::ptr_eq(&self.identity, &b.identity) || !b.valid {
            return Err(error(
                "summary bucket belongs to another plan or contains a failed observation",
            ));
        }
        Ok(())
    }
    /// Read selected typed fields only. No annotations or field names are
    /// interpreted in this loop; the plan supplies the source ordinals.
    pub fn observe(&self, bucket: &mut SummaryBucket, row: &PillView<'_>) -> Result<(), Error> {
        self.check(bucket)?;
        if !std::ptr::eq(row.prescription(), &**self.source.prescription()) {
            return Err(error("summary input uses another prescription"));
        }
        let count = bucket.count.checked_add(1).ok_or(Error::Overflow)?;
        bucket.valid = false;
        for (b, state) in self.bindings.iter().zip(&mut bucket.fields) {
            let value = row.native_value(b.field.ordinal)?;
            match value {
                NativeValue::Scalar(Value::Absent) => continue,
                NativeValue::Scalar(Value::Null) => {
                    state.nulls += 1;
                    continue;
                }
                _ => {}
            }
            match &mut state.stats {
                Statistics::Numeric(n) => {
                    let value = Number::native(value)?;
                    if !value.finite() {
                        n.nonfinite += 1;
                    } else {
                        if state.count == n.nonfinite {
                            n.min = value;
                            n.max = value;
                        } else {
                            if value.compare(n.min) == Some(Ordering::Less) {
                                n.min = value;
                            }
                            if value.compare(n.max) == Some(Ordering::Greater) {
                                n.max = value;
                            }
                        }
                        if b.field.class == SummaryClass::Numeric {
                            n.add(value);
                            if !b.histogram.is_empty() {
                                let index = b.histogram.partition_point(|bound| {
                                    bound.compare(value) == Some(Ordering::Less)
                                });
                                n.histogram[index] += 1;
                            }
                        }
                    }
                }
                Statistics::Boolean(n) => {
                    let NativeValue::Scalar(Value::Bool(v)) = value else {
                        return Err(error("summary expects bool"));
                    };
                    *n += u64::from(v);
                }
                Statistics::Categories(counts) => {
                    let crate::Cell::Value(value) = crate::value::own(value)? else {
                        return Err(error("summary category is absent"));
                    };
                    let index = b
                        .field
                        .values
                        .iter()
                        .position(|v| *v == value)
                        .ok_or_else(|| error("summary category is outside its Definition"))?;
                    counts[index] += 1;
                }
            }
            state.count += 1;
        }
        bucket.count = count;
        bucket.valid = true;
        Ok(())
    }
    /// Encode an event once through the existing Seed builder, prepare its
    /// summary, then return that capped Seed for byte append into the event Pack.
    /// This is an explicit capped ingestion path, not the uncapped batch path.
    pub fn ingest<'a, T>(
        &'a self,
        bucket: &mut SummaryBucket,
        event: T,
        transform: impl FnOnce(T, &mut Seed<'a>) -> Result<(), Error>,
    ) -> Result<Seed<'a>, Error> {
        let seed = self.source.ingest(event, transform)?;
        self.observe_seed(bucket, &seed)?;
        Ok(seed)
    }
    /// An owned prescription reference lets a returned Seed outlive the
    /// mutable accumulator borrow. Event bytes still use the same encoder.
    pub(super) fn ingest_owned<'a, T>(
        &self,
        bucket: &mut SummaryBucket,
        event: T,
        transform: impl FnOnce(T, &mut Seed<'a>) -> Result<(), Error>,
    ) -> Result<Seed<'a>, Error> {
        let mut seed: Seed<'a> = self.source.seed_owned();
        transform(event, &mut seed)?;
        self.observe_seed(bucket, &seed)?;
        Ok(seed)
    }
    fn observe_seed(&self, bucket: &mut SummaryBucket, seed: &Seed<'_>) -> Result<(), Error> {
        seed.cap()?;
        let bytes = seed.bytes()?;
        let mut decoder = self.source.prescription().decoder();
        self.observe(bucket, &decoder.read(&bytes)?)
    }
    /// Restore bounded accumulator state for rollups without revisiting events.
    pub fn restore(&self, row: &PillView<'_>) -> Result<SummaryBucket, Error> {
        let mut bucket = self.bucket();
        bucket.count = view_uint(row, 0)?;
        for (index, (b, state)) in self.bindings.iter().zip(&mut bucket.fields).enumerate() {
            let v = self.decoded_field(row, index)?;
            state.count = uint_value(&v, "count")?;
            state.nulls = uint_value(&v, "null_count")?;
            if state
                .count
                .checked_add(state.nulls)
                .filter(|n| *n <= bucket.count)
                .is_none()
            {
                return Err(error("invalid summary population"));
            }
            match &mut state.stats {
                Statistics::Numeric(n) => {
                    let zero = Number::zero(b.scalar.unwrap());
                    let number = |v: &Data| -> Result<Number, Error> {
                        Ok(match zero {
                            Number::Unsigned(_) => Number::Unsigned(pil_codec::unsigned_native(v)?),
                            Number::Signed(_) => Number::Signed(pil_codec::signed_native(v)?),
                            Number::Float(_) => Number::Float(
                                v.as_float()
                                    .ok_or_else(|| error("invalid floating summary"))?,
                            ),
                        })
                    };
                    n.min = number(get(&v, "min")?)?;
                    n.max = number(get(&v, "max")?)?;
                    if b.field.class == SummaryClass::Numeric {
                        n.sum = number(get(&v, "sum")?)?;
                        n.nonfinite = uint_value(&v, "nonfinite_count")?;
                        n.overflow = get(&v, "overflow")?
                            .as_bool()
                            .ok_or_else(|| error("invalid overflow state"))?;
                        if matches!(zero, Number::Float(_)) {
                            n.compensation = get(&v, "compensation")?
                                .as_float()
                                .ok_or_else(|| error("invalid compensation"))?;
                        }
                    }
                    if !b.histogram.is_empty() {
                        let Data::Array(counts) = get(&v, "histogram")? else {
                            return Err(error("invalid histogram counts"));
                        };
                        if counts.len() != n.histogram.len() {
                            return Err(error("histogram shape mismatch"));
                        }
                        let mut total = 0u64;
                        for (target, value) in n.histogram.iter_mut().zip(counts) {
                            *target = as_uint(value)?;
                            total = total.checked_add(*target).ok_or(Error::Overflow)?;
                        }
                        if total.checked_add(n.nonfinite) != Some(state.count) {
                            return Err(error("histogram population mismatch"));
                        }
                    }
                    if n.nonfinite > state.count
                        || !n.min.finite()
                        || !n.max.finite()
                        || !n.sum.finite()
                        || !n.compensation.is_finite()
                    {
                        return Err(error("invalid numeric summary state"));
                    }
                    if state.count > n.nonfinite && n.min.compare(n.max) == Some(Ordering::Greater)
                    {
                        return Err(error("inverted summary bounds"));
                    }
                }
                Statistics::Boolean(n) => {
                    *n = uint_value(&v, "true_count")?;
                    if *n > state.count {
                        return Err(error("invalid Boolean population"));
                    }
                }
                Statistics::Categories(counts) => {
                    let Data::Array(values) = get(&v, "values")? else {
                        return Err(error("invalid category counts"));
                    };
                    if values.len() != counts.len() {
                        return Err(error("category shape mismatch"));
                    }
                    let mut total = 0u64;
                    for (n, v) in counts.iter_mut().zip(values) {
                        *n = as_uint(v)?;
                        total = total.checked_add(*n).ok_or(Error::Overflow)?;
                    }
                    if total != state.count {
                        return Err(error("invalid category population"));
                    }
                }
            }
        }
        Ok(bucket)
    }
    pub fn merge(&self, target: &mut SummaryBucket, source: &SummaryBucket) -> Result<(), Error> {
        self.check(target)?;
        self.check(source)?;
        let count = target
            .count
            .checked_add(source.count)
            .ok_or(Error::Overflow)?;
        for (b, (a, z)) in self
            .bindings
            .iter()
            .zip(target.fields.iter_mut().zip(&source.fields))
        {
            match (&mut a.stats, &z.stats) {
                (Statistics::Numeric(a_num), Statistics::Numeric(z_num)) => {
                    if z.count > z_num.nonfinite {
                        if a.count == a_num.nonfinite {
                            a_num.min = z_num.min;
                            a_num.max = z_num.max;
                        } else {
                            if z_num.min.compare(a_num.min) == Some(Ordering::Less) {
                                a_num.min = z_num.min;
                            }
                            if z_num.max.compare(a_num.max) == Some(Ordering::Greater) {
                                a_num.max = z_num.max;
                            }
                        }
                    }
                    for (a, z) in a_num.histogram.iter_mut().zip(&z_num.histogram) {
                        *a += z;
                    }
                    a_num.nonfinite += z_num.nonfinite;
                    a_num.overflow |= z_num.overflow;
                    if b.field.class == SummaryClass::Numeric {
                        a_num.add(z_num.sum);
                        if matches!(z_num.sum, Number::Float(_)) {
                            a_num.add(Number::Float(z_num.compensation));
                        }
                    }
                }
                (Statistics::Boolean(a), Statistics::Boolean(b)) => *a += b,
                (Statistics::Categories(a), Statistics::Categories(b)) => {
                    for (a, b) in a.iter_mut().zip(b) {
                        *a += b;
                    }
                }
                _ => unreachable!("plan identity checked"),
            }
            a.count += z.count;
            a.nulls += z.nulls;
        }
        target.count = count;
        Ok(())
    }
    /// Every stored slot is populated. Missing statistics use count/overflow
    /// state, never a sparse child or a variable-length placeholder.
    pub fn seed(&self, bucket: &SummaryBucket) -> Result<Seed<'_>, Error> {
        self.check(bucket)?;
        let mut fields = Vec::with_capacity(self.bindings.len());
        for (b, state) in self.bindings.iter().zip(&bucket.fields) {
            let mut values = vec![
                ("count".into(), state.count.into()),
                ("null_count".into(), state.nulls.into()),
            ];
            match &state.stats {
                Statistics::Numeric(n) => {
                    values.extend([("min".into(), n.min.data()), ("max".into(), n.max.data())]);
                    if !n.histogram.is_empty() {
                        values.push((
                            "histogram".into(),
                            Data::Array(n.histogram.iter().map(|n| (*n).into()).collect()),
                        ));
                    }
                    if b.field.class == SummaryClass::Numeric {
                        let sum = if n.overflow {
                            Number::zero(b.scalar.unwrap())
                        } else {
                            n.sum
                        };
                        values.extend([
                            ("sum".into(), sum.data()),
                            ("nonfinite_count".into(), n.nonfinite.into()),
                            ("overflow".into(), n.overflow.into()),
                        ]);
                        if matches!(n.sum, Number::Float(_)) {
                            values.push((
                                "compensation".into(),
                                Data::Float(if n.overflow { 0.0 } else { n.compensation }),
                            ));
                        }
                    }
                }
                Statistics::Boolean(n) => values.push(("true_count".into(), (*n).into())),
                Statistics::Categories(counts) => {
                    values.push((
                        "values".into(),
                        Data::Array(counts.iter().map(|v| (*v).into()).collect()),
                    ));
                    let index = counts
                        .iter()
                        .enumerate()
                        .max_by_key(|(i, n)| (**n, std::cmp::Reverse(*i)))
                        .map(|(i, _)| i)
                        .unwrap();
                    values.push(("mode".into(), b.field.values[index].clone()));
                }
            }
            fields.push((b.field.name.clone().into(), Data::Map(values)));
        }
        let mut seed = self.layout.definition().seed();
        seed.set_uint("count", bucket.count)?
            .set_object("fields", fields)?;
        Ok(seed)
    }
    pub(super) fn decoded_field(
        &self,
        row: &PillView<'_>,
        index: usize,
    ) -> Result<Vec<(Data, Data)>, Error> {
        if !std::ptr::eq(
            row.prescription(),
            &**self.layout.definition().prescription(),
        ) && row.prescription().bytes() != self.layout.definition().prescription().bytes()
        {
            return Err(error("summary row uses another Definition"));
        }
        let pil_codec::api::FieldBytes::Bytes(bytes) = row.field("fields")? else {
            return Err(error("summary fields object is missing"));
        };
        let mut decoder = self.fields_rx.decoder();
        let fields = decoder.read(bytes)?;
        let NativeValue::Composite(Data::Map(values)) = fields.native_value(index)? else {
            return Err(error("summary field object is missing"));
        };
        Ok(values)
    }
    /// Materialize just one source field's analytic object. Quantiles remain
    /// unavailable until a bounded distribution profile is configured.
    pub(super) fn field_index(&self, name: &str) -> Option<usize> {
        self.bindings.iter().position(|b| b.field.name == name)
    }
    fn with_field<T>(
        &self,
        row: &PillView<'_>,
        index: usize,
        read: impl FnOnce(&PillView<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        if !std::ptr::eq(
            row.prescription(),
            &**self.layout.definition().prescription(),
        ) && row.prescription().bytes() != self.layout.definition().prescription().bytes()
        {
            return Err(error("summary Definition mismatch"));
        }
        let pil_codec::api::FieldBytes::Bytes(raw) = row.field("fields")? else {
            return Err(error("missing summary fields"));
        };
        let mut fields = self.fields_rx.decoder();
        let view = fields.read(raw)?;
        let pil_codec::api::FieldBytes::Bytes(raw) = view.field(index)? else {
            return Err(error("missing summary field"));
        };
        let mut field = self.bindings[index].rx.decoder();
        read(&field.read(raw)?)
    }
    pub(super) fn counts(&self, row: &PillView<'_>, index: usize) -> Result<(u64, u64), Error> {
        self.with_field(row, index, |v| Ok((view_uint(v, 0)?, view_uint(v, 1)?)))
    }
    pub(super) fn comparison(
        &self,
        row: &PillView<'_>,
        index: usize,
        op: crate::Compare,
        rhs: &Data,
    ) -> Result<super::CountBounds, Error> {
        use crate::Compare;
        let b = &self.bindings[index];
        let exact = super::CountBounds::exact;
        if matches!(
            b.field.class,
            SummaryClass::Numeric | SummaryClass::Temporal
        ) {
            return self.with_field(row, index, |v| {
                let count = view_uint(v, 0)?;
                if count == 0 || *rhs == Data::Null {
                    return Ok(exact(0));
                }
                if b.field.class == SummaryClass::Numeric && view_uint(v, 5)? > 0 {
                    return Ok(super::CountBounds {
                        lower: 0,
                        upper: count,
                    });
                }
                let a = crate::value::compare(v.native_value(2)?, rhs)?;
                let z = crate::value::compare(v.native_value(3)?, rhs)?;
                let (Some(a), Some(z)) = (a, z) else {
                    return Ok(exact(0));
                };
                Ok(bound_comparison(a, z, op, count))
            });
        }
        self.with_field(row, index, |v| {
            let count = view_uint(v, 0)?;
            if count == 0 || *rhs == Data::Null {
                return Ok(exact(0));
            }
            let matches = |value: &Data| -> Result<bool, Error> {
                let Some(order) = crate::value::compare_data(value, rhs)? else {
                    return Ok(false);
                };
                Ok(match op {
                    Compare::Eq => order == Ordering::Equal,
                    Compare::Ne => order != Ordering::Equal,
                    Compare::Gt => order == Ordering::Greater,
                    Compare::Ge => order != Ordering::Less,
                    Compare::Lt => order == Ordering::Less,
                    Compare::Le => order != Ordering::Greater,
                })
            };
            if b.field.class == SummaryClass::Boolean {
                let trues = view_uint(v, 2)?;
                let falses = count
                    .checked_sub(trues)
                    .ok_or_else(|| error("invalid Boolean population"))?;
                return Ok(exact(
                    if matches(&true.into())? { trues } else { 0 }
                        + if matches(&false.into())? { falses } else { 0 },
                ));
            }
            let pil_codec::api::FieldBytes::Bytes(raw) = v.field(2)? else {
                return Err(error("missing category counts"));
            };
            let counts = pil_codec::ArrayView::parse(Scalar::U64, raw)?;
            if counts.len() != b.field.values.len() {
                return Err(error("category shape mismatch"));
            }
            let mut total = 0u64;
            let mut selected = 0u64;
            for (value, n) in b.field.values.iter().zip(counts.iter()) {
                let Value::U64(n) = n? else {
                    return Err(error("invalid category count"));
                };
                total = total.checked_add(n).ok_or(Error::Overflow)?;
                if matches(value)? {
                    selected = selected.checked_add(n).ok_or(Error::Overflow)?;
                }
            }
            if total != count {
                return Err(error("invalid category population"));
            }
            Ok(exact(selected))
        })
    }
    pub fn field(&self, row: &PillView<'_>, name: &str) -> Result<Data, Error> {
        let index = self
            .bindings
            .iter()
            .position(|b| b.field.name == name)
            .ok_or_else(|| error("field has no summary class"))?;
        self.project_field(row, index)
    }
    /// Query any percentile from merged summary state, independently of
    /// annotations, host language or presentation. None means unavailable.
    pub fn perc(
        &self,
        bucket: &SummaryBucket,
        name: &str,
        percentile: f64,
    ) -> Result<Option<super::Percentile>, Error> {
        self.check(bucket)?;
        let index = self
            .field_index(name)
            .ok_or_else(|| error("field has no summary class"))?;
        let b = &self.bindings[index];
        let state = &bucket.fields[index];
        let rank = super::distribution::rank(percentile, state.count)?;
        if rank == 0 {
            return Ok(None);
        }
        match &state.stats {
            Statistics::Numeric(n) => {
                percentile_numeric(b, n, percentile, rank, n.histogram.iter().copied().map(Ok))
            }
            Statistics::Categories(counts) if b.field.class == SummaryClass::Ordinal => {
                let mut cumulative = 0u64;
                for (value, count) in b.field.values.iter().zip(counts) {
                    cumulative = cumulative.checked_add(*count).ok_or(Error::Overflow)?;
                    if cumulative >= rank {
                        return Ok(Some(super::Percentile::bounded(
                            value.clone(),
                            value.clone(),
                        )));
                    }
                }
                Err(error("invalid category population"))
            }
            _ => Ok(None),
        }
    }
    /// Read one field's stored accumulator, not event rows or other fields.
    pub fn perc_row(
        &self,
        row: &PillView<'_>,
        name: &str,
        percentile: f64,
    ) -> Result<Option<super::Percentile>, Error> {
        let index = self
            .field_index(name)
            .ok_or_else(|| error("field has no summary class"))?;
        let b = &self.bindings[index];
        self.with_field(row, index, |v| {
            let count = view_uint(v, 0)?;
            let rank = super::distribution::rank(percentile, count)?;
            if rank == 0 {
                return Ok(None);
            }
            if b.field.class == SummaryClass::Ordinal {
                let pil_codec::api::FieldBytes::Bytes(raw) = v.field("values")? else {
                    return Err(error("missing category counts"));
                };
                let counts = pil_codec::ArrayView::parse(Scalar::U64, raw)?;
                let mut total = 0u64;
                for (i, value) in b.field.values.iter().enumerate() {
                    let Value::U64(n) = counts.get(i)? else {
                        return Err(error("invalid category counter"));
                    };
                    total = total.checked_add(n).ok_or(Error::Overflow)?;
                    if total >= rank {
                        return Ok(Some(super::Percentile::bounded(
                            value.clone(),
                            value.clone(),
                        )));
                    }
                }
                return Err(error("invalid category population"));
            }
            if !matches!(
                b.field.class,
                SummaryClass::Numeric | SummaryClass::Temporal
            ) {
                return Ok(None);
            }
            let mut numeric = Numeric::new(b.scalar.unwrap(), 0);
            numeric.min = Number::native(v.native_value(2)?)?;
            numeric.max = Number::native(v.native_value(3)?)?;
            if b.field.class == SummaryClass::Numeric {
                numeric.nonfinite = view_uint(v, 5)?;
            }
            if !b.histogram.is_empty() {
                let pil_codec::api::FieldBytes::Bytes(raw) = v.field("histogram")? else {
                    return Err(error("missing histogram counters"));
                };
                let counts = pil_codec::ArrayView::parse(Scalar::U64, raw)?;
                return percentile_numeric(
                    b,
                    &numeric,
                    percentile,
                    rank,
                    (0..=b.histogram.len()).map(|i| {
                        let Value::U64(n) = counts.get(i)? else {
                            return Err(error("invalid histogram counter"));
                        };
                        Ok(n)
                    }),
                );
            }
            percentile_numeric(b, &numeric, percentile, rank, std::iter::empty())
        })
    }
    fn project_field(&self, row: &PillView<'_>, index: usize) -> Result<Data, Error> {
        let b = &self.bindings[index];
        let values = self.decoded_field(row, index)?;
        let count = uint_value(&values, "count")?;
        let nulls = uint_value(&values, "null_count")?;
        let total = match row.native_value(0)? {
            NativeValue::Scalar(Value::U64(v)) => v,
            _ => return Err(error("summary event count type")),
        };
        let absent = total
            .checked_sub(count.checked_add(nulls).ok_or(Error::Overflow)?)
            .ok_or_else(|| error("summary field counts exceed event count"))?;
        let mut object = vec![
            ("count".into(), count.into()),
            ("null_count".into(), nulls.into()),
            ("absent_count".into(), absent.into()),
        ];
        match b.field.class {
            SummaryClass::Numeric | SummaryClass::Temporal => {
                let nonfinite = if b.field.class == SummaryClass::Numeric {
                    uint_value(&values, "nonfinite_count")?
                } else {
                    0
                };
                let finite = count
                    .checked_sub(nonfinite)
                    .ok_or_else(|| error("invalid nonfinite count"))?;
                let min = if finite > 0 {
                    get(&values, "min")?.clone()
                } else {
                    Data::Null
                };
                let max = if finite > 0 {
                    get(&values, "max")?.clone()
                } else {
                    Data::Null
                };
                if b.field.class == SummaryClass::Temporal {
                    object.extend([("earliest".into(), min), ("latest".into(), max)]);
                } else {
                    let overflow = get(&values, "overflow")?
                        .as_bool()
                        .ok_or_else(|| error("invalid summary overflow state"))?;
                    let mut sum = get(&values, "sum")?.clone();
                    if let Data::Float(v) = &mut sum {
                        let c = get(&values, "compensation")?
                            .as_float()
                            .ok_or_else(|| error("invalid compensation"))?;
                        *v += c;
                    }
                    let sum = if count == 0 || nonfinite > 0 || overflow {
                        Data::Null
                    } else {
                        sum
                    };
                    let mean = if sum == Data::Null {
                        Data::Null
                    } else {
                        Data::Float(data_float(&sum)? / count as f64)
                    };
                    object.extend([
                        ("min".into(), min),
                        ("max".into(), max),
                        ("sum".into(), sum),
                        ("mean".into(), mean),
                        ("nonfinite_count".into(), nonfinite.into()),
                    ]);
                }
            }
            SummaryClass::Boolean => {
                let trues = uint_value(&values, "true_count")?;
                let falses = count
                    .checked_sub(trues)
                    .ok_or_else(|| error("invalid Boolean summary counts"))?;
                object.extend([
                    ("true_count".into(), trues.into()),
                    ("false_count".into(), falses.into()),
                    ("true_proportion".into(), proportion(trues, count)),
                ]);
            }
            SummaryClass::Categorical | SummaryClass::Ordinal => {
                let Data::Array(raw) = get(&values, "values")? else {
                    return Err(error("invalid category counts"));
                };
                let counts = raw.iter().map(as_uint).collect::<Result<Vec<_>, _>>()?;
                if counts.len() != b.field.values.len()
                    || counts
                        .iter()
                        .try_fold(0u64, |a, n| a.checked_add(*n))
                        .ok_or(Error::Overflow)?
                        != count
                {
                    return Err(error("invalid category population"));
                }
                let mode = counts
                    .iter()
                    .enumerate()
                    .max_by_key(|(i, n)| (**n, std::cmp::Reverse(*i)))
                    .filter(|(_, n)| **n > 0)
                    .map(|(i, _)| b.field.values[i].clone())
                    .unwrap_or(Data::Null);
                let entries = b
                    .field
                    .values
                    .iter()
                    .zip(&counts)
                    .map(|(value, n)| {
                        (
                            match value {
                                Data::Bool(v) => Data::Text(v.to_string()),
                                _ => value.clone(),
                            },
                            Data::Map(vec![
                                ("count".into(), (*n).into()),
                                ("proportion".into(), proportion(*n, count)),
                            ]),
                        )
                    })
                    .collect();
                object.extend([
                    (
                        "distinct".into(),
                        (counts.iter().filter(|n| **n > 0).count() as u64).into(),
                    ),
                    ("mode".into(), mode),
                    ("values".into(), Data::Map(entries)),
                ]);
                if b.field.class == SummaryClass::Ordinal {
                    let value = |i: Option<usize>| {
                        i.map(|i| b.field.values[i].clone()).unwrap_or(Data::Null)
                    };
                    let mut cumulative = 0;
                    let median = counts.iter().position(|n| {
                        cumulative += n;
                        count > 0 && cumulative >= count.div_ceil(2)
                    });
                    object.extend([
                        ("min".into(), value(counts.iter().position(|n| *n > 0))),
                        ("max".into(), value(counts.iter().rposition(|n| *n > 0))),
                        ("median".into(), value(median)),
                    ]);
                }
            }
        }
        if let Some(requested) = self
            .source
            .summary_analytics()
            .iter()
            .find(|a| a.ordinal() == b.field.ordinal)
        {
            let mut percentiles = Vec::new();
            for n in requested.percentiles() {
                let value = self.perc_row(row, &b.field.name, *n)?;
                percentiles.push((
                    Data::Text(n.to_string()),
                    value
                        .map(|p| {
                            Data::Map(vec![
                                ("value".into(), p.value),
                                ("lower".into(), p.lower),
                                ("upper".into(), p.upper),
                                ("exact".into(), p.exact.into()),
                            ])
                        })
                        .unwrap_or(Data::Null),
                ));
            }
            object.push(("percentiles".into(), Data::Map(percentiles)));
        }
        Ok(Data::Map(object))
    }
    pub fn object(&self, row: &PillView<'_>) -> Result<Object, Error> {
        self.bindings
            .iter()
            .enumerate()
            .map(|(i, b)| Ok((b.field.name.clone(), self.project_field(row, i)?)))
            .collect()
    }
}
fn percentile_numeric(
    b: &Binding,
    n: &Numeric,
    percentile: f64,
    rank: u64,
    counts: impl Iterator<Item = Result<u64, Error>>,
) -> Result<Option<super::Percentile>, Error> {
    if n.nonfinite > 0 {
        return Ok(None);
    }
    if !n.min.finite() || !n.max.finite() || n.min.compare(n.max) == Some(Ordering::Greater) {
        return Err(error("invalid percentile bounds"));
    }
    let exact = |value: Number| Some(super::Percentile::bounded(value.data(), value.data()));
    if percentile == 0.0 || n.min == n.max {
        return Ok(exact(n.min));
    }
    if percentile == 100.0 {
        return Ok(exact(n.max));
    }
    if b.histogram.is_empty() {
        return Ok(None);
    }
    let mut total = 0u64;
    for (i, count) in counts.enumerate() {
        total = total.checked_add(count?).ok_or(Error::Overflow)?;
        if total >= rank {
            let lower = i
                .checked_sub(1)
                .map(|i| b.histogram[i])
                .filter(|v| v.compare(n.min) == Some(Ordering::Greater))
                .unwrap_or(n.min);
            let upper = b
                .histogram
                .get(i)
                .copied()
                .filter(|v| v.compare(n.max) == Some(Ordering::Less))
                .unwrap_or(n.max);
            if lower.compare(upper) == Some(Ordering::Greater) {
                return Err(error("histogram count lies outside observed bounds"));
            }
            return Ok(Some(super::Percentile::bounded(lower.data(), upper.data())));
        }
    }
    Err(error("histogram population is too small for percentile"))
}
fn uint(name: &str) -> Field {
    Field::new(name).r#type("uint").size(8)
}
fn number_type(s: Scalar) -> Result<(&'static str, u16), Error> {
    Ok(match s {
        Scalar::U8 | Scalar::U16 | Scalar::U24 | Scalar::U32 | Scalar::U64 | Scalar::Uint(_) => {
            ("uint", s.width().unwrap() as u16)
        }
        Scalar::I8 | Scalar::I16 | Scalar::I32 | Scalar::I64 | Scalar::Int(_) => {
            ("int", s.width().unwrap() as u16)
        }
        Scalar::F32 | Scalar::F64 => ("float", s.width().unwrap() as u16),
        _ => return Err(error("unsupported numeric summary type")),
    })
}
fn get<'a>(values: &'a [(Data, Data)], name: &str) -> Result<&'a Data, Error> {
    values
        .iter()
        .find(|(k, _)| k.as_text() == Some(name))
        .map(|(_, v)| v)
        .ok_or_else(|| error(format!("missing summary {name}")))
}
fn as_uint(v: &Data) -> Result<u64, Error> {
    v.as_integer()
        .and_then(|v| u64::try_from(v).ok())
        .ok_or_else(|| error("invalid summary count"))
}
fn uint_value(v: &[(Data, Data)], name: &str) -> Result<u64, Error> {
    as_uint(get(v, name)?)
}
fn proportion(n: u64, count: u64) -> Data {
    if count == 0 {
        Data::Null
    } else {
        Data::Float(n as f64 / count as f64)
    }
}
fn data_float(v: &Data) -> Result<f64, Error> {
    match v {
        Data::Integer(v) => Ok(i128::from(*v) as f64),
        Data::Float(v) => Ok(*v),
        Data::Tag(tag, bytes) if *tag == 2 || *tag == 3 => {
            let Data::Bytes(bytes) = &**bytes else {
                return Err(error("invalid integer"));
            };
            let mut n = 0u128;
            for b in bytes {
                n = n
                    .checked_mul(256)
                    .and_then(|n| n.checked_add(*b as u128))
                    .ok_or(Error::Overflow)?;
            }
            Ok(if *tag == 2 {
                n as f64
            } else {
                -(n as f64) - 1.0
            })
        }
        _ => Err(error("invalid numeric summary")),
    }
}

fn view_uint(row: &PillView<'_>, i: usize) -> Result<u64, Error> {
    match row.native_value(i)? {
        NativeValue::Scalar(Value::U64(v)) => Ok(v),
        _ => Err(error("invalid summary count")),
    }
}
fn bound_comparison(
    a: Ordering,
    z: Ordering,
    op: crate::Compare,
    count: u64,
) -> super::CountBounds {
    use crate::Compare;
    let (all, none) = match op {
        Compare::Gt => (a == Ordering::Greater, z != Ordering::Greater),
        Compare::Ge => (a != Ordering::Less, z == Ordering::Less),
        Compare::Lt => (z == Ordering::Less, a != Ordering::Less),
        Compare::Le => (z != Ordering::Greater, a == Ordering::Greater),
        Compare::Eq => (
            a == Ordering::Equal && z == Ordering::Equal,
            a == Ordering::Greater || z == Ordering::Less,
        ),
        Compare::Ne => (
            a == Ordering::Greater || z == Ordering::Less,
            a == Ordering::Equal && z == Ordering::Equal,
        ),
    };
    if all {
        super::CountBounds::exact(count)
    } else if none {
        super::CountBounds::exact(0)
    } else {
        super::CountBounds {
            lower: 0,
            upper: count,
        }
    }
}
