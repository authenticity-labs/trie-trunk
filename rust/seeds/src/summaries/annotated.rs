//! Fixed class arrays selected by #summ(class). Aggregation and projection
//! belong to Seeds; all stored fields and nested objects use the Rust RX codec.
use super::{SummaryClass, SummaryField, SummaryLayout};
use crate::{Document, Error, Field, Object, Seed, SeedDefinition, doc, error};
use ciborium::Value as Data;
use pil_codec::{
    Scalar, Value,
    api::{NativeValue, PillView, Prescription, Binding as ParameterBinding, Step},
};
use std::{cmp::Ordering, sync::Arc};

#[path = "mixed.rs"]
pub mod mixed;

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
}
impl Numeric {
    fn new(s: Scalar) -> Self {
        let z = Number::zero(s);
        Self {
            min: z,
            max: z,
            sum: z,
            compensation: 0.0,
            nonfinite: 0,
            overflow: false,
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
pub(super) struct Binding {
    field: SummaryField,
    scalar: Option<Scalar>,
    rx: Arc<Prescription>,
    array: usize,
    slot: usize,
    element: usize,
    counts: usize,
}

// No source Arc is retained here: the source owns this cache, so keeping one
// would create a cycle. All state is immutable and independent of day/row count.
pub(crate) struct Prepared {
    pub(super) bindings:Arc<[Binding]>,
    pub(super) definition:Arc<SeedDefinition>,
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

/// One bound generic RX layout and its compiled source bindings. Reuse it for
/// every five-minute bucket in the daily Pack and all subsequent daily Packs.
#[derive(Clone)]
pub struct SummaryPlan {
    source: Arc<SeedDefinition>,
    bindings: Arc<[Binding]>,
    identity: Arc<()>,
    document: &'static Document,
    counter_rx: Arc<Prescription>,
    empty: Arc<[pil_codec::api::Input<'static>]>,
    arrays: Arc<[(usize, usize)]>,
    layout: Arc<SummaryLayout>,
    // Seeds-only exception context; normal class plans carry no bitmap.
    unknown: Option<Arc<[u8]>>,
}
pub(super) struct SummaryMetric {
    pub samples: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}
impl SummaryPlan {
    #[cfg(feature = "registration")]
    pub(super) fn binding_key(&self)->usize {self.bindings.as_ptr() as usize}
    pub(super) fn source(&self) -> &Arc<SeedDefinition> { &self.source }
    pub(crate) fn source_context(&self) -> Result<[u32; 2], Error> {
        self.source.registration.ok_or_else(|| error("summary source is not registered"))
    }
    /// Bind the shared summary library to this source and period count.
    pub fn new(source: Arc<SeedDefinition>, rows: usize) -> Result<Self, Error> {
        super::SummaryDefinitions::new().prescribe(source, rows)
    }
    pub(crate) fn generate(source: Arc<SeedDefinition>, rows: usize) -> Result<Self, Error> {
        let prepared=Self::prepared(&source)?;
        let bindings=prepared.bindings.clone();
        let document = super::definitions::template()?;
        let mut layout = SummaryLayout::compiled(document, rows, crate::EncodingMode::FixedFull, prepared.definition.clone(),None)?;
        Arc::make_mut(&mut layout).class_bindings=Some(bindings.clone());
        if source.registration.is_none() {
            layout = layout.with_bindings(super::bindings::write(&source)?)?;
        }
        let layout = layout.with_source(source.clone())?;
        Ok(Self {
            source,
            bindings: bindings.into(),
            identity: Arc::new(()),
            document,
            counter_rx: super::definitions::counter()?,
            empty: Self::empty(&layout)?,
            arrays: Self::arrays(&layout),
            layout,
            unknown: None,
        })
    }
    pub(super) fn prepared(source:&SeedDefinition)->Result<&Prepared,Error>{
        source.summary_plan.get_or_init(|| (||->Result<Prepared,Error>{
            let (bindings,parameters)=Self::bindings(source)?;
            let definition=super::definitions::definition()?.parameters(&parameters)?;
            Ok(Prepared{bindings:bindings.into(),definition})
        })().map_err(|e|e.to_string())).as_ref().map_err(error)
    }
    pub(super) fn bindings(source: &SeedDefinition) -> Result<(Vec<Binding>, Vec<ParameterBinding>), Error> {
        if source.summary_fields().is_empty() {
            return Err(error("Definition has no #summ fields"));
        }
        let document = super::definitions::template()?;
        let mut lengths = vec![0usize; document.definitions()[0].fields.len()];
        let counts_array = lengths.len() - 1;
        let mut bindings = Vec::with_capacity(source.summary_fields().len());
        let mut parameters=Vec::new();
        let mut readers:Vec<(usize,Vec<ParameterBinding>,Arc<Prescription>)>=Vec::new();
        for field in source.summary_fields() {
            let scalar = source.prescription().native_scalar_kind(field.ordinal);
            let name = match field.class {
                SummaryClass::Numeric if matches!(scalar,Some(Scalar::F32|Scalar::F64)) => "real",
                SummaryClass::Numeric => "numeric",
                SummaryClass::Temporal => "temporal",
                SummaryClass::Boolean => "boolean",
                _ => "categories",
            };
            let array = document.definitions()[0]
                .fields
                .iter()
                .position(|f| f.name == name)
                .ok_or_else(|| error("missing summary class array"))?;
            let member = document
                .definitions()
                .iter()
                .position(|d| d.header.rx_name == name)
                .ok_or_else(|| error("missing summary class"))?;
            let element = lengths[array];
            lengths[array] += 1;
            let counts = lengths[counts_array];
            lengths[counts_array] = counts
                .checked_add(field.values.len())
                .ok_or(Error::Overflow)?;
            let mut member_parameters=Vec::new();
            if matches!(field.class,SummaryClass::Numeric|SummaryClass::Temporal){
                let (kind,width)=number_type(scalar.ok_or_else(||error("summary scalar unavailable"))?)?;
                use pil_codec::authoring_language::language as l;
                let mut descriptor=l::TypeDescriptor::scalar(match kind {"uint"=>l::UINT,"int"=>l::INT,_=>l::FLOAT});
                descriptor.size=width;
                for ordinal in [2,3] {member_parameters.push(ParameterBinding::scalar(vec![Step::Field(ordinal)],descriptor.clone()));}
                if field.class==SummaryClass::Numeric {
                    descriptor.size=if kind=="float" {8}else{16};
                    member_parameters.push(ParameterBinding::scalar(vec![Step::Field(4)],descriptor));
                }
                for parameter in &member_parameters {
                    let mut parameter=parameter.clone();
                    parameter.path.splice(..0,[Step::Field(array),Step::Element(element)]);
                    parameters.push(parameter);
                }
            }
            let rx=if let Some((_,_,rx))=readers.iter().find(|(m,p,_)|*m==member&&p==&member_parameters){rx.clone()}else{
                let base=super::definitions::prescription(member)?;
                let rx=if member_parameters.is_empty(){base}else{Arc::new(base.parameters(&member_parameters)?)};
                readers.push((member,member_parameters,rx.clone()));rx
            };
            bindings.push(Binding {
                field: field.clone(),
                scalar,
                rx,
                array,
                slot: 0,
                element,
                counts,
            });
        }
        for binding in &mut bindings {
            binding.slot = lengths[1..binding.array].iter().filter(|&&n| n > 0).count();
        }
        parameters.extend(lengths.into_iter().enumerate().skip(1).map(|(f,n)|ParameterBinding::count(vec![Step::Field(f)],n)));
        Ok((bindings, parameters))
    }
    fn arrays(layout: &SummaryLayout) -> Arc<[(usize, usize)]> {
        layout
            .definition()
            .prescription()
            .array_lengths()
            .iter()
            .copied()
            .filter(|&(_, length)| length > 0)
            .collect()
    }
    fn empty(layout: &SummaryLayout) -> Result<Arc<[pil_codec::api::Input<'static>]>, Error> {
        let rx = layout.definition().prescription();
        let mut inputs = vec![pil_codec::api::Input::Absent; rx.definition().fields().len()];
        for &(field, length) in rx.array_lengths() {
            if length == 0 {
                inputs[field] = rx.native_input(field, Some(Data::Array(Vec::new())))?;
            }
        }
        Ok(inputs.into())
    }
    pub fn version(&self) -> (u32, u32) {
        let header = &self.document.definitions()[0].header;
        (header.rx_major, header.rx_minor)
    }
    /// Restore names, classes and category mappings from Seed metadata. The
    /// generic RX supplies only storage types and fixed object extents.
    pub fn from_layout(layout: Arc<SummaryLayout>) -> Result<Self, Error> {
        let source = match &layout.source {
            Some(source) => source.clone(),
            None if !layout.context.is_empty() => return Err(error("summary Box requires its registered source context")),
            None => super::bindings::read(&layout.bindings)?,
        };
        let bindings=if let Some(bindings)=&layout.class_bindings {bindings.clone()}else{
            let (bindings,parameters)=Self::bindings(&source)?;
            let expected=super::definitions::prescription(0)?.parameters(&parameters)?;
            if expected.binding_bytes()!=layout.definition.prescription().binding_bytes(){return Err(error("summary bindings disagree with prepared plan"));}
            bindings.into()
        };
        Ok(Self {
            source,
            bindings: bindings.into(),
            identity: Arc::new(()),
            document: super::definitions::template()?,
            counter_rx: super::definitions::counter()?,
            empty: Self::empty(&layout)?,
            arrays: Self::arrays(&layout),
            layout,
            unknown: None,
        })
    }
    pub fn document(&self) -> &Document {
        &self.document
    }
    pub fn from_box(
        source: Arc<SeedDefinition>,
        reader: &super::SummaryBoxReader<'_>,
    ) -> Result<Self, Error> {
        let mut definitions =
            super::SummaryDefinitions::read(reader.layout().definition().prescription().bytes())?;
        let plan = definitions.prescribe(source, reader.layout().rows())?;
        plan.check_box(reader)?;
        Ok(plan)
    }
    /// Check the prepared shape before using a Box with this source binding.
    /// This compares existing Definition bytes; it does not rehash stored data.
    pub fn check_box(&self, reader: &super::SummaryBoxReader<'_>) -> Result<(), Error> {
        if !reader.is_uniform()
            || self.layout.mode() != reader.layout().mode()
            || self.layout.rows() != reader.layout().rows()
            || self.layout.bindings != reader.layout().bindings
            || self.layout.context != reader.layout().context
            || self.layout.definition().prescription().bytes()
                != reader.layout().definition().prescription().bytes()
        {
            return Err(error("summary Box uses another preparation plan"));
        }
        Ok(())
    }
    /// Selected summary periods can be reduced for an arbitrary window
    /// without loading event Packs. Series queries group this class state by
    /// interval before calculating a distribution of aggregate values.
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
    pub fn preview_pack(
        &self,
        pack: &pil_codec::api::Pack<'_>,
        first: u64,
        window: &super::SummaryWindow,
        predicate: &crate::Predicate,
    ) -> Result<super::SummaryWindowResult, Error> {
        let preview = self.preview(predicate);
        let mut scan = preview.window(window.requested())?;
        let mut cursor = pack.reader();
        for ordinal in window.rows(first, pack.len())? {
            scan.observe(
                first + ordinal as u64 * super::SUMMARY_PERIOD_MS,
                &cursor.get(ordinal)?,
                pack.is_sealed(),
            )?;
        }
        scan.finish()
    }
    pub fn layout(&self) -> &Arc<SummaryLayout> {
        &self.layout
    }
    pub fn fields(&self) -> impl Iterator<Item = &SummaryField> {
        self.bindings.iter().map(|b| &b.field)
    }
    /// Resolve one numeric aggregate of a period. Integer results retain their
    /// declared precision; means/proportions are floating-point calculations.
    pub(super) fn aggregate_value(
        &self,
        bucket: &SummaryBucket,
        index: Option<usize>,
        aggregate: super::SummaryAggregate,
    ) -> Result<Option<Data>, Error> {
        use super::SummaryAggregate as A;
        self.check(bucket)?;
        let Some(index) = index else {
            return Ok(Some(bucket.count.into()));
        };
        let s = &bucket.fields[index];
        match aggregate {
            A::Count => return Ok(Some(s.count.into())),
            A::NullCount => return Ok(Some(s.nulls.into())),
            A::AbsentCount => return Ok(Some((bucket.count - s.count - s.nulls).into())),
            _ => {}
        }
        Ok(match (&s.stats, aggregate) {
            (Statistics::Numeric(n), A::Min) if s.count > n.nonfinite => Some(n.min.data()),
            (Statistics::Numeric(n), A::Max) if s.count > n.nonfinite => Some(n.max.data()),
            (Statistics::Numeric(n), A::Sum | A::Mean)
                if s.count > 0 && n.nonfinite == 0 && !n.overflow =>
            {
                let sum = match n.sum {
                    Number::Float(v) => Data::Float(v + n.compensation),
                    v => v.data(),
                };
                Some(if aggregate == A::Mean {
                    Data::Float(data_float(&sum)? / s.count as f64)
                } else {
                    sum
                })
            }
            (Statistics::Boolean(n), A::TrueCount) => Some((*n).into()),
            (Statistics::Boolean(n), A::FalseCount) => Some((s.count - n).into()),
            (Statistics::Boolean(n), A::TrueProportion) if s.count > 0 => {
                Some(Data::Float(*n as f64 / s.count as f64))
            }
            (Statistics::Categories(v), A::Distinct) => {
                Some((v.iter().filter(|n| **n > 0).count() as u64).into())
            }
            // Ordered category extrema can be text; numeric series require
            // a numeric summary class, checked once when the measure is made.
            _ => None,
        })
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
                            Statistics::Numeric(Numeric::new(b.scalar.unwrap()))
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
        if !std::ptr::eq(row.prescription(), &**self.source.prescription()) {
            return Err(error("summary input uses another prescription"));
        }
        self.observe_values(bucket, |i| row.native_value(i))
    }
    pub(crate) fn observe_values<'a>(&self, bucket:&mut SummaryBucket,
        mut field:impl FnMut(usize)->Result<NativeValue<'a>,Error>) -> Result<(),Error> {
        self.check(bucket)?;
        let count = bucket.count.checked_add(1).ok_or(Error::Overflow)?;
        bucket.valid = false;
        for (b, state) in self.bindings.iter().zip(&mut bucket.fields) {
            let value = field(b.field.ordinal)?;
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
        let mut arrays = self
            .arrays
            .iter()
            .map(|&(_, length)| Vec::with_capacity(length))
            .collect::<Vec<_>>();
        let counts_array = arrays.len() - 1;
        for (binding, state) in self.bindings.iter().zip(&bucket.fields) {
            let mut values = Vec::with_capacity(binding.rx.definition().fields().len());
            values.push(("count".into(), state.count.into()));
            values.push(("null_count".into(), state.nulls.into()));
            match &state.stats {
                Statistics::Numeric(n) => {
                    values.extend([("min".into(), n.min.data()), ("max".into(), n.max.data())]);
                    if binding.field.class == SummaryClass::Numeric {
                        let sum = if n.overflow {
                            Number::zero(binding.scalar.unwrap())
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
                    for count in counts {
                        if *count > 0xff_ffff {
                            return Err(error("summary category counter exceeds uint(3)"));
                        }
                        arrays[counts_array].push((*count).into());
                    }
                }
            }
            arrays[binding.slot].push(Data::Map(values));
        }
        let mut seed = self.layout.definition().seed();
        seed.load(&*self.empty)?;
        seed.set_uint(0, bucket.count)?;
        for (&(ordinal, _), values) in self.arrays.iter().zip(arrays) {
            seed.set_array(ordinal, values)?;
        }
        Ok(seed)
    }
    pub(super) fn decoded_field(
        &self,
        row: &PillView<'_>,
        index: usize,
    ) -> Result<Vec<(Data, Data)>, Error> {
        let binding = &self.bindings[index];
        let mut values = self.with_field(row, index, |view| {
            binding
                .rx
                .definition()
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    let value = match crate::value::own(view.native_value(i)?)? {
                        crate::Cell::Value(value) => value,
                        crate::Cell::Absent => return Err(error("missing summary slot")),
                    };
                    Ok((f.name.clone().into(), value))
                })
                .collect::<Result<Vec<_>, Error>>()
        })?;
        if matches!(
            binding.field.class,
            SummaryClass::Categorical | SummaryClass::Ordinal
        ) {
            let counts = self
                .category_counts(row, binding)?
                .collect::<Result<Vec<_>, _>>()?;
            let index = counts
                .iter()
                .enumerate()
                .max_by_key(|(i, n)| (**n, std::cmp::Reverse(*i)))
                .map(|(i, _)| i)
                .ok_or_else(|| error("empty summary category mapping"))?;
            values.push(("mode".into(), binding.field.values[index].clone()));
            values.push((
                "values".into(),
                Data::Array(counts.into_iter().map(Data::from).collect()),
            ));
        }
        Ok(values)
    }
    fn category_counts<'a>(
        &'a self,
        row: &'a PillView<'_>,
        binding: &'a Binding,
    ) -> Result<impl Iterator<Item = Result<u64, Error>> + 'a, Error> {
        let ordinal = self.document.definitions()[0].fields.len() - 1;
        let pil_codec::api::FieldBytes::Bytes(raw) = row.field(ordinal)? else {
            return Err(error("missing category counters"));
        };
        Ok((0..binding.field.values.len()).map(move |i| {
            let bytes = row
                .prescription()
                .array_element(ordinal, binding.counts + i, raw)?;
            match self.counter_rx.read_field(0, bytes)? {
                NativeValue::Scalar(Value::U24(n)) => Ok(n as u64),
                _ => Err(error("invalid category counter type")),
            }
        }))
    }
    /// Materialize just one source field's analytic object. Quantiles remain
    /// unavailable when the stored extrema cannot narrow the predicate.
    pub(super) fn field_index(&self, name: &str) -> Option<usize> {
        let name = if name.starts_with('@') {
            &self.source.rx.definition().fields()[self.source.query_ordinal(name)?].name
        } else { name };
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
        ) && (row.prescription().bytes() != self.layout.definition().prescription().bytes()
            || row.prescription().binding_bytes()
                != self.layout.definition().prescription().binding_bytes())
        {
            return Err(error("summary plan mismatch"));
        }
        let binding = &self.bindings[index];
        let pil_codec::api::FieldBytes::Bytes(raw) = row.field(binding.array)? else {
            return Err(error("missing summary class array"));
        };
        let raw = row
            .prescription()
            .array_element(binding.array, binding.element, raw)?;
        let mut decoder = binding.rx.decoder();
        read(&decoder.read(raw)?)
    }
    pub(super) fn counts(&self, row: &PillView<'_>, index: usize) -> Result<(u64, u64), Error> {
        self.with_field(row, index, |v| Ok((view_uint(v, 0)?, view_uint(v, 1)?)))
    }
    /// Read a scalar analytic member without decoding the class object. The
    /// outer Option distinguishes nonnumeric/unknown members from real nulls.
    #[cfg(feature = "registration")]
    pub(super) fn number_member(&self, row: &PillView<'_>, index: usize, member: &str) -> Result<Option<Option<crate::relational::Num>>, Error> {
        use crate::relational::Num;
        if self.is_null(row, index)? {return Ok(Some(None));}
        let class=self.bindings[index].field.class;
        let numeric=class==SummaryClass::Numeric;
        let temporal=class==SummaryClass::Temporal;
        let boolean=class==SummaryClass::Boolean;
        if !matches!(member,"count"|"null_count"|"absent_count")
            && !(numeric && matches!(member,"min"|"max"|"sum"|"mean"|"nonfinite_count"))
            && !(temporal && matches!(member,"earliest"|"latest"))
            && !(boolean && matches!(member,"true_count"|"false_count"|"true_proportion")) {return Ok(None);}
        self.with_field(row,index,|view| {
            let count=view_uint(view,0)?;let nulls=view_uint(view,1)?;
            let total=view_uint(row,0)?;
            let absent=total.checked_sub(count.checked_add(nulls).ok_or(Error::Overflow)?).ok_or_else(||error("summary field counts exceed event count"))?;
            let uint=|v|Ok(Some(Some(Num::Uint(v as u128))));
            match member {"count"=>return uint(count),"null_count"=>return uint(nulls),"absent_count"=>return uint(absent),_=>{}}
            if boolean {
                let trues=view_uint(view,2)?;
                let falses=count.checked_sub(trues).ok_or_else(||error("invalid Boolean summary counts"))?;
                return match member {"true_count"=>uint(trues),"false_count"=>uint(falses),_=>Ok(Some((count>0).then(||Num::Float(trues as f64/count as f64))))};
            }
            let nonfinite=if numeric {view_uint(view,5)?} else {0};
            let finite=count.checked_sub(nonfinite).ok_or_else(||error("invalid nonfinite count"))?;
            if member=="nonfinite_count" {return uint(nonfinite);}
            let number=|i|->Result<Num,Error> {Ok(match Number::native(view.native_value(i)?)? {Number::Unsigned(n)=>Num::Uint(n),Number::Signed(n)=>Num::Int(n),Number::Float(n)=>Num::Float(n)})};
            if matches!(member,"min"|"earliest"|"max"|"latest") {return Ok(Some(if finite==0 {None} else {Some(number(if matches!(member,"min"|"earliest"){2}else{3})?)}));}
            let overflow=match view.native_value(6)? {NativeValue::Scalar(Value::Bool(v))=>v,_=>return Err(error("invalid summary overflow state"))};
            if count==0||nonfinite>0||overflow {return Ok(Some(None));}
            let mut sum=number(4)?;
            if let Num::Float(n)=sum {sum=Num::Float(n+number(7)?.float());}
            if !sum.float().is_finite(){return Err(error("non-finite summary value"));}
            Ok(Some(Some(if member=="mean" {Num::Float(sum.float()/count as f64)} else {sum})))
        })
    }
    pub(super) fn metric(
        &self,
        row: &PillView<'_>,
        index: usize,
    ) -> Result<Option<SummaryMetric>, Error> {
        if self.bindings[index].field.class != SummaryClass::Numeric || self.is_null(row, index)? {
            return Ok(None);
        }
        self.with_field(row, index, |view| {
            let samples = view_uint(view, 0)?;
            if samples == 0 || view_uint(view, 5)? != 0 {
                return Ok(None);
            }
            match view.native_value(6)? {
                NativeValue::Scalar(Value::Bool(true)) => return Ok(None),
                NativeValue::Scalar(Value::Bool(false)) => {}
                _ => return Err(error("invalid summary overflow state")),
            }
            let number = |index| -> Result<f64, Error> {
                Ok(match Number::native(view.native_value(index)?)? {
                    Number::Unsigned(n) => n as f64,
                    Number::Signed(n) => n as f64,
                    Number::Float(n) => n,
                })
            };
            let mut sum = number(4)?;
            if matches!(
                view.native_value(4)?,
                NativeValue::Scalar(Value::F64(_) | Value::F32(_))
            ) {
                sum += number(7)?;
            }
            if !sum.is_finite() {
                return Err(error("summary chart sum exceeds finite range"));
            }
            Ok(Some(SummaryMetric {
                samples,
                sum,
                min: number(2)?,
                max: number(3)?,
            }))
        })
    }
    pub(super) fn comparison(
        &self,
        row: &PillView<'_>,
        index: usize,
        op: crate::Compare,
        rhs: &Data,
    ) -> Result<super::CountBounds, Error> {
        self.comparison_with(row, index, op, rhs, None)
    }
    pub(super) fn categories(
        &self,
        index: usize,
        op: crate::Compare,
        rhs: &Data,
    ) -> Option<Vec<bool>> {
        let binding = &self.bindings[index];
        if !matches!(
            binding.field.class,
            SummaryClass::Categorical | SummaryClass::Ordinal
        ) {
            return None;
        }
        binding
            .field
            .values
            .iter()
            .map(|v| category_matches(v, op, rhs))
            .collect::<Result<Vec<_>, _>>()
            .ok()
    }
    pub(super) fn comparison_with(
        &self,
        row: &PillView<'_>,
        index: usize,
        op: crate::Compare,
        rhs: &Data,
        categories: Option<&[bool]>,
    ) -> Result<super::CountBounds, Error> {
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
            let matches = |value: &Data| category_matches(value, op, rhs);
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
            let counts = self.category_counts(row, b)?;
            let mut total = 0u64;
            let mut selected = 0u64;
            for (i, (value, n)) in b.field.values.iter().zip(counts).enumerate() {
                let n = n?;
                total = total.checked_add(n).ok_or(Error::Overflow)?;
                if categories.map_or_else(|| matches(value), |bits| Ok(bits[i]))? {
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
    pub(super) fn field_count(&self) -> usize { self.bindings.len() }
    pub(super) fn with_unknown(&self, bits: Vec<u8>) -> Self {
        let mut plan = self.clone();
        plan.unknown = Some(bits.into());
        plan
    }
    pub(super) fn is_null(&self, _row: &PillView<'_>, index: usize) -> Result<bool, Error> {
        if self.layout.mode() == crate::EncodingMode::FixedSparse && self.unknown.is_none() {
            return Err(error("mixed summary access requires its period exception context"));
        }
        Ok(self.unknown.as_ref().is_some_and(|bits| bits[index / 8] & (1 << (index % 8)) != 0))
    }
    pub(super) fn project_field(&self, row: &PillView<'_>, index: usize) -> Result<Data, Error> {
        if self.is_null(row, index)? {
            return Ok(Data::Null);
        }
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

pub(super) fn number_type(s: Scalar) -> Result<(&'static str, u16), Error> {
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

fn category_matches(value: &Data, op: crate::Compare, rhs: &Data) -> Result<bool, Error> {
    use crate::Compare;
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
}
