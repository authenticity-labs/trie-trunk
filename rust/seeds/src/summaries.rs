//! Row-oriented fixed summary storage. Each Pill contains one period's full
//! set of annotated-field class objects. Columns are an execution view, not a
//! different on-disk PAC layout. Event storage remains separate.
mod accumulator;
mod writer;
pub use writer::SummaryWrite;
mod bindings;
mod definitions;
mod template;
pub use definitions::SummaryDefinitions;
pub use template::summary_template;
mod annotated;
mod classes;
#[cfg(feature = "registration")]
mod columns;
mod preview;
mod projection;
mod reader;
#[cfg(feature = "registration")]
mod report;
#[cfg(feature = "registration")]
pub use report::SummaryReport;
#[cfg(feature = "registration")]
mod table;
pub use projection::{SummaryProjection, SummaryPeriods};
pub use reader::SummaryReader;
#[cfg(feature = "registration")]
pub use table::{Coverage, SummaryTable};
mod selection;
mod series;
mod versions;
mod window;
use crate::{
    Document, EncodingMode, Error, Field, PackOptions, Seed, SeedDefinition, SeedPack, doc, error,
};
pub use accumulator::SummaryAccumulator;
pub use annotated::mixed::{MixedSummaryBucket, MixedSummaryPlan};
pub use annotated::{SummaryBucket, SummaryPlan};
pub(crate) use classes::analytics;
pub(crate) use classes::classify;
pub use classes::{SummaryAnalytics, SummaryClass, SummaryField};
use pil_codec::{
    Value,
    api::{NativeValue, Pack, encode_headers},
    r#box::{Box as PackBox, BoxOptions, Commit},
};
pub(crate) use annotated::Prepared;
pub use preview::{
    CountBounds, SummaryPeriod, SummaryPreview, SummaryWindowPreview, SummaryWindowResult,
};
pub use selection::SummarySelection;
pub use series::{
    SummaryAggregate, SummaryMeasure, SummaryPoint, SummarySeries, SummarySeriesBuilder,
};
use std::{collections::BTreeMap, sync::Arc};
pub use window::{
    SUMMARY_DAY_MS, SUMMARY_DAY_ROWS, SUMMARY_PERIOD_MS, SummaryCalendar, SummaryWindow,
};

/// One immutable row shape for a daily Pack. Object classes may have different
/// widths, but every Pill in that Pack contains the same set of objects and
/// therefore the same total width. SummaryBox binds each day to its shape.
#[derive(Clone)]
pub struct SummaryLayout {
    definition: Arc<SeedDefinition>,
    rows: usize,
    mode: EncodingMode,
    shape: Vec<u8>,
    headers: Vec<u8>,
    variants: Vec<Arc<SeedDefinition>>,
    class_bindings: Option<Arc<[annotated::Binding]>>,
    bindings: Vec<u8>,
    // Taxonomy-owned context, shared with the event reader. Never serialized
    // into a daily PAC or turned into another Definition.
    source: Option<Arc<SeedDefinition>>,
    context: Vec<[u32; 2]>,
}

fn shape_document() -> Result<Document, Error> {
    doc::new("summary-shape")
        .field(Field::new("rows").r#type("uint").size(8))
        .field(Field::new("mode").r#type("uint").size(1))
        .field(Field::new("arrays").array("uint").element_size(8))
        .sign()
}

fn shape_rx() -> Result<&'static pil_codec::api::Prescription, Error> {
    static RX: std::sync::OnceLock<Result<pil_codec::api::Prescription, String>> =
        std::sync::OnceLock::new();
    RX.get_or_init(|| {
        (|| -> Result<_, Error> { shape_document()?.prescribe() })().map_err(|e| e.to_string())
    })
    .as_ref()
    .map_err(error)
}

impl SummaryLayout {
    pub fn new(document: &Document, rows: usize) -> Result<Arc<Self>, Error> {
        Self::with_mode(document, rows, EncodingMode::FixedFull)
    }
    pub fn sparse(document: &Document, rows: usize) -> Result<Arc<Self>, Error> {
        Self::with_mode(document, rows, EncodingMode::FixedSparse)
    }
    /// Application-known dimensions specialize the plan, never the Definition.
    pub fn arrays(
        document: &Document,
        rows: usize,
        lengths: &[(usize, usize)],
    ) -> Result<Arc<Self>, Error> {
        Self::with_arrays(document, rows, EncodingMode::FixedFull, lengths)
    }
    fn with_mode(document: &Document, rows: usize, mode: EncodingMode) -> Result<Arc<Self>, Error> {
        Self::with_arrays(document, rows, mode, &[])
    }
    fn with_arrays(
        document: &Document,
        rows: usize,
        mode: EncodingMode,
        lengths: &[(usize, usize)],
    ) -> Result<Arc<Self>, Error> {
        let parameters=lengths.iter().map(|&(f,n)|pil_codec::api::Binding::count(vec![pil_codec::api::Step::Field(f)],n)).collect::<Vec<_>>();
        Self::prepare(document,rows,mode,&parameters,None)
    }
    pub fn parameters(document:&Document,rows:usize,parameters:&[pil_codec::api::Binding])->Result<Arc<Self>,Error>{
        Self::prepare(document,rows,EncodingMode::FixedFull,parameters,None)
    }
    fn prepare(document:&Document,rows:usize,mode:EncodingMode,parameters:&[pil_codec::api::Binding],deferred:Option<&[(usize,usize)]>)->Result<Arc<Self>,Error>{
        if rows==0 {return Err(error("summary Packs require a positive, fixed row count"));}
        let definition=if document.bytes()?==definitions::template()?.bytes()? {definitions::definition()?}else{SeedDefinition::prescribe(document)?};
        let definition=if parameters.is_empty(){definition}else{definition.parameters(parameters)?};
        Self::compiled(document,rows,mode,definition,deferred)
    }
    fn compiled(document:&Document,rows:usize,mode:EncodingMode,definition:Arc<SeedDefinition>,deferred:Option<&[(usize,usize)]>)->Result<Arc<Self>,Error>{
        if rows==0 {return Err(error("summary Packs require a positive, fixed row count"));}
        // Ask the shared encoder's prepared plan, rather than duplicating its
        // rules for fixed scalar, boolean, array, or composite layouts.
        if deferred.is_none() && !matches!(
            definition.seed().mode()?,
            EncodingMode::FixedFull | EncodingMode::FixedSparse
        ) {
            return Err(error("summary Definition must have fixed field extents"));
        }
        let rx = shape_rx()?;
        let mut pill = rx.dispense();
        pill.set_uint("rows", u64::try_from(rows).map_err(|_| Error::Overflow)?)?;
        pill.set_uint("mode", mode as u8)?;
        let dimensions = deferred.unwrap_or_else(||definition.prescription().array_lengths())
            .iter()
            .flat_map(|&(field, count)| [field, count])
            .map(|n| {
                u64::try_from(n)
                    .map(ciborium::Value::from)
                    .map_err(|_| Error::Overflow)
            })
            .collect::<Result<Vec<_>, _>>()?;
        pill.set_array("arrays", dimensions)?;
        pill.cap()?;
        let shape = pill.bytes()?.to_vec();
        let headers = encode_headers(&BTreeMap::from([
            ("summary.rx".into(), document.bytes()?.to_vec()),
            ("summary-shape".into(), shape.clone()),
        ]))?;
        Ok(Arc::new(Self {
            definition,
            rows,
            mode,
            shape,
            headers,
            variants: Vec::new(),
            class_bindings: None,
            bindings: Vec::new(),
            source: None,
            context: Vec::new(),
        }))
    }

    fn from_headers(headers: &BTreeMap<String, Vec<u8>>) -> Result<Arc<Self>, Error> {
        let bytes = match headers.get("summary.rx") {
            Some(bytes) => bytes.as_slice(),
            None if headers.contains_key("summary.source") => definitions::template()?.bytes()?,
            None => return Err(error("missing summary Definition")),
        };
        let parsed;
        let document = if bytes == definitions::template()?.bytes()? {
            definitions::template()?
        } else {
            parsed = doc::read(bytes)?;
            &parsed
        };
        let rx = shape_rx()?;
        let mut decoder = rx.decoder();
        let view = decoder.read(
            headers
                .get("summary-shape")
                .ok_or_else(|| error("missing summary shape"))?,
        )?;
        let NativeValue::Scalar(Value::U64(rows)) = view.native_value(0)? else {
            return Err(error("invalid summary row count"));
        };
        let mode = match view.native_value(1)? {
            NativeValue::Scalar(Value::U8(2)) => EncodingMode::FixedSparse,
            NativeValue::Scalar(Value::U8(3)) => EncodingMode::FixedFull,
            _ => return Err(error("invalid summary encoding mode")),
        };
        let crate::Cell::Value(ciborium::Value::Array(dimensions)) =
            crate::value::own(view.native_value(2)?)?
        else {
            return Err(error("invalid summary array dimensions"));
        };
        if dimensions.len() % 2 != 0 {
            return Err(error("invalid summary array dimensions"));
        }
        let number = |v: &ciborium::Value| -> Result<usize, Error> {
            usize::try_from(i128::from(
                v.as_integer()
                    .ok_or_else(|| error("invalid summary array dimension"))?,
            ))
            .map_err(|_| Error::Overflow)
        };
        let lengths = dimensions
            .chunks_exact(2)
            .map(|pair| Ok((number(&pair[0])?, number(&pair[1])?)))
            .collect::<Result<Vec<_>, Error>>()?;
        // Generic class types derive from the owning source context, not a
        // duplicated type catalogue in the Box. Registered context is resolved
        // by registered_box before any rows are exposed.
        let mut base = if document.bytes()?==definitions::template()?.bytes()? {
            if let Some(bytes)=headers.get(bindings::HEADER) {
                let source=bindings::read(bytes)?;
                let prepared=SummaryPlan::prepared(&source)?;
                let mut layout=Self::compiled(document,usize::try_from(rows).map_err(|_|Error::Overflow)?,mode,prepared.definition.clone(),None)?;
                Arc::make_mut(&mut layout).class_bindings=Some(prepared.bindings.clone());
                layout
            }else{
                Self::prepare(document,usize::try_from(rows).map_err(|_|Error::Overflow)?,mode,&[],Some(&lengths))?
            }
        } else {Self::with_arrays(document,usize::try_from(rows).map_err(|_|Error::Overflow)?,mode,&lengths)?};
        if base.shape != headers["summary-shape"] {return Err(error("summary source differs from stored shape"));}
        if let Some(bindings) = headers.get(bindings::HEADER) {
            Arc::make_mut(&mut base).bindings = bindings.clone();
            Arc::make_mut(&mut base).headers = encode_headers(&base.header_fields())?;
        }
        if let Some(context) = headers.get("summary.source") {
            if context.is_empty() || context.len() % 8 != 0 || !base.bindings.is_empty() {
                return Err(error("invalid summary source context"));
            }
            Arc::make_mut(&mut base).context = context.chunks_exact(8).map(|pair| [
                u32::from_le_bytes(pair[..4].try_into().unwrap()),
                u32::from_le_bytes(pair[4..].try_into().unwrap()),
            ]).collect();
            Arc::make_mut(&mut base).headers = encode_headers(&base.header_fields())?;
        }
        let count = headers
            .keys()
            .filter_map(|key| key.strip_prefix("summary.variant.")?.parse::<usize>().ok())
            .count();
        if count == 0 {
            return Ok(base);
        }
        let mut layouts = vec![base];
        for index in 1..=count {
            let raw = headers
                .get(&format!("summary.variant.{index}"))
                .ok_or_else(|| error("summary variant gap"))?;
            layouts.push(Self::with_mode(
                &doc::read(raw.as_slice())?,
                rows as usize,
                mode,
            )?);
        }
        Self::family(layouts)
    }
    fn header_fields(&self) -> BTreeMap<String, Vec<u8>> {
        let mut fields = BTreeMap::from([
            ("summary-shape".into(), self.shape.clone()),
        ]);
        if self.context.is_empty() {
            fields.insert("summary.rx".into(), self.definition.prescription().bytes().to_vec());
        }
        if !self.bindings.is_empty() {
            fields.insert(bindings::HEADER.into(), self.bindings.clone());
        }
        if !self.context.is_empty() {
            fields.insert("summary.source".into(), self.context.iter().flatten().copied().flat_map(u32::to_le_bytes).collect());
        }
        for (index, definition) in self.variants.iter().enumerate() {
            fields.insert(
                format!("summary.variant.{}", index + 1),
                definition.prescription().bytes().to_vec(),
            );
        }
        fields
    }
    fn family(layouts: Vec<Arc<Self>>) -> Result<Arc<Self>, Error> {
        let base = layouts
            .first()
            .ok_or_else(|| error("missing summary layouts"))?;
        if layouts.iter().any(|layout| {
            layout.rows != base.rows || layout.mode != base.mode || !layout.variants.is_empty()
        }) {
            return Err(error("summary family shape mismatch"));
        }
        let mut family = (**base).clone();
        family.variants = layouts
            .iter()
            .skip(1)
            .map(|layout| layout.definition.clone())
            .collect();
        family.headers = encode_headers(&family.header_fields())?;
        Ok(Arc::new(family))
    }
    pub(super) fn with_bindings(self: Arc<Self>, bytes: Vec<u8>) -> Result<Arc<Self>, Error> {
        let mut layout = self;
        Arc::make_mut(&mut layout).bindings = bytes;
        Arc::make_mut(&mut layout).headers = encode_headers(&layout.header_fields())?;
        Ok(layout)
    }
    pub(super) fn with_context(mut self: Arc<Self>, context: Vec<[u32; 2]>) -> Result<Arc<Self>, Error> {
        // Attaching the same owner must not serialize unchanged metadata again.
        if self.context == context && (context.is_empty() || self.bindings.is_empty()) {
            return Ok(self);
        }
        Arc::make_mut(&mut self).context = context;
        if !self.context.is_empty() { Arc::make_mut(&mut self).bindings.clear(); }
        Arc::make_mut(&mut self).headers = encode_headers(&self.header_fields())?;
        Ok(self)
    }
    pub(super) fn with_source(mut self: Arc<Self>, source: Arc<SeedDefinition>) -> Result<Arc<Self>, Error> {
        let context: Vec<_> = source.registration.into_iter().collect();
        if !self.context.is_empty() && self.context != context {
            return Err(error("summary source context mismatch"));
        }
        Arc::make_mut(&mut self).source = Some(source);
        self.with_context(context)
    }
    pub fn definitions(&self) -> impl Iterator<Item = &Arc<SeedDefinition>> {
        std::iter::once(&self.definition).chain(self.variants.iter())
    }
    pub fn is_versioned(&self) -> bool {
        !self.variants.is_empty()
    }

    pub fn definition(&self) -> &Arc<SeedDefinition> {
        &self.definition
    }
    pub fn rows(&self) -> usize {
        self.rows
    }
    pub fn mode(&self) -> EncodingMode {
        self.mode
    }

    /// Open a Pack outside the Box. The application appends completed
    /// accumulator intervals and seals only when the daily Pack is complete.
    pub fn fill(&self) -> Result<SummaryPackBuilder<'_>, Error> {
        let mut pack = self.definition.fill(self.rows)?;
        if !self.variants.is_empty() {
            for (reference, definition) in self.definitions().enumerate() {
                pack.register(reference as u64, definition.prescription())?;
            }
        }
        pack.configure(PackOptions {
            fixed: self.variants.is_empty(),
            // Factoring would make all-zero and active Packs different sizes.
            common_values: false,
            ..Default::default()
        })?;
        Ok(SummaryPackBuilder {
            layout: self,
            inner: Some(pack),
            exceptions: None,
        })
    }

    /// Every period has a row. Ordinary layouts require Fixed Full; mixed
    /// layouts use Fixed Sparse and reserve value slots for native nulls.
    pub fn pack<'a>(
        &'a self,
        rows: impl IntoIterator<Item = Seed<'a>>,
    ) -> Result<SummaryPack, Error> {
        let mut pack = self.fill()?;
        for row in rows {
            pack.load(row)?;
        }
        pack.seal()
    }

    /// Share the Box allocation with the query reader. PAC-local version
    /// references resolve to prepared row schemas; no child payload is copied.
    pub fn open_range(
        &self,
        owner: Arc<Vec<u8>>,
        range: std::ops::Range<usize>,
    ) -> Result<Arc<crate::SeedPackSnapshot>, Error> {
        let raw = owner.get(range.clone()).ok_or(Error::Incomplete)?;
        let flags = pil_codec::pac::flags(raw)?;
        if flags & pil_codec::pac::MULTI_DEFINITION == 0 {
            let lengths = self.definition.prescription().array_lengths();
            let empty_arrays = !lengths.is_empty()
                && lengths.len() == self.definition.prescription().definition().fields().len()
                && lengths.iter().all(|&(_, count)| count == 0);
            if empty_arrays {
                self.open(raw)?;
            }
            let snapshot = self.definition.open_range(owner, range)?;
            // Check the summary contract against metadata the shared reader
            // already parsed. Do not open and discard a second Pack reader.
            let headers = snapshot.pack.headers();
            if (!empty_arrays
                && flags
                    & (pil_codec::pac::ADDRESSING
                        | pil_codec::pac::COMMON
                        | pil_codec::pac::PRESENCE)
                    != 0)
                || snapshot.len() != self.rows
                || headers
                    .keys()
                    .any(|key| !(self.mode == EncodingMode::FixedSparse && key == "summary.unknown"))
            {
                return Err(error("summary Pack violates its fixed storage contract"));
            }
            if snapshot.pack.prescription().binding_bytes() != self.definition.prescription().binding_bytes() {
                return Err(error("summary Pack dimensions differ from its layout"));
            }
            if snapshot.pack.reader().get(0)?.mode() != self.mode {
                return Err(error("summary Pack encoding mode differs from its layout"));
            }
            return Ok(snapshot);
        }
        let index = pil_codec::pac::versions::PackIndex::open(raw)?;
        if index.count() != self.rows as u64 {
            return Err(error("invalid summary Pack extent"));
        }
        let byte_length = range.len();
        let allocation = owner.as_ptr() as usize;
        let mut lineage = std::collections::HashMap::new();
        let pack = pil_codec::api::OwnedPack::shared_registered_range(
            self.definition.rx.clone(),
            owner,
            range,
            |reference| {
                let index = usize::try_from(reference).map_err(|_| Error::Overflow)?;
                let definition = if index == 0 {
                    &self.definition
                } else {
                    self.variants
                        .get(index - 1)
                        .ok_or_else(|| error("unregistered summary Definition"))?
                };
                lineage.insert(definition.rx.source_identity(), definition.clone());
                Ok(definition.rx.clone())
            },
        )?;
        Ok(Arc::new(crate::SeedPackSnapshot {
            definition: self.definition.clone(),
            lineage: Some(Arc::new(lineage)),
            pack: Arc::new(pack),
            byte_length,
            allocation,
            storage: None,
            memory: None,
            write_checked: std::sync::atomic::AtomicBool::new(false),
        }))
    }
    pub fn open<'a>(&'a self, bytes: &'a [u8]) -> Result<Pack<'a>, Error> {
        // RX owns all footer/mode/address interpretation. Compression and
        // factoring are deliberately absent from this fixed-size policy.
        let pack = if pil_codec::pac::flags(bytes)? & pil_codec::pac::MULTI_DEFINITION != 0 {
            let mut pack = self.definition.prescription().fill(0)?;
            for (reference, definition) in self.definitions().enumerate() {
                pack.register(reference as u64, definition.prescription())?;
            }
            pack.restore(bytes)?;
            pack
        } else {
            self.definition.prescription().open(bytes)?
        };
        // An all-empty bound row has no member values. RX uses its existing
        // common-only representation; there is still exactly one empty row per
        // period and no record directory. Other shapes must not be factored.
        let lengths = self.definition.prescription().array_lengths();
        let empty_arrays = !lengths.is_empty()
            && lengths.len() == self.definition.prescription().definition().fields().len()
            && lengths.iter().all(|&(_, count)| count == 0);
        if (self.variants.is_empty() && !pack.options().fixed && !empty_arrays)
            || pack.len() != self.rows
            || (!empty_arrays && pack.options().common_values)
            || (!empty_arrays && !pack.options().common.is_empty())
            || pack
                .headers()
                .keys()
                .any(|key| !(self.mode == EncodingMode::FixedSparse && key == "summary.unknown"))
        {
            return Err(error("summary Pack violates its fixed storage contract"));
        }
        if pack.reader().get(0)?.prescription().binding_bytes() != self.definition.prescription().binding_bytes() {
            return Err(error("summary Pack dimensions differ from its layout"));
        }
        if pack.reader().get(0)?.mode() != self.mode {
            return Err(error("summary Pack encoding mode differs from its layout"));
        }
        Ok(pack)
    }
}

/// An open daily Pack cannot be passed to SummaryBox::append. Only seal()
/// produces the immutable child type accepted by that operation.
pub struct SummaryPackBuilder<'a> {
    layout: &'a SummaryLayout,
    inner: Option<SeedPack<'a>>,
    exceptions: Option<(usize, Vec<u8>)>,
}
impl<'a> SummaryPackBuilder<'a> {
    pub(super) fn exceptions(&mut self, unknown: &[bool]) -> Result<(), Error> {
        let ordinal = self.len();
        if ordinal >= self.layout.rows { return Err(error("summary exception period outside Pack")); }
        let bits = unknown.len().checked_mul(self.layout.rows).ok_or(Error::Overflow)?;
        let (fields, bytes) = self.exceptions.get_or_insert_with(|| (unknown.len(), vec![0; bits.div_ceil(8)]));
        if *fields != unknown.len() { return Err(error("summary exception field count changed")); }
        for (field, &missing) in unknown.iter().enumerate() {
            let bit = ordinal * *fields + field;
            let mask = 1 << (bit % 8);
            if missing { bytes[bit / 8] |= mask; } else { bytes[bit / 8] &= !mask; }
        }
        Ok(())
    }
    fn publish_exceptions(&mut self) -> Result<(), Error> {
        if let Some((_, bytes)) = &self.exceptions {
            self.inner.as_mut().ok_or_else(|| error("summary Pack is sealed"))?
                .header("summary.unknown", bytes.clone())?;
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.inner
            .as_ref()
            .map_or(self.layout.rows, |pack| pack.len())
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn is_sealed(&self) -> bool {
        self.inner.is_none()
    }
    pub fn load(&mut self, mut row: Seed<'a>) -> Result<&mut Self, Error> {
        let pack = self
            .inner
            .as_mut()
            .ok_or_else(|| error("summary Pack is sealed"))?;
        if pack.len() == self.layout.rows {
            return Err(error("too many summary rows"));
        }
        if self.layout.mode == EncodingMode::FixedSparse {
            row.sparse()?;
        } else if row.mode()? != EncodingMode::FixedFull {
            return Err(error("summary rows must be Fixed Full"));
        }
        pack.load(row)?;
        Ok(self)
    }
    /// Recovery bytes remain unsealed and are not admissible Box children.
    pub fn snapshot(&mut self) -> Result<Vec<u8>, Error> {
        self.publish_exceptions()?;
        self.inner
            .as_ref()
            .ok_or_else(|| error("summary Pack is sealed"))?
            .snapshot()
    }
    pub fn seal(&mut self) -> Result<SummaryPack, Error> {
        self.publish_exceptions()?;
        let pack = self
            .inner
            .as_mut()
            .ok_or_else(|| error("summary Pack is sealed"))?;
        if pack.len() != self.layout.rows {
            return Err(error("incomplete summary Pack"));
        }
        pack.seal()?;
        let bytes = self
            .inner
            .take()
            .expect("open Pack checked above")
            .into_bytes()?;
        self.layout.open(&bytes)?;
        Ok(SummaryPack {
            definition: self.layout.definition.clone(),
            rows: self.layout.rows,
            layout: Arc::new(self.layout.clone()),
            bytes,
        })
    }
}

/// Only sealing a complete SummaryPackBuilder can produce an admitted child.
pub struct SummaryPack {
    layout: Arc<SummaryLayout>,
    definition: Arc<SeedDefinition>,
    rows: usize,
    bytes: Vec<u8>,
}
impl SummaryPack {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// `.summ` storage, using the existing Box codec and its replacement-tail
/// commit. File writes and publication belong to the datastore caller.
pub struct SummaryBox {
    versions: versions::Versions,
    inner: PackBox<'static>,
}
impl SummaryBox {
    pub fn new(layout: Arc<SummaryLayout>) -> Result<Self, Error> {
        let mut inner = PackBox::new(
            &layout.headers,
            BoxOptions {
                // Each day retains fixed rows, but its array dimensions can
                // differ from another day's. Address days through the Box
                // directory instead of imposing one global Pack stride.
                fixed_length_objects: false,
                external_addresses: false,
            },
        )?;
        let versions = versions::Versions::new(layout);
        versions.write(&mut inner)?;
        Ok(Self { versions, inner })
    }
    pub fn open(bytes: Vec<u8>) -> Result<Self, Error> {
        let inner = PackBox::open_owned(bytes)?;
        Self::from_box(inner)
    }
    /// Resume appends from storage metadata without loading historical Packs.
    /// The caller writes commit.bytes at commit.offset and truncates to length.
    pub fn open_footer(footer: Vec<u8>, length: u64) -> Result<Self, Error> {
        Self::from_box(PackBox::open_footer_owned(footer, length)?)
    }
    fn from_box(inner: PackBox<'static>) -> Result<Self, Error> {
        if inner.options().external_addresses {
            return Err(error("summary Box requires ordinal addressing"));
        }
        let versions = versions::Versions::read(&inner)?;
        Ok(Self { versions, inner })
    }
    /// Initial layout. Use layout_at for a selected day in a versioned Box.
    pub fn layout(&self) -> &Arc<SummaryLayout> {
        self.versions.base()
    }
    pub fn layout_at(&self, ordinal: usize) -> Result<&Arc<SummaryLayout>, Error> {
        self.versions.at(
            self.inner.member_range(ordinal)?.start,
            self.inner.next_address(),
        )
    }
    pub fn is_uniform(&self) -> bool {
        self.versions.is_uniform()
    }
    /// Recover persisted summary revisions directly from the existing footer
    /// catalog. Pack bodies and source event Definitions are not required.
    pub fn definitions(&self) -> Result<SummaryDefinitions, Error> {
        self.versions.definitions()
    }
    pub fn definition_transitions(&self) -> &[pil_codec::r#box::versions::Transition] {
        self.versions.transitions()
    }
    pub fn len(&self) -> usize {
        self.inner.len()
    }
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    pub fn append(&mut self, pack: SummaryPack) -> Result<&mut Self, Error> {
        self.versions.append(&mut self.inner, pack)?;
        Ok(self)
    }
    pub fn pack(&self, ordinal: usize) -> Result<Pack<'_>, Error> {
        self.layout_at(ordinal)?.open(self.inner.get(ordinal)?)
    }
    pub fn pack_range(&self, ordinal: usize) -> Result<std::ops::Range<u64>, Error> {
        self.inner.member_range(ordinal)
    }
    pub fn commit(&mut self) -> Result<Commit<'_>, Error> {
        self.inner.commit()
    }
    /// Prepare the existing replacement tail for the node's commit scheduler.
    /// The returned writer retains its borrow until the owner has staged it.
    pub fn write(&mut self) -> Result<SummaryWrite<'_>, Error> {
        SummaryWrite::new(self.inner.commit()?)
    }
    pub fn seal(&mut self) -> Result<Commit<'_>, Error> {
        self.inner.seal()
    }
    pub fn bytes(&self) -> Result<&[u8], Error> {
        self.inner.bytes()
    }
}

/// Borrow the complete `.summ` allocation. Opening reads its footer; selecting
/// a Pack checks only that child, without decoding the other summary rows.
pub struct SummaryBoxReader<'a> {
    versions: versions::Versions,
    inner: PackBox<'a>,
}
impl<'a> SummaryBoxReader<'a> {
    pub fn open_owned(bytes: Vec<u8>) -> Result<SummaryBoxReader<'static>, Error> {
        SummaryBoxReader::from_box(PackBox::open_owned(bytes)?)
    }
    pub fn open(bytes: &'a [u8]) -> Result<Self, Error> {
        let inner = PackBox::open(bytes)?;
        Self::from_box(inner)
    }
    pub fn open_footer(footer: &'a [u8], length: u64) -> Result<Self, Error> {
        Self::from_box(PackBox::open_footer(footer, length)?)
    }
    pub fn open_footer_owned(
        footer: Vec<u8>,
        length: u64,
    ) -> Result<SummaryBoxReader<'static>, Error> {
        SummaryBoxReader::from_box(PackBox::open_footer_owned(footer, length)?)
    }
    fn from_box(inner: PackBox<'a>) -> Result<Self, Error> {
        if inner.options().external_addresses {
            return Err(error("summary Box requires ordinal addressing"));
        }
        let versions = versions::Versions::read(&inner)?;
        Ok(Self { versions, inner })
    }
    /// Initial layout. Use layout_at for a selected day in a versioned Box.
    pub fn layout(&self) -> &Arc<SummaryLayout> {
        self.versions.base()
    }
    pub fn layout_at(&self, ordinal: usize) -> Result<&Arc<SummaryLayout>, Error> {
        self.versions.at(
            self.inner.member_range(ordinal)?.start,
            self.inner.next_address(),
        )
    }
    pub fn is_uniform(&self) -> bool {
        self.versions.is_uniform()
    }
    /// Recover persisted summary revisions directly from the existing footer
    /// catalog. Pack bodies and source event Definitions are not required.
    pub fn definitions(&self) -> Result<SummaryDefinitions, Error> {
        self.versions.definitions()
    }
    pub fn definition_transitions(&self) -> &[pil_codec::r#box::versions::Transition] {
        self.versions.transitions()
    }
    pub fn len(&self) -> usize {
        self.inner.len()
    }
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    pub fn pack(&self, ordinal: usize) -> Result<Pack<'_>, Error> {
        self.layout_at(ordinal)?.open(self.inner.get(ordinal)?)
    }
    /// Select an on-disk child without fetching the other Packs.
    pub fn storage_range(&self, ordinal: usize) -> Result<std::ops::Range<u64>, Error> {
        self.inner.member_range(ordinal)
    }
    pub fn open_pack<'b>(&'b self, bytes: &'b [u8]) -> Result<Pack<'b>, Error> {
        if !self.is_uniform() {
            return Err(error("versioned summary Box requires a Pack ordinal"));
        }
        self.layout().open(bytes)
    }
    pub fn open_pack_at<'b>(&'b self, ordinal: usize, bytes: &'b [u8]) -> Result<Pack<'b>, Error> {
        let range = self.storage_range(ordinal)?;
        if range.end - range.start != bytes.len() as u64 {
            return Err(error("summary Pack extent mismatch"));
        }
        self.layout_at(ordinal)?.open(bytes)
    }
}

impl SummaryBoxReader<'_> {
    /// The Box codec locates the sealed child; callers retain the original
    /// allocation and bind this range without copying the child payload.
    pub fn pack_range(&self, ordinal: usize) -> Result<std::ops::Range<usize>, Error> {
        let child = self.inner.get(ordinal)?;
        if pil_codec::pac::flags(child)? & pil_codec::pac::MULTI_DEFINITION == 0 {
            self.layout_at(ordinal)?.open(child)?;
        } else {
            let index = pil_codec::pac::versions::PackIndex::open(child)?;
            if !index.versions().is_sealed()
                || index.count() != self.layout_at(ordinal)?.rows as u64
            {
                return Err(error("invalid summary Pack extent"));
            }
        }
        let owner = self.inner.bytes()?;
        let start = child.as_ptr() as usize - owner.as_ptr() as usize;
        Ok(start..start + child.len())
    }
}
