//! Definition transitions at Box-relative byte offsets. Present even for one shape;
//! physical Pack offsets stay in the Box's ordinary addressing machinery.
use super::*;
use pil_codec::r#box::versions::{Binding, Directory, Transition};

const RX: &str = "summary.rx.";
pub(super) struct Versions {
    pub(super) layouts: Vec<Arc<SummaryLayout>>,
    runs: Directory,
    contexts: BTreeMap<u64, usize>,
}
impl Versions {
    pub fn new(layout: Arc<SummaryLayout>) -> Self {
        let mixed = layout.is_versioned();
        Self {
            layouts: vec![layout],
            runs: Directory::new(if mixed {
                Binding::MixedSpan
            } else {
                Binding::Definition(0)
            }),
            contexts: if mixed {
                BTreeMap::from([(0, 0)])
            } else {
                BTreeMap::new()
            },
        }
    }

    pub fn read(inner: &PackBox<'_>) -> Result<Self, Error> {
        if inner.options().external_addresses {
            return Err(error("summary Box requires ordinal addressing"));
        }
        let headers = inner.headers();
        let base = SummaryLayout::from_headers(headers)?;
        let extra = headers.keys().filter(|key| key.starts_with("summary-shape.")).count();
        let mut layouts = vec![base.clone()];
        for index in 1..=extra {
            let shape = headers
                .get(&format!("summary-shape.{index}"))
                .ok_or_else(|| error("missing summary version shape"))?;
            let mut metadata = BTreeMap::from([("summary-shape".into(), shape.clone())]);
            if let Some(bytes) = headers.get(&format!("{RX}{index}")) {
                metadata.insert("summary.rx".into(), bytes.clone());
            }
            if let Some(bindings) = headers.get(&format!("summary.bindings.{index}")) {
                metadata.insert(bindings::HEADER.into(), bindings.clone());
            }
            if let Some(context) = headers.get(&format!("summary.source.{index}")) {
                metadata.insert("summary.source".into(), context.clone());
            }
            let prefix = format!("summary.variants.{index}.");
            for (key, value) in headers {
                if let Some(suffix) = key.strip_prefix(&prefix) {
                    metadata.insert(format!("summary.variant.{suffix}"), value.clone());
                }
            }
            let layout = SummaryLayout::from_headers(&metadata)?;
            if layout.rows != base.rows {
                return Err(error("summary period count changed"));
            }
            layouts.push(layout);
        }
        let runs = inner
            .versions()?
            .ok_or_else(|| error("missing summary version directory"))?;
        let mut contexts = BTreeMap::new();
        for (key, value) in headers {
            let Some(offset) = key.strip_prefix("summary.context.") else {
                continue;
            };
            let offset = offset
                .parse::<u64>()
                .map_err(|_| error("invalid summary context offset"))?;
            if value.is_empty() || value.len() > 8 {
                return Err(error("invalid summary context reference"));
            }
            let mut bytes = [0; 8];
            bytes[..value.len()].copy_from_slice(value);
            let reference =
                usize::try_from(u64::from_le_bytes(bytes)).map_err(|_| Error::Overflow)?;
            if layouts
                .get(reference)
                .is_none_or(|layout| !layout.is_versioned())
            {
                return Err(error("invalid mixed summary context"));
            }
            contexts.insert(offset, reference);
        }
        for entry in runs.entries() {
            match entry.binding {
                Binding::Definition(reference)
                    if usize::try_from(reference)
                        .ok()
                        .is_some_and(|i| i < layouts.len() && !layouts[i].is_versioned()) => {}
                Binding::MixedSpan if contexts.contains_key(&entry.offset) => {}
                _ => return Err(error("unregistered summary Definition span")),
            }
        }
        Ok(Self {
            layouts,
            runs,
            contexts,
        })
    }

    pub fn definitions(&self) -> Result<SummaryDefinitions, Error> {
        let documents = self.layouts.iter().flat_map(|layout| layout.definitions())
            .map(|definition| doc::read(definition.prescription().bytes()))
            .collect::<Result<Vec<_>, _>>()?;
        SummaryDefinitions::restore(documents)
    }

    pub fn base(&self) -> &Arc<SummaryLayout> {
        &self.layouts[0]
    }
    pub fn is_uniform(&self) -> bool {
        self.layouts.len() == 1
    }
    pub fn transitions(&self) -> &[Transition] {
        self.runs.entries()
    }
    pub fn for_pack(
        &self,
        inner: &PackBox<'_>,
        ordinal: usize,
    ) -> Result<&Arc<SummaryLayout>, Error> {
        self.at(inner.member_range(ordinal)?.start, inner.next_address())
    }
    pub fn at(&self, offset: u64, member_end: u64) -> Result<&Arc<SummaryLayout>, Error> {
        let reference = match self.runs.binding_at(offset, member_end) {
            Some(Binding::Definition(reference)) => {
                usize::try_from(reference).map_err(|_| Error::Overflow)?
            }
            Some(Binding::MixedSpan) => {
                *self
                    .contexts
                    .range(..=offset)
                    .next_back()
                    .ok_or_else(|| error("missing mixed summary context"))?
                    .1
            }
            None => return Err(error("summary Definition binding unavailable")),
        };
        self.layouts
            .get(reference)
            .ok_or_else(|| error("summary Definition binding unavailable"))
    }
    pub fn write(&self, inner: &mut PackBox<'_>) -> Result<(), Error> {
        self.runs.attach(inner)?;
        for (&offset, &reference) in &self.contexts {
            let mut bytes = (reference as u64).to_le_bytes().to_vec();
            while bytes.len() > 1 && bytes.last() == Some(&0) {
                bytes.pop();
            }
            inner.header(format!("summary.context.{offset}"), bytes)?;
        }
        Ok(())
    }
    pub fn append(&mut self, inner: &mut PackBox<'_>, pack: SummaryPack) -> Result<(), Error> {
        if inner.is_sealed() {
            return Err(error("summary Box is sealed"));
        }
        if pack.rows != self.base().rows {
            return Err(error("summary period count changed"));
        }
        let existing = self
            .layouts
            .iter()
            .position(|layout| layout.headers == pack.layout.headers);
        let index = existing.unwrap_or(self.layouts.len());
        let mut runs = self.runs.clone();
        let offset = inner.next_address();
        let mixed = pack.layout.is_versioned();
        let new_context = mixed
            && (self
                .runs
                .entries()
                .last()
                .is_none_or(|r| r.binding != Binding::MixedSpan)
                || self
                    .contexts
                    .last_key_value()
                    .is_none_or(|(_, reference)| *reference != index));
        runs.change(
            offset,
            if mixed {
                Binding::MixedSpan
            } else {
                Binding::Definition(index as u64)
            },
        )?;
        if inner.options().fixed_length_objects && !inner.is_empty() {
            let range = inner.member_range(0)?;
            if range.end - range.start != pack.bytes.len() as u64 {
                inner.indexed()?;
            }
        }
        let new_layout = existing.is_none().then(|| pack.layout.clone());
        inner.pack(pack.bytes)?;
        if let Some(layout) = new_layout {
            if layout.context.is_empty() {
                inner.header(format!("{RX}{index}"), pack.definition.prescription().bytes().to_vec())?;
            }
            inner.header(format!("summary-shape.{index}"), layout.shape.clone())?;
            if !layout.bindings.is_empty() { inner.header(format!("summary.bindings.{index}"), layout.bindings.clone())?; }
            if !layout.context.is_empty() {
                inner.header(format!("summary.source.{index}"), layout.context.iter().flatten().copied().flat_map(u32::to_le_bytes).collect::<Vec<_>>())?;
            }
            for (variant, definition) in layout.variants.iter().enumerate() {
                inner.header(
                    format!("summary.variants.{index}.{}", variant + 1),
                    definition.prescription().bytes().to_vec(),
                )?;
            }
            self.layouts.push(layout);
        }
        if new_context {
            self.contexts.insert(offset, index);
        }
        self.runs = runs;
        self.write(inner)
    }
}
