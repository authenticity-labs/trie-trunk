//! One reusable summary Definition. Field bindings and array dimensions are
//! Seed application metadata; changing either does not create RX revisions.
use super::{SummaryClass, SummaryPlan};
use crate::{Document, Error, SeedDefinition, doc, error};
use std::sync::{Arc, OnceLock};
const TEMPLATE: &[u8] = include_bytes!("../../definitions/summary.rx");
pub(crate) fn template() -> Result<&'static Document, Error> {
    static DOCUMENT: OnceLock<Result<Document, String>> = OnceLock::new();
    DOCUMENT
        .get_or_init(|| doc::read(TEMPLATE).map_err(|e| e.to_string()))
        .as_ref()
        .map_err(error)
}
// Immutable schemas are shared; only dimensions and source bindings belong to
// a resource plan. This cache is bounded by the authored class library.
pub(super) fn prescription(member: usize) -> Result<Arc<pil_codec::api::Prescription>, Error> {
    static READERS: OnceLock<Vec<OnceLock<Result<Arc<pil_codec::api::Prescription>, String>>>> =
        OnceLock::new();
    let document = template()?;
    let readers = READERS.get_or_init(|| {
        (0..document.definitions().len())
            .map(|_| OnceLock::new())
            .collect()
    });
    readers
        .get(member)
        .ok_or_else(|| error("unknown summary class"))?
        .get_or_init(|| {
            document
                .prescribe_member(member)
                .map(Arc::new)
                .map_err(|e| e.to_string())
        })
        .as_ref()
        .cloned()
        .map_err(error)
}
pub(super) fn definition() -> Result<Arc<SeedDefinition>, Error> {
    static ROOT: OnceLock<Result<Arc<SeedDefinition>, String>> = OnceLock::new();
    ROOT.get_or_init(|| {
        (|| -> Result<_, Error> { SeedDefinition::prescribe(template()?) })()
            .map_err(|e| e.to_string())
    })
    .as_ref()
    .cloned()
    .map_err(error)
}
pub(super) fn counter() -> Result<Arc<pil_codec::api::Prescription>, Error> {
    static RX: OnceLock<Result<Arc<pil_codec::api::Prescription>, String>> = OnceLock::new();
    RX.get_or_init(|| {
        (|| -> Result<_, Error> {
            Ok(Arc::new(
                doc::new("category-count")
                    .field(crate::Field::new("count").r#type("uint").size(3))
                    .sign()?
                    .prescribe()?,
            ))
        })()
        .map_err(|e| e.to_string())
    })
    .as_ref()
    .cloned()
    .map_err(error)
}
#[derive(Default)]
pub struct SummaryDefinitions {
    used: bool,
}
impl SummaryDefinitions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn len(&self) -> usize {
        usize::from(self.used)
    }
    pub fn is_empty(&self) -> bool {
        !self.used
    }
    pub fn prescribe(
        &mut self,
        source: Arc<SeedDefinition>,
        rows: usize,
    ) -> Result<SummaryPlan, Error> {
        let plan = SummaryPlan::generate(source, rows)?;
        self.used = true;
        Ok(plan)
    }
    pub fn document(&self) -> Result<Option<Document>, Error> {
        self.used.then(|| template().cloned()).transpose()
    }
    pub fn read(bytes: &[u8]) -> Result<Self, Error> {
        if doc::read(bytes)?.bytes()? != template()?.bytes()? {
            return Err(error("unsupported summary class library"));
        }
        Ok(Self { used: true })
    }
    pub(crate) fn restore(documents: Vec<Document>) -> Result<Self, Error> {
        if documents.is_empty() {
            return Err(error("summary catalog is empty"));
        }
        for document in documents {
            Self::read(document.bytes()?)?;
        }
        Ok(Self { used: true })
    }
}

pub(crate) fn semantic_annotations(source: &SeedDefinition, ordinal: usize) -> Vec<String> {
    let mut annotations = source
        .annotations(ordinal)
        .unwrap_or_default()
        .iter()
        .filter(|a| !a.starts_with("#summ(") && !a.starts_with("#percentile("))
        .cloned()
        .collect::<Vec<_>>();
    annotations.sort();
    annotations.dedup();
    annotations
}

pub(crate) fn class_name(class: SummaryClass) -> &'static str {
    match class {
        SummaryClass::Numeric => "numeric",
        SummaryClass::Temporal => "temporal",
        SummaryClass::Boolean => "boolean",
        SummaryClass::Categorical => "categorical",
        SummaryClass::Ordinal => "ordinal",
    }
}
