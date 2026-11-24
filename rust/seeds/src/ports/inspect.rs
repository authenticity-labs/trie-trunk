//! Seed policy extends the generic DOC sample without duplicating extraction.
use crate::{Error, doc};
pub use doc::source::{CandidateField, Choice, Limits, Range, Record, TimeSource, uuid_time};
use serde::Serialize;
use std::io::BufRead;
#[derive(Serialize, Debug)]
pub struct Candidate {
    #[serde(flatten)]
    source: doc::source::Candidate,
    pub annotations: Vec<super::editor::Annotation>,
}
impl std::ops::Deref for Candidate {
    type Target = doc::source::Candidate;
    fn deref(&self) -> &Self::Target {
        &self.source
    }
}
impl Candidate {
    fn extend(mut source: doc::source::Candidate) -> Self {
        // This import mapping stores JSON objects as dynamic PIL values until it
        // can carry referenced RX members and map key/value descriptors.
        for field in &mut source.fields {
            if matches!(field.kind.as_str(), "object" | "map") {
                field.kind = "any".into();
            }
        }
        if source.time.is_empty() {
            source.warnings.push(
                "Map an event timestamp before import; import time is not a substitute.".into(),
            );
        }
        Self {
            source,
            annotations: super::editor::catalog(),
        }
    }
    pub fn read(source: impl BufRead, limits: Limits) -> Result<Self, Error> {
        Ok(Self::extend(doc::source::Candidate::read(source, limits)?))
    }
    pub fn sample(source: impl BufRead, limits: Limits, size: Option<u64>) -> Result<Self, Error> {
        Self::sample_with(source, limits, size, None)
    }
    pub fn sample_with(
        source: impl BufRead,
        limits: Limits,
        size: Option<u64>,
        profile: Option<super::format::Profile>,
    ) -> Result<Self, Error> {
        Ok(Self::extend(doc::source::Candidate::sample_with(
            source, limits, size, profile,
        )?))
    }
}
