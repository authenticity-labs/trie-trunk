//! Source preparation shared by native and WASM hosts. Ports inspect and propose;
//! registered Seed ingestion retains ownership of admission and durability.
pub mod design;
pub mod model;
pub mod editor;
pub mod event;
pub mod format;
pub mod import;
mod inspect;
use crate::Error;
pub use inspect::{Candidate, CandidateField, Choice, Limits, Range, TimeSource, uuid_time};
use std::io::BufRead;

/// Stateless inspection is also available before a Trunk is installed. The
/// normal node entry point is `trunk.ports()`; no second registry is allocated.
pub struct Ports;
#[derive(serde::Serialize)]
pub struct Review {
    pub fields: Vec<ReviewedField>,
}
#[derive(serde::Serialize)]
pub struct ReviewedField {
    pub name: String,
    pub size: u16,
    pub annotations: Vec<String>,
}
impl Ports {
    pub fn review(&self, mapping: &import::Mapping) -> Result<Review, Error> {
        let plan = mapping.prepare()?;
        Ok(Review {
            fields: plan.document.definitions()[0]
                .fields
                .iter()
                .map(|f| ReviewedField {
                    name: f.name.clone(),
                    size: f.size,
                    annotations: f.annotations.clone(),
                })
                .collect(),
        })
    }
    pub fn inspect(&self, source: impl BufRead, limits: Limits) -> Result<Candidate, Error> {
        Candidate::read(source, limits)
    }

    pub fn sample(
        &self,
        source: impl BufRead,
        limits: Limits,
        source_bytes: Option<u64>,
    ) -> Result<Candidate, Error> {
        Candidate::sample(source, limits, source_bytes)
    }
    pub fn sample_with(
        &self,
        source: impl BufRead,
        limits: Limits,
        source_bytes: Option<u64>,
        profile: Option<format::Profile>,
    ) -> Result<Candidate, Error> {
        Candidate::sample_with(source, limits, source_bytes, profile)
    }
}
