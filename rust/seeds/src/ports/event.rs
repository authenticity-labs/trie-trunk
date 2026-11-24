//! Every event has time and UUIDv7 identity. Input needs time, not pre-generated
//! entropy. These are review plans, never proof that unsampled rows are valid.
use super::Candidate;
use crate::Error;

#[derive(Debug, PartialEq, Eq)]
pub enum Time {
    UuidV7(String),
    Timestamp(String),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Identity {
    /// Preserve the validated source UUIDv7 and derive its millisecond time.
    Source(String),
    /// Generate entropy using the mapped event time, never import wall time.
    Generate,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Plan {
    pub time: Time,
    pub identity: Identity,
}

impl Candidate {
    /// Confirm a recognized sample time source. Manual extraction/format rules
    /// must be validated by their parser before taking this path. A missing ID
    /// is not an error; a missing/invalid event time is. Full import must check
    /// every row and retain generated identity across durable retries.
    pub fn event(&self, field: &str) -> Result<Plan, Error> {
        let source = self
            .time
            .iter()
            .find(|time| time.field == field)
            .ok_or_else(|| {
                Error::Definition("Map and validate an event timestamp before import".into())
            })?;
        if source.matched != self.records || source.missing_or_invalid != 0 || self.records == 0 {
            return Err(Error::Definition(
                "Resolve missing or invalid event timestamps before import".into(),
            ));
        }
        Ok(if source.kind == "uuidv7" {
            Plan {
                time: Time::UuidV7(field.into()),
                identity: Identity::Source(field.into()),
            }
        } else {
            Plan {
                time: Time::Timestamp(field.into()),
                identity: Identity::Generate,
            }
        })
    }
}
