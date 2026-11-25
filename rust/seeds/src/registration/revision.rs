//! The object branch pins T for the complete lifetime of a Revision<T> PIL.
use super::*;
use crate::{Field, error};
use pil_codec::api::{Binding, NativeValue, PillView, Step};

pub const PAYLOAD: &str = "object";

impl Trunk {
    pub(super) fn revision(
        &self,
        branch: BranchId,
        target: Arc<SeedDefinition>,
    ) -> Result<Arc<SeedDefinition>, Error> {
        let mut cache = self
            .revisions
            .lock()
            .map_err(|_| error("revision plans poisoned"))?;
        if let Some(plan) = cache.get(&branch) {
            return Ok(plan.clone());
        }
        // A declared revision already supplies its own envelope and index fields.
        if target
            .annotations
            .iter()
            .flatten()
            .any(|s| s == "@revision")
        {
            return Ok(target);
        }
        let key = target
            .annotations
            .iter()
            .position(|a| a.iter().any(|s| s == "@object"))
            .ok_or_else(|| error("missing object identity"))?;
        static TEMPLATE: std::sync::OnceLock<Result<crate::Document, Error>> =
            std::sync::OnceLock::new();
        let template = TEMPLATE
            .get_or_init(|| {
                doc::new("revision")
                    .field(
                        Field::new("time")
                            .r#type("uint")
                            .size(6)
                            .annotation("@time(ms)"),
                    )
                    .field(
                        Field::new("event_id")
                            .r#type("bytes")
                            .size(10)
                            .annotation("@event"),
                    )
                    .field(
                        Field::new("revision_id")
                            .r#type("uint")
                            .size(4)
                            .annotation("@revision"),
                    )
                    .field(Field::new(PAYLOAD).r#type("any"))
                    .sign()
            })
            .as_ref()
            .map_err(|e| error(e.to_string()))?;
        let mut envelope = SeedDefinition::prescribe(template)?;
        let plan = Arc::get_mut(&mut envelope).unwrap();
        plan.rx = Arc::new(plan.rx.parameters_with(
            &[Binding::record([Step::Field(3)], u64::from(branch))],
            |_| Ok(target.rx.clone()),
        )?);
        plan.change_key = Some(target.rx.definition().fields()[key].name.clone());
        plan.digest = target.digest.clone();
        plan.version = target.version;
        cache.insert(branch, envelope.clone());
        Ok(envelope)
    }
}

impl SeedDefinition {
    pub fn change_key(&self) -> Option<&str> {
        self.change_key.as_deref()
    }

    pub fn revision_key(&self, row: &PillView<'_>) -> Result<([u8; 16], u32), Error> {
        let name = self
            .change_key()
            .ok_or_else(|| error("object ingress requires a bound revision"))?;
        row.validate()?;
        let NativeValue::Scalar(pil_codec::Value::Bytes(id)) = row.native_value(1)? else {
            return Err(error("missing event UUIDv7"));
        };
        if id.len() != 10 || id[0] >> 4 != 7 || id[2] >> 6 != 2 {
            return Err(error("invalid event UUIDv7"));
        }
        let NativeValue::Scalar(pil_codec::Value::U32(revision)) = row.native_value(2)? else {
            return Err(error("missing revision ID"));
        };
        if revision == 0 {
            return Err(error("revision zero is reserved"));
        }
        let NativeValue::Composite(ciborium::Value::Map(object)) = row.native_value(3)? else {
            return Err(error("missing partial object"));
        };
        let uuid = object
            .iter()
            .find(|(key, _)| key.as_text() == Some(name))
            .and_then(|(_, v)| v.as_bytes())
            .and_then(|b| b.as_slice().try_into().ok())
            .ok_or_else(|| error("missing object UUID"))?;
        Ok((uuid, revision))
    }

    pub fn inspect_revision(&self, pill: &[u8]) -> Result<([u8; 16], u32), Error> {
        self.revision_key(&self.rx.decoder().read(pill)?)
    }
}

impl crate::SeedPackSnapshot {
    pub fn revision_key(&self, ordinal: usize) -> Result<([u8; 16], u32), Error> {
        self.definition_at(ordinal)?
            .revision_key(&self.pack.reader().get(ordinal)?)
    }
}
