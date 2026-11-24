//! Query observations are ordinary events, authored and encoded by Rust.
//! Input durations are milliseconds; stored durations are microseconds.
//! Durations describe explicit milestones; absent timings are not zeroes.
use crate::{Document, Error, Field, SeedDefinition, doc, error};
use std::sync::{Arc, OnceLock};

pub fn definition() -> Result<Document, Error> {
    let checkpoint = doc::new("checkpoint")
        .field(Field::new("name").r#type("tstr"))
        .field(Field::new("start").r#type("float").size(8))
        .field(Field::new("duration").r#type("float").size(8))
        .sign()?;
    let mut d = doc::new("query")
        .compound(checkpoint.bytes()?)
        .namespace("log")
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
        )
        .field(Field::new("id").r#type("bytes").size(10).annotation("@log"))
        .field(Field::new("level").r#type("uint").size(1).annotation("@otel(SeverityNumber)"));
    for name in ["operation", "source", "query", "status", "error", "user_agent"] {
        d = d.field(Field::new(name).r#type("tstr"));
    }
    for name in ["from", "to"] {
        d = d.field(Field::new(name).r#type("uint").size(6));
    }
    d = d.field(Field::new("rows").r#type("uint").size(4).annotation("#summ(numeric)"));
    d = d.field(
            Field::new("duration")
                .r#type("float")
                .size(8)
                .annotation("@unit(us)").annotation("#summ(numeric)"),
        ).field(Field::new("checkpoints").array_object("checkpoint").annotation("@unit(us)").annotation("@checkpoints"));
    d.sign()
}
#[derive(serde::Deserialize)]
pub struct QueryEvent {
    pub time: u64,
    pub id: [u8; 10],
    pub source: String,
    pub query: String,
    pub status: String,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub user_agent: Option<String>,
    pub from: u64,
    pub to: u64,
    pub rows: u32,
    pub duration: f64,
    #[serde(default)]
    pub worker: Option<f64>,
    #[serde(default)]
    pub worker_start: Option<f64>,
    #[serde(default)]
    pub before_collection: Option<f64>,
    #[serde(default)]
    pub collection: Option<f64>,
    #[serde(default)]
    pub after_collection: Option<f64>,
    #[serde(default)]
    pub presentation: Option<f64>,
}
impl QueryEvent {
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        if self.time >= (1 << 48)
            || self.from >= self.to
            || self.to >= (1 << 48)
            || self.source.len() > 4096
            || self.query.len() > 65536
            || self.error.as_ref().is_some_and(|s| s.len() > 16384)
            || self.user_agent.as_ref().is_some_and(|s| s.len() > 4096)
            || !["complete", "error", "cancelled"].contains(&self.status.as_str())
        {
            return Err(error("invalid query observation"));
        }
        static CODEC: OnceLock<Result<Arc<SeedDefinition>, String>> = OnceLock::new();
        let codec = CODEC
            .get_or_init(|| {
                definition()
                    .and_then(|d| SeedDefinition::prescribe(&d))
                    .map_err(|e| e.to_string())
            })
            .as_ref()
            .map_err(|e| error(e.as_str()))?;
        let seed = codec.ingest(self, |event, seed| {
            seed.set_uint("time", event.time)?
                .set_byte_string("id", &event.id)?
                .set_uint("level", match event.status.as_str() { "error" => 17u8, "cancelled" => 13, _ => 9 })?
                .set_text_string("operation", "execute")?
                .set_text_string("source", &event.source)?
                .set_text_string("query", &event.query)?
                .set_text_string("status", &event.status)?
                .set_uint("from", event.from)?
                .set_uint("to", event.to)?
                .set_uint("rows", event.rows)?;
            if let Some(text) = &event.error {
                seed.set_text_string("error", text)?;
            }
            if let Some(text) = &event.user_agent {
                seed.set_text_string("user_agent", text)?;
            }
            seed.set_float("duration", micros(event.duration)?)?;
            let mut checkpoints = Vec::with_capacity(5);
            let start = event.worker_start;
            let collection_start = start.zip(event.before_collection).map(|(s,n)| s+n);
            let after_start = collection_start.zip(event.collection).map(|(s,n)| s+n);
            for (name, at, value) in [
                ("worker", start, event.worker),
                ("before_collection", start, event.before_collection),
                ("collection", collection_start, event.collection),
                ("after_collection", after_start, event.after_collection),
                ("presentation", event.presentation.map(|n| event.duration-n), event.presentation),
            ] {
                if let Some(value) = value {
                    let mut fields = vec![("name".into(), name.into()),
                        ("duration".into(), ciborium::Value::Float(micros(value)?))];
                    if let Some(at) = at { fields.push(("start".into(), ciborium::Value::Float(micros(at)?))); }
                    checkpoints.push(ciborium::Value::Map(fields));
                }
            }
            if !checkpoints.is_empty() { seed.set_array("checkpoints", checkpoints)?; }
            Ok(())
        })?;
        seed.cap()?;
        Ok(seed.bytes()?.to_vec())
    }
}

fn micros(value: f64) -> Result<f64, Error> {
    if !value.is_finite() || value < 0.0 { return Err(error("invalid query duration")); }
    let value = value * 1000.0;
    if !value.is_finite() { return Err(error("query duration overflow")); }
    Ok(value)
}
