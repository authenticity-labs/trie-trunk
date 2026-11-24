//! Reviewed source mappings. No source value is cast through a host JS number.
use super::{
    format::{Cell, Event, Profile},
    inspect::Record,
};
use crate::{Document, Error, Field, SeedDefinition, doc, error};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mapping {
    #[serde(default)]
    pub log:bool,
    #[serde(default)]
    pub format: Profile,
    pub name: String,
    pub time_field: String,
    pub time_format: String,
    pub fields: Vec<Column>,
}
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    pub kind: String,
    pub size: u16,
    #[serde(default)]
    pub summarize: bool,
    /// Source key stays stable when the authored field is renamed.
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub exclude: bool,
    #[serde(default)]
    pub annotations: Vec<String>,
    /// Exact JSON scalar spellings; never routed through JS numeric values.
    #[serde(default)]
    pub values: Vec<String>,
}
impl Column {
    pub fn source(&self) -> &str {
        self.source.as_deref().unwrap_or(&self.name)
    }
}
pub struct Plan {
    pub document: Document,
    definition: Arc<SeedDefinition>,
    mapping: Mapping,
    time: String,
    id: String,
    ordinals: Vec<usize>,
    bounds: Vec<super::editor::Bounds>,
}
impl Mapping {
    pub fn prepare(&self) -> Result<Plan, Error> {
        self.format.reader(std::io::empty(), 1 << 20)?;
        if self.fields.len() > 4096
            || self.fields.is_empty()
            || !self
                .fields
                .iter()
                .any(|f| f.source() == self.time_field && !f.exclude)
        {
            return Err(error("Map a source timestamp before import"));
        }
        if ![
            "uuidv7", "rfc3339", "unix_s", "unix_ms", "unix_us", "unix_ns",
        ]
        .contains(&self.time_format.as_str())
        {
            return Err(error("Unsupported timestamp mapping"));
        }
        let unique = |base: &str| {
            let mut name = base.to_string();
            while self.fields.iter().any(|f| !f.exclude && f.name == name) {
                name.push('_');
            }
            name
        };
        let time = if self.time_format == "uuidv7" {
            unique("time")
        } else {
            self.fields
                .iter()
                .find(|f| f.source() == self.time_field)
                .unwrap()
                .name
                .clone()
        };
        let id = if self.time_format == "uuidv7" {
            self.fields
                .iter()
                .find(|f| f.source() == self.time_field)
                .unwrap()
                .name
                .clone()
        } else {
            unique("id")
        };
        let mut document = doc::new(&self.name)
            .field(Field::new(&time).r#type("uint").size(6).annotation("@time"))
            .field(Field::new(&id).r#type("bytes").size(10).annotation(if self.log {"@log"} else {"@event"}));
        let mut names = std::collections::HashSet::new();
        let mut sources = std::collections::HashSet::new();
        for field in &self.fields {
            if !sources.insert(field.source()) {
                return Err(error("Duplicate source mapping"));
            }
            if field.exclude {
                continue;
            }
            if field.name.trim().is_empty() {
                return Err(error("Field name cannot be empty"));
            }
            if !names.insert(&field.name) {
                return Err(error("Duplicate mapped field"));
            }
            if field.source() == self.time_field {
                if !field.annotations.is_empty() || !field.values.is_empty() {
                    return Err(error(
                        "Event time and identity annotations are managed by the timestamp mapping",
                    ));
                }
                continue;
            }
            if !["uint", "int", "float", "tstr", "bool", "any"].contains(&field.kind.as_str()) {
                return Err(error("Unsupported imported field type"));
            }
            let mut column = Field::new(&field.name)
                .r#type(&field.kind)
                .size(field.size.into());
            super::editor::annotations(field)?;
            if !field.values.is_empty() {
                column = column.closed_enum(&field.kind, super::editor::values(field)?);
            }
            column = column.annotations(field.annotations.clone());
            if field.summarize && !field.annotations.iter().any(|a| a.starts_with("#summ(")) {
                column = column.annotation(match field.kind.as_str() {
                    "uint" | "int" | "float" => "#summ(numeric)",
                    "bool" => "#summ(boolean)",
                    _ => {
                        return Err(error(
                            "This field needs an explicit bounded category before summarizing",
                        ));
                    }
                });
            }
            document = document.field(column);
        }
        let document = document.sign()?;
        let definition = SeedDefinition::prescribe(&document)?;
        let ordinals = self
            .fields
            .iter()
            .map(|f| {
                definition
                    .prescription()
                    .definition()
                    .fields()
                    .iter()
                    .position(|v| v.name == f.name)
                    .unwrap_or(0)
            })
            .collect();
        Ok(Plan {
            document,
            definition,
            mapping: self.clone(),
            time,
            id,
            ordinals,
            bounds: self
                .fields
                .iter()
                .map(super::editor::Bounds::compile)
                .collect::<Result<_, _>>()?,
        })
    }
}
impl Plan {
    pub fn encode(&self, line: &[u8], entropy: [u8; 10]) -> Result<(u64, Vec<u8>), Error> {
        self.encode_event(&Event::json(line)?, entropy)
    }
    pub fn encode_event(&self, row: &Event, entropy: [u8; 10]) -> Result<(u64, Vec<u8>), Error> {
        for key in row.fields.keys() {
            if !self.mapping.fields.iter().any(|f| f.source() == *key) {
                return Err(error(format!(
                    "Unmapped field {key}; review the Definition"
                )));
            }
        }
        let raw = row
            .fields
            .get(&self.mapping.time_field)
            .ok_or_else(|| error("Missing mapped event timestamp"))?;
        let mut id = entropy;
        let time = if self.mapping.time_format == "uuidv7" {
            let text = raw.text()?;
            let uuid = crate::seed::identity::parse(&text)?;
            let time = crate::seed::identity::millis(&uuid).ok_or_else(|| error("Invalid source UUIDv7"))?;
            id.copy_from_slice(&uuid[6..]);
            time
        } else {
            let text = raw.text()?;
            let nanos = if self.mapping.time_format == "rfc3339" {
                crate::cast::timestamp(
                    &text,
                    &crate::cast::Context {
                        reference_ms: None,
                        ..Default::default()
                    },
                )?
            } else {
                let multiplier = match self.mapping.time_format.as_str() {
                    "unix_s" => 1_000_000_000,
                    "unix_ms" => 1_000_000,
                    "unix_us" => 1000,
                    _ => 1,
                };
                text.parse::<i128>()
                    .map_err(|_| error("Expected an integer Unix timestamp"))?
                    .checked_mul(multiplier)
                    .ok_or(Error::Overflow)?
            };
            if nanos < 0 || nanos % 1_000_000 != 0 {
                return Err(error(
                    "Import timestamp must be an exact nonnegative millisecond; finer precision needs a reviewed time Definition",
                ));
            }
            u64::try_from(nanos / 1_000_000).map_err(|_| Error::Overflow)?
        };
        if time >= 1 << 48 {
            return Err(error("Timestamp exceeds UUIDv7 range"));
        }
        let seed = self.definition.ingest((), |_, s| {
            s.set_uint(self.time.as_str(), time)?
                .set_byte_string(self.id.as_str(), &id)?;
            for (index, field) in self.mapping.fields.iter().enumerate() {
                if field.exclude || field.source() == self.mapping.time_field {
                    continue;
                }
                let Some(value) = row.fields.get(field.source()) else {
                    continue;
                };
                let raw = match value {
                    Cell::Json(v) => v.get(),
                    Cell::Text(s) => s.as_str(),
                    _ => "",
                };
                if value.is_null() {
                    s.set_null(field.name.as_str())?;
                    continue;
                }
                self.bounds[index]
                    .check(raw)
                    .map_err(|e| error(format!("{}: {e}", field.name)))?;
                if !field.values.is_empty() {
                    s.set_native(self.ordinals[index], Some(cell_data(value, &field.kind)?))?;
                    continue;
                }
                match field.kind.as_str() {
                    "uint" => {
                        s.set_uint(
                            field.name.as_str(),
                            raw.parse::<u128>()
                                .map_err(|_| error("Expected unsigned integer"))?,
                        )?;
                    }
                    "int" => {
                        s.set_int(
                            field.name.as_str(),
                            raw.parse::<i128>()
                                .map_err(|_| error("Expected signed integer"))?,
                        )?;
                    }
                    "float" => {
                        let value = raw
                            .parse::<f64>()
                            .map_err(|_| error("Expected floating-point number"))?;
                        if !value.is_finite()
                            || (field.size == 4 && f64::from(value as f32) != value)
                        {
                            return Err(error(
                                "Value cannot be represented exactly by the reviewed float width",
                            ));
                        }
                        s.set_float(field.name.as_str(), value)?;
                    }
                    "tstr" => {
                        s.set_native(self.ordinals[index], Some(cell_data(value, &field.kind)?))?;
                    }
                    "bool" => {
                        s.set_bool(
                            field.name.as_str(),
                            serde_json::from_str(raw).map_err(|_| error("Expected Boolean"))?,
                        )?;
                    }
                    _ => {
                        s.set_any(field.name.as_str(), cell_data(value, "any")?)?;
                    }
                }
            }
            Ok(())
        })?;
        seed.cap()?;
        Ok((time, seed.bytes()?.to_vec()))
    }
}
fn cell_data(value: &Cell, kind: &str) -> Result<ciborium::Value, Error> {
    use ciborium::Value as D;
    Ok(match value {
        Cell::Json(raw) => data(raw)?,
        Cell::Null => D::Null,
        Cell::List(values) => D::Array(
            values
                .iter()
                .map(|v| cell_data(v, "any"))
                .collect::<Result<_, _>>()?,
        ),
        Cell::Text(text) if kind == "tstr" || kind == "any" => D::Text(text.clone()),
        Cell::Text(text) => data(
            serde_json::from_str::<&RawValue>(text)
                .map_err(|_| error("Source value does not match the reviewed enum type"))?,
        )?,
    })
}
fn data(raw: &RawValue) -> Result<ciborium::Value, Error> {
    use ciborium::Value as D;
    let text = raw.get();
    Ok(match text.as_bytes().first() {
        Some(b'{') => D::Map(
            serde_json::from_str::<Record>(text)
                .map_err(|e| error(e.to_string()))?
                .0
                .into_iter()
                .map(|(k, v)| Ok((D::Text(k), data(&v)?)))
                .collect::<Result<_, Error>>()?,
        ),
        Some(b'[') => D::Array(
            serde_json::from_str::<Vec<Box<RawValue>>>(text)
                .map_err(|e| error(e.to_string()))?
                .iter()
                .map(|v| data(v))
                .collect::<Result<_, _>>()?,
        ),
        Some(b'"') => D::Text(serde_json::from_str(text).map_err(|e| error(e.to_string()))?),
        _ if text == "null" => D::Null,
        _ if text == "true" || text == "false" => D::Bool(text == "true"),
        _ if !text.contains(['.', 'e', 'E']) => D::Integer(
            text.parse::<i128>()
                .map_err(|_| error("Nested integer exceeds supported exact range"))?
                .try_into()
                .map_err(|_| Error::Overflow)?,
        ),
        _ => {
            let n = text.parse::<f64>().map_err(|_| error("Invalid number"))?;
            if !n.is_finite() {
                return Err(error("Non-finite number"));
            }
            D::Float(n)
        }
    })
}
