//! Producer vocabulary over one prescribed log Definition.
use super::Definition;
use crate::{Error, Seed, error};
use ciborium::Value;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

pub const HTTP: &[u8] = include_bytes!("../../../../schemas/logs/log.http.rx");

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Style {
    Trunk,
    OpenTelemetry,
}

#[derive(Clone, serde::Serialize)]
pub struct LogOption {
    pub name: String,
    pub field: String,
    pub kind: String,
    pub unit: Option<String>,
}

pub struct Logger {
    definition: Arc<Definition>,
    options: Vec<LogOption>,
    names: HashMap<String, usize>,
    reserved: HashSet<String>,
    custom: Option<usize>,
    time: usize,
    id: usize,
    ns_per_tick: u64,
    // Only OTel Timestamp and ObservedTimestamp take Unix nanoseconds.
    clocks: HashMap<usize, u64>,
}

fn argument<'a>(annotations: &'a [String], prefix: &str) -> Option<&'a str> {
    annotations
        .iter()
        .find_map(|a| a.strip_prefix(prefix)?.strip_suffix(')'))
}

fn nanos(unit: &str) -> Result<u64, Error> {
    match unit {
        "ms" => Ok(1_000_000),
        "us" => Ok(1_000),
        "ns" => Ok(1),
        _ => Err(error(format!("Unsupported log timestamp unit {unit}"))),
    }
}

impl Definition {
    pub fn logger(self: &Arc<Self>, style: Style) -> Result<Logger, Error> {
        let identity = self
            .identity
            .filter(|id| self.annotations[id.id].iter().any(|a| a == "@log"))
            .ok_or_else(|| error("Select a log Definition with @log and @time"))?;
        let mut logger = Logger {
            definition: self.clone(),
            options: Vec::new(),
            names: HashMap::new(),
            reserved: HashSet::new(),
            custom: None,
            time: identity.time,
            id: identity.id,
            ns_per_tick: 1_000_000 / identity.units_per_ms,
            clocks: HashMap::new(),
        };
        let document = crate::doc::read(self.prescription().bytes())?;
        let member =
            document.member(&self.namespace, &self.name, self.version.0, self.version.1)?;
        for (i, f) in document.definitions()[member].fields.iter().enumerate() {
            let annotations = &self.annotations[i];
            let provider = argument(annotations, "@otel(");
            logger.reserved.insert(f.name.clone());
            if let Some(alias) = provider {
                logger.reserved.insert(alias.to_owned());
            }
            if i == identity.id || annotations.iter().any(|a| a == "@leaf") {
                continue;
            }
            if annotations.iter().any(|a| a == "@fields") {
                use pil_codec::authoring_language::language as lang;
                if f.base_kind() != [lang::ARRAY, lang::ANY] || f.size != 0 {
                    return Err(error(format!("{}: @fields requires array(any)", f.name)));
                }
                if logger.custom.replace(i).is_some() {
                    return Err(error("Ambiguous @fields log Definition"));
                }
                continue;
            }
            let alias = provider.filter(|_| style == Style::OpenTelemetry);
            let name = alias.unwrap_or(&f.name);
            let mut units = argument(annotations, "@unit(");
            if i == identity.time {
                units = Some(argument(annotations, "@time(").unwrap_or("ms"));
            }
            if matches!(alias, Some("Timestamp" | "ObservedTimestamp")) {
                let stored =
                    units.ok_or_else(|| error(format!("Missing timestamp unit on {}", f.name)))?;
                logger.clocks.insert(i, nanos(stored)?);
                units = Some("ns");
            }
            if logger.names.insert(name.to_owned(), i).is_some() {
                return Err(error(format!("Ambiguous log option {name}")));
            }
            logger.options.push(LogOption {
                name: name.into(),
                field: f.name.clone(),
                kind: trie_trunk_taxonomy::registration::attribute_label(f),
                unit: units.map(str::to_owned),
            });
        }
        Ok(logger)
    }
}

impl Logger {
    pub fn options(&self) -> &[LogOption] {
        &self.options
    }
    pub fn custom_fields(&self) -> bool {
        self.custom.is_some()
    }

    /// Host supplies capture time and entropy once so retries preserve event identity.
    pub fn event<'a>(&'a self, unix_ns: u64, entropy: &'a [u8; 10]) -> Result<Event<'a>, Error> {
        let mut seed = self.definition.seed();
        seed.set_uint(self.time, unix_ns / self.ns_per_tick)?
            .set_byte_string(self.id, entropy)?;
        Ok(Event {
            logger: self,
            seed,
            custom: BTreeMap::new(),
        })
    }
}

pub struct Event<'a> {
    logger: &'a Logger,
    seed: Seed<'a>,
    custom: BTreeMap<String, Value>,
}

impl Event<'_> {
    pub fn set(&mut self, name: &str, value: impl Into<Value>) -> Result<&mut Self, Error> {
        let i = *self.logger.names.get(name).ok_or_else(|| {
            error(format!(
                "Unknown log option {name}. Use field() for a custom attribute."
            ))
        })?;
        let mut value = value.into();
        if i == self.logger.time && value.is_null() {
            return Err(error("Log timestamp is required"));
        }
        if let Some(divisor) = self.logger.clocks.get(&i).filter(|_| !value.is_null()) {
            let ns = value
                .as_integer()
                .and_then(|v| u64::try_from(v).ok())
                .ok_or_else(|| error(format!("{name} requires unsigned Unix nanoseconds")))?;
            if ns % divisor != 0 {
                return Err(error(format!(
                    "{name} exceeds the precision of the selected Definition"
                )));
            }
            value = (ns / divisor).into();
        }
        self.seed
            .set_native(i, Some(value))
            .map_err(|e| error(format!("{name}: {e}")))?;
        Ok(self)
    }

    pub fn field(&mut self, name: &str, value: impl Into<Value>) -> Result<&mut Self, Error> {
        if self.logger.custom.is_none() {
            return Err(error("This log Definition has no @fields collection"));
        }
        if name.is_empty() || self.logger.reserved.contains(name) {
            return Err(error(format!(
                "Custom field requires a distinct name: {name}"
            )));
        }
        self.custom.insert(name.into(), value.into());
        Ok(self)
    }

    pub fn encode(mut self) -> Result<Vec<u8>, Error> {
        if let Some(i) = self.logger.custom.filter(|_| !self.custom.is_empty()) {
            let entries = self
                .custom
                .into_iter()
                .map(|(k, v)| (k.into(), v))
                .collect();
            self.seed.set_array(i, vec![Value::Map(entries)])?;
        }
        self.seed.cap()?;
        Ok(self.seed.bytes()?.to_vec())
    }
}
