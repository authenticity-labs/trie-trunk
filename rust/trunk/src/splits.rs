//! Finite partition groups are registration configuration, independent of the
//! field's encoding and its presentation labels. No log-specific rules here.
use crate::Error;
use pil_codec::authoring_language::language as lang;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

#[derive(Debug)]
pub struct Splits {
    pub field: String,
    groups: HashMap<String, String>,
    fallback: Option<String>,
}
impl Splits {
    pub fn compile(
        binding: &BTreeMap<String, Value>,
        fields: &[lang::Field],
    ) -> Result<Option<Self>, Error> {
        let configured_groups: Vec<_> = binding
            .iter()
            .filter_map(|(key, value)| key.strip_prefix("split.").map(|name| (name, value)))
            .collect();
        let Some(configured) = binding.get("field.split") else {
            if !configured_groups.is_empty() {
                return Err("Split groups require field.split".into());
            }
            return Ok(None);
        };
        let name = configured
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or("field.split must name a Definition field")?;
        let field = fields
            .iter()
            .find(|f| f.name == name)
            .ok_or("Unknown split field")?;
        if configured_groups.is_empty() {
            return Err("Split field requires explicit groups".into());
        }
        let mut groups = HashMap::new();
        let mut fallback = None;
        for (group, values) in configured_groups {
            if group.is_empty()
                || !group
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-' || c == b'_')
            {
                return Err("Split group must be a lowercase storage label".into());
            }
            for value in literals(field, values)? {
                if value.as_str() == Some("*") {
                    if fallback.replace(group.to_owned()).is_some() {
                        return Err("Only one split wildcard is allowed".into());
                    }
                    continue;
                }
                if !accepts(field, &value) {
                    return Err("Split value does not fit the declared field".into());
                }
                if groups.insert(key(&value)?, group.to_owned()).is_some() {
                    return Err("Split groups overlap or repeat a value".into());
                }
            }
        }
        Ok(Some(Self {
            field: name.into(),
            groups,
            fallback,
        }))
    }
    /// Explicit matches take precedence over the wildcard, independent of
    /// configuration order. None means the caller must reject the event.
    pub fn select(&self, value: Option<&Value>) -> Option<&str> {
        value
            .and_then(|v| key(v).ok())
            .and_then(|k| self.groups.get(&k))
            .or(self.fallback.as_ref())
            .map(String::as_str)
    }
    pub fn group(&self, value: &Value) -> Option<&str> {
        self.select(Some(value))
    }
    pub fn contains(&self, label: &str) -> bool {
        self.fallback.as_deref() == Some(label) || self.groups.values().any(|v| v == label)
    }
}
fn literals(field: &lang::Field, input: &Value) -> Result<Vec<Value>, Error> {
    if let Some(values) = input.as_array().filter(|v| !v.is_empty()) {
        return Ok(values.clone());
    }
    let text = input
        .as_str()
        .ok_or("Split group requires literals or a comma-separated list")?;
    let [kind, element] = field.base_kind();
    let kind = if kind == lang::ENUM { element } else { kind };
    text.split(',')
        .map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return Err("Empty split value".into());
            }
            if part == "*" || kind == lang::TSTR {
                return Ok(Value::String(part.into()));
            }
            serde_json::from_str(part).map_err(|_| "Invalid split literal".into())
        })
        .collect()
}
fn key(value: &Value) -> Result<String, Error> {
    match value {
        Value::String(_) | Value::Number(_) | Value::Bool(_) => Ok(value.to_string()),
        _ => Err("Split requires a scalar literal".into()),
    }
}
fn accepts(field: &lang::Field, value: &Value) -> bool {
    let [kind, second] = field.base_kind();
    if kind == lang::ENUM {
        return field.values.contains(value);
    }
    if second != 0 {
        return false;
    }
    let width = field.size;
    match kind {
        lang::UINT => value
            .as_number()
            .and_then(|v| v.to_string().parse::<u128>().ok())
            .is_some_and(|v| width == 0 || width >= 16 || v < (1u128 << (width * 8))),
        lang::INT => value
            .as_number()
            .and_then(|v| v.to_string().parse::<i128>().ok())
            .is_some_and(|v| {
                width == 0 || width >= 16 || {
                    let limit = 1i128 << (width * 8 - 1);
                    v >= -limit && v < limit
                }
            }),
        lang::TSTR => value
            .as_str()
            .is_some_and(|v| width == 0 || v.len() <= width as usize),
        lang::BOOL => value.is_boolean(),
        _ => false,
    }
}
