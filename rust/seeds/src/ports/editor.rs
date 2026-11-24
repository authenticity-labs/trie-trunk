//! Authoring assistance shared by CLI, native and browser Ports. Annotations
//! retain their existing Seed semantics; the editor does not invent codec types.
use super::import::Column;
use crate::{Error, error};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Annotation {
    pub value: String,
    pub description: String,
    pub kinds: Vec<String>,
    pub enumeration: bool,
}
pub fn catalog() -> Vec<Annotation> {
    let mut out = Vec::new();
    let mut add = |value: &str, description: &str, kinds: &[&str], enumeration| {
        out.push(Annotation {
            value: value.into(),
            description: description.into(),
            kinds: kinds.iter().map(|s| (*s).into()).collect(),
            enumeration,
        })
    };
    for unit in ["ns", "us", "ms", "s", "m", "kg", "K"] {
        add(
            &format!("@unit({unit})"),
            "Measurement unit used by query comparisons and conversion.",
            &["uint", "int", "float"],
            false,
        );
    }
    add(
        "#summ(numeric)",
        "Numeric count, extrema and additive statistics.",
        &["uint", "int", "float"],
        false,
    );
    add(
        "#summ(temporal)",
        "Temporal statistics for an integer measurement.",
        &["uint", "int"],
        false,
    );
    add("#summ(boolean)", "Boolean counts.", &["bool"], false);
    add(
        "#summ(categorical)",
        "Counts for each explicitly declared enum value.",
        &["uint", "int", "bool", "tstr"],
        true,
    );
    add(
        "#summ(ordinal)",
        "Ordered categories in the declared enum order.",
        &["uint", "int", "bool", "tstr"],
        true,
    );
    add(
        "#percentile(95)",
        "Requested percentile over summary-period aggregates; does not retain event distributions.",
        &["uint", "int", "float"],
        false,
    );
    add(
        "@min(0)",
        "Inclusive minimum, checked during file import.",
        &["uint", "int", "float"],
        false,
    );
    add(
        "@max(100)",
        "Inclusive maximum, checked during file import.",
        &["uint", "int", "float"],
        false,
    );
    add(
        "@range(0,100)",
        "Inclusive minimum and maximum, checked during file import.",
        &["uint", "int", "float"],
        false,
    );
    // Derive provider completions from the same metadata the query planner uses.
    let profiles: serde_json::Value =
        serde_json::from_str(include_str!("../metadata.json")).expect("checked-in metadata");
    for p in profiles.as_array().unwrap() {
        let field = p["field"].as_str().unwrap();
        add(
            &format!("@otel({field})"),
            p["name"].as_str().unwrap(),
            if p["kind"] == "integer" {
                &["uint", "int"]
            } else {
                &["tstr"]
            },
            false,
        );
    }
    out
}

pub fn annotations(field: &Column) -> Result<(), Error> {
    let numeric = matches!(field.kind.as_str(), "uint" | "int" | "float");
    let mut units = 0;
    let mut profiles = 0;
    for a in &field.annotations {
        if a.trim() != a || a.is_empty() {
            return Err(error("Empty or untrimmed annotation"));
        }
        if let Some(unit) = a.strip_prefix("@unit(").and_then(|s| s.strip_suffix(')')) {
            units += 1;
            if !numeric || unit.trim().is_empty() || units > 1 {
                return Err(error("One nonempty @unit is allowed on a numeric field"));
            }
        } else if a.starts_with("@otel(") {
            profiles += 1;
            if profiles > 1
                || !field.values.is_empty()
                || !catalog()
                    .iter()
                    .any(|c| c.value == *a && c.kinds.contains(&field.kind))
            {
                return Err(error(
                    "Unknown, conflicting or incompatible OpenTelemetry annotation",
                ));
            }
        } else if ["@min", "@max", "@range"]
            .iter()
            .any(|name| argument(a, name).is_some())
        {
            Bounds::compile(field)?;
        } else if a.starts_with("#summ(") {
            // Seed prescription validates class, type and finite enum membership.
        } else if a.starts_with("#percentile(") {
            if !numeric
                || !field.values.is_empty()
                || !field
                    .annotations
                    .iter()
                    .any(|s| matches!(s.as_str(), "#summ(numeric)" | "#summ(temporal)"))
                    && !field.summarize
            {
                return Err(error("#percentile requires a numeric or temporal summary"));
            }
        } else {
            return Err(error(format!(
                "Unsupported import annotation {a}; event identity/time are supplied by the timestamp mapping"
            )));
        }
    }
    Ok(())
}

pub fn values(field: &Column) -> Result<Vec<serde_json::Value>, Error> {
    if field.values.len() > 65535 {
        return Err(error("Too many enum values"));
    }
    let mut out = Vec::new();
    for text in &field.values {
        let value: serde_json::Value =
            serde_json::from_str(text).map_err(|_| error("Enum entries must be JSON scalars"))?;
        let valid = match field.kind.as_str() {
            "uint" => value.as_u64().is_some(),
            "int" => value.as_i64().is_some(),
            "bool" => value.is_boolean(),
            "tstr" => value.is_string(),
            _ => false,
        };
        if !valid {
            return Err(error(
                "Enum literal must match its type (integer enum literals currently fit 64 bits)",
            ));
        }
        if out.contains(&value) {
            return Err(error("Duplicate enum value"));
        }
        out.push(value);
    }
    Ok(out)
}

/// Import constraints are compiled during review, never reparsed per row.
/// They are application annotations; PIL sizing remains explicit uint(N)/int(N).
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
enum Number {
    Unsigned(u128),
    Signed(i128),
    Float(f64),
}
#[derive(Clone, Debug)]
pub struct Bounds {
    kind: String,
    min: Option<Number>,
    max: Option<Number>,
}
impl Bounds {
    fn number(kind: &str, text: &str) -> Result<Number, Error> {
        let invalid =
            || error("Bounds must be exact finite numeric literals matching the field type");
        Ok(match kind {
            "uint" => Number::Unsigned(text.trim().parse().map_err(|_| invalid())?),
            "int" => Number::Signed(text.trim().parse().map_err(|_| invalid())?),
            "float" => {
                let n: f64 = text.trim().parse().map_err(|_| invalid())?;
                if !n.is_finite() {
                    return Err(invalid());
                }
                Number::Float(n)
            }
            _ => return Err(error("Min, Max and Range apply to numeric fields")),
        })
    }
    pub fn compile(field: &Column) -> Result<Self, Error> {
        let mut bounds = Self {
            kind: field.kind.clone(),
            min: None,
            max: None,
        };
        if field.exclude {
            return Ok(bounds);
        }
        for a in &field.annotations {
            let (min, max) = if let Some(s) = argument(a, "@range") {
                let (a, b) = s
                    .split_once(',')
                    .ok_or_else(|| error("Expected @range(min,max)"))?;
                (Some(a), Some(b))
            } else if let Some(s) = argument(a, "@min") {
                (Some(s), None)
            } else if let Some(s) = argument(a, "@max") {
                (None, Some(s))
            } else {
                continue;
            };
            if let Some(n) = min {
                if bounds.min.replace(Self::number(&field.kind, n)?).is_some() {
                    return Err(error("Duplicate minimum bound"));
                }
            }
            if let Some(n) = max {
                if bounds.max.replace(Self::number(&field.kind, n)?).is_some() {
                    return Err(error("Duplicate maximum bound"));
                }
            }
        }
        if matches!((bounds.min,bounds.max),(Some(a),Some(b)) if a>b) {
            return Err(error("Minimum exceeds maximum"));
        }
        Ok(bounds)
    }
    pub fn check(&self, raw: &str) -> Result<(), Error> {
        if self.min.is_none() && self.max.is_none() {
            return Ok(());
        }
        let n = Self::number(&self.kind, raw)?;
        if self.min.is_some_and(|v| n < v) || self.max.is_some_and(|v| n > v) {
            return Err(error("Value is outside the reviewed field bounds"));
        }
        Ok(())
    }
}
fn argument<'a>(text: &'a str, name: &str) -> Option<&'a str> {
    text.strip_prefix(name)?
        .strip_prefix('(')?
        .strip_suffix(')')
}
