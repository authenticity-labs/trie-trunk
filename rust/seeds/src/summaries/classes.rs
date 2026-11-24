//! Trie Trunk's analytic annotations. DOC and PIL treat them as opaque text.
use crate::{Document, Error, error};
use ciborium::Value as Data;
use pil_codec::authoring_language::language as lang;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SummaryClass {
    Numeric,
    Categorical,
    Ordinal,
    Boolean,
    Temporal,
}

/// Requested analytics, independent of any UI. These do not select an
/// accumulator or change its stored shape. Queries may request others.
#[derive(Clone, Debug, PartialEq)]
pub struct SummaryAnalytics {
    ordinal: usize,
    name: String,
    percentiles: Vec<f64>,
}
impl SummaryAnalytics {
    pub fn ordinal(&self) -> usize {
        self.ordinal
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn percentiles(&self) -> &[f64] {
        &self.percentiles
    }
}

pub(crate) fn analytics(
    document: &Document,
    member: usize,
) -> Result<Vec<SummaryAnalytics>, Error> {
    let mut views = Vec::new();
    for (ordinal, field) in document.definitions()[member].fields.iter().enumerate() {
        let mut percentiles = Vec::new();
        for annotation in &field.annotations {
            let Some(rest) = annotation.trim().strip_prefix("#percentile") else {
                continue;
            };
            if !rest.is_empty() && !rest.starts_with('(') && !rest.starts_with(char::is_whitespace)
            {
                continue;
            }
            let n = rest
                .strip_prefix('(')
                .and_then(|s| s.strip_suffix(')'))
                .and_then(|s| s.trim().parse::<f64>().ok())
                .filter(|n| n.is_finite() && (0.0..=100.0).contains(n))
                .ok_or_else(|| {
                    error(format!(
                        "{}: expected #percentile(n), with n from 0 to 100",
                        field.name
                    ))
                })?;
            let n = if n == 0.0 { 0.0 } else { n };
            if !percentiles.contains(&n) {
                percentiles.push(n);
            }
        }
        if !percentiles.is_empty() {
            views.push(SummaryAnalytics {
                ordinal,
                name: field.name.clone(),
                percentiles,
            });
        }
    }
    Ok(views)
}

/// Resolved once during Seed prescription. Category labels are Definition
/// metadata; rows store a fixed counter for each declared value.
#[derive(Clone, Debug)]
pub struct SummaryField {
    pub(crate) ordinal: usize,
    pub(crate) name: String,
    pub(crate) class: SummaryClass,
    pub(crate) values: Vec<Data>,
}
impl SummaryField {
    pub fn ordinal(&self) -> usize {
        self.ordinal
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn class(&self) -> SummaryClass {
        self.class
    }
    pub fn values(&self) -> &[Data] {
        &self.values
    }
}

pub(crate) fn classify(document: &Document, member: usize) -> Result<Vec<SummaryField>, Error> {
    let mut selected = Vec::new();
    for (ordinal, field) in document.definitions()[member].fields.iter().enumerate() {
        let mut class = None;
        for annotation in &field.annotations {
            let text = annotation.trim();
            let Some(rest) = text.strip_prefix("#summ") else {
                continue;
            };
            if !rest.is_empty() && !rest.starts_with('(') && !rest.starts_with(char::is_whitespace)
            {
                continue; // Another analytic annotation, not this directive.
            }
            let name = rest
                .strip_prefix('(')
                .and_then(|s| s.strip_suffix(')'))
                .map(str::trim)
                .ok_or_else(|| error(format!("{}: expected #summ(class)", field.name)))?;
            let parsed = match name {
                "numeric" => SummaryClass::Numeric,
                "categorical" => SummaryClass::Categorical,
                "ordinal" => SummaryClass::Ordinal,
                "boolean" => SummaryClass::Boolean,
                "temporal" => SummaryClass::Temporal,
                _ => {
                    return Err(error(format!(
                        "{}: unsupported summary class {name:?}; fixed summary classes are numeric, categorical, ordinal, boolean, temporal",
                        field.name
                    )));
                }
            };
            if class.replace(parsed).is_some() {
                return Err(error(format!(
                    "{}: only one #summ(class) is allowed",
                    field.name
                )));
            }
        }
        let Some(class) = class else { continue };
        let kind = field.base_kind();
        let valid = match class {
            SummaryClass::Numeric => matches!(kind, [lang::UINT | lang::INT | lang::FLOAT, 0]),
            SummaryClass::Boolean => kind == [lang::BOOL, 0],
            SummaryClass::Temporal => matches!(kind, [lang::UINT | lang::INT, 0]),
            SummaryClass::Categorical | SummaryClass::Ordinal => {
                kind[0] == lang::ENUM
                    && field.size > 0
                    && matches!(kind[1], lang::UINT | lang::INT | lang::BOOL | lang::TSTR)
                    && !field.values.is_empty()
            }
        };
        if !valid {
            return Err(error(format!(
                "{}: #summ({}) is incompatible with its declared type; categorical/ordinal summaries require a closed enum with positive size, numeric/temporal summaries require numbers, and boolean summaries require bool",
                field.name,
                match class {
                    SummaryClass::Numeric => "numeric",
                    SummaryClass::Categorical => "categorical",
                    SummaryClass::Ordinal => "ordinal",
                    SummaryClass::Boolean => "boolean",
                    SummaryClass::Temporal => "temporal",
                }
            )));
        }
        let values = if matches!(class, SummaryClass::Categorical | SummaryClass::Ordinal) {
            if field.values.len() > u16::MAX as usize {
                return Err(error(format!(
                    "{}: summary category set exceeds fixed array capacity",
                    field.name
                )));
            }
            field
                .values
                .iter()
                .map(|v| {
                    if let Some(v) = v.as_str() {
                        Ok(Data::Text(v.to_owned()))
                    } else if let Some(v) = v.as_u64() {
                        Ok(Data::Integer(v.into()))
                    } else if let Some(v) = v.as_i64() {
                        Ok(Data::Integer(v.into()))
                    } else if let Some(v) = v.as_bool() {
                        Ok(Data::Bool(v))
                    } else {
                        Err(error(format!(
                            "{}: unsupported summary category literal",
                            field.name
                        )))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };
        selected.push(SummaryField {
            ordinal,
            name: field.name.clone(),
            class,
            values,
        });
    }
    Ok(selected)
}
