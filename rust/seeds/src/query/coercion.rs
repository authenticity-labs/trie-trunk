//! Only query literals are coerced. Stored values and Definition layouts stay typed.
use crate::{Compare, Error, Predicate, SeedDefinition, cast, error};
use ciborium::Value as Data;
use num_bigint::BigInt;
use num_rational::BigRational;
use pil_codec::{Scalar, Type};
use std::borrow::Cow;

impl SeedDefinition {
    pub(crate) fn cast_plan(&self,field:&str,spec:cast::Spec)->Result<cast::Plan,Error> {
        cast::Plan::new(spec,self.query_ordinal(field).and_then(|i|self.annotation(i,"@unit")))
    }

    /// Lower annotation views, then resolve text against the physical field plan.
    pub fn prepare<'a>(
        &self,
        predicate: &'a Predicate,
        context: &cast::Context,
    ) -> Result<Cow<'a, Predicate>, Error> {
        let lowered = self.predicate(predicate);
        match self.coerce(&lowered, context, false)? {
            Some(p) => Ok(Cow::Owned(p)),
            None => Ok(lowered),
        }
    }
    #[cfg(any(feature = "registration", feature = "query-language"))]
    pub(crate) fn prepare_summary(
        &self,
        p: &Predicate,
        context: &cast::Context,
    ) -> Result<Predicate, Error> {
        let lowered = self.predicate(p);
        Ok(self
            .coerce(&lowered, context, true)?
            .unwrap_or_else(|| lowered.into_owned()))
    }
    pub(crate) fn clocked(&self, p: &Predicate) -> bool {
        match p {
            Predicate::Compare {
                field,
                value: Data::Text(_),
                ..
            } => self
                .query_ordinal(field)
                .is_some_and(|i| self.time_unit(i).is_some()),
            Predicate::And(p) | Predicate::Or(p) => p.iter().any(|p| self.clocked(p)),
            Predicate::Not(p) => self.clocked(p),
            _ => false,
        }
    }
    pub(crate) fn annotation<'a>(&'a self, i: usize, name: &str) -> Option<&'a str> {
        self.annotations
            .get(i)?
            .iter()
            .find_map(|a| a.strip_prefix(name)?.strip_prefix('(')?.strip_suffix(')'))
    }
    pub fn timestamp_unit(&self, field: &str) -> Option<&str> {
        self.time_unit(self.query_ordinal(field)?)
    }
    pub(crate) fn time_unit(&self, i: usize) -> Option<&str> {
        if self
            .annotations
            .get(i)
            .is_some_and(|a| a.iter().any(|a| a == "@time"))
        {
            return Some("ms");
        }
        if let Some(unit) = self.annotation(i, "@time").or_else(|| self.annotation(i, "@timestamp")) {
            return Some(unit);
        }
        match self.rx.definition().fields()[i].kind {
            Type::Scalar(Scalar::TimestampMs) => Some("ms"),
            Type::Scalar(Scalar::TimestampUs) => Some("us"),
            Type::Scalar(Scalar::TimestampNs) => Some("ns"),
            _ => None,
        }
    }
    fn coerce(
        &self,
        p: &Predicate,
        context: &cast::Context,
        summary: bool,
    ) -> Result<Option<Predicate>, Error> {
        use Predicate::*;
        match p {
            Cast{field,cast,op,value:Data::Text(text)}=> {
                let plan=self.cast_plan(field,cast::Spec{tolerant:false,..*cast})?;
                let crate::Cell::Value(value)=crate::value::own(plan.value(pil_codec::api::NativeValue::Scalar(pil_codec::Value::String(text)))?)? else {unreachable!()};
                Ok(Some(Cast{field:field.clone(),cast:*cast,op:*op,value}))
            },
            Compare {
                field,
                op,
                value: Data::Text(text),
            } => {
                let (i, aggregate) = if let Some(i) = self.query_ordinal(field) {
                    (i, None)
                } else if summary {
                    let Some((base, part)) = field.rsplit_once('.') else {
                        return Ok(None);
                    };
                    let Some(i) = self.query_ordinal(base) else {
                        return Ok(None);
                    };
                    (i, Some(part))
                } else {
                    return Ok(None);
                };
                if !summary && self.annotations(i).is_some_and(|a|
                    a.iter().any(|s| matches!(s.as_str(), "@event" | "@log" | "@object"))) {
                    let bytes = crate::seed::identity::parse(text)?;
                    return Ok(Some(Predicate::compare(field, *op, Data::Bytes(bytes.to_vec()))));
                }
                let kind = match aggregate {
                    Some(
                        "count" | "nulls" | "absent" | "trues" | "falses" | "null_count"
                        | "absent_count" | "true_count" | "false_count",
                    ) => Some(Scalar::U64),
                    Some("mean" | "average") => Some(Scalar::F64),
                    _ => self.enum_kinds.get(i).copied().flatten().or_else(|| {
                        match self.rx.definition().fields()[i].kind {
                            Type::Scalar(s) => Some(s),
                            _ => None,
                        }
                    }),
                };
                let Some(kind) = kind else {
                    return Ok(None);
                };
                let result = (|| {
                    if kind == Scalar::Bool {
                        let value = match text.trim().to_ascii_lowercase().as_str() {
                            "true" | "yes" | "on" | "1" => true,
                            "false" | "no" | "off" | "0" => false,
                            _ => return Err(error("unrecognized Boolean literal")),
                        };
                        return Ok(Some(Predicate::compare(field, *op, Data::Bool(value))));
                    }
                    if !matches!(
                        kind,
                        Scalar::I8
                            | Scalar::I16
                            | Scalar::I32
                            | Scalar::I64
                            | Scalar::Int(_)
                            | Scalar::U8
                            | Scalar::U16
                            | Scalar::U24
                            | Scalar::U32
                            | Scalar::U64
                            | Scalar::Uint(_)
                            | Scalar::F32
                            | Scalar::F64
                            | Scalar::TimestampMs
                            | Scalar::TimestampUs
                            | Scalar::TimestampNs
                    ) {
                        return Ok(None);
                    }
                    let unit = if matches!(
                        aggregate,
                        Some(
                            "count"
                                | "nulls"
                                | "absent"
                                | "trues"
                                | "falses"
                                | "null_count"
                                | "absent_count"
                                | "true_count"
                                | "false_count"
                        )
                    ) {
                        None
                    } else {
                        self.time_unit(i).or_else(|| self.annotation(i, "@unit"))
                    };
                    // Plain integers avoid arbitrary-precision allocation entirely.
                    if text
                        .trim()
                        .trim_start_matches(['+', '-'])
                        .bytes()
                        .all(|c| c.is_ascii_digit())
                        && !text.trim().is_empty()
                    {
                        return Ok(Some(Predicate::compare(
                            field,
                            *op,
                            pil_codec::native_integer_text(text.trim().trim_start_matches('+'))?,
                        )));
                    }
                    let number = match cast::number(text, unit) {
                        Ok(n) => n,
                        Err(e) if self.time_unit(i).is_some() => {
                            let ns = cast::timestamp(text, context)
                                .map_err(|t| error(format!("{e}; {t}")))?;
                            let scale = match unit {
                                Some("ms") => 1_000_000,
                                Some("us") => 1_000,
                                Some("ns") => 1,
                                _ => return Err(error("unsupported timestamp unit")),
                            };
                            cast::Number(BigRational::new(BigInt::from(ns), BigInt::from(scale)))
                        }
                        Err(e) => return Err(e),
                    };
                    if matches!(kind, Scalar::F32 | Scalar::F64) {
                        return Ok(Some(Predicate::compare(
                            field,
                            *op,
                            Data::Float(number.float()?),
                        )));
                    }
                    Ok(Some(integer(field, *op, number.0)?))
                })();
                result.map_err(|e| error(format!("cannot compare {field} with {text:?}: {e}")))
            }
            And(parts) | Or(parts) => {
                let mut changed = false;
                let converted = parts
                    .iter()
                    .map(|p| {
                        self.coerce(p, context, summary).map(|v| {
                            changed |= v.is_some();
                            v
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if !changed {
                    return Ok(None);
                }
                let parts = parts
                    .iter()
                    .zip(converted)
                    .map(|(p, v)| v.unwrap_or_else(|| p.clone()))
                    .collect();
                Ok(Some(if matches!(p, And(_)) {
                    And(parts)
                } else {
                    Or(parts)
                }))
            }
            Not(p) => Ok(self.coerce(p, context, summary)?.map(|p| Not(Box::new(p)))),
            _ => Ok(None),
        }
    }
}
fn data(n: BigInt) -> Result<Data, Error> {
    pil_codec::native_integer_text(&n.to_string())
}
pub(crate) fn integer(field: &str, op: Compare, n: BigRational) -> Result<Predicate, Error> {
    if n.is_integer() {
        return Ok(Predicate::compare(field, op, data(n.to_integer())?));
    }
    // Integer fields cannot equal a fractional threshold. Normalize inequalities
    // exactly, retaining ordinary integer comparator/byte plans and SQL unknowns.
    let floor = data(n.floor().to_integer())?;
    Ok(match op {
        Compare::Lt | Compare::Le => Predicate::compare(field, Compare::Le, floor),
        Compare::Gt | Compare::Ge => Predicate::compare(field, Compare::Gt, floor),
        Compare::Eq | Compare::Ne => {
            let comparisons = vec![
                Predicate::compare(field, Compare::Le, floor.clone()),
                Predicate::compare(field, Compare::Gt, floor),
            ];
            if op == Compare::Eq {
                Predicate::And(comparisons)
            } else {
                Predicate::Or(comparisons)
            }
        }
    })
}
