//! Resolve authored summary predicates against their source Definition once.
use super::*;
use crate::{Compare, Predicate, SeedDefinition, cast::Context};
use ciborium::Value as Data;

pub(super) fn plan_predicate(
    filters: &[Json],
    d: &SeedDefinition,
    c: &Context,
) -> Result<Json, String> {
    let mut out = None;
    for f in filters {
        let next = bound_predicate(&json!({"kind":"filter","filter":f}), d, c)?;
        out = Some(match out {
            None => next,
            Some(previous) => json!({"kind":"and","left":previous,"right":next}),
        });
    }
    out.ok_or_else(|| "empty filter list".into())
}
pub(super) fn bound_predicate(p: &Json, d: &SeedDefinition, c: &Context) -> Result<Json, String> {
    let mut result = p.clone();
    match p["kind"].as_str() {
        Some("and" | "or" | "xor") => {
            result["left"] = bound_predicate(&p["left"], d, c)?;
            result["right"] = bound_predicate(&p["right"], d, c)?;
        }
        Some("not") => result["value"] = bound_predicate(&p["value"], d, c)?,
        Some("filter") => {
            let f = &p["filter"];
            if f.get("cast").is_some(){return Err("Stored-field CAST requires event rows; summary aggregates cannot reproduce it".into());}
            let literal = &f["value"];
            if !matches!(literal["kind"].as_str(), Some("string" | "duration")) {
                return Ok(result);
            }
            let name = f["field"].as_str().ok_or("Missing comparison field")?;
            let text = literal["value"].as_str().ok_or("Missing comparison text")?;
            let op = match f["op"].as_str() {
                Some("=" | "==") => Compare::Eq,
                Some("!=") => Compare::Ne,
                Some("<") => Compare::Lt,
                Some("<=") => Compare::Le,
                Some(">") => Compare::Gt,
                Some(">=") => Compare::Ge,
                _ => return Ok(result),
            };
            // Summary-owned count/bucket fields do not appear in the event RX.
            if matches!(name, "count" | "bucket") {
                let n = crate::cast::number(text, None)
                    .or_else(|e| {
                        if name != "bucket" {
                            return Err(e);
                        }
                        Ok(crate::cast::Number(num_rational::BigRational::new(
                            crate::cast::timestamp(text, c)?.into(),
                            1_000_000.into(),
                        )))
                    })
                    .map_err(|e| e.to_string())?;
                result = expression(
                    &crate::query::coercion::integer(name, op, n.0).map_err(|e| e.to_string())?,
                )?;
            } else {
                let prepared = d
                    .prepare_summary(&Predicate::compare(name, op, text), c)
                    .map_err(|e| e.to_string())?;
                result = expression(&prepared)?;
                if literal["kind"] == "duration" && result["filter"]["value"]["kind"] == "string" {
                    return Err("Unit literal requires a numeric field with a declared unit".into());
                }
            }
        }
        _ => {}
    }
    Ok(result)
}
fn expression(p: &Predicate) -> Result<Json, String> {
    Ok(match p {
        Predicate::Compare { field, op, value } => {
            let (kind, text) = match value {
                Data::Integer(n) => ("number", i128::from(*n).to_string()),
                Data::Float(n) => ("number", n.to_string()),
                Data::Text(s) => ("string", s.clone()),
                Data::Bool(v) => ("boolean", v.to_string()),
                Data::Null => ("null", String::new()),
                Data::Tag(2, _) => (
                    "number",
                    pil_codec::unsigned_native(value)
                        .map_err(|e| e.to_string())?
                        .to_string(),
                ),
                Data::Tag(3, _) => (
                    "number",
                    pil_codec::signed_native(value)
                        .map_err(|e| e.to_string())?
                        .to_string(),
                ),
                _ => return Err("unsupported prepared summary literal".into()),
            };
            let op = match op {
                Compare::Eq => "==",
                Compare::Ne => "!=",
                Compare::Lt => "<",
                Compare::Le => "<=",
                Compare::Gt => ">",
                Compare::Ge => ">=",
            };
            json!({"kind":"filter","filter":{"field":field,"op":op,"value":{"kind":kind,"value":text}}})
        }
        Predicate::And(parts) | Predicate::Or(parts) => {
            let mut items = parts.iter();
            let mut out = expression(items.next().ok_or("empty prepared predicate")?)?;
            for p2 in items {
                out = json!({"kind":if matches!(p,Predicate::And(_)){"and"}else{"or"},"left":out,"right":expression(p2)?});
            }
            out
        }
        Predicate::Not(p) => json!({"kind":"not","value":expression(p)?}),
        _ => return Err("unsupported prepared summary predicate".into()),
    })
}
