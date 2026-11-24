//! Shared query request decoding for native and WASM transport adapters.
use crate::{Compare, Direction, Error, Predicate, Query};
use ciborium::Value as Data;
use serde_json::Value as Json;
fn fail(s: impl Into<String>) -> Error {
    Error::Definition(s.into())
}
/// Lower the shared language predicate into the registered Seed query model.
pub fn language_predicate(v: &Json) -> Result<Json, Error> {
    use serde_json::json;
    match v["kind"].as_str() {
        Some("filter") => {
            let f=&v["filter"];let field=required_text(f,"field")?;let op=required_text(f,"op")?;
            if f["value"]["kind"]=="null" {
                return match op {
                    "=="=>Ok(json!({"field":field,"op":"is-null","value":null})),
                    "!="=>Ok(json!({"and":[{"field":field,"op":"is-present","value":null},{"not":{"field":field,"op":"is-null","value":null}}]})),
                    _=>Err(fail("Invalid null comparison")),
                };
            }
            let mut result=json!({"field":field,"op":op,"value":{"$literal":f["value"]}});
            if let Some(cast)=f.get("cast"){result["cast"]=cast.clone();}
            Ok(result)
        },
        Some("not")=>Ok(json!({"not":language_predicate(&v["value"])?})),
        Some("and"|"or")=>Ok(json!({v["kind"].as_str().unwrap():[language_predicate(&v["left"])?,language_predicate(&v["right"])?]})),
        Some("xor")=>{
            let left=language_predicate(&v["left"])?;
            let right=language_predicate(&v["right"])?;
            Ok(json!({"or":[{"and":[&left,{"not":&right}]},{"and":[{"not":&left},&right]}]}))
        },
        _=>Err(fail("Unsupported language predicate")),
    }
}
pub fn context(v: &Json) -> Result<crate::cast::Context, Error> {
    let mut context = crate::cast::Context::default();
    if let Some(v) = v.get("context") {
        if let Some(n) = v.get("referenceMs") {
            context.reference_ms = Some(
                n.as_i64()
                    .ok_or_else(|| fail("referenceMs requires integer milliseconds"))?,
            );
        }
        if let Some(s) = v.get("timezone") {
            context.timezone = s
                .as_str()
                .ok_or_else(|| fail("timezone requires text"))?
                .into();
        }
        context.date_order = match v.get("dateOrder").and_then(Json::as_str) {
            None => None,
            Some("mdy") => Some(crate::cast::DateOrder::MonthFirst),
            Some("dmy") => Some(crate::cast::DateOrder::DayFirst),
            _ => return Err(fail("dateOrder must be mdy or dmy")),
        };
    }
    Ok(context)
}
pub fn query(v: &Json) -> Result<Query, Error> {
    let mut q = Query::new().context(context(v)?);
    if let Some(version) = v.get("version") {
        let pair = version
            .as_array()
            .filter(|v| v.len() == 2)
            .ok_or_else(|| fail("Definition version requires major and minor"))?;
        let major = pair[0]
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| fail("Invalid Definition major version"))?;
        let minor = pair[1]
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| fail("Invalid Definition minor version"))?;
        q = q.version(major, minor);
    }
    if let Some(p) = v.get("where") {
        q = q.filter(predicate(p)?);
    }
    if let Some(names) = v.get("select") {
        q = q.select(
            names
                .as_array()
                .ok_or_else(|| fail("select requires names"))?
                .iter()
                .map(|v| v.as_str().ok_or_else(|| fail("field name must be text")))
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    if let Some(casts) = v.get("casts") {
        for item in casts
            .as_array()
            .ok_or_else(|| fail("casts requires array"))?
        {
            let spec =
                serde_json::from_value(item["cast"].clone()).map_err(|e| fail(e.to_string()))?;
            q = q.cast(
                required_text(item, "field")?,
                required_text(item, "alias")?,
                spec,
            );
        }
    }
    if let Some(order) = v.get("orderBy") {
        let direction = match order
            .get("direction")
            .and_then(Json::as_str)
            .unwrap_or("asc")
        {
            "asc" => Direction::Ascending,
            "desc" => Direction::Descending,
            _ => return Err(fail("invalid sort direction")),
        };
        q = q.order_by(required_text(order, "field")?, direction);
    }
    if let Some(n) = v.get("limit") {
        q = q.limit(count(n)?);
    }
    if let Some(n) = v.get("offset") {
        q = q.offset(count(n)?);
    }
    Ok(q)
}
pub fn count(v: &Json) -> Result<usize, Error> {
    usize::try_from(
        v.as_u64()
            .ok_or_else(|| fail("count must be a nonnegative integer"))?,
    )
    .map_err(|_| Error::Overflow)
}
pub fn required_text<'a>(v: &'a Json, k: &str) -> Result<&'a str, Error> {
    v.get(k)
        .and_then(Json::as_str)
        .ok_or_else(|| fail(format!("missing text {k}")))
}
pub fn predicate(v: &Json) -> Result<Predicate, Error> {
    if let Some(a) = v.get("and") {
        return Ok(Predicate::And(
            a.as_array()
                .ok_or_else(|| fail("and requires array"))?
                .iter()
                .map(predicate)
                .collect::<Result<_, _>>()?,
        ));
    }
    if let Some(a) = v.get("or") {
        return Ok(Predicate::Or(
            a.as_array()
                .ok_or_else(|| fail("or requires array"))?
                .iter()
                .map(predicate)
                .collect::<Result<_, _>>()?,
        ));
    }
    if let Some(a) = v.get("not") {
        return Ok(Predicate::Not(Box::new(predicate(a)?)));
    }
    let field = required_text(v, "field")?.to_owned();
    let cast: Option<crate::cast::Spec> = v
        .get("cast")
        .map(|c| serde_json::from_value(c.clone()).map_err(|e| fail(e.to_string())))
        .transpose()?;
    if required_text(v, "op")? == "is-null" {
        if let Some(cast) = cast {
            return Ok(Predicate::CastNull { field, cast });
        }
    }
    let op = match required_text(v, "op")? {
        "is-null" => return Ok(Predicate::IsNull(field)),
        "is-absent" => return Ok(Predicate::IsAbsent(field)),
        "is-present" => return Ok(Predicate::IsPresent(field)),
        "contains" => {
            return match literal(v.get("value").ok_or_else(|| fail("missing text"))?)? {
                Data::Text(text) => Ok(Predicate::Contains { field, text }),
                _ => Err(fail("contains requires text")),
            };
        }
        "=" | "==" => Compare::Eq,
        "!=" => Compare::Ne,
        "<" => Compare::Lt,
        "<=" => Compare::Le,
        ">" => Compare::Gt,
        ">=" => Compare::Ge,
        _ => return Err(fail("unsupported comparison")),
    };
    let value = literal(
        v.get("value")
            .ok_or_else(|| fail("missing comparison value"))?,
    )?;
    Ok(match cast {
        Some(cast) => Predicate::Cast {
            field,
            cast,
            op,
            value,
        },
        None => Predicate::Compare { field, op, value },
    })
}
pub fn literal(v: &Json) -> Result<Data, Error> {
    if let Some(v) = v.get("$literal") {
        let text = required_text(v, "value")?;
        return match required_text(v, "kind")? {
            "string" => Ok(Data::Text(text.to_owned())),
            "null" => Ok(Data::Null),
            "boolean" => text
                .parse::<bool>()
                .map(Data::Bool)
                .map_err(|_| fail("invalid boolean")),
            "number" => {
                if text.contains(['.', 'e', 'E']) {
                    text.parse::<f64>()
                        .map(Data::Float)
                        .map_err(|_| fail("invalid numeric literal"))
                } else {
                    pil_codec::native_integer_text(text).map_err(|e| fail(e.to_string()))
                }
            }
            "duration" => Ok(Data::Text(text.to_owned())),
            _ => Err(fail("unsupported query literal")),
        };
    }
    Ok(match v {
        Json::Null => Data::Null,
        Json::Bool(v) => Data::Bool(*v),
        Json::String(v) => Data::Text(v.clone()),
        Json::Number(v) => {
            if let Some(i) = v.as_u64() {
                Data::Integer(i.into())
            } else if let Some(i) = v.as_i64() {
                Data::Integer(i.into())
            } else {
                Data::Float(v.as_f64().ok_or_else(|| fail("invalid number"))?)
            }
        }
        Json::Object(v) if v.len() == 1 && v.contains_key("$integer") => {
            pil_codec::native_integer_text(
                v["$integer"]
                    .as_str()
                    .ok_or_else(|| fail("integer requires decimal text"))?,
            )?
        }
        _ => return Err(fail("comparison requires a scalar literal")),
    })
}
