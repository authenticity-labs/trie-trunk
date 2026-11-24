//! Reduce an exact event selection using compatible period aggregates and filtered Seed views.
use super::*;
use crate::{Cell, Error, error};
use ciborium::Value as Data;
pub(crate) fn scalar(value: Data) -> Result<Json, Error> {
    Ok(match value {
        Data::Null => Json::Null,
        Data::Bytes(v) => json!({"$bytes":v}),
        Data::Bool(v) => v.into(),
        Data::Text(v) => v.into(),
        Data::Integer(v) => json!({"$integer":i128::from(v).to_string()}),
        Data::Float(v) => serde_json::Number::from_f64(v)
            .map(Json::Number)
            .ok_or_else(|| error("non-finite summary value"))?,
        Data::Array(v) => Json::Array(v.into_iter().map(scalar).collect::<Result<_, _>>()?),
        Data::Map(v) => {
            if v.iter().all(|(k, _)| matches!(k, Data::Text(_))) {
                Json::Object(
                    v.into_iter()
                        .map(|(k, v)| {
                            let Data::Text(k) = k else { unreachable!() };
                            Ok((k, scalar(v)?))
                        })
                        .collect::<Result<_, Error>>()?,
                )
            } else {
                json!({"$map":v.into_iter().map(|(k,v)|Ok(vec![scalar(k)?,scalar(v)?])).collect::<Result<Vec<_>,Error>>()?})
            }
        }
        Data::Tag(tag, v) if tag == 2 || tag == 3 => {
            let Data::Bytes(b) = *v else {
                return Err(error("invalid wide summary integer"));
            };
            if b.len() > 16 {
                return Err(error("summary integer exceeds 128 bits"));
            }
            let n = b.into_iter().fold(0u128, |n, b| (n << 8) | u128::from(b));
            let text = if tag == 2 {
                n.to_string()
            } else {
                let n =
                    i128::try_from(n).map_err(|_| error("summary integer exceeds signed range"))?;
                (-1 - n).to_string()
            };
            json!({"$integer":text})
        }
        _ => return Err(error("unsupported summary value")),
    })
}

pub(crate) type Contribution = (u64, Option<Num>, Option<Num>, Option<Num>);

/// A missing proof requests event evaluation. Read errors still fail the query.
pub(crate) trait Period: Row {
    fn key(&self, field: &str) -> Result<Option<Option<Json>>, String>;
    fn contribution(&self, field: &str, op: &str) -> Result<Option<Contribution>, String>;
}

impl Reducer {
    pub(crate) fn selected_fields(&self) -> impl Iterator<Item = String> + '_ {
        self.spec
            .selection
            .iter()
            .filter_map(|s| s.field.clone())
            .chain(self.spec.group_by.iter().flatten().cloned())
    }
    pub(crate) fn period_total(&mut self, row: &impl Period, matched: u64) -> Result<bool, String> {
        let mut keys = Vec::new();
        for field in self.spec.group_by.iter().flatten() {
            let Some(key) = row.key(field)? else {
                return Ok(false);
            };
            keys.push(key);
        }
        // Stage the complete projection before merging: a missing statistic sends the
        // entire period to the cursor, including columns that could be summarized.
        let mut contributions = Vec::with_capacity(self.spec.selection.len());
        for s in &self.spec.selection {
            let Some(op) = s.function.as_deref() else {
                contributions.push(None);
                continue;
            };
            let part = if let Some(field) = &s.field {
                if matched != row.count()? {
                    return Ok(false);
                }
                let Some(value) = row.contribution(field, op)? else {
                    return Ok(false);
                };
                value
            } else {
                (matched, None, None, None)
            };
            contributions.push(Some(part));
        }
        let acc = accumulators(
            &mut self.groups,
            &mut self.memory,
            self.spec.selection.len(),
            keys,
        )?;
        for (acc, part) in acc.iter_mut().zip(contributions) {
            if let Some((count, min, max, sum)) = part {
                acc.merge(count, min, max, sum)?;
            }
        }
        Ok(true)
    }
    pub(crate) fn events(&mut self, batch: &crate::Batch) -> Result<(), String> {
        let groups = self
            .spec
            .group_by
            .as_deref()
            .ok_or("Missing aggregate keys")?;
        let size = self.spec.selection.len();
        if groups.is_empty() && self.spec.selection.iter().all(|s| s.field.is_none()) {
            for acc in accumulators(&mut self.groups, &mut self.memory, size, vec![])? {
                acc.merge(batch.len() as u64, None, None, None)?;
            }
            return Ok(());
        }
        let mut reader = batch.reader();
        for i in 0..batch.len() {
            let view = reader.view(i).map_err(|e| e.to_string())?;
            let get = |f: &str| view.get(f).map_err(|e| e.to_string());
            let mut keys = Vec::with_capacity(groups.len());
            for field in groups {
                keys.push(match get(field)? {
                    Cell::Absent => None,
                    Cell::Value(v) => Some(scalar(v).map_err(|e| e.to_string())?),
                });
            }
            let acc = accumulators(&mut self.groups, &mut self.memory, size, keys)?;
            for (s, a) in self.spec.selection.iter().zip(acc) {
                let Some(op) = s.function.as_deref() else {
                    continue;
                };
                let Some(f) = &s.field else {
                    a.merge(1, None, None, None)?;
                    continue;
                };
                let Cell::Value(v) = get(f)? else { continue };
                if matches!(v, Data::Null) {
                    continue;
                }
                if op == "count" {
                    a.merge(1, None, None, None)?;
                    continue;
                }
                let n = match v {
                    Data::Integer(n) => Num::Int(n.into()),
                    Data::Float(n) if n.is_finite() => Num::Float(n),
                    Data::Tag(2, _) => {
                        Num::Uint(pil_codec::unsigned_native(&v).map_err(|e| e.to_string())?)
                    }
                    Data::Tag(3, _) => {
                        Num::Int(pil_codec::signed_native(&v).map_err(|e| e.to_string())?)
                    }
                    _ => return Err(format!("{op} requires finite numeric data for {f}")),
                };
                a.merge(
                    1,
                    (op == "min").then_some(n),
                    (op == "max").then_some(n),
                    matches!(op, "sum" | "avg").then_some(n),
                )?;
            }
        }
        Ok(())
    }
}
