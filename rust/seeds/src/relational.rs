//! Streaming relational reductions shared by native and WASM readers.
#[cfg(feature = "query-language")]
pub mod join;
mod literals;
#[cfg(feature="registration")]
pub(crate) mod aggregate;
use literals::{bound_predicate,plan_predicate};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Selection {
    pub field: Option<String>,
    pub function: Option<String>,
    pub alias: String,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub cast: Option<crate::cast::Spec>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Relational {
    pub selection: Vec<Selection>,
    #[serde(rename = "groupBy")]
    pub group_by: Option<Vec<String>>,
}
#[derive(Clone, Copy)]
pub(crate) enum Num {
    Int(i128),
    Uint(u128),
    Float(f64),
}
impl Num {
    pub(crate) fn read(v: &Json) -> Result<Self, String> {
        if let Some(s) = v.get("$integer").and_then(Json::as_str) {
            return s.parse::<i128>().map(Self::Int).or_else(|_|s.parse::<u128>().map(Self::Uint)).map_err(|_|"Aggregate integer exceeds 128-bit range".into());
        }
        if let Some(n) = v.as_i64() {
            return Ok(Self::Int(n.into()));
        }
        if let Some(n) = v.as_u64() {
            return Ok(Self::Int(n.into()));
        }
        v.as_f64()
            .filter(|v| v.is_finite())
            .map(Self::Float)
            .ok_or_else(|| "Expected a finite numeric aggregate".into())
    }
    pub(crate) fn float(self) -> f64 {
        match self {
            Self::Int(n) => n as f64,
            Self::Uint(n) => n as f64,
            Self::Float(n) => n,
        }
    }
    fn add(self, b: Self) -> Result<Self, String> {
        match (self, b) {
            (Self::Int(a), Self::Int(b)) => a
                .checked_add(b)
                .map(Self::Int)
                .ok_or_else(|| "Aggregate sum overflow".into()),
            (Self::Uint(a),Self::Uint(b))=>a.checked_add(b).map(Self::Uint).ok_or_else(||"Aggregate sum overflow".into()),
            (Self::Int(a),Self::Uint(b)) | (Self::Uint(b),Self::Int(a))=> {
                if a<0 && a.unsigned_abs()>b {let n=a.unsigned_abs()-b;Ok(Self::Int(if n==1u128<<127{i128::MIN}else{-(n as i128)}))}
                else if a<0 {Ok(Self::Uint(b-a.unsigned_abs()))}
                else {b.checked_add(a as u128).map(Self::Uint).ok_or_else(||"Aggregate sum overflow".into())}
            },
            _ => {
                let n = self.float() + b.float();
                if n.is_finite() {
                    Ok(Self::Float(n))
                } else {
                    Err("Aggregate sum overflow".into())
                }
            }
        }
    }
    pub(crate) fn cmp(self, b: Self) -> std::cmp::Ordering {
        use crate::comparison::Number;
        let number = |v| match v {
            Self::Int(n) => Number::Signed(n),
            Self::Uint(n) => Number::Unsigned(n),
            Self::Float(n) => Number::Float(n),
        };
        // read() and add() reject non-finite aggregate values.
        number(self).compare(number(b)).unwrap()
    }

    pub(crate) fn json(self) -> Json {
        match self {
            Self::Int(n) => json!({"$integer":n.to_string()}),
            Self::Uint(n) => json!({"$integer":n.to_string()}),
            Self::Float(n) => json!(n),
        }
    }
}
#[derive(Default)]
struct Acc {
    count: u64,
    sum: Option<Num>,
    min: Option<Num>,
    max: Option<Num>,
}
impl Acc {
    fn merge(&mut self, count:u64, min:Option<Num>, max:Option<Num>, sum:Option<Num>) -> Result<(), String> {
            self.count = self
                .count
                .checked_add(count)
                .ok_or("Aggregate count overflow")?;
            if let Some(n) = sum {
                self.sum = Some(if let Some(old) = self.sum {
                    old.add(n)?
                } else {
                    n
                })
            }
            if let Some(n) = min {
                self.min = Some(
                    self.min
                        .map(|old| if old.cmp(n).is_lt() { old } else { n })
                        .unwrap_or(n),
                )
            }
            if let Some(n) = max {
                self.max = Some(
                    self.max
                        .map(|old| if old.cmp(n).is_gt() { old } else { n })
                        .unwrap_or(n),
                )
            }
        Ok(())
    }
}
fn integer(v: &Json) -> Result<u64, String> {
    v.as_u64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .ok_or_else(|| "Invalid summary count".into())
}
fn scalar(row: &Json, field: &str) -> Result<Option<Json>, String> {
    if let Some(v) = row["values"].get(field) {
        return Ok(Some(v.clone()));
    }
    let m = &row["measures"][field];
    if m.is_null() {
        return Err(format!("Summary does not retain field {field}"));
    }
    let total = integer(&row["count"])?;
    let count = integer(&m["count"])?;
    if count == 0 {
        return if integer(&m["nulls"])? == total {
            Ok(Some(Json::Null))
        } else if integer(&m["nulls"])? == 0 {
            Ok(None)
        } else {
            Err(format!(
                "Summary cannot separate null and absent groups for {field}"
            ))
        };
    }
    if count == total && m.get("min").is_some() && m["min"] == m["max"] {
        return Ok(Some(m["min"].clone()));
    }
    Err(format!(
        "Summary cannot separate values of {field} within a bucket"
    ))
}
/// Borrowed input to the existing reducer. Storage readers supply typed numeric
/// members directly; JSON is only required for retained keys and output cells.
pub(crate) trait Row {
    fn count(&self) -> Result<u64, String>;
    fn scalar(&self, field: &str) -> Result<Option<Json>, String>;
    fn values(&self) -> Result<Json, String>;
    fn number(&self, field: &str) -> Result<Option<Num>, String> {
        self.scalar(field)?.filter(|v| !v.is_null()).as_ref().map(Num::read).transpose()
    }
    fn aggregate(&self, field: &str, op: &str) -> Result<(u64, Option<Num>, Option<Num>, Option<Num>), String>;
}
impl Row for Json {
    fn count(&self) -> Result<u64, String> { integer(&self["count"]) }
    fn scalar(&self, field: &str) -> Result<Option<Json>, String> { scalar(self, field) }
    fn values(&self) -> Result<Json, String> { Ok(self["values"].clone()) }
    fn aggregate(&self, field: &str, op: &str) -> Result<(u64, Option<Num>, Option<Num>, Option<Num>), String> {
        let m=&self["measures"][field];
        if m.is_null() { return Err(format!("Summary has no aggregate for {field}")); }
        let count=integer(&m["count"])?;
        let get=|key| { if count==0 {Ok(None)} else {m.get(key).ok_or_else(||format!("Summary has no {key} aggregate for {field}")).and_then(Num::read).map(Some)} };
        Ok((count, if op=="min" {get("min")?} else {None}, if op=="max" {get("max")?} else {None}, if op=="sum"||op=="avg" {get("sum")?} else {None}))
    }
}
#[cfg(test)]
pub(crate) fn matches(row: &Json, p: &Json) -> Result<bool, String> { matches_row(row, p) }
fn matches_row(row: &impl Row, p: &Json) -> Result<bool, String> {
    Ok(match p["kind"].as_str() {
        Some("and") => matches_row(row, &p["left"])? && matches_row(row, &p["right"])?,
        Some("or") => matches_row(row, &p["left"])? || matches_row(row, &p["right"])?,
        Some("xor") => matches_row(row, &p["left"])? != matches_row(row, &p["right"])?,
        Some("not") => !matches_row(row, &p["value"])?,
        Some("filter") => {
            let f = &p["filter"];
            if f.get("cast").is_some(){return Err("Stored-field CAST requires event rows".into());}
            let Some(a) = row.scalar(f["field"].as_str().ok_or("Missing predicate field")?)?
            else {
                return Ok(false);
            };
            let v = &f["value"];
            let op = f["op"].as_str().ok_or("Missing comparison")?;
            if v["kind"] == "null" {
                return Ok(if op == "==" {
                    a.is_null()
                } else if op == "!=" {
                    !a.is_null()
                } else {
                    false
                });
            }
            if a.is_null() {
                return Ok(false);
            }
            let b = match v["kind"].as_str() {
                Some("duration") => return Err("Unit literal requires a bound Definition".into()),
                Some("number") => {
                    let s = v["value"].as_str().ok_or("Missing numeric literal")?;
                    if !s.contains(['.','e','E']) {
                        if s.parse::<i128>().is_err() && s.parse::<u128>().is_err(){return Err("Numeric literal exceeds 128-bit range".into());}
                        json!({"$integer":s})
                    } else {
                        json!(s.parse::<f64>().map_err(|_| "Invalid number")?)
                    }
                }
                Some("boolean") => json!(v["value"] == "true"),
                Some("string") => v["value"].clone(),
                _ => return Err("Unsupported predicate literal".into()),
            };
            let cmp = if let (Ok(a), Ok(b)) = (Num::read(&a), Num::read(&b)) {
                a.cmp(b)
            } else if a.is_string() && b.is_string() {
                a.as_str().cmp(&b.as_str())
            } else if a.is_boolean() && b.is_boolean() {
                a.as_bool().cmp(&b.as_bool())
            } else {
                return Err("Predicate types do not match".into());
            };
            match op {
                "==" => cmp.is_eq(),
                "!=" => !cmp.is_eq(),
                ">" => cmp.is_gt(),
                ">=" => !cmp.is_lt(),
                "<" => cmp.is_lt(),
                "<=" => !cmp.is_gt(),
                _ => return Err("Unsupported summary comparison".into()),
            }
        }
        _ => return Err("Invalid predicate".into()),
    })
}
/// Consume already-decoded accumulator values supplied by the storage adapter.
/// All grouping/reduction executes here; neither SQL nor the host owns a second planner.
/// One bounded execution state. Input rows are consumed immediately; only
/// group accumulators or the requested result rows survive each push.
pub struct Reducer {
    plan: Json,
    authored: Json,
    binding: Option<std::sync::Arc<crate::SeedDefinition>>,
    context: crate::cast::Context,
    spec: Relational,
    groups: std::collections::BTreeMap<String, (Vec<Option<Json>>, Vec<Acc>)>,
    raw: Vec<Vec<Json>>,
    star: Vec<Json>,
    inputs: usize,
    memory: Retained,
}
fn accumulators<'a>(groups:&'a mut std::collections::BTreeMap<String,(Vec<Option<Json>>,Vec<Acc>)>,memory:&mut Retained,size:usize, values:Vec<Option<Json>>)->Result<&'a mut Vec<Acc>,String>{
        let key = if values.is_empty(){String::new()}else{serde_json::to_string(
            &values
                .iter()
                .map(|v| v.clone().unwrap_or_else(|| json!({"$absent":true})))
                .collect::<Vec<_>>(),
        )
        .unwrap()};
        if !groups.contains_key(&key) && groups.len() >= 10000 {
            return Err("Query exceeds 10000 groups; narrow the scope or time range".into());
        }
        if !groups.contains_key(&key) {
            memory.charge(
                256 + key.len()
                    + values
                        .iter()
                        .flatten()
                        .map(json_bytes)
                        .sum::<usize>()
                        .saturating_mul(2)
                    + size * 256,
            )?;
        }
        let (_, acc) = groups.entry(key).or_insert_with(|| {
            (
                values,
                (0..size).map(|_| Acc::default()).collect(),
            )
        });
    Ok(acc)
}
impl Reducer {
    pub fn new(plan: &Json) -> Result<Self, String> {
        let spec: Relational =
            serde_json::from_value(plan["relational"].clone()).map_err(|e| e.to_string())?;
        let grouped = spec.group_by.as_ref();
        if spec.selection.iter().any(|s| s.function.is_some()) && grouped.is_none() {
            return Err("Aggregate plan is missing its grouping keys".into());
        }
        for s in &spec.selection {
            if s.cast.is_some(){return Err("CAST projections require event rows".into());}
            if let Some(op) = &s.function {
                if !["count", "sum", "min", "max", "avg"].contains(&op.as_str())
                    || s.field.is_none() && op != "count"
                {
                    return Err("Invalid aggregate selection".into());
                }
            } else if s.field.is_none()
                || grouped.is_some_and(|keys| !keys.contains(s.field.as_ref().unwrap()))
            {
                return Err("Selected field must appear in GROUP BY".into());
            }
        }
        Ok(Self {
            plan: plan.clone(),
            authored: plan.clone(),
            binding: None,
            context: crate::cast::Context::default(),
            spec,
            groups: Default::default(),
            raw: Vec::new(),
            star: Vec::new(),
            inputs: 0,
            memory: Retained::default(),
        })
    }
    pub fn context(mut self, context:crate::cast::Context)->Self {self.context=context;self}
    pub(crate) fn bind(&mut self, definition:&std::sync::Arc<crate::SeedDefinition>)->Result<(),String> {
        if self.binding.as_ref().is_some_and(|d|std::sync::Arc::ptr_eq(d,definition)){return Ok(());}
        let mut plan=self.authored.clone();
        if let Some(p)=plan.get_mut("predicate"){*p=bound_predicate(p,definition,&self.context)?;}
        if let Some(filters)=plan.get_mut("filters").and_then(Json::as_array_mut) {
            if filters.is_empty(){self.plan=plan;self.binding=Some(definition.clone());return Ok(());}
            let mut predicate=plan_predicate(filters,definition,&self.context)?;
            filters.clear();
            if let Some(existing)=plan.get("predicate"){predicate=json!({"kind":"and","left":existing,"right":predicate});}
            plan["predicate"]=predicate;
        }
        self.plan=plan;self.binding=Some(definition.clone());Ok(())
    }
    pub fn memory_budget(mut self, budget: crate::MemoryBudget) -> Self {
        self.memory.budget = budget;
        self
    }
    pub fn push(&mut self, row: &Json) -> Result<(), String> { self.push_row(row) }
    pub(crate) fn push_row(&mut self, row: &impl Row) -> Result<(), String> {
        if self.inputs >= 1_000_000 {
            return Err("Query exceeds 1000000 summary rows".into());
        }
        self.inputs += 1;
        let plan = &self.plan;
        let spec = &self.spec;
        let grouped = spec.group_by.as_ref();
        let summary_source = plan["source"] == "summaries";
        let groups = &mut self.groups;
        let raw = &mut self.raw;
        if let Some(p) = plan.get("predicate") {
            if !matches_row(row, p)? {
                return Ok(());
            }
        }
        for f in plan["filters"].as_array().into_iter().flatten() {
            if !matches_row(row, &json!({"kind":"filter","filter":f}))? {
                return Ok(());
            }
        }
        let Some(keys) = grouped else {
            if !summary_source {
                return Err("Event projections use the event reader".into());
            }
            if spec.selection.is_empty() {
                let values = json!({"values":row.values()?});
                self.memory.charge(json_bytes(&values).saturating_mul(2))?;
                self.star.push(values);
                return Ok(());
            }
            let selection = &spec.selection;
            let values = selection
                .iter()
                .map(|s| {
                    row.scalar(s.field.as_ref().unwrap())
                        .map(|v| v.unwrap_or_else(|| json!({"$absent":true})))
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.memory.charge(
                values
                    .iter()
                    .map(json_bytes)
                    .sum::<usize>()
                    .saturating_mul(2)
                    + 64,
            )?;
            raw.push(values);
            return Ok(());
        };
        if !summary_source && row.count()? == 0 {
            return Ok(());
        }
        let values = keys
            .iter()
            .map(|f| row.scalar(f))
            .collect::<Result<Vec<_>, _>>()?;
        let acc=accumulators(groups,&mut self.memory,spec.selection.len(),values)?;
        for (s, a) in spec.selection.iter().zip(acc) {
            let Some(op) = s.function.as_deref() else {
                continue;
            };
            let (count, min, max, sum) = if summary_source {
                if let Some(f) = &s.field {
                    if op=="count" {
                        (u64::from(row.scalar(f)?.is_some_and(|v| !v.is_null())),None,None,None)
                    } else {
                        let n=row.number(f)?;
                        (u64::from(n.is_some()),n,n,n)
                    }
                } else {
                    (1, None, None, None)
                }
            } else if let Some(f) = &s.field {
                row.aggregate(f,op)?
            } else {
                (row.count()?, None, None, None)
            };
            a.merge(count,min,max,sum)?;
        }
        Ok(())
    }
    pub fn finish(self) -> Result<Json, String> {
        let spec = self.spec;
        let grouped = spec.group_by.as_ref();
        let mut groups = self.groups;
        if grouped.is_some_and(Vec::is_empty) && groups.is_empty() {
            groups.insert("[]".into(), (Vec::new(), spec.selection.iter().map(|_|Acc::default()).collect()));
        }
        let mut raw = self.raw;
        let columns: Vec<String> = if spec.selection.is_empty() {
            self.star
                .iter()
                .filter_map(|r| r["values"].as_object())
                .flat_map(|v| v.keys().cloned())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect()
        } else {
            spec.selection.iter().map(|s| s.alias.clone()).collect()
        };
        for row in &self.star {
            raw.push(
                columns
                    .iter()
                    .map(|n| scalar(row, n).map(|v| v.unwrap_or_else(|| json!({"$absent":true}))))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        if let Some(keys) = grouped {
            for (_, (values, acc)) in groups {
                raw.push(
                    spec.selection
                        .iter()
                        .zip(acc)
                        .map(|(s, a)| match s.function.as_deref() {
                            None => values[keys
                                .iter()
                                .position(|k| Some(k) == s.field.as_ref())
                                .unwrap()]
                            .clone()
                            .unwrap_or_else(|| json!({"$absent":true})),
                            Some("count") => json!({"$integer":a.count.to_string()}),
                            Some("sum") => a.sum.map(Num::json).unwrap_or(Json::Null),
                            Some("min") => a.min.map(Num::json).unwrap_or(Json::Null),
                            Some("max") => a.max.map(Num::json).unwrap_or(Json::Null),
                            Some("avg") => a
                                .sum
                                .filter(|_| a.count > 0)
                                .map(|n| json!(n.float() / a.count as f64))
                                .unwrap_or(Json::Null),
                            _ => Json::Null,
                        })
                        .collect(),
                );
            }
        }
        Ok(json!({"columns":columns,"rows":raw}))
    }
}
pub fn reduce(plan: &Json, rows: &[Json]) -> Result<Json, String> {
    let mut reducer = Reducer::new(plan)?;
    for row in rows {
        reducer.push(row)?;
    }
    reducer.finish()
}

// Conservative retained working-set accounting, not a process RSS estimate.
// Chunked leases avoid one allocation/atomic operation per input row.
struct Retained {
    budget: crate::MemoryBudget,
    leases: Vec<crate::MemoryLease>,
    used: usize,
    admitted: usize,
}
impl Default for Retained {
    fn default() -> Self {
        Self {
            budget: crate::MemoryBudget::new(64 * 1024 * 1024),
            leases: Vec::new(),
            used: 0,
            admitted: 0,
        }
    }
}
impl Retained {
    fn charge(&mut self, bytes: usize) -> Result<(), String> {
        let used = self
            .used
            .checked_add(bytes)
            .ok_or("summary memory overflow")?;
        if used > self.admitted {
            let amount = (used - self.admitted).max(16 * 1024);
            self.leases
                .push(self.budget.reserve(amount).map_err(|e| e.to_string())?);
            self.admitted += amount;
        }
        self.used = used;
        Ok(())
    }
}
fn json_bytes(value: &Json) -> usize {
    128 + match value {
        Json::String(s) => s.len(),
        Json::Array(v) => v.iter().map(json_bytes).sum(),
        Json::Object(v) => v.iter().map(|(k, v)| k.len() + 128 + json_bytes(v)).sum(),
        _ => 0,
    }
}

#[cfg(test)]
mod numeric_tests {
    use super::*;
    #[test]
    fn wide_unsigned_reductions_never_round_through_float() {
        let max=Num::read(&json!({"$integer":u128::MAX.to_string()})).unwrap();
        assert!(max.cmp(Num::Float(2f64.powi(128))).is_lt());
        assert_eq!(max.add(Num::Int(-1)).unwrap().json(),json!({"$integer":(u128::MAX-1).to_string()}));
        assert!(max.add(Num::Int(1)).is_err());
        assert_eq!(Num::Uint(1).add(Num::Int(i128::MIN)).unwrap().json(),json!({"$integer":(i128::MIN+1).to_string()}));
    }
}
