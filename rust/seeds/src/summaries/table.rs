//! Summary-backed relational execution. Only plans, coverage and final results
//! cross the host boundary; aggregate rows remain inside Rust.
use super::{SummaryPlan, SummaryReader, SummaryWindow};
use crate::relational::aggregate::{Contribution, Period, scalar};
use crate::{
    Error, error,
    relational::{Num, Reducer, Row},
};
use ciborium::Value as Data;
use serde::Deserialize;
use serde_json::{Value as Json, json};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Deserialize)]
pub struct Coverage {
    pub source: String,
    pub definition: String,
    pub time: u64,
    pub count: u64,
}
pub struct SummaryTable {
    reducer: Option<Reducer>,
    fields: Option<Vec<String>>,
    summary_source: bool,
    totals: bool,
    predicate: crate::Predicate,
    context: crate::cast::Context,
    scan: BTreeMap<(String, String), Vec<(u64, u64)>>,
    scanning: bool,
    expected: BTreeMap<(String, String, u64), (u64, bool)>,
    window: SummaryWindow,
    failed: bool,
}
impl SummaryTable {
    pub fn new(plan: &Json, coverage: Vec<Coverage>, from: u64, to: u64) -> Result<Self, Error> {
        let mut expected = BTreeMap::new();
        for row in coverage {
            if expected
                .insert((row.source, row.definition, row.time), (row.count, false))
                .is_some()
            {
                return Err(error("duplicate summary coverage hour"));
            }
        }
        let fields = if plan["relational"]["selection"]
            .as_array()
            .is_some_and(Vec::is_empty)
        {
            None
        } else {
            let mut fields = BTreeSet::new();
            fn visit(v: &Json, out: &mut BTreeSet<String>) {
                match v {
                    Json::Object(map) => {
                        if let Some(name) = map.get("field").and_then(Json::as_str) {
                            out.insert(name.to_owned());
                        }
                        for v in map.values() {
                            visit(v, out);
                        }
                    }
                    Json::Array(a) => {
                        for v in a {
                            visit(v, out)
                        }
                    }
                    _ => {}
                }
            }
            visit(plan, &mut fields);
            for name in plan["relational"]["groupBy"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Json::as_str)
            {
                fields.insert(name.to_owned());
            }
            Some(
                fields
                    .into_iter()
                    .filter(|n| n != "bucket" && n != "count")
                    .collect(),
            )
        };
        let totals = plan["source"] != "summaries" && plan["relational"]["groupBy"].is_array();
        let mut predicates = Vec::new();
        if totals {
            for f in plan["filters"].as_array().into_iter().flatten() {
                predicates.push(crate::request::predicate(
                    &crate::request::language_predicate(&json!({"kind":"filter","filter":f}))?,
                )?);
            }
            if let Some(p) = plan.get("predicate") {
                predicates.push(crate::request::predicate(
                    &crate::request::language_predicate(p)?,
                )?);
            }
        }
        let predicate = match predicates.len() {
            0 => crate::Predicate::All,
            1 => predicates.pop().unwrap(),
            _ => crate::Predicate::And(predicates),
        };
        Ok(Self {
            totals,
            predicate,
            context: Default::default(),
            scan: BTreeMap::new(),
            scanning: false,
            reducer: Some(Reducer::new(plan).map_err(error)?),
            fields,
            summary_source: plan["source"] == "summaries",
            expected,
            window: SummaryWindow::new(from..to)?,
            failed: false,
        })
    }
    pub fn context(mut self, context: crate::cast::Context) -> Self {
        self.context = context.clone();
        self.reducer = self.reducer.take().map(|r| r.context(context));
        self
    }
    pub fn memory_budget(mut self, budget: crate::MemoryBudget) -> Self {
        self.reducer = self.reducer.take().map(|r| r.memory_budget(budget));
        self
    }
    pub fn read(
        &mut self,
        reader: &mut SummaryReader,
        source: &str,
        definition: &str,
    ) -> Result<(), Error> {
        if self.failed || self.scanning || self.reducer.is_none() {
            return Err(error("summary query is no longer writable"));
        }
        self.failed = true;
        let key = (source.to_owned(), definition.to_owned());
        let mut selected = BTreeSet::new();
        for (time, count) in reader.hour_counts()? {
            let Some((expected, seen)) =
                self.expected.get_mut(&(key.0.clone(), key.1.clone(), time))
            else {
                continue;
            };
            if *seen {
                return Err(error("Duplicate summary hour"));
            }
            if *expected != count {
                if self.totals {
                    continue;
                }
                return Err(error(
                    "Summary aggregates do not cover the selected timeline",
                ));
            }
            *seen = true;
            selected.insert(time);
        }
        let extent = reader.extent()?;
        let covered = self.window.covered();
        let from = extent.start.max(covered.start);
        let to = extent.end.min(covered.end);
        if from < to {
            let reducer = self.reducer.as_mut().unwrap();
            let summary = self.summary_source;
            let mut bindings = std::collections::HashMap::new();
            let preview = if self.totals && !matches!(self.predicate, crate::Predicate::All) {
                let proof = reader.preview_with(
                    &SummaryWindow::new(from..to)?,
                    &self.predicate,
                    &self.context,
                )?;
                Some((
                    proof.predicate_complete,
                    proof
                        .periods
                        .into_iter()
                        .map(|p| (p.time, p))
                        .collect::<BTreeMap<_, _>>(),
                ))
            } else {
                None
            };
            let window = self.window.requested();
            reader.views(&SummaryWindow::new(from..to)?, |plan, time, count, row| {
                if !selected.contains(&(time / 3_600_000 * 3_600_000)) {
                    return Ok(());
                }
                if !self.totals {
                    reducer.bind(plan.source()).map_err(error)?;
                }
                let names = bindings.entry(plan.binding_key()).or_insert_with(|| {
                    self.fields
                        .as_ref()
                        .map(|names| {
                            names
                                .iter()
                                .map(|name| (name.clone(), Lookup::new(plan, name)))
                                .collect::<BTreeMap<_, _>>()
                        })
                        .unwrap_or_default()
                });
                let view = View {
                    plan,
                    row,
                    time,
                    count,
                    summary,
                    names,
                };
                if !self.totals {
                    return reducer.push_row(&view).map_err(error);
                }
                if count == 0 {
                    return Ok(());
                }
                let end = time + super::SUMMARY_PERIOD_MS;
                let exact = match &preview {
                    None => Some(count),
                    Some((complete, periods)) => periods.get(&time).and_then(|p| {
                        if p.complete && p.count.upper == 0 {
                            Some(0)
                        } else if *complete && p.complete && p.count.is_exact() {
                            Some(p.count.upper)
                        } else {
                            None
                        }
                    }),
                };
                if exact == Some(0) {
                    return Ok(());
                }
                if time >= window.start && end <= window.end {
                    if let Some(n) = exact {
                        if reducer.period_total(&view, n).map_err(error)? {
                            return Ok(());
                        }
                    }
                }
                self.scan
                    .entry(key.clone())
                    .or_default()
                    .push((time.max(window.start), end.min(window.end)));
                Ok(())
            })?;
        }
        self.failed = false;
        Ok(())
    }
    /// Freeze summary coverage before opening event cursors. Each range belongs
    /// to one registered storage generation and uses half-open event time.
    pub fn remaining(&mut self) -> Result<Vec<(String, String, Vec<(u64, u64)>)>, Error> {
        if self.failed || self.scanning || !self.totals {
            return Err(error("invalid aggregate scan transition"));
        }
        self.scanning = true;
        let window = self.window.requested();
        for ((source, definition, hour), (count, seen)) in &self.expected {
            if !seen && *count != 0 {
                let from = (*hour).max(window.start);
                let to = hour.saturating_add(crate::TIME_HOUR_MS).min(window.end);
                if from < to {
                    self.scan
                        .entry((source.clone(), definition.clone()))
                        .or_default()
                        .push((from, to));
                }
            }
        }
        let mut output = Vec::new();
        for ((source, definition), mut spans) in std::mem::take(&mut self.scan) {
            spans.sort_unstable();
            let mut ranges = Vec::<(u64, u64)>::new();
            for (a, b) in spans {
                if let Some(last) = ranges.last_mut().filter(|last| last.1 >= a) {
                    last.1 = last.1.max(b);
                } else {
                    ranges.push((a, b));
                }
            }
            output.push((source, definition, ranges));
        }
        Ok(output)
    }
    pub fn event_query(&self) -> crate::Query {
        crate::Query::new()
            .filter(self.predicate.clone())
            .context(self.context.clone())
            .select(
                self.reducer
                    .as_ref()
                    .into_iter()
                    .flat_map(|r| r.selected_fields())
                    .collect::<BTreeSet<_>>(),
            )
    }
    pub fn events(&mut self, batch: &crate::Batch) -> Result<(), Error> {
        if !self.scanning || self.failed {
            return Err(error("aggregate scan is not active"));
        }
        self.failed = true;
        self.reducer
            .as_mut()
            .ok_or_else(|| error("aggregate finished"))?
            .events(batch)
            .map_err(error)?;
        self.failed = false;
        Ok(())
    }
    pub fn finish(&mut self) -> Result<Json, Error> {
        if self.failed {
            return Err(error("summary query failed"));
        }
        if !self.scanning
            && (!self.scan.is_empty()
                || self
                    .expected
                    .values()
                    .any(|(count, seen)| *count > 0 && !*seen))
        {
            return Err(error(
                "Summary aggregates do not cover the selected timeline",
            ));
        }
        self.reducer
            .take()
            .ok_or_else(|| error("summary query already finished"))?
            .finish()
            .map_err(error)
    }
}

#[derive(Clone)]
enum Lookup {
    Field(usize),
    Member(usize, String),
    Missing,
}
impl Lookup {
    fn new(plan: &SummaryPlan, name: &str) -> Self {
        if let Some((base, member)) = name.rsplit_once('.') {
            if let Some(index) = plan.field_index(base) {
                return Self::Member(index, member.into());
            }
        }
        plan.field_index(name)
            .map(Self::Field)
            .unwrap_or(Self::Missing)
    }
}
struct View<'a, 'b> {
    plan: &'a SummaryPlan,
    row: &'a pil_codec::api::PillView<'b>,
    time: u64,
    count: u64,
    summary: bool,
    names: &'a BTreeMap<String, Lookup>,
}
impl View<'_, '_> {
    fn lookup(&self, name: &str) -> Lookup {
        self.names
            .get(name)
            .cloned()
            .unwrap_or_else(|| Lookup::new(self.plan, name))
    }
    fn counts(&self, index: usize) -> Result<(u64, u64), String> {
        let (count, nulls) = self
            .plan
            .counts(self.row, index)
            .map_err(|e| e.to_string())?;
        if count.checked_add(nulls).is_none_or(|n| n > self.count) {
            return Err("summary field counts exceed event count".into());
        }
        Ok((count, nulls))
    }
    fn member(&self, index: usize, name: &str) -> Result<Option<Json>, String> {
        if let Some(n) = self
            .plan
            .number_member(self.row, index, name)
            .map_err(|e| e.to_string())?
        {
            return Ok(Some(n.map(Num::json).unwrap_or(Json::Null)));
        }
        let value = self
            .plan
            .project_field(self.row, index)
            .map_err(|e| e.to_string())?;
        if let Data::Map(fields) = value {
            fields
                .into_iter()
                .find(|(k, _)| k.as_text() == Some(name))
                .map(|(_, v)| scalar(v).map_err(|e| e.to_string()))
                .transpose()
        } else {
            Ok(Some(Json::Null))
        }
    }
    fn measure(&self, index: usize, name: &str) -> Result<Option<Num>, String> {
        if let Some(value) = self
            .plan
            .number_member(self.row, index, name)
            .map_err(|e| e.to_string())?
        {
            return Ok(value);
        }
        self.member(index, name)?
            .filter(|v| !v.is_null())
            .as_ref()
            .map(Num::read)
            .transpose()
    }
}
impl Row for View<'_, '_> {
    fn count(&self) -> Result<u64, String> {
        Ok(self.count)
    }
    fn values(&self) -> Result<Json, String> {
        let mut values = serde_json::Map::new();
        values.insert("bucket".into(), self.time.into());
        values.insert("count".into(), Num::Uint(self.count.into()).json());
        for (name, value) in self.plan.object(self.row).map_err(|e| e.to_string())? {
            match scalar(value).map_err(|e| e.to_string())? {
                Json::Object(fields) => {
                    for (member, value) in fields {
                        values.insert(format!("{name}.{member}"), value);
                    }
                }
                Json::Null => {
                    values.insert(name, Json::Null);
                }
                _ => return Err("summary class must be an object".into()),
            }
        }
        Ok(Json::Object(values))
    }
    fn scalar(&self, name: &str) -> Result<Option<Json>, String> {
        if self.summary {
            match name {
                "bucket" => return Ok(Some(self.time.into())),
                "count" => return Ok(Some(Num::Uint(self.count.into()).json())),
                _ => {}
            }
        }
        if self.summary {
            match self.lookup(name) {
                Lookup::Member(index, member) => {
                    return self
                        .member(index, &member)?
                        .map(Some)
                        .ok_or_else(|| format!("Summary does not retain field {name}"));
                }
                Lookup::Field(index)
                    if self
                        .plan
                        .is_null(self.row, index)
                        .map_err(|e| e.to_string())? =>
                {
                    return Ok(Some(Json::Null));
                }
                _ => {}
            }
        }
        self.key(name)?
            .ok_or_else(|| format!("Summary cannot separate values of {name} within a bucket"))
    }
    fn number(&self, name: &str) -> Result<Option<Num>, String> {
        if self.summary {
            match name {
                "bucket" => return Ok(Some(Num::Uint(self.time.into()))),
                "count" => return Ok(Some(Num::Uint(self.count.into()))),
                _ => {}
            }
            if let Lookup::Member(index, member) = self.lookup(name) {
                if let Some(value) = self
                    .plan
                    .number_member(self.row, index, &member)
                    .map_err(|e| e.to_string())?
                {
                    return Ok(value);
                }
            }
        }
        self.scalar(name)?
            .filter(|v| !v.is_null())
            .as_ref()
            .map(Num::read)
            .transpose()
    }
    fn aggregate(
        &self,
        name: &str,
        op: &str,
    ) -> Result<(u64, Option<Num>, Option<Num>, Option<Num>), String> {
        self.contribution(name, op)?
            .ok_or_else(|| format!("Summary has no {op} aggregate for {name}"))
    }
}

impl Period for View<'_, '_> {
    fn key(&self, name: &str) -> Result<Option<Option<Json>>, String> {
        let Some(index) = self.plan.field_index(name) else {
            return Ok(None);
        };
        if self
            .plan
            .is_null(self.row, index)
            .map_err(|e| e.to_string())?
        {
            return Ok(None);
        }
        let (count, nulls) = self.counts(index)?;
        if count == 0 {
            return Ok(if nulls == self.count {
                Some(Some(Json::Null))
            } else if nulls == 0 {
                Some(None)
            } else {
                None
            });
        }
        if count == self.count {
            let min = self.member(index, "min")?;
            if min.is_some() && min == self.member(index, "max")? {
                return Ok(Some(min));
            }
        }
        Ok(None)
    }

    fn contribution(&self, name: &str, op: &str) -> Result<Option<Contribution>, String> {
        let Some(index) = self.plan.field_index(name) else {
            return Ok(None);
        };
        if self
            .plan
            .is_null(self.row, index)
            .map_err(|e| e.to_string())?
        {
            return Ok(None);
        }
        let (count, _) = self.counts(index)?;
        let mut part = (count, None, None, None);
        if count > 0 && op != "count" {
            let (member, slot) = match op {
                "min" => ("min", &mut part.1),
                "max" => ("max", &mut part.2),
                "avg" | "sum" => ("sum", &mut part.3),
                _ => return Err(format!("Unsupported aggregate {op}")),
            };
            let Some(n) = self.measure(index, member)? else {
                return Ok(None);
            };
            *slot = Some(n);
        }
        Ok(Some(part))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Field, SeedDefinition, doc, summaries::*};

    // Retained only as an independent oracle for the prior object adapter.
    fn materialized(time: u64, count: u64, fields: crate::Object, summary: bool) -> Json {
        let mut values = serde_json::Map::new();
        let mut measures = serde_json::Map::new();
        if summary {
            values.insert("bucket".into(), time.into());
            values.insert("count".into(), json!({"$integer":count.to_string()}));
        }
        for (name, field) in fields {
            if field == Data::Null {
                if summary {
                    values.insert(name, Json::Null);
                }
                continue;
            }
            let Json::Object(fields) = scalar(field).unwrap() else {
                panic!()
            };
            let mut measure = serde_json::Map::new();
            for (key, value) in fields {
                if summary {
                    values.insert(format!("{name}.{key}"), value.clone());
                }
                let value = if key == "count" || key == "null_count" {
                    value.get("$integer").cloned().unwrap_or(value)
                } else {
                    value
                };
                measure.insert(
                    if key == "null_count" {
                        "nulls".into()
                    } else {
                        key
                    },
                    value,
                );
            }
            measures.insert(name, Json::Object(measure));
        }
        json!({"count":count,"values":values,"measures":measures})
    }
    #[test]
    fn typed_members_match_object_reduction_across_classes_and_widths() {
        let document = doc::new("reduction")
            .field(
                Field::new("wide")
                    .r#type("uint")
                    .size(16)
                    .annotation("#summ(numeric)"),
            )
            .field(
                Field::new("signed")
                    .r#type("int")
                    .size(16)
                    .annotation("#summ(numeric)"),
            )
            .field(
                Field::new("float")
                    .r#type("float")
                    .size(8)
                    .annotation("#summ(numeric)"),
            )
            .field(
                Field::new("time")
                    .r#type("uint")
                    .size(6)
                    .annotation("#summ(temporal)"),
            )
            .field(
                Field::new("flag")
                    .r#type("bool")
                    .annotation("#summ(boolean)"),
            )
            .field(
                Field::new("status")
                    .closed_enum("uint", [200u32, 404, 500].map(Into::into))
                    .annotation("#summ(categorical)"),
            )
            .field(
                Field::new("duration.max")
                    .r#type("uint")
                    .size(3)
                    .annotation("#summ(numeric)"),
            )
            .sign()
            .unwrap();
        let source = SeedDefinition::prescribe(&document).unwrap();
        let plan = source.summaries(288).unwrap();
        let mut day = plan.accumulator(0).unwrap();
        for i in 0..8u32 {
            day.ingest(i as u64 * 300_000, i, |n, s| {
                s.set_uint("wide", (1u128 << 100) + n as u128)?;
                s.set_int("signed", -(1i128 << 100) + n as i128)?;
                if n % 3 == 0 {
                    s.set_null("float")?;
                } else if n % 3 == 1 {
                    s.set_float("float", n as f64 + 0.25)?;
                }
                s.set_uint("time", 1000 + n)?;
                s.set_bool("flag", n % 2 == 0)?;
                s.set_enum("status", [200u32, 404, 500][n as usize % 3].into())?;
                s.set_uint("duration.max", n)?;
                Ok(())
            })
            .unwrap();
        }
        day.complete(day.end()).unwrap();
        let bytes = day.seal().unwrap().bytes().to_vec();
        let mut reader = SummaryReader::pack(plan.layout().clone(), bytes, 0).unwrap();
        let projections = [
            "bucket",
            "count",
            "wide.min",
            "wide.max",
            "wide.sum",
            "wide.mean",
            "signed.sum",
            "float.sum",
            "float.mean",
            "float.count",
            "float.null_count",
            "float.absent_count",
            "time.earliest",
            "time.latest",
            "flag.true_count",
            "flag.false_count",
            "flag.true_proportion",
            "status.values",
            "status.distinct",
            "duration.max.sum",
        ];
        let cases = [
            json!({"source":"summaries","relational":{"selection":[]}}),
            json!({"source":"summaries","relational":{"selection":projections.iter().map(|f|json!({"field":f,"alias":f})).collect::<Vec<_>>()}}),
            json!({"source":"summaries","relational":{"selection":[{"field":"wide.sum","function":"sum","alias":"sum"},{"field":"signed.min","function":"min","alias":"min"},{"field":"flag.true_count","function":"sum","alias":"true"}],"groupBy":[]}}),
            json!({"source":"events","relational":{"selection":[{"function":"count","alias":"count"},{"field":"wide","function":"sum","alias":"wide"},{"field":"float","function":"avg","alias":"float"}],"groupBy":[]}}),
            json!({"source":"summaries","predicate":{"kind":"filter","filter":{"field":"float.count","op":">","value":{"kind":"number","value":"0"}}},"relational":{"selection":[{"field":"float.mean","alias":"mean"}]}}),
        ];
        for query in cases {
            let summary = query["source"] == "summaries";
            let mut reference = Reducer::new(&query).unwrap();
            reader
                .rows(
                    &SummaryWindow::new(0..86_400_000).unwrap(),
                    |time, count, fields| {
                        reference
                            .push(&materialized(time, count, fields, summary))
                            .map_err(error)
                    },
                )
                .unwrap();
            let coverage = reader
                .hour_counts()
                .unwrap()
                .into_iter()
                .map(|(time, count)| Coverage {
                    source: "test".into(),
                    definition: "rx".into(),
                    time,
                    count,
                })
                .collect();
            let mut table = SummaryTable::new(&query, coverage, 0, 86_400_000).unwrap();
            table.read(&mut reader, "test", "rx").unwrap();
            assert_eq!(
                table.finish().unwrap(),
                reference.finish().unwrap(),
                "{query}"
            );
        }
    }
}
