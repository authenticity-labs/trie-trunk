//! Bounded presentation: stable columns across pages, no whole-result buffering.
use crate::Result;
use serde_json::Value;
use std::{collections::BTreeSet, io::Write};

pub(super) struct Table {
    columns: Vec<String>,
    terminal: bool,
    widths: Option<Vec<usize>>,
}
impl Table {
    pub(super) fn new(columns: Vec<String>, terminal: bool) -> Self {
        Self {
            columns,
            terminal,
            widths: None,
        }
    }
    pub(super) fn page(&mut self, rows: &[Value], output: &mut impl Write) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        // Never silently discard a field if an upstream result shape changes.
        for row in rows {
            for key in row.as_object().ok_or("Expected an object row")?.keys() {
                if !self.columns.contains(key) {
                    return Err(format!(
                        "Unplanned table column {key}; use --json for heterogeneous output"
                    )
                    .into());
                }
            }
        }
        if self.widths.is_none() {
            let headers = self.columns.iter().map(|s| text(s)).collect::<Vec<_>>();
            let mut widths = headers
                .iter()
                .map(|s| s.chars().count().min(48))
                .collect::<Vec<_>>();
            if self.terminal {
                for row in rows {
                    for (i, name) in self.columns.iter().enumerate() {
                        widths[i] = widths[i].max(cell(row.get(name)).chars().count().min(48));
                    }
                }
            }
            self.widths = Some(widths);
            self.write(&headers, output)?;
        }
        for row in rows {
            let cells = self
                .columns
                .iter()
                .map(|name| cell(row.get(name)))
                .collect::<Vec<_>>();
            self.write(&cells, output)?;
        }
        Ok(())
    }
    fn write(&self, cells: &[String], output: &mut impl Write) -> Result<()> {
        for (i, value) in cells.iter().enumerate() {
            output.write_all(value.as_bytes())?;
            if i + 1 < cells.len() {
                if self.terminal {
                    let padding =
                        self.widths.as_ref().unwrap()[i].saturating_sub(value.chars().count()) + 2;
                    write!(output, "{:padding$}", "")?;
                } else {
                    output.write_all(b"\t")?;
                }
            }
        }
        output.write_all(b"\n")?;
        Ok(())
    }
}
fn text(s: &str) -> String {
    if s.is_empty() || s == "null" || s.chars().any(|c| c.is_control() || c == '\\' || c == '"') {
        serde_json::to_string(s).expect("String serialization")
    } else {
        s.into()
    }
}
fn cell(value: Option<&Value>) -> String {
    match value {
        None => "\\N".into(),
        Some(Value::String(s)) => text(s),
        Some(v)
            if v.as_object().is_some_and(|o| {
                o.len() == 1 && o.get("$integer").is_some_and(Value::is_string)
            }) =>
        {
            v["$integer"].as_str().unwrap().into()
        }
        Some(v) => v.to_string(),
    }
}

/// A report has hierarchical metadata as well as data; show every property
/// instead of projecting away coverage/errors to fabricate a flat result set.
pub(super) fn report(value: &Value, output: &mut impl Write, terminal: bool) -> Result<()> {
    let mut table = Table::new(vec!["Field".into(), "Value".into()], terminal);
    for (key, value) in value.as_object().ok_or("Expected a report object")? {
        table.page(&[serde_json::json!({"Field":key,"Value":value})], output)?;
    }
    Ok(())
}

pub(super) fn columns(
    query: &Value,
    branches: &Value,
    bootstrap: Option<&Value>,
) -> Result<Vec<String>> {
    let mut names = Vec::new();
    if let Some(fields) = query.get("select") {
        names.extend(
            fields
                .as_array()
                .ok_or("select requires names")?
                .iter()
                .map(|n| {
                    n.as_str()
                        .map(str::to_owned)
                        .ok_or("field name must be text")
                })
                .collect::<std::result::Result<Vec<_>, _>>()?,
        );
    } else {
        let bootstrap = bootstrap.ok_or("Missing registered column metadata")?;
        let selected = branches.as_array().ok_or("Missing branches")?;
        let digests = bootstrap["bindings"]
            .as_array()
            .ok_or("Missing bindings")?
            .iter()
            .filter(|b| selected.contains(&b["branchId"]))
            .filter_map(|b| b["digest"].as_str())
            .collect::<BTreeSet<_>>();
        for definition in bootstrap["definitions"]
            .as_array()
            .ok_or("Missing Definitions")?
        {
            if !definition["digest"]
                .as_str()
                .is_some_and(|d| digests.contains(d))
            {
                continue;
            }
            for field in definition["registered"]["fields"]
                .as_array()
                .ok_or("Missing Definition fields")?
            {
                names.push(
                    field["name"]
                        .as_str()
                        .ok_or("Missing field name")?
                        .to_owned(),
                );
            }
        }
    }
    if let Some(casts) = query["casts"].as_array() {
        for cast in casts {
            names.push(
                cast["alias"]
                    .as_str()
                    .ok_or("Missing cast alias")?
                    .to_owned(),
            );
        }
    }
    let mut seen = BTreeSet::new();
    names.retain(|name| seen.insert(name.clone()));
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn streaming_table_preserves_missing_null_controls_and_late_fields() {
        let mut table = Table::new(vec!["name".into(), "value".into()], false);
        let mut out = Vec::new();
        table
            .page(&[json!({"name":"a\tb\n\u{1b}[31m","value":null})], &mut out)
            .unwrap();
        table
            .page(
                &[json!({"value":{"$integer":"340282366920938463463374607431768211455"}})],
                &mut out,
            )
            .unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "name\tvalue\n\"a\\tb\\n\\u001b[31m\"\tnull\n\\N\t340282366920938463463374607431768211455\n"
        );
    }
    #[test]
    fn terminal_is_padded_without_truncation_and_empty_output_stays_empty() {
        let mut table = Table::new(vec!["a".into(), "b".into()], true);
        let mut out = Vec::new();
        table.page(&[], &mut out).unwrap();
        assert!(out.is_empty());
        table.page(&[json!({"a":1,"b":true})], &mut out).unwrap();
        table
            .page(&[json!({"a":"longer","b":false})], &mut out)
            .unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "a  b\n1  true\nlonger  false\n"
        );
    }
    #[test]
    fn schema_columns_include_fields_absent_from_the_first_page() {
        let registry = json!({"bindings":[{"branchId":1,"digest":"a"},{"branchId":2,"digest":"b"}],
            "definitions":[{"digest":"a","registered":{"fields":[{"name":"a"},{"name":"later"}]}},
            {"digest":"b","registered":{"fields":[{"name":"other"}]}}]});
        assert_eq!(
            columns(&json!({}), &json!([1]), Some(&registry)).unwrap(),
            ["a", "later"]
        );
        assert_eq!(
            columns(&json!({"select":["later","a"]}), &json!([1]), None).unwrap(),
            ["later", "a"]
        );
    }
}
