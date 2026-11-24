//! One-shot native queries. Shell adapters move bytes; the shared node executes.
mod table;
use crate::{
    Node, Result,
    service::{REQUEST_BYTES, RESPONSE_BYTES, Session},
};
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

pub const HELP: &str = "Usage: trunk <beds|time> query [options]
  --dir PATH | --socket PATH   Exclusive local owner or existing native node
  --scope CACHE/SERVICE/RESOURCE/TYPE   Registered scope (required)
  --from TIME --to TIME        Half-open window; RFC3339 or Unix milliseconds
  --draft TOKEN               Query test data for this exact scope and saved generation
  --sql TEXT|-                SQL SELECT through the shared Rust planner
  --query JSON|-               Shared query object; '-' reads bounded stdin
  --where FIELD OP VALUE      Typed comparison; repeat for AND (VALUE is JSON)
  --select FIELD              Field projection; repeat for multiple fields
  --limit N                  Global row limit (default 100)
  --all                      Stream all matches, subject to query memory limits
  --order FIELD --desc        Optional global ordering (requires a limit)
  --page-size N              Transfer batch size, 1–1000 (default 100)
  --stats                    Write elapsed time and emitted row count to stderr
  --json                     JSONL output instead of the default table
  --help                     Print this help
Results: tables on stdout (TSV when piped); --json selects JSONL. Diagnostics on stderr.
--query uses the shared JSON query model, not SQL text.";

pub(super) enum Connection {
    Local(Session),
    Socket(PathBuf),
    Draft(Box<Connection>, String, String),
}
impl Connection {
    pub(super) fn open(dir: Option<PathBuf>, socket: Option<PathBuf>) -> Result<Self> {
        match (dir, socket) {
            (Some(path), None) => Ok(Self::Local(Session::new(Arc::new(Node::open(&path)?)))),
            (None, Some(path)) => Ok(Self::Socket(path)),
            _ => Err("Choose exactly one of --dir or --socket".into()),
        }
    }

    pub(super) fn call(&mut self, value: Value) -> Result<Value> {
        match self {
            Self::Draft(inner,scope,generation) => {
                if matches!(value["op"].as_str(),Some("fetch"|"release"|"language")){return inner.call(value);}
                inner.call(json!({"op":"draft-read","scope":scope,"generation":generation,"request":value}))
            }
            Self::Local(session) => session.request(serde_json::from_value(value)?),
            Self::Socket(path) => {
                let bytes = serde_json::to_vec(&value)?;
                if bytes.len() > REQUEST_BYTES {
                    return Err("Node request exceeds limit".into());
                }
                let mut socket = UnixStream::connect(path)?;
                socket.set_read_timeout(Some(Duration::from_secs(30)))?;
                socket.set_write_timeout(Some(Duration::from_secs(30)))?;
                socket.write_all(&(bytes.len() as u32).to_le_bytes())?;
                socket.write_all(&bytes)?;
                let mut size = [0; 4];
                socket.read_exact(&mut size)?;
                let size = u32::from_le_bytes(size) as usize;
                if size == 0 || size > RESPONSE_BYTES {
                    return Err("Invalid node response size".into());
                }
                let mut bytes = vec![0; size];
                socket.read_exact(&mut bytes)?;
                let response: Value = serde_json::from_slice(&bytes)?;
                if response["ok"] != true {
                    return Err(response["error"]
                        .as_str()
                        .unwrap_or("Native query failed")
                        .to_owned()
                        .into());
                }
                Ok(response["value"].clone())
            }
        }
    }
}
fn time(text: &str) -> Result<u64> {
    if let Ok(ms) = text.parse::<u64>() {
        return Ok(ms);
    }
    let ns = data_seeds::cast::timestamp(text, &Default::default())?;
    if ns % 1_000_000 != 0 {
        return Err("Query window requires whole milliseconds".into());
    }
    Ok(u64::try_from(ns / 1_000_000)?)
}
fn line(output: &mut impl Write, value: &Value) -> Result<()> {
    serde_json::to_writer(&mut *output, value)?;
    output.write_all(b"\n")?;
    Ok(())
}

pub fn run(
    args: &[String],
    input: impl Read,
    output: &mut impl Write,
    errors: &mut impl Write,
) -> Result<()> {
    run_output(args, input, output, errors, false)
}

pub fn run_output(
    args: &[String],
    mut input: impl Read,
    output: &mut impl Write,
    errors: &mut impl Write,
    terminal: bool,
) -> Result<()> {
    if args.iter().any(|s| s == "--help" || s == "-h") {
        writeln!(output, "{HELP}")?;
        return Ok(());
    }
    if args.len() < 2 || !matches!(args[0].as_str(), "beds" | "time") || args[1] != "query" {
        return Err(HELP.into());
    }
    let resource = &args[0];
    let (mut dir, mut socket, mut scope, mut from, mut to) = (None, None, None, None, None);
    let (mut query_text, mut limit, mut order) = (None, None, None);
    let (mut all, mut desc, mut stats) = (false, false, false);
    let mut sql_text: Option<String> = None;
    let mut draft=None;
    let mut json = false;
    let mut page_size = 100usize;
    let (mut filters, mut fields) = (Vec::new(), Vec::new());
    let mut args = args[2..].iter();
    while let Some(flag) = args.next() {
        let mut value = || {
            args.next()
                .cloned()
                .ok_or_else(|| format!("{flag} requires a value"))
        };
        match flag.as_str() {
            "--dir" => dir = Some(PathBuf::from(value()?)),
            "--socket" => socket = Some(PathBuf::from(value()?)),
            "--draft" => draft=Some(value()?),
            "--scope" => scope = Some(value()?),
            "--from" => from = Some(time(&value()?)?),
            "--to" => to = Some(time(&value()?)?),
            "--query" => query_text = Some(value()?),
            "--sql" => sql_text = Some(value()?),
            "--limit" => limit = Some(value()?.parse::<usize>()?),
            "--order" => order = Some(value()?),
            "--page-size" => page_size = value()?.parse()?,
            "--select" => fields.push(value()?),
            "--where" => {
                let field = value()?;
                let op = value()?;
                let literal = value()?;
                filters.push(
                    json!({"field":field,"op":op,"value":serde_json::from_str::<Value>(&literal)?}),
                );
            }
            "--all" => all = true,
            "--desc" => desc = true,
            "--stats" => stats = true,
            "--json" => json = true,
            _ => return Err(format!("Unknown query option {flag}").into()),
        }
    }
    if dir.is_some() == socket.is_some() {
        return Err("Choose exactly one of --dir or --socket".into());
    }
    let scope = scope
        .filter(|s: &String| !s.is_empty())
        .ok_or("--scope is required")?;
    let (from, to) = (
        from.ok_or("--from is required")?,
        to.ok_or("--to is required")?,
    );
    if from >= to {
        return Err("Query end must be after start".into());
    }
    if page_size == 0 || page_size > 1000 {
        return Err("Page size must be 1–1000".into());
    }
    if all && limit.is_some() {
        return Err("Choose --all or --limit".into());
    }
    if sql_text.is_some()
        && (resource != "beds"
            || query_text.is_some()
            || !filters.is_empty()
            || !fields.is_empty()
            || order.is_some()
            || desc)
    {
        return Err("Use --sql with beds query; put predicates, fields and ordering in SQL".into());
    }
    if sql_text.as_deref() == Some("-") {
        let mut bytes = Vec::new();
        input.by_ref().take(8193).read_to_end(&mut bytes)?;
        if bytes.len() > 8192 {
            return Err("SQL exceeds 8192 bytes".into());
        }
        sql_text = Some(String::from_utf8(bytes)?);
    }
    let mut query = if let Some(text) = query_text {
        if text == "-" {
            let mut bytes = Vec::new();
            input
                .take(REQUEST_BYTES as u64 + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() > REQUEST_BYTES {
                return Err("Query input exceeds node limit".into());
            }
            serde_json::from_slice::<Value>(&bytes)?
        } else {
            serde_json::from_str(&text)?
        }
    } else {
        json!({})
    };
    if !query.is_object() {
        return Err("Query must be a JSON object".into());
    }
    for key in query.as_object().unwrap().keys() {
        let supported = if resource == "beds" {
            matches!(
                key.as_str(),
                "where"
                    | "select"
                    | "casts"
                    | "context"
                    | "orderBy"
                    | "limit"
                    | "offset"
                    | "version"
            )
        } else {
            false
        };
        if !supported {
            return Err(format!("Unsupported {resource} query option {key}").into());
        }
    }
    if !filters.is_empty() {
        if let Some(existing) = query.get("where") {
            filters.insert(0, existing.clone());
        }
        query["where"] = json!({"and":filters});
    }
    if !fields.is_empty() {
        query["select"] = json!(fields);
    }
    if desc && order.is_none() {
        return Err("--desc requires --order".into());
    }
    if let Some(field) = order {
        query["orderBy"] = json!({"field":field,"direction":if desc{"desc"}else{"asc"}});
    }
    if resource != "beds"
        && (all
            || limit.is_some()
            || query.get("select").is_some()
            || query.get("orderBy").is_some())
    {
        return Err("Row selection and ordering options require beds query".into());
    }
    if resource == "time" && query.as_object().is_some_and(|m| !m.is_empty()) {
        return Err("Time query reports timeline counts; predicates require beds".into());
    }
    if resource == "beds" {
        if all {
            query.as_object_mut().unwrap().remove("limit");
        } else if let Some(n) = limit {
            query["limit"] = json!(n);
        } else if query.get("limit").is_none() {
            query["limit"] = json!(100);
        }
        // Validate through the same decoder before opening files or a socket.
        data_seeds::request::query(&query)?;
        if query.get("orderBy").is_some() && query.get("limit").is_none() {
            return Err("Ordered query requires a global limit".into());
        }
    }
    let started = Instant::now();
    let mut connection = Connection::open(dir, socket)?;
    if let Some(token)=draft {connection=Connection::Draft(Box::new(connection),scope.clone(),token);}
    let branches = connection.call(json!({"op":"resolve","scope":scope}))?;
    if branches.as_array().is_none_or(Vec::is_empty) {
        return Err("No registered branches match scope".into());
    }
    let mut sql_plan = None;
    let mut join_plan = None;
    let mut aliases = Vec::<(String, String)>::new();
    if let Some(text) = sql_text {
        let bootstrap = connection.call(json!({"op":"bootstrap"}))?;
        let fields = table::columns(&json!({}), &branches, Some(&bootstrap))?;
        let ast = connection
            .call(json!({"op":"language","query":{"op":"prepare","text":text,"fields":fields}}))?;
        let plan = ast["plan"].clone();
        if let Some(join) = plan.get("join") {
            join_plan = Some(join.clone());
        } else if !plan["relational"]["groupBy"].is_null() {
            sql_plan = Some(plan);
        } else {
            aliases = crate::sql::rows(&plan, &mut query)?;
            data_seeds::request::query(&query)?;
            if query.get("orderBy").is_some() && all {
                return Err("Ordered query requires a global limit".into());
            }
        }
    }
    let mut table = if !json && resource == "beds" && join_plan.is_none() {
        let bootstrap = if query.get("select").is_none() {
            Some(connection.call(json!({"op":"bootstrap"}))?)
        } else {
            None
        };
        Some(table::Table::new(
            if aliases.is_empty() {
                table::columns(&query, &branches, bootstrap.as_ref())?
            } else {
                aliases.iter().map(|(_, name)| name.clone()).collect()
            },
            terminal,
        ))
    } else {
        None
    };
    let mut cursor = None;
    let result = (|| -> Result<usize> {
        if resource != "beds" {
            let request = json!({"op":"timeline","branches":branches,"from":from,"to":to});
            let report = connection.call(request)?;
            if json {
                line(output, &report)?;
            } else {
                table::report(&report, output, terminal)?;
            }
            output.flush()?;
            return Ok(1);
        }
        if let Some(plan) = sql_plan {
            let report=connection.call(json!({"op":"summary-table","branches":branches,"from":from,"to":to,"plan":plan,"context":{}}))?;
            let columns = report["columns"]
                .as_array()
                .ok_or("Missing result columns")?;
            let names = columns
                .iter()
                .map(|v| v.as_str().ok_or("Invalid result column").map(str::to_owned))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let maximum = if all {
                usize::MAX
            } else {
                limit.unwrap_or(100)
            };
            let mut table = table::Table::new(names.clone(), terminal);
            let mut count = 0;
            for row in report["rows"]
                .as_array()
                .ok_or("Missing aggregate rows")?
                .iter()
                .take(maximum)
            {
                let values = row.as_array().ok_or("Invalid aggregate row")?;
                let row =
                    Value::Object(names.iter().cloned().zip(values.iter().cloned()).collect());
                if json {
                    line(output, &row)?;
                } else {
                    table.page(&[row], output)?;
                }
                count += 1;
            }
            output.flush()?;
            return Ok(count);
        }
        let mut page = if let Some(plan) = join_plan {
            let page=connection.call(json!({"op":"join","branches":branches,"from":from,"to":to,"plan":plan,"limit":query.get("limit"),"page_size":page_size}))?;
            if !json {
                table = Some(table::Table::new(
                    serde_json::from_value(page["columns"].clone())?,
                    terminal,
                ));
            }
            page
        } else {
            connection.call(json!({"op":"query","branches":branches,"from":from,"to":to,"query":query,"page_size":page_size}))?
        };
        let mut count = 0;
        loop {
            cursor = page["cursor"].as_u64();
            let raw = page["rows"].as_array().ok_or("Missing result rows")?;
            let renamed;
            let rows = if aliases.is_empty() {
                raw
            } else {
                renamed = raw
                    .iter()
                    .map(|row| {
                        Value::Object(
                            aliases
                                .iter()
                                .filter_map(|(field, name)| {
                                    row.get(field).map(|v| (name.clone(), v.clone()))
                                })
                                .collect(),
                        )
                    })
                    .collect::<Vec<_>>();
                &renamed
            };
            if let Some(table) = &mut table {
                table.page(rows, output)?;
            } else {
                for row in rows {
                    line(output, row)?;
                }
            }
            count += rows.len();
            output.flush()?;
            let Some(id) = cursor else {
                break;
            };
            page = connection.call(json!({"op":"fetch","cursor":id}))?;
        }
        Ok(count)
    })();
    if let Some(id) = cursor {
        let _ = connection.call(json!({"op":"release","cursor":id}));
    }
    let count = result?;
    if stats {
        line(
            errors,
            &json!({"rows":count,"elapsed_ms":started.elapsed().as_secs_f64()*1000.0}),
        )?;
    }
    Ok(())
}
