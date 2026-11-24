// Real, sealed Beds, read through the same registered reader as Node::query.
use crate::{Node, Result};
use data_seeds::{
    Compare, Direction, Field, Object, Predicate, Query, SeedDefinition, TIME_HOUR_MS as HOUR, doc,
    planner_lab::{Access, PlanModel, Ranking, Selection},
    registration::{Request, ingress::recovery},
};
use serde::Deserialize;
use serde_json::json;
use std::{collections::BTreeMap, fs, io::Write, path::Path};

#[derive(Deserialize)]
struct Event {
    time: u64,
    source_id: String,
    kind: String,
    #[serde(flatten)]
    fields: BTreeMap<String, serde_json::Value>,
}

fn replay(node: &Node, name: &str, category: &str, events: &[Event], dir: &Path) -> Result<u16> {
    let numeric = category == "status";
    let mut doc = doc::new(name)
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
        )
        .field(
            Field::new("id")
                .r#type("bytes")
                .size(10)
                .annotation("@event"),
        )
        .field(Field::new("source_id").r#type("tstr"))
        .field(if numeric {
            Field::new(category).r#type("uint").size(2)
        } else {
            Field::new(category).r#type("tstr")
        });
    let extras: &[(&str, &str)] = match name {
        "nasa" => &[("size", "uint")],
        "github" | "comments" => &[("repo", "uint"), ("issue", "uint")],
        "usgs" => &[("depth", "float"), ("magnitude", "float")],
        _ => return Err("unknown dataset".into()),
    };
    for &(key, ty) in extras {
        doc = doc.field(Field::new(key).r#type(ty).size(8));
    }
    let rx = doc.sign()?;
    let def = SeedDefinition::prescribe(&rx)?;
    let branch = node.register_request(
        &Request {
            cache: "lab".into(),
            service: "public".into(),
            resource: name.into(),
            resource_type: name.into(),
            file: format!("{name}.rx"),
            level: String::new(),
            selection: None,
            binding: BTreeMap::from([("path".into(), json!([category]))]),
        },
        rx.bytes()?,
    )?;
    let mut hours = BTreeMap::new();
    for chunk in events.chunk_by(|a, b| a.time / HOUR == b.time / HOUR) {
        let hour = chunk[0].time / HOUR * HOUR;
        let path = dir.join(format!("{name}-{hour}.spool"));
        let mut spool = std::io::BufWriter::new(fs::File::create_new(&path)?);
        for (ordinal, event) in chunk.iter().enumerate() {
            let status = if numeric {
                Some(event.kind.parse::<u16>()?)
            } else {
                None
            };
            let suffix = (ordinal as u128).to_be_bytes();
            let pill = def.ingest((), |_, s| {
                // Deterministic suffix unique within each replay hour.
                s.set_uint("time", event.time)?
                    .set_byte_string("id", &suffix[6..])?
                    .set_text_string("source_id", &event.source_id)?;
                if let Some(code) = status {
                    s.set_uint(category, code)?;
                } else {
                    s.set_text_string(category, &event.kind)?;
                }
                for &(key, ty) in extras {
                    let v = &event.fields[key];
                    if v.is_null() {
                        s.set_null(key)?;
                    } else if ty == "uint" {
                        s.set_uint(key, v.as_u64().unwrap())?;
                    } else {
                        s.set_float(key, v.as_f64().unwrap())?;
                    }
                }
                Ok(())
            })?;
            pill.cap()?;
            let bytes = pill.bytes()?;
            spool.write_all(&recovery::prefix(branch, &bytes)?)?;
            spool.write_all(&bytes)?;
        }
        spool.flush()?;
        hours.insert(hour, path);
    }
    // Use the same hourly replay, factoring and publication as file imports.
    node.runtime.lock().unwrap().import(
        node,
        branch,
        hours,
        events.last().unwrap().time + 2 * HOUR,
    )?;
    Ok(branch)
}

#[test]
#[ignore = "requires public source extracts produced by prepare.py"]
fn rank_public_events() -> Result<()> {
    let dir = std::path::PathBuf::from(std::env::var("TRUNK_PUBLIC_EVENTS")?);
    let scratch = dir.join(format!("replay-{}", std::process::id()));
    fs::create_dir(&scratch)?;
    let node = Node::install(&scratch.join("node"), "Public workloads")?;
    let mut reports = Vec::new();
    for (name, category, common, rare) in [
        ("usgs", "network", "ak", "nm"),
        ("nasa", "status", "200", "404"),
        ("github", "event_type", "PushEvent", "ReleaseEvent"),
    ] {
        let rows: Vec<Event> =
            serde_json::from_slice(&fs::read(dir.join(format!("{name}.json")))?)?;
        let branch = replay(&node, name, category, &rows, &scratch)?;
        let start = rows[0].time / HOUR * HOUR;
        let end = (rows.last().unwrap().time / HOUR + 1) * HOUR;
        let reader = node
            .reader(branch, start, end)?
            .ok_or("missing published reader")?;
        let middle = rows[rows.len() / 2].time;
        for (window, from, to) in [
            ("broad", start, end),
            ("five-minute", middle, middle + 300_000),
        ] {
            for value in [common, rare] {
                let by_category = if category == "status" {
                    Predicate::compare(category, Compare::Eq, value.parse::<u64>()?)
                } else {
                    Predicate::compare(category, Compare::Eq, value)
                };
                let query = Query::new()
                    .filter(Predicate::And(vec![
                        Predicate::compare("time", Compare::Ge, from),
                        Predicate::compare("time", Compare::Lt, to),
                        by_category,
                    ]))
                    .select(["source_id"])
                    .order_by("source_id", Direction::Descending)
                    .limit(10);
                let mut ids: Vec<_> = rows
                    .iter()
                    .filter(|r| (from..to).contains(&r.time) && r.kind == value)
                    .map(|r| &r.source_id)
                    .collect();
                let matches = ids.len();
                ids.sort_unstable_by(|a, b| b.cmp(a));
                let expected: Vec<Object> = ids
                    .into_iter()
                    .take(10)
                    .map(|v| vec![("source_id".into(), v.as_str().into())])
                    .collect();
                let plans: Vec<_> = [
                    Access::Scan,
                    Access::Time,
                    Access::Leaves,
                    Access::TimeAndLeaves,
                ]
                .into_iter()
                .map(|a| {
                    PlanModel::new(format!("{a:?}"))
                        .access(a)
                        .selection(Selection::BoundedTopK)
                        .max_candidates(10)
                })
                .collect();
                let ranked = Ranking::run(&reader, &query, &plans, &expected, 12, 42)?;
                eprintln!(
                    "{name} {window} {value} matched={matches}: {:?}",
                    ranked
                        .scores
                        .iter()
                        .map(|s| (
                            &s.plan.model.name,
                            (s.median_ns / 1000.0).round(),
                            s.samples[0].scanned_rows
                        ))
                        .collect::<Vec<_>>()
                );
                reports.push(json!({"source":name,"events":rows.len(),"window":window,"value":value,"matches":matches,"ranking":ranked}));
            }
        }
    }
    fs::write(
        std::env::var("TRUNK_PUBLIC_REPORT")?,
        serde_json::to_vec(&reports)?,
    )?;
    drop(node);
    fs::remove_dir_all(scratch)?;
    Ok(())
}

#[test]
#[ignore = "requires the GH Archive extract produced by prepare.py"]
fn join_public_events() -> Result<()> {
    use std::time::Instant;
    let dir = std::path::PathBuf::from(std::env::var("TRUNK_PUBLIC_EVENTS")?);
    let scratch = dir.join(format!("join-{}", std::process::id()));
    fs::create_dir(&scratch)?;
    let root = scratch.join("node");
    let node = Node::install(&root, "Event joins")?;
    let rows: Vec<Event> = serde_json::from_slice(&fs::read(dir.join("github.json"))?)?;
    replay(&node, "github", "event_type", &rows, &scratch)?;
    let key = |r: &Event| {
        Some((
            r.fields.get("repo")?.as_u64()?,
            r.fields.get("issue")?.as_u64()?,
        ))
    };
    let mut inner = vec![];
    let mut outer = vec![];
    // Independent nested loop over the source file, preserving repeated pairs.
    for a in rows.iter().filter(|r| r.kind == "IssuesEvent") {
        let mut found = false;
        for b in rows.iter().filter(|r| r.kind == "IssueCommentEvent") {
            if key(a).is_some() && key(a) == key(b) {
                found = true;
                inner.push((a.source_id.clone(), Some(b.source_id.clone())));
                outer.push((a.source_id.clone(), Some(b.source_id.clone())));
            }
        }
        if !found {
            outer.push((a.source_id.clone(), None));
        }
    }
    inner.sort();
    outer.sort();
    assert_eq!((inner.len(), outer.len()), (205, 588));
    let from = rows[0].time.to_string();
    let to = (rows.last().unwrap().time + 1).to_string();
    let comments = rows
        .into_iter()
        .filter(|r| r.kind == "IssueCommentEvent")
        .collect::<Vec<_>>();
    replay(&node, "comments", "event_type", &comments, &scratch)?;
    drop(node);
    for (kind, expected) in [("INNER", &inner), ("LEFT", &outer)] {
        for source in ["events", "lab.public.comments.comments"] {
            let sql = format!(
                "SELECT i.source_id AS activity, c.source_id AS comment FROM events i {kind} JOIN {source} c ON c.repo=i.repo AND i.issue=c.issue AND c.event_type='IssueCommentEvent' WHERE i.event_type='IssuesEvent'"
            );
            let args = [
                "beds",
                "query",
                "--dir",
                root.to_str().unwrap(),
                "--scope",
                "lab/public/github/github",
                "--from",
                &from,
                "--to",
                &to,
                "--sql",
                &sql,
                "--json",
                "--all",
                "--page-size",
                "7",
            ]
            .map(str::to_string);
            let mut samples = Vec::new();
            for round in 0..8 {
                let mut output = Vec::new();
                let start = Instant::now();
                crate::cli::run(&args, &[][..], &mut output, &mut Vec::new())?;
                let elapsed = start.elapsed();
                let mut actual = vec![];
                for line in std::str::from_utf8(&output)?.lines() {
                    let r: serde_json::Value = serde_json::from_str(line)?;
                    assert!(r.get("comment").is_some());
                    actual.push((
                        r["activity"].as_str().unwrap().to_owned(),
                        r["comment"].as_str().map(str::to_owned),
                    ));
                }
                actual.sort();
                assert_eq!(&actual, expected);
                if round != 0 {
                    samples.push(elapsed);
                }
            }
            samples.sort_unstable();
            eprintln!(
                "{kind} JOIN {source}: {} rows, median {:?} (7 runs including node open, CLI and JSONL)",
                expected.len(),
                samples[3]
            );
        }
    }
    fs::remove_dir_all(scratch)?;
    Ok(())
}

#[test]
#[ignore = "requires psql and the GH Archive extract produced by prepare.py"]
fn postgres_public_events() -> Result<()> {
    use std::{io::Write, process::{Command, Stdio}, sync::Arc};
    let dir = std::path::PathBuf::from(std::env::var("TRUNK_PUBLIC_EVENTS")?);
    let scratch = dir.join(format!("postgres-{}", std::process::id()));
    fs::create_dir(&scratch)?;
    let node = Arc::new(Node::install(&scratch.join("node"), "PostgreSQL")?);
    let events: Vec<Event> = serde_json::from_slice(&fs::read(dir.join("github.json"))?)?;
    replay(&node, "github", "event_type", &events, &scratch)?;
    let mut breakdown = std::collections::BTreeMap::<String, usize>::new();
    for event in &events { *breakdown.entry(event.kind.clone()).or_default() += 1; }
    let total = events.len();
    let comments = events.into_iter().filter(|r| r.kind == "IssueCommentEvent").collect::<Vec<_>>();
    replay(&node, "comments", "event_type", &comments, &scratch)?;
    let service = crate::postgres::Service::start(node, "127.0.0.1:0", "fixture".into())?;
    let run = |sql: &str, password: &str| -> Result<(bool,String,String)> {
        let mut child = Command::new("psql")
            .args(["-X", "-qAtw", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1", "-p",
                &service.address.port().to_string(), "-U", "trunk", "-d", "lab"])
            .env("PGPASSWORD", password).env("PGSSLMODE","disable")
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()?;
        child.stdin.take().unwrap().write_all(sql.as_bytes())?;
        let out = child.wait_with_output()?;
        Ok((out.status.success(), String::from_utf8(out.stdout)?, String::from_utf8(out.stderr)?))
    };
    let read = |sql: &str| -> Result<String> {
        let (ok, stdout, stderr) = run(sql, "fixture")?;
        if !ok {return Err(format!("{sql}\n{stderr}").into());}
        Ok(stdout.trim_end().into())
    };
    assert!(!run("select 1;", "incorrect")?.0);
    assert!(!run("delete from public.\"github.github\";", "fixture")?.0);
    assert_eq!(read("SELECT CURRENT_SCHEMA() AS schema;")?, "public");
    assert_eq!(read("SELECT datname FROM pg_database WHERE datistemplate = $1\n\\bind false\n\\g\n")?, "lab");
    assert_eq!(read("SELECT t.table_name FROM information_schema.tables AS t JOIN pg_class AS pc ON t.table_name=pc.relname AND quote_ident(t.table_schema)=pc.relnamespace::regnamespace::text ORDER BY t.table_name;")?, "comments.comments\ngithub.github");
    assert_eq!(read("SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 AND ordinal_position=1\n\\bind public github.github\n\\g\n")?, "time");
    assert_eq!(read(r#"SELECT COUNT(*) FROM public."github.github";"#)?, total.to_string());
    let counts = read(r#"SELECT event_type, COUNT(*) FROM public."github.github" GROUP BY event_type;"#)?;
    let expected = breakdown.into_iter().map(|(kind,n)|format!("{kind}|{n}")).collect::<Vec<_>>();
    let mut groups = counts.lines().collect::<Vec<_>>();
    groups.sort_unstable();
    assert_eq!(groups, expected);
    assert_eq!(read(r#"SELECT COUNT(*), SUM(repo) FROM public."github.github" WHERE event_type='no such event';"#)?, "0|");
    let raw = read("SELECT source_id FROM public.\"github.github\" WHERE event_type=$1 LIMIT $2\n\\bind IssuesEvent 3\n\\g\n")?;
    assert_eq!(raw.lines().count(), 3);
    for (kind, count) in [("INNER", 205), ("LEFT", 588)] {
        let sql = format!("SELECT i.source_id AS activity, c.source_id AS comment FROM public.\"github.github\" i {kind} JOIN public.\"comments.comments\" c ON c.repo=i.repo AND i.issue=c.issue AND c.event_type='IssueCommentEvent' WHERE i.event_type='IssuesEvent';");
        let rows = read(&sql)?;
        assert_eq!(rows.lines().count(), count);
        assert_eq!(rows.lines().filter(|r| r.ends_with('|')).count(), count-205);
    }
    drop(service);
    fs::remove_dir_all(scratch)?;
    Ok(())
}
