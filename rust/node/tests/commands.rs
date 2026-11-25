use data_seeds::{Field, SeedDefinition, TIME_HOUR_MS as HOUR, doc};
use std::{io::Cursor, sync::Arc};
use trunk_node::{Node, Storage};

fn command(args: &[String], bytes: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut errors = Vec::new();
    trunk_node::command::run(args, Cursor::new(bytes), &mut out, &mut errors, false)
        .map_err(|e| e.to_string())?;
    assert!(errors.is_empty());
    Ok(out)
}

#[test]
fn cli_registration_ingestion_queries_and_recovery_share_the_node() {
    for storage in [Storage::Browse, Storage::Pack] {
        for socket in [false, true] {
            let root = std::env::temp_dir().join(format!(
                "trunk-cli-{storage:?}-{socket}-{}",
                std::process::id()
            ));
            struct Clean(std::path::PathBuf);
            impl Drop for Clean {
                fn drop(&mut self) {
                    let _ = std::fs::remove_dir_all(&self.0);
                }
            }
            let _clean = Clean(root.clone());
            let node = Arc::new(Node::install_with(&root, "CLI", storage).unwrap());
            let endpoint = root.join("node.sock");
            let service = socket
                .then(|| trunk_node::service::Service::start(node.clone(), &endpoint).unwrap());
            drop(node);
            let target = if socket {
                ["--socket".into(), endpoint.to_string_lossy().into_owned()]
            } else {
                ["--dir".into(), root.to_string_lossy().into_owned()]
            };
            let args = |prefix: &[&str], options: &[&str]| {
                prefix
                    .iter()
                    .map(|s| s.to_string())
                    .chain(target.clone())
                    .chain(options.iter().map(|s| s.to_string()))
                    .collect::<Vec<_>>()
            };
            let document = doc::new("sample")
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
                .field(
                    Field::new("value")
                        .r#type("uint")
                        .size(3)
                        .annotation("@duration")
                        .annotation("#summ(numeric)"),
                )
                .sign()
                .unwrap();
            let register = args(
                &["definitions", "register"],
                &[
                    "--cache",
                    "local",
                    "--service",
                    "lab",
                    "--resource",
                    "readings",
                    "--type",
                    "sample",
                    "--file",
                    "-",
                ],
            );
            let registered = command(&register, document.bytes().unwrap()).unwrap();
            let branch =
                serde_json::from_slice::<serde_json::Value>(&registered).unwrap()["branch"]
                    .as_u64()
                    .unwrap();
            assert_eq!(
                registered,
                command(&register, document.bytes().unwrap()).unwrap()
            );
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let hour = now / HOUR * HOUR;
            let rx = SeedDefinition::prescribe(&document).unwrap();
            let seed = rx
                .ingest((), |_, s| {
                    s.set_uint("time", now)?
                        .set_byte_string("id", &[1; 10])?
                        .set_uint("value", 42)?;
                    Ok(())
                })
                .unwrap();
            seed.cap().unwrap();
            let ingest = args(
                &["seeds", "load"],
                &["--branch", &branch.to_string(), "--file", "-"],
            );
            let ack = command(&ingest, &seed.bytes().unwrap()).unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&ack).unwrap()["durable"],
                true
            );
            let query = args(
                &["search"],
                &[
                    "--scope",
                    "local/lab/readings/sample",
                    "--from",
                    &hour.to_string(),
                    "--to",
                    &(hour + HOUR).to_string(),
                    "--sql",
                    "SELECT value FROM events WHERE value >= 40",
                    "--json",
                ],
            );
            assert_eq!(command(&query, &[]).unwrap(), b"{\"value\":42}\n");
            let ms = format!("{now:012x}");
            let uuid = format!("{}-{}-7101-8101-010101010101", &ms[..8], &ms[8..]);
            let mut by_id = query.clone();
            let sql = by_id.iter().position(|arg| arg == "--sql").unwrap() + 1;
            by_id[sql] = format!("SELECT value FROM events WHERE id = '{uuid}'");
            assert_eq!(command(&by_id, &[]).unwrap(), b"{\"value\":42}\n");
            by_id[sql] = format!("SELECT @duration AS value FROM events WHERE @event = '{uuid}' AND @time = {now}");
            assert_eq!(command(&by_id, &[]).unwrap(), b"{\"value\":42}\n");
            let mut totals = query.clone();
            totals[sql]="SELECT COUNT(*) AS n, SUM(value) AS s, MIN(value) AS lo, MAX(value) AS hi, AVG(value) AS mean FROM events WHERE value >= 40".into();
            let check = |args: &[String], n: &str, empty: bool| {
                let row: serde_json::Value =
                    serde_json::from_slice(&command(args, &[]).unwrap()).unwrap();
                assert_eq!(row["n"]["$integer"], n, "{row}");
                for k in ["s", "lo", "hi"] {
                    if empty {
                        assert!(row[k].is_null());
                    } else {
                        assert_eq!(row[k]["$integer"], "42");
                    }
                }
                if empty {
                    assert!(row["mean"].is_null());
                } else {
                    assert_eq!(row["mean"], 42.0);
                }
            };
            check(&totals, "1", false);
            let mut annotated_totals = totals.clone();
            annotated_totals[sql] = totals[sql].replace("value", "@duration");
            check(&annotated_totals, "1", false);
            let since = totals.iter().position(|a| a == "--from").unwrap() + 1;
            let until = totals.iter().position(|a| a == "--to").unwrap() + 1;
            totals[since] = now.to_string();
            totals[until] = (now + 1).to_string();
            check(&totals, "1", false);
            totals[since] = (now + 1).to_string();
            totals[until] = (now + 2).to_string();
            check(&totals, "0", true);
            assert!(command(&ingest, b"not a PIL").is_err());
            assert!(command(&register, b"{}").is_err());
            assert_eq!(command(&query, &[]).unwrap(), b"{\"value\":42}\n");
            drop(service);
            let recovered = Node::open(&root).unwrap();
            assert_eq!(
                recovered
                    .timeline(&[branch as u16], hour, hour + HOUR)
                    .unwrap()["count"],
                1
            );
            assert_eq!(
                recovered
                    .summaries(
                        &[branch as u16],
                        hour,
                        hour + HOUR,
                        300_000,
                        "value",
                        &data_seeds::Predicate::All
                    )
                    .unwrap()["summary"]["count"],
                1
            );
        }
    }
}
