//! Reproducible workload examples. All Pack data goes through Seed ingress.
use ciborium::Value as Data;
use data_seeds::{
    planner_lab::{Experiment, PlanModel, Selection},
    *,
};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc};

struct Event {
    sequence: u64,
    score: Option<u64>,
    group: u64,
    note: String,
}
fn fixture(
    kind: &str,
    n: usize,
    definition: &Arc<SeedDefinition>,
) -> Result<(Sources, Vec<Event>), Error> {
    let mut sources = Sources::new();
    let mut events = Vec::with_capacity(n);
    for i in 0..n {
        let mixed = (i as u64)
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let group = i as u64 % 8;
        let score = match kind {
            "hot-key" => {
                if i % 10 < 9 {
                    999
                } else {
                    mixed % 1000
                }
            }
            "correlated" => {
                if group == 0 {
                    990 + mixed % 10
                } else {
                    mixed % 500
                }
            }
            _ => mixed % 1000,
        };
        events.push(Event {
            sequence: i as u64,
            score: if i % 31 == 0 { None } else { Some(score) },
            group,
            note: format!(
                "sample-{mixed:016x}-{}",
                "x".repeat(if kind == "wide-sort" { 32 + i % 224 } else { 4 })
            ),
        });
    }
    for chunk in events.chunks(256) {
        let mut pack = definition.fill(chunk.len())?;
        for event in chunk {
            let seed = definition.ingest(event, |event, seed| {
                seed.set_uint("sequence", event.sequence)?
                    .set_uint("group", event.group)?
                    .set_text_string("note", &event.note)?;
                match event.score {
                    Some(v) => {
                        seed.set_uint("score", v)?;
                    }
                    None if event.sequence % 2 == 0 => {
                        seed.set_null("score")?;
                    }
                    None => {}
                }
                Ok(())
            })?;
            pack.load(seed)?;
        }
        pack.seal()?;
        sources.pack(definition.open(pack.into_bytes()?)?);
    }
    Ok((sources, events))
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = PathBuf::from(std::env::args().nth(1).expect("output JSON path"));
    let n = 32768;
    let definition = SeedDefinition::prescribe(
        &doc::new("planner-sample")
            .field(Field::new("sequence").r#type("uint").size(8))
            .field(Field::new("score").r#type("uint").size(4))
            .field(Field::new("group").r#type("uint").size(2))
            .field(Field::new("note").r#type("tstr"))
            .sign()?,
    )?;
    let mut source_hash = Sha256::new();
    for text in [
        include_str!("../src/query.rs"),
        include_str!("../src/planner_lab.rs"),
        include_str!("../src/value.rs"),
        include_str!("../src/lib.rs"),
        include_str!("../src/store.rs"),
        include_str!("../Cargo.toml"),
        include_str!("../Cargo.lock"),
        include_str!("planner_lab.rs"),
    ] {
        source_hash.update(text.as_bytes());
    }
    let source_hash = format!("{:x}", source_hash.finalize());
    let rustc = std::process::Command::new("rustc")
        .arg("--version")
        .output()?;
    let mut reports = Vec::new();
    for scenario in ["uniform", "hot-key", "correlated", "wide-sort", "streaming"] {
        let (sources, events) = fixture(scenario, n, &definition)?;
        let threshold = if scenario == "uniform" { 995 } else { 900 };
        let mut predicate =
            Predicate::compare("score", Compare::Ge, Data::Integer(threshold.into()));
        if scenario == "correlated" {
            predicate = Predicate::And(vec![
                predicate,
                Predicate::compare("group", Compare::Eq, Data::Integer(0.into())),
            ]);
        }
        let mut q = Query::new()
            .filter(predicate)
            .select(["sequence", "score", "note"])
            .offset(5)
            .limit(25);
        let (a, b) = if scenario == "streaming" {
            (
                PlanModel::new("stream-16").batch_rows(16),
                PlanModel::new("stream-256").batch_rows(256),
            )
        } else {
            q = q.order_by(
                if scenario == "wide-sort" {
                    "note"
                } else {
                    "score"
                },
                Direction::Descending,
            );
            (
                PlanModel::new("full-sort-reference")
                    .selection(Selection::FullSortReference)
                    .max_candidates(n),
                PlanModel::new("bounded-top-k")
                    .selection(Selection::BoundedTopK)
                    .max_candidates(30),
            )
        };
        // Independent oracle: original ingress events and ordinary Rust sort,
        // with no codec reads, compiled predicates, or query Candidate comparator.
        let mut expected: Vec<_> = events
            .iter()
            .filter(|e| {
                e.score.is_some_and(|v| v >= threshold as u64)
                    && (scenario != "correlated" || e.group == 0)
            })
            .collect();
        if scenario != "streaming" {
            expected.sort_by(|a, b| {
                (if scenario == "wide-sort" {
                    b.note.cmp(&a.note)
                } else {
                    b.score.cmp(&a.score)
                })
                .then_with(|| a.sequence.cmp(&b.sequence))
            });
        }
        let expected = expected
            .into_iter()
            .skip(5)
            .take(25)
            .map(|e| {
                vec![
                    ("sequence".into(), Data::Integer(e.sequence.into())),
                    ("score".into(), Data::Integer(e.score.unwrap().into())),
                    ("note".into(), Data::Text(e.note.clone())),
                ]
            })
            .collect();
        let report=Experiment::new(scenario,q,a,b).expected(expected).warmups(2).pairs(12).seed(20261109)
            .provenance("generator","planner_lab example; deterministic affine sequence; schema and fixture bytes fingerprinted")
            .provenance("query_source_sha256",&source_hash)
            .provenance("source_scope","Seed query/example sources and Cargo manifest/lock; path dependency codec source must be recorded separately before promotion")
            .provenance("rustc",String::from_utf8_lossy(&rustc.stdout).trim())
            .provenance("target",format!("{}-{}",std::env::consts::ARCH,std::env::consts::OS))
            .provenance("build","cargo run --offline --release --features planner-lab --example planner_lab")
            .provenance("coverage","Synthetic development fixture, not a held-out or production workload")
            .run(&sources)?;
        eprintln!(
            "{scenario}: {:.3}x [{:.3}, {:.3}], verified",
            report.median_speedup, report.paired_bootstrap_95[0], report.paired_bootstrap_95[1]
        );
        reports.push(report);
    }
    std::fs::write(output, serde_json::to_vec_pretty(&reports)?)?;
    Ok(())
}
