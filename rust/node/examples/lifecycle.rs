//! Same typed input through the shared ingestion, commit and query owner in both
//! storage configurations. Not a byte-append or pre-encoded-input benchmark.
use data_seeds::{Compare, Field, Predicate, Query, SeedDefinition, TIME_HOUR_MS as HOUR, doc};
use serde_json::json;
use std::{path::Path, time::Instant};
use trunk_node::{Node, RegistrationTarget, Result, Storage};

fn elapsed(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}
fn run(parent: &Path, mode: Storage, count: u64) -> Result<serde_json::Value> {
    let root = parent.join(mode.suffix().trim_start_matches('.'));
    let node = Node::install_with(&root, "Lifecycle Baseline", mode)?;
    let document = doc::new("sample")
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
        )
        .field(Field::new("id").r#type("bytes").size(10).annotation("@event"))
        .field(
            Field::new("duration")
                .r#type("uint")
                .size(4)
                .annotation("#summ(numeric)")
                .annotation("@unit(ms)"),
        )
        .field(Field::new("message").r#type("tstr"))
        .sign()?;
    let registration = node.registration(
        &RegistrationTarget {
            cache: "local".into(),
            service: "lab".into(),
            resource: "sample".into(),
            resource_type: "sample".into(),
            split: String::new(),
            branch: None,
            ..Default::default()
        },
        document.bytes()?,
    )?;
    let rx = SeedDefinition::prescribe(&document)?;
    let hour = 1_794_960_000_000; // Identical inputs in both storage modes.
    let started = Instant::now();
    for ordinal in 0..count {
        let mut id = [0; 10];
        id[2..].copy_from_slice(&ordinal.to_be_bytes());
        let message = format!(
            "Reading {ordinal}: {}",
            "sample ".repeat((ordinal % 17) as usize)
        );
        let seed = rx.ingest((), |_, seed| {
            seed.set_uint("time", hour + ordinal * HOUR / count)?
                .set_byte_string("id", &id)?
                .set_uint("duration", ordinal % 1000)?
                .set_text_string("message", &message)?;
            Ok(())
        })?;
        seed.cap()?;
        node.ingest(registration.branch, &seed.bytes()?, hour + HOUR - 1)?;
    }
    let ingestion_ms = elapsed(started);
    assert_eq!(
        node.timeline(&[registration.branch], hour, hour + HOUR)?["count"],
        count
    );
    let started = Instant::now();
    node.commit(hour + 48 * HOUR)?;
    let publication_ms = elapsed(started);
    drop(node);
    let started = Instant::now();
    let node = Node::open(&root)?;
    let reopen_ms = elapsed(started);
    let mut cases = Vec::new();
    for (name, threshold, expected) in [
        ("all", 0, count),
        ("selective", 990, count / 100),
        ("zero", 1000, 0),
    ] {
        let mut runs = Vec::new();
        for _ in 0..6 {
            let started = Instant::now();
            let mut cursor = node.query(
                registration.branch,
                hour,
                hour + HOUR,
                Query::new().filter(Predicate::compare("duration", Compare::Ge, threshold)),
            )?;
            let mut rows = 0;
            loop {
                let page = cursor.fetch(100)?;
                rows += page.len();
                std::hint::black_box(page.objects()?);
                if page.len() < 100 {
                    break;
                }
            }
            let total_ms = elapsed(started);
            assert_eq!(rows as u64, expected);
            runs.push(json!({"rows":rows,"ms":total_ms}));
        }
        cases.push(json!({"name":name,"runs":runs}));
    }
    let mut summaries = Vec::new();
    for _ in 0..6 {
        let started = Instant::now();
        let result = node.summaries(
            &[registration.branch],
            hour,
            hour + HOUR,
            300_000,
            "duration",
            &Predicate::All,
        )?;
        let total_ms = elapsed(started);
        assert_eq!(result["summary"]["count"], count);
        summaries.push(total_ms);
    }
    let directory = root.join("local/lab/sample");
    let files = std::fs::read_dir(directory)?
        .map(|entry| {
            let entry = entry?;
            Ok(json!({"name":entry.file_name().to_string_lossy(),"bytes":entry.metadata()?.len()}))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(
        json!({"storage":mode,"records":count,"typed_encoding_and_durable_ingestion_ms":ingestion_ms,
        "publication_ms":publication_ms,"reopen_ms":reopen_ms,"queries":cases,"summary_ms":summaries,"files":files}),
    )
}
fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [directory] = args.as_slice() else {
        return Err("Expected a fresh output directory".into());
    };
    let root = Path::new(directory);
    std::fs::create_dir(root)?;
    let result = json!({"scope":"Rust node; 2,000 identical typed events per mode. Ingestion timer includes builder setters, cap, and each durable append. Publication timed separately. Queries include planning, disk reads, filtering, and Rust object materialization; exclude JSON, IPC and UI. OS cache is not evicted. No before/after claim.",
        "results":[run(root,Storage::Browse,2000)?,run(root,Storage::Pack,2000)?]});
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
