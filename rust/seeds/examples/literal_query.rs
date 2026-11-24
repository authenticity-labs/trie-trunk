//! Query-only timing: real records are ingested before timing; every timed run
//! plans and scans 100k records, retaining ordinal results, not materialized rows.
use data_seeds::*;
use std::{hint::black_box, time::Instant};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let document = doc::new("literal-query")
        .field(
            Field::new("duration")
                .r#type("uint")
                .size(4)
                .annotation("@unit(us)"),
        )
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time(ms)"),
        )
        .sign()?;
    let d = SeedDefinition::prescribe(&document)?;
    let mut pack = d.fill(100_000)?;
    for n in 0..100_000u32 {
        pack.load(d.ingest(n, |n, p| {
            p.set_uint("duration", n)?;
            p.set_uint("time", 1773072000000u64)?;
            Ok(())
        })?)?;
    }
    pack.seal()?;
    let bytes = pack.into_bytes()?;
    if let Some(dir) = std::env::args().nth(1) {
        std::fs::create_dir_all(&dir)?;
        std::fs::write(format!("{dir}/query.rx"), document.bytes()?)?;
        std::fs::write(format!("{dir}/query.pac"), &bytes)?;
    }
    let pack = d.open(bytes)?;
    let predicates = [
        Predicate::compare("duration", Compare::Ge, 50_000),
        Predicate::compare("duration", Compare::Ge, "50000"),
        Predicate::compare("duration", Compare::Ge, "50 ms"),
    ];
    let mut times = [Vec::new(), Vec::new(), Vec::new()];
    for round in 0..33 {
        for j in 0..3 {
            let i = (round + j) % 3;
            let start = Instant::now();
            let rows = pack.filter(predicates[i].clone())?.fetch(100_000)?;
            let elapsed = start.elapsed().as_nanos();
            assert_eq!(rows.len(), 50_000);
            black_box(rows);
            if round >= 3 {
                times[i].push(elapsed);
            }
        }
    }
    for (name, mut t) in ["typed", "numeric text", "unit text"]
        .into_iter()
        .zip(times)
    {
        t.sort();
        println!(
            "{name}: median {:.3} ms; 100000 scanned, 50000 matches; 30 alternating trials",
            t[15] as f64 / 1e6
        );
    }
    Ok(())
}
