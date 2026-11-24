//! Same ingested Pack, physical comparisons versus annotation-derived comparisons.
//! Times scan + ordinal result allocation, excluding ingestion and Pack opening.
use data_seeds::*;
use std::{hint::black_box, time::Instant};

fn main() -> Result<(), Error> {
    let physical = doc::new("logs").field(Field::new("level").r#type("uint").size(1)).sign()?;
    let logical = doc::new("logs").field(Field::new("level").r#type("uint").size(1).annotation("@otel(SeverityNumber)")).sign()?;
    let raw=SeedDefinition::prescribe(&physical)?;
    let mapped=SeedDefinition::prescribe(&logical)?;
    let mut pack=raw.fill(100_000)?;
    for ordinal in 0..100_000u32 {
        pack.load(raw.ingest(ordinal,|i,seed|{seed.set_uint("level",(i%24+1) as u8)?;Ok(())})?)?;
    }
    pack.seal()?;
    let bytes=pack.into_bytes()?;
    let raw=raw.open(bytes.clone())?;
    let mapped=mapped.open(bytes)?;
    for (name,physical,logical) in [
        ("level = ERROR",Predicate::And(vec![Predicate::compare("level",Compare::Ge,17),Predicate::compare("level",Compare::Le,20)]),Predicate::compare("level",Compare::Eq,"ERROR")),
        ("severity = 4",Predicate::Or([4,8,12,16,20,24].map(|n|Predicate::compare("level",Compare::Eq,n)).to_vec()),Predicate::compare("severity",Compare::Eq,4)),
    ] {
        let reference=raw.filter(physical.clone())?.fetch(100_000)?;
        assert_eq!(reference,mapped.filter(logical.clone())?.fetch(100_000)?);
        let mut times=[Vec::new(),Vec::new()];
        for trial in 0..42 {
            for i in if trial%2==0 {[0,1]}else{[1,0]} {
                let (pack,predicate)=if i==0 {(&raw,&physical)}else{(&mapped,&logical)};
                let start=Instant::now();
                let rows=pack.filter(predicate.clone())?.fetch(100_000)?;
                black_box(&rows);
                let elapsed=start.elapsed().as_nanos();
                assert_eq!(rows,reference);
                if trial>=2 {times[i].push(elapsed);}
            }
        }
        for samples in &mut times {samples.sort_unstable();}
        println!("{}",serde_json::json!({"query":name,"input_records":100_000,"matches":reference.len(),"trials":40,"physical_median_ms":times[0][20] as f64/1e6,"mapped_median_ms":times[1][20] as f64/1e6}));
    }
    Ok(())
}
