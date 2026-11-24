//! Dense summary benchmark inputs. Every value passes through Rust ingestion.
use data_seeds::{doc, Field, SeedDefinition, summaries::SummaryBox};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let destination = std::env::args().nth(1).ok_or("expected output directory")?;
    std::fs::create_dir_all(&destination)?;
    for width in [1, 8, 32] {
        let names = (0..width).map(|i| if i == 0 { "duration".into() } else { format!("metric_{i}") }).collect::<Vec<String>>();
        let mut d = doc::new("summary-benchmark");
        for name in &names { d = d.field(Field::new(name).r#type("float").size(8).annotation("#summ(numeric)")); }
        let document = d.sign()?;
        let source = SeedDefinition::prescribe(&document)?;
        let plan = source.summaries(288)?;
        let mut day = plan.accumulator(0)?;
        let mut events = source.fill(2880)?;
        for period in 0..288u64 { for event in 0..10u64 {
            let seed = day.ingest(period * 300_000 + event, (period,event), |(period,event), seed| {
                for (i,name) in names.iter().enumerate() { seed.set_float(name.as_str(), (period + event + i as u64) as f64 + 0.25)?; }
                Ok(())
            })?;
            events.load(seed)?;
        }}
        events.seal()?;
        day.complete(day.end())?;
        let mut storage = SummaryBox::new(plan.layout().clone())?;
        storage.append(day.seal()?)?.commit()?;
        std::fs::write(format!("{destination}/{width}.rx"), document.bytes()?)?;
        std::fs::write(format!("{destination}/{width}.summ"), storage.bytes()?)?;
    }
    Ok(())
}
