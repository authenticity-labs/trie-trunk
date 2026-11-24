//! Ephemeral comparison data enters through typed Seed ingress; no encoded
//! fixtures or prepared field bytes are injected into the encoder.
use ciborium::Value as Data;
use data_seeds::summaries::{SummaryBox};
use data_seeds::*;
use std::{path::PathBuf, time::Instant};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = PathBuf::from(std::env::args().nth(1).expect("output directory"));
    std::fs::create_dir_all(&out)?;
    let mut document = doc::new("sample")
        .field(Field::new("sequence").r#type("uint").size(8))
        .field(Field::new("reading").r#type("uint").size(4).annotation("#summ(numeric)"))
        .field(Field::new("active").r#type("bool"))
        .field(Field::new("note").r#type("tstr"))
        .field(Field::new("extra").array("any"));
    for i in 5..32 {
        document = document.field(Field::new(format!("field{i}")).r#type("tstr"));
    }
    let document = document.sign()?;
    document.write(out.join("sample.rx"))?;
    let def = SeedDefinition::prescribe(&document)?;
    let summaries = def.summaries(288)?;
    let mut buckets = vec![summaries.bucket(); 288];
    let start = Instant::now();
    let mut pack = def.fill(10_000)?;
    for i in 0..10_000u64 {
        let message = format!("sample-{i}: measured value {}", i * 17 % 1000);
        let mut seed = summaries.ingest(&mut buckets[i as usize % 288], (i, message.as_str()), |(i, message), seed| {
            seed.set_uint("sequence", i)?
                .set_uint("reading", i * 17 % 1000)?
                .set_bool("active", i % 2 == 0)?;
            if i % 3 == 0 {
                seed.set_null("note")?;
            } else if i % 3 == 1 {
                seed.set_text_string("note", message)?;
            }
            for j in 5..32 {
                seed.set_text_string(j, message)?;
            }
            Ok(())
        })?;
        if i == 0 {
            seed.set_array(
                "extra",
                vec![Data::Map(vec![
                    (Data::Text("__proto__".into()), Data::Text("safe".into())),
                    (Data::Text("large".into()), Data::Integer(u64::MAX.into())),
                ])],
            )?;
        }
        pack.load(seed)?;
    }
    let load = start.elapsed();
    pack.seal()?;
    let elapsed = start.elapsed();
    #[cfg(feature = "compression")]
    std::fs::write(
        out.join("sample.zstd.pac"),
        pack.compress(Compression::default())?.bytes(),
    )?;
    let bytes = pack.into_bytes()?;
    std::fs::write(out.join("sample.pac"), &bytes)?;
    let mut container = pil_codec::r#box::Box::new(&[], Default::default())?;
    container.pack(bytes.as_slice())?;
    container.commit()?;
    std::fs::write(out.join("sample.box"), container.bytes()?)?;
    let mut summary_box = SummaryBox::new(summaries.layout().clone())?;
    let empty = summaries.bucket();
    summary_box.append(
        summaries
            .layout()
            .pack((0..288).map(|_| summaries.seed(&empty).unwrap()))?,
    )?;
    summary_box.append(
        summaries
            .layout()
            .pack(buckets.iter().map(|b| summaries.seed(b).unwrap()))?,
    )?;
    summary_box.commit()?;
    std::fs::write(out.join("sample.summ"), summary_box.bytes()?)?;
    println!(
        "10000 events / 32 fields: generate + typed ingress + load {:?}; through seal {:?}; {} bytes",
        load,
        elapsed,
        bytes.len()
    );
    Ok(())
}
