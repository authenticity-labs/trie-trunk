//! Numeric host-boundary fixture, produced through typed Seed ingestion.
use data_seeds::{Field, HourIndexBuilder, IndexFields, SeedDefinition, doc};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output =
        std::path::PathBuf::from(std::env::args().nth(1).ok_or("output directory required")?);
    std::fs::create_dir_all(&output)?;
    let document = doc::new("wide")
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
        )
        .field(Field::new("wide").r#type("uint").size(16))
        .field(Field::new("signed").r#type("int").size(16))
        .field(Field::new("array").array("uint").element_size(16))
        .field(Field::new("map").map("tstr", "uint").value_size(16))
        .sign()?;
    document.write(output.join("wide.rx"))?;
    let definition = SeedDefinition::prescribe(&document)?;
    let mut pack = definition.fill(3)?;
    let mut index = HourIndexBuilder::new(
        (1700000000000 / 3_600_000) * 3_600_000,
        3,
        IndexFields::from_definition(&definition)?,
    )?;
    for i in 0..3u64 {
        let value = (1u128 << 120) + i as u128;
        let small = (1u128 << 100) + i as u128;
        pack.load(definition.ingest(i, |i, seed| {
            seed.set_uint("time", 1700000000000 + i)?
                .set_uint("wide", value)?
                .set_int("signed", i128::MIN + i as i128)?
                .set_array("array", vec![pil_codec::native_unsigned(small)])?
                .set_map(
                    "map",
                    vec![("key".into(), pil_codec::native_unsigned(value))],
                )?;
            index.append(1700000000000 + i, 0)?;
            Ok(())
        })?)?;
    }
    std::fs::write(output.join("wide.indx"), index.finish().bytes())?;
    pack.seal()?;
    std::fs::write(output.join("wide.pac"), pack.into_bytes()?)?;
    Ok(())
}
