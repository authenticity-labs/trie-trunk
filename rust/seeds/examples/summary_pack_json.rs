//! Inspect one real summary Pack, built through the ingestion API.
use data_seeds::{Field, SeedDefinition, doc};
use serde_json::json;
fn main() {
    let source = SeedDefinition::prescribe(
        &doc::new("http")
            .namespace("log")
            .field(
                Field::new("duration")
                    .r#type("uint")
                    .size(4)
                    .annotation("@unit(ms)")
                    .annotation("#summ(numeric)"),
            )
            .field(
                Field::new("status")
                    .closed_enum("uint", [200, 404, 500].map(Into::into))
                    .annotation("#summ(categorical)"),
            )
            .sign()
            .unwrap(),
    )
    .unwrap();
    let plan = source.summaries(288).unwrap();
    let mut day = plan.accumulator(0).unwrap();
    let mut events = source.fill(4).unwrap();
    for (i, (duration, status)) in [(10u32, 200u32), (20, 200), (30, 404), (40, 500)]
        .into_iter()
        .enumerate()
    {
        events
            .load(
                day.ingest(i as u64, (duration, status), |(duration, status), seed| {
                    seed.set_uint("duration", duration)?
                        .set_enum("status", status.into())?;
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
    }
    events.seal().unwrap();
    day.complete(day.end()).unwrap();
    let bytes = day.seal().unwrap();
    let pack = plan.layout().open(bytes.bytes()).unwrap();
    let mut cursor = pack.reader();
    let row = cursor.get(0).unwrap();
    let dimensions = plan
        .layout()
        .definition()
        .prescription()
        .array_lengths()
        .iter()
        .filter(|(_, n)| *n > 0)
        .map(|&(field, n)| {
            (
                plan.document().definitions()[0].fields[field].name.clone(),
                json!(n),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let fields = ["count", "numeric", "categories", "counts"];
    let stored = row.crush_fields(&fields).unwrap();
    let logical = plan
        .object(&row)
        .unwrap()
        .into_iter()
        .map(|(name, value)| (name, pil_codec::json_view::data_json(value).unwrap()))
        .collect::<serde_json::Map<_, _>>();
    println!("{}", serde_json::to_string_pretty(&json!({
        "description":"Decoded inspection of a real Pack. First period shown; unused class arrays omitted.",
        "definition":"summary.rx", "mode":"Fixed Full", "memberCount":pack.len(), "byteLength":bytes.bytes().len(),
        "arrayLengths":dimensions,
        "seedBindings":{"numeric":["duration"],"categories":[{"field":"status","values":[200,404,500],"counterStart":0}]},
        "storedRows":[stored], "seedRows":[logical]
    })).unwrap());
}
