use ciborium::Value as Data;
use data_seeds::{Field, SeedDefinition, doc, summaries::*};
use std::sync::Arc;

fn plan(version: u32, width: u16) -> SummaryPlan {
    let document = doc::new("events")
        .version(1, version)
        .field(
            Field::new("latency")
                .r#type("uint")
                .size(width)
                .annotation("#summ(numeric)"),
        )
        .sign()
        .unwrap();
    SeedDefinition::prescribe(&document)
        .unwrap()
        .summaries(288)
        .unwrap()
}

fn build(base_mixed: bool) -> Vec<u8> {
    let a = plan(0, 2);
    let b = plan(1, 4);
    let mut days = [a.accumulator(0).unwrap(), b.accumulator(0).unwrap()];
    for (period, source, count) in [
        (0, 0, 3),
        (0, 1, 2),
        (1, 0, 1),
        (1, 1, 4),
        (2, 1, 2),
        (3, 0, 1),
    ] {
        for i in 0..count {
            days[source]
                .ingest(period * SUMMARY_PERIOD_MS + i, (), |(), seed| {
                    seed.set_uint("latency", if source == 0 { 100u64 } else { 70_000 })?;
                    Ok(())
                })
                .unwrap();
        }
    }
    for day in &mut days {
        day.complete(SUMMARY_DAY_MS).unwrap();
    }
    let (mixed, pack) = SummaryAccumulator::seal_mixed(&mut days).unwrap();
    let mut storage = SummaryBox::new(if base_mixed {
        mixed.layout().clone()
    } else {
        a.layout().clone()
    })
    .unwrap();
    if !base_mixed {
        let mut silent = a.accumulator(0).unwrap();
        silent.complete(SUMMARY_DAY_MS).unwrap();
        storage.append(silent.seal().unwrap()).unwrap();
    }
    storage.append(pack).unwrap().commit().unwrap();
    storage.bytes().unwrap().to_vec()
}

#[test]
fn mixed_day_reopens_by_offset_with_fixed_rows_and_source_field_names() {
    for base_mixed in [false, true] {
        let bytes = Arc::new(build(base_mixed));
        let day = usize::from(!base_mixed);
        let footer = pil_codec::r#box::Box::footer_range(&bytes, bytes.len() as u64).unwrap();
        let directory =
            SummaryBoxReader::open_footer(&bytes[footer.start as usize..], bytes.len() as u64)
                .unwrap();
        let extent = directory.storage_range(day).unwrap();
        let slice = &bytes[extent.start as usize..extent.end as usize];
        let pack = directory.open_pack_at(day, slice).unwrap();
        assert_eq!(pack.len(), 288);
        assert_eq!(pack.options().address_bytes, 0);
        let definition = directory.layout_at(day).unwrap().definition();
        let schema = doc::read(definition.prescription().bytes()).unwrap();
        assert_eq!(schema.definitions()[0].header.rx_name, "summary");
        let reopened = MixedSummaryPlan::new(&[plan(0, 2), plan(1, 4)]).unwrap();
        let projection = SummaryProjection::Mixed(reopened);
        let periods = projection.periods(pack.headers()).unwrap().unwrap();
        let mut reader = pack.reader();
        for (period, sum) in [140_300u64, 280_100, 140_000, 100].into_iter().enumerate() {
            let row = reader.get(period).unwrap();
            let value = periods
                .period(period)
                .unwrap()
                .field(&row, "latency")
                .unwrap();
            let sum_field = value
                .as_map()
                .unwrap()
                .iter()
                .find(|(key, _)| key.as_text() == Some("sum"))
                .unwrap();
            assert_eq!(sum_field.1, Data::from(sum));
        }
        let range = extent.start as usize..extent.end as usize;
        let shared = projection
            .layout()
            .open_range(bytes.clone(), range)
            .unwrap();
        assert_eq!(shared.stored_bytes().as_ptr(), slice.as_ptr());
    }
}
