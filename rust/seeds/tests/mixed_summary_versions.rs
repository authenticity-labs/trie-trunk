use ciborium::Value as Data;
use data_seeds::{Compare, Field, SeedDefinition, doc, summaries::*};
use std::sync::Arc;
fn member<'a>(value: &'a Data, name: &str) -> &'a Data {
    &value
        .as_map()
        .unwrap()
        .iter()
        .find(|(k, _)| k.as_text() == Some(name))
        .unwrap()
        .1
}
fn build(base_mixed: bool) -> Vec<u8> {
    let make = |version, boolean| {
        let metric = if boolean {
            Field::new("metric")
                .r#type("bool")
                .annotation("#summ(boolean)")
        } else {
            Field::new("metric")
                .r#type("uint")
                .size(16)
                .annotation("#summ(numeric)")
        };
        let document = doc::new("events")
            .version(0, version)
            .field(metric)
            .field(
                Field::new("latency")
                    .r#type("uint")
                    .size(if boolean { 4 } else { 2 })
                    .annotation("#summ(numeric)"),
            )
            .sign()
            .unwrap();
        SeedDefinition::prescribe(&document)
            .unwrap()
            .summaries(288)
            .unwrap()
    };
    let a = make(0, false);
    let b = make(1, true);
    let mut days = [a.accumulator(0).unwrap(), b.accumulator(0).unwrap()];
    // Period zero: A wins by total events, although B has more non-null metric
    // observations. Period one: B wins. Periods two and three have one version.
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
                    if source == 1 {
                        seed.set_bool("metric", true)?;
                    } else if i == 0 {
                        seed.set_uint("metric", u128::MAX)?;
                    } else {
                        seed.set_null("metric")?;
                    }
                    Ok(())
                })
                .unwrap();
        }
    }
    for day in &mut days {
        day.complete(SUMMARY_DAY_MS).unwrap();
    }
    let (mixed, pack) = SummaryAccumulator::seal_mixed(&mut days).unwrap();
    let directory = pil_codec::pac::versions::PackIndex::open(pack.bytes()).unwrap();
    assert_eq!(directory.count(), 288);
    // The schema owns names and object references. Each participating row
    // Definition is fixed, and null rows retain that Definition's byte width.
    use pil_codec::authoring_language::language::OBJECT;
    for definition in mixed.layout().definitions() {
        let schema = doc::read(definition.prescription().bytes()).unwrap();
        let fields = schema.definitions()[0]
            .fields
            .iter()
            .find(|field| field.name == "fields")
            .unwrap();
        assert_eq!(fields.base_kind()[0], OBJECT);
        for field in &schema.definitions()[fields.base_kind()[1] as usize].fields {
            assert_eq!(field.base_kind()[0], OBJECT);
            assert!(matches!(field.name.as_str(), "metric" | "latency"));
        }
        assert!(matches!(
            definition.seed().mode().unwrap(),
            data_seeds::EncodingMode::FixedFull | data_seeds::EncodingMode::FixedSparse
        ));
    }
    let mut widths = std::collections::BTreeMap::new();
    for ordinal in 0..directory.count() {
        let (reference, bytes) = directory.member(ordinal).unwrap();
        if let Some(width) = widths.insert(reference, bytes.len()) {
            assert_eq!(width, bytes.len());
        }
        assert!(
            !bytes
                .windows(b"latency".len())
                .any(|part| part == b"latency")
        );
        assert!(!bytes.windows(b"metric".len()).any(|part| part == b"metric"));
    }
    let references: Vec<_> = (0..5)
        .map(|i| directory.versions().range(i).unwrap().definition)
        .collect();
    assert_eq!(references, [0, 1, 1, 0, 0]);
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
    // Every event Definition, accumulator and writer plan is dropped here.
}
#[test]
#[ignore = "TODO: custom log class changes; these historical assertions describe the deferred per-class winner path"]
fn stored_definitions_keep_real_names_and_period_winners_after_restart() {
    for base_mixed in [false, true] {
        let bytes = Arc::new(build(base_mixed));
        let storage = SummaryBoxReader::open(&bytes).unwrap();
        let day = if base_mixed { 0 } else { 1 };
        assert!(
            storage
                .definition_transitions()
                .iter()
                .any(|r| r.binding == pil_codec::r#box::versions::Binding::MixedSpan)
        );
        let footer = pil_codec::r#box::Box::footer_range(&bytes, bytes.len() as u64).unwrap();
        let metadata =
            SummaryBoxReader::open_footer(&bytes[footer.start as usize..], bytes.len() as u64)
                .unwrap();
        let extent = metadata.storage_range(day).unwrap();
        let reopened = metadata
            .open_pack_at(day, &bytes[extent.start as usize..extent.end as usize])
            .unwrap();
        let restored =
            SummaryProjection::from_layout(metadata.layout_at(day).unwrap().clone()).unwrap();
        assert_eq!(
            restored
                .field(&reopened.reader().get(0).unwrap(), "metric")
                .unwrap(),
            Data::Null
        );
        let layout = storage.layout_at(day).unwrap();
        let projection = SummaryProjection::from_layout(layout.clone()).unwrap();
        let pack = storage.pack(day).unwrap();
        let mut reader = pack.reader();
        for period in 0..4 {
            let row = reader.get(period).unwrap();
            let raw = row.crush().unwrap();
            let fields = raw["fields"].as_ref().unwrap().as_object().unwrap();
            assert_eq!(fields.len(), 2);
            assert!(fields.contains_key("metric") && fields.contains_key("latency"));
            assert!(!fields.contains_key("f0") && !fields.contains_key("f1"));
            let metric = projection.field(&row, "metric").unwrap();
            if period < 2 {
                assert_eq!(metric, Data::Null);
                let count = 5;
                let bounds = projection
                    .comparison(&row, "metric", Compare::Gt, &0.into())
                    .unwrap();
                assert_eq!((bounds.lower, bounds.upper), (0, count));
            } else if period == 2 {
                assert_eq!(member(&metric, "true_count"), &2u64.into());
            } else {
                assert_eq!(
                    pil_codec::unsigned_native(member(&metric, "max")).unwrap(),
                    u128::MAX
                );
            }
            let latency = projection.field(&row, "latency").unwrap();
            let expected = [140_300u64, 280_100, 140_000, 100][period];
            assert_eq!(member(&latency, "sum"), &expected.into());
        }
        // The query path shares the original Box allocation, resolves each
        // row's version and exposes actual field names without aliases.
        let range = storage.pack_range(day).unwrap();
        let shared = layout.open_range(bytes.clone(), range.clone()).unwrap();
        assert_eq!(shared.stored_bytes().as_ptr(), bytes[range].as_ptr());
        let source = data_seeds::Packs::open_summary_box(bytes.clone(), day..day + 1).unwrap();
        let result = source
            .query(data_seeds::Query::new().limit(4))
            .unwrap()
            .fetch(4)
            .unwrap();
        assert_eq!(result.len(), 4);
        let objects = result.objects().unwrap();
        assert!(
            objects
                .iter()
                .all(|row| row.iter().any(|(k, _)| k == "fields"))
        );
    }
}
