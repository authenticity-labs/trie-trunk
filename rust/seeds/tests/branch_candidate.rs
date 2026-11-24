#![cfg(feature = "registration")]
use data_seeds::{registration::Trunk, ports::{Candidate, Limits, uuid_time}};

fn sample(text: &str) -> Candidate {
    Candidate::read(text.as_bytes(), Limits::default()).unwrap()
}

#[test]
fn uuid_v7_precedes_timestamp_and_requires_correct_variant() {
    let id = "01890f47-3c00-7000-8000-000000000000";
    assert_eq!(uuid_time(id), Some(0x01890f473c00));
    for bad in [
        "01890f47-3c00-4000-8000-000000000000",
        "01890f47-3c00-7000-c000-000000000000",
        "01890f47-3c00-7000-0000-000000000000",
        "01890f47-3c00-7000-8000-00000000000z",
    ] {
        assert_eq!(uuid_time(bad), None);
    }
    let c = sample(&format!(
        "{{\"a_time\":\"2026-11-19T00:00:00Z\",\"id\":\"{id}\"}}\n"
    ));
    assert_eq!(c.time[0].kind, "uuidv7");
    assert_eq!(c.time[0].field, "id");
    assert_eq!(c.time[1].kind, "timestamp");
    assert!(c.complete);
}

#[test]
fn incomplete_uuid_coverage_is_visible_not_an_automatic_mapping() {
    let c = sample(
        "{\"id\":\"01890f47-3c00-7000-8000-000000000000\"}\n{\"id\":null}\n{}\n{\"id\":\"bad\"}\n",
    );
    assert_eq!(c.time[0].matched, 1);
    assert_eq!(c.time[0].missing_or_invalid, 3);
    let doc = c.document("candidate").unwrap();
    assert!(
        doc.definitions()[0]
            .fields
            .iter()
            .all(|f| f.annotations.is_empty())
    );
}

#[test]
fn widths_are_lossless_including_non_power_of_two_and_wide_json_integers() {
    let c = sample("{\"a\":65536,\"b\":-129,\"c\":340282366920938463463374607431768211455}\n");
    assert_eq!(
        c.fields
            .iter()
            .map(|f| (f.kind.as_str(), f.size))
            .collect::<Vec<_>>(),
        vec![("uint", 3), ("int", 2), ("uint", 16)]
    );
    c.document("candidate").unwrap().validate().unwrap();
    let c = sample("{\"n\":-128}\n{\"n\":127}\n");
    assert_eq!((c.fields[0].kind.as_str(), c.fields[0].size), ("int", 1));
    let c = sample("{\"n\":-170141183460469231731687303715884105728}\n");
    assert_eq!(c.fields[0].size, 16);
}

#[test]
fn conflicts_never_silently_cast_to_float_or_truncate() {
    for input in [
        "{\"n\":-1}\n{\"n\":340282366920938463463374607431768211455}\n",
        "{\"n\":9007199254740993}\n{\"n\":0.5}\n",
    ] {
        let c = sample(input);
        assert_eq!(c.fields[0].kind, "any");
        assert!(c.warnings.iter().any(|w| w.contains("review")));
    }
    assert!(
        Candidate::read(
            b"{\"n\":340282366920938463463374607431768211456}".as_slice(),
            Limits::default()
        )
        .is_err()
    );
}

#[test]
fn null_absent_and_nested_values_remain_distinct() {
    let c = sample("{\"n\":null,\"list\":[1,\"x\"],\"object\":{\"a\":1}}\n{\"n\":4}\n{}\n");
    let n = c.fields.iter().find(|f| f.name == "n").unwrap();
    assert_eq!((n.present, n.null, n.kind.as_str()), (2, 1, "uint"));
    let doc = c.document("candidate").unwrap();
    doc.validate().unwrap();
    assert_eq!(doc.definitions().len(), 1);
}

#[test]
fn limits_and_parse_failures_are_explicit() {
    let input = b"{\"a\":1}\n{\"a\":2}\n";
    let c = Candidate::read(
        input.as_slice(),
        Limits {
            records: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(c.records, 1);
    assert!(!c.complete);
    let c = Candidate::read(
        input.as_slice(),
        Limits {
            bytes: 12,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(c.records, 1);
    assert!(!c.complete);
    for invalid in [
        "{\"a\":1,\"a\":2}\n",
        "{\"a\":1}\nnot-json\n",
        "",
        "[1,2,3]\n",
    ] {
        assert!(
            Candidate::read(invalid.as_bytes(), Limits::default()).is_err(),
            "{invalid}"
        );
    }
    assert!(
        Candidate::read(
            input.as_slice(),
            Limits {
                record_bytes: 4,
                ..Default::default()
            }
        )
        .is_err()
    );
    assert!(
        Candidate::read(
            b"{\"a\":1,\"b\":2}".as_slice(),
            Limits {
                fields: 1,
                ..Default::default()
            }
        )
        .is_err()
    );
}

#[test]
fn text_without_time_requires_mapping_and_does_not_invent_annotations() {
    let c = sample("hello world\nsecond line\n");
    assert_eq!(c.format, "text");
    assert!(c.time.is_empty());
    assert!(
        c.warnings
            .iter()
            .any(|w| w.contains("Map an event timestamp"))
    );
    assert_eq!(c.fields[0].name, "message");
}

#[test]
fn candidate_belongs_to_trunk_ports_and_does_not_register() {
    let taxonomy = trie_trunk_taxonomy::Trunk::new("Local").unwrap();
    let bytes = taxonomy.bytes().to_vec();
    let trunk = Trunk::open(bytes.as_slice()).unwrap();
    let c = trunk
        .ports()
        .inspect(b"{\"message\":\"hello\"}".as_slice(), Limits::default())
        .unwrap();
    assert_eq!(c.records, 1);
    assert_eq!(taxonomy.bytes(), bytes);
}

#[test]
fn event_source_requires_time_but_not_an_id() {
    use data_seeds::ports::event::{Identity, Time};
    let candidate = sample("{\"observed\":\"2026-11-19T00:00:00Z\",\"value\":12}\n");
    let plan = candidate.event("observed").unwrap();
    assert_eq!(plan.time, Time::Timestamp("observed".into()));
    assert_eq!(plan.identity, Identity::Generate);
    assert!(candidate.event("value").is_err());
    assert!(sample("{\"value\":12}\n").event("time").is_err());
    assert!(sample("{\"time\":\"2026-11-19T00:00:00Z\"}\n{}\n").event("time").is_err());
}

#[test]
fn event_source_can_preserve_uuidv7_identity_and_time() {
    use data_seeds::ports::event::{Identity, Time};
    let candidate = sample("{\"key\":\"01890f47-3c00-7000-8000-000000000000\"}\n");
    let plan = candidate.event("key").unwrap();
    assert_eq!(plan.time, Time::UuidV7("key".into()));
    assert_eq!(plan.identity, Identity::Source("key".into()));
    assert!(sample("{\"key\":\"01890f47-3c00-4000-8000-000000000000\"}\n").event("key").is_err());
    assert!(sample("{\"key\":\"01890f47-3c00-7000-8000-000000000000\"}\n{\"key\":null}\n").event("key").is_err());
}

#[test]
fn ranges_preserve_actual_bounds_without_zero_padding() {
    for (input, min, max, kind, size) in [
        ("{\"n\":256}\n{\"n\":65536}\n", "256", "65536", "uint", 3),
        ("{\"n\":-129}\n{\"n\":-128}\n", "-129", "-128", "int", 2),
        ("{\"n\":-128}\n{\"n\":255}\n", "-128", "255", "int", 2),
        (
            "{\"n\":null}\n{\"n\":340282366920938463463374607431768211455}\n",
            "340282366920938463463374607431768211455",
            "340282366920938463463374607431768211455",
            "uint",
            16,
        ),
    ] {
        let c = sample(input);
        let f = &c.fields[0];
        let range = f.range.as_ref().unwrap();
        assert_eq!((range.min.as_str(), range.max.as_str()), (min, max));
        assert_eq!((f.kind.as_str(), f.size), (kind, size));
        c.document("ranges").unwrap().validate().unwrap();
    }
}

#[test]
fn float_width_requires_exact_roundtrip_not_only_small_magnitude() {
    for (values, width) in [
        ("1.5,2.25", 4),
        ("0.1,0.2", 8),
        ("1e40,2e40", 8),
        ("1e-50,2e-50", 8),
        ("-0.0,0.0", 4),
        ("16777217.0,1.0", 8),
    ] {
        let input = values
            .split(',')
            .map(|v| format!("{{\"n\":{v}}}\n"))
            .collect::<String>();
        let c = sample(&input);
        assert_eq!(
            (c.fields[0].kind.as_str(), c.fields[0].size),
            ("float", width),
            "{values}"
        );
        c.document("floats").unwrap().validate().unwrap();
    }
    let c = sample("{\"n\":1.5}\n{\"n\":2.25}\n");
    let range = c.fields[0].range.as_ref().unwrap();
    assert_eq!((range.min.as_str(), range.max.as_str()), ("1.5", "2.25"));
    assert!(Candidate::read(b"{\"n\":1e-999}".as_slice(), Limits::default()).is_err());
    assert!(Candidate::read(b"{\"n\":1e999}".as_slice(), Limits::default()).is_err());
}

#[test]
fn strings_measure_utf8_bytes_and_propose_observed_capacity() {
    let c = sample("{\"s\":\"é\"}\n{\"s\":\"ab\"}\n");
    let f = &c.fields[0];
    assert_eq!(f.size, 2);
    let bytes = f.string_bytes.as_ref().unwrap();
    assert_eq!((bytes.min, bytes.max), (2, 2));
    c.document("text").unwrap().validate().unwrap();
    let c = sample("{\"s\":\"\"}\n{\"s\":\"🌱\"}\n{\"s\":null}\n");
    assert_eq!(c.fields[0].size, 4);
    let bytes = c.fields[0].string_bytes.as_ref().unwrap();
    assert_eq!((bytes.min, bytes.max), (0, 4));
}

#[test]
fn potential_enums_are_bounded_counted_and_never_automatically_restrict_schema() {
    let c = sample(
        "{\"status\":200,\"method\":\"GET\"}\n{\"status\":200,\"method\":\"POST\"}\n{\"status\":404,\"method\":\"GET\"}\n{\"status\":200,\"method\":\"POST\"}\n{}\n{\"status\":null}\n",
    );
    let status = c.fields.iter().find(|f| f.name == "status").unwrap();
    assert_eq!(
        status
            .enum_values
            .iter()
            .map(|v| (v.value.as_str(), v.count))
            .collect::<Vec<_>>(),
        vec![("200", 3), ("404", 1)]
    );
    let method = c.fields.iter().find(|f| f.name == "method").unwrap();
    assert_eq!(
        method
            .enum_values
            .iter()
            .map(|v| (v.value.as_str(), v.count))
            .collect::<Vec<_>>(),
        vec![("\"GET\"", 2), ("\"POST\"", 2)]
    );
    let doc = c.document("categories").unwrap();
    assert!(
        doc.definitions()[0]
            .fields
            .iter()
            .all(|f| f.values.is_empty())
    );
    for input in [
        (0..100)
            .map(|i| format!("{{\"n\":{i}}}\n"))
            .collect::<String>(),
        "{\"id\":\"01890f47-3c00-7000-8000-000000000000\"}\n".repeat(4),
        "{\"at\":\"2026-11-19T00:00:00Z\"}\n".repeat(4),
        format!("{{\"text\":\"{}\"}}\n", "x".repeat(300)).repeat(4),
    ] {
        assert!(sample(&input).fields[0].enum_values.is_empty());
    }
}

#[test]
fn compatible_integer_and_float_samples_share_a_lossless_width() {
    for (input, width, min, max) in [
        ("{\"n\":1}\n{\"n\":2.5}\n", 4, "1", "2.5"),
        ("{\"n\":-3}\n{\"n\":2.5}\n", 4, "-3", "2.5"),
        ("{\"n\":16777217}\n{\"n\":2.5}\n", 8, "2.5", "16777217"),
        ("{\"n\":0}\n{\"n\":0.5}\n", 4, "0", "0.5"),
    ] {
        let c = sample(input);
        let f = &c.fields[0];
        assert_eq!((f.kind.as_str(), f.size), ("float", width));
        let r = f.range.as_ref().unwrap();
        assert_eq!((r.min.as_str(), r.max.as_str()), (min, max));
        c.document("numbers").unwrap().validate().unwrap();
    }
}

#[test]
fn inferred_widths_roundtrip_through_the_existing_seed_codec() {
    use ciborium::Value as Data;
    use data_seeds::{Cell, Packs, Query, SeedDefinition};
    let candidate = sample(
        "{\"n\":65536,\"ratio\":1.5,\"text\":\"🌱\"}\n{\"n\":256,\"ratio\":2.25,\"text\":\"a\\u0000\"}\n",
    );
    let definition = SeedDefinition::prescribe(&candidate.document("sized").unwrap()).unwrap();
    let mut pack = definition.fill(2).unwrap();
    let values = [(65536u32, 1.5, "🌱"), (256, 2.25, "a\0")];
    for &(n, ratio, text) in &values {
        let mut seed = definition.seed();
        seed.set_uint("n", n).unwrap();
        seed.set_float("ratio", ratio).unwrap();
        seed.set_text_string("text", text).unwrap();
        pack.load(seed).unwrap();
    }
    pack.seal().unwrap();
    let mut packs = Packs::new();
    packs.pack(definition.open(pack.into_bytes().unwrap()).unwrap());
    let batch = packs.query(Query::new()).unwrap().fetch(2).unwrap();
    let mut reader = batch.reader();
    for (i, (n, ratio, text)) in values.into_iter().enumerate() {
        let view = reader.view(i).unwrap();
        assert_eq!(view.get("n").unwrap(), Cell::Value(Data::from(n)));
        assert_eq!(view.get("ratio").unwrap(), Cell::Value(Data::Float(ratio)));
        assert_eq!(
            view.get("text").unwrap(),
            Cell::Value(Data::Text(text.into()))
        );
    }
}
