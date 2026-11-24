use data_seeds::{Field, SeedDefinition, doc, summaries::SummaryClass};

#[test]
fn summary_classes_are_resolved_from_annotations_and_preserved_through_doc() {
    let document = doc::new("summary-source")
        .field(
            Field::new("duration")
                .r#type("uint")
                .size(3)
                .annotation("#summ(numeric)"),
        )
        .field(
            Field::new("price")
                .r#type("int")
                .size(16)
                .annotation("#summ(numeric)"),
        )
        .field(
            Field::new("status")
                .closed_enum("uint", [200.into(), 400.into(), 500.into()])
                .annotation("#summ(categorical)"),
        )
        .field(
            Field::new("priority")
                .closed_enum("tstr", ["low".into(), "high".into()])
                .annotation("#summ(ordinal)"),
        )
        .field(
            Field::new("cached")
                .r#type("bool")
                .annotation("#summ(boolean)"),
        )
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time")
                .annotation("#summ(temporal)"),
        )
        .field(
            Field::new("message")
                .r#type("tstr")
                .annotation("@display(message)"),
        )
        .sign()
        .unwrap();
    let source = SeedDefinition::read(document.bytes().unwrap()).unwrap();
    let fields = source.summary_fields();
    assert_eq!(
        fields
            .iter()
            .map(|f| (f.ordinal(), f.name(), f.class()))
            .collect::<Vec<_>>(),
        vec![
            (0, "duration", SummaryClass::Numeric),
            (1, "price", SummaryClass::Numeric),
            (2, "status", SummaryClass::Categorical),
            (3, "priority", SummaryClass::Ordinal),
            (4, "cached", SummaryClass::Boolean),
            (5, "time", SummaryClass::Temporal),
        ]
    );
    assert_eq!(fields[2].values(), &[200.into(), 400.into(), 500.into()]);
    assert_eq!(fields[3].values(), &["low".into(), "high".into()]);
    assert_eq!(source.annotations(0).unwrap(), ["#summ(numeric)"]);
}

#[test]
fn invalid_summary_types_are_rejected_by_seeds_not_the_codec() {
    let fields = [
        Field::new("status").enumeration("uint", [200.into()]).size(0).annotation("#summ(categorical)"),
        Field::new("text")
            .r#type("tstr")
            .annotation("#summ(numeric)"),
        Field::new("text")
            .r#type("tstr")
            .annotation("#summ(categorical)"),
        Field::new("text")
            .r#type("tstr")
            .size(20)
            .annotation("#summ(categorical)"),
        Field::new("bytes")
            .r#type("bytes")
            .size(8)
            .annotation("#summ(numeric)"),
        Field::new("array")
            .array("uint")
            .element_size(4)
            .size(3)
            .annotation("#summ(numeric)"),
        Field::new("map")
            .map("tstr", "uint")
            .annotation("#summ(numeric)"),
        Field::new("any").r#type("any").annotation("#summ(numeric)"),
        Field::new("number")
            .r#type("uint")
            .size(2)
            .annotation("#summ(categorical)"),
        Field::new("number")
            .r#type("uint")
            .size(1)
            .annotation("#summ(boolean)"),
        Field::new("number")
            .r#type("float")
            .size(8)
            .annotation("#summ(temporal)"),
    ];
    for field in fields {
        let document = doc::new("invalid-summary").field(field).sign().unwrap();
        assert!(
            document.prescribe().is_ok(),
            "Pill Packer must not interpret #summ"
        );
        assert!(SeedDefinition::prescribe(&document).is_err());
    }
}

#[test]
fn malformed_unknown_or_duplicate_summary_classes_fail_explicitly() {
    for annotations in [
        vec!["#summ"],
        vec!["#summ()"],
        vec!["#summ(numeric"],
        vec!["#summ(numeric) trailing"],
        vec!["#summ(text)"],
        vec!["#summ(numeric)", "#summ(numeric)"],
        vec!["#summ(numeric,boolean)"],
    ] {
        let document = doc::new("invalid-summary")
            .field(
                Field::new("value")
                    .r#type("uint")
                    .size(4)
                    .annotations(annotations),
            )
            .sign()
            .unwrap();
        assert!(SeedDefinition::prescribe(&document).is_err());
    }
    let document = doc::new("unrelated-annotations")
        .field(Field::new("value").r#type("tstr").annotations([
            "@summ(numeric)",
            "#summary(other)",
            "#future(analytics)",
        ]))
        .sign()
        .unwrap();
    assert!(
        SeedDefinition::prescribe(&document)
            .unwrap()
            .summary_fields()
            .is_empty()
    );
}

#[test]
fn percentile_requests_do_not_select_or_resize_accumulators() {
    let make = |annotations: Vec<&str>| {
        doc::new("analytics")
            .field(
                Field::new("duration")
                    .r#type("uint")
                    .size(4)
                    .annotations(annotations),
            )
            .sign()
            .unwrap()
    };
    let only = SeedDefinition::prescribe(&make(vec![
        "#percentile(95)",
        "#percentile(99.9)",
        "#percentile(95.0)",
    ]))
    .unwrap();
    assert!(only.summary_fields().is_empty());
    assert_eq!(only.summary_analytics()[0].percentiles(), &[95.0, 99.9]);
    let plain = SeedDefinition::prescribe(&make(vec!["#summ(numeric)"]))
        .unwrap()
        .summaries(288)
        .unwrap();
    let requested = SeedDefinition::prescribe(&make(vec![
        "#summ(numeric)",
        "#percentile(0)",
        "#percentile(50)",
        "#percentile(100)",
    ]))
    .unwrap()
    .summaries(288)
    .unwrap();
    assert_eq!(
        plain.document().bytes().unwrap(),
        requested.document().bytes().unwrap(),
        "analytics requests are not stored aggregate state"
    );
    for annotation in [
        "#percentile",
        "#percentile()",
        "#percentile(-1)",
        "#percentile(101)",
        "#percentile(NaN)",
        "#percentile(inf)",
        "#percentile(95,98)",
        "#percentile(95) extra",
    ] {
        let doc = make(vec![annotation]);
        assert!(
            doc.prescribe().is_ok(),
            "codec preserves application annotations"
        );
        assert!(SeedDefinition::prescribe(&doc).is_err(), "{annotation}");
    }
}
