use ciborium::Value as Data;
use data_seeds::{EncodingMode, Field, SeedDefinition, doc, summaries::*};
use std::sync::Arc;
fn source() -> Arc<SeedDefinition> {
    let d = doc::new("analytics")
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
                .closed_enum("tstr", ["low".into(), "medium".into(), "high".into()])
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
        .field(Field::new("message").r#type("tstr"))
        .sign()
        .unwrap();
    SeedDefinition::prescribe(&d).unwrap()
}
fn value<'a>(v: &'a Data, k: &str) -> &'a Data {
    let Data::Map(m) = v else {
        panic!("not an object")
    };
    m.iter()
        .find(|(key, _)| key.as_text() == Some(k))
        .map(|(_, v)| v)
        .unwrap()
}
#[test]
fn annotated_classes_prepare_actual_fixed_objects_from_ingestion() {
    let source = source();
    let plan = source.summaries(288).unwrap();
    let mut bucket = plan.bucket();
    let mut events = source.fill(4).unwrap();
    for i in 0..4u64 {
        let seed = plan
            .ingest(&mut bucket, i, |i, seed| {
                match i {
                    0 | 1 => {
                        seed.set_uint("duration", 100 + i * 100)?;
                    }
                    2 => {
                        seed.set_null("duration")?;
                    }
                    _ => {}
                }
                seed.set_int("price", -(1i128 << 90) + i as i128)?
                    .set_enum(
                        "status",
                        if i < 2 {
                            200.into()
                        } else if i == 2 {
                            400.into()
                        } else {
                            500.into()
                        },
                    )?
                    .set_enum("priority", if i < 3 { "low".into() } else { "high".into() })?
                    .set_bool("cached", i < 3)?
                    .set_uint("time", 1000 + i * 100)?
                    .set_text_string("message", "unbounded event text is not summarized")?;
                Ok(())
            })
            .unwrap();
        events.load(seed).unwrap();
    }
    events.seal().unwrap();
    let empty = plan.bucket();
    let empty_pack = plan
        .layout()
        .pack((0..288).map(|_| plan.seed(&empty).unwrap()))
        .unwrap();
    let active_pack = plan
        .layout()
        .pack((0..288).map(|i| plan.seed(if i == 0 { &bucket } else { &empty }).unwrap()))
        .unwrap();
    assert_eq!(empty_pack.bytes().len(), active_pack.bytes().len());
    // A PAC member is a time-period row containing every selected class
    // object, not a column holding just one class. Different object widths
    // must not change either the row stride or the daily Pack length.
    let expected_fields = ["duration", "price", "status", "priority", "cached", "time"];
    let mut row_width = None;
    for bytes in [empty_pack.bytes(), active_pack.bytes()] {
        let pack = plan.layout().open(bytes).unwrap();
        assert_eq!(pack.len(), 288);
        assert_eq!(pack.options().address_bytes, 0);
        for ordinal in [0, 143, 287] {
            let pill = pack.get(ordinal).unwrap();
            let bytes = pill.bytes().unwrap();
            let width = *row_width.get_or_insert(bytes.len());
            assert_eq!(bytes.len(), width);
            let mut reader = pack.reader();
            let row = reader.get(ordinal).unwrap();
            let fields = plan.object(&row).unwrap();
            assert_eq!(fields.iter().map(|(name, _)| name.as_str()).collect::<Vec<_>>(), expected_fields);
            assert!(fields.iter().all(|(_, value)| matches!(value, Data::Map(_))));
        }
    }
    let opened = plan
        .layout()
        .definition()
        .prescription()
        .open(active_pack.bytes())
        .unwrap();
    assert_eq!(opened.options().address_bytes, 0);
    let mut reader = opened.reader();
    let row = reader.get(0).unwrap();
    assert_eq!(row.mode(), EncodingMode::FixedFull);
    row.validate().unwrap();
    // The codec stores reusable class arrays; Seeds restores source field names.
    let actual = row.crush().unwrap();
    assert!(actual["numeric"].as_ref().unwrap().is_array());
    assert!(!actual.contains_key("fields"));
    let duration = plan.field(&row, "duration").unwrap();
    for (k, n) in [
        ("count", 2),
        ("null_count", 1),
        ("absent_count", 1),
        ("min", 100),
        ("max", 200),
        ("sum", 300),
    ] {
        assert_eq!(value(&duration, k), &Data::Integer(n.into()));
    }
    assert_eq!(value(&duration, "mean"), &Data::Float(150.0));
    let status = plan.field(&row, "status").unwrap();
    assert_eq!(value(&status, "distinct"), &3.into());
    let Data::Map(categories) = value(&status, "values") else {
        panic!()
    };
    assert_eq!(categories[0].0, 200.into());
    assert_eq!(value(&categories[0].1, "count"), &2.into());
    let priority = plan.field(&row, "priority").unwrap();
    assert_eq!(value(&priority, "median"), &"low".into());
    let cached = plan.field(&row, "cached").unwrap();
    assert_eq!(value(&cached, "true_count"), &3.into());
    assert_eq!(value(&cached, "false_count"), &1.into());
    let time = plan.field(&row, "time").unwrap();
    assert_eq!(value(&time, "earliest"), &1000.into());
    assert_eq!(value(&time, "latest"), &1300.into());
    let price = plan.field(&row, "price").unwrap();
    assert_eq!(
        value(&price, "sum"),
        &pil_codec::native_signed(-(4i128 << 90) + 6)
    );
    let mut box_ = SummaryBox::new(plan.layout().clone()).unwrap();
    box_.append(empty_pack)
        .unwrap()
        .append(active_pack)
        .unwrap();
    box_.commit().unwrap();
    let reopened = SummaryBoxReader::open(box_.bytes().unwrap()).unwrap();
    let stored = reopened.pack(1).unwrap();
    let mut rows = stored.reader();
    let row = rows.get(0).unwrap();
    assert_eq!(plan.field(&row, "duration").unwrap(), duration);
}
#[test]
fn merge_is_weighted_and_lossless_and_does_not_merge_different_plans() {
    let source = source();
    let plan = source.summaries(1).unwrap();
    let mut left = plan.bucket();
    let mut right = plan.bucket();
    let mut events = source.fill(3).unwrap();
    for (v, b) in [(10, &mut left), (100, &mut right)] {
        events
            .load(
                plan.ingest(b, v, |v, s| {
                    s.set_uint("duration", v as u64)?;
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
    }
    events
        .load(
            plan.ingest(&mut right, 200u64, |v, s| {
                s.set_uint("duration", v)?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    events.seal().unwrap();
    plan.merge(&mut left, &right).unwrap();
    assert_eq!(left.count(), 3);
    let pack = plan.layout().pack([plan.seed(&left).unwrap()]).unwrap();
    let opened = plan
        .layout()
        .definition()
        .prescription()
        .open(pack.bytes())
        .unwrap();
    let mut rows = opened.reader();
    let duration = plan.field(&rows.get(0).unwrap(), "duration").unwrap();
    assert_eq!(value(&duration, "sum"), &310.into());
    assert_eq!(value(&duration, "mean"), &Data::Float(310.0 / 3.0));
    let another = source.summaries(1).unwrap();
    assert!(plan.merge(&mut left, &another.bucket()).is_err());
}
#[test]
fn empty_nonfinite_and_overflow_have_explicit_unavailable_statistics() {
    let doc = doc::new("edges")
        .field(
            Field::new("wide")
                .r#type("uint")
                .size(16)
                .annotation("#summ(numeric)"),
        )
        .field(
            Field::new("float")
                .r#type("float")
                .size(8)
                .annotation("#summ(numeric)"),
        )
        .sign()
        .unwrap();
    let source = SeedDefinition::prescribe(&doc).unwrap();
    let plan = source.summaries(2).unwrap();
    let mut bucket = plan.bucket();
    let mut events = source.fill(2).unwrap();
    for f in [f64::NAN, 1.0] {
        events
            .load(
                plan.ingest(&mut bucket, f, |f, s| {
                    s.set_uint("wide", u128::MAX)?.set_float("float", f)?;
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
    }
    events.seal().unwrap();
    let pack = plan
        .layout()
        .pack([
            plan.seed(&bucket).unwrap(),
            plan.seed(&plan.bucket()).unwrap(),
        ])
        .unwrap();
    let opened = plan
        .layout()
        .definition()
        .prescription()
        .open(pack.bytes())
        .unwrap();
    let mut rows = opened.reader();
    let active = rows.get(0).unwrap();
    let wide = plan.field(&active, "wide").unwrap();
    assert_eq!(value(&wide, "sum"), &Data::Null);
    assert_eq!(value(&wide, "max"), &pil_codec::native_unsigned(u128::MAX));
    let float = plan.field(&active, "float").unwrap();
    assert_eq!(value(&float, "sum"), &Data::Null);
    assert_eq!(value(&float, "min"), &Data::Float(1.0));
    assert_eq!(value(&float, "nonfinite_count"), &1.into());
    let empty = plan.field(&rows.get(1).unwrap(), "wide").unwrap();
    assert_eq!(value(&empty, "min"), &Data::Null);
    assert_eq!(value(&empty, "mean"), &Data::Null);
}

#[test]
fn class_previews_match_ingested_queries_and_stop_before_unsupported_fields() {
    use data_seeds::{Compare, Predicate, Query};
    let source = source();
    let plan = source.summaries(1).unwrap();
    let mut bucket = plan.bucket();
    let mut events = source.fill(6).unwrap();
    for i in 0..6u64 {
        events
            .load(
                plan.ingest(&mut bucket, i, |i, s| {
                    if i == 4 {
                        s.set_null("duration")?;
                    } else if i != 5 {
                        s.set_uint("duration", i * 100)?;
                    }
                    s.set_enum("status", if i < 3 { 200.into() } else { 500.into() })?
                        .set_bool("cached", i % 2 == 0)?
                        .set_text_string("message", "hello")?;
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
    }
    events.seal().unwrap();
    let events = source.open(events.into_bytes().unwrap()).unwrap();
    let pack = plan.layout().pack([plan.seed(&bucket).unwrap()]).unwrap();
    let opened = plan
        .layout()
        .definition()
        .prescription()
        .open(pack.bytes())
        .unwrap();
    let mut rows = opened.reader();
    let row = rows.get(0).unwrap();
    let predicates = [
        Predicate::compare("duration", Compare::Gt, 10000),
        Predicate::compare("duration", Compare::Ge, 0),
        Predicate::compare("duration", Compare::Gt, 150),
        Predicate::compare("status", Compare::Eq, 500),
        Predicate::compare("cached", Compare::Eq, true),
        Predicate::IsNull("duration".into()),
        Predicate::IsAbsent("duration".into()),
        Predicate::Not(Box::new(Predicate::compare("duration", Compare::Gt, 150))),
        Predicate::And(vec![
            Predicate::compare("duration", Compare::Gt, 150),
            Predicate::compare("status", Compare::Eq, 200),
        ]),
        Predicate::Or(vec![
            Predicate::compare("duration", Compare::Gt, 150),
            Predicate::compare("status", Compare::Eq, 200),
        ]),
    ];
    let mut packs = data_seeds::Packs::new();
    packs.pack(events);
    for p in predicates {
        let preview = plan.preview(&p);
        assert!(preview.complete());
        let bounds = preview.count(&row).unwrap();
        let actual = packs
            .query(Query::new().filter(p))
            .unwrap()
            .fetch(100)
            .unwrap()
            .len() as u64;
        assert!(
            bounds.lower <= actual && actual <= bounds.upper,
            "{bounds:?} vs {actual}"
        );
    }
    let draft = Predicate::And(vec![
        Predicate::compare("status", Compare::Eq, 500),
        Predicate::Contains {
            field: "message".into(),
            text: "hello".into(),
        },
    ]);
    let preview = plan.preview(&draft);
    assert!(!preview.complete());
    assert_eq!(preview.pending(), ["message"]);
    assert_eq!(preview.count(&row).unwrap(), CountBounds::exact(3));
    let either = Predicate::Or(vec![
        Predicate::compare("status", Compare::Eq, 400),
        Predicate::Contains {
            field: "message".into(),
            text: "hello".into(),
        },
    ]);
    assert_eq!(
        plan.preview(&either).count(&row).unwrap(),
        CountBounds::exact(6)
    );
}

#[test]
fn persisted_state_merges_and_equal_width_category_versions_remain_distinct() {
    let source = source();
    let plan = source.summaries(1).unwrap();
    let mut bucket = plan.bucket();
    let mut events = source.fill(1).unwrap();
    events
        .load(
            plan.ingest(&mut bucket, 10u64, |v, s| {
                s.set_uint("duration", v)?.set_enum("status", 200.into())?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    events.seal().unwrap();
    let pack = plan.layout().pack([plan.seed(&bucket).unwrap()]).unwrap();
    let opened = plan
        .layout()
        .definition()
        .prescription()
        .open(pack.bytes())
        .unwrap();
    let mut reader = opened.reader();
    let restored = plan.restore(&reader.get(0).unwrap()).unwrap();
    plan.merge(&mut bucket, &restored).unwrap();
    let merged = plan.layout().pack([plan.seed(&bucket).unwrap()]).unwrap();
    let opened = plan
        .layout()
        .definition()
        .prescription()
        .open(merged.bytes())
        .unwrap();
    let mut reader = opened.reader();
    assert_eq!(
        value(
            &plan.field(&reader.get(0).unwrap(), "duration").unwrap(),
            "sum"
        ),
        &20.into()
    );
    let definition = |labels: [u64; 2]| {
        doc::new("category-shape")
            .field(
                Field::new("status")
                    .closed_enum("uint", labels.map(Into::into))
                    .annotation("#summ(categorical)"),
            )
            .sign()
            .unwrap()
    };
    let a = SeedDefinition::prescribe(&definition([200, 400]))
        .unwrap()
        .summaries(1)
        .unwrap();
    let b = SeedDefinition::prescribe(&definition([200, 500]))
        .unwrap()
        .summaries(1)
        .unwrap();
    let pa = a.layout().pack([a.seed(&a.bucket()).unwrap()]).unwrap();
    let pb = b.layout().pack([b.seed(&b.bucket()).unwrap()]).unwrap();
    assert_eq!(pa.bytes().len(), pb.bytes().len());
    let mut file = SummaryBox::new(a.layout().clone()).unwrap();
    file.append(pa).unwrap();
    file.append(pb).unwrap();
    assert_eq!(file.definition_transitions().len(), 2);
    file.commit().unwrap();
    let reader = SummaryBoxReader::open(file.bytes().unwrap()).unwrap();
    assert!(!reader.is_uniform());
    assert!(a.check_box(&reader).is_err(), "a single plan must not decode the other version");
    let pack = reader.pack(1).unwrap();
    let mut cursor = pack.reader();
    let row = cursor.get(0).unwrap();
    let labels = b.field(&row, "status").unwrap();
    let Data::Map(values) = value(&labels, "values") else { panic!() };
    assert_eq!(values[1].0, 500.into());
}

#[test]
fn compensated_float_sum_survives_storage_and_merge() {
    let doc = doc::new("float-summary")
        .field(
            Field::new("value")
                .r#type("float")
                .size(8)
                .annotation("#summ(numeric)"),
        )
        .sign()
        .unwrap();
    let source = SeedDefinition::prescribe(&doc).unwrap();
    let plan = source.summaries(1).unwrap();
    let mut bucket = plan.bucket();
    let mut events = source.fill(3).unwrap();
    for n in [1e16, 1.0, -1e16] {
        events
            .load(
                plan.ingest(&mut bucket, n, |n, s| {
                    s.set_float("value", n)?;
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
    }
    events.seal().unwrap();
    let pack = plan.layout().pack([plan.seed(&bucket).unwrap()]).unwrap();
    let opened = plan
        .layout()
        .definition()
        .prescription()
        .open(pack.bytes())
        .unwrap();
    let mut reader = opened.reader();
    let row = reader.get(0).unwrap();
    assert_eq!(
        value(&plan.field(&row, "value").unwrap(), "sum"),
        &Data::Float(1.0)
    );
    let restored = plan.restore(&row).unwrap();
    plan.merge(&mut bucket, &restored).unwrap();
    let pack = plan.layout().pack([plan.seed(&bucket).unwrap()]).unwrap();
    let opened = plan
        .layout()
        .definition()
        .prescription()
        .open(pack.bytes())
        .unwrap();
    let mut reader = opened.reader();
    assert_eq!(
        value(
            &plan.field(&reader.get(0).unwrap(), "value").unwrap(),
            "sum"
        ),
        &Data::Float(2.0)
    );
}

#[test]
fn arbitrary_windows_roll_up_and_preview_whole_periods_without_changing_event_bounds() {
    use data_seeds::{Compare, Predicate};
    let source = source();
    let plan = source.summaries(12).unwrap();
    let mut buckets = (0..12).map(|_| plan.bucket()).collect::<Vec<_>>();
    let mut events = source.fill(78).unwrap();
    for (i, bucket) in buckets.iter_mut().enumerate() {
        for _ in 0..=i {
            events
                .load(
                    plan.ingest(bucket, i as u64 * 100, |n, s| {
                        s.set_uint("duration", n)?;
                        Ok(())
                    })
                    .unwrap(),
                )
                .unwrap();
        }
    }
    events.seal().unwrap();
    let pack = plan
        .layout()
        .pack(buckets.iter().map(|b| plan.seed(b).unwrap()))
        .unwrap();
    let opened = plan
        .layout()
        .definition()
        .prescription()
        .open(pack.bytes())
        .unwrap();
    let first = 10 * 3_600_000;
    let window = SummaryWindow::new(first + 120_000..first + 1_020_000).unwrap();
    assert_eq!(window.rows(first, 12).unwrap(), 0..4);
    let mut rollup = plan.bucket();
    assert_eq!(
        plan.rollup(&mut rollup, &opened, first, &window).unwrap(),
        4
    );
    assert_eq!(rollup.count(), 10);
    let filter = Predicate::compare("duration", Compare::Gt, 150);
    let preview = plan.preview(&filter);
    let mut scan = preview.window(window.requested()).unwrap();
    let mut reader = opened.reader();
    for i in (0..12).rev() {
        scan.observe(first + i * 300_000, &reader.get(i as usize).unwrap(), true)
            .unwrap();
    }
    assert!(scan.observe(first, &reader.get(0).unwrap(), true).is_err());
    let result = scan.finish().unwrap();
    assert_eq!(result.count, Some(CountBounds::exact(7)));
    assert!(result.coverage_complete);
    assert_eq!(result.periods.len(), 4);
    assert_eq!(result.eligible, vec![first + 600_000..first + 1_020_000]);
    let mut missing = preview.window(window.requested()).unwrap();
    missing
        .observe(first, &reader.get(0).unwrap(), true)
        .unwrap();
    let result = missing.finish().unwrap();
    assert!(!result.coverage_complete);
    assert_eq!(result.count, None);
    assert_eq!(result.eligible, vec![first + 300_000..first + 1_020_000]);
    let impossible = plan.preview(&Predicate::compare("duration", Compare::Gt, 10_000));
    let mut empty = impossible.window(window.requested()).unwrap();
    for i in 0..4 {
        empty
            .observe(first + i * 300_000, &reader.get(i as usize).unwrap(), true)
            .unwrap();
    }
    let result = empty.finish().unwrap();
    assert_eq!(result.count, Some(CountBounds::exact(0)));
    assert!(result.eligible.is_empty());
}

