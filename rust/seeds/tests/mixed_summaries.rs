use ciborium::Value as Data;
use data_seeds::{Compare, EncodingMode, Field, SeedDefinition, doc, summaries::*};
use std::sync::Arc;

fn source(width: u16, labels: &[&str], renamed: &str, boolean: bool) -> Arc<SeedDefinition> {
    let changing = if boolean {
        Field::new("changing")
            .r#type("bool")
            .annotation("#summ(boolean)")
    } else {
        Field::new("changing")
            .r#type("uint")
            .size(16)
            .annotation("#summ(numeric)")
    };
    let d = doc::new(format!("events-{width}-{renamed}-{boolean}"))
        .field(
            Field::new("duration")
                .r#type("uint")
                .size(width)
                .annotation("#summ(numeric)"),
        )
        .field(
            Field::new("status")
                .closed_enum("tstr", labels.iter().map(|s| (*s).into()))
                .annotation("#summ(categorical)"),
        )
        .field(
            Field::new(renamed)
                .r#type("int")
                .size(16)
                .annotation("#summ(numeric)"),
        )
        .field(changing)
        .sign()
        .unwrap();
    SeedDefinition::prescribe(&d).unwrap()
}
fn member<'a>(value: &'a Data, name: &str) -> &'a Data {
    value
        .as_map()
        .unwrap()
        .iter()
        .find(|(k, _)| k.as_text() == Some(name))
        .map(|(_, v)| v)
        .unwrap()
}
fn contribute(
    plan: &SummaryPlan,
    bucket: &mut SummaryBucket,
    duration: u64,
    label: &str,
    renamed: &str,
    boolean: bool,
) {
    let seed = plan
        .ingest(bucket, duration, |duration, seed| {
            seed.set_uint("duration", duration)?
                .set_enum("status", label.into())?
                .set_int(renamed, -(1i128 << 100))?;
            if boolean {
                seed.set_bool("changing", true)?;
            } else {
                seed.set_uint("changing", (1u128 << 100) + 7)?;
            }
            Ok(())
        })
        .unwrap();
    // Test events use the real ingress and builder path.
    seed.cap().unwrap();
}
#[test]
#[ignore = "TODO: custom log class changes; retained for the deferred mixed-class design"]
fn mixed_periods_merge_losslessly_without_adding_period_rows() {
    let a = source(2, &["short", "1234567890"], "old_name", false)
        .summaries(288)
        .unwrap();
    let b = source(4, &["12345678901234567890", "short"], "new_name", false)
        .summaries(288)
        .unwrap();
    let c = source(2, &["short", "12345678"], "new_name", true)
        .summaries(288)
        .unwrap();
    let mixed = MixedSummaryPlan::new(&[a.clone(), b.clone(), c.clone()]).unwrap();
    let mut aa = a.bucket();
    let mut bb = b.bucket();
    let mut cc = c.bucket();
    contribute(&a, &mut aa, 65_535, "1234567890", "old_name", false);
    contribute(
        &b,
        &mut bb,
        4_000_000_000,
        "12345678901234567890",
        "new_name",
        false,
    );
    contribute(&c, &mut cc, 42, "12345678", "new_name", true);
    let mut transition = mixed.bucket();
    mixed.merge(&mut transition, &a, &aa).unwrap();
    mixed.merge(&mut transition, &b, &bb).unwrap();
    // A rollback extends the same logical period, never another row.
    mixed.merge(&mut transition, &a, &aa).unwrap();
    let mut incompatible = mixed.bucket();
    mixed.merge(&mut incompatible, &b, &bb).unwrap();
    mixed.merge(&mut incompatible, &c, &cc).unwrap();
    let mut after = mixed.bucket();
    mixed.merge(&mut after, &c, &cc).unwrap();
    let empty = mixed.bucket();
    let pack = mixed
        .layout()
        .pack((0..288).map(|i| {
            mixed
                .seed(match i {
                    100 => &transition,
                    101 => &incompatible,
                    102 => &after,
                    _ => &empty,
                })
                .unwrap()
        }))
        .unwrap();
    let empty_pack = mixed
        .layout()
        .pack((0..288).map(|_| mixed.seed(&empty).unwrap()))
        .unwrap();
    assert_eq!(mixed.layout().open(empty_pack.bytes()).unwrap().len(), 288);
    let opened = mixed.layout().open(pack.bytes()).unwrap();
    assert_eq!(opened.len(), 288);
    assert_ne!(opened.options().address_bytes, 0);
    let mut reader = opened.reader();
    let row = reader.get(100).unwrap();
    assert_eq!(row.mode(), EncodingMode::FixedSparse);
    row.validate().unwrap();
    let duration = mixed.field(&row, "duration").unwrap();
    assert_eq!(member(&duration, "sum"), &Data::from(4_000_131_070u64));
    assert_eq!(member(&duration, "max"), &Data::from(4_000_000_000u64));
    assert_eq!(member(&duration, "count"), &Data::from(3u64));
    let status = mixed.field(&row, "status").unwrap();
    let labels = member(&status, "values");
    assert_eq!(
        member(member(labels, "12345678901234567890"), "count"),
        &Data::from(1u64)
    );
    assert_eq!(
        member(member(labels, "1234567890"), "count"),
        &Data::from(2u64)
    );
    let exact = mixed.field(&row, "changing").unwrap();
    assert_eq!(
        pil_codec::unsigned_native(member(&exact, "sum")).unwrap(),
        3 * ((1u128 << 100) + 7)
    );
    for name in ["old_name", "new_name"] {
        assert_eq!(mixed.field(&row, name).unwrap(), Data::Null);
        let bounds = mixed
            .comparison(&row, name, Compare::Gt, &0.into())
            .unwrap();
        assert_eq!((bounds.lower, bounds.upper), (0, 3));
    }
    let row = reader.get(101).unwrap();
    let raw = row.crush().unwrap();
    assert!(!raw.contains_key("states"));
    assert!(
        raw["fields"]
            .as_ref()
            .unwrap()
            .as_object()
            .unwrap()
            .values()
            .any(|v| v.is_null())
    );
    assert_eq!(mixed.field(&row, "changing").unwrap(), Data::Null);
    let bounds = mixed
        .comparison(&row, "changing", Compare::Eq, &false.into())
        .unwrap();
    assert_eq!((bounds.lower, bounds.upper), (0, 2));
    let exact = mixed.field(&row, "new_name").unwrap();
    assert_eq!(
        pil_codec::signed_native(member(&exact, "sum")).unwrap(),
        -(2i128 << 100)
    );
    // The incompatible version alone remains useful, not poisoned by the
    // fact that another period used a different class under the same name.
    let row = reader.get(102).unwrap();
    assert_eq!(
        member(&mixed.field(&row, "changing").unwrap(), "true_count"),
        &1u64.into()
    );
    let row = reader.get(287).unwrap();
    assert_eq!(mixed.field(&row, "duration").unwrap(), Data::Null);
    assert_eq!(
        mixed
            .comparison(&row, "duration", Compare::Gt, &0.into())
            .unwrap()
            .upper,
        0
    );
}

#[test]
fn daily_commit_combines_source_accumulators_only_after_all_complete() {
    let a = source(2, &["a"], "value", false).summaries(288).unwrap();
    let b = source(4, &["b"], "value", false).summaries(288).unwrap();
    let mut sources = [a.accumulator(0).unwrap(), b.accumulator(0).unwrap()];
    for (i, source) in sources.iter_mut().enumerate() {
        source
            .ingest(5 * SUMMARY_PERIOD_MS + i as u64, i, |i, seed| {
                seed.set_uint("duration", 10u64 + i as u64)?
                    .set_enum("status", if i == 0 { "a".into() } else { "b".into() })?
                    .set_int("value", 20i64)?
                    .set_uint("changing", 30u64)?;
                Ok(())
            })
            .unwrap();
    }
    sources[0].complete(SUMMARY_DAY_MS).unwrap();
    assert!(SummaryAccumulator::seal_mixed(&mut sources).is_err());
    assert!(sources.iter().all(|s| !s.is_sealed()));
    assert_eq!(sources[0].bucket(5).unwrap().count(), 1);
    sources[1].complete(SUMMARY_DAY_MS).unwrap();
    let (plan, pack) = SummaryAccumulator::seal_mixed(&mut sources).unwrap();
    assert!(sources.iter().all(|s| s.is_sealed()));
    assert!(SummaryAccumulator::seal_mixed(&mut sources).is_err());
    let opened = plan.layout().open(pack.bytes()).unwrap();
    assert_eq!(opened.len(), 288);
    assert_eq!(opened.options().address_bytes, 0);
    let projection = SummaryProjection::from_layout(plan.layout().clone()).unwrap();
    let periods = projection.periods(opened.headers()).unwrap().unwrap();
    let mut reader = opened.reader();
    assert_eq!(reader.get(5).unwrap().mode(), EncodingMode::FixedSparse);
    assert_eq!(
        member(
            &periods.period(5).unwrap().field(&reader.get(5).unwrap(), "duration").unwrap(),
            "sum"
        ),
        &21u64.into()
    );
    let empty = periods.period(6).unwrap().field(&reader.get(6).unwrap(), "duration").unwrap();
    assert_eq!(member(&empty, "count"), &0u64.into());
    assert_eq!(member(&empty, "min"), &Data::Null);
    drop(reader);
    drop(opened);
    let mut container = SummaryBox::new(a.layout().clone()).unwrap();
    container
        .append(
            a.layout()
                .pack((0..288).map(|_| a.seed(&a.bucket()).unwrap()))
                .unwrap(),
        )
        .unwrap();
    container.append(pack).unwrap();
    container
        .append(
            a.layout()
                .pack((0..288).map(|_| a.seed(&a.bucket()).unwrap()))
                .unwrap(),
        )
        .unwrap();
    container.commit().unwrap();
    let container = SummaryBoxReader::open(container.bytes().unwrap()).unwrap();
    assert_eq!(
        container.layout_at(0).unwrap().mode(),
        EncodingMode::FixedFull
    );
    assert_eq!(
        container.layout_at(1).unwrap().mode(),
        EncodingMode::FixedSparse
    );
    assert_eq!(
        container.layout_at(2).unwrap().mode(),
        EncodingMode::FixedFull
    );
    let reopened = container.pack(1).unwrap();
    let mut reader = reopened.reader();
    let empty = periods.period(6).unwrap().field(&reader.get(6).unwrap(), "duration").unwrap();
    assert_eq!(member(&empty, "count"), &0u64.into());
    assert_eq!(member(&empty, "min"), &Data::Null);
}

#[test]
#[ignore = "TODO: custom log signedness changes; retained for the deferred mixed-class design"]
fn signedness_is_never_reinterpreted_and_overflow_is_not_wrapped() {
    let make = |kind: &str| {
        let d = doc::new(format!("{kind}-events"))
            .field(
                Field::new("n")
                    .r#type(kind)
                    .size(16)
                    .annotation("#summ(numeric)"),
            )
            .sign()
            .unwrap();
        SeedDefinition::prescribe(&d).unwrap().summaries(1).unwrap()
    };
    let unsigned = make("uint");
    let signed = make("int");
    let mixed = MixedSummaryPlan::new(&[unsigned.clone(), signed.clone()]).unwrap();
    let mut u = unsigned.bucket();
    unsigned
        .ingest(&mut u, u128::MAX, |v, seed| {
            seed.set_uint("n", v)?;
            Ok(())
        })
        .unwrap()
        .cap()
        .unwrap();
    let mut s = signed.bucket();
    signed
        .ingest(&mut s, i128::MIN, |v, seed| {
            seed.set_int("n", v)?;
            Ok(())
        })
        .unwrap()
        .cap()
        .unwrap();
    let mut conflict = mixed.bucket();
    mixed.merge(&mut conflict, &unsigned, &u).unwrap();
    mixed.merge(&mut conflict, &signed, &s).unwrap();
    let pack = mixed
        .layout()
        .pack([mixed.seed(&conflict).unwrap()])
        .unwrap();
    let opened = mixed.layout().open(pack.bytes()).unwrap();
    let mut reader = opened.reader();
    let row = reader.get(0).unwrap();
    assert_eq!(mixed.field(&row, "n").unwrap(), Data::Null);
    assert_eq!(
        mixed
            .comparison(&row, "n", Compare::Gt, &0.into())
            .unwrap()
            .upper,
        2
    );
    let mut overflow = mixed.bucket();
    mixed.merge(&mut overflow, &unsigned, &u).unwrap();
    mixed.merge(&mut overflow, &unsigned, &u).unwrap();
    let pack = mixed
        .layout()
        .pack([mixed.seed(&overflow).unwrap()])
        .unwrap();
    let opened = mixed.layout().open(pack.bytes()).unwrap();
    let mut reader = opened.reader();
    let row = reader.get(0).unwrap();
    let pil_codec::api::NativeValue::Composite(raw_fields) = row.native_value(1).unwrap() else {
        panic!()
    };
    assert_eq!(member(member(&raw_fields, "n"), "sum"), &Data::Null);
    let value = mixed.field(&row, "n").unwrap();
    assert_eq!(member(&value, "sum"), &Data::Null);
    assert_eq!(
        pil_codec::unsigned_native(member(&value, "max")).unwrap(),
        u128::MAX
    );
}

#[test]
fn sparse_normalization_keeps_full_integer_domains_and_rejects_variable_layouts() {
    let document = doc::new("fixed-scalars")
        .field(Field::new("u").r#type("uint").size(16))
        .field(Field::new("i").r#type("int").size(16))
        .field(Field::new("b").r#type("bool"))
        .sign()
        .unwrap();
    let source = SeedDefinition::prescribe(&document).unwrap();
    let mut seed = source
        .ingest((), |(), seed| {
            seed.set_uint("u", u128::MAX)?
                .set_int("i", i128::MIN)?
                .set_bool("b", true)?;
            Ok(())
        })
        .unwrap();
    assert_eq!(seed.mode().unwrap(), EncodingMode::FixedFull);
    seed.sparse().unwrap();
    assert_eq!(seed.mode().unwrap(), EncodingMode::FixedSparse);
    let first = seed.bytes().unwrap().into_owned();
    seed.sparse().unwrap();
    assert_eq!(seed.bytes().unwrap().as_ref(), first);
    let mut decoder = source.prescription().decoder();
    let row = decoder.read(&first).unwrap();
    row.validate().unwrap();
    assert!(matches!(
        row.native_value(0).unwrap(),
        pil_codec::api::NativeValue::Scalar(pil_codec::Value::U128(u128::MAX))
    ));
    assert!(matches!(
        row.native_value(1).unwrap(),
        pil_codec::api::NativeValue::Scalar(pil_codec::Value::I128(i128::MIN))
    ));
    let document = doc::new("variable")
        .field(Field::new("text").r#type("tstr"))
        .sign()
        .unwrap();
    let source = SeedDefinition::prescribe(&document).unwrap();
    let mut seed = source
        .ingest("abc", |v, seed| {
            seed.set_text_string("text", v)?;
            Ok(())
        })
        .unwrap();
    assert!(seed.sparse().is_err());
    assert_eq!(seed.mode().unwrap(), EncodingMode::VariableFast);
}

#[test]
fn custom_log_class_changes_fail_explicitly_until_supported() {
    let numeric = source(2, &["ok"], "value", false).summaries(288).unwrap();
    let boolean = source(2, &["ok"], "value", true).summaries(288).unwrap();
    let error = match MixedSummaryPlan::new(&[numeric, boolean]) {
        Ok(_) => panic!("incompatible classes must not reinterpret stored slots"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("class or unit changes are not supported"));
}
