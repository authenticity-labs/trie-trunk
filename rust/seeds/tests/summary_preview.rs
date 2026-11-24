use data_seeds::{summaries::*, *};

#[test]
fn preview_counts_are_proofs_not_estimated_event_matches() {
    let source = SeedDefinition::prescribe(
        &doc::new("preview")
            .field(Field::new("duration").r#type("uint").size(4).annotation("#summ(numeric)"))
            .field(Field::new("message").r#type("tstr"))
            .sign()
            .unwrap(),
    )
    .unwrap();
    let plan = source.summaries(1).unwrap();
    let mut bucket = plan.bucket();
    let mut events = source.fill(5).unwrap();
    for value in [
        Some(Some(10u64)),
        Some(Some(20)),
        Some(Some(30)),
        Some(None),
        None,
    ] {
        let seed = plan
            .ingest(&mut bucket, value, |value, seed| {
                match value {
                    Some(Some(n)) => {
                        seed.set_uint("duration", n)?;
                    }
                    Some(None) => {
                        seed.set_null("duration")?;
                    }
                    None => {}
                }
                seed.set_text_string("message", "test")?;
                Ok(())
            })
            .unwrap();
        events.load(seed).unwrap();
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
    let compare = |op, n: u64| Predicate::compare("duration", op, n);
    for (predicate, expected) in [
        (Predicate::All, CountBounds::exact(5)),
        (compare(Compare::Gt, 30), CountBounds::exact(0)),
        (compare(Compare::Ge, 10), CountBounds::exact(3)),
        (compare(Compare::Le, 30), CountBounds::exact(3)),
        (compare(Compare::Ne, 0), CountBounds::exact(3)),
        (compare(Compare::Gt, 20), CountBounds { lower: 0, upper: 3 }),
        (Predicate::IsNull("duration".into()), CountBounds::exact(1)),
        (
            Predicate::IsAbsent("duration".into()),
            CountBounds::exact(1),
        ),
        (
            Predicate::IsPresent("duration".into()),
            CountBounds::exact(4),
        ),
        (
            Predicate::Not(Box::new(compare(Compare::Gt, 0))),
            CountBounds::exact(0),
        ),
        (
            Predicate::Not(Box::new(Predicate::IsNull("duration".into()))),
            CountBounds::exact(4),
        ),
    ] {
        let preview = plan.preview(&predicate);
        assert!(preview.complete());
        assert_eq!(preview.count(&row).unwrap(), expected, "{predicate:?}");
    }
    let unsupported = Predicate::Contains {
        field: "message".into(),
        text: "test".into(),
    };
    let prefix = plan.preview(&Predicate::And(vec![
        compare(Compare::Ge, 10),
        unsupported.clone(),
        compare(Compare::Gt, 30),
    ]));
    assert!(!prefix.complete());
    assert_eq!(prefix.fields(), ["duration"]);
    assert_eq!(prefix.pending(), ["message", "duration"]);
    assert_eq!(
        prefix.count(&row).unwrap(),
        CountBounds::exact(3),
        "stop at unsupported predicate"
    );
    let alternatives = plan.preview(&Predicate::Or(vec![compare(Compare::Gt, 30), unsupported]));
    assert!(!alternatives.complete());
    assert!(alternatives.fields().is_empty());
    assert_eq!(
        alternatives.count(&row).unwrap(),
        CountBounds::exact(5),
        "unsupported OR must preserve the window"
    );

    // Compare compound bounds with the actual shared query evaluator. This
    // catches accidental independence assumptions between fields/predicates.
    let snapshot = source.open(events.bytes().unwrap().to_vec()).unwrap();
    let mut packs = Packs::new();
    packs.pack(snapshot);
    for op in [
        Compare::Eq,
        Compare::Ne,
        Compare::Lt,
        Compare::Le,
        Compare::Gt,
        Compare::Ge,
    ] {
        for n in [0, 10, 15, 20, 30, 40] {
            for combine in [false, true] {
                let parts = vec![compare(op, n), Predicate::IsNull("duration".into())];
                let predicate = if combine {
                    Predicate::Or(parts)
                } else {
                    Predicate::And(parts)
                };
                for predicate in [predicate.clone(), Predicate::Not(Box::new(predicate))] {
                    let bounds = plan.preview(&predicate).count(&row).unwrap();
                    let mut cursor = packs.query(Query::new().filter(predicate.clone())).unwrap();
                    let matches = cursor.fetch(10).unwrap().objects().unwrap().len() as u64;
                    assert!(
                        bounds.lower <= matches && matches <= bounds.upper,
                        "{predicate:?}: {bounds:?} vs {matches}"
                    );
                }
            }
        }
    }
}

#[test]
fn summary_preview_resolves_names_in_each_definition_version() {
    for reversed in [false, true] {
        let value = Field::new("value").r#type("uint").size(4).annotation("#summ(numeric)");
        let label = Field::new("label").r#type("tstr");
        let document = if reversed {
            doc::new("reordered").field(label).field(value)
        } else {
            doc::new("reordered").field(value).field(label)
        }
        .sign()
        .unwrap();
        let source = SeedDefinition::prescribe(&document).unwrap();
        let plan = source.summaries(1).unwrap();
        let predicate = Predicate::compare("value", Compare::Gt, 0u64);
        let preview = plan.preview(&predicate);
        assert!(preview.complete());
        assert_eq!(preview.fields(), ["value"]);
        assert!(
            !plan
                .preview(&Predicate::compare("label", Compare::Eq, "test"))
                .complete()
        );
    }
}

#[test]
fn ordered_preview_storage_accepts_reordered_arrivals_and_rejects_duplicates() {
    let source = SeedDefinition::prescribe(
        &doc::new("preview")
            .field(
                Field::new("duration")
                    .r#type("uint")
                    .size(4)
                    .annotation("#summ(numeric)"),
            )
            .sign()
            .unwrap(),
    )
    .unwrap();
    let plan = source.summaries(1).unwrap();
    let mut bucket = plan.bucket();
    plan.ingest(&mut bucket, 20u32, |n, seed| {
        seed.set_uint("duration", n)?;
        Ok(())
    })
    .unwrap();
    let sealed = plan.layout().pack([plan.seed(&bucket).unwrap()]).unwrap();
    let pack = plan.layout().open(sealed.bytes()).unwrap();
    let mut cursor = pack.reader();
    let row = cursor.get(0).unwrap();
    let preview = plan.preview(&Predicate::All);
    let range = 0..3 * SUMMARY_PERIOD_MS;
    let mut combined = preview.window(range.clone()).unwrap();
    for n in [2, 0, 1] {
        combined.observe(n * SUMMARY_PERIOD_MS, &row, true).unwrap();
    }
    assert!(combined.observe(SUMMARY_PERIOD_MS, &row, true).is_err());
    let result = combined.finish().unwrap();
    assert_eq!(result.count, Some(CountBounds::exact(3)));
    assert_eq!(
        result.periods.iter().map(|p| p.time).collect::<Vec<_>>(),
        vec![0, SUMMARY_PERIOD_MS, 2 * SUMMARY_PERIOD_MS]
    );
    let mut parts = Vec::new();
    for n in [2, 0, 1] {
        let mut part = preview.window(range.clone()).unwrap();
        part.observe(n * SUMMARY_PERIOD_MS, &row, true).unwrap();
        parts.push(part.finish().unwrap());
    }
    let merged =
        SummaryWindowResult::combine(SummaryWindow::new(range.clone()).unwrap(), parts.clone())
            .unwrap();
    assert_eq!(merged.count, result.count);
    assert_eq!(merged.eligible, result.eligible);
    parts.push(parts[0].clone());
    assert!(SummaryWindowResult::combine(SummaryWindow::new(range).unwrap(), parts).is_err());
}
