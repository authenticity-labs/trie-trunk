use data_seeds::{
    cast::{self, Context, DateOrder},
    *,
};

#[test]
fn units_are_exact_and_not_an_expression_evaluator() {
    for (text, target, want) in [
        ("250ms", "us", 250000),
        ("1h30m", "s", 5400),
        ("01:30:00", "s", 5400),
        ("1e3 ms", "s", 1),
        ("6 feet", "um", 1828800),
        ("98.6 °F", "°C", 37),
        ("12 MiB", "bytes", 12582912),
        ("9007199254740993 ms", "us", 9007199254740993000),
    ] {
        assert_eq!(
            cast::number(text, Some(target))
                .unwrap_or_else(|e| panic!("{text}: {e}"))
                .uint()
                .unwrap(),
            want
        );
    }
    assert_eq!(
        cast::number("−1.5 seconds", Some("ms"))
            .unwrap()
            .int()
            .unwrap(),
        -1500
    );
    assert_eq!(
        cast::number(&u128::MAX.to_string(), None)
            .unwrap()
            .uint()
            .unwrap(),
        u128::MAX
    );
    assert!(cast::number("1.5", None).unwrap().int().is_err());
    for text in [
        "1+2",
        "x=10",
        "12 ms garbage",
        "250 ms; 2",
        "1e100000",
        "NaN",
        "infinity",
        "1/2",
        "2 s to hours",
    ] {
        assert!(cast::number(text, Some("ms")).is_err(), "accepted {text}");
    }
    assert!(cast::number("2 ft", Some("ms")).is_err());
    assert!(
        cast::number("2 months", Some("s")).is_err(),
        "calendar months need a reference date"
    );
}
#[test]
fn currencies_and_separators_do_not_guess_exchange_rates() {
    for (text, target, want) in [
        ("$1,234.50", "USD", 1234.5),
        ("€1.234,50", "EUR", 1234.5),
        ("£12.50", "GBP", 12.5),
        ("¥1200", "JPY", 1200.0),
        ("12.50 USD", "USD", 12.5),
    ] {
        assert_eq!(
            cast::number(text, Some(target)).unwrap().float().unwrap(),
            want
        );
    }
    assert_eq!(
        cast::number("1,5 s", Some("ms")).unwrap().uint().unwrap(),
        1500
    );
    for (text, target) in [
        ("$12", Some("EUR")),
        ("¥12", None),
        ("1,234", None),
        ("1,23.50", None),
    ] {
        assert!(cast::number(text, target).is_err(), "accepted {text}");
    }
}
fn context() -> Context {
    Context {
        reference_ms: Some(
            chrono::DateTime::parse_from_rfc3339("2026-03-09T16:00:00Z")
                .unwrap()
                .timestamp_millis(),
        ),
        timezone: "America/New_York".into(),
        date_order: None,
    }
}
#[test]
fn temporal_parsing_resolves_calendar_and_elapsed_time_separately() {
    let c = context();
    let parse = |s| cast::timestamp(s, &c).unwrap();
    assert_eq!(
        parse("now"),
        i128::from(c.reference_ms.unwrap()) * 1_000_000
    );
    assert_eq!(parse("2 hours ago"), parse("2026-03-09T14:00:00Z"));
    assert_eq!(parse("1 day ago"), parse("2026-03-08T16:00:00Z"));
    assert_eq!(parse("2 days ago"), parse("2026-03-07T17:00:00Z"));
    assert_eq!(parse("today"), parse("2026-03-09T04:00:00Z"));
    assert_eq!(parse("2026-03-09 12:00:00"), parse("now"));
    assert_eq!(
        parse("2026-03-09T16:00:00.123456789Z") % 1_000_000_000,
        123456789
    );
    for s in [
        "2026-03-08 02:30:00",
        "2026-11-01 01:30:00",
        "03/04/2026",
        "now nonsense",
        "12pm meeting",
    ] {
        assert!(cast::timestamp(s, &c).is_err(), "accepted {s}");
    }
    let mut m = c.clone();
    m.date_order = Some(DateOrder::DayFirst);
    assert_eq!(
        cast::timestamp("03/04/2026", &m).unwrap(),
        parse("2026-04-03")
    );
}
fn definition() -> std::sync::Arc<SeedDefinition> {
    SeedDefinition::prescribe(
        &doc::new("coercion")
            .field(
                Field::new("duration")
                    .r#type("uint")
                    .size(4)
                    .annotation("@unit(us)")
                    .annotation("#summ(numeric)"),
            )
            .field(
                Field::new("time")
                    .r#type("uint")
                    .size(6)
                    .annotation("@time(ms)"),
            )
            .field(Field::new("active").r#type("bool"))
            .field(Field::new("message").r#type("tstr"))
            .sign()
            .unwrap(),
    )
    .unwrap()
}
#[test]
fn timestamp_annotation_requires_integer_milliseconds() {
    for (kind, tag, valid) in [
        ("int", "@timestamp(ms)", true),
        ("uint", "@timestamp(ms)", true),
        ("float", "@timestamp(ms)", false),
        ("int", "@timestamp", false),
        ("int", "@timestamp(s)", false),
    ] {
        let document = doc::new("reading")
            .field(Field::new("observed").r#type(kind).size(8).annotation(tag))
            .sign().unwrap();
        assert_eq!(SeedDefinition::prescribe(&document).is_ok(), valid, "{kind} {tag}");
    }
}
#[test]
fn text_predicates_use_the_same_typed_executor_and_summary_threshold() {
    let d = definition();
    let plan = d.summaries(1).unwrap();
    let mut bucket = plan.bucket();
    let mut pack = d.fill(5).unwrap();
    for n in [
        Some(Some(1000)),
        Some(Some(2000)),
        Some(Some(3000)),
        Some(None),
        None,
    ] {
        pack.load(
            plan.ingest(&mut bucket, n, |n, p| {
                match n {
                    Some(Some(n)) => {
                        p.set_uint("duration", n)?;
                    }
                    Some(None) => {
                        p.set_null("duration")?;
                    }
                    None => {}
                }
                p.set_uint("time", 1773072000000u64)?;
                p.set_bool("active", true)?;
                p.set_text_string("message", "2000")?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    }
    pack.seal().unwrap();
    let mut beds = Packs::new();
    beds.pack(d.open(pack.into_bytes().unwrap()).unwrap());
    let rows = |p| {
        beds.query(Query::new().filter(p))
            .unwrap()
            .fetch(10)
            .unwrap()
            .objects()
            .unwrap()
            .len()
    };
    for (op, text, count) in [
        (Compare::Ge, "2ms", 2),
        (Compare::Gt, "1999.5", 2),
        (Compare::Lt, "2000.5", 2),
        (Compare::Eq, "2000.5", 0),
        (Compare::Ne, "2000.5", 3),
    ] {
        assert_eq!(rows(Predicate::compare("duration", op, text)), count);
    }
    assert_eq!(rows(Predicate::compare("active", Compare::Eq, "yes")), 5);
    assert_eq!(rows(Predicate::compare("message", Compare::Eq, "2000")), 5);
    let predicate = Predicate::compare("duration", Compare::Gt, "3ms");
    let summary = plan.layout().pack([plan.seed(&bucket).unwrap()]).unwrap();
    let opened = plan
        .layout()
        .definition()
        .prescription()
        .open(summary.bytes())
        .unwrap();
    let mut reader = opened.reader();
    let preview = plan.preview(&predicate);
    assert!(preview.complete());
    assert_eq!(preview.count(&reader.get(0).unwrap()).unwrap().upper, 0);
    assert!(
        beds.query(Query::new().filter(Predicate::compare("duration", Compare::Eq, "bad")))
            .is_err()
    );
    let c = context();
    let a = beds
        .query(Query::new().context(c.clone()).filter(Predicate::compare(
            "time",
            Compare::Eq,
            "now",
        )))
        .unwrap()
        .fetch(10)
        .unwrap()
        .objects()
        .unwrap()
        .len();
    let mut later = c;
    later.reference_ms = Some(later.reference_ms.unwrap() + 1000);
    let b = beds
        .query(
            Query::new()
                .context(later)
                .filter(Predicate::compare("time", Compare::Eq, "now")),
        )
        .unwrap()
        .fetch(10)
        .unwrap()
        .objects()
        .unwrap()
        .len();
    assert_eq!(
        (a, b),
        (5, 0),
        "cached relative literals must not freeze the clock"
    );
}
