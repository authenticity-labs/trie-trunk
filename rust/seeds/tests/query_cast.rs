use data_seeds::{
    cast::{Spec, Target},
    *,
};
fn fixture() -> (std::sync::Arc<SeedDefinition>, Packs) {
    let document = doc::new("casts")
        .field(Field::new("text").r#type("tstr").size(0))
        .field(
            Field::new("elapsed")
                .r#type("tstr")
                .size(0)
                .annotation("@unit(us)"),
        )
        .sign()
        .unwrap();
    let d = SeedDefinition::prescribe(&document).unwrap();
    let mut pack = d.fill(6).unwrap();
    for text in [
        Some("12"),
        Some("invalid"),
        Some("32768"),
        Some("1.5"),
        Some("null"),
        None,
    ] {
        pack.load(
            d.ingest(text, |text, p| {
                match text {
                    Some("null") => {
                        p.set_null("text")?;
                    }
                    Some(s) => {
                        p.set_text_string("text", s)?;
                    }
                    None => {}
                }
                p.set_text_string("elapsed", "250 ms")?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    }
    pack.seal().unwrap();
    let bytes = pack.into_bytes().unwrap();
    if let Ok(dir) = std::env::var("SEED_CAST_FIXTURE") {
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(format!("{dir}/casts.rx"), document.bytes().unwrap()).unwrap();
        std::fs::write(format!("{dir}/casts.pac"), &bytes).unwrap();
    }
    let mut beds = Packs::new();
    beds.pack(d.open(bytes).unwrap());
    (d, beds)
}
fn spec(tolerant: bool) -> Spec {
    Spec {
        target: Target::Int,
        bytes: 2,
        tolerant,
    }
}
#[test]
fn casts_filter_project_and_preserve_absence() {
    let (_, beds) = fixture();
    let q = Query::new()
        .select(["number"])
        .cast("text", "number", spec(true));
    let mut cursor = beds.query(q).unwrap();
    let batch = cursor.fetch(10).unwrap();
    let rows = batch.objects().unwrap();
    assert_eq!(rows[0][0].1, ciborium::Value::Integer(12.into()));
    for row in &rows[1..5] {
        assert_eq!(row[0].1, ciborium::Value::Null);
    }
    assert!(rows[5].is_empty());
    let mut strict = beds
        .query(Query::new().cast("text", "number", spec(false)))
        .unwrap();
    assert!(strict.fetch(10).unwrap().objects().is_err());
    for (p, n) in [
        (
            Predicate::Cast {
                field: "text".into(),
                cast: spec(true),
                op: Compare::Gt,
                value: 10.into(),
            },
            1,
        ),
        (
            Predicate::Not(Box::new(Predicate::Cast {
                field: "text".into(),
                cast: spec(true),
                op: Compare::Gt,
                value: 10.into(),
            })),
            0,
        ),
        (
            Predicate::CastNull {
                field: "text".into(),
                cast: spec(true),
            },
            4,
        ),
    ] {
        assert_eq!(
            beds.query(Query::new().filter(p))
                .unwrap()
                .fetch(10)
                .unwrap()
                .len(),
            n
        );
    }
    let q = Query::new().select(["micros"]).cast(
        "elapsed",
        "micros",
        Spec {
            bytes: 4,
            ..spec(false)
        },
    );
    assert!(
        beds.query(q)
            .unwrap()
            .fetch(10)
            .unwrap()
            .objects()
            .unwrap()
            .iter()
            .all(|r| r[0].1 == ciborium::Value::Integer(250000.into()))
    );
}
#[cfg(feature = "query-language")]
#[test]
fn sql_casts_are_structural_and_unsupported_targets_fail() {
    let names = vec!["text".into()];
    let q=data_seeds::language::parse("SELECT TRY_CAST(text AS SMALLINT) AS number FROM events WHERE TRY_CAST(text AS INTEGER) >= 10",&names).unwrap();
    assert_eq!(q["plan"]["relational"]["selection"][0]["cast"]["bytes"], 2);
    assert_eq!(q["plan"]["predicate"]["filter"]["cast"]["bytes"], 4);
    for sql in [
        "SELECT CAST(text AS DECIMAL(10,2)) FROM events",
        "SELECT CAST(text AS INTEGER) FROM summaries",
        "SELECT CAST(text AS INTEGER) + 1 FROM events",
    ] {
        assert!(data_seeds::language::parse(sql, &names).is_err(), "{sql}");
    }
}
