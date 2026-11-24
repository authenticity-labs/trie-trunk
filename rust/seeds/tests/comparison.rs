use data_seeds::comparison::{Kind, Number as N, Scalar as S};
use std::cmp::Ordering::{Equal, Greater, Less};

#[test]
fn numeric_boundaries_are_exact_and_symmetric() {
    let cases = [
        (N::Signed(-1), N::Unsigned(u128::MAX), Less),
        (N::Signed(i128::MAX), N::Unsigned(i128::MAX as u128), Equal),
        (N::Unsigned(u128::MAX), N::Float(2f64.powi(128)), Less),
        (N::Signed(i128::MIN), N::Float(-2f64.powi(127)), Equal),
        (N::Signed(i128::MIN + 1), N::Float(-2f64.powi(127)), Greater),
        (
            N::Unsigned(9_007_199_254_740_993),
            N::Float(9_007_199_254_740_992.0),
            Greater,
        ),
        (
            N::Signed(-9_007_199_254_740_993),
            N::Float(-9_007_199_254_740_992.0),
            Less,
        ),
        (N::Signed(-3), N::Float(-3.5), Greater),
        (N::Unsigned(3), N::Float(3.5), Less),
        (N::Unsigned(0), N::Float(f64::from_bits(1)), Less),
        (N::Unsigned(0), N::Float(-f64::from_bits(1)), Greater),
        (N::Unsigned(0), N::Float(-0.0), Equal),
        (N::Signed(i128::MIN), N::Float(f64::NEG_INFINITY), Greater),
        (N::Unsigned(u128::MAX), N::Float(f64::INFINITY), Less),
    ];
    for (a, b, expected) in cases {
        assert_eq!(a.compare(b), Some(expected), "{a:?} versus {b:?}");
        assert_eq!(b.compare(a), Some(expected.reverse()));
    }
}

#[test]
fn all_numeric_pairs_preserve_order_and_unknown() {
    // Exhaust both signs and fractional values; this range is exactly
    // representable, so the reference float comparison is lossless here.
    for i in -128i128..128 {
        for quarter in -513..513 {
            let f = f64::from(quarter) / 4.0;
            assert_eq!(
                N::Signed(i).compare(N::Float(f)),
                (i as f64).partial_cmp(&f)
            );
            if i >= 0 {
                assert_eq!(
                    N::Unsigned(i as u128).compare(N::Float(f)),
                    (i as f64).partial_cmp(&f)
                );
            }
        }
    }
    for n in [N::Signed(-1), N::Unsigned(0), N::Float(1.0)] {
        assert_eq!(n.compare(N::Float(f64::NAN)), None);
        assert_eq!(N::Float(f64::NAN).compare(n), None);
    }
}

#[test]
fn scalar_comparison_preserves_types_and_borrows_contents() {
    assert_eq!(
        S::Text("alpha").compare(S::Text("beta")).unwrap(),
        Some(Less)
    );
    assert_eq!(
        S::Bytes(&[0, 255]).compare(S::Bytes(&[1, 0])).unwrap(),
        Some(Less)
    );
    assert_eq!(S::Bool(false).compare(S::Bool(true)).unwrap(), Some(Less));
    let e = S::Text("12").compare(S::Number(N::Signed(12))).unwrap_err();
    assert_eq!((e.left, e.right), (Kind::Text, Kind::Number));
    assert!(S::Bool(true).compare(S::Number(N::Unsigned(1))).is_err());
    assert!(S::Bytes(b"12").compare(S::Text("12")).is_err());
    assert!(S::Composite.compare(S::Composite).is_err());
    for value in [
        S::Number(N::Signed(1)),
        S::Text("x"),
        S::Composite,
        S::Absent,
        S::Null,
    ] {
        assert_eq!(S::Absent.compare(value).unwrap(), None);
        assert_eq!(value.compare(S::Null).unwrap(), None);
    }
}

#[test]
fn pack_queries_compare_numeric_versions_and_coerce_query_text_exactly() {
    use ciborium::Value as Data;
    use data_seeds::{Compare, Direction, Field, Packs, Predicate, Query, SeedDefinition, doc};

    let mut packs = Packs::new();
    for (major, kind, size) in [(1, "int", 16), (2, "uint", 16), (3, "float", 8)] {
        let definition = SeedDefinition::prescribe(
            &doc::new("sample")
                .version(major, 0)
                .field(Field::new("value").r#type(kind).size(size))
                .sign()
                .unwrap(),
        )
        .unwrap();
        let mut pack = definition.fill(1).unwrap();
        pack.load(
            definition
                .ingest((), |(), seed| {
                    match kind {
                        "int" => {
                            seed.set_int("value", -1i128)?;
                        }
                        "uint" => {
                            seed.set_uint("value", 9_007_199_254_740_993u128)?;
                        }
                        _ => {
                            seed.set_float("value", 9_007_199_254_740_992.0)?;
                        }
                    }
                    Ok(())
                })
                .unwrap(),
        )
        .unwrap();
        pack.seal().unwrap();
        packs.pack(definition.open(pack.into_bytes().unwrap()).unwrap());
    }
    let query = Query::new().filter(Predicate::compare(
        "value",
        Compare::Gt,
        Data::Float(9_007_199_254_740_992.0),
    ));
    let rows = packs
        .query(query)
        .unwrap()
        .fetch(10)
        .unwrap()
        .objects()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        pil_codec::unsigned_native(&rows[0][0].1).unwrap(),
        9_007_199_254_740_993
    );
    let rows = packs
        .query(
            Query::new()
                .order_by("value", Direction::Ascending)
                .limit(10),
        )
        .unwrap()
        .fetch(10)
        .unwrap()
        .objects()
        .unwrap();
    assert_eq!(pil_codec::signed_native(&rows[0][0].1).unwrap(), -1);
    assert_eq!(rows[1][0].1, Data::Float(9_007_199_254_740_992.0));
    assert_eq!(
        pil_codec::unsigned_native(&rows[2][0].1).unwrap(),
        9_007_199_254_740_993
    );
    let mut query = packs
        .query(Query::new().filter(Predicate::compare(
            "value",
            Compare::Eq,
            Data::Text("-1".into()),
        )))
        .unwrap();
    assert_eq!(query.fetch(10).unwrap().objects().unwrap().len(),1);
}
