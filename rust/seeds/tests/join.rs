#![cfg(feature = "query-language")]
use ciborium::Value as Data;
use data_seeds::{
    relational::join::{Join, Plan},
    *,
};
use serde_json::json;
use std::sync::{Arc, atomic::Ordering};

fn plan(sql: &str) -> Result<Plan, String> {
    let prepared = language::request(&json!({"op":"prepare", "text":sql, "fields":[]}))?;
    serde_json::from_value(prepared["plan"]["join"].clone()).map_err(|e| e.to_string())
}

const LEFT: &str = "SELECT a.n AS l, b.n AS r FROM events a LEFT JOIN events b ON a.k=b.k";

fn setup(sql: &str, memory: &MemoryBudget, limit: Option<usize>) -> Join {
    let d = doc::new("reading")
        .field(Field::new("k").r#type("uint").size(8))
        .field(Field::new("n").r#type("uint").size(1))
        .sign()
        .unwrap();
    let d = SeedDefinition::prescribe(&d).unwrap();
    let mut pac = d.fill(5).unwrap();
    for n in 0..5 {
        pac.load(
            d.ingest(n, |n, row| {
                row.set_uint("n", n)?;
                match n {
                    0 | 1 => {
                        row.set_uint("k", 1)?;
                    }
                    2 => {
                        row.set_null("k")?;
                    }
                    4 => {
                        row.set_uint("k", u64::MAX)?;
                    }
                    _ => {}
                }
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    }
    pac.seal().unwrap();
    let mut src = Packs::new();
    src.pack(d.open(pac.into_bytes().unwrap()).unwrap());
    let defs = std::slice::from_ref(&d);
    plan(sql)
        .unwrap()
        .bind([defs, defs])
        .unwrap()
        .open(memory.clone(), limit, |q| src.query(q), |q| src.query(q))
        .unwrap()
}

#[test]
fn duplicates_null_absence_and_global_limit() {
    let budget = MemoryBudget::new(1 << 20);
    let mut join = setup(LEFT, &budget, None);
    let mut got = vec![];
    while let Some(row) = join.next_row().unwrap() {
        got.push((row[0].1.clone(), row[1].1.clone()));
    }
    let expected = [
        (0, Some(0)),
        (0, Some(1)),
        (1, Some(0)),
        (1, Some(1)),
        (2, None),
        (3, None),
        (4, Some(4)),
    ];
    assert_eq!(
        got,
        expected.map(|(l, r)| (Data::from(l), r.map(Data::from).unwrap_or(Data::Null)))
    );
    assert_eq!(budget.used(), 0);
    let mut join = setup(LEFT, &budget, Some(1));
    assert!(join.next_row().unwrap().is_some());
    assert_eq!(budget.used(), 0);
    assert!(join.next_row().unwrap().is_none());
}

#[test]
fn cancellation_and_exhausted_budget_release_join_state() {
    let budget = MemoryBudget::new(1 << 20);
    let mut join = setup(LEFT, &budget, None);
    join.next_row().unwrap().unwrap();
    assert!(budget.used() > 0);
    join.cancellation()[1].store(true, Ordering::Relaxed);
    assert!(join.next_row().unwrap_err().to_string().contains("cancel"));
    assert_eq!(budget.used(), 0);
    assert!(join.next_row().unwrap().is_none());
    let small = MemoryBudget::new(600);
    let mut join = setup(LEFT, &small, None);
    assert!(join.next_row().is_err());
    assert_eq!(small.used(), 0);
    assert!(join.next_row().unwrap().is_none());
}

#[test]
fn invalid_join_criteria_fail_before_scanning() {
    let def = |kind: &str, size: u16| -> Arc<SeedDefinition> {
        let doc = doc::new("readings")
            .field(Field::new("k").r#type(kind).size(size))
            .field(Field::new("n").r#type("uint").size(1))
            .sign()
            .unwrap();
        SeedDefinition::prescribe(&doc).unwrap()
    };
    let a = [def("uint", 3)];
    let b = [def("tstr", 8)];
    assert!(
        plan(LEFT)
            .unwrap()
            .bind([&a, &b])
            .err()
            .unwrap()
            .to_string()
            .contains("Incompatible")
    );
    assert!(
        plan(&LEFT.replace("a.n AS l", "a.missing AS l"))
            .unwrap()
            .bind([&a, &a])
            .is_err()
    );
    for invalid in [
        format!("{LEFT} WHERE b.n=2"),
        format!("{LEFT} AND a.n=2"),
        LEFT.replace("a.k=b.k", "a.k>b.k"),
        LEFT.replace("a.k=b.k", "a.k=a.n"),
    ] {
        assert!(plan(&invalid).is_err(), "{invalid}");
    }
}

#[test]
fn numeric_join_compares_large_integer_keys_without_rounding() {
    fn source(float: bool) -> (Arc<SeedDefinition>, Packs) {
        let kind = if float { "float" } else { "uint" };
        let doc = doc::new("scalar")
            .field(Field::new("k").r#type(kind).size(8))
            .sign()
            .unwrap();
        let def = SeedDefinition::prescribe(&doc).unwrap();
        let mut pack = def.fill(2).unwrap();
        for k in [1u64 << 53, (1u64 << 53) + 1] {
            pack.load(
                def.ingest(k, |k, s| {
                    if float {
                        s.set_float("k", k as f64)?;
                    } else {
                        s.set_uint("k", k)?;
                    }
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
        }
        pack.seal().unwrap();
        let mut src = Packs::new();
        src.pack(def.open(pack.into_bytes().unwrap()).unwrap());
        (def, src)
    }
    let (ld, l) = source(false);
    let (rd, r) = source(true);
    let p = plan("SELECT a.k FROM events a JOIN events b ON a.k=b.k").unwrap();
    let mut j = p
        .bind([&[ld], &[rd]])
        .unwrap()
        .open(
            MemoryBudget::new(1 << 20),
            None,
            |q| l.query(q),
            |q| r.query(q),
        )
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            j.next_row().unwrap().unwrap(),
            vec![("k".to_owned(), Data::from(1u64 << 53))]
        );
    }
    assert!(j.next_row().unwrap().is_none());
}
