#![cfg(feature = "planner-lab")]
use ciborium::Value as Data;
use data_seeds::{planner_lab::*, *};
fn sources() -> Sources {
    let def = SeedDefinition::prescribe(
        &doc::new("lab")
            .field(Field::new("n").r#type("uint").size(8))
            .sign()
            .unwrap(),
    )
    .unwrap();
    let mut sources = Sources::new();
    for p in 0..4 {
        let mut pack = def.fill(25).unwrap();
        for i in p * 25..p * 25 + 25 {
            pack.load(
                def.ingest(i, |i, s| {
                    s.set_uint("n", i)?;
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
        }
        pack.seal().unwrap();
        sources.pack(def.open(pack.into_bytes().unwrap()).unwrap());
    }
    sources
}
fn models() -> (PlanModel, PlanModel) {
    (
        PlanModel::new("reference")
            .selection(Selection::FullSortReference)
            .max_candidates(100),
        PlanModel::new("heap")
            .selection(Selection::BoundedTopK)
            .max_candidates(5),
    )
}
#[test]
fn experiment_checks_oracle_and_records_work_not_just_time() {
    let (a, b) = models();
    let q = Query::new()
        .order_by("n", Direction::Descending)
        .offset(2)
        .limit(3);
    let expected = (95..=97u64)
        .rev()
        .map(|n| vec![("n".into(), Data::Integer(n.into()))])
        .collect();
    let report = Experiment::new("check", q, a, b)
        .expected(expected)
        .pairs(4)
        .warmups(1)
        .run(&sources())
        .unwrap();
    assert_eq!(report.samples.len(), 4);
    assert_eq!(
        report.samples.iter().filter(|p| p.baseline_first).count(),
        2
    );
    for pair in report.samples {
        assert_eq!(pair.baseline.peak_sort_candidates, 100);
        assert_eq!(pair.candidate.peak_sort_candidates, 5);
        assert_eq!(pair.candidate.scanned_rows, 100);
        assert_eq!(pair.candidate.output_rows, 3);
        assert_eq!(
            pair.candidate.plan_ns
                + pair.candidate.fetch_ns
                + pair.candidate.materialize_ns
                + pair.candidate.other_ns,
            pair.candidate.total_ns
        );
    }
}
#[test]
fn wrong_oracle_or_incompatible_or_overbudget_plans_fail() {
    let (a, b) = models();
    let q = Query::new().order_by("n", Direction::Descending).limit(3);
    assert!(
        Experiment::new("wrong", q.clone(), a.clone(), b.clone())
            .expected(vec![])
            .pairs(2)
            .run(&sources())
            .is_err()
    );
    assert!(PlanModel::new("stream").explain(&q).is_err());
    assert!(b.clone().max_candidates(1).explain(&q).is_err());
    assert!(
        Experiment::new("budget", q, a.max_candidates(2), b)
            .pairs(2)
            .run(&sources())
            .is_err()
    );
}
#[test]
fn streaming_batch_choices_preserve_one_global_limit() {
    let report = Experiment::new(
        "stream",
        Query::new().limit(7),
        PlanModel::new("one").batch_rows(1),
        PlanModel::new("many").batch_rows(50),
    )
    .pairs(2)
    .run(&sources())
    .unwrap();
    for p in report.samples {
        assert_eq!(p.baseline.scanned_rows, 7);
        assert_eq!(p.candidate.scanned_rows, 7);
        assert_eq!(p.baseline.output_rows, 7);
    }
    assert!(report.correctness.starts_with("Differential"));
}
