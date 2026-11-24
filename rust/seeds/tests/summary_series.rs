use ciborium::Value as Data;
use data_seeds::{Field, SeedDefinition, doc, summaries::*};
const PERIOD: u64 = 300_000;

fn plan() -> SummaryPlan {
    let d = doc::new("interval-analytics")
        .field(
            Field::new("duration")
                .r#type("uint")
                .size(16)
                .annotation("#summ(numeric)")
                .annotation("#percentile(95)"),
        )
        .field(
            Field::new("cached")
                .r#type("bool")
                .annotation("#summ(boolean)"),
        )
        .field(
            Field::new("status")
                .closed_enum("uint", [200.into(), 500.into()])
                .annotation("#summ(categorical)"),
        )
        .sign()
        .unwrap();
    SeedDefinition::prescribe(&d)
        .unwrap()
        .summaries(288)
        .unwrap()
}
fn add(day: &mut SummaryAccumulator, time: u64, duration: u128) {
    day.ingest(time, duration, |value, seed| {
        seed.set_uint("duration", value)?
            .set_bool("cached", value == 0)?
            .set_enum("status", if value == 0 { 200.into() } else { 500.into() })?;
        Ok(())
    })
    .unwrap();
}
fn stored(plan: &SummaryPlan) -> SummaryBox {
    let mut day = plan.accumulator(0).unwrap();
    for i in 0..100 {
        add(&mut day, i, 0);
    }
    add(&mut day, PERIOD, 100);
    add(&mut day, 2 * PERIOD, 1000);
    add(&mut day, 2 * PERIOD + 1, 2000);
    day.complete(day.end()).unwrap();
    let mut file = SummaryBox::new(plan.layout().clone()).unwrap();
    file.append(day.seal().unwrap()).unwrap().commit().unwrap();
    file
}
fn values(series: &SummarySeries) -> Vec<Option<Data>> {
    series.points().iter().map(|p| p.value.clone()).collect()
}
#[test]
fn distribution_samples_are_interval_aggregates_not_events() {
    let plan = plan();
    let file = stored(&plan);
    let reader = SummaryBoxReader::open(file.bytes().unwrap()).unwrap();
    let window = SummaryWindow::new(1..4 * PERIOD - 1).unwrap();
    let series = plan
        .series_box(
            &reader,
            0,
            &window,
            PERIOD,
            &[
                SummaryMeasure::new("duration", SummaryAggregate::Mean),
                SummaryMeasure::new("duration", SummaryAggregate::Max),
                SummaryMeasure::events(),
            ],
        )
        .unwrap();
    assert_eq!(series[0].count, 103);
    assert_eq!(
        values(&series[0]),
        vec![
            Some(Data::Float(0.0)),
            Some(Data::Float(100.0)),
            Some(Data::Float(1500.0)),
            None
        ]
    );
    // Event P95 would be zero. Interval-mean P95 is 1500.
    assert_eq!(series[0].perc(95.0).unwrap(), Some(Data::Float(1500.0)));
    assert_eq!(series[0].perc(50.0).unwrap(), Some(Data::Float(100.0)));
    assert_eq!(series[0].samples(), 3);
    assert_eq!(series[1].perc(95.0).unwrap(), Some(2000u64.into()));
    assert_eq!(
        values(&series[2]),
        [100u64, 1, 2, 0].map(|n| Some(n.into())).to_vec()
    );
    assert_eq!(series[2].samples(), 4);
    assert_eq!(series[2].perc(50.0).unwrap(), Some(1u64.into()));
    assert_eq!(series[0].points()[0].from, 0);
    assert_eq!(series[0].points()[3].to, 4 * PERIOD);
    // Source fields are still complete nested class objects on storage reads.
    let pack = reader.pack(0).unwrap();
    let mut rows = pack.reader();
    let row = rows.get(0).unwrap();
    let object = plan.object(&row).unwrap();
    assert!(object.iter().all(|(_, v)| matches!(v, Data::Map(_))));
    assert!(
        !plan
            .document()
            .bytes()
            .unwrap()
            .windows(9)
            .any(|v| v == b"histogram")
    );
}
#[test]
fn span_combines_class_state_before_projecting_each_interval() {
    let plan = plan();
    let file = stored(&plan);
    let reader = SummaryBoxReader::open(file.bytes().unwrap()).unwrap();
    let window = SummaryWindow::new(0..4 * PERIOD).unwrap();
    let series = plan
        .series_box(
            &reader,
            0,
            &window,
            2 * PERIOD,
            &[
                SummaryMeasure::new("duration", SummaryAggregate::Mean),
                SummaryMeasure::new("cached", SummaryAggregate::TrueProportion),
                SummaryMeasure::new("status", SummaryAggregate::Distinct),
            ],
        )
        .unwrap();
    assert_eq!(
        values(&series[0]),
        vec![Some(Data::Float(100.0 / 101.0)), Some(Data::Float(1500.0))]
    );
    assert_eq!(
        values(&series[1]),
        vec![Some(Data::Float(100.0 / 101.0)), Some(Data::Float(0.0))]
    );
    assert_eq!(
        values(&series[2]),
        vec![Some(2u64.into()), Some(1u64.into())]
    );
    assert_eq!(
        series[0].perc(50.0).unwrap(),
        Some(Data::Float(100.0 / 101.0))
    );
    let clipped = plan
        .series_box(&reader, 0, &window, 3 * PERIOD, &[SummaryMeasure::events()])
        .unwrap();
    assert_eq!(
        values(&clipped[0]),
        vec![Some(103u64.into()), Some(0u64.into())]
    );
    assert_eq!(clipped[0].points()[1].to, 4 * PERIOD);
    assert!(
        plan.series_box(&reader, 0, &window, 0, &[SummaryMeasure::events()])
            .is_err()
    );
    assert!(
        plan.series_box(&reader, 0, &window, PERIOD + 1, &[SummaryMeasure::events()])
            .is_err()
    );
    assert!(
        plan.series_box(
            &reader,
            0,
            &window,
            PERIOD,
            &[SummaryMeasure::new("status", SummaryAggregate::Mean)]
        )
        .is_err()
    );
    assert!(
        plan.series_box(
            &reader,
            0,
            &SummaryWindow::new(0..90_000_000).unwrap(),
            PERIOD,
            &[SummaryMeasure::events()]
        )
        .is_err()
    );
    assert!(series[0].perc(f64::NAN).is_err());
    assert!(series[0].perc(101.0).is_err());
}
#[test]
fn three_day_spans_cross_pack_boundaries_and_preserve_integer_precision() {
    let plan = plan();
    let mut file = SummaryBox::new(plan.layout().clone()).unwrap();
    let large = 1u128 << 90;
    let day_ms = 86_400_000;
    for day in 0..6u64 {
        let mut live = plan.accumulator(day * day_ms).unwrap();
        add(&mut live, day * day_ms, large + day as u128);
        live.complete(live.end()).unwrap();
        file.append(live.seal().unwrap()).unwrap();
    }
    file.commit().unwrap();
    let reader = SummaryBoxReader::open(file.bytes().unwrap()).unwrap();
    let series = plan
        .series_box(
            &reader,
            0,
            &SummaryWindow::new(0..6 * day_ms).unwrap(),
            3 * day_ms,
            &[SummaryMeasure::new("duration", SummaryAggregate::Sum)],
        )
        .unwrap();
    assert_eq!(series[0].points().len(), 2);
    assert_eq!(
        series[0].perc(0.0).unwrap(),
        Some(pil_codec::native_unsigned(3 * large + 3))
    );
    assert_eq!(
        series[0].perc(95.0).unwrap(),
        Some(pil_codec::native_unsigned(3 * large + 12))
    );
    assert_eq!(series[0].count, 6);
    let measures = [SummaryMeasure::new("duration", SummaryAggregate::Sum)];
    let window = SummaryWindow::new(0..6 * day_ms).unwrap();
    let mut stream = plan.series(&window, 3 * day_ms, &measures).unwrap();
    for day in 0..6 {
        // Independent daily Packs, as delivered by /summaries. There is no
        // aggregate calculation, framing parser or Box assembly in the host.
        stream
            .pack(&reader.pack(day).unwrap(), day as u64 * day_ms)
            .unwrap();
    }
    let streamed = stream.finish().unwrap();
    assert_eq!(values(&streamed[0]), values(&series[0]));
    assert_eq!(
        streamed[0].perc(95.0).unwrap(),
        series[0].perc(95.0).unwrap()
    );
    let mut missing = plan.series(&window, 3 * day_ms, &measures).unwrap();
    missing.pack(&reader.pack(0).unwrap(), 0).unwrap();
    assert!(missing.pack(&reader.pack(2).unwrap(), 2 * day_ms).is_err());
    assert!(missing.finish().is_err());
    let mut duplicate = plan.series(&window, 3 * day_ms, &measures).unwrap();
    duplicate.pack(&reader.pack(0).unwrap(), 0).unwrap();
    assert!(duplicate.pack(&reader.pack(0).unwrap(), 0).is_err());
}
#[test]
fn empty_class_objects_do_not_invent_numeric_samples() {
    let plan = plan();
    let file = stored(&plan);
    let reader = SummaryBoxReader::open(file.bytes().unwrap()).unwrap();
    let series = plan
        .series_box(
            &reader,
            0,
            &SummaryWindow::new(4 * PERIOD..6 * PERIOD).unwrap(),
            PERIOD,
            &[
                SummaryMeasure::new("duration", SummaryAggregate::Mean),
                SummaryMeasure::events(),
            ],
        )
        .unwrap();
    assert_eq!(series[0].samples(), 0);
    assert_eq!(series[0].perc(95.0).unwrap(), None);
    assert_eq!(series[1].samples(), 2);
    assert_eq!(series[1].perc(95.0).unwrap(), Some(0u64.into()));
}
