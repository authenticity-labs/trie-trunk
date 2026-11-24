use data_seeds::{Field, SeedDefinition, doc, summaries::*};

fn plan() -> SummaryPlan {
    let document = doc::new("live-summary")
        .field(
            Field::new("time")
                .r#type("uint")
                .size(6)
                .annotation("@time"),
        )
        .field(
            Field::new("duration")
                .r#type("uint")
                .size(4)
                .annotation("#summ(numeric)"),
        )
        .sign()
        .unwrap();
    SeedDefinition::prescribe(&document)
        .unwrap()
        .summaries(288)
        .unwrap()
}
fn ingest(day: &mut SummaryAccumulator, time: u64, duration: u64) {
    // All event bytes originate through the actual Rust ingestion builder.
    let seed = day
        .ingest(time, duration, |duration, seed| {
            seed.set_uint("time", time)?
                .set_uint("duration", duration)?;
            Ok(())
        })
        .unwrap();
    assert!(!seed.bytes().unwrap().is_empty());
}

#[test]
fn daily_summary_calendar_uses_the_published_fixed_utc_offset() {
    let plan = plan();
    let midnight = 1_767_225_600_000i64;
    for offset in -12..=14 {
        let calendar = SummaryCalendar::new(offset).unwrap();
        let boundary = (midnight - i64::from(offset) * 3_600_000) as u64;
        assert_eq!(calendar.day(boundary).unwrap(), boundary..boundary + SUMMARY_DAY_MS);
        assert_eq!(calendar.day(boundary - 1).unwrap(), boundary - SUMMARY_DAY_MS..boundary);
        let mut current = plan.accumulator_day(boundary + 3_600_000, calendar).unwrap();
        assert_eq!(current.first(), boundary);
        assert_eq!(current.end(), boundary + SUMMARY_DAY_MS);
        assert_eq!(current.plan().layout().rows(), SUMMARY_DAY_ROWS);
        ingest(&mut current, boundary + 3_600_000, 90);
        // An hourly commit does not end or re-plan the daily accumulator.
        current.complete(boundary + 7_200_000).unwrap();
        assert!(!current.is_sealed());
        assert!(current.seal().is_err());
    }
    assert!(SummaryCalendar::new(-13).is_err());
    assert!(SummaryCalendar::new(15).is_err());
    assert!(SummaryCalendar::new(0).unwrap().day(u64::MAX).is_err());
}

#[test]
fn completion_follows_commit_watermark_not_new_hour_arrival() {
    let plan = plan();
    let mut day = plan.accumulator(0).unwrap();
    ingest(&mut day, 3_600_000, 800);
    ingest(&mut day, 1_000, 200); // previous hour's straggler
    assert_eq!(day.bucket(0).unwrap().count(), 1);
    assert_eq!(day.bucket(12).unwrap().count(), 1);
    assert!(day.seal().is_err());
    let first = SummaryWindow::new(1..3_600_000).unwrap();
    assert!(day.rollup(&first).is_err());
    day.complete(3_600_000).unwrap();
    let merged = day.rollup(&first).unwrap();
    assert_eq!(merged.count(), 1);
    assert!(day.ingest(1000, (), |_, _| Ok(())).is_err());
    ingest(&mut day, 3_600_001, 900);
    assert!(day.complete(3_599_999).is_err());
    assert!(day.complete(300_000).is_err());
    assert!(day.complete(day.end() + 300_000).is_err());
}

#[test]
fn snapshot_resumes_open_day_and_daily_seal_releases_state() {
    let plan = plan();
    let mut live = plan.accumulator(0).unwrap();
    ingest(&mut live, 10, 50);
    ingest(&mut live, 3_600_000, 900);
    live.complete(3_600_000).unwrap();
    let bytes = live.snapshot().unwrap();
    let mut restored =
        SummaryAccumulator::restore(&plan, 0, live.complete_through(), &bytes).unwrap();
    assert_eq!(restored.complete_through(), 3_600_000);
    assert_eq!(restored.bucket(0).unwrap().count(), 1);
    assert_eq!(restored.bucket(12).unwrap().count(), 1);
    ingest(&mut restored, 3_600_001, 700);
    restored.complete(restored.end()).unwrap();
    let pack = restored.seal().unwrap();
    assert!(restored.is_sealed());
    assert!(restored.bucket(0).is_err());
    assert!(restored.snapshot().is_err());
    assert!(restored.seal().is_err());
    assert!(restored.ingest(7_200_000, (), |_, _| Ok(())).is_err());
    assert!(SummaryAccumulator::restore(&plan, 0, 0, pack.bytes()).is_err());

    let mut stored = SummaryBox::new(plan.layout().clone()).unwrap();
    stored.append(pack).unwrap().commit().unwrap();
    let reader = SummaryBoxReader::open(stored.bytes().unwrap()).unwrap();
    let window = SummaryWindow::new(1..7_200_000).unwrap();
    let total = plan.rollup_box(&reader, 0, &window).unwrap();
    assert_eq!(total.count(), 3);
}

#[test]
fn silent_and_active_days_have_identical_pack_extents() {
    let plan = plan();
    let mut silent = plan.accumulator(0).unwrap();
    let mut active = plan.accumulator(86_400_000).unwrap();
    ingest(&mut active, 86_400_010, 600);
    silent.complete(silent.end()).unwrap();
    active.complete(active.end()).unwrap();
    let a = silent.seal().unwrap();
    let b = active.seal().unwrap();
    assert_eq!(a.bytes().len(), b.bytes().len());
    let mut stored = SummaryBox::new(plan.layout().clone()).unwrap();
    stored
        .append(a)
        .unwrap()
        .append(b)
        .unwrap()
        .commit()
        .unwrap();
    let reader = SummaryBoxReader::open(stored.bytes().unwrap()).unwrap();
    assert_eq!(
        reader.storage_range(0).unwrap().end,
        reader.storage_range(1).unwrap().start
    );
    let window = SummaryWindow::new(0..86_400_000).unwrap();
    let result = plan
        .preview_box(&reader, 0, &window, &data_seeds::Predicate::All)
        .unwrap();
    assert!(result.coverage_complete);
    assert_eq!(result.count.unwrap().upper, 0);
    assert!(result.eligible.is_empty());
}

#[test]
fn calendar_bounds_and_failed_ingestion_do_not_complete_periods() {
    let plan = plan();
    assert!(plan.accumulator(1).is_err());
    assert!(plan.accumulator(u64::MAX / 300_000 * 300_000).is_err());
    let mut day = plan.accumulator(86_400_000).unwrap();
    assert!(day.ingest(0, (), |_, _| Ok(())).is_err());
    assert!(day.ingest(day.end(), (), |_, _| Ok(())).is_err());
    assert!(
        day.ingest(day.first(), (), |_, seed| {
            seed.set_text_string("duration", "bad")?;
            Ok(())
        })
        .is_err()
    );
    assert_eq!(day.bucket(0).unwrap().count(), 0);
    assert_eq!(day.complete_through(), day.first());
}
