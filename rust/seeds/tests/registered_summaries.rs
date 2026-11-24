#![cfg(feature = "registration")]
use ciborium::Value;
use data_seeds::{EncodingMode, Field, doc, registration::Trunk, summaries::*};
use trie_trunk_taxonomy::{Trunk as Taxonomy, registration::Registration};

fn fixture() -> (Trunk, u16, u16) {
    let old = doc::new("sample")
        .field(
            Field::new("duration")
                .r#type("uint")
                .size(2)
                .annotation("#summ(numeric)"),
        )
        .sign()
        .unwrap();
    let new = doc::new("sample")
        .version(0, 1)
        .field(
            Field::new("success")
                .r#type("bool")
                .annotation("#summ(boolean)"),
        )
        .field(
            Field::new("duration")
                .r#type("uint")
                .size(4)
                .annotation("#summ(numeric)"),
        )
        .compound(old.bytes().unwrap())
        .sign()
        .unwrap();
    let target = || Registration {
        cache: "test",
        service: "lab",
        resource: "events",
        resource_type: "sample",
        level: "",
        file: "sample.rx",
    };
    let (trie, a) = Taxonomy::new("Summaries")
        .unwrap()
        .registration(target(), &old)
        .unwrap();
    let (trie, b) = trie.registration(target(), &new).unwrap();
    let mut trunk = Trunk::open(trie.bytes()).unwrap();
    trunk
        .prescribe_file("sample.rx", new.bytes().unwrap())
        .unwrap();
    (trunk, a.branch, b.branch)
}
fn number(value: &Value, key: &str) -> Value {
    value
        .as_map()
        .unwrap()
        .iter()
        .find(|(k, _)| k.as_text() == Some(key))
        .unwrap()
        .1
        .clone()
}

#[test]
fn deployments_and_rollbacks_share_period_ordinals_and_keep_real_event_bytes() {
    let (trunk, a, b) = fixture();
    let summaries = trunk.trie().branches().select([a]).summaries().unwrap();
    let events = trunk.trie().branches().select([a]).packs().unwrap();
    let mut event_pack = events.fill(a, 5).unwrap();
    let mut day = summaries
        .accumulator_day(0, SummaryCalendar::new(0).unwrap())
        .unwrap();
    // Definition A, B, A, B in one period, then B alone in the next period.
    for (ordinal, (branch, duration)) in [(a, 10u64), (b, 70_000), (a, 20), (b, 90_000), (b, 300)]
        .into_iter()
        .enumerate()
    {
        let time = if ordinal == 4 {
            SUMMARY_PERIOD_MS
        } else {
            ordinal as u64
        };
        let seed = day
            .ingest(branch, time, duration, |duration, seed| {
                seed.set_uint("duration", duration)?;
                if branch == b {
                    seed.set_bool("success", true)?;
                }
                Ok(())
            })
            .unwrap();
        event_pack.load(seed).unwrap();
    }
    assert_eq!(day.active_definitions(), 2);
    assert!(day.seal().is_err());
    day.complete(SUMMARY_PERIOD_MS).unwrap();
    assert!(day.ingest(a, 0, (), |_, _| Ok(())).is_err());
    assert!(day.complete(0).is_err());
    assert_eq!(day.complete_through(), SUMMARY_PERIOD_MS);
    event_pack.seal().unwrap();
    assert_eq!(event_pack.len(), 5);
    day.complete(day.end()).unwrap();
    let (projection, pack) = day.seal().unwrap();
    assert!(day.is_sealed());
    assert_eq!(day.active_definitions(), 0);
    assert!(day.seal().is_err());
    assert_eq!(projection.layout().mode(), EncodingMode::FixedSparse);
    let opened = projection.layout().open(pack.bytes()).unwrap();
    assert_eq!(opened.len(), 288);
    assert!(!opened.headers().contains_key("summary.source"));
    assert!(!opened.headers().contains_key("summary.bindings"));
    assert!(!opened.headers().contains_key("summary.rx"));
    assert_eq!(opened.headers()["summary.unknown"].len(), (288usize * 2).div_ceil(8));
    assert_eq!(opened.options().address_bytes, 0);
    let periods = projection.periods(opened.headers()).unwrap().unwrap();
    let mut reader = opened.reader();
    let mixed = reader.get(0).unwrap();
    assert!(projection.field(&mixed, "success").is_err());
    let mut bad = opened.headers().clone();
    bad.get_mut("summary.unknown").unwrap().pop();
    assert!(projection.periods(&bad).is_err());
    bad.remove("summary.unknown");
    assert!(projection.periods(&bad).is_err());
    assert_eq!(
        number(&periods.period(0).unwrap().field(&mixed, "duration").unwrap(), "sum"),
        160_030u64.into()
    );
    assert_eq!(periods.period(0).unwrap().field(&mixed, "success").unwrap(), Value::Null);
    let next = reader.get(1).unwrap();
    assert_eq!(
        number(&periods.period(1).unwrap().field(&next, "duration").unwrap(), "sum"),
        300u64.into()
    );
    assert_eq!(
        number(&periods.period(1).unwrap().field(&next, "success").unwrap(), "true_count"),
        1u64.into()
    );
    drop(reader);
    drop(opened);
    let mut storage = SummaryBox::new(summaries.plan().layout().clone()).unwrap();
    storage.append(pack).unwrap().commit().unwrap();
    let reopened = SummaryBoxReader::open(storage.bytes().unwrap()).unwrap();
    // Registered summary metadata needs its branch-owned plan. The Box
    // contains no second copy of scalar type bindings.
    assert!(reopened.pack(0).is_err());
    let bytes=storage.bytes().unwrap();
    let range=reopened.storage_range(0).unwrap();
    let reopened_pack = projection.layout().open(&bytes[range.start as usize..range.end as usize]).unwrap();
    assert_eq!(reopened_pack.len(), 288);
    assert_eq!(
        number(
            &projection.periods(reopened_pack.headers()).unwrap().unwrap().period(0).unwrap()
                .field(&reopened_pack.reader().get(0).unwrap(), "duration")
                .unwrap(),
            "sum"
        ),
        160_030u64.into()
    );
}

#[test]
fn unused_versions_and_failed_ingestion_do_not_force_sparse_days() {
    let (trunk, a, b) = fixture();
    let summaries = trunk.trie().branches().select([a]).summaries().unwrap();
    let mut day = summaries
        .accumulator_day(0, SummaryCalendar::new(0).unwrap())
        .unwrap();
    assert!(day.ingest(u16::MAX, 0, (), |_, _| Ok(())).is_err());
    assert!(
        day.ingest(a, 0, (), |_, seed| {
            seed.set_text_string("duration", "bad")?;
            Ok(())
        })
        .is_err()
    );
    assert_eq!(day.active_definitions(), 0);
    day.complete(SUMMARY_PERIOD_MS).unwrap();
    assert!(day.ingest(b, 0, (), |_, _| Ok(())).is_err());
    day.ingest(b, SUMMARY_PERIOD_MS, (), |_, seed| {
        seed.set_uint("duration", 80_000u64)?
            .set_bool("success", false)?;
        Ok(())
    })
    .unwrap();
    assert_eq!(day.active_definitions(), 1);
    day.complete(day.end()).unwrap();
    let (projection, _) = day.seal().unwrap();
    assert_eq!(projection.layout().mode(), EncodingMode::FixedFull);
    let mut silent = summaries
        .accumulator_day(SUMMARY_DAY_MS, SummaryCalendar::new(0).unwrap())
        .unwrap();
    silent.complete(silent.end()).unwrap();
    let (projection, pack) = silent.seal().unwrap();
    assert_eq!(projection.layout().mode(), EncodingMode::FixedFull);
    assert_eq!(projection.layout().open(pack.bytes()).unwrap().len(), 288);
}

#[test]
fn annotation_changes_refresh_an_open_day_without_changing_the_event_version() {
    let definition = |summarize_cached: bool| doc::new("http").namespace("log").version(2, 9)
        .field(Field::new("duration").r#type("uint").size(3).annotation("#summ(numeric)"))
        .field(Field::new("cached").r#type("bool")
            .annotations(if summarize_cached { vec!["#summ(boolean)"] } else { vec![] }))
        .sign().unwrap();
    let before = definition(false);
    let after = definition(true);
    assert_eq!(before.definitions()[0].header.rx_hash, after.definitions()[0].header.rx_hash);
    let (taxonomy, registration) = Taxonomy::new("Annotations").unwrap()
        .registration(Registration { cache:"test", service:"lab", resource:"events", resource_type:"http", level:"", file:"http.rx" }, &before).unwrap();
    let (taxonomy, _) = taxonomy.registration(Registration { cache:"test", service:"lab", resource:"events", resource_type:"http", level:"", file:"http.rx" }, &after).unwrap();
    let branch = registration.branch;
    let mut trunk = Trunk::open(taxonomy.bytes()).unwrap();
    trunk.prescribe_file("http.rx", before.bytes().unwrap()).unwrap();
    let old = trunk.trie().branches().select([branch]).summaries().unwrap();
    let taxonomy = taxonomy.register_summaries(branch, "http-summary.rx", &old.definitions().unwrap()).unwrap();
    let mut day = old.accumulator_day(0, SummaryCalendar::new(0).unwrap()).unwrap();
    let transform = |value: u32, seed: &mut data_seeds::Seed<'_>| {
        seed.set_uint("duration", value)?.set_bool("cached", true)?;
        Ok(())
    };
    day.ingest(branch, 0, 10, transform).unwrap();
    trunk.prescribe_file("http.rx", after.bytes().unwrap()).unwrap();
    let new = trunk.trie().branches().select([branch]).summaries().unwrap();
    assert_eq!(old.plan().version(), (0, 0));
    assert_eq!(new.plan().version(), (0, 0));
    day.refresh(&new).unwrap();
    day.ingest(branch, 1, 20, transform).unwrap();
    day.refresh(&old).unwrap();
    day.ingest(branch, 2, 30, transform).unwrap();
    day.refresh(&new).unwrap();
    day.ingest(branch, SUMMARY_PERIOD_MS, 40, transform).unwrap();
    assert_eq!(day.active_definitions(), 2);
    day.complete(day.end()).unwrap();
    let (published, summary_document, projection, pack) = day.seal_registered(&taxonomy, "http-summary.rx").unwrap();
    assert_eq!(projection.layout().mode(), EncodingMode::FixedSparse);
    let opened = projection.layout().open(pack.bytes()).unwrap();
    assert_eq!(opened.len(), 288);
    let periods = projection.periods(opened.headers()).unwrap().unwrap();
    let mut cursor = opened.reader();
    let first = cursor.get(0).unwrap();
    assert_eq!(number(&periods.period(0).unwrap().field(&first, "duration").unwrap(), "sum"), 60u64.into());
    assert_eq!(periods.period(0).unwrap().field(&first, "cached").unwrap(), Value::Null);
    let second = cursor.get(1).unwrap();
    assert_eq!(number(&periods.period(1).unwrap().field(&second, "cached").unwrap(), "true_count"), 1u64.into());

    // Restart reads the stored summary context, even though the event's
    // version and hash never changed. Unknown mixed contributions must not
    // become proof that a period contains no matching events.
    let mut storage = SummaryBox::new(old.plan().layout().clone()).unwrap();
    storage.append(pack).unwrap().commit().unwrap();
    let mut replica = Trunk::open(published.bytes()).unwrap();
    replica.prescribe_file("http.rx", after.bytes().unwrap()).unwrap();
    replica.prescribe_file("http-summary.rx", summary_document.bytes().unwrap()).unwrap();
    let mut reader = SummaryReader::registered_box(&replica, branch, storage.bytes().unwrap().to_vec(), 0).unwrap();
    let window = SummaryWindow::new(0..2 * SUMMARY_PERIOD_MS).unwrap();
    for predicate in [
        data_seeds::Predicate::compare("cached", data_seeds::Compare::Eq, false),
        data_seeds::Predicate::IsNull("cached".into()),
        data_seeds::Predicate::IsAbsent("cached".into()),
    ] {
        let preview = reader.preview(&window, &predicate).unwrap();
        assert!(preview.coverage_complete);
        assert_eq!(preview.count, Some(CountBounds { lower: 0, upper: 3 }));
        assert_eq!(preview.eligible, vec![0..SUMMARY_PERIOD_MS]);
    }
    let empty = reader.preview(&window, &data_seeds::Predicate::compare("duration", data_seeds::Compare::Gt, 100u64)).unwrap();
    assert_eq!(empty.count, Some(CountBounds::exact(0)));
    assert!(empty.eligible.is_empty());
    let counts = reader.series(&window, SUMMARY_PERIOD_MS, &[SummaryMeasure::events()]).unwrap();
    assert_eq!(counts[0].points().iter().map(|p| p.value.clone()).collect::<Vec<_>>(), vec![Some(3u64.into()), Some(1u64.into())]);

    // The replicated daily PAC resolves historical source context through its
    // owning branch, using the same shared class library for every day.
    let raw = SummaryBoxReader::open(storage.bytes().unwrap()).unwrap().storage_range(0).unwrap();
    let bytes = storage.bytes().unwrap()[raw.start as usize..raw.end as usize].to_vec();
    let mut received = SummaryReader::registered(&replica, branch, bytes.clone(), 0).unwrap();
    let received_preview = received.preview(&window, &data_seeds::Predicate::compare("duration", data_seeds::Compare::Gt, 100u64)).unwrap();
    assert_eq!(received_preview.count, empty.count);
    assert_eq!(received_preview.eligible, empty.eligible);
    assert!(received_preview.coverage_complete);
    let mut missing = Trunk::open(published.bytes()).unwrap();
    missing.prescribe_file("http.rx", after.bytes().unwrap()).unwrap();
    // The generic class library ships with Seeds; only historical event
    // Definitions need fetching. Requiring a second summary RX was redundant.
    let mut built_in = SummaryReader::registered(&missing, branch, bytes.clone(), 0).unwrap();
    let preview = built_in.preview(&window, &data_seeds::Predicate::compare("duration", data_seeds::Compare::Gt, 100u64)).unwrap();
    assert_eq!(preview.count, empty.count);
    assert_eq!(preview.eligible, empty.eligible);
    let no_source = Trunk::open(published.bytes()).unwrap();
    assert!(SummaryReader::registered(&no_source, branch, bytes, 0).is_err());

    if let Ok(directory) = std::env::var("TRUNK_MIXED_SUMMARY_FIXTURE") {
        let directory = std::path::Path::new(&directory);
        std::fs::create_dir_all(directory).unwrap();
        for (name, bytes) in [
            ("trunk.trie", published.bytes()),
            ("http.rx", after.bytes().unwrap()),
            ("summary.rx", summary_document.bytes().unwrap()),
            ("summaries.box", storage.bytes().unwrap()),
        ] { std::fs::write(directory.join(name), bytes).unwrap(); }
        std::fs::write(directory.join("day.pac"), &storage.bytes().unwrap()[raw.start as usize..raw.end as usize]).unwrap();
    }

    let history = new.definitions().unwrap();
    let mut restored = Trunk::open(taxonomy.bytes()).unwrap();
    restored.prescribe_file("http.rx", after.bytes().unwrap()).unwrap();
    restored.prescribe_summaries(branch, history.bytes().unwrap()).unwrap();
    let handle = restored.trie().branches().select([branch]).summaries().unwrap();
    assert_eq!(handle.plan().version(), (0, 0));
    assert!(restored.prescribe_summaries(branch, history.bytes().unwrap()).is_err());
}

#[test]
fn full_pil_observations_match_builder_summaries_without_reencoding() {
    let (trunk,a,b)=fixture();
    let summaries=trunk.trie().branches().select([a]).summaries().unwrap();
    let events=trunk.trie().branches().select([a]).packs().unwrap();
    let calendar=SummaryCalendar::new(0).unwrap();
    let mut observed=summaries.accumulator_day(0,calendar).unwrap();
    let mut built=summaries.accumulator_day(0,calendar).unwrap();
    for (i,(branch,duration)) in [(a,10u64),(b,70_000),(a,20),(b,90_000)].into_iter().enumerate() {
        let seed=built.ingest(branch,i as u64,duration,|duration,seed|{
            seed.set_uint("duration",duration)?;
            if branch==b {seed.set_bool("success",true)?;}
            Ok(())
        }).unwrap();
        seed.cap().unwrap();
        let bytes=seed.bytes().unwrap();
        let definition=events.definition(branch).unwrap();
        let mut decoder=definition.prescription().decoder();
        let row=decoder.read(&bytes).unwrap();
        observed.observe(branch,i as u64,&row).unwrap();
        // Selecting a different Definition cannot reinterpret those bytes.
        assert!(observed.observe(if branch==a {b} else {a},i as u64,&row).is_err());
    }
    built.complete(built.end()).unwrap();observed.complete(observed.end()).unwrap();
    assert_eq!(built.seal().unwrap().1.bytes(),observed.seal().unwrap().1.bytes());
}
