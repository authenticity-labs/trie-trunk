#![cfg(feature = "registration")]
use ciborium::Value;
use data_seeds::{summaries::*, *};
use std::sync::Arc;

fn document(name: &str) -> doc::Document {
    doc::new("logs")
        .field(
            Field::new(name)
                .r#type("uint")
                .size(1)
                .annotation("@otel(SeverityNumber)")
                .annotation("#summ(numeric)"),
        )
        .sign()
        .unwrap()
}
fn pack(definition: &Arc<SeedDefinition>, name: &str) -> Arc<SeedPackSnapshot> {
    let mut pack = definition.fill(27).unwrap();
    for level in 0..27 {
        let seed = definition
            .ingest(level, |level, seed| {
                if level < 25 {
                    seed.set_uint(name, level as u8)?;
                } else if level == 25 {
                    seed.set_null(name)?;
                }
                Ok(())
            })
            .unwrap();
        pack.load(seed).unwrap();
    }
    pack.seal().unwrap();
    definition.open(pack.into_bytes().unwrap()).unwrap()
}
fn result(pack: &Arc<SeedPackSnapshot>, predicate: Predicate) -> Vec<u8> {
    let mut sources = Sources::new();
    sources.pack(pack.clone());
    let batch = sources
        .query(
            Query::new()
                .filter(predicate)
                .select(["severity"])
                .limit(100),
        )
        .unwrap()
        .fetch(100)
        .unwrap();
    let mut reader = batch.reader();
    (0..batch.len())
        .map(|i| {
            let stored = batch.origin(i).unwrap().1 as u8;
            assert_eq!(
                reader.view(i).unwrap().get("severity").unwrap(),
                Cell::Value(Value::from(if stored == 0 {
                    0
                } else {
                    (stored - 1) % 4 + 1
                }))
            );
            stored
        })
        .collect()
}
#[test]
fn group_and_position_views_share_the_original_ordinal() {
    let doc = document("level");
    let bytes = doc.bytes().unwrap();
    let def = SeedDefinition::prescribe(&doc).unwrap();
    assert_eq!(def.query_ordinal("level"), def.query_ordinal("severity"));
    assert_eq!(def.prescription().definition().fields().len(), 1);
    assert_eq!(def.prescription().bytes(), bytes);
    let pack = pack(&def, "level");
    if let Ok(directory) = std::env::var("SEED_SEMANTIC_FIXTURE") {
        use trie_trunk_taxonomy::{Trunk, registration::Registration};
        let (trie, _) = Trunk::new("Semantic Views")
            .unwrap()
            .registration(
                Registration {
                    cache: "demo",
                    service: "lab",
                    resource: "logs",
                    resource_type: "log",
                    level: "",
                    file: "log.rx",
                },
                &doc,
            )
            .unwrap();
        let path = std::path::Path::new(&directory);
        std::fs::create_dir_all(path).unwrap();
        std::fs::write(path.join("trunk.trie"), trie.bytes()).unwrap();
        std::fs::write(path.join("log.rx"), bytes).unwrap();
        std::fs::write(path.join("events.pac"), pack.stored_bytes()).unwrap();
    }

    assert_eq!(
        result(&pack, Predicate::compare("level", Compare::Eq, "ERROR")),
        vec![17, 18, 19, 20]
    );
    assert_eq!(
        result(&pack, Predicate::compare("level", Compare::Eq, "error")),
        vec![17, 18, 19, 20]
    );
    assert_eq!(
        result(&pack, Predicate::compare("level", Compare::Eq, 5)),
        vec![17, 18, 19, 20]
    );
    assert_eq!(
        result(&pack, Predicate::compare("severity", Compare::Eq, 4)),
        vec![4, 8, 12, 16, 20, 24]
    );
    assert_eq!(
        result(&pack, Predicate::compare("level", Compare::Ge, "ERROR")),
        (17..=24).collect::<Vec<_>>()
    );
    assert_eq!(
        result(
            &pack,
            Predicate::Not(Box::new(Predicate::compare("level", Compare::Eq, "ERROR")))
        ),
        (0..=24)
            .filter(|v| !(17..=20).contains(v))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        result(
            &pack,
            Predicate::And(vec![
                Predicate::compare("level", Compare::Eq, "ERROR"),
                Predicate::compare("severity", Compare::Eq, 3)
            ])
        ),
        vec![19]
    );
    let ordinals = pack
        .filter(Predicate::compare("level", Compare::Eq, "ERROR"))
        .unwrap()
        .fetch(100)
        .unwrap();
    assert_eq!(ordinals, vec![17, 18, 19, 20]);
    let mut sources = Sources::new();
    sources.pack(pack);
    let rows = sources
        .query(Query::new().filter(Predicate::IsNull("severity".into())))
        .unwrap()
        .fetch(100)
        .unwrap()
        .objects()
        .unwrap();
    assert_eq!(
        rows,
        vec![vec![
            ("level".into(), Value::Null),
            ("severity".into(), Value::Null)
        ]]
    );
    assert_eq!(
        sources
            .query(Query::new().filter(Predicate::IsAbsent("severity".into())))
            .unwrap()
            .fetch(100)
            .unwrap()
            .objects()
            .unwrap(),
        vec![vec![]]
    );
}
#[test]
fn numeric_comparisons_match_views_including_unspecified_and_unknown_values() {
    let def = SeedDefinition::prescribe(&document("level")).unwrap();
    let mut pack = def.fill(258).unwrap();
    for n in 0..258u16 {
        pack.load(
            def.ingest(n, |n, seed| {
                if n < 256 {
                    seed.set_uint("level", n as u8)?;
                } else if n == 256 {
                    seed.set_null("level")?;
                }
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    }
    pack.seal().unwrap();
    let pack = def.open(pack.into_bytes().unwrap()).unwrap();
    let mut sources = Sources::new();
    sources.pack(pack.clone());
    let batch = sources.query(Query::new()).unwrap().fetch(258).unwrap();
    let mut reader = batch.reader();
    for n in 0..256 {
        let level = if (1..=24).contains(&n) {
            (n - 1) / 4 + 1
        } else {
            n
        };
        let severity = if (1..=24).contains(&n) {
            (n - 1) % 4 + 1
        } else {
            n
        };
        let view = reader.view(n).unwrap();
        assert_eq!(
            view.get("level").unwrap(),
            Cell::Value(Value::from(level as u64))
        );
        assert_eq!(
            view.get("severity").unwrap(),
            Cell::Value(Value::from(severity as u64))
        );
        assert_eq!(
            view.stored("level").unwrap(),
            Cell::Value(Value::from(n as u64))
        );
    }
    for field in ["level", "severity"] {
        for literal in [-1, 0, 1, 3, 4, 5, 6, 7, 17, 24, 25, 255, 256] {
            for op in [
                Compare::Eq,
                Compare::Ne,
                Compare::Lt,
                Compare::Le,
                Compare::Gt,
                Compare::Ge,
            ] {
                let expected = (0..256i64)
                    .filter(|&n| {
                        let v = if (1..=24).contains(&n) {
                            if field == "level" {
                                (n - 1) / 4 + 1
                            } else {
                                (n - 1) % 4 + 1
                            }
                        } else {
                            n
                        };
                        match op {
                            Compare::Eq => v == literal,
                            Compare::Ne => v != literal,
                            Compare::Lt => v < literal,
                            Compare::Le => v <= literal,
                            Compare::Gt => v > literal,
                            Compare::Ge => v >= literal,
                        }
                    })
                    .map(|n| n as usize)
                    .collect::<Vec<_>>();
                let actual = pack
                    .filter(Predicate::compare(field, op, literal))
                    .unwrap()
                    .fetch(258)
                    .unwrap();
                assert_eq!(actual, expected, "{field} {op:?} {literal}");
            }
        }
    }
    let ordered = sources
        .query(
            Query::new()
                .filter(Predicate::compare("level", Compare::Eq, "ERROR"))
                .order_by("severity", Direction::Descending)
                .limit(100),
        )
        .unwrap()
        .fetch(100)
        .unwrap();
    assert_eq!(
        (0..ordered.len())
            .map(|i| ordered.origin(i).unwrap().1)
            .collect::<Vec<_>>(),
        vec![20, 19, 18, 17]
    );
}
#[test]
fn aliases_follow_annotations_and_do_not_shadow_real_fields() {
    let def = SeedDefinition::prescribe(&document("importance")).unwrap();
    let pack = pack(&def, "importance");
    assert_eq!(
        result(
            &pack,
            Predicate::compare("importance", Compare::Eq, "FATAL")
        ),
        vec![21, 22, 23, 24]
    );
    assert_eq!(
        result(
            &pack,
            Predicate::And(vec![
                Predicate::compare("importance", Compare::Eq, 6),
                Predicate::compare("severity", Compare::Eq, 1)
            ])
        ),
        vec![21]
    );
    let doc = doc::new("real-field")
        .field(
            Field::new("level")
                .r#type("uint")
                .size(1)
                .annotation("@otel(SeverityNumber)"),
        )
        .field(Field::new("severity").r#type("uint").size(2))
        .sign()
        .unwrap();
    let def = SeedDefinition::prescribe(&doc).unwrap();
    assert_eq!(def.query_ordinal("severity"), Some(1));
    assert_eq!(
        def.predicate(&Predicate::compare("severity", Compare::Eq, 4))
            .as_ref(),
        &Predicate::compare("severity", Compare::Eq, 4)
    );
    let plain = SeedDefinition::prescribe(
        &doc::new("plain")
            .field(Field::new("level").r#type("uint").size(1))
            .sign()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(plain.query_fields(), vec!["level"]);
    let p = Predicate::compare("level", Compare::Eq, 17);
    assert!(matches!(plain.predicate(&p), std::borrow::Cow::Borrowed(_)));
}
#[test]
fn stalks_materialize_the_same_logical_values_without_changing_source_storage() {
    let def = SeedDefinition::prescribe(&document("level")).unwrap();
    let pack = pack(&def, "level");
    let mut beds = Beds::new();
    beds.pack(pack);
    let budget = MemoryBudget::new(100_000);
    let mut growth = beds
        .stalks(
            Query::new().filter(Predicate::compare("level", Compare::Eq, "ERROR")),
            budget.clone(),
            100,
        )
        .unwrap();
    while !growth.poll(10).unwrap() {}
    let stalks = growth.finish().unwrap();
    assert_eq!(stalks.len(), 4);
    assert_eq!(stalks.sum("severity").unwrap(), Cell::Value(10u64.into()));
    assert_eq!(stalks.sum("level").unwrap(), Cell::Value(20u64.into()));
    drop(stalks);
    assert_eq!(budget.used(), 0);
}
#[test]
fn summary_preview_uses_the_same_semantic_binding() {
    let def = SeedDefinition::prescribe(&document("level")).unwrap();
    let plan = def.summaries(288).unwrap();
    let mut live = plan.accumulator(0).unwrap();
    for level in 17..=19 {
        live.ingest(1, level, |level, seed| {
            seed.set_uint("level", level as u8)?;
            Ok(())
        })
        .unwrap();
    }
    live.complete(SUMMARY_DAY_MS).unwrap();
    let pack = live.seal().unwrap();
    let mut reader = SummaryReader::pack(plan.layout().clone(), pack.bytes().to_vec(), 0).unwrap();
    let window = SummaryWindow::new(0..SUMMARY_PERIOD_MS).unwrap();
    let category = reader
        .preview(&window, &Predicate::compare("level", Compare::Eq, "ERROR"))
        .unwrap();
    assert!(category.predicate_complete);
    assert_eq!(category.count, Some(CountBounds::exact(3)));
    let zero = reader
        .preview(&window, &Predicate::compare("severity", Compare::Eq, 4))
        .unwrap();
    assert!(zero.predicate_complete);
    assert_eq!(zero.count, Some(CountBounds::exact(0)));
    assert!(zero.eligible.is_empty());
}

#[test]
fn provider_labels_and_groups_bind_without_retyping_the_stored_field() {
    let doc = doc::new("http")
        .field(
            Field::new("status")
                .r#type("uint")
                .size(2)
                .annotation("@otel(http.response.status_code)"),
        )
        .sign()
        .unwrap();
    let def = SeedDefinition::prescribe(&doc).unwrap();
    let mut pack = def.fill(5).unwrap();
    for status in [200u16, 404, 500, 599] {
        pack.load(
            def.ingest(status, |v, s| {
                s.set_uint("status", v)?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    }
    pack.load(
        def.ingest((), |_, s| {
            s.set_null("status")?;
            Ok(())
        })
        .unwrap(),
    )
    .unwrap();
    pack.seal().unwrap();
    let pack = def.open(pack.into_bytes().unwrap()).unwrap();
    assert_eq!(
        pack.filter(Predicate::compare("status", Compare::Eq, "Not Found"))
            .unwrap()
            .fetch(10)
            .unwrap(),
        vec![1]
    );
    assert_eq!(
        pack.filter(Predicate::compare("status", Compare::Eq, "Server Error"))
            .unwrap()
            .fetch(10)
            .unwrap(),
        vec![2, 3]
    );
    assert_eq!(
        pack.filter(Predicate::compare("status", Compare::Ne, "Server Error"))
            .unwrap()
            .fetch(10)
            .unwrap(),
        vec![0, 1]
    );
}

#[test]
fn query_selectors_are_limited_to_named_field_roles() {
    let document = doc::new("measurement")
        .field(Field::new("recorded").r#type("uint").size(6).annotation("@time(ms)"))
        .field(Field::new("elapsed").r#type("uint").size(2).annotation("@duration").annotation("@unit(us)"))
        .field(Field::new("event_id").r#type("bytes").size(10).annotation("@event"))
        .field(Field::new("object_id").r#type("uuid").annotation("@object"))
        .field(Field::new("state").r#type("uint").size(2).annotation("@leaf").annotation("@custom"))
        .sign().unwrap();
    let definition = SeedDefinition::prescribe(&document).unwrap();
    for (selector, field) in [("@time", "recorded"), ("@duration", "elapsed"), ("@event", "event_id"), ("@object", "object_id")] {
        assert_eq!(definition.query_ordinal(selector), definition.prescription().field_ordinal(field));
    }
    for name in ["@unit", "@leaf", "@custom", "@field"] {
        assert_eq!(definition.query_ordinal(name), None, "{name}");
    }
}

#[test]
fn explicit_annotation_selectors_bind_to_each_source_field() {
    let mut sources = Sources::new();
    for (name, tag) in [("elapsed", "@duration"), ("latency", "@duration(us)"), ("duration", "@other")] {
        let doc = doc::new(name)
            .field(Field::new(name).r#type("uint").size(1).annotation(tag))
            .sign().unwrap();
        let definition = SeedDefinition::prescribe(&doc).unwrap();
        assert_eq!(definition.query_fields(), [name]);
        sources.pack(pack(&definition, name));
    }
    let selected = sources.query(Query::new()
        .filter(Predicate::compare("@duration", Compare::Eq, 12))
        .select(["@duration", "elapsed", "latency"])).unwrap().fetch(100).unwrap();
    assert_eq!(selected.len(), 2);
    let mut reader = selected.reader();
    for i in 0..2 {
        let view = reader.view(i).unwrap();
        let object = view.object().unwrap();
        assert_eq!(object.len(), 2);
        assert_eq!(object[0], ("@duration".into(), Value::from(12)));
        assert_eq!(object[1].1, object[0].1);
    }
    let literal = sources.query(Query::new().filter(Predicate::compare("duration", Compare::Eq, 12))).unwrap().fetch(100).unwrap();
    assert_eq!(literal.len(), 1);
    let untagged = sources.query(Query::new().filter(Predicate::IsAbsent("@duration".into()))).unwrap().fetch(100).unwrap();
    assert_eq!(untagged.len(), 29); // 27 from the third source and one absent value per tagged Pack
    let explicit_null = sources.query(Query::new().filter(Predicate::IsNull("@duration".into()))).unwrap().fetch(100).unwrap();
    assert_eq!(explicit_null.len(), 2);
    let doc = doc::new("ambiguous")
        .field(Field::new("elapsed").r#type("uint").size(1).annotation("@duration"))
        .field(Field::new("latency").r#type("uint").size(1).annotation("@duration"))
        .sign().unwrap();
    sources.pack(pack(&SeedDefinition::prescribe(&doc).unwrap(), "elapsed"));
    assert!(sources.query(Query::new().select(["@duration"])).err().unwrap().to_string().contains("ambiguous annotation @duration"));
}

#[cfg(feature="query-language")]
#[test]
fn sql_annotation_selectors_survive_parsing_and_quoting() {
    let fields = ["@event".into(), "@duration".into()];
    let plan = language::parse("SELECT @duration AS elapsed FROM events WHERE @duration >= 12", &fields).unwrap();
    assert_eq!(plan["plan"]["relational"]["selection"][0]["field"], "@duration");
    assert_eq!(plan, language::parse("SELECT \"@duration\" AS elapsed FROM events WHERE \"@duration\" >= 12", &fields).unwrap());
    let literal = language::parse("SELECT @event FROM events WHERE @event = '@object'", &fields).unwrap();
    assert!(literal.to_string().contains(r#""value":"@object""#));
    assert!(language::parse("SELECT @ time FROM events", &fields).is_err());
}
