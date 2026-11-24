use ciborium::Value as Data;
use data_seeds::{Field, SeedDefinition, doc, summaries::*};
use std::sync::Arc;

fn source(width: u16, extra: bool, unit: &str) -> Arc<SeedDefinition> {
    let mut document = doc::new("http").namespace("log").field(
        Field::new("duration")
            .r#type("uint")
            .size(width)
            .annotation("#summ(numeric)")
            .annotation(format!("@unit({unit})")),
    );
    if extra {
        document = document.field(
            Field::new("cached")
                .r#type("bool")
                .annotation("#summ(boolean)"),
        );
    }
    SeedDefinition::prescribe(&document.sign().unwrap()).unwrap()
}
fn value(object: &Data, name: &str) -> Data {
    object
        .as_map()
        .unwrap()
        .iter()
        .find(|(k, _)| k.as_text() == Some(name))
        .unwrap()
        .1
        .clone()
}
#[test]
fn generic_library_is_reproducible_and_classes_are_fixed() {
    let library = summary_template().unwrap();
    assert_eq!(
        library.bytes().unwrap(),
        include_bytes!("../definitions/summary.rx")
    );
    assert_eq!(
        library
            .definitions()
            .iter()
            .map(|d| d.header.rx_name.as_str())
            .collect::<Vec<_>>(),
        [
            "summary",
            "numeric",
            "real",
            "temporal",
            "boolean",
            "categories"
        ]
    );
    assert_eq!(library.definitions()[0].fields.len(), 7);
    for definition in library.definitions() {
        assert!(
            !definition
                .header
                .rx_name
                .chars()
                .any(|c| c.is_ascii_digit()),
            "width-specific summary Definition"
        );
    }
    let plan = SummaryPlan::new(source(3, false, "ms"), 288).unwrap();
    assert!(matches!(
        plan.layout().definition().seed().mode().unwrap(),
        data_seeds::EncodingMode::FixedFull | data_seeds::EncodingMode::FixedSparse
    ));
}
#[test]
fn field_sizes_names_and_annotations_change_bindings_not_definitions() {
    let mut engine = SummaryDefinitions::new();
    let a = engine.prescribe(source(2, false, "ms"), 288).unwrap();
    let b = engine.prescribe(source(4, true, "us"), 288).unwrap();
    assert_eq!(a.document().bytes().unwrap(), b.document().bytes().unwrap());
    assert_eq!(engine.len(), 1);
    assert_ne!(
        a.layout().definition().prescription().array_lengths(),
        b.layout().definition().prescription().array_lengths()
    );
    let document = engine.document().unwrap().unwrap();
    assert!(
        document
            .definitions()
            .iter()
            .all(|d| !d.header.rx_name.contains("http")
                && !d.fields.iter().any(|f| f.name == "duration"))
    );
    let mut restored = SummaryDefinitions::read(document.bytes().unwrap()).unwrap();
    assert_eq!(
        restored
            .prescribe(source(2, false, "ms"), 288)
            .unwrap()
            .document()
            .bytes()
            .unwrap(),
        a.document().bytes().unwrap()
    );
}

#[test]
fn annotation_only_field_selection_preserves_historical_meaning() {
    // Both configurations have the same event schema and exactly one uint4
    // summary object. Dimensions alone cannot identify which field it means.
    let documents = ["duration", "size"].map(|selected| {
        let mut document = doc::new("request");
        for name in ["duration", "size"] {
            let mut field = Field::new(name).r#type("uint").size(4);
            if name == selected {
                field = field.annotation("#summ(numeric)");
            }
            document = document.field(field);
        }
        document.sign().unwrap()
    });
    assert_eq!(
        documents[0].definitions()[0].header.rx_hash,
        documents[1].definitions()[0].header.rx_hash
    );
    let plans = documents
        .iter()
        .map(|document| {
            SeedDefinition::prescribe(document)
                .unwrap()
                .summaries(288)
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        plans[0]
            .layout()
            .definition()
            .prescription()
            .array_lengths(),
        plans[1]
            .layout()
            .definition()
            .prescription()
            .array_lengths()
    );
    let mut file = SummaryBox::new(plans[0].layout().clone()).unwrap();
    let mut events = Vec::new();
    for (day, plan) in plans.iter().enumerate() {
        let mut accumulator = plan.accumulator(day as u64 * SUMMARY_DAY_MS).unwrap();
        let event = accumulator
            .ingest(
                accumulator.first(),
                (10u32, 200u32),
                |(duration, size), seed| {
                    seed.set_uint("duration", duration)?
                        .set_uint("size", size)?;
                    Ok(())
                },
            )
            .unwrap();
        event.cap().unwrap();
        events.push(event.bytes().unwrap().to_vec());
        accumulator.complete(accumulator.end()).unwrap();
        file.append(accumulator.seal().unwrap()).unwrap();
    }
    file.commit().unwrap();
    let mut reader = SummaryReader::open(file.bytes().unwrap().to_vec(), 0).unwrap();
    let mut objects = Vec::new();
    reader
        .rows(
            &SummaryWindow::new(0..2 * SUMMARY_DAY_MS).unwrap(),
            |_, count, object| {
                if count > 0 {
                    objects.push(object);
                }
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(objects.len(), 2);
    assert_eq!(objects[0].len(), 1);
    assert_eq!(objects[0][0].0, "duration");
    assert_eq!(value(&objects[0][0].1, "sum"), 10.into());
    assert_eq!(objects[1].len(), 1);
    assert_eq!(objects[1][0].0, "size");
    assert_eq!(value(&objects[1][0].1, "sum"), 200.into());
    assert_eq!(events.len(), 2);
}
#[test]
fn different_day_dimensions_and_names_survive_restart_and_rollup() {
    let first = source(2, false, "ms").summaries(288).unwrap();
    let second = source(4, true, "ms").summaries(288).unwrap();
    let mut file = SummaryBox::new(first.layout().clone()).unwrap();
    for (day, plan) in [&first, &second].iter().enumerate() {
        let mut accumulator = plan.accumulator(day as u64 * SUMMARY_DAY_MS).unwrap();
        accumulator
            .ingest(accumulator.first(), 11u32, |v, s| {
                s.set_uint("duration", v)?;
                if day == 1 {
                    s.set_bool("cached", true)?;
                }
                Ok(())
            })
            .unwrap();
        accumulator.complete(accumulator.end()).unwrap();
        file.append(accumulator.seal().unwrap()).unwrap();
    }
    file.commit().unwrap();
    let reader = SummaryBoxReader::open(file.bytes().unwrap()).unwrap();
    assert!(!reader.is_uniform());
    let mut reader = SummaryReader::open(file.bytes().unwrap().to_vec(), 0).unwrap();
    let mut objects = Vec::new();
    reader
        .projected_rows(
            &SummaryWindow::new(0..2 * SUMMARY_DAY_MS).unwrap(),
            None,
            |_, count, object| {
                if count > 0 {
                    objects.push(object);
                }
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(objects.len(), 2);
    for object in &objects {
        assert_eq!(
            value(
                &object.iter().find(|(k, _)| k == "duration").unwrap().1,
                "sum"
            ),
            11.into()
        );
    }
    assert!(objects[0].iter().all(|(k, _)| k != "cached"));
    assert_eq!(
        value(
            &objects[1].iter().find(|(k, _)| k == "cached").unwrap().1,
            "true_count"
        ),
        1.into()
    );
}
#[test]
fn source_ordinals_and_field_counts_do_not_change_library() {
    let mut documents = Vec::new();
    for count in [1, 128] {
        let mut source = doc::new("source");
        for i in 0..count {
            source = source.field(
                Field::new(format!("field{i}"))
                    .r#type("uint")
                    .size(3)
                    .annotation("#summ(numeric)"),
            );
        }
        let source = SeedDefinition::prescribe(&source.sign().unwrap()).unwrap();
        let plan = source.summaries(288).unwrap();
        let mut bucket = plan.bucket();
        plan.ingest(&mut bucket, (), |_, seed| {
            for i in 0..count {
                seed.set_uint(i, i as u32 + 1)?;
            }
            Ok(())
        })
        .unwrap();
        let pack = plan
            .layout()
            .pack((0..288).map(|_| plan.seed(&bucket).unwrap()))
            .unwrap();
        let opened = plan.layout().open(pack.bytes()).unwrap();
        let mut cursor = opened.reader();
        let last = plan
            .field(&cursor.get(0).unwrap(), &format!("field{}", count - 1))
            .unwrap();
        assert_eq!(value(&last, "sum"), (count as u64).into());
        documents.push(plan.document().bytes().unwrap().to_vec());
    }
    assert_eq!(documents[0], documents[1]);
}
#[test]
fn categories_use_uint3_counters_and_seed_owned_vocabularies() {
    let mut documents = Vec::new();
    for values in [vec![200, 500], vec![200, 201, 204, 301, 400, 401, 404, 500]] {
        let source = SeedDefinition::prescribe(
            &doc::new("http")
                .field(
                    Field::new("status")
                        .closed_enum("uint", values.iter().map(|&v| v.into()).collect::<Vec<_>>())
                        .annotation("#summ(categorical)"),
                )
                .sign()
                .unwrap(),
        )
        .unwrap();
        let plan = source.summaries(288).unwrap();
        let mut bucket = plan.bucket();
        plan.ingest(&mut bucket, (), |_, seed| {
            seed.set_enum("status", 200.into())?;
            Ok(())
        })
        .unwrap();
        let data = plan
            .layout()
            .pack((0..288).map(|_| plan.seed(&bucket).unwrap()))
            .unwrap();
        let mut file = SummaryBox::new(plan.layout().clone()).unwrap();
        file.append(data).unwrap().commit().unwrap();
        let mut reader = SummaryReader::open(file.bytes().unwrap().to_vec(), 0).unwrap();
        reader
            .projected_rows(
                &SummaryWindow::new(0..SUMMARY_PERIOD_MS).unwrap(),
                None,
                |_, _, object| {
                    assert_eq!(value(&object[0].1, "mode"), 200.into());
                    Ok(())
                },
            )
            .unwrap();
        let counts = &plan.document().definitions()[0].fields.last().unwrap();
        assert_eq!(counts.element_size, Some(3));
        documents.push(plan.document().bytes().unwrap().to_vec());
    }
    assert_eq!(documents[0], documents[1]);
}

#[cfg(feature = "registration")]
#[test]
fn registered_packs_bind_fields_from_the_selected_taxonomy() {
    use data_seeds::registration::Trunk;
    use trie_trunk_taxonomy::{Trunk as Taxonomy, registration::Registration};
    let mut taxonomy = Taxonomy::new("Summary Test").unwrap();
    let mut sources = Vec::new();
    for (name, field) in [("http", "duration"), ("transfer", "size")] {
        let document = doc::new(name)
            .field(
                Field::new(field)
                    .r#type("uint")
                    .size(4)
                    .annotation("#summ(numeric)"),
            )
            .sign()
            .unwrap();
        let file = format!("{name}.rx");
        let (next, registered) = taxonomy
            .registration(
                Registration {
                    cache: "test",
                    service: "app",
                    resource: name,
                    resource_type: name,
                    level: "",
                    file: &file,
                },
                &document,
            )
            .unwrap();
        taxonomy = next;
        sources.push((registered.branch, file, document, field));
    }
    let mut writer = Trunk::open(taxonomy.bytes()).unwrap();
    for (_, file, doc, _) in &sources {
        writer.prescribe_file(file, doc.bytes().unwrap()).unwrap();
    }
    let mut packs = Vec::new();
    let mut library = None;
    for (branch, _, _, field) in &sources {
        let summaries = writer
            .trie()
            .branches()
            .select([*branch])
            .summaries()
            .unwrap();
        let mut day = summaries
            .accumulator_day(0, SummaryCalendar::new(0).unwrap())
            .unwrap();
        day.ingest(*branch, 0, 7u32, |value, seed| {
            seed.set_uint(*field, value)?;
            Ok(())
        })
        .unwrap();
        let (next, doc, pack) = day.snapshot_registered(&taxonomy, "summary.rx").unwrap();
        taxonomy = next;
        library = Some(doc);
        packs.push((*branch, pack, *field));
    }
    let mut replica = Trunk::open(taxonomy.bytes()).unwrap();
    for (_, file, doc, _) in &sources {
        replica.prescribe_file(file, doc.bytes().unwrap()).unwrap();
    }
    replica
        .prescribe_file("summary.rx", library.unwrap().bytes().unwrap())
        .unwrap();
    for (branch, pack, field) in packs {
        assert!(
            pil_codec::pac::application_header(&pack, "summary.bindings")
                .unwrap()
                .is_none()
        );
        assert!(
            pil_codec::pac::application_header(&pack, "summary.definitions")
                .unwrap()
                .is_none()
        );
        let mut reader = SummaryReader::registered(&replica, branch, pack, 0).unwrap();
        reader
            .projected_rows(
                &SummaryWindow::new(0..SUMMARY_PERIOD_MS).unwrap(),
                None,
                |_, count, object| {
                    assert_eq!(count, 1);
                    assert_eq!(object[0].0, field);
                    assert_eq!(value(&object[0].1, "sum"), 7.into());
                    Ok(())
                },
            )
            .unwrap();
    }
}

#[test]
fn prepared_categories_keep_each_sources_vocabulary_and_predicate() {
    use data_seeds::{Compare, Predicate};
    for vocabulary in [[200u32, 404, 500], [100, 201, 302]] {
        let source = SeedDefinition::prescribe(
            &doc::new("http")
                .field(
                    Field::new("status")
                        .closed_enum("uint", vocabulary.map(Into::into))
                        .annotation("#summ(categorical)"),
                )
                .sign()
                .unwrap(),
        )
        .unwrap();
        let plan = source.summaries(1).unwrap();
        let mut bucket = plan.bucket();
        for (i, status) in vocabulary.iter().enumerate() {
            for _ in 0..=i {
                plan.ingest(&mut bucket, *status, |status, seed| {
                    seed.set_enum("status", status.into())?;
                    Ok(())
                })
                .unwrap();
            }
        }
        let sealed = plan.layout().pack([plan.seed(&bucket).unwrap()]).unwrap();
        let pack = plan.layout().open(sealed.bytes()).unwrap();
        let mut cursor = pack.reader();
        let row = cursor.get(0).unwrap();
        for threshold in [0, 200, 302, 404, 500, 999] {
            for op in [
                Compare::Eq,
                Compare::Ne,
                Compare::Gt,
                Compare::Ge,
                Compare::Lt,
                Compare::Le,
            ] {
                let expected: u64 = vocabulary
                    .iter()
                    .enumerate()
                    .filter(|(_, v)| match op {
                        Compare::Eq => **v == threshold,
                        Compare::Ne => **v != threshold,
                        Compare::Gt => **v > threshold,
                        Compare::Ge => **v >= threshold,
                        Compare::Lt => **v < threshold,
                        Compare::Le => **v <= threshold,
                    })
                    .map(|(i, _)| i as u64 + 1)
                    .sum();
                let predicate = Predicate::compare("status", op, threshold);
                assert_eq!(
                    plan.preview(&predicate).count(&row).unwrap(),
                    CountBounds::exact(expected)
                );
            }
        }
    }
}

#[test]
fn prescribing_parsed_doc_matches_loading_signed_bundle() {
    use pil_codec::Value;
    use pil_codec::api::{Doc, NativeValue};
    let document = doc::new("container")
        .field(Field::new("values").array_object("item"))
        .definition("item")
        .field(Field::new("value").r#type("uint").size(3))
        .select(0)
        .sign()
        .unwrap();
    for member in 0..document.definitions().len() {
        let parsed = document.prescribe_member(member).unwrap();
        let loaded = Doc::write_member(document.bytes().unwrap(), member).unwrap();
        assert_eq!(parsed.bytes(), loaded.bytes());
        let mut a = parsed.dispense();
        let mut b = loaded.dispense();
        if member == 0 {
            let values = vec![Data::Map(vec![("value".into(), 12345.into())])];
            a.set_array(0, values.clone()).unwrap();
            b.set_array(0, values).unwrap();
        } else {
            a.set_uint(0, 12345u32).unwrap();
            b.set_uint(0, 12345u32).unwrap();
        }
        a.cap().unwrap();
        b.cap().unwrap();
        assert_eq!(a.bytes().unwrap(), b.bytes().unwrap());
        if member == 1 {
            assert!(matches!(
                parsed
                    .decoder()
                    .read(&a.bytes().unwrap())
                    .unwrap()
                    .native_value(0)
                    .unwrap(),
                NativeValue::Scalar(Value::U24(12345))
            ));
        }
    }
    assert!(document.prescribe_member(2).is_err());
    assert!(
        document
            .replace_field(0, Field::new("changed").r#type("uint"))
            .prescribe()
            .is_err()
    );
}

#[test]
fn prepared_class_plans_share_only_with_their_immutable_source() {
    let definition = source(3, false, "ms");
    let first = definition.summaries(288).unwrap();
    let next = definition.summaries(12).unwrap();
    assert!(Arc::ptr_eq(
        first.layout().definition(),
        next.layout().definition()
    ));
    let unrelated = source(3, false, "ms").summaries(288).unwrap();
    assert!(!Arc::ptr_eq(
        first.layout().definition(),
        unrelated.layout().definition()
    ));
    let changed = source(5, false, "ms").summaries(288).unwrap();
    assert_ne!(
        first.layout().definition().prescription().binding_bytes(),
        changed.layout().definition().prescription().binding_bytes()
    );
    let weak = Arc::downgrade(first.layout().definition());
    drop(first);
    drop(next);
    assert!(weak.upgrade().is_some());
    drop(definition);
    assert!(weak.upgrade().is_none(), "no cache/source ownership cycle");
}
