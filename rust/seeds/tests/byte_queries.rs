use data_seeds::*;

#[test]
fn compound_byte_predicates_work_across_all_four_encoding_paths() {
    for signed in [false, true] {
        for mode in [
            EncodingMode::FixedFull,
            EncodingMode::FixedSparse,
            EncodingMode::VariableFast,
            EncodingMode::VariableConcise,
        ] {
            let variable = matches!(
                mode,
                EncodingMode::VariableFast | EncodingMode::VariableConcise
            );
            let doc = doc::new("byte-query")
                .field(
                    Field::new("n")
                        .r#type(if signed { "int" } else { "uint" })
                        .size(1),
                )
                .field(if variable {
                    Field::new("other").r#type("tstr")
                } else {
                    Field::new("other").r#type("uint").size(1)
                })
                .sign()
                .unwrap();
            let def = SeedDefinition::prescribe(&doc).unwrap();
            let mut pack = def.fill(256).unwrap();
            for i in 0..256 {
                let text = format!("input-{i}");
                let mut seed = def
                    .ingest(i, |i, s| {
                        if signed {
                            s.set_int("n", i as u8 as i8)?;
                        } else {
                            s.set_uint("n", i as u8)?;
                        }
                        if variable {
                            s.set_text_string("other", &text)?;
                        } else if mode == EncodingMode::FixedFull {
                            s.set_uint("other", (i % 7) as u8)?;
                        } else if i % 2 == 0 {
                            s.set_null("other")?;
                        }
                        Ok(())
                    })
                    .unwrap();
                if mode == EncodingMode::VariableConcise {
                    seed.squeeze().unwrap();
                }
                assert_eq!(seed.mode().unwrap(), mode);
                pack.load(seed).unwrap();
            }
            pack.seal().unwrap();
            let pack = def.open(pack.into_bytes().unwrap()).unwrap();
            for predicate in [
                Predicate::And(vec![
                    Predicate::compare("n", Compare::Ge, -17),
                    Predicate::compare("n", Compare::Le, 71),
                ]),
                Predicate::Or(
                    [-128, -1, 1, 20, 127, 255]
                        .map(|n| Predicate::compare("n", Compare::Eq, n))
                        .to_vec(),
                ),
            ] {
                let expected = (0..256)
                    .filter(|&i| {
                        let n = if signed {
                            i as u8 as i8 as i32
                        } else {
                            i as i32
                        };
                        if matches!(predicate, Predicate::And(_)) {
                            (-17..=71).contains(&n)
                        } else {
                            [-128, -1, 1, 20, 127, 255].contains(&n)
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    pack.filter(predicate.clone()).unwrap().fetch(256).unwrap(),
                    expected,
                    "{mode:?}, signed={signed}"
                );
                let mut sources = Packs::new();
                sources.pack(pack.clone());
                let mut cursor = sources
                    .query(Query::new().filter(predicate).select(["n"]))
                    .unwrap();
                let mut actual = Vec::new();
                loop {
                    let slice = cursor.poll(13, 17).unwrap();
                    assert!(slice.work <= 17);
                    actual.extend((0..slice.batch.len()).map(|i| slice.batch.origin(i).unwrap().1));
                    if slice.done {
                        break;
                    }
                }
                assert_eq!(actual, expected);
            }
        }
    }
}
