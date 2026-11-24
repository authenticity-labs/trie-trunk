use ciborium::Value as Data;
use data_seeds::{summaries::*, *};
use pil_codec::api::{FieldBytes, NativeValue};
use std::sync::Arc;

fn document() -> Document {
    doc::new("summary-arrays")
        .field(
            Field::new("measurements")
                .array_object("measurement")
                .size(0),
        )
        .field(Field::new("counts").array("uint").element_size(3).size(0))
        .definition("measurement")
        .field(Field::new("min").r#type("uint").size(4))
        .field(Field::new("max").r#type("uint").size(4))
        .select(0)
        .sign()
        .unwrap()
}

// Only ingest creates event values; summary rows are accumulator output.
fn pack(layout: &SummaryLayout, input: &[u32]) -> SummaryPack {
    let source = SeedDefinition::prescribe(
        &doc::new("measurement")
            .field(Field::new("value").r#type("uint").size(4))
            .sign()
            .unwrap(),
    )
    .unwrap();
    let mut events = source.fill(input.len()).unwrap();
    let mut lo = u32::MAX;
    let mut hi = 0;
    let mut count = 0u32;
    for &value in input {
        events
            .load(
                source
                    .ingest(value, |v, seed| {
                        seed.set_uint("value", v)?;
                        lo = lo.min(v);
                        hi = hi.max(v);
                        count += 1;
                        Ok(())
                    })
                    .unwrap(),
            )
            .unwrap();
    }
    events.seal().unwrap();
    if count == 0 {
        lo = 0;
    }
    let lengths = layout.definition().prescription().array_lengths();
    let objects = lengths.iter().find(|p| p.0 == 0).unwrap().1;
    let counters = lengths.iter().find(|p| p.0 == 1).unwrap().1;
    layout
        .pack((0..layout.rows()).map(|_| {
            let mut row = layout.definition().seed();
            row.set_array(
                "measurements",
                (0..objects)
                    .map(|_| Data::Map(vec![("min".into(), lo.into()), ("max".into(), hi.into())]))
                    .collect(),
            )
            .unwrap();
            row.set_array("counts", vec![count.into(); counters])
                .unwrap();
            row
        }))
        .unwrap()
}

#[test]
fn sizes_bind_plans_without_new_definitions_and_survive_box_reopen() {
    let document = document();
    let bytes = document.bytes().unwrap().to_vec();
    let a = SummaryLayout::arrays(&document, 12, &[(0, 1), (1, 2)]).unwrap();
    let b = SummaryLayout::arrays(&document, 12, &[(0, 3), (1, 5)]).unwrap();
    assert_eq!(a.definition().prescription().bytes(), bytes);
    assert_eq!(b.definition().prescription().bytes(), bytes);
    let empty = pack(&a, &[]);
    let active = pack(&a, &[7, 10, 99]);
    assert_eq!(empty.bytes().len(), active.bytes().len());
    let mut file = SummaryBox::new(a.clone()).unwrap();
    file.append(active)
        .unwrap()
        .append(pack(&b, &[8, 21]))
        .unwrap();
    file.commit().unwrap();
    let owner = Arc::new(file.bytes().unwrap().to_vec());
    let reader = SummaryBoxReader::open(&owner).unwrap();
    for (day, lengths) in [(0, vec![(0, 1), (1, 2)]), (1, vec![(0, 3), (1, 5)])] {
        let opened = reader.pack(day).unwrap();
        assert!(opened.options().fixed);
        assert_eq!(opened.options().address_bytes, 0);
        let mut cursor = opened.reader();
        let row = cursor.get(11).unwrap();
        assert_eq!(row.mode(), EncodingMode::FixedFull);
        assert_eq!(row.prescription().array_lengths(), lengths);
        let FieldBytes::Bytes(raw) = row.field(0).unwrap() else {
            panic!()
        };
        let object = row
            .prescription()
            .array_element(0, lengths[0].1 - 1, raw)
            .unwrap();
        let rx = document.prescribe_member(1).unwrap();
        let mut decoder = rx.decoder();
        let NativeValue::Scalar(pil_codec::Value::U32(max)) =
            decoder.read(object).unwrap().native_value(1).unwrap()
        else {
            panic!()
        };
        assert_eq!(max, if day == 0 { 99 } else { 21 });
    }
}

#[test]
fn seed_readers_restore_pack_dimensions_and_retain_shared_storage() {
    let document = document();
    let base = SeedDefinition::prescribe(&document).unwrap();
    let layout = SummaryLayout::arrays(&document, 12, &[(0, 1), (1, 2)]).unwrap();
    let sealed = pack(&layout, &[42]);
    let owner = Arc::new([&[0u8; 3][..], sealed.bytes(), &[0u8; 4]].concat());
    let view = base
        .open_range(owner.clone(), 3..3 + sealed.bytes().len())
        .unwrap();
    assert_eq!(view.stored_bytes().as_ptr(), owner[3..].as_ptr());
    assert_eq!(
        view.definition().prescription().array_lengths(),
        &[(0, 1), (1, 2)]
    );
    assert!(base.prescription().array_lengths().is_empty());
    let owned = base.open(sealed.bytes().to_vec()).unwrap();
    assert_eq!(
        owned.definition().prescription().array_lengths(),
        &[(0, 1), (1, 2)]
    );
}

#[test]
fn zero_lengths_and_wrong_dimensions_preserve_the_fixed_contract() {
    let document = document();
    let empty = SummaryLayout::arrays(&document, 12, &[(0, 0), (1, 0)]).unwrap();
    let data = pack(&empty, &[]);
    let opened = empty.open(data.bytes()).unwrap();
    assert_eq!(opened.len(), 12);
    let mut cursor = opened.reader();
    assert_eq!(cursor.get(11).unwrap().mode(), EncodingMode::FixedFull);
    let wrong = SummaryLayout::arrays(&document, 12, &[(0, 1), (1, 0)]).unwrap();
    assert!(wrong.open(data.bytes()).is_err());
    assert!(SummaryLayout::arrays(&document, 12, &[(0, usize::MAX)]).is_err());
}

#[test]
fn bound_object_arrays_match_individual_objects_for_wide_scalars() {
    use ciborium::Value as Data;
    use data_seeds::{Field, SeedDefinition, doc};
    use pil_codec::api::FieldBytes;
    let document = doc::new("objects")
        .field(Field::new("rows").array_object("row"))
        .field(Field::new("single").object("row"))
        .definition("row")
        .field(Field::new("wide").r#type("uint").size(16))
        .field(Field::new("signed").r#type("int").size(6))
        .field(Field::new("ratio").r#type("float").size(8))
        .field(Field::new("enabled").r#type("bool"))
        .field(Field::new("tag").r#type("tstr").size(4))
        .select(0)
        .sign()
        .unwrap();
    let source = SeedDefinition::prescribe(&document)
        .unwrap()
        .arrays(&[(0, 2)])
        .unwrap();
    let object = vec![
        (Data::from("wide"), pil_codec::native_unsigned(u128::MAX)),
        ("signed".into(), (-(1i64 << 39)).into()),
        ("ratio".into(), Data::Float(-0.0)),
        ("enabled".into(), true.into()),
        ("tag".into(), "abc".into()),
    ];
    for reversed in [false, true] {
        let mut values = object.clone();
        if reversed {
            values.reverse();
        }
        let mut seed = source
            .ingest(values, |values, seed| {
                seed.set_array(
                    "rows",
                    vec![Data::Map(values.clone()), Data::Map(values.clone())],
                )?;
                seed.set_object("single", values)?;
                Ok(())
            })
            .unwrap();
        seed.cap().unwrap();
        let bytes = seed.bytes().unwrap();
        let mut decoder = source.prescription().decoder();
        let row = decoder.read(&bytes).unwrap();
        let FieldBytes::Bytes(array) = row.field(0).unwrap() else {
            panic!()
        };
        let FieldBytes::Bytes(single) = row.field(1).unwrap() else {
            panic!()
        };
        for i in 0..2 {
            assert_eq!(
                source.prescription().array_element(0, i, array).unwrap(),
                single
            );
        }
    }
    for (field, invalid) in [(1, Data::from(1i64 << 47)), (4, Data::from("abcde"))] {
        let mut values = object.clone();
        values[field].1 = invalid;
        assert!(
            source
                .ingest(values, |values, seed| {
                    seed.set_array("rows", vec![Data::Map(values.clone()), Data::Map(values)])?;
                    Ok(())
                })
                .is_err()
        );
    }
}
