use data_seeds::{summaries::*, *};
use pil_codec::r#box::Box as PackBox;
use std::sync::Arc;

fn layout(rows: usize) -> Arc<SummaryLayout> {
    let document = doc::new("measurement-summary")
        .field(Field::new("count").r#type("uint").size(8))
        .field(Field::new("min").r#type("uint").size(4))
        .field(Field::new("max").r#type("uint").size(4))
        .sign()
        .unwrap();
    SummaryLayout::new(&document, rows).unwrap()
}

// Source events enter through the Seed ingress adapter. The test accumulator
// models producer-owned fixed state; no summary record is an event entry.
fn summarize(layout: &SummaryLayout, readings: impl Iterator<Item = (usize, u32)>) -> SummaryPack {
    let source = doc::new("measurement")
        .field(Field::new("value").r#type("uint").size(4))
        .sign()
        .unwrap();
    let source = SeedDefinition::prescribe(&source).unwrap();
    let mut events = source.fill(0).unwrap();
    let mut state = vec![(0u64, 0u32, 0u32); layout.rows()];
    for (bucket, value) in readings {
        let seed = source
            .ingest(value, |value, seed| {
                seed.set_uint("value", value as u64)?;
                let row = &mut state[bucket];
                row.1 = if row.0 == 0 { value } else { row.1.min(value) };
                row.2 = row.2.max(value);
                row.0 += 1;
                Ok(())
            })
            .unwrap();
        events.load(seed).unwrap();
    }
    events.seal().unwrap();
    let rows = state.into_iter().map(|(count, min, max)| {
        let mut row = layout.definition().seed();
        row.set_uint("count", count)
            .unwrap()
            .set_uint("min", min as u64)
            .unwrap()
            .set_uint("max", max as u64)
            .unwrap();
        row
    });
    layout.pack(rows).unwrap()
}

#[test]
fn empty_and_active_days_keep_fixed_rows_with_box_directory_addressing() {
    let layout = layout(288);
    let empty = summarize(&layout, std::iter::empty());
    let active = summarize(&layout, (0..1200).map(|n| (n % 288, (n * 17) as u32)));
    assert_eq!(empty.bytes().len(), active.bytes().len());
    let width = empty.bytes().len();
    let mut file = SummaryBox::new(layout).unwrap();
    file.append(empty).unwrap().append(active).unwrap();
    file.commit().unwrap();
    let box_view = PackBox::open(file.bytes().unwrap()).unwrap();
    assert!(!box_view.options().fixed_length_objects);
    assert_eq!(
        box_view.get(1).unwrap().as_ptr() as usize - box_view.get(0).unwrap().as_ptr() as usize,
        width
    );
    let reader = SummaryBoxReader::open(file.bytes().unwrap()).unwrap();
    assert_eq!(reader.len(), 2);
    let active = reader.pack(1).unwrap();
    assert!(active.options().fixed);
    assert_eq!(active.options().address_bytes, 0);
    assert!(!active.options().common_values);
    let mut rows = active.reader();
    let row = rows.get(0).unwrap();
    assert_eq!(row.mode(), EncodingMode::FixedFull);
    assert!(matches!(
        row.native_value(0).unwrap(),
        pil_codec::api::NativeValue::Scalar(pil_codec::Value::U64(5))
    ));
}

#[test]
fn selected_summary_packs_query_shared_bytes_with_seed_predicates() {
    let layout = layout(288);
    let mut file = SummaryBox::new(layout.clone()).unwrap();
    file.append(summarize(&layout, [(0, 7)].into_iter()))
        .unwrap();
    file.append(summarize(&layout, [(1, 10001), (1, 10002)].into_iter()))
        .unwrap();
    file.append(summarize(&layout, [(2, 90000)].into_iter()))
        .unwrap();
    file.commit().unwrap();
    let bytes = Arc::new(file.bytes().unwrap().to_vec());
    let packs = Packs::open_summary_box(bytes.clone(), 1..2).unwrap();
    assert!(
        Arc::strong_count(&bytes) > 1,
        "selected Pack must retain the Box allocation"
    );
    let query = Query::new()
        .filter(Predicate::compare("max", Compare::Gt, 10000u64))
        .select(["count", "max"]);
    let batch = packs.query(query.clone()).unwrap().fetch(288).unwrap();
    let objects = batch.objects().unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(
        objects[0],
        vec![
            ("count".into(), 2u64.into()),
            ("max".into(), 10002u64.into())
        ]
    );
    let beds = Beds::open_summary_box(bytes.clone(), 1..2).unwrap();
    assert_eq!(
        beds.query(query)
            .unwrap()
            .fetch(288)
            .unwrap()
            .objects()
            .unwrap(),
        objects
    );
    assert!(Packs::open_summary_box(bytes.clone(), 0..4).is_err());
    assert!(
        Packs::open_summary_box(bytes, 2..2)
            .unwrap()
            .query(Query::new())
            .unwrap()
            .fetch(1)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn commit_reopen_append_preserves_child_addresses_and_seal_rejects_writes() {
    let layout = layout(12);
    let mut file = SummaryBox::new(layout.clone()).unwrap();
    let first = summarize(&layout, [(0, 5)].into_iter());
    let expected = first.bytes().to_vec();
    file.append(first).unwrap();
    let commit = file.commit().unwrap();
    let mut disk = commit.bytes.to_vec();
    let mut reopened = SummaryBox::open(disk.clone()).unwrap();
    reopened
        .append(summarize(&layout, [(11, u32::MAX)].into_iter()))
        .unwrap();
    let commit = reopened.commit().unwrap();
    assert_eq!(commit.offset as usize, expected.len());
    disk.resize(commit.length as usize, 0);
    disk[commit.offset as usize..].copy_from_slice(commit.bytes);
    assert_eq!(disk, reopened.bytes().unwrap());
    assert_eq!(PackBox::open(&disk).unwrap().get(0).unwrap(), expected);
    reopened.seal().unwrap();
    assert!(
        reopened
            .append(summarize(&layout, std::iter::empty()))
            .is_err()
    );
    assert_eq!(reopened.len(), 2);
}

#[test]
fn reject_variable_sparse_incomplete_and_register_changed_shapes() {
    let variable = doc::new("variable")
        .field(Field::new("message").r#type("tstr"))
        .sign()
        .unwrap();
    assert!(SummaryLayout::new(&variable, 12).is_err());
    let layout = layout(12);
    assert!(
        layout
            .pack((0..12).map(|_| layout.definition().seed()))
            .is_err()
    );
    assert!(layout.pack(std::iter::empty()).is_err());
    let mut file = SummaryBox::new(layout.clone()).unwrap();
    file.append(summarize(&layout, std::iter::empty())).unwrap();
    let changed = doc::new("measurement-summary")
        .field(Field::new("count").r#type("uint").size(8))
        .field(Field::new("min").r#type("uint").size(2))
        .field(Field::new("max").r#type("uint").size(2))
        .sign()
        .unwrap();
    let changed = SummaryLayout::new(&changed, 12).unwrap();
    file.append(summarize(&changed, std::iter::empty())).unwrap();
    assert_eq!(file.len(), 2);
    assert_eq!(file.definition_transitions().len(), 2);
    assert_eq!(file.pack(1).unwrap().len(), 12);
}

#[test]
fn annotations_do_not_change_the_fixed_storage_contract() {
    let a = layout(12);
    let document = doc::new("measurement-summary")
        .field(
            Field::new("count")
                .r#type("uint")
                .size(8)
                .annotation("@presentation(count)"),
        )
        .field(Field::new("min").r#type("uint").size(4))
        .field(Field::new("max").r#type("uint").size(4))
        .sign()
        .unwrap();
    let b = SummaryLayout::new(&document, 12).unwrap();
    let mut file = SummaryBox::new(a.clone()).unwrap();
    file.append(summarize(&a, std::iter::empty())).unwrap();
    file.append(summarize(&b, std::iter::empty())).unwrap();
    assert_eq!(file.len(), 2);
}

#[test]
fn daily_pack_appends_until_sealed_and_box_rejects_open_snapshots() {
    let layout = layout(288);
    let mut daily = layout.fill().unwrap();
    let source = doc::new("measurement")
        .field(Field::new("value").r#type("uint").size(4))
        .sign()
        .unwrap();
    let source = SeedDefinition::prescribe(&source).unwrap();
    let mut events = source.fill(288).unwrap();
    let mut container = SummaryBox::new(layout.clone()).unwrap();
    let mut generic_box = PackBox::new(&[], Default::default()).unwrap();
    for hour in 0..24 {
        for interval in 0..12 {
            let mut aggregate = (0u64, 0u64, 0u64);
            let event = source
                .ingest((hour * 12 + interval) as u64, |value, seed| {
                    seed.set_uint("value", value)?;
                    aggregate = (1, value, value);
                    Ok(())
                })
                .unwrap();
            events.load(event).unwrap();
            let mut row = layout.definition().seed();
            row.set_uint("count", aggregate.0)
                .unwrap()
                .set_uint("min", aggregate.1)
                .unwrap()
                .set_uint("max", aggregate.2)
                .unwrap();
            daily.load(row).unwrap();
        }
        assert!(!daily.is_sealed());
        let snapshot = daily.snapshot().unwrap();
        let view = layout.definition().prescription().open(&snapshot).unwrap();
        assert!(!view.is_sealed());
        assert_eq!(view.len(), (hour + 1) * 12);
        // Even a full day's recovery snapshot is not a sealed child.
        assert!(generic_box.pack(snapshot.clone()).is_err());
        assert!(generic_box.is_empty());
        assert!(container.is_empty());
        if hour < 23 {
            assert!(daily.seal().is_err());
            assert!(!daily.is_sealed());
        }
    }
    events.seal().unwrap();
    let sealed = daily.seal().unwrap();
    assert!(daily.is_sealed());
    assert!(daily.load(layout.definition().seed()).is_err());
    assert!(daily.snapshot().is_err());
    assert!(daily.seal().is_err());
    container.append(sealed).unwrap();
    container.commit().unwrap();
    assert_eq!(container.len(), 1);
    assert_eq!(container.pack(0).unwrap().len(), 288);
}

#[test]
fn footer_only_summary_storage_reads_and_appends_without_historical_payloads() {
    let layout = layout(288);
    let mut file = SummaryBox::new(layout.clone()).unwrap();
    file.append(summarize(&layout, [(0,17)].into_iter())).unwrap();
    let mut disk = file.commit().unwrap().bytes.to_vec();
    let first = file.pack_range(0).unwrap();
    for day in 1..4 {
        let footer = PackBox::footer_range(&disk[disk.len().saturating_sub(27)..], disk.len() as u64).unwrap();
        let mut file = SummaryBox::open_footer(disk[footer.start as usize..].to_vec(), disk.len() as u64).unwrap();
        assert_eq!(file.pack_range(0).unwrap(), first);
        assert!(file.bytes().is_err());
        file.append(summarize(&layout, [(day,day as u32 * 10001)].into_iter())).unwrap();
        let commit = file.commit().unwrap();
        assert_eq!(commit.offset, footer.start);
        assert_eq!(commit.bytes.len() as u64, commit.length - commit.offset);
        disk.truncate(commit.offset as usize);
        disk.extend_from_slice(commit.bytes);
        let footer = PackBox::footer_range(&disk,disk.len() as u64).unwrap();
        let reader = SummaryBoxReader::open_footer(&disk[footer.start as usize..],disk.len() as u64).unwrap();
        assert_eq!(reader.storage_range(0).unwrap(),first);
        let range = reader.storage_range(day).unwrap();
        let selected = reader.open_pack(&disk[range.start as usize..range.end as usize]).unwrap();
        assert_eq!(selected.len(),288);
        let mut cursor = selected.reader();
        assert!(matches!(cursor.get(day).unwrap().native_value(2).unwrap(),pil_codec::api::NativeValue::Scalar(pil_codec::Value::U32(value)) if value == day as u32 * 10001));
    }
}

#[test]
fn version_directory_preserves_offsets_through_size_changes_and_rollbacks() {
    let a = layout(288);
    let d = doc::new("measurement-summary-wide")
        .field(Field::new("count").r#type("uint").size(8))
        .field(Field::new("min").r#type("uint").size(8))
        .field(Field::new("max").r#type("uint").size(8))
        .sign().unwrap();
    let b = SummaryLayout::new(&d, 288).unwrap();
    let mut file = SummaryBox::new(a.clone()).unwrap();
    file.append(summarize(&a, [(0,7)].into_iter())).unwrap();
    file.append(summarize(&a, [(0,9)].into_iter())).unwrap();
    let first = file.pack_range(0).unwrap();
    let second = file.pack_range(1).unwrap();
    let mut disk = file.commit().unwrap().bytes.to_vec();
    assert_eq!(file.definition_transitions().len(), 1);
    assert!(PackBox::open(&disk).unwrap().headers().contains_key("box.versions"));
    let original = disk[..second.end as usize].to_vec();
    // Every append resumes from the footer alone, including the initial
    // fixed-to-indexed conversion. Historical Pack bodies are unavailable.
    for (i, shape) in [b.clone(), a.clone(), b.clone(), b.clone()].into_iter().enumerate() {
        let footer = PackBox::footer_range(&disk, disk.len() as u64).unwrap();
        let mut file = SummaryBox::open_footer(disk[footer.start as usize..].to_vec(),disk.len() as u64).unwrap();
        file.append(summarize(&shape, [(0,100+i as u32)].into_iter())).unwrap();
        assert_eq!(file.pack_range(0).unwrap(),first);
        assert_eq!(file.pack_range(1).unwrap(),second);
        let commit = file.commit().unwrap();
        assert_eq!(commit.offset,footer.start);
        disk.truncate(commit.offset as usize);
        disk.extend_from_slice(commit.bytes);
        assert_eq!(&disk[..original.len()], original);
    }
    let reader = SummaryBoxReader::open(&disk).unwrap();
    use pil_codec::r#box::versions::Binding;
    assert_eq!(reader.definition_transitions().iter().map(|r|(r.binding,r.offset)).collect::<Vec<_>>(),
        [(Binding::Definition(0),0),
         (Binding::Definition(1),reader.storage_range(2).unwrap().start),
         (Binding::Definition(0),reader.storage_range(3).unwrap().start),
         (Binding::Definition(1),reader.storage_range(4).unwrap().start)]);
    assert!(!PackBox::open(&disk).unwrap().options().fixed_length_objects);
    let footer = PackBox::footer_range(&disk,disk.len() as u64).unwrap();
    let metadata = SummaryBoxReader::open_footer(&disk[footer.start as usize..],disk.len() as u64).unwrap();
    for ordinal in [5,0,3,2,4,1] {
        let range = metadata.storage_range(ordinal).unwrap();
        let raw = &disk[range.start as usize..range.end as usize];
        assert!(metadata.open_pack(raw).is_err());
        let pack = metadata.open_pack_at(ordinal,raw).unwrap();
        assert_eq!(pack.len(),288);
        let mut cursor = pack.reader();
        let row = cursor.get(0).unwrap();
        let values = row.crush().unwrap();
        let expected = [7,9,100,101,102,103][ordinal];
        assert_eq!(values["max"],Some(serde_json::json!(expected)));
    }
    // Shared-buffer query loading must select each day's prescription too.
    let store = Packs::open_summary_box(Arc::new(disk.clone()),0..6).unwrap();
    let objects = store.query(Query::new().filter(Predicate::compare("count",Compare::Gt,0u64)))
        .unwrap().fetch(20).unwrap().objects().unwrap();
    assert_eq!(objects.len(),6);
    let mut file = SummaryBox::open(disk).unwrap();
    file.seal().unwrap();
    assert!(file.append(summarize(&a,std::iter::empty())).is_err());
    assert_eq!(SummaryBoxReader::open(file.bytes().unwrap()).unwrap().len(),6);
}
