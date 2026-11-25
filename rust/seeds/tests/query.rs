use ciborium::Value as Data;
use data_seeds::*;
use std::sync::Arc;

fn definition() -> Arc<SeedDefinition> {
    let document = doc::new("measurement")
        .field(Field::new("sequence").r#type("uint").size(8))
        .field(Field::new("reading").r#type("uint").size(4))
        .field(Field::new("note").r#type("tstr"))
        .field(Field::new("extra").array("any").annotation("@fields"))
        .sign()
        .unwrap();
    SeedDefinition::prescribe(&document).unwrap()
}
fn ingress(def: &Arc<SeedDefinition>, start: u64, count: usize) -> Arc<SeedPackSnapshot> {
    ingress_pack(def, start, count, false)
}
fn ingress_pack(def: &Arc<SeedDefinition>, start: u64, count: usize, stored: bool) -> Arc<SeedPackSnapshot> {
    let mut pack = def.fill(count).unwrap();
    if stored {
        pack.header(integrity::HEADER, vec![0; 4]).unwrap();
    }
    for i in start..start + count as u64 {
        let seed = def
            .ingest(i, |i, s| {
                s.set_uint("sequence", i)?
                    .set_uint("reading", (i * 17) % 1000)?;
                if i % 3 == 0 {
                    s.set_null("note")?;
                } else if i % 3 == 1 {
                    s.set_text_string("note", "observed")?;
                }
                Ok(())
            })
            .unwrap();
        pack.load(seed).unwrap();
    }
    pack.seal().unwrap();
    let mut bytes = pack.into_bytes().unwrap();
    if stored {
        integrity::stamp(&mut bytes).unwrap();
    }
    def.open(bytes).unwrap()
}
fn numbers(batch: &Batch, name: &str) -> Vec<u64> {
    batch
        .objects()
        .unwrap()
        .iter()
        .map(|o| {
            let Data::Integer(v) = o.iter().find(|(n, _)| n == name).unwrap().1 else {
                panic!()
            };
            u64::try_from(v).unwrap()
        })
        .collect()
}

#[test]
fn resumable_scan_yields_without_matches_and_can_be_cancelled_between_slices() {
    let def = definition();
    let pack = ingress(&def, 0, 5000);
    let mut sources = Packs::new(); sources.pack(pack.clone());
    let mut slow = sources.query(Query::new().filter(Predicate::compare("sequence", Compare::Gt, 99999))).unwrap();
    let mut fast = sources.query(Query::new().select(["sequence"]).limit(1)).unwrap();
    let slice = slow.poll(10, 17).unwrap();
    assert!(!slice.done); assert!(slice.batch.is_empty());
    assert_eq!(slice.work, 17); assert_eq!(slow.scanned, 17);
    assert_eq!(numbers(&fast.poll(1, 1).unwrap().batch, "sequence"), vec![0]);
    // Accepted writes/ingestion can run between slices; no cursor owns a node lock.
    sources.pack(ingress(&def, 5000, 2));
    let pins = Arc::strong_count(&pack);
    slow.cancellation().store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(slow.poll(10, 17).is_err());
    assert!(Arc::strong_count(&pack) < pins);
}

#[test]
fn resumable_ordering_matches_existing_encoder_ingested_query_results() {
    let def = definition(); let mut sources = Packs::new();
    for i in 0..7 { sources.pack(ingress(&def, i * 37, 37)); }
    for direction in [Direction::Ascending, Direction::Descending] {
        for field in ["sequence", "reading", "note"] {
            let query = Query::new().select(["sequence"]).order_by(field, direction).offset(9).limit(31);
            let expected = numbers(&sources.query(query.clone()).unwrap().fetch(100).unwrap(), "sequence");
            for work in [1, 7, 64] {
                let mut cursor = sources.query(query.clone()).unwrap(); let mut got = Vec::new();
                for turn in 0..1000 {
                    let slice = cursor.poll(5, work).unwrap();
                    assert!(slice.work <= work); got.extend(numbers(&slice.batch, "sequence"));
                    if slice.done { break; }
                    assert!(turn < 999, "cursor did not finish");
                }
                assert_eq!(got, expected);
                assert!(cursor.poll(5, work).unwrap().done);
            }
        }
    }
}

#[test]
fn resumable_unordered_offset_limit_and_returned_batch_survive_cursor_release() {
    let def = definition(); let mut sources = Packs::new(); sources.pack(ingress(&def, 0, 50));
    let mut cursor = sources.query(Query::new().select(["sequence"]).offset(12).limit(3)).unwrap();
    let first = cursor.poll(3, 12).unwrap(); assert!(!first.done); assert!(first.batch.is_empty());
    let last = cursor.poll(3, 3).unwrap(); assert!(last.done);
    assert_eq!(cursor.scanned, 15); drop(cursor); drop(sources);
    assert_eq!(numbers(&last.batch, "sequence"), vec![12,13,14]);
}

#[test]
fn shared_sort_budget_rejects_before_allocation_and_releases_on_abort() {
    let def = definition(); let mut sources = Packs::new(); sources.pack(ingress(&def, 0, 100));
    let query = Query::new().order_by("sequence", Direction::Ascending).limit(80);
    let probe = MemoryBudget::new(1024*1024);
    let mut first = sources.query(query.clone()).unwrap().memory_budget(probe.clone()).unwrap();
    assert!(!first.poll(1,1).unwrap().done);
    let needed = probe.used(); assert!(needed > 0); first.abort(); assert_eq!(probe.used(),0);
    let shared = MemoryBudget::new(needed);
    let mut first = sources.query(query.clone()).unwrap().memory_budget(shared.clone()).unwrap();
    first.poll(1,1).unwrap();
    let mut second = sources.query(query).unwrap().memory_budget(shared.clone()).unwrap();
    assert!(second.poll(1,1).is_err()); assert_eq!(shared.used(),needed);
    first.abort(); assert_eq!(shared.used(),0);
    let huge = sources.query(Query::new().order_by("sequence",Direction::Ascending).limit(usize::MAX)).unwrap();
    assert!(huge.memory_budget(shared.clone()).unwrap().poll(1,1).is_err());
    assert_eq!(shared.used(),0);
}

#[test]
fn sort_key_memory_is_charged_and_cancelled_independently() {
    let def = definition(); let mut sources = Packs::new(); sources.pack(ingress(&def, 1, 20));
    let budget = MemoryBudget::new(1024*1024);
    let mut cursor = sources.query(Query::new().order_by("note",Direction::Ascending).limit(10)).unwrap().memory_budget(budget.clone()).unwrap();
    cursor.poll(1,1).unwrap(); let with_text = budget.used();
    let bare = MemoryBudget::new(1024*1024);
    let mut numeric = sources.query(Query::new().order_by("sequence",Direction::Ascending).limit(10)).unwrap().memory_budget(bare.clone()).unwrap();
    numeric.poll(1,1).unwrap(); assert_eq!(with_text,bare.used()+"observed".len());
    cursor.abort(); assert_eq!(budget.used(),0); assert!(bare.used()>0);
}

#[test]
fn zero_limit_and_empty_packs_do_not_bypass_poll_budget() {
    let def = definition(); let mut sources = Packs::new();
    for _ in 0..100 { sources.pack(ingress(&def,0,0)); }
    sources.pack(ingress(&def,0,2));
    let mut cursor = sources.query(Query::new()).unwrap();
    let slice = cursor.poll(1,3).unwrap(); assert_eq!(slice.work,3); assert!(!slice.done); assert!(slice.batch.is_empty());
    let mut zero = sources.query(Query::new().order_by("sequence",Direction::Ascending).limit(0)).unwrap();
    assert!(zero.poll(1,1).unwrap().done);
}
#[test]
fn thousand_packs_one_global_ordered_limit() {
    let def = definition();
    let mut packs = Packs::new();
    for p in 0..1000 {
        packs.pack(ingress(&def, p * 10, 10));
    }
    let mut cursor = packs
        .query(
            Query::new()
                .select(["sequence"])
                .order_by("sequence", Direction::Descending)
                .limit(10),
        )
        .unwrap();
    let first = cursor.fetch(4).unwrap();
    let next = cursor.fetch(100).unwrap();
    assert_eq!(numbers(&first, "sequence"), vec![9999, 9998, 9997, 9996]);
    assert_eq!(
        numbers(&next, "sequence"),
        vec![9995, 9994, 9993, 9992, 9991, 9990]
    );
    assert!(cursor.fetch(1).unwrap().is_empty());
    assert_eq!(cursor.scanned, 10_000);
}
#[test]
fn unordered_limit_stops_globally_and_pack_fetch_resumes() {
    let def = definition();
    let pack = ingress(&def, 0, 30);
    let filter = Predicate::compare("sequence", Compare::Ge, Data::Integer(10.into()));
    let mut scan = pack.filter(filter.clone()).unwrap();
    assert_eq!(scan.fetch(2).unwrap(), vec![10, 11]);
    assert_eq!(scan.fetch(2).unwrap(), vec![12, 13]);
    let mut packs = Packs::new();
    packs.pack(pack).pack(ingress(&def, 30, 30));
    let mut c = packs
        .query(Query::new().filter(filter).offset(2).limit(3))
        .unwrap();
    assert_eq!(numbers(&c.fetch(10).unwrap(), "sequence"), vec![12, 13, 14]);
    assert_eq!(c.scanned, 15);
    assert!(c.fetch(10).unwrap().is_empty());
}
#[test]
fn exact_uint64_sort_and_filter() {
    let def = definition();
    let mut packs = Packs::new();
    packs.pack(ingress(&def, 9_007_199_254_740_992, 3));
    let q = Query::new()
        .filter(Predicate::compare(
            "sequence",
            Compare::Gt,
            Data::Integer(9_007_199_254_740_992u64.into()),
        ))
        .order_by("sequence", Direction::Descending)
        .offset(1)
        .limit(1);
    assert_eq!(
        numbers(&packs.query(q).unwrap().fetch(10).unwrap(), "sequence"),
        vec![9_007_199_254_740_993]
    );
}
#[test]
fn absent_null_and_three_valued_not_remain_distinct() {
    let def = definition();
    let pack = ingress(&def, 0, 6);
    assert_eq!(
        pack.filter(Predicate::IsNull("note".into()))
            .unwrap()
            .fetch(10)
            .unwrap(),
        vec![0, 3]
    );
    assert_eq!(
        pack.filter(Predicate::IsAbsent("note".into()))
            .unwrap()
            .fetch(10)
            .unwrap(),
        vec![2, 5]
    );
    let not = Predicate::Not(Box::new(Predicate::compare(
        "note",
        Compare::Eq,
        Data::Text("different".into()),
    )));
    assert_eq!(pack.filter(not).unwrap().fetch(10).unwrap(), vec![1, 4]);
    let mut packs = Packs::new();
    packs.pack(pack);
    let b = packs
        .query(Query::new().select(["note"]))
        .unwrap()
        .fetch(6)
        .unwrap();
    assert_eq!(
        b.reader().view(0).unwrap().get("note").unwrap(),
        Cell::Value(Data::Null)
    );
    assert_eq!(
        b.reader().view(2).unwrap().get("note").unwrap(),
        Cell::Absent
    );
    assert!(b.objects().unwrap()[2].is_empty());
}
#[test]
fn dynamic_objects_and_annotations_stay_codec_owned() {
    let def = definition();
    assert_eq!(def.annotations(3).unwrap(), ["@fields"]);
    let extra = Data::Map(vec![(
        Data::Text("unplanned".into()),
        Data::Array(vec![
            Data::Null,
            Data::Bool(true),
            Data::Integer(u64::MAX.into()),
        ]),
    )]);
    let mut seed = def.seed();
    seed.set_array("extra", vec![extra.clone()]).unwrap();
    let mut p = def.fill(1).unwrap();
    p.load(seed).unwrap();
    p.seal().unwrap();
    let mut packs = Packs::new();
    packs.pack(def.open(p.into_bytes().unwrap()).unwrap());
    let b = packs.query(Query::new()).unwrap().fetch(1).unwrap();
    assert_eq!(
        b.reader().view(0).unwrap().get("extra").unwrap(),
        Cell::Value(Data::Array(vec![extra]))
    );
}
#[test]
fn box_children_share_bytes_and_views_outlive_store() {
    let def = definition();
    let a = ingress(&def, 0, 5);
    let b = ingress(&def, 5, 5);
    let mut container = pil_codec::r#box::Box::new(&[], Default::default()).unwrap();
    container
        .pack(a.stored_bytes())
        .unwrap()
        .pack(b.stored_bytes())
        .unwrap();
    container.commit().unwrap();
    let bytes = Arc::new(container.bytes().unwrap().to_vec());
    let borrowed = pil_codec::r#box::Box::open(&bytes).unwrap();
    let raw = borrowed.get(0).unwrap();
    let start = raw.as_ptr() as usize - bytes.as_ptr() as usize;
    let child = def
        .open_range(bytes.clone(), start..start + raw.len())
        .unwrap();
    assert_eq!(child.stored_bytes().as_ptr(), raw.as_ptr());
    assert_eq!(child.retained_bytes(), bytes.len());
    let packs = Packs::open_box(bytes.clone(), |_| Ok(def.clone())).unwrap();
    let result = packs
        .query(
            Query::new()
                .order_by("sequence", Direction::Descending)
                .limit(3),
        )
        .unwrap()
        .fetch(10)
        .unwrap();
    drop(packs);
    drop(bytes);
    drop(child);
    assert_eq!(numbers(&result, "sequence"), vec![9, 8, 7]);
}
#[test]
fn projection_limits_cancellation_and_errors() {
    let def = definition();
    let mut packs = Packs::new();
    packs.pack(ingress(&def, 0, 10));
    assert!(packs.query(Query::new().select(["typo"])).is_err());
    let mut c = packs.query(Query::new().limit(0)).unwrap();
    assert!(c.fetch(10).unwrap().is_empty());
    assert_eq!(c.scanned, 0);
    let mut c = packs.query(Query::new()).unwrap();
    c.cancel();
    assert!(c.fetch(1).is_err());
    let mut c = packs.query(Query::new().select(["sequence"])).unwrap();
    let b = c.fetch(1).unwrap();
    assert!(b.reader().view(0).unwrap().get("note").is_err());
    assert!(def.open_range(Arc::new(vec![]), 1..2).is_err());
}

#[test]
fn float_and_integer_comparisons_do_not_round_large_integers() {
    let def = definition();
    let pack = ingress(&def, 9_007_199_254_740_992, 3);
    assert_eq!(
        pack.filter(Predicate::compare(
            "sequence",
            Compare::Gt,
            Data::Float(9_007_199_254_740_992.0)
        ))
        .unwrap()
        .fetch(10)
        .unwrap(),
        vec![1, 2]
    );
    let pack = ingress(&def, 0, 5);
    assert_eq!(
        pack.filter(Predicate::compare(
            "sequence",
            Compare::Gt,
            Data::Float(1.5)
        ))
        .unwrap()
        .fetch(10)
        .unwrap(),
        vec![2, 3, 4]
    );
}

#[test]
fn differently_ordered_definitions_bind_by_field_name() {
    let a = definition();
    let document = doc::new("other")
        .field(Field::new("note").r#type("tstr"))
        .field(Field::new("sequence").r#type("uint").size(8))
        .sign()
        .unwrap();
    let b = SeedDefinition::prescribe(&document).unwrap();
    let mut p = b.fill(1).unwrap();
    let mut s = b.seed();
    s.set_uint("sequence", 100).unwrap();
    p.load(s).unwrap();
    p.seal().unwrap();
    let mut packs = Packs::new();
    packs.pack(ingress(&a, 0, 5))
        .pack(b.open(p.into_bytes().unwrap()).unwrap());
    let mut q = packs
        .query(
            Query::new()
                .select(["sequence", "reading"])
                .order_by("sequence", Direction::Descending)
                .limit(2),
        )
        .unwrap();
    let batch = q.fetch(10).unwrap();
    assert_eq!(numbers(&batch, "sequence"), vec![100, 4]);
    assert_eq!(
        batch.reader().view(0).unwrap().get("reading").unwrap(),
        Cell::Absent
    );
}

#[test]
fn shared_ranges_read_all_modes_and_factored_common_values() {
    for variable in [false, true] {
        for sparse in [false, true] {
            for concise in [false, true] {
                let mut document = doc::new("modes")
                    .field(Field::new("n").r#type("uint").size(8))
                    .field(Field::new("flag").r#type("bool"));
                if variable {
                    document = document.field(Field::new("text").r#type("tstr"));
                }
                let def = SeedDefinition::prescribe(&document.sign().unwrap()).unwrap();
                let mut pack = def.fill(4).unwrap();
                pack.configure(PackOptions {
                    common_values: true,
                    ..Default::default()
                })
                .unwrap();
                for _ in 0..4 {
                    let mut seed = def.seed();
                    seed.set_uint("n", 17).unwrap();
                    if !sparse {
                        seed.set_bool("flag", true).unwrap();
                    }
                    if variable {
                        seed.set_text_string("text", "").unwrap();
                    }
                    if concise {
                        seed.squeeze().unwrap();
                    }
                    pack.load(seed).unwrap();
                }
                pack.seal().unwrap();
                let bytes = pack.into_bytes().unwrap();
                let length = bytes.len();
                let mut owner = vec![0; 13];
                owner.extend(bytes);
                owner.extend([0; 9]);
                let owner = Arc::new(owner);
                let p = def.open_range(owner, 13..13 + length).unwrap();
                assert_eq!(
                    p.filter(Predicate::compare(
                        "n",
                        Compare::Eq,
                        Data::Integer(17.into())
                    ))
                    .unwrap()
                    .fetch(9)
                    .unwrap(),
                    vec![0, 1, 2, 3]
                );
            }
        }
    }
}

#[cfg(feature = "compression")]
#[test]
fn compressed_pack_inside_box_uses_existing_decompressor() {
    use pil_codec::compression::Algorithm;
    let def = definition();
    let mut pack = def.fill(2).unwrap();
    for i in 0..2 {
        let seed = def.ingest(i, |i, seed| {
            seed.set_uint("sequence", i)?;
            Ok(())
        }).unwrap();
        pack.load(seed).unwrap();
    }
    pack.seal().unwrap();
    let compressed = pack
        .compress(Compression::default())
        .unwrap()
        .into_bytes()
        .into_owned();
    let mut container = pil_codec::r#box::Box::new(&[], Default::default()).unwrap();
    container.pack(compressed).unwrap();
    container.commit().unwrap();
    let packs = Packs::open_box(Arc::new(container.bytes().unwrap().to_vec()), |_| {
        Ok(def.clone())
    })
    .unwrap();
    assert_eq!(
        numbers(
            &packs.query(Query::new()).unwrap().fetch(10).unwrap(),
            "sequence"
        ),
        vec![0, 1]
    );
    // The outer Box keeps EOF controls even when its child PAC starts at BOF.
    container.seal().unwrap();
    for algorithm in [Algorithm::Gzip, Algorithm::Zstd, Algorithm::Brotli] {
        let bytes = Arc::new(container.compress(Compression {
            algorithm, level: 1, only_if_smaller: false,
        }).unwrap());
        let packs = Packs::open_box(bytes.clone(), |_| Ok(def.clone())).unwrap();
        let beds = Beds::open_box(bytes, |_| Ok(def.clone())).unwrap();
        for batch in [
            packs.query(Query::new()).unwrap().fetch(10).unwrap(),
            beds.query(Query::new()).unwrap().fetch(10).unwrap(),
        ] {
            assert_eq!(numbers(&batch, "sequence"), vec![0, 1]);
        }
    }
}

#[test]
fn capped_and_uncapped_sleeves_keep_the_shared_builder_path() {
    let def = definition();
    let mut pack = def.fill(2).unwrap();
    let mut seeds = Vec::new();
    for i in 0..6 {
        let seed = def
            .ingest(i, |i, s| {
                s.set_uint("sequence", i)?;
                Ok(())
            })
            .unwrap();
        if i % 2 == 0 {
            seed.cap().unwrap();
        }
        seeds.push(seed);
    }
    pack.refill(4).unwrap();
    pack.load(seeds).unwrap();
    let snapshot = def.open(pack.snapshot().unwrap()).unwrap();
    assert_eq!(
        snapshot.filter(Predicate::All).unwrap().fetch(10).unwrap(),
        vec![0, 1, 2, 3, 4, 5]
    );
    pack.seal().unwrap();
    let p = def.open(pack.into_bytes().unwrap()).unwrap();
    assert_eq!(
        p.filter(Predicate::All).unwrap().fetch(10).unwrap(),
        vec![0, 1, 2, 3, 4, 5]
    );
}

#[test]
fn declared_wide_unsigned_values_filter_sort_and_materialize_losslessly() {
    let definition=SeedDefinition::prescribe(&doc::new("wide")
        .field(Field::new("time").r#type("uint").size(6).annotation("@time"))
        .field(Field::new("value").r#type("uint").size(16))
        .sign().unwrap()).unwrap();
    let base=1u128<<120;
    let mut pack=definition.fill(3).unwrap();
    for i in 0..3u64 {
        pack.load(definition.ingest(i,|i,seed| {
            seed.set_uint("time",1700000000000+i)?.set_uint("value",base+i as u128)?;
            Ok(())
        }).unwrap()).unwrap();
    }
    pack.seal().unwrap();
    let mut packs=Packs::new();packs.pack(definition.open(pack.into_bytes().unwrap()).unwrap());
    let threshold=pil_codec::native_unsigned(base);
    let mut cursor=packs.query(Query::new().filter(Predicate::compare("value",Compare::Gt,threshold)).order_by("value",Direction::Descending).limit(10)).unwrap();
    let batch=cursor.fetch(10).unwrap();let objects=batch.objects().unwrap();assert_eq!(objects.len(),2);
    for (index,object) in objects.iter().enumerate() {
        let value=&object.iter().find(|(n,_)|n=="value").unwrap().1;
        assert_eq!(pil_codec::unsigned_native(value).unwrap(),base+(2-index) as u128);
    }
}

#[test]
fn time_selected_box_addresses_need_no_box_directory_or_chronological_append_order() {
    let def=definition();
    let mut box_writer=Packs::create_box(&[]).unwrap();
    let mut selected=std::collections::BTreeMap::new();
    for hour in [10,12,11] {
        let source=ingress_pack(&def,hour*100,3,true);
        let raw=source.stored_bytes().to_vec();
        #[cfg(feature = "compression")]
        let raw = if hour==12 {
            let mut compressed=vec![];
            pil_codec::compression::seal(&raw,pil_codec::compression::Algorithm::Zstd,1,false,&mut compressed).unwrap();
            compressed
        } else { raw };
        selected.insert(hour,box_writer.next_address());
        box_writer.pack(raw).unwrap();
    }
    box_writer.commit().unwrap();
    let bytes=Arc::new(box_writer.bytes().unwrap().to_vec());
    let container=pil_codec::r#box::Box::open(&bytes).unwrap();
    assert!(container.options().external_addresses);
    assert!(container.member_range(0).is_err());
    container.validate().unwrap();
    // These addresses are the outputs of the existing time lookup, not an
    // ordinal-indexed Box directory. The final Pack needs no following hour.
    let addresses=[selected[&11],selected[&12]];
    let packs=Packs::open_box_addresses(bytes.clone(),addresses, |_|Ok(def.clone())).unwrap();
    let beds=Beds::open_box_addresses(bytes.clone(),addresses, |_|Ok(def.clone())).unwrap();
    let query=Query::new().order_by("sequence",Direction::Ascending).limit(10);
    let rows=packs.query(query.clone()).unwrap().fetch(10).unwrap();
    assert_eq!(numbers(&rows,"sequence"),[1100,1101,1102,1200,1201,1202]);
    assert_eq!(rows.objects().unwrap(),beds.query(query).unwrap().fetch(10).unwrap().objects().unwrap());
    let footer=pil_codec::r#box::Box::footer_range(&bytes,bytes.len() as u64).unwrap();
    let metadata=pil_codec::r#box::Box::open_footer(&bytes[footer.start as usize..],bytes.len() as u64).unwrap();
    for address in addresses {
        let start=address as usize;
        let range=metadata.range_at(address,&bytes[start..(start+27).min(footer.start as usize)]).unwrap();
        assert_eq!(container.get_at(address).unwrap(),&bytes[range.start as usize..range.end as usize]);
    }
    assert!(Packs::open_box_addresses(bytes,[footer.start], |_|Ok(def.clone())).is_err());
}

#[test]
fn storage_verifies_crc_before_write_not_when_opening_selected_packs() {
    let def = definition();
    let first = ingress_pack(&def, 0, 3, true);
    let second = ingress_pack(&def, 3, 3, true);
    let mut writer = Packs::create_box(&[]).unwrap();
    writer.pack(first.stored_bytes()).unwrap();
    let second_address = writer.next_address();
    writer.pack(second.stored_bytes()).unwrap();
    writer.commit().unwrap();
    let mut bytes = writer.bytes().unwrap().to_vec();
    integrity::before_write(first.stored_bytes()).unwrap();
    integrity::before_write(second.stored_bytes()).unwrap();
    // Changing only the checksum distinguishes pre-write verification from
    // query-time structural checks; no data value or offset is malformed.
    let offset = {
        let container = pil_codec::r#box::Box::open(&bytes).unwrap();
        let child = container.get_at(second_address).unwrap();
        let checksum = pil_codec::pac::application_header(child, integrity::HEADER)
            .unwrap().unwrap();
        checksum.as_ptr() as usize - bytes.as_ptr() as usize
    };
    bytes[offset] ^= 1;
    let bytes = Arc::new(bytes);
    let selected = Packs::open_box_addresses(bytes.clone(), [0], |_| Ok(def.clone())).unwrap();
    assert_eq!(numbers(&selected.query(Query::new()).unwrap().fetch(10).unwrap(), "sequence"), [0, 1, 2]);
    let container = pil_codec::r#box::Box::open(&bytes).unwrap();
    let child = container.get_at(second_address).unwrap();
    assert!(matches!(integrity::before_write(child), Err(e) if e.to_string().contains("crc32c mismatch")));
    let packs = Packs::open_box_addresses(bytes.clone(), [second_address], |_| Ok(def.clone())).unwrap();
    let beds = Beds::open_box_addresses(bytes.clone(), [second_address], |_| Ok(def.clone())).unwrap();
    assert_eq!(numbers(&packs.query(Query::new()).unwrap().fetch(10).unwrap(), "sequence"), [3, 4, 5]);
    assert_eq!(numbers(&beds.query(Query::new()).unwrap().fetch(10).unwrap(), "sequence"), [3, 4, 5]);
    def.open_stored(child.to_vec()).unwrap();
    def.open_stored_range_limited(bytes.clone(), second_address as usize..second_address as usize + child.len(), child.len()).unwrap();
    let missing = ingress(&def, 6, 3);
    let mut writer = Packs::create_box(&[]).unwrap();
    writer.pack(missing.stored_bytes()).unwrap();
    writer.commit().unwrap();
    assert!(matches!(integrity::before_write(missing.stored_bytes()), Err(e) if e.to_string().contains("requires crc32c")));
    Packs::open_box_addresses(Arc::new(writer.bytes().unwrap().to_vec()), [0], |_| Ok(def.clone())).unwrap();
}

// A file-backed adapter for the same cursor. Fixtures are emitted through Seed
// ingestion; no payloads are resident when the cursor is constructed.
struct DiskSource {
    file: std::fs::File,
    path: std::path::PathBuf,
    definition: Arc<SeedDefinition>,
    extents: Vec<(u64, usize)>,
    stats: Arc<std::sync::Mutex<DiskStats>>,
}
#[derive(Default)]
struct DiskStats {
    loads: Vec<usize>,
    owners: Vec<std::sync::Weak<SeedPackSnapshot>>,
    peak: usize,
}
impl DiskSource {
    fn ingest(count: usize, rows: usize) -> (Self, Arc<std::sync::Mutex<DiskStats>>) {
        use std::io::Write;
        static ID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!("seed-query-{}-{}", std::process::id(), ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)));
        let mut file = std::fs::OpenOptions::new().read(true).write(true).create_new(true).open(&path).unwrap();
        let definition = definition();
        let mut extents = Vec::new(); let mut at = 0;
        for i in 0..count {
            let pack = ingress(&definition, (i * rows) as u64, rows);
            let bytes = pack.stored_bytes();
            file.write_all(bytes).unwrap(); extents.push((at, bytes.len())); at += bytes.len() as u64;
        }
        file.flush().unwrap();
        let stats = Arc::new(std::sync::Mutex::new(DiskStats::default()));
        (Self { file, path, definition, extents, stats: stats.clone() }, stats)
    }
}
impl Drop for DiskSource { fn drop(&mut self) { let _ = std::fs::remove_file(&self.path); } }
impl PackSource for DiskSource {
    fn len(&self) -> usize { self.extents.len() }
    fn definitions(&self) -> Vec<Arc<SeedDefinition>> { vec![self.definition.clone()] }
    fn load(&mut self, ordinal: usize) -> Result<Arc<SeedPackSnapshot>, Error> {
        use std::io::{Read, Seek};
        let (at, length) = self.extents[ordinal];
        self.file.seek(std::io::SeekFrom::Start(at)).unwrap();
        let mut bytes = vec![0; length]; self.file.read_exact(&mut bytes).unwrap();
        let pack = self.definition.open(bytes)?;
        let mut stats = self.stats.lock().unwrap();
        stats.loads.push(ordinal); stats.owners.retain(|p| p.strong_count() > 0);
        stats.owners.push(Arc::downgrade(&pack)); stats.peak = stats.peak.max(stats.owners.len());
        Ok(pack)
    }
}

#[test]
fn disk_query_reads_only_demanded_packs_and_honors_one_global_limit() {
    let (source, stats) = DiskSource::ingest(1000, 7);
    let mut cursor = Cursor::from_source(source, Query::new().offset(6).limit(5).select(["sequence"])).unwrap();
    assert!(stats.lock().unwrap().loads.is_empty(), "planning must not load payloads");
    let mut got = Vec::new();
    loop {
        let slice = cursor.poll(3, 2).unwrap();
        got.extend(numbers(&slice.batch, "sequence"));
        if slice.done { break; }
    }
    assert_eq!(got, vec![6,7,8,9,10]);
    assert_eq!(stats.lock().unwrap().loads, vec![0,1]);
    assert_eq!(cursor.scanned, 11);
    assert!(stats.lock().unwrap().owners.iter().all(|p| p.strong_count() == 0));
}

#[test]
fn disk_query_scan_memory_is_independent_of_pack_count() {
    let (source, stats) = DiskSource::ingest(1000, 3);
    let query = Query::new().filter(Predicate::compare("sequence", Compare::Gt, 99999));
    let mut cursor = Cursor::from_source(source, query).unwrap();
    loop {
        let slice = cursor.poll(10, 19).unwrap(); assert!(slice.batch.is_empty());
        assert!(stats.lock().unwrap().owners.iter().filter(|p| p.strong_count()>0).count() <= 1);
        if slice.done { break; }
    }
    assert_eq!(stats.lock().unwrap().loads, (0..1000).collect::<Vec<_>>());
    assert_eq!(stats.lock().unwrap().peak, 1);
    assert_eq!(cursor.scanned, 3000);
}

#[test]
fn disk_query_pages_hold_only_contributing_packs_and_share_readers() {
    let (source, stats) = DiskSource::ingest(20, 3);
    let mut cursor = Cursor::from_source(source, Query::new()).unwrap();
    let first = cursor.poll(4, 100).unwrap(); // Crosses a Pack boundary.
    assert_eq!(numbers(&first.batch, "sequence"), vec![0,1,2,3]);
    assert_eq!(stats.lock().unwrap().loads, vec![0,1], "page construction must not reread earlier Packs");
    let second = cursor.poll(1, 100).unwrap();
    assert_eq!(numbers(&second.batch, "sequence"), vec![4]);
    assert_eq!(stats.lock().unwrap().loads, vec![0,1]);
    cursor.abort(); drop(cursor);
    assert_eq!(numbers(&first.batch, "sequence"), vec![0,1,2,3]);
    drop(first);
    assert_eq!(stats.lock().unwrap().owners.iter().filter(|p| p.strong_count()>0).count(),1);
    drop(second);
    assert!(stats.lock().unwrap().owners.iter().all(|p| p.strong_count()==0));
}

#[test]
fn disk_query_top_k_reopens_selected_packs_without_pinning_the_scan() {
    for direction in [Direction::Ascending, Direction::Descending] {
        let (source, stats) = DiskSource::ingest(100, 10);
        let def = source.definition.clone();
        let mut resident = Packs::new();
        for i in 0..100 { resident.pack(ingress(&def, i * 10, 10)); }
        let query = Query::new().select(["sequence"]).order_by("reading", direction).offset(4).limit(7);
        let expected = numbers(&resident.query(query.clone()).unwrap().fetch(7).unwrap(), "sequence");
        let mut cursor = Cursor::from_source(source, query).unwrap();
        let mut got = Vec::new();
        loop {
            let slice = cursor.poll(3, 17).unwrap();
            if cursor.scanned < 1000 { assert!(stats.lock().unwrap().peak <= 1, "sort must retain keys, not the dataset"); }
            got.extend(numbers(&slice.batch, "sequence"));
            if slice.done { break; }
        }
        assert_eq!(got, expected);
        assert_eq!(&stats.lock().unwrap().loads[..100], &(0..100).collect::<Vec<_>>());
        assert!(stats.lock().unwrap().loads.len() <= 107);
        assert!(stats.lock().unwrap().owners.iter().all(|p| p.strong_count()==0));
    }
}

#[test]
fn disk_query_zero_limit_and_early_cancellation_perform_no_io() {
    let (source, stats) = DiskSource::ingest(3, 5);
    let mut zero = Cursor::from_source(source, Query::new().limit(0)).unwrap();
    assert!(zero.poll(10, 10).unwrap().done); assert!(stats.lock().unwrap().loads.is_empty());
    let (source, stats) = DiskSource::ingest(3, 5);
    let mut cancelled = Cursor::from_source(source, Query::new()).unwrap();
    cancelled.cancel(); assert!(cancelled.poll(10,10).is_err()); assert!(stats.lock().unwrap().loads.is_empty());
}

#[test]
fn scan_checkpoint_uses_each_sources_annotated_clock_before_filtering() {
    use data_seeds::{doc, Field, SeedDefinition};
    let mut sources = data_seeds::Packs::new();
    for (name, clock, unit, times) in [
        ("first", "received", "@time(ms)", [101u64, 117, 202]),
        ("second", "measured", "@time(us)", [111_000, 123_999, 200_000]),
    ] {
        let document = doc::new(name)
            .field(Field::new(clock).r#type("uint").size(8).annotation(unit))
            .field(Field::new("status").r#type("uint").size(1))
            .sign().unwrap();
        let definition = SeedDefinition::prescribe(&document).unwrap();
        let mut pack = definition.fill(times.len()).unwrap();
        for time in times {
            pack.load(definition.ingest((), |_, row| {
                row.set_uint(clock, time)?.set_uint("status", 1u8)?;
                Ok(())
            }).unwrap()).unwrap();
        }
        pack.seal().unwrap();
        sources.pack(definition.open(pack.into_bytes().unwrap()).unwrap());
    }
    for direction in [data_seeds::Direction::Ascending, data_seeds::Direction::Descending] {
        let query = data_seeds::Query::new().checkpoint(100..200)
            .filter(data_seeds::Predicate::compare("status", data_seeds::Compare::Eq, 2u64))
            .order_by("status", direction).limit(1);
        let mut cursor = sources.query(query).unwrap();
        loop {
            let slice = cursor.poll(1, 2).unwrap();
            assert!(slice.batch.is_empty());
            if slice.done { break; }
        }
        assert_eq!(cursor.checkpoint_ms, Some(123));
    }
}
