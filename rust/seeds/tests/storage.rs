use data_seeds::*;
use pil_codec::{Value, api::NativeValue};
use std::sync::Arc;

struct Indexed {
    rows: Vec<(u64, u16)>,
    time: usize,
    leaf: usize,
    status: usize,
}
impl StorageBinding for Indexed {
    fn row_count(&self) -> usize {
        self.rows.len()
    }
    fn value(&self, row: usize, field: usize) -> Result<Option<NativeValue<'_>>, Error> {
        let (time, leaf) = *self.rows.get(row).ok_or(Error::Incomplete)?;
        Ok(Some(NativeValue::Scalar(if field == self.time {
            Value::U64(time)
        } else if field == self.leaf {
            Value::U16(leaf)
        } else if field == self.status {
            match leaf {
                1 => Value::String("ready"),
                2 => Value::Null,
                _ => Value::Absent,
            }
        } else {
            return Ok(None);
        })))
    }
}

#[test]
fn indexed_fields_filter_sort_and_render_without_a_stored_record_ordinal() {
    let document = doc::new("event")
        .field(
            Field::new("time")
                .r#type("uint")
                .size(8)
                .annotation("@time"),
        )
        .field(Field::new("id").r#type("bytes").size(10).annotation("@event"))
        .field(
            Field::new("leaf_id")
                .r#type("uint")
                .size(2)
                .annotation("@leaf"),
        )
        .field(Field::new("status").r#type("tstr"))
        .field(Field::new("message").r#type("tstr"))
        .sign()
        .unwrap();
    let definition = SeedDefinition::prescribe(&document).unwrap();
    assert!(definition.prescription().field_ordinal("@record").is_none());
    assert_eq!(definition.annotations(2).unwrap(), ["@leaf"]);
    let ids = [[1u8; 10], [2; 10], [3; 10]];
    let mut pack = definition.fill(3).unwrap();
    let mut rows = vec![];
    // Each source event enters through ingress. The storage adapter retains the
    // indexed time/leaf separately, just as the active index does today.
    for (index, id) in ids.iter().enumerate() {
        pack.load(
            definition
                .ingest((id, index), |(id, index), seed| {
                    rows.push((1000, (index + 1) as u16));
                    seed.set_byte_string("id", id)?
                        .set_text_string("message", "received")?;
                    Ok(())
                })
                .unwrap(),
        )
        .unwrap();
    }
    pack.seal().unwrap();
    let snapshot = definition.open(pack.into_bytes().unwrap()).unwrap();
    let raw_ptr = snapshot.stored_bytes().as_ptr();
    assert_eq!(
        snapshot
            .filter(Predicate::IsAbsent("time".into()))
            .unwrap()
            .fetch(4)
            .unwrap()
            .len(),
        3
    );
    let binding = Arc::new(Indexed {
        rows,
        time: 0,
        leaf: 2,
        status: 3,
    });
    let bound = snapshot.with_storage(binding.clone()).unwrap();
    assert_eq!(bound.stored_bytes().as_ptr(), raw_ptr);
    assert_eq!(bound.stored_bytes(), snapshot.stored_bytes());
    assert_eq!(
        bound
            .filter(Predicate::IsNull("status".into()))
            .unwrap()
            .fetch(4)
            .unwrap(),
        [1]
    );
    assert_eq!(
        bound
            .filter(Predicate::IsAbsent("status".into()))
            .unwrap()
            .fetch(4)
            .unwrap(),
        [2]
    );
    assert_eq!(
        bound
            .filter(Predicate::compare("status", Compare::Eq, "ready"))
            .unwrap()
            .fetch(4)
            .unwrap(),
        [0]
    );
    let mut packs = Packs::new();
    packs.pack(bound);
    let batch = packs
        .query(
            Query::new()
                .filter(Predicate::compare("time", Compare::Eq, 1000u64))
                .order_by("leaf_id", Direction::Descending)
                .limit(3),
        )
        .unwrap()
        .fetch(3)
        .unwrap();
    drop(packs);
    drop(snapshot);
    drop(binding);
    drop(definition);
    let objects = batch.objects().unwrap();
    let mut reader = batch.reader();
    for (i, object) in objects.iter().enumerate() {
        let view = reader.view(i).unwrap();
        assert_eq!(&view.object().unwrap(), object);
        assert_eq!(view.get("time").unwrap(), Cell::Value(1000u64.into()));
        assert_eq!(
            view.get("leaf_id").unwrap(),
            Cell::Value((3u64 - i as u64).into())
        );
        assert_eq!(
            view.get("id").unwrap(),
            Cell::Value(ciborium::Value::Bytes({
                let b = (3 - i) as u8;
                vec![0, 0, 0, 0, 3, 232, 0x70 | b, b, 0x80 | b, b, b, b, b, b, b, b]
            }))
        );
        assert!(view.is_present_at(0).unwrap());
        assert!(!object.iter().any(|(name, _)| name == "@record"));
    }
}

#[test]
fn index_count_must_match_pack() {
    let definition = SeedDefinition::prescribe(
        &doc::new("empty")
            .field(Field::new("time").r#type("uint").size(8))
            .sign()
            .unwrap(),
    )
    .unwrap();
    let mut pack = definition.fill(1).unwrap();
    pack.load(
        definition
            .ingest(123, |time, seed| {
                seed.set_uint("time", time)?;
                Ok(())
            })
            .unwrap(),
    )
    .unwrap();
    pack.seal().unwrap();
    let snapshot = definition.open(pack.into_bytes().unwrap()).unwrap();
    assert!(
        snapshot
            .with_storage(Arc::new(Indexed {
                rows: vec![],
                time: 0,
                leaf: 1,
                status: 2
            }))
            .is_err()
    );
}

#[test]
fn native_hour_index_restores_six_byte_time_without_copying_pack_or_index() {
    let document=doc::new("indexed")
        .field(Field::new("when").r#type("uint").size(6).annotation("@time"))
        .field(Field::new("state").r#type("uint").size(2).annotation("@leaf"))
        .field(Field::new("message").r#type("tstr"))
        .sign().unwrap();
    let definition=SeedDefinition::prescribe(&document).unwrap();
    let fields=IndexFields::from_definition(&definition).unwrap();
    let hour=((1u64<<32)/3_600_000)*3_600_000;
    let mut index=HourIndexBuilder::new(hour,3,fields).unwrap();
    let mut pack=definition.fill(3).unwrap();
    let times=[hour,(1<<32)-1,(1<<32)+1];
    for (i,time) in times.into_iter().enumerate() {
        pack.load(definition.ingest((time,i),|(time,i),seed| {
            index.append(time,i as u16)?;
            seed.set_text_string("message","indexed ingress")?;Ok(())
        }).unwrap()).unwrap();
    }
    pack.seal().unwrap();let pack=definition.open(pack.into_bytes().unwrap()).unwrap();
    let index=index.finish();assert_eq!(index.row_count(),3);
    assert_eq!(&index.bytes()[..6],&[0,0,0,0,0,0]);
    assert_eq!(&index.bytes()[12..18],&[255,255,255,255,1,0]);
    let mut padded=vec![99;17];padded.extend_from_slice(index.bytes());let end=padded.len();padded.extend_from_slice(&[99;7]);
    let backing=Arc::new(padded);let pointer=backing[17..].as_ptr();
    let index=HourIndex::open(backing.clone(),17..end,hour,fields).unwrap();assert_eq!(index.bytes().as_ptr(),pointer);
    for (i,time) in times.into_iter().enumerate() { assert_eq!(index.time_leaf(i).unwrap(),(time,i as u16)); }
    assert!(index.time_leaf(3).is_err());
    let bound=pack.with_storage(Arc::new(index)).unwrap();assert_eq!(bound.stored_bytes().as_ptr(),pack.stored_bytes().as_ptr());
    drop(pack);drop(backing);
    let mut packs=Packs::new();packs.pack(bound);
    let rows=packs.query(Query::new().filter(Predicate::compare("when",Compare::Ge,ciborium::Value::Integer(((1u64<<32)-1).into()))).order_by("when",Direction::Descending).limit(10)).unwrap().fetch(10).unwrap().objects().unwrap();
    assert_eq!(rows.len(),2);assert_eq!(rows[0][0].1,ciborium::Value::Integer(((1u64<<32)+1).into()));
    assert_eq!(rows[0][1].1,ciborium::Value::Integer(2.into()));
}

#[test]
fn hour_index_checks_boundaries_without_scanning_untouched_rows() {
    let document=doc::new("index").field(Field::new("time").r#type("uint").size(6).annotation("@time")).sign().unwrap();
    let definition=SeedDefinition::prescribe(&document).unwrap();let fields=IndexFields::from_definition(&definition).unwrap();
    let mut index=HourIndexBuilder::new(0,2,fields).unwrap();
    assert!(index.append(3_600_000,0).is_err());
    index.append(0,0).unwrap();index.append(1,0).unwrap();let index=index.finish();
    let mut bytes=index.bytes().to_vec();bytes[12..16].copy_from_slice(&4_000_000u32.to_le_bytes());
    let index=HourIndex::open(Arc::new(bytes),0..18,0,fields).unwrap();
    assert_eq!(index.time_leaf(0).unwrap().0,0);assert!(index.time_leaf(1).is_err());
    assert!(HourIndex::open(Arc::new(vec![0;7]),0..7,0,fields).is_err());
    assert!(HourIndex::open(Arc::new(vec![0;6]),0..6,1,fields).is_err());
    assert!(HourIndex::open(Arc::new(vec![0;6]),0..6,4_294_800_000+3_600_000,fields).is_err());
}

#[test]
fn leaf_fields_use_the_shared_codec_for_filtering_and_views() {
    use pil_codec::api::FieldBytes;
    let document = doc::new("state-bound")
        .field(Field::new("time").r#type("uint").size(6).annotation("@time"))
        .field(Field::new("leaf_id").r#type("uint").size(2).annotation("@leaf"))
        .field(Field::new("enabled").r#type("bool"))
        .field(Field::new("region").r#type("tstr"))
        .field(Field::new("counter").r#type("uint").size(16))
        .field(Field::new("labels").map("tstr", "uint").value_size(3))
        .field(Field::new("message").r#type("tstr"))
        .sign().unwrap();
    let definition = SeedDefinition::prescribe(&document).unwrap();
    let mut trie = LeafTrie::new(definition.clone());
    let fields = IndexFields::from_definition(&definition).unwrap();
    let mut index = HourIndexBuilder::new(0, 2, fields).unwrap();
    let mut pack = definition.fill(2).unwrap();
    for (row, region) in ["west", "east"].into_iter().enumerate() {
        let seed = definition.ingest((row, region), |(row, region), seed| {
            seed.set_bool("enabled", row == 0)?
                .set_text_string("region", region)?
                .set_uint("counter", (1u128 << 100) + row as u128)?
                .set_map("labels", vec![("code".into(), 70000u64.into())])?
                .set_text_string("message", "received")?;
            let mut leaf = 0;
            for field in 2..=5 {
                let FieldBytes::Bytes(raw) = seed.get(field)? else { panic!("missing ingress field") };
                leaf = trie.append(leaf, field as u16, raw.to_vec())?;
                seed.set(field, FieldBytes::Absent)?;
            }
            index.append(row as u64, leaf)?;
            Ok(())
        }).unwrap();
        pack.load(seed).unwrap();
    }
    trie.validate().unwrap();
    pack.seal().unwrap();
    let pack = definition.open(pack.into_bytes().unwrap()).unwrap();
    let bound = pack.with_storage(Arc::new(LeafIndex::new(index.finish(), Arc::new(trie)))).unwrap();
    assert_eq!(bound.stored_bytes().as_ptr(), pack.stored_bytes().as_ptr());
    let mut packs = Packs::new(); packs.pack(bound);
    let batch = packs.query(Query::new().filter(Predicate::compare("region", Compare::Eq, "west"))).unwrap().fetch(10).unwrap();
    let objects = batch.objects().unwrap();
    assert_eq!(objects.len(), 1);
    let mut reader = batch.reader(); let view = reader.view(0).unwrap();
    assert_eq!(view.get("enabled").unwrap(), Cell::Value(true.into()));
    assert_eq!(view.get("counter").unwrap(), Cell::Value(pil_codec::native_unsigned(1u128 << 100)));
    assert_eq!(view.get("labels").unwrap(), Cell::Value(ciborium::Value::Map(vec![("code".into(), 70000u64.into())])));
    assert_eq!(view.object().unwrap(), objects[0]);
}

#[test]
fn leaf_bindings_reject_shadowing_and_defer_unused_value_checks() {
    let definition = SeedDefinition::prescribe(&doc::new("leaf-checks")
        .field(Field::new("time").r#type("uint").size(6).annotation("@time"))
        .field(Field::new("region").r#type("tstr"))
        .sign().unwrap()).unwrap();
    let mut trie = LeafTrie::new(definition.clone());
    let leaf = trie.append(0, 1, vec![255]).unwrap();
    assert!(trie.append(leaf, 1, b"repeat".to_vec()).is_err());
    assert!(trie.append(9, 1, b"parent".to_vec()).is_err());
    assert!(trie.append(0, 2, vec![]).is_err());
    assert!(trie.validate().is_err());
    assert!(trie.value(leaf, 1).is_err());
    assert!(trie.value(0, 1).unwrap().is_none());
    assert!(trie.value(99, 1).is_err());
    let mut trie = LeafTrie::new(definition.clone());
    // This node comes from the typed ingress setter, as it does at sealing.
    let seed = definition.ingest("west", |v,s| { s.set_text_string("region",v)?; Ok(()) }).unwrap();
    let pil_codec::api::FieldBytes::Bytes(raw) = seed.get("region").unwrap() else { panic!() };
    let leaf = trie.append(0,1,raw.to_vec()).unwrap();
    let mut index = HourIndexBuilder::new(0,1,IndexFields::from_definition(&definition).unwrap()).unwrap();
    index.append(0,leaf).unwrap();
    let mut pack = definition.fill(1).unwrap(); pack.load(seed).unwrap(); pack.seal().unwrap();
    let pack = definition.open(pack.into_bytes().unwrap()).unwrap().with_storage(Arc::new(LeafIndex::new(index.finish(),Arc::new(trie)))).unwrap();
    assert_eq!(pack.filter(Predicate::compare("time",Compare::Eq,0u64)).unwrap().fetch(1).unwrap(),[0]);
    assert!(pack.filter(Predicate::compare("region",Compare::Eq,"west")).unwrap().fetch(1).is_err());
}

#[test]
fn leaf_replication_batches_preserve_ids_and_roll_back_invalid_appends() {
    let definition = SeedDefinition::prescribe(&doc::new("leaves")
        .field(Field::new("region").r#type("tstr"))
        .field(Field::new("zone").r#type("uint").size(3))
        .sign().unwrap()).unwrap();
    let mut source = LeafTrie::new(definition.clone());
    let seed = definition.ingest((), |_,s| { s.set_text_string("region","west")?.set_uint("zone",70000u32)?; Ok(()) }).unwrap();
    let pil_codec::api::FieldBytes::Bytes(region) = seed.get(0).unwrap() else { panic!() };
    let pil_codec::api::FieldBytes::Bytes(zone) = seed.get(1).unwrap() else { panic!() };
    let parent = source.append(0,0,region.to_vec()).unwrap();
    let mut replica = LeafTrie::new(definition.clone());
    replica.append_pil(0,&source.encode_pil(0).unwrap()).unwrap();
    let leaf = source.append(parent,1,zone.to_vec()).unwrap();
    replica.append_pil(2,&source.encode_pil(2).unwrap()).unwrap();
    assert_eq!(source.encode_pil(0).unwrap(),replica.encode_pil(0).unwrap());
    assert!(matches!(replica.value(leaf,1).unwrap(),Some(NativeValue::Scalar(Value::U24(70000)))));
    assert!(replica.append_pil(2,&source.encode_pil(2).unwrap()).is_err());
    let mut snapshot = replica.clone();
    let bad = ciborium::Value::Array(vec![
        ciborium::Value::Map(vec![("0".into(),0u64.into()),("1".into(),0u64.into()),("2".into(),ciborium::Value::Bytes(b"east".to_vec()))]),
        ciborium::Value::Null,
    ]);
    assert!(snapshot.append_pil(3,&pil_codec::object::encode_value(&bad).unwrap()).is_err());
    assert_eq!(snapshot.encode_pil(0).unwrap(),replica.encode_pil(0).unwrap());
}

#[test]
fn identical_shapes_cannot_bind_a_leaf_root_from_another_version() {
    let make = |minor| doc::new("versioned-state").version(0,minor)
        .field(Field::new("time").r#type("uint").size(6).annotation("@time"))
        .field(Field::new("leaf_id").r#type("uint").size(2).annotation("@leaf"))
        .field(Field::new("region").r#type("tstr")).sign().unwrap();
    let old=SeedDefinition::prescribe(&make(0)).unwrap();
    let new=SeedDefinition::prescribe(&make(1)).unwrap();
    let leaves=Arc::new(LeafTrie::new(old));
    let fields=IndexFields::from_definition(&new).unwrap();
    let mut index=HourIndexBuilder::new(0,1,fields).unwrap();index.append(1,0).unwrap();
    let mut pack=new.fill(1).unwrap();
    pack.load(new.ingest("west",|region,seed|{seed.set_text_string("region",region)?;Ok(())}).unwrap()).unwrap();
    pack.seal().unwrap();
    let pack=new.open(pack.into_bytes().unwrap()).unwrap();
    assert!(pack.with_storage(Arc::new(LeafIndex::new(index.finish(),leaves))).is_err());
}
