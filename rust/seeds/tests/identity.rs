use data_seeds::{doc, Cell, Field, Packs, Query, SeedDefinition};
use ciborium::Value as Data;

const UUID: [u8;16] = [0x01,0x99,0x23,0x45,0x67,0x89,0x7a,0xbc,0x81,0x23,0x45,0x67,0x89,0xab,0xcd,0xef];
const MS: u64 = 0x019923456789;

#[test]
fn ingress_keeps_compact_bytes_and_views_restore_uuid_for_each_time_unit() {
    for (unit, multiplier, fraction) in [("@time",1,0), ("@time(ms)",1,0), ("@time(us)",1000,500), ("@time(ns)",1_000_000,500_000)] {
        // Reordered, nonstandard names ensure the plan follows annotations.
        let doc = doc::new("identity")
            .field(Field::new("key").r#type("bytes").size(10).annotation("@event"))
            .field(Field::new("at").r#type("uint").size(8).annotation(unit))
            .field(Field::new("opaque").r#type("bytes").size(10))
            .sign().unwrap();
        let definition = SeedDefinition::prescribe(&doc).unwrap();
        let time = MS * multiplier + fraction;
        let mut expected = UUID;
        if multiplier > 1 { expected[6] = 0x78; expected[7] = 0; }
        let mut pack = definition.fill(3).unwrap();
        for state in 0..3 {
            let seed = definition.ingest(state, |state, s| {
                s.set_uint("at", time)?.set_byte_string("opaque", &UUID[6..])?;
                if state == 0 { s.set_byte_string("key", &UUID[6..])?; }
                if state == 1 { s.set_null("key")?; }
                Ok(())
            }).unwrap();
            seed.cap().unwrap();
            let mut raw = definition.prescription().decoder();
            {
                let bytes = seed.bytes().unwrap();
                let view = raw.read(&bytes).unwrap();
                if state == 0 {
                    let pil_codec::api::NativeValue::Scalar(pil_codec::Value::Bytes(id)) = view.native_value(0).unwrap() else { panic!("raw ID must remain bytes") };
                    assert_eq!(id, &UUID[6..]);
                }
            }
            pack.load(seed).unwrap();
        }
        pack.seal().unwrap();
        let bytes = pack.into_bytes().unwrap();
        if let Some(directory) = std::env::var_os("TRUNK_IDENTITY_FIXTURE") {
            let directory = std::path::Path::new(&directory);
            std::fs::create_dir_all(directory).unwrap();
            let name = match unit { "@time(us)" => "us", "@time(ns)" => "ns", _ => "ms" };
            std::fs::write(directory.join(format!("{name}.rx")), doc.bytes().unwrap()).unwrap();
            std::fs::write(directory.join(format!("{name}.pac")), &bytes).unwrap();
        }
        let owner = std::sync::Arc::new(bytes.clone());
        let snapshot = definition.open_range(owner.clone(), 0..bytes.len()).unwrap();
        let mut packs = Packs::new(); packs.pack(snapshot);
        let uuid = if multiplier == 1 { "01992345-6789-7abc-8123-456789abcdef" }
            else { "01992345-6789-7800-8123-456789abcdef" };
        for (literal, count) in [(uuid, 1), ("01992345-6789-7abc-8123-456789abcdee", 0)] {
            let q = Query::new().filter(data_seeds::Predicate::compare("key", data_seeds::Compare::Eq, literal));
            assert_eq!(packs.query(q).unwrap().fetch(3).unwrap().len(), count);
        }
        let batch = packs.query(Query::new().select(["key", "opaque"])).unwrap().fetch(3).unwrap();
        let objects = batch.objects().unwrap();
        let mut reader = batch.reader();
        let view = reader.view(0).unwrap();
        assert_eq!(view.get("key").unwrap(), Cell::Value(Data::Bytes(expected.to_vec())));
        assert_eq!(view.object().unwrap(), objects[0]);
        assert_eq!(view.get("opaque").unwrap(), Cell::Value(Data::Bytes(UUID[6..].to_vec())));
        assert_eq!(reader.view(1).unwrap().get("key").unwrap(), Cell::Value(Data::Null));
        assert_eq!(reader.view(2).unwrap().get("key").unwrap(), Cell::Absent);
        // Readers have not changed any persisted bytes.
        assert_eq!(owner.as_slice(), bytes.as_slice());
    }
}

#[test]
fn object_identity_compares_all_sixteen_bytes_without_a_time_field() {
    let document = doc::new("account")
        .field(Field::new("account_key").r#type("bytes").size(16).annotation("@object"))
        .sign().unwrap();
    let definition = SeedDefinition::prescribe(&document).unwrap();
    let mut pack = definition.fill(1).unwrap();
    pack.load(definition.ingest((), |_, row| {
        row.set_byte_string("account_key", &[0; 16])?;
        Ok(())
    }).unwrap()).unwrap();
    pack.seal().unwrap();
    let mut packs = Packs::new();
    packs.pack(definition.open(pack.into_bytes().unwrap()).unwrap());
    let q = Query::new().filter(data_seeds::Predicate::compare("account_key", data_seeds::Compare::Eq,
        "00000000-0000-0000-0000-000000000000"));
    assert_eq!(packs.query(q).unwrap().fetch(2).unwrap().len(), 1);
}

#[test]
fn missing_or_out_of_range_time_never_fabricates_an_identity() {
    let doc = doc::new("identity")
        .field(Field::new("id").r#type("bytes").size(10).annotation("@event"))
        .field(Field::new("time").r#type("uint").size(8).annotation("@time"))
        .sign().unwrap();
    let definition = SeedDefinition::prescribe(&doc).unwrap();
    let mut pack = definition.fill(2).unwrap();
    for time in [None, Some(1u64 << 48)] {
        pack.load(definition.ingest(time, |time,s| {
            s.set_byte_string("id", &UUID[6..])?;
            if let Some(t) = time { s.set_uint("time",t)?; }
            Ok(())
        }).unwrap()).unwrap();
    }
    pack.seal().unwrap();
    let mut packs = Packs::new(); packs.pack(definition.open(pack.into_bytes().unwrap()).unwrap());
    let batch = packs.query(Query::new()).unwrap().fetch(2).unwrap();
    let mut reader = batch.reader();
    assert!(reader.view(0).unwrap().get("id").is_err());
    assert!(reader.view(1).unwrap().get("id").is_err());
}
