//! Native local storage: ingress and publication use the existing Seed APIs.
//! This is an I/O integration test, not a complete desktop node or benchmark.
#![cfg(all(feature = "registration", any(unix, windows)))]
use data_seeds::reader::{Selection, Storage as ReadStorage};
use data_seeds::registration::Trunk;
use data_seeds::writer::{Buffer, NativeFiles, WriteBatch};
use data_seeds::{Field, MemoryBudget, Query, doc};
use pil_codec::r#box::Box as Container;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom},
    sync::Arc,
};
use trie_trunk_taxonomy::{Trunk as Taxonomy, registration::Registration};

#[test]
fn native_ingress_appends_plain_packs_to_one_beds_and_reopens_them() {
    let directory = std::env::temp_dir().join(format!("trunk-native-beds-{}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(directory.clone());
    let path = directory.join("sample.beds");
    let document = doc::new("sample")
        .field(Field::new("sequence").r#type("uint").size(4))
        .field(Field::new("message").r#type("tstr"))
        .sign()
        .unwrap();
    let (taxonomy, registered) = Taxonomy::new("Local")
        .unwrap()
        .registration(
            Registration {
                cache: "test",
                service: "lab",
                resource: "native",
                resource_type: "sample",
                level: "",
                file: "sample.rx",
            },
            &document,
        )
        .unwrap();
    let mut trunk = Trunk::open(taxonomy.bytes()).unwrap();
    let definition = trunk
        .prescribe(registered.definition, document.bytes().unwrap())
        .unwrap();
    let scope = trunk
        .trie()
        .branches()
        .select([registered.branch])
        .beds()
        .unwrap();
    let budget = MemoryBudget::new(1 << 20);
    let mut files = NativeFiles(HashMap::from([(
        1,
        OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap(),
    )]));
    let mut writer = scope.create(&[]).unwrap();
    let mut addresses = Vec::new();
    for group in 0..2 {
        let mut pack = scope.fill(registered.branch, 8).unwrap();
        pack.header("crc32c", vec![0; 4]).unwrap();
        for sequence in group * 8..(group + 1) * 8 {
            let text = format!("event {sequence}");
            pack.load(
                definition
                    .ingest((sequence, text.as_str()), |(sequence, text), seed| {
                        seed.set_uint("sequence", sequence)?
                            .set_text_string("message", text)?;
                        Ok(())
                    })
                    .unwrap(),
            )
            .unwrap();
        }
        pack.seal().unwrap();
        let mut bytes = pack.into_bytes().unwrap();
        data_seeds::integrity::stamp(&mut bytes).unwrap();
        data_seeds::integrity::before_write(&bytes).unwrap();
        let address = writer.next_address();
        assert_eq!(
            address,
            writer.next_beds_address(),
            "plain storage has one physical member address"
        );
        addresses.push(address);
        writer.pack(bytes).unwrap();
        let commit = writer.commit().unwrap();
        let mut batch = WriteBatch::new(8);
        let buffer = Buffer::copy(commit.bytes, &budget).unwrap();
        batch
            .write(1, commit.offset, buffer, 0..commit.bytes.len())
            .unwrap();
        batch.resize(1, commit.length).unwrap();
        let mut write = batch.start().unwrap();
        while !write.poll(&mut files, 2, 17).unwrap().ready {}
        assert_eq!(budget.used(), 0);
        let stored = std::fs::read(&path).unwrap();
        let boxed = Container::open(&stored).unwrap();
        assert_eq!(boxed.len(), group as usize + 1);
        for &at in &addresses {
            let raw = boxed.get_at(at).unwrap();
            let plain = pil_codec::pac_envelope::Plain::read_prefix(raw).unwrap();
            assert_eq!(
                plain.size,
                raw.len() as u64,
                "stored members are plain PACs"
            );
        }
        let footer = Container::footer_range(&stored, stored.len() as u64).unwrap();
        writer = scope
            .resume(
                stored[footer.start as usize..].to_vec(),
                stored.len() as u64,
            )
            .unwrap();
    }
    drop(writer);
    drop(files);
    let mut file = File::open(&path).unwrap();
    file.seek(SeekFrom::Start(7)).unwrap();
    let length = file.metadata().unwrap().len();
    let storage = Arc::new(NativeFiles(HashMap::from([(1, file)])));
    let reader = scope
        .reader(
            storage.clone(),
            Selection {
                file: 1,
                length,
                addresses,
            },
            1 << 20,
            budget.clone(),
        )
        .unwrap();
    let mut cursor = reader.query(Query::new().offset(7).limit(3)).unwrap();
    let batch = cursor.fetch(3).unwrap();
    let objects = batch.objects().unwrap();
    for (row, sequence) in objects.iter().zip(7u64..10) {
        assert_eq!(
            row.iter().find(|(name, _)| name == "sequence").unwrap().1,
            ciborium::Value::Integer(sequence.into())
        );
        assert_eq!(
            row.iter().find(|(name, _)| name == "message").unwrap().1,
            ciborium::Value::Text(format!("event {sequence}"))
        );
    }
    assert_eq!(objects.len(), 3);
    let mut shared = storage.0[&1].try_clone().unwrap();
    assert_eq!(
        shared.stream_position().unwrap(),
        7,
        "positional reads preserve the shared seek cursor"
    );
    assert_eq!(
        ReadStorage::read_at(storage.as_ref(), 99, 0, &mut [0; 1])
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::NotFound
    );
    drop(batch);
    drop(cursor);
    drop(reader);
    assert_eq!(budget.used(), 0);
    assert_eq!(
        std::fs::read_dir(&directory).unwrap().count(),
        1,
        "no compressed datastore or replica duplicate"
    );
}
