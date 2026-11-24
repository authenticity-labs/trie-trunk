#![cfg(feature = "registration")]
use ciborium::Value;
use data_seeds::{
    Compare, Direction, Field, MemoryBudget, Predicate, Query, doc,
    reader::{Selection, Storage},
    registration::Trunk,
};
use std::{
    io,
    sync::{Arc, Mutex},
};
use trie_trunk_taxonomy::{Trunk as Taxonomy, registration::Registration};
struct File {
    bytes: Vec<u8>,
    reads: Mutex<Vec<(u64, usize)>>,
}
impl Storage for File {
    fn read_at(&self, _: u64, offset: u64, target: &mut [u8]) -> io::Result<usize> {
        self.reads.lock().unwrap().push((offset, target.len()));
        let bytes = self.bytes.get(offset as usize..).unwrap_or(&[]);
        let n = bytes.len().min(target.len());
        target[..n].copy_from_slice(&bytes[..n]);
        Ok(n)
    }
}
fn numbers(batch: &data_seeds::Batch) -> Vec<u64> {
    batch
        .objects()
        .unwrap()
        .iter()
        .map(|row| {
            let Value::Integer(n) = row.iter().find(|(k, _)| k == "sequence").unwrap().1 else {
                panic!()
            };
            n.try_into().unwrap()
        })
        .collect()
}
#[test]
fn registered_live_resident_and_paged_queries_resolve_reordered_fields_and_versions() {
    let old = doc::new("sample")
        .field(Field::new("sequence").r#type("uint").size(3))
        .sign()
        .unwrap();
    let new = doc::new("sample")
        .version(0, 1)
        .field(Field::new("message").r#type("tstr"))
        .field(Field::new("sequence").r#type("uint").size(8))
        .compound(old.bytes().unwrap())
        .sign()
        .unwrap();
    let target = || Registration {
        cache: "test",
        service: "lab",
        resource: "data",
        resource_type: "sample",
        level: "",
        file: "sample.rx",
    };
    let (taxonomy, a) = Taxonomy::new("Mixed")
        .unwrap()
        .registration(target(), &old)
        .unwrap();
    let (taxonomy, b) = taxonomy.registration(target(), &new).unwrap();
    let mut trunk = Trunk::open(taxonomy.bytes()).unwrap();
    trunk
        .prescribe_file("sample.rx", new.bytes().unwrap())
        .unwrap();
    let old = trunk.definition(a.definition).unwrap();
    let new = trunk.definition(b.definition).unwrap();
    let mut scope = trunk.trie().branches().select([a.branch]).packs().unwrap();
    let bytes = {
        let mut pack = scope.fill(a.branch, 10).unwrap();
        for n in 0..10u64 {
            let definition = if n % 2 == 0 { &old } else { &new };
            pack.load(
                definition
                    .ingest(n, |n, s| {
                        s.set_uint("sequence", n)?;
                        if n % 2 != 0 {
                            s.set_text_string("message", "new")?;
                        }
                        Ok(())
                    })
                    .unwrap(),
            )
            .unwrap();
        }
        let snapshot = pack.snapshot().unwrap();
        let live = scope
            .open_pack(
                Arc::new(snapshot.clone()),
                0..snapshot.len(),
                snapshot.len(),
            )
            .unwrap();
        assert!(!live.is_sealed());
        let mut source = data_seeds::Packs::new();
        source.pack(live);
        assert_eq!(
            numbers(
                &source
                    .query(Query::new().version(0, 1))
                    .unwrap()
                    .fetch(20)
                    .unwrap()
            ),
            [1, 3, 5, 7, 9]
        );
        pack.seal().unwrap();
        pack.into_bytes().unwrap()
    };
    let owner = Arc::new(bytes.clone());
    let opened = scope
        .open_pack(owner.clone(), 0..bytes.len(), bytes.len())
        .unwrap();
    assert_eq!(opened.stored_bytes().as_ptr(), owner.as_ptr());
    let mut filter = opened
        .filter(Predicate::compare("sequence", Compare::Ge, 7))
        .unwrap();
    assert_eq!(filter.fetch(10).unwrap(), [7, 8, 9]);
    scope.pack(a.branch, opened).unwrap();
    let query = Query::new()
        .filter(Predicate::compare("sequence", Compare::Ge, 3))
        .select(["sequence", "message"])
        .order_by("sequence", Direction::Descending)
        .limit(4);
    let batch = scope.query(query.clone()).unwrap().fetch(10).unwrap();
    assert_eq!(numbers(&batch), [9, 8, 7, 6]);
    assert_ne!(batch.origin(0).unwrap().2, batch.origin(1).unwrap().2);
    let file = Arc::new(File {
        bytes,
        reads: Mutex::new(vec![]),
    });
    let reader = scope
        .reader(
            file.clone(),
            Selection {
                file: 0,
                length: file.bytes.len() as u64,
                addresses: vec![0],
            },
            file.bytes.len(),
            MemoryBudget::new(1 << 20),
        )
        .unwrap();
    reader.prepare(0, 1).unwrap();
    let before = file.reads.lock().unwrap().len();
    reader.prepare(0, 1).unwrap();
    assert_eq!(file.reads.lock().unwrap().len(), before);
    let mut cursor = data_seeds::Cursor::from_source(reader.clone(), query).unwrap();
    let mut found = Vec::new();
    loop {
        let slice = cursor.poll(2, 2).unwrap();
        found.extend(numbers(&slice.batch));
        if slice.done {
            break;
        }
    }
    assert_eq!(found, [9, 8, 7, 6]);
    let mut cursor =
        data_seeds::Cursor::from_source(reader, Query::new().version(0, 0).select(["sequence"]))
            .unwrap();
    assert_eq!(numbers(&cursor.fetch(20).unwrap()), [0, 2, 4, 6, 8]);
}
