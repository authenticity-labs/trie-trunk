//! Publication conformance probes using complete Packs from Rust ingestion.
//! These exercise Box replacement tails, not a production storage adapter.
use data_seeds::{doc, Field, SeedDefinition};
use pil_codec::r#box::{Box as Container, BoxOptions};

fn packs() -> (Vec<u8>, Vec<u8>) {
    let document = doc::new("publication")
        .field(Field::new("sequence").r#type("uint").size(6))
        .field(Field::new("message").r#type("tstr"))
        .sign().unwrap();
    let definition = SeedDefinition::prescribe(&document).unwrap();
    let mut batches = (0..2).map(|batch| {
        let mut pack = definition.fill(8).unwrap();
        for sequence in batch * 8..(batch + 1) * 8 {
            let message = format!("event {sequence}");
            let seed = definition.ingest((sequence, message.as_str()), |(sequence, message), seed| {
                seed.set_uint("sequence", sequence)?.set_text_string("message", message)?;
                Ok(())
            }).unwrap();
            pack.load(seed).unwrap();
        }
        pack.seal().unwrap();
        pack.into_bytes().unwrap()
    });
    (batches.next().unwrap(), batches.next().unwrap())
}

#[test]
fn replacement_tail_preserves_published_children_through_every_short_write() {
    let (first, second) = packs();
    let mut container = Container::new(&[], BoxOptions {
        external_addresses: true, ..Default::default()
    }).unwrap();
    container.pack(first.as_slice()).unwrap();
    container.commit().unwrap();
    let original = container.bytes().unwrap().to_vec();
    let footer_range = Container::footer_range(&original, original.len() as u64).unwrap();
    let footer = original[footer_range.start as usize..].to_vec();
    let old_snapshot = Container::open_footer(&footer, original.len() as u64).unwrap();
    let mut next = Container::open_footer_owned(footer.clone(), original.len() as u64).unwrap();
    next.pack(second.as_slice()).unwrap();
    let commit = next.commit().unwrap();
    assert_eq!(commit.offset as usize, first.len());
    let offset = commit.offset as usize;

    // A short write may destroy the physical old footer. Published readers
    // must retain its snapshot and length; old child bytes remain untouched.
    for written in 0..=commit.bytes.len() {
        let mut disk = original.clone();
        disk.resize(disk.len().max(offset + written), 0);
        disk[offset..offset + written].copy_from_slice(&commit.bytes[..written]);
        let old_range = old_snapshot.range_at(0, &disk[..first.len()]).unwrap();
        assert_eq!(&disk[old_range.start as usize..old_range.end as usize], first);
        assert!(old_snapshot.range_at(offset as u64, &second).is_err());

        // Retrying the same bounded replacement tail is idempotent. Publication
        // may advance only after every byte, the final length, and the barrier.
        disk.resize(commit.length as usize, 0);
        disk[offset..].copy_from_slice(commit.bytes);
        let opened = Container::open(&disk).unwrap();
        assert_eq!(opened.len(), 2);
        assert_eq!(opened.get_at(0).unwrap(), first);
        assert_eq!(opened.get_at(offset as u64).unwrap(), second);
        opened.validate().unwrap();
    }
}

#[test]
fn time_addressed_read_does_not_require_other_children_to_be_resident() {
    let (first, second) = packs();
    let mut container = Container::new(&[], BoxOptions {
        external_addresses: true, ..Default::default()
    }).unwrap();
    container.pack(first.as_slice()).unwrap();
    container.pack(second.as_slice()).unwrap();
    container.commit().unwrap();
    let bytes = container.bytes().unwrap();
    let footer_range = Container::footer_range(bytes, bytes.len() as u64).unwrap();
    let reader = Container::open_footer(&bytes[footer_range.start as usize..], bytes.len() as u64).unwrap();
    // Only the selected child's header and the footer are needed to locate it.
    // This is a range-reader test; it does not claim sparse writes are supported.
    let header = &second[..second.len().min(27)];
    assert_eq!(reader.range_at(first.len() as u64, header).unwrap(),
        first.len() as u64..(first.len() + second.len()) as u64);
}

#[test]
fn sparse_replica_fills_origin_addresses_without_republishing_footer() {
    use std::io::{Read, Seek, SeekFrom, Write};
    let (first, second) = packs();
    let mut origin = Container::new(&[], BoxOptions {
        external_addresses: true, ..Default::default()
    }).unwrap();
    origin.pack(first.as_slice()).unwrap();
    let second_address = origin.next_address();
    origin.pack(second.as_slice()).unwrap();
    origin.commit().unwrap();
    let published = origin.bytes().unwrap();
    let extent = published.len() as u64;
    let footer_range = Container::footer_range(published, extent).unwrap();
    let footer = &published[footer_range.start as usize..];
    let reader = Container::open_footer(footer, extent).unwrap();

    // The replica mirrors origin coordinates. Extending the logical file is
    // not evidence that its missing Packs have arrived, nor a promise about
    // physical sparse allocation on any particular filesystem or OPFS engine.
    let path = std::env::temp_dir().join(format!("trunk-sparse-publication-{}-{}.beds",
        std::process::id(), std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) { let _ = std::fs::remove_file(&self.0); }
    }
    let _cleanup = Cleanup(path.clone());
    let mut file = std::fs::OpenOptions::new().create_new(true)
        .read(true).write(true).open(path).unwrap();
    let mut storage = data_seeds::writer::NativeFiles(std::collections::HashMap::from([
        (1, file.try_clone().unwrap())
    ]));
    file.set_len(extent).unwrap();
    file.seek(SeekFrom::Start(footer_range.start)).unwrap();
    file.write_all(footer).unwrap();

    // Fetch the later Pack first. No preceding Pack or reconstructed local
    // Box directory is needed. The selection is supplied by the time index.
    let selected = reader.range_at(second_address, &second[..second.len().min(27)]).unwrap();
    assert_eq!(selected.start, second_address);
    let budget = data_seeds::MemoryBudget::new(first.capacity() + second.capacity());
    let buffer = data_seeds::writer::Buffer::admit(std::sync::Arc::new(second.clone()), &budget).unwrap();
    let mut writes = data_seeds::writer::WriteBatch::new(2);
    writes.write(1, selected.start, buffer, 0..second.len()).unwrap();
    let mut cursor = writes.start().unwrap();
    while !cursor.poll(&mut storage, 2, 17).unwrap().ready {}
    let mut read = vec![0; second.len()];
    file.seek(SeekFrom::Start(second_address)).unwrap();
    file.read_exact(&mut read).unwrap();
    assert_eq!(read, second);

    // Fill the earlier hole in place. This does not call Box::pack/commit,
    // change the child count, move the later Pack, or rewrite the footer.
    let buffer = data_seeds::writer::Buffer::admit(std::sync::Arc::new(first.clone()), &budget).unwrap();
    let mut writes = data_seeds::writer::WriteBatch::new(2);
    writes.write(1, 0, buffer, 0..first.len()).unwrap();
    let mut cursor = writes.start().unwrap();
    while !cursor.poll(&mut storage, 2, 17).unwrap().ready {}
    assert_eq!(budget.used(), 0);
    assert_eq!(file.metadata().unwrap().len(), extent);
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut complete = Vec::new();
    file.read_to_end(&mut complete).unwrap();
    assert_eq!(complete, published);
    Container::open(&complete).unwrap().validate().unwrap();
}
