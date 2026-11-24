use data_seeds::replication::{PackStream, ReceivedPack};
use data_seeds::writer::{FileId, Storage};
use data_seeds::{Beds, Field, MemoryBudget, SeedDefinition, doc, integrity};
use std::{io, sync::Arc};

fn ingress(sealed: bool) -> Vec<Vec<u8>> {
    let document = doc::new("replica")
        .field(Field::new("sequence").r#type("uint").size(6))
        .field(Field::new("message").r#type("tstr"))
        .sign()
        .unwrap();
    let definition = SeedDefinition::prescribe(&document).unwrap();
    (0..2)
        .map(|batch| {
            let mut pack = definition.fill(3).unwrap();
            pack.header(integrity::HEADER, vec![0; 4]).unwrap();
            for sequence in batch * 3..batch * 3 + 3 {
                let text = format!("received {sequence}");
                pack.load(
                    definition
                        .ingest(sequence, |sequence, seed| {
                            seed.set_uint("sequence", sequence)?
                                .set_text_string("message", &text)?;
                            Ok(())
                        })
                        .unwrap(),
                )
                .unwrap();
            }
            let mut bytes = if sealed {
                pack.seal().unwrap();
                pack.into_bytes().unwrap()
            } else {
                pack.snapshot().unwrap()
            };
            integrity::stamp(&mut bytes).unwrap();
            bytes
        })
        .collect()
}
fn receive(bytes: Arc<Vec<u8>>) -> Vec<ReceivedPack> {
    let mut stream = PackStream::new(bytes.len());
    let mut received = Vec::new();
    stream
        .load(bytes, |pack| {
            received.push(pack);
            Ok(())
        })
        .unwrap();
    stream.finish().unwrap();
    received
}
#[derive(Default)]
struct File {
    bytes: Vec<u8>,
    writes: Vec<(u64, usize)>,
    flushes: usize,
    pointers: Vec<usize>,
}
impl Storage for File {
    fn write_at(&mut self, _: FileId, at: u64, bytes: &[u8]) -> io::Result<usize> {
        self.pointers.push(bytes.as_ptr() as usize);
        let at = at as usize;
        self.bytes.resize(self.bytes.len().max(at + bytes.len()), 0);
        self.bytes[at..at + bytes.len()].copy_from_slice(bytes);
        self.writes.push((at as u64, bytes.len()));
        Ok(bytes.len())
    }
    fn resize(&mut self, _: FileId, _: u64) -> io::Result<()> {
        panic!("gap fills must not resize")
    }
    fn flush(&mut self, _: FileId) -> io::Result<()> {
        self.flushes += 1;
        Ok(())
    }
}

#[test]
fn received_pack_handles_write_original_allocation_and_share_admission() {
    let packs = ingress(true);
    let owner = Arc::new(packs.concat());
    let base = owner.as_ptr() as usize;
    let capacity = owner.capacity();
    let definition = SeedDefinition::prescribe(&doc::new("replica")
        .field(Field::new("sequence").r#type("uint").size(6))
        .field(Field::new("message").r#type("tstr"))
        .sign().unwrap()).unwrap();
    let mut handles = receive(owner.clone()).into_iter().map(|pack|
        pack.open_stored_for_write(&definition, capacity).unwrap()).collect::<Vec<_>>();
    let (length, _) = pil_codec::r#box::Box::external_footer_from_payload(owner.len() as u64, 2, &[], false).unwrap();
    let replica = Beds::reconstruct(length, 2).unwrap();
    let budget = MemoryBudget::new(capacity);
    let mut batch = replica.batch(1, &budget, 8);
    batch.pack(packs[0].len() as u64, handles.pop().unwrap()).unwrap();
    batch.pack(0, handles.pop().unwrap()).unwrap();
    assert_eq!(budget.used(), capacity, "shared network allocation charged once");
    drop(owner);
    let mut file = File::default();
    run(batch.start().unwrap(), &mut file);
    assert_eq!(file.bytes, packs.concat());
    assert!(file.pointers.iter().all(|p| *p >= base && *p < base + capacity));
    assert_eq!(budget.used(), 0);
}
fn run(mut writes: data_seeds::writer::WriteCursor, file: &mut File) {
    while !writes.poll(file, 3, 19).unwrap().ready {}
}

// Only Time metadata is constructed here. Every record above goes through
// ingestion and the real builder/encoder. The intervening hour is empty.
fn collection_time(second_address: usize) -> Arc<data_seeds::TimeTrie> {
    let first = 1_767_225_600_000u64; // 2026-01-01 UTC
    let hour = data_seeds::TIME_HOUR_MS;
    let mut bytes = vec![0; 64 + 366 * 24 * 16];
    bytes[..8].copy_from_slice(b"TTTIME00");
    for (at, value) in [(8, first), (16, first), (24, 8), (32, first + 3 * hour)] {
        bytes[at..at + 8].copy_from_slice(&value.to_le_bytes());
    }
    bytes[40..46].copy_from_slice(&[4, 5, 5, 2, 2, 2]);
    bytes[48..64].fill(7);
    for (index, start, address, count) in [(0, 0u64, 0u64, 3u64), (1, 4, 0, 0), (2, 4, second_address as u64, 3)] {
        let mut at = 64 + index * 16;
        for (value, width) in [(start, 4), (address, 5), (address, 5), (count, 2)] {
            bytes[at..at + width].copy_from_slice(&value.to_le_bytes()[..width]);
            at += width;
        }
    }
    Arc::new(data_seeds::TimeTrie::open(Arc::new(bytes)).unwrap())
}
fn collection_window(time: &data_seeds::TimeTrie) -> data_seeds::replication::Window {
    data_seeds::replication::Window { generation: time.header().generation,
        retained: time.header().first, sealed: time.header().sealed }
}
fn opened(packs: &[Vec<u8>]) -> Vec<Arc<data_seeds::SeedPackSnapshot>> {
    let definition = SeedDefinition::prescribe(&doc::new("replica")
        .field(Field::new("sequence").r#type("uint").size(6))
        .field(Field::new("message").r#type("tstr"))
        .sign().unwrap()).unwrap();
    receive(Arc::new(packs.concat())).into_iter()
        .map(|pack| pack.open_stored_for_write(&definition, 64 * 1024).unwrap()).collect()
}

#[test]
fn collection_uses_time_addresses_and_retries_admission_without_advancing() {
    let packs = ingress(true);
    let handles = opened(&packs);
    let time = collection_time(packs[0].len());
    let first = time.header().first;
    let end = time.header().sealed;
    let (length, _) = pil_codec::r#box::Box::external_footer_from_payload(packs.concat().len() as u64, 2, &[], false).unwrap();
    let replica = Beds::reconstruct(length, 2).unwrap();
    let mut collection = replica.collect(time.clone(), vec![first..end], collection_window(&time)).unwrap();
    assert!(collection.finish().is_err(), "missing selected Packs must not complete");
    let tiny = MemoryBudget::new(1);
    assert!(collection.load(&mut replica.batch(1, &tiny, 8), handles[0].clone()).is_err());
    let budget = MemoryBudget::new(64 * 1024);
    let mut batch = replica.batch(1, &budget, 8);
    assert_eq!(collection.load(&mut batch, handles[0].clone()).unwrap().beds, 0);
    assert!(collection.finish().is_err());
    assert_eq!(collection.load(&mut batch, handles[1].clone()).unwrap().beds, packs[0].len() as u64);
    assert_eq!(collection.finish().unwrap(), 2);
    assert!(collection.load(&mut batch, handles[0].clone()).is_err());
    let mut file = File::default();
    run(batch.start().unwrap(), &mut file);
    assert_eq!(file.bytes, packs.concat());
    assert_eq!(budget.used(), 0);

    // Independent requests may fill later hours before earlier hours. Only
    // ordering inside a response is required; no addresses are locally rebased.
    for (index, range) in [(1, first + 2 * data_seeds::TIME_HOUR_MS..end), (0, first..first + 1)] {
        let mut collection = replica.collect(time.clone(), vec![range], collection_window(&time)).unwrap();
        let mut batch = replica.batch(1, &budget, 8);
        let leaf = collection.load(&mut batch, handles[index].clone()).unwrap();
        assert_eq!(leaf.beds, if index == 0 {0} else {packs[0].len() as u64});
        assert_eq!(collection.finish().unwrap(), 1);
        run(batch.start().unwrap(), &mut file);
    }
    assert_eq!(file.bytes, packs.concat());
}

#[test]
fn collection_rejects_snapshot_mismatch_and_distinguishes_missing_from_empty_time() {
    let packs = ingress(true);
    let time = collection_time(packs[0].len());
    let first = time.header().first;
    let hour = data_seeds::TIME_HOUR_MS;
    let end = time.header().sealed;
    let (length, _) = pil_codec::r#box::Box::external_footer_from_payload(packs.concat().len() as u64, 2, &[], false).unwrap();
    let replica = Beds::reconstruct(length, 2).unwrap();
    let mut wrong = collection_window(&time); wrong.generation[0] ^= 1;
    assert!(replica.collect(time.clone(), vec![first..end], wrong).is_err());
    assert!(replica.collect(time.clone(), vec![first + hour..end, first..first + hour], collection_window(&time)).is_err());
    let mut empty = replica.collect(time.clone(), vec![first + hour..first + 2 * hour], collection_window(&time)).unwrap();
    assert_eq!(empty.finish().unwrap(), 0);
    let budget = MemoryBudget::new(64 * 1024);
    let handles = opened(&packs);
    let mut collection = replica.collect(time.clone(), vec![first..end], collection_window(&time)).unwrap();
    let other = Beds::reconstruct(length, 2).unwrap();
    assert!(collection.load(&mut other.batch(1, &budget, 8), handles[0].clone()).is_err());
    assert_eq!(budget.used(), 0);

    let mut sparse = time.bytes()[..64].to_vec(); sparse[46] = 1;
    for row in time.bytes()[64..].chunks_exact(16) { sparse.extend_from_slice(row); sparse.push(0); }
    let sparse = Arc::new(data_seeds::TimeTrie::open_replica(Arc::new(sparse), vec![first..first + 2 * hour]).unwrap());
    assert_eq!(sparse.leaf(first + hour).unwrap().unwrap().count, 0);
    assert!(sparse.leaf(first + 2 * hour).unwrap().is_none());
    assert!(sparse.spans(first, end).is_err());
    let mut collection = replica.collect(sparse.clone(), vec![first..end], collection_window(&sparse)).unwrap();
    let mut batch = replica.batch(1, &budget, 8);
    collection.load(&mut batch, handles[0].clone()).unwrap();
    assert!(collection.finish().unwrap_err().to_string().contains("missing Time"));
    assert!(collection.load(&mut batch, handles[1].clone()).is_err());
}

#[test]
fn source_scalars_reconstruct_footer_and_sparse_fills_keep_origin_addresses() {
    let packs = ingress(true);
    let mut origin = Beds::create_box(&[]).unwrap();
    for pack in &packs {
        origin.pack(pack.as_slice()).unwrap();
    }
    origin.commit().unwrap();
    let source = origin.bytes().unwrap();
    let replica = Beds::reconstruct(source.len() as u64, 2).unwrap();
    assert_eq!(
        replica.footer(),
        &source[source.len() - replica.footer().len()..]
    );
    let budget = MemoryBudget::new(64 * 1024);
    let mut file = File::default();
    let mut batch = replica.batch(1, &budget, 8);
    batch.footer().unwrap();
    run(batch.start().unwrap(), &mut file);
    assert_eq!(file.bytes.len(), source.len());

    let owner = Arc::new(packs.concat());
    let retained = owner.capacity();
    let mut incoming = receive(owner);
    let mut batch = replica.batch(1, &budget, 8);
    batch
        .load(packs[0].len() as u64, incoming.pop().unwrap())
        .unwrap();
    batch.load(0, incoming.pop().unwrap()).unwrap();
    assert_eq!(
        budget.used(),
        retained,
        "one network allocation is charged once per batch"
    );
    file.writes.clear();
    let flushes = file.flushes;
    run(batch.start().unwrap(), &mut file);
    assert_eq!(file.bytes, source);
    assert_eq!(file.flushes - flushes, 1);
    assert_eq!(budget.used(), 0);
    assert!(
        file.writes
            .iter()
            .all(|(at, n)| at + *n as u64 <= (packs[0].len() + packs[1].len()) as u64),
        "gap fill never rewrites footer"
    );
}

#[cfg(feature = "compression")]
#[test]
fn origin_publication_tracks_plain_extents_across_compression_and_footer_resume() {
    use data_seeds::packs::{Publication, Writer};
    use pil_codec::r#box::Box as Container;
    let packs = ingress(true);
    let mut writer = Writer::new(&[]).unwrap();
    writer.compression(data_seeds::Compression::default()).unwrap();
    assert_eq!(writer.next_beds_address(), 0);
    writer.pack(packs[0].as_slice()).unwrap();
    let mut disk = writer.commit().unwrap().bytes.to_vec();
    let range = Container::footer_range(&disk, disk.len() as u64).unwrap();
    let first = Publication::read(&disk[range.start as usize..], disk.len() as u64).unwrap();
    let mut resumed = Writer::resume(disk[range.start as usize..].to_vec(), disk.len() as u64).unwrap();
    assert_eq!(resumed.next_beds_address(), packs[0].len() as u64);
    assert_eq!(resumed.publication().unwrap(), first);
    let stored_start = resumed.next_address();
    // The first member is compressed, the next is plain. Address accounting
    // must depend on each plain extent, never the compression ratio.
    resumed.pack(packs[1].clone()).unwrap();
    let tail = resumed.commit().unwrap();
    assert_eq!(tail.offset, stored_start);
    let old_prefix = disk[..stored_start as usize].to_vec();
    disk.truncate(tail.offset as usize);
    disk.extend_from_slice(tail.bytes);
    assert_eq!(disk.len() as u64, tail.length);
    assert_eq!(&disk[..stored_start as usize], old_prefix);
    let range = Container::footer_range(&disk, disk.len() as u64).unwrap();
    let publication = Publication::read(&disk[range.start as usize..], disk.len() as u64).unwrap();
    let replica = Beds::reconstruct(publication.length, publication.count).unwrap();
    let mut plain = packs.concat();
    plain.extend_from_slice(replica.footer());
    assert_eq!(publication.length, plain.len() as u64);
    assert_eq!(publication.count, 2);
    assert_ne!(publication.length, disk.len() as u64);
    assert_eq!(Container::open(&plain).unwrap().get_at(packs[0].len() as u64).unwrap(), packs[1]);
    assert!(resumed.commit().unwrap().bytes.is_empty(), "unchanged publication is a no-op");
    resumed.seal().unwrap();
    assert!(resumed.pack(packs[0].clone()).is_err());
    assert!(resumed.commit().is_err());
}

#[test]
fn origin_publication_rejects_unknown_extents_and_preserves_failure_state() {
    use data_seeds::packs::{Publication, Writer};
    use pil_codec::r#box::Box as Container;
    let mut writer = Writer::new(&[]).unwrap();
    let empty = writer.commit().unwrap().bytes.to_vec();
    let publication = Publication::read(&empty, empty.len() as u64).unwrap();
    assert_eq!(publication.count, 0);
    assert_eq!(Beds::reconstruct(publication.length, 0).unwrap().footer().len() as u64, publication.length);
    for pack in ingress(false) {
        assert!(writer.pack(pack).is_err());
        assert_eq!(writer.next_address(), 0);
        assert_eq!(writer.next_beds_address(), 0);
        assert_eq!(writer.publication().unwrap(), publication);
    }
    let (length, missing) = Container::external_footer_from_payload(0, 0, &[], false).unwrap();
    assert!(Publication::read(&missing, length).is_err());
    assert!(Writer::resume(missing, length).is_err());
    // Width transitions and very large logical extents allocate only a footer.
    for body in [65_524, 65_525, 16_777_204, 16_777_205, 1u64 << 40, 1u64 << 56] {
        let (length, footer) = Container::external_footer_from_payload(body, 1, &[], false).unwrap();
        assert_eq!(Container::external_footer(length, 1, &[], false).unwrap(), footer);
        assert_eq!(length-footer.len() as u64, body);
    }
}

#[test]
fn reject_unsealed_out_of_bounds_overlapping_and_corrupt_packs() {
    let packs = ingress(true);
    let mut origin = Beds::create_box(&[]).unwrap();
    for pack in &packs {
        origin.pack(pack.as_slice()).unwrap();
    }
    origin.commit().unwrap();
    let replica = Beds::reconstruct(origin.bytes().unwrap().len() as u64, 2).unwrap();
    let budget = MemoryBudget::new(64 * 1024);
    let mut batch = replica.batch(1, &budget, 8);
    let receive_one = |raw: Vec<u8>| receive(Arc::new(raw)).pop().unwrap();
    assert!(
        batch
            .load(0, receive_one(ingress(false).remove(0)))
            .is_err()
    );
    assert!(batch.load(u64::MAX, receive_one(packs[0].clone())).is_err());
    assert_eq!(budget.used(), 0);
    batch.load(0, receive_one(packs[0].clone())).unwrap();
    assert!(batch.load(1, receive_one(packs[0].clone())).is_err());
    let mut corrupt = packs[1].clone();
    *corrupt.last_mut().unwrap() ^= 1;
    assert!(
        batch
            .load(packs[0].len() as u64, receive_one(corrupt))
            .is_err()
    );
    batch.footer().unwrap();
    assert!(
        batch
            .load(packs[0].len() as u64, receive_one(packs[1].clone()))
            .is_err()
    );
}

#[test]
fn footer_width_transitions_do_not_allocate_the_box_extent() {
    // Widening three footer integers adds three bytes: the first canonical
    // container length beyond each boundary skips those three lengths.
    for length in [
        65_535,
        65_539,
        16_777_215,
        16_777_219,
        (1u64 << 32) + 3,
        (1u64 << 40) + 3,
        u64::MAX,
    ] {
        let replica = Beds::reconstruct(length, 1).unwrap();
        assert!(replica.footer().len() <= 29);
        assert_eq!(replica.length(), length);
        let controls = pil_codec::pac_footer::Plain::read_tail(replica.footer()).unwrap();
        assert_eq!(controls.size, length);
        assert_eq!(controls.count, 1);
    }
    assert!(Beds::reconstruct(10, 1).is_err());
    assert!(Beds::reconstruct(65_536, 1).is_err());
    assert!(Beds::reconstruct(100, 0).is_err());
    let mut origin = Beds::create_box(&[]).unwrap();
    origin.commit().unwrap();
    let empty = Beds::reconstruct(origin.bytes().unwrap().len() as u64, 0).unwrap();
    assert_eq!(empty.footer(), origin.bytes().unwrap());
    assert_eq!(empty.count(), 0);
}

#[test]
fn generic_footer_serializer_preserves_headers_and_sealed_state() {
    let packs = ingress(true);
    let headers = pil_codec::api::encode_headers(&std::collections::BTreeMap::from([(
        "owner".to_owned(),
        b"application".to_vec(),
    )]))
    .unwrap();
    for sealed in [false, true] {
        let mut origin = Beds::create_box(&headers).unwrap();
        origin.pack(packs[0].as_slice()).unwrap();
        if sealed {
            origin.seal().unwrap();
        } else {
            origin.commit().unwrap();
        }
        let bytes = origin.bytes().unwrap();
        let footer =
            pil_codec::r#box::Box::external_footer(bytes.len() as u64, 1, &headers, sealed)
                .unwrap();
        assert_eq!(&bytes[bytes.len() - footer.len()..], footer);
        let restored = pil_codec::r#box::Box::open_footer(&footer, bytes.len() as u64).unwrap();
        assert_eq!(restored.is_sealed(), sealed);
        assert_eq!(restored.headers()["owner"], b"application");
    }
}
