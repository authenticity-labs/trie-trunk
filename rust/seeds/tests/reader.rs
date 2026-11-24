#![cfg(feature = "registration")]
use data_seeds::reader::{Reader, Selection, Storage};
use data_seeds::registration::{Packs, Trunk};
use data_seeds::{Compare, Direction, Field, MemoryBudget, Predicate, Query, doc};
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use trie_trunk_taxonomy::{Trunk as Taxonomy, registration::Registration};

struct File {
    bytes: Vec<u8>,
    calls: Mutex<Vec<(u64, usize)>>,
    chunk: usize,
    fail: AtomicUsize,
}
impl Storage for File {
    fn read_at(&self, _: u64, offset: u64, target: &mut [u8]) -> io::Result<usize> {
        self.calls.lock().unwrap().push((offset, target.len()));
        if self.fail.load(Ordering::Relaxed) == 1 {
            return Err(io::Error::other("read fault"));
        }
        if self.fail.load(Ordering::Relaxed) == 2 {
            return Ok(target.len() + 1);
        }
        let bytes = self.bytes.get(offset as usize..).unwrap_or(&[]);
        let n = bytes.len().min(target.len()).min(self.chunk);
        target[..n].copy_from_slice(&bytes[..n]);
        Ok(n)
    }
}
fn fixture(chunk: usize) -> (Packs, Arc<File>, Vec<u64>, Vec<usize>) {
    fixture_records(chunk, 5, 0)
}
fn fixture_records(
    chunk: usize,
    rows: u64,
    payload: usize,
) -> (Packs, Arc<File>, Vec<u64>, Vec<usize>) {
    let document = doc::new("sample")
        .field(Field::new("sequence").r#type("uint").size(6))
        .field(Field::new("note").r#type("tstr"))
        .sign()
        .unwrap();
    let (taxonomy, registration) = Taxonomy::new("Reader")
        .unwrap()
        .registration(
            Registration {
                cache: "demo",
                service: "lab",
                resource: "measurements",
                resource_type: "sample",
                level: "",
                file: "sample.rx",
            },
            &document,
        )
        .unwrap();
    let mut trunk = Trunk::open(taxonomy.bytes()).unwrap();
    let definition = trunk
        .prescribe(registration.definition, document.bytes().unwrap())
        .unwrap();
    let scope = trunk.trie().branches().packs().unwrap();
    let mut bytes = Vec::new();
    let mut addresses = Vec::new();
    let mut lengths = Vec::new();
    for group in 0..4 {
        let mut pack = scope.fill(registration.branch, rows as usize).unwrap();
        pack.header("crc32c", vec![0; 4]).unwrap();
        for sequence in group * rows..group * rows + rows {
            let text = format!("value {sequence}{}", "x".repeat(payload));
            pack.load(
                definition
                    .ingest(sequence, |sequence, seed| {
                        seed.set_uint("sequence", sequence)?
                            .set_text_string("note", &text)?;
                        Ok(())
                    })
                    .unwrap(),
            )
            .unwrap();
        }
        pack.seal().unwrap();
        let mut encoded = pack.into_bytes().unwrap();
        data_seeds::integrity::stamp(&mut encoded).unwrap();
        #[cfg(feature = "compression")]
        if group == 3 {
            let mut compressed = Vec::new();
            pil_codec::compression::seal(
                &encoded,
                pil_codec::compression::Algorithm::Zstd,
                1,
                false,
                &mut compressed,
            )
            .unwrap();
            encoded = compressed;
        }
        // Sparse placements are supplied, not inferred from adjacent offsets.
        let offset = ((bytes.len() + 65535) / 65536) * 65536;
        bytes.resize(offset, 0);
        addresses.push(offset as u64);
        lengths.push(encoded.len());
        bytes.extend(encoded);
    }
    (
        scope,
        Arc::new(File {
            bytes,
            calls: Mutex::new(Vec::new()),
            chunk,
            fail: AtomicUsize::new(0),
        }),
        addresses,
        lengths,
    )
}
fn reader(scope: &Packs, file: &Arc<File>, addresses: Vec<u64>, budget: MemoryBudget) -> Reader {
    scope
        .reader(
            file.clone(),
            Selection {
                file: 7,
                length: file.bytes.len() as u64,
                addresses,
            },
            4096,
            budget,
        )
        .unwrap()
}
fn sequences(batch: &data_seeds::Batch) -> Vec<u64> {
    batch
        .objects()
        .unwrap()
        .iter()
        .map(
            |o| match &o.iter().find(|(n, _)| n == "sequence").unwrap().1 {
                ciborium::Value::Integer(i) => u64::try_from(*i).unwrap(),
                _ => panic!(),
            },
        )
        .collect()
}
#[test]
fn combined_readers_keep_global_limits_sorting_and_independent_payloads() {
    let (scope, first, addresses, _) = fixture(7);
    let (_, second, _, _) = fixture(13);
    let budget = MemoryBudget::new(1 << 20);
    let a = reader(&scope, &first, vec![addresses[0]], budget.clone());
    let empty = reader(&scope, &first, vec![], budget.clone());
    let b = reader(&scope, &second, vec![addresses[1]], budget.clone());
    let mut short = Reader::query_many(vec![empty.clone(), a.clone(), b.clone()], Query::new().limit(2)).unwrap();
    assert_eq!(sequences(&short.fetch(2).unwrap()), vec![0, 1]);
    assert!(second.calls.lock().unwrap().is_empty(), "global early stop must not read the second file");
    drop(short);
    let mut crossing = Reader::query_many(vec![a.clone(), empty.clone(), b.clone()], Query::new().offset(4).limit(3)).unwrap();
    assert_eq!(sequences(&crossing.fetch(8).unwrap()), vec![4, 5, 6]);
    drop(crossing);
    let mut sorted = Reader::query_many(vec![a.clone(), empty, b.clone()], Query::new().order_by("sequence", Direction::Descending).offset(1).limit(3)).unwrap();
    let mut got = Vec::new();
    loop {
        let slice = sorted.poll(2, 1).unwrap();
        got.extend(sequences(&slice.batch));
        if slice.done { break; }
    }
    assert_eq!(got, vec![8, 7, 6]);
    drop(sorted);
    let mut cancelled = Reader::query_many(vec![a.clone(), b.clone()], Query::new()).unwrap();
    cancelled.abort();
    assert!(cancelled.fetch(1).is_err());
    drop(cancelled);drop(a);drop(b);
    assert_eq!(budget.used(), 0);
    assert!(Reader::query_many(vec![], Query::new()).unwrap().fetch(1).unwrap().is_empty());
}
#[test]
fn registered_reader_preserves_sparse_addresses_and_short_reads() {
    let (scope, file, addresses, lengths) = fixture(3);
    let budget = MemoryBudget::new(1 << 20);
    let reader = reader(
        &scope,
        &file,
        vec![addresses[2], addresses[0]],
        budget.clone(),
    );
    let mut cursor = reader
        .query(Query::new().select(["sequence"]).limit(7))
        .unwrap();
    assert!(file.calls.lock().unwrap().is_empty());
    let batch = cursor.fetch(7).unwrap();
    assert_eq!(sequences(&batch), vec![10, 11, 12, 13, 14, 0, 1]);
    assert!(budget.used() <= lengths[0] + lengths[2]);
    assert!(batch.retained_pack_bytes() < lengths[0] + lengths[2]);
    assert!(
        file.calls
            .lock()
            .unwrap()
            .iter()
            .all(|(at, _)| *at < lengths[0] as u64
                || (*at >= addresses[2] && *at < addresses[2] + lengths[2] as u64))
    );
    cursor.abort();
    drop(cursor);
    drop(reader);
    drop(scope);
    assert_eq!(sequences(&batch), vec![10, 11, 12, 13, 14, 0, 1]);
    drop(batch);
    assert_eq!(budget.used(), 0);
}
#[test]
fn reader_queries_share_allocations_and_charge_views_until_released() {
    let (scope, file, addresses, lengths) = fixture(usize::MAX);
    let budget = MemoryBudget::new(lengths[0]);
    let reader = reader(&scope, &file, addresses, budget.clone());
    let first = reader.pack(0).unwrap();
    let calls = file.calls.lock().unwrap().len();
    let also = reader.pack(0).unwrap();
    assert!(Arc::ptr_eq(&first, &also));
    assert_eq!(file.calls.lock().unwrap().len(), calls);
    assert_eq!(budget.used(), lengths[0]);
    let scan = first.filter(Predicate::All).unwrap();
    drop(first);
    drop(also);
    assert_eq!(
        budget.used(),
        lengths[0],
        "a standalone scan still owns its byte allocation"
    );
    assert!(reader.pack(1).is_err());
    drop(scan);
    assert_eq!(budget.used(), 0);
    let mut a = reader
        .query(
            Query::new()
                .filter(Predicate::compare("sequence", Compare::Ge, 0))
                .limit(2),
        )
        .unwrap();
    let mut b = reader
        .query(
            Query::new()
                .filter(Predicate::compare("sequence", Compare::Ge, 0))
                .limit(2),
        )
        .unwrap();
    let one = a.fetch(1).unwrap();
    let calls = file.calls.lock().unwrap().len();
    let two = b.fetch(1).unwrap();
    assert_eq!(file.calls.lock().unwrap().len(), calls);
    assert_eq!(budget.used(), lengths[0]);
    drop(a);
    drop(b);
    assert!(budget.used() > 0 && budget.used() < lengths[0]);
    drop(one);
    drop(two);
    assert_eq!(budget.used(), reader.metadata_bytes().unwrap());
    drop(reader);
    assert_eq!(budget.used(), 0);
}
#[test]
fn reader_sort_releases_scan_allocations_and_reloads_only_results() {
    let (scope, file, addresses, _) = fixture(usize::MAX);
    let budget = MemoryBudget::new(1 << 20);
    let reader = reader(&scope, &file, addresses, budget.clone());
    let mut cursor = reader
        .query(
            Query::new()
                .order_by("sequence", Direction::Descending)
                .limit(2),
        )
        .unwrap();
    let mut got = Vec::new();
    loop {
        let result = cursor.poll(2, 2).unwrap();
        got.extend(sequences(&result.batch));
        if result.done {
            break;
        }
    }
    assert_eq!(got, vec![19, 18]);
    assert_eq!(budget.used(), reader.metadata_bytes().unwrap());
    drop(reader);
    assert_eq!(budget.used(), 0);
}
#[test]
fn reader_faults_release_current_bytes_and_block_results() {
    let (scope, file, addresses, _) = fixture(usize::MAX);
    let budget = MemoryBudget::new(1 << 20);
    let reader = reader(&scope, &file, addresses, budget.clone());
    let mut cursor = reader
        .query(Query::new().filter(Predicate::compare("sequence", Compare::Gt, 999)))
        .unwrap();
    cursor.poll(1, 5).unwrap();
    assert!(budget.used() > 0);
    file.fail.store(1, Ordering::Relaxed);
    assert!(cursor.poll(1, 100).is_err());
    assert_eq!(budget.used(), reader.metadata_bytes().unwrap());
    file.fail.store(2, Ordering::Relaxed);
    assert!(reader.pack(0).is_err());
    assert_eq!(budget.used(), reader.metadata_bytes().unwrap());
    drop(reader);
    assert_eq!(budget.used(), 0);
}
#[test]
fn published_reader_does_not_repeat_checksum_validation() {
    let (scope, mut file, addresses, _) = fixture(usize::MAX);
    let bytes = &mut Arc::get_mut(&mut file).unwrap().bytes;
    let length = pil_codec::pac_envelope::stored_length(bytes).unwrap() as usize;
    let checksum = pil_codec::pac::application_header(&bytes[..length], "crc32c")
        .unwrap()
        .unwrap();
    let at = checksum.as_ptr() as usize - bytes.as_ptr() as usize;
    bytes[at] ^= 1;
    assert!(data_seeds::integrity::before_write(&bytes[..length]).is_err());
    let reader = reader(
        &scope,
        &file,
        vec![addresses[0]],
        MemoryBudget::new(1 << 20),
    );
    assert_eq!(
        sequences(&reader.query(Query::new()).unwrap().fetch(5).unwrap()),
        vec![0, 1, 2, 3, 4]
    );
}

#[test]
fn readers_share_payloads_across_native_query_threads() {
    fn send<T: Send>() {}
    fn shared<T: Send + Sync>() {}
    send::<data_seeds::Cursor>();
    shared::<Reader>();
    let (scope, file, addresses, lengths) = fixture(usize::MAX);
    let budget = MemoryBudget::new(lengths[0]);
    let reader = reader(&scope, &file, addresses, budget.clone());
    let gate = Arc::new(std::sync::Barrier::new(3));
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let reader = reader.clone();
        let gate = gate.clone();
        tasks.push(std::thread::spawn(move || {
            let mut query = reader
                .query(
                    Query::new()
                        .filter(Predicate::compare("sequence", Compare::Ge, 0))
                        .limit(2),
                )
                .unwrap();
            let batch = query.fetch(1).unwrap();
            gate.wait();
            gate.wait();
            assert_eq!(sequences(&batch), vec![0]);
        }));
    }
    gate.wait();
    assert_eq!(budget.used(), lengths[0]);
    assert_eq!(
        file.calls.lock().unwrap().len(),
        3,
        "one prefix, metadata, and shared payload window across both threads"
    );
    gate.wait();
    for task in tasks {
        task.join().unwrap();
    }
    assert_eq!(budget.used(), reader.metadata_bytes().unwrap());
    drop(reader);
    assert_eq!(budget.used(), 0);
}

#[test]
fn selective_query_fits_a_window_budget_smaller_than_one_pack() {
    let (scope, file, addresses, lengths) = fixture_records(usize::MAX, 1024, 2048);
    assert!(lengths[0] > 2 * 1024 * 1024);
    let budget = MemoryBudget::new(128 * 1024);
    let reader = scope
        .reader(
            file.clone(),
            Selection {
                file: 7,
                length: file.bytes.len() as u64,
                addresses: vec![addresses[0]],
            },
            4 * 1024 * 1024,
            budget.clone(),
        )
        .unwrap();
    let mut query = reader.query(Query::new().limit(2)).unwrap();
    let batch = query.fetch(2).unwrap();
    assert_eq!(sequences(&batch), [0, 1]);
    let calls = file.calls.lock().unwrap();
    assert!(calls.iter().all(|(_, n)| *n <= 64 * 1024));
    assert!(calls.iter().map(|(_, n)| *n).sum::<usize>() < 128 * 1024);
    drop(calls);
    drop(query);
    assert!(budget.used() > 0 && budget.used() < 64 * 1024);
    drop(batch);
    assert_eq!(budget.used(), reader.metadata_bytes().unwrap());
    drop(reader);
    assert_eq!(budget.used(), 0);
}

#[test]
fn oversized_member_window_is_admitted_and_failed_reads_are_retryable() {
    let (scope, file, addresses, _) = fixture_records(usize::MAX, 2, 70000);
    let budget = MemoryBudget::new(128 * 1024);
    let reader = scope
        .reader(
            file.clone(),
            Selection {
                file: 7,
                length: file.bytes.len() as u64,
                addresses: vec![addresses[0]],
            },
            1 << 20,
            budget.clone(),
        )
        .unwrap();
    let batch = reader
        .query(Query::new().limit(1))
        .unwrap()
        .fetch(1)
        .unwrap();
    let metadata = budget.used();
    let mut view = batch.reader();
    file.fail.store(1, Ordering::Relaxed);
    assert!(view.view(0).is_err());
    assert_eq!(
        budget.used(),
        metadata,
        "failed payload read released its admission"
    );
    file.fail.store(0, Ordering::Relaxed);
    let object = view.view(0).unwrap().object().unwrap();
    let ciborium::Value::Text(text) = &object.iter().find(|(name, _)| name == "note").unwrap().1
    else {
        panic!()
    };
    assert_eq!(text.len(), 70007);
    assert!(budget.used() > 70000 && budget.used() < 128 * 1024);
    drop(view);
    drop(batch);
    assert_eq!(budget.used(), reader.metadata_bytes().unwrap());
    drop(reader);
    assert_eq!(budget.used(), 0);
}

#[test]
fn prepared_metadata_survives_queries_without_pinning_members() {
    let (scope, file, addresses, lengths) = fixture_records(usize::MAX, 1024, 2048);
    let budget = MemoryBudget::new(128 * 1024);
    let reader = scope
        .reader(
            file.clone(),
            Selection {
                file: 7,
                length: file.bytes.len() as u64,
                addresses: addresses[..3].to_vec(),
            },
            4 << 20,
            budget.clone(),
        )
        .unwrap();
    assert_eq!(reader.prepare(0, 2).unwrap(), 2);
    assert_eq!(reader.prepare(2, 2).unwrap(), 3);
    let reads = file.calls.lock().unwrap().len();
    let metadata = reader.metadata_bytes().unwrap();
    assert!(metadata > 0 && metadata < 16000);
    assert_eq!(budget.used(), metadata);
    assert!(
        file.calls
            .lock()
            .unwrap()
            .iter()
            .map(|(_, n)| *n)
            .sum::<usize>()
            < 16000
    );
    assert_eq!(reader.prepare(0, 3).unwrap(), 3);
    assert_eq!(file.calls.lock().unwrap().len(), reads);
    assert!(reader.prepare(0, 0).is_err());
    for _ in 0..2 {
        let rows = reader
            .query(Query::new().limit(1))
            .unwrap()
            .fetch(1)
            .unwrap();
        assert_eq!(sequences(&rows), [0]);
        drop(rows);
        assert_eq!(budget.used(), metadata);
    }
    assert!(metadata < lengths[0]);
    let retained = reader.metadata(2).unwrap();
    drop(reader);
    assert_eq!(budget.used(), retained.retained_bytes());
    drop(retained);
    assert_eq!(budget.used(), 0);
}

#[cfg(feature = "compression")]
#[test]
fn compressed_preparation_keeps_only_expanded_metadata() {
    let (scope, file, addresses, _) = fixture(usize::MAX);
    let budget = MemoryBudget::new(1 << 20);
    let reader = reader(&scope, &file, vec![addresses[3]], budget.clone());
    reader.prepare(0, 1).unwrap();
    let retained = reader.metadata_bytes().unwrap();
    assert_eq!(budget.used(), retained);
    let reads = file.calls.lock().unwrap().len();
    reader.prepare(0, 1).unwrap();
    assert_eq!(file.calls.lock().unwrap().len(), reads);
    let rows = reader
        .query(Query::new().limit(1))
        .unwrap()
        .fetch(1)
        .unwrap();
    assert_eq!(sequences(&rows), [15]);
    drop(rows);
    assert_eq!(budget.used(), retained);
    drop(reader);
    assert_eq!(budget.used(), 0);
}

#[cfg(feature = "compression")]
#[test]
fn compressed_reader_uses_available_admission_not_the_configured_size_ceiling() {
    let (scope, file, addresses, _) = fixture(usize::MAX);
    let budget = MemoryBudget::new(1 << 20);
    let reader = scope.reader(file.clone(), Selection {
        file: 7, length: file.bytes.len() as u64, addresses: vec![addresses[3]],
    }, 32 << 20, budget.clone()).unwrap();
    let rows = reader.query(Query::default()).unwrap().fetch(10).unwrap();
    assert_eq!(sequences(&rows), [15, 16, 17, 18, 19]);
    drop(rows);
    drop(reader);
    assert_eq!(budget.used(), 0);
}

#[test]
fn metadata_admission_failure_keeps_completed_entries_and_no_partial_entry() {
    let (scope, file, addresses, _) = fixture(usize::MAX);
    let first_header = pil_codec::pac_envelope::Plain::read_prefix(&file.bytes)
        .unwrap()
        .header_length as usize;
    let budget = MemoryBudget::new(first_header);
    let reader = reader(&scope, &file, addresses, budget.clone());
    assert!(reader.prepare(0, 2).is_err());
    assert_eq!(budget.used(), first_header);
    assert_eq!(reader.metadata_bytes().unwrap(), first_header);
    let reads = file.calls.lock().unwrap().len();
    reader.prepare(0, 1).unwrap();
    assert_eq!(file.calls.lock().unwrap().len(), reads);
    drop(reader);
    assert_eq!(budget.used(), 0);
}

#[test]
fn collected_pack_retains_commons_and_application_headers_without_disk_reads() {
    use pil_codec::api::FieldBytes;
    let document = doc::new("common")
        .field(Field::new("sequence").r#type("uint").size(4))
        .field(Field::new("category").r#type("tstr"))
        .sign()
        .unwrap();
    let (taxonomy, registration) = Taxonomy::new("Metadata")
        .unwrap()
        .registration(
            Registration {
                cache: "demo",
                service: "lab",
                resource: "metadata",
                resource_type: "sample",
                level: "",
                file: "common.rx",
            },
            &document,
        )
        .unwrap();
    let mut trunk = Trunk::open(taxonomy.bytes()).unwrap();
    let definition = trunk
        .prescribe(registration.definition, document.bytes().unwrap())
        .unwrap();
    let scope = trunk.trie().branches().packs().unwrap();
    let mut pack = scope.fill(registration.branch, 4).unwrap();
    pack.configure(data_seeds::PackOptions {
        common_values: true,
        ..Default::default()
    })
    .unwrap();
    pack.header("test", vec![1, 2, 3]).unwrap();
    for row in 0..4 {
        pack.load(
            definition
                .ingest(row, |row, seed| {
                    seed.set_uint("sequence", row)?
                        .set_text_string("category", "shared")?;
                    Ok(())
                })
                .unwrap(),
        )
        .unwrap();
    }
    pack.seal().unwrap();
    let bytes = pack.into_bytes().unwrap();
    let collected = scope
        .open_pack(Arc::new(bytes.clone()), 0..bytes.len(), 4096)
        .unwrap();
    let file = Arc::new(File {
        bytes,
        calls: Mutex::new(Vec::new()),
        chunk: usize::MAX,
        fail: AtomicUsize::new(1),
    });
    let budget = MemoryBudget::new(4096);
    let reader = reader(&scope, &file, vec![0], budget.clone());
    reader.retain(0, &collected).unwrap();
    {
        let mut query = reader.query(Query::new().select(["sequence"])).unwrap();
        let batch = query.fetch(4).unwrap();
        assert_eq!(sequences(&batch), vec![0, 1, 2, 3]);
        assert!(file.calls.lock().unwrap().is_empty());
    }
    // A cache refresh prescribes equivalent Definitions at new addresses.
    // Ready handoff must still restore the same data without touching disk.
    let mut refreshed = Trunk::open(taxonomy.bytes()).unwrap();
    refreshed.prescribe(registration.definition, document.bytes().unwrap()).unwrap();
    let fresh_scope = refreshed.trie().branches().packs().unwrap();
    let ready_budget = MemoryBudget::new(65536);
    let ready = data_seeds::replication::Readiness::new(ready_budget.clone());
    ready.admit("source", 0, 65536, 1).unwrap();
    ready.load("source", 0, collected.clone(), 1).unwrap();
    let fresh_reader = fresh_scope.reader(file.clone(), Selection {
        file: 1, length: file.bytes.len() as u64, addresses: vec![0],
    }, 65536, ready_budget.clone()).unwrap();
    fresh_reader.ready(&ready, "source").unwrap();
    ready.clear("source").unwrap();
    assert_eq!(sequences(&fresh_reader.query(Query::new()).unwrap().fetch(4).unwrap()), vec![0, 1, 2, 3]);
    assert!(file.calls.lock().unwrap().is_empty());
    drop(fresh_reader);
    assert_eq!(ready_budget.used(), 0);
    drop(collected);
    let metadata = reader.metadata(0).unwrap();
    assert_eq!(
        metadata.common("category").unwrap(),
        FieldBytes::Bytes(b"shared")
    );
    assert_eq!(metadata.common("sequence").unwrap(), FieldBytes::Absent);
    assert_eq!(metadata.header("test").unwrap(), Some(&[1, 2, 3][..]));
    assert_eq!(metadata.len(), 4);
    assert!(file.calls.lock().unwrap().is_empty());
    assert_eq!(budget.used(), metadata.retained_bytes());
    drop(reader);
    drop(metadata);
    assert_eq!(budget.used(), 0);
}

#[test]
fn collected_metadata_follows_commit_publication_and_recovery() {
    use data_seeds::writer::{CommitScheduler, QueueLimits, QueueProgress, Storage as WriterStorage};
    struct Sink { fail: bool }
    impl WriterStorage for Sink {
        fn write_at(&mut self, _: u64, _: u64, bytes: &[u8]) -> io::Result<usize> { Ok(bytes.len()) }
        fn resize(&mut self, _: u64, _: u64) -> io::Result<()> { Ok(()) }
        fn flush(&mut self, _: u64) -> io::Result<()> {
            if self.fail { Err(io::Error::other("flush failed")) } else { Ok(()) }
        }
    }
    let (scope, file, addresses, lengths) = fixture(usize::MAX);
    let pack = scope.open_pack(Arc::new(file.bytes[..lengths[0]].to_vec()), 0..lengths[0], 4096).unwrap();
    let header_length = {
        let reader = reader(&scope, &file, vec![addresses[0]], MemoryBudget::new(4096));
        reader.metadata(0).unwrap().retained_bytes()
    };
    let memory = MemoryBudget::new(header_length);
    let selected = reader(&scope, &file, vec![addresses[0]], memory.clone());
    let replica = scope.reconstruct(file.bytes.len() as u64 + 32, 4).unwrap();
    let mut commits = CommitScheduler::new(MemoryBudget::new(4096), QueueLimits { batches: 1, operations: 8 }).unwrap();
    for fail_io in [true, false] {
        let mut batch = replica.batch(7, commits.memory(), 8);
        batch.pack_selected(&selected, 0, pack.clone()).unwrap();
        let id = commits.submit(batch.start().unwrap()).unwrap();
        assert_eq!(selected.metadata_bytes().unwrap(), 0);
        assert_eq!(memory.used(), header_length);
        let result = commits.poll(&mut Sink { fail: fail_io }, 8, 4096);
        if fail_io { assert!(result.is_err()); }
        else {
            assert!(matches!(result.unwrap(), QueueProgress::Ready(_)));
            assert_eq!(selected.metadata_bytes().unwrap(), 0);
            commits.publication_failed(id).unwrap();
        }
        assert_eq!(commits.recovered().unwrap(), vec![id]);
        assert_eq!(selected.metadata_bytes().unwrap(), 0);
        assert_eq!(memory.used(), 0);
    }
    let mut batch = replica.batch(7, commits.memory(), 8);
    batch.pack_selected(&selected, 0, pack.clone()).unwrap();
    let id = commits.submit(batch.start().unwrap()).unwrap();
    assert!(matches!(commits.poll(&mut Sink { fail: false }, 8, 4096).unwrap(), QueueProgress::Ready(_)));
    assert_eq!(selected.metadata_bytes().unwrap(), 0);
    commits.published(id).unwrap();
    assert_eq!(selected.metadata_bytes().unwrap(), header_length);
    let reads = file.calls.lock().unwrap().len();
    selected.prepare(0, 1).unwrap();
    assert_eq!(file.calls.lock().unwrap().len(), reads);
    drop(selected);
    assert_eq!(memory.used(), 0);
}
