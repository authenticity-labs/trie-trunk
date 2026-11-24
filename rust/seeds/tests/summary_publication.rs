#![cfg(feature = "registration")]
use data_seeds::{Field, MemoryBudget, doc, registration::Trunk, summaries::*, writer::*};
use std::{collections::HashMap, io};
use trie_trunk_taxonomy::{Trunk as Taxonomy, registration::Registration};

fn fixture() -> (Vec<u8>, u16, data_seeds::Document, data_seeds::Document) {
    let source = |extra| {
        doc::new("http")
            .namespace("log")
            .version(3, 8)
            .field(
                Field::new("duration")
                    .r#type("uint")
                    .size(3)
                    .annotation("#summ(numeric)"),
            )
            .field(Field::new("cached").r#type("bool").annotations(if extra {
                vec!["#summ(boolean)"]
            } else {
                vec![]
            }))
            .sign()
            .unwrap()
    };
    let before = source(false);
    let after = source(true);
    let (trie, registered) = Taxonomy::new("Summary Persistence")
        .unwrap()
        .registration(
            Registration {
                cache: "test",
                service: "lab",
                resource: "events",
                resource_type: "http",
                level: "",
                file: "http.rx",
            },
            &before,
        )
        .unwrap();
    let (trie, _) = trie.registration(Registration { cache:"test", service:"lab", resource:"events", resource_type:"http", level:"", file:"http.rx" }, &after).unwrap();
    let trie = trie.register_summaries(registered.branch, "http.summary.rx", &summary_template().unwrap()).unwrap();
    (trie.bytes().to_vec(), registered.branch, before, after)
}
fn transform(value: u32, seed: &mut data_seeds::Seed<'_>) -> Result<(), data_seeds::Error> {
    seed.set_uint("duration", value)?
        .set_bool("cached", value % 2 == 0)?;
    Ok(())
}
fn day(scope: &data_seeds::registration::Summaries, branch: u16, ordinal: u64) -> SummaryPack {
    let mut day = scope
        .accumulator_day(ordinal * SUMMARY_DAY_MS, SummaryCalendar::new(0).unwrap())
        .unwrap();
    day.ingest(branch, day.first(), 10 + ordinal as u32, transform)
        .unwrap();
    day.complete(day.end()).unwrap();
    day.seal().unwrap().1
}
fn ready(queue: &mut CommitScheduler, storage: &mut impl Storage) -> CommitId {
    loop {
        match queue.poll(storage, 2, 4096).unwrap() {
            QueueProgress::Ready(id) => return id,
            QueueProgress::Writing { .. } => (),
            QueueProgress::Idle => panic!("missing summary commit"),
        }
    }
}
fn persist(
    box_: &mut SummaryBox,
    prior: u64,
    queue: &mut CommitScheduler,
    storage: &mut impl Storage,
) -> (Vec<u8>, u64) {
    let write = box_.write().unwrap();
    assert!(!write.is_empty());
    if write.offset() > 0 {
        assert!(write.start(1, 0, queue.memory()).is_err());
    }
    // Budget exhaustion does not consume this prepared tail.
    assert!(write.start(1, prior, &MemoryBudget::new(1)).is_err());
    let cursor = write.start(1, prior, queue.memory()).unwrap();
    let id = queue.submit(cursor).unwrap();
    assert_eq!(ready(queue, storage), id);
    assert_eq!(queue.memory().used(), 0);
    let result = (write.footer().unwrap().to_vec(), write.length());
    queue.published(id).unwrap();
    result
}

#[test]
fn native_commit_restarts_from_footer_and_keeps_annotation_lineage() {
    let (trie, branch, before, after) = fixture();
    let mut trunk = Trunk::open(trie.clone()).unwrap();
    trunk
        .prescribe_file("http.rx", before.bytes().unwrap())
        .unwrap();
    let old = trunk
        .trie()
        .branches()
        .select([branch])
        .summaries()
        .unwrap();
    let mut container = SummaryBox::new(old.plan().layout().clone()).unwrap();
    container.append(day(&old, branch, 0)).unwrap();
    let path = std::env::temp_dir().join(format!(
        "summary-publication-{}-{}.summ",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let _cleanup = Cleanup(path.clone());
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut files = NativeFiles(HashMap::from([(1, file)]));
    let mut queue = CommitScheduler::new(
        MemoryBudget::new(1 << 20),
        QueueLimits {
            batches: 1,
            operations: 8,
        },
    )
    .unwrap();
    let (footer, length) = persist(&mut container, 0, &mut queue, &mut files);
    let first = std::fs::read(&path).unwrap();
    let first_reader = SummaryBoxReader::open_footer(&footer, length).unwrap();
    let first_range = first_reader.storage_range(0).unwrap();
    assert_eq!(first_reader.definitions().unwrap().len(), 1);
    drop(container);

    // The next append loads only the footer; historical children stay on disk.
    trunk
        .prescribe_file("http.rx", after.bytes().unwrap())
        .unwrap();
    let new = trunk
        .trie()
        .branches()
        .select([branch])
        .summaries()
        .unwrap();
    assert_eq!(new.plan().version(), (0, 0));
    let mut container = SummaryBox::open_footer(footer, length).unwrap();
    container.append(day(&new, branch, 1)).unwrap();
    let (footer, next_length) = persist(&mut container, length, &mut queue, &mut files);
    let stored = std::fs::read(&path).unwrap();
    assert_eq!(stored.len() as u64, next_length);
    assert_eq!(
        &stored[first_range.start as usize..first_range.end as usize],
        &first[first_range.start as usize..first_range.end as usize]
    );
    let reader = SummaryBoxReader::open_footer(&footer, next_length).unwrap();
    assert_eq!(reader.definitions().unwrap().len(), 1);
    assert_eq!(reader.len(), 2);
    let mut restarted = Trunk::open(trie).unwrap();
    restarted
        .prescribe_file("http.rx", after.bytes().unwrap())
        .unwrap();
    restarted.prescribe_file("http.summary.rx", summary_template().unwrap().bytes().unwrap()).unwrap();
    restarted.prescribe_summary_box(branch, &reader).unwrap();
    let restored = restarted
        .trie()
        .branches()
        .select([branch])
        .summaries()
        .unwrap();
    assert_eq!(restored.plan().version(), (0, 0));
    assert!(restarted.prescribe_summary_box(branch, &reader).is_err());

    // Mix an annotation rollback into the next day, using the same event version.
    let mut mixed = new
        .accumulator_day(2 * SUMMARY_DAY_MS, SummaryCalendar::new(0).unwrap())
        .unwrap();
    mixed.ingest(branch, mixed.first(), 20, transform).unwrap();
    mixed.refresh(&old).unwrap();
    mixed
        .ingest(branch, mixed.first() + 1, 21, transform)
        .unwrap();
    mixed.complete(mixed.end()).unwrap();
    let (projection, pack) = mixed.seal().unwrap();
    assert_eq!(
        projection.layout().mode(),
        data_seeds::EncodingMode::FixedSparse
    );
    let mut container = SummaryBox::open_footer(footer, next_length).unwrap();
    container.append(pack).unwrap();
    let (footer, final_length) = persist(&mut container, next_length, &mut queue, &mut files);
    let reader = SummaryBoxReader::open_footer(&footer, final_length).unwrap();
    let mut definitions = reader.definitions().unwrap();
    assert_eq!(definitions.len(), 1);
    assert_eq!(
        definitions
            .prescribe(data_seeds::SeedDefinition::prescribe(&before).unwrap(), 288)
            .unwrap()
            .version(),
        (0, 0)
    );
    let stored = std::fs::read(path).unwrap();
    let mut reader = SummaryReader::registered_box(&restarted, branch, stored, 0).unwrap();
    let window = SummaryWindow::new(2 * SUMMARY_DAY_MS..2 * SUMMARY_DAY_MS + SUMMARY_PERIOD_MS).unwrap();
    let mut rows = Vec::new();
    reader.rows(&window, |_, _, row| { rows.push(row); Ok(()) }).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].iter().find(|(name, _)| name == "cached").unwrap().1, ciborium::Value::Null);

}

#[derive(Default)]
struct FaultStorage {
    bytes: Vec<u8>,
    flushes: usize,
    failed: bool,
    writes: Vec<(u64, usize)>,
    resizes: usize,
}
impl Storage for FaultStorage {
    fn write_at(&mut self, _: FileId, offset: u64, bytes: &[u8]) -> io::Result<usize> {
        let n = bytes.len().min(31);
        let offset = offset as usize;
        self.bytes.resize(self.bytes.len().max(offset + n), 0);
        self.bytes[offset..offset + n].copy_from_slice(&bytes[..n]);
        self.writes.push((offset as u64, n));
        Ok(n)
    }
    fn resize(&mut self, _: FileId, length: u64) -> io::Result<()> {
        self.resizes += 1;
        self.bytes.resize(length as usize, 0);
        Ok(())
    }
    fn flush(&mut self, _: FileId) -> io::Result<()> {
        self.flushes += 1;
        if !self.failed {
            self.failed = true;
            Err(io::Error::other("injected payload flush failure"))
        } else {
            Ok(())
        }
    }
}
#[test]
fn dependency_failure_blocks_footer_and_retry_reuses_the_prepared_tail() {
    let (trie, branch, before, _) = fixture();
    let mut trunk = Trunk::open(trie).unwrap();
    trunk
        .prescribe_file("http.rx", before.bytes().unwrap())
        .unwrap();
    let scope = trunk
        .trie()
        .branches()
        .select([branch])
        .summaries()
        .unwrap();
    let mut container = SummaryBox::new(scope.plan().layout().clone()).unwrap();
    container.append(day(&scope, branch, 0)).unwrap();
    let write = container.write().unwrap();
    let footer_at = write.length() - write.footer().unwrap().len() as u64;
    let budget = MemoryBudget::new(1 << 20);
    let mut queue = CommitScheduler::new(
        budget.clone(),
        QueueLimits {
            batches: 1,
            operations: 8,
        },
    )
    .unwrap();
    let id = queue.submit(write.start(1, 0, &budget).unwrap()).unwrap();
    let mut storage = FaultStorage::default();
    assert!(loop {
        match queue.poll(&mut storage, 256, usize::MAX) {
            Err(_) => break true,
            Ok(QueueProgress::Writing { .. }) => (),
            other => panic!("unexpected {other:?}"),
        }
    });
    assert_eq!(storage.bytes.len() as u64, footer_at);
    assert!(
        storage
            .writes
            .iter()
            .all(|(at, len)| at + *len as u64 <= footer_at)
    );
    assert!(queue.published(id).is_err());
    assert_eq!(queue.recovered().unwrap(), [id]);
    assert_eq!(budget.used(), 0);
    let cursor = write.start(1, 0, &budget).unwrap();
    let id = queue.submit(cursor).unwrap();
    let rejected = queue
        .submit(WriteBatch::new(0).start().unwrap())
        .unwrap_err();
    let retry = rejected.into_cursor();
    assert_eq!(ready(&mut queue, &mut storage), id);
    assert!(
        queue.submit(retry).is_err(),
        "Ready still holds the publication lane"
    );
    assert_eq!(storage.bytes, write.bytes());
    assert_eq!(
        storage.resizes, 0,
        "growing positional writes need no extra resize"
    );
    queue.published(id).unwrap();
    assert_eq!(SummaryBoxReader::open(&storage.bytes).unwrap().len(), 1);
    drop(write);
    let unchanged = container.write().unwrap();
    assert!(unchanged.is_empty());
    assert!(unchanged.start(1, 0, &budget).is_err());
    let mut cursor = unchanged.start(1, unchanged.length(), &budget).unwrap();
    let calls = storage.writes.len();
    assert!(cursor.poll(&mut storage, 1, 1).unwrap().ready);
    assert_eq!(storage.writes.len(), calls);
}

#[test]
fn resident_summary_snapshots_invalidate_on_ingest_completion_and_annotation_refresh() {
    let (bytes, branch, before, after) = fixture();
    let taxonomy = Taxonomy::open(bytes.clone()).unwrap();
    let mut trunk = Trunk::open(bytes).unwrap();
    trunk.prescribe_file("http.rx", before.bytes().unwrap()).unwrap();
    let scope = trunk.trie().branches().select([branch]).summaries().unwrap();
    let mut live = scope.accumulator_day(0, SummaryCalendar::new(0).unwrap()).unwrap();
    live.ingest(branch, 1, 10, transform).unwrap();
    assert!(live.published_snapshot(taxonomy.bytes(), "http.summary.rx").unwrap().is_none());
    let (registered, _, first) = live.snapshot_registered(&taxonomy, "http.summary.rx").unwrap();
    assert_eq!(live.published_snapshot(registered.bytes(), "http.summary.rx").unwrap().as_ref(), Some(&first));
    assert!(live.published_snapshot(taxonomy.bytes(), "http.summary.rx").unwrap().is_none());
    assert!(live.published_snapshot(registered.bytes(), "other.summary.rx").unwrap().is_none());
    assert_eq!(first, live.snapshot_registered(&registered, "http.summary.rx").unwrap().2);
    let old = live.clone();
    live.ingest(branch, 2, 20, transform).unwrap();
    assert!(live.published_snapshot(registered.bytes(), "http.summary.rx").unwrap().is_none());
    assert_eq!(old.published_snapshot(registered.bytes(), "http.summary.rx").unwrap().as_ref(), Some(&first));
    let (_, _, changed) = live.snapshot_registered(&registered, "http.summary.rx").unwrap();
    assert_ne!(first, changed);
    assert_eq!(first, old.snapshot_registered(&registered, "http.summary.rx").unwrap().2);
    live.complete(SUMMARY_PERIOD_MS).unwrap();
    assert!(live.published_snapshot(registered.bytes(), "http.summary.rx").unwrap().is_none());
    let (_, _, completed) = live.snapshot_registered(&registered, "http.summary.rx").unwrap();
    assert_eq!(live.complete_through(), SUMMARY_PERIOD_MS);
    assert_eq!(completed, live.snapshot_registered(&registered, "http.summary.rx").unwrap().2);
    trunk.prescribe_file("http.rx", after.bytes().unwrap()).unwrap();
    let revised = trunk.trie().branches().select([branch]).summaries().unwrap();
    live.refresh(&revised).unwrap();
    assert!(live.published_snapshot(registered.bytes(), "http.summary.rx").unwrap().is_none());
    live.ingest(branch, SUMMARY_PERIOD_MS, 30, transform).unwrap();
    let (_, document, annotated) = live.snapshot_registered(&registered, "http.summary.rx").unwrap();
    assert_ne!(completed, annotated);
    assert!(document.definitions().len() > scope.definitions().unwrap().definitions().len() || annotated.len() != completed.len());
    // Input registry metadata also participates; a different file cannot reuse
    // the prior registration, even when the accumulator itself did not change.
    assert!(old.snapshot_registered(&registered, "other.summary.rx").is_err());
}

#[test]
fn empty_summary_box_has_no_hour_counts() {
    let (trie, branch, before, _) = fixture();
    let mut trunk = Trunk::open(trie).unwrap();
    trunk.prescribe_file("http.rx", before.bytes().unwrap()).unwrap();
    trunk.prescribe_file("http.summary.rx", summary_template().unwrap().bytes().unwrap()).unwrap();
    let scope = trunk.trie().branches().select([branch]).summaries().unwrap();
    let mut box_ = SummaryBox::new(scope.plan().layout().clone()).unwrap();
    box_.commit().unwrap();
    let reader = SummaryReader::registered_box(&trunk, branch, box_.bytes().unwrap().to_vec(), 0).unwrap();
    assert_eq!(reader.extent().unwrap(), 0..0);
    assert!(reader.hour_counts().unwrap().is_empty());
}
