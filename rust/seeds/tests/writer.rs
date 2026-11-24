use data_seeds::writer::{Buffer, FileId, Storage, WriteBatch};
use data_seeds::{Field, MemoryBudget, SeedDefinition, doc};
use std::{collections::HashMap, io, sync::Arc};

fn ingress() -> Arc<Vec<u8>> {
    let document = doc::new("writer")
        .field(Field::new("sequence").r#type("uint").size(6))
        .field(Field::new("message").r#type("tstr"))
        .sign()
        .unwrap();
    let definition = SeedDefinition::prescribe(&document).unwrap();
    let mut pack = definition.fill(8).unwrap();
    for i in 0..8 {
        let seed = definition
            .ingest(i, |i, seed| {
                seed.set_uint("sequence", i)?
                    .set_text_string("message", "storage test")?;
                Ok(())
            })
            .unwrap();
        pack.load(seed).unwrap();
    }
    pack.seal().unwrap();
    Arc::new(pack.into_bytes().unwrap())
}

#[derive(Debug, PartialEq, Eq)]
enum Call {
    Write(FileId, u64, usize),
    Resize(FileId, u64),
    Flush(FileId),
}
#[derive(Default)]
struct Files {
    bytes: HashMap<FileId, Vec<u8>>,
    calls: Vec<Call>,
    short: Option<usize>,
    interrupt: bool,
    fail_flush: bool,
    pointers: Vec<usize>,
}
impl Storage for Files {
    fn write_at(&mut self, file: FileId, offset: u64, bytes: &[u8]) -> io::Result<usize> {
        if self.interrupt {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let n = bytes.len().min(self.short.unwrap_or(usize::MAX));
        self.calls.push(Call::Write(file, offset, n));
        self.pointers.push(bytes.as_ptr() as usize);
        let target = self.bytes.entry(file).or_default();
        target.resize(target.len().max(offset as usize + n), 0);
        target[offset as usize..offset as usize + n].copy_from_slice(&bytes[..n]);
        Ok(n)
    }
    fn resize(&mut self, file: FileId, length: u64) -> io::Result<()> {
        self.calls.push(Call::Resize(file, length));
        self.bytes
            .entry(file)
            .or_default()
            .resize(length as usize, 0);
        Ok(())
    }
    fn flush(&mut self, file: FileId) -> io::Result<()> {
        self.calls.push(Call::Flush(file));
        if self.fail_flush {
            Err(io::Error::other("injected flush failure"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn sparse_writes_borrow_one_allocation_and_flush_once_per_file() {
    let bytes = ingress();
    let budget = MemoryBudget::new(bytes.capacity());
    let buffer = Buffer::admit(bytes.clone(), &budget).unwrap();
    let mut batch = WriteBatch::new(8);
    batch
        .write(1, 8192, buffer.clone(), 0..bytes.len())
        .unwrap();
    batch.write(1, 0, buffer.clone(), 0..bytes.len()).unwrap();
    batch.write(2, 0, buffer, 0..bytes.len()).unwrap();
    let mut cursor = batch.start().unwrap();
    let mut files = Files {
        short: Some(7),
        ..Default::default()
    };
    loop {
        let progress = cursor.poll(&mut files, 3, 11).unwrap();
        assert!(progress.calls <= 3 && progress.bytes <= 11);
        if progress.ready {
            break;
        }
        assert_eq!(budget.used(), bytes.capacity());
    }
    assert_eq!(budget.used(), 0);
    assert_eq!(&files.bytes[&1][8192..8192 + bytes.len()], bytes.as_slice());
    assert_eq!(&files.bytes[&1][..bytes.len()], bytes.as_slice());
    assert_eq!(files.bytes[&2], *bytes);
    assert_eq!(
        files
            .calls
            .iter()
            .filter(|c| matches!(c, Call::Flush(_)))
            .collect::<Vec<_>>(),
        vec![&Call::Flush(1), &Call::Flush(2)]
    );
    assert!(!files.calls.iter().any(|c| matches!(c, Call::Resize(..))));
    let base = bytes.as_ptr() as usize;
    assert!(
        files
            .pointers
            .iter()
            .all(|p| *p >= base && *p < base + bytes.len())
    );
    assert!(cursor.poll(&mut files, 1, 1).unwrap().ready);
}

#[test]
fn barriers_preserve_dependency_order_and_failure_never_reports_ready() {
    let bytes = ingress();
    let budget = MemoryBudget::new(bytes.capacity());
    let buffer = Buffer::admit(bytes.clone(), &budget).unwrap();
    let mut batch = WriteBatch::new(8);
    batch
        .write(1, 0, buffer.clone(), 0..bytes.len())
        .unwrap()
        .barrier()
        .unwrap();
    batch.write(2, 0, buffer, 0..bytes.len()).unwrap();
    let mut cursor = batch.start().unwrap();
    let mut files = Files {
        fail_flush: true,
        ..Default::default()
    };
    assert!(cursor.poll(&mut files, 10, usize::MAX).is_err());
    assert_eq!(
        files.calls,
        vec![Call::Write(1, 0, bytes.len()), Call::Flush(1)]
    );
    assert!(!files.bytes.contains_key(&2));
    files.fail_flush = false;
    assert!(cursor.poll(&mut files, 10, usize::MAX).is_err());
    assert_eq!(
        budget.used(),
        bytes.capacity(),
        "owner retains failed batch until recovery decision"
    );
    drop(cursor);
    assert_eq!(budget.used(), 0);
}

#[test]
fn interrupts_are_bounded_and_zero_writes_fail() {
    let bytes = ingress();
    let budget = MemoryBudget::new(bytes.capacity());
    let mut batch = WriteBatch::new(2);
    batch
        .write(
            1,
            0,
            Buffer::admit(bytes.clone(), &budget).unwrap(),
            0..bytes.len(),
        )
        .unwrap();
    let mut cursor = batch.start().unwrap();
    let mut files = Files {
        interrupt: true,
        ..Default::default()
    };
    let progress = cursor.poll(&mut files, 5, 1024).unwrap();
    assert_eq!(
        (progress.calls, progress.bytes, progress.ready),
        (5, 0, false)
    );
    files.interrupt = false;
    files.short = Some(0);
    assert_eq!(
        cursor.poll(&mut files, 5, 1024).unwrap_err().kind(),
        io::ErrorKind::WriteZero
    );
    assert!(!files.calls.iter().any(|c| matches!(c, Call::Flush(_))));
}

#[test]
fn admission_bounds_and_empty_batches() {
    let bytes = ingress();
    assert!(Buffer::admit(bytes.clone(), &MemoryBudget::new(bytes.capacity() - 1)).is_err());
    let buffer = Buffer::admit(bytes.clone(), &MemoryBudget::new(bytes.capacity())).unwrap();
    let mut batch = WriteBatch::new(2);
    assert!(batch.write(1, u64::MAX, buffer.clone(), 0..1).is_err());
    assert!(
        batch
            .write(1, 0, buffer.clone(), 0..bytes.len() + 1)
            .is_err()
    );
    batch.write(1, 0, buffer, 0..bytes.len()).unwrap();
    batch.resize(1, bytes.len() as u64).unwrap();
    assert!(
        batch.start().is_err(),
        "barriers count toward operation limit"
    );
    let mut empty = WriteBatch::new(0).start().unwrap();
    let mut files = Files::default();
    assert!(empty.poll(&mut files, 0, 1).is_err());
    assert!(empty.poll(&mut files, 1, 1).unwrap().ready);
    assert!(files.calls.is_empty());
}

fn planned(file: FileId, buffer: Buffer) -> data_seeds::writer::WriteCursor {
    let len = buffer.bytes().len();
    let mut batch = WriteBatch::new(2);
    batch.write(file, 0, buffer, 0..len).unwrap();
    batch.start().unwrap()
}

#[test]
fn commit_queue_holds_next_batch_until_metadata_publication() {
    use data_seeds::writer::{CommitScheduler, QueueLimits, QueueProgress};
    let bytes = ingress();
    let budget = MemoryBudget::new(bytes.capacity());
    let mut queue = CommitScheduler::new(
        budget.clone(),
        QueueLimits {
            batches: 2,
            operations: 4,
        },
    )
    .unwrap();
    let buffer = queue.buffer(bytes.clone()).unwrap();
    let first = queue.submit(planned(1, buffer.clone())).unwrap();
    let second = queue.submit(planned(2, buffer.clone())).unwrap();
    let rejected = queue.submit(planned(3, buffer.clone())).unwrap_err();
    assert_eq!(queue.pending(), 2);
    assert_eq!(budget.used(), bytes.capacity());
    let mut files = Files::default();
    assert!(queue.published(first).is_err());
    loop {
        match queue.poll(&mut files, 1, 7).unwrap() {
            QueueProgress::Ready(id) => {
                assert_eq!(id, first);
                break;
            }
            QueueProgress::Writing { id, progress } => {
                assert_eq!(id, first);
                assert!(progress.bytes <= 7);
            }
            _ => panic!("queued work disappeared"),
        }
    }
    let calls = files.calls.len();
    assert!(
        matches!(queue.poll(&mut files, 10, 1024).unwrap(), QueueProgress::Ready(id) if id == first)
    );
    assert_eq!(
        files.calls.len(),
        calls,
        "metadata barrier blocks later writes"
    );
    assert!(!files.bytes.contains_key(&2));
    assert!(queue.published(second).is_err());
    queue.published(first).unwrap();
    let third = queue.submit(rejected.into_cursor()).unwrap();
    for id in [second, third] {
        assert!(
            matches!(queue.poll(&mut files, 10, usize::MAX).unwrap(), QueueProgress::Ready(ready) if ready == id)
        );
        queue.published(id).unwrap();
    }
    assert!(matches!(
        queue.poll(&mut files, 1, 1).unwrap(),
        QueueProgress::Idle
    ));
    drop(buffer);
    assert_eq!(budget.used(), 0);
}

#[test]
fn commit_failure_discards_dependent_plans_only_after_owner_recovery() {
    use data_seeds::writer::{CommitScheduler, QueueLimits};
    let bytes = ingress();
    let budget = MemoryBudget::new(bytes.capacity());
    let mut queue = CommitScheduler::new(
        budget.clone(),
        QueueLimits {
            batches: 2,
            operations: 4,
        },
    )
    .unwrap();
    let buffer = queue.buffer(bytes.clone()).unwrap();
    let first = queue.submit(planned(1, buffer.clone())).unwrap();
    let second = queue.submit(planned(2, buffer)).unwrap();
    assert!(queue.recovered().is_err());
    let mut files = Files {
        fail_flush: true,
        ..Default::default()
    };
    assert!(queue.poll(&mut files, 10, usize::MAX).is_err());
    assert!(queue.published(first).is_err());
    files.fail_flush = false;
    assert!(queue.poll(&mut files, 10, usize::MAX).is_err());
    assert!(!files.bytes.contains_key(&2));
    assert_eq!(queue.recovered().unwrap(), vec![first, second]);
    assert_eq!(budget.used(), 0);
    assert_eq!(queue.pending(), 0);
}

#[test]
fn queue_requires_its_budget_and_blocks_after_publication_failure() {
    use data_seeds::writer::{CommitScheduler, QueueLimits, QueueProgress};
    let bytes = ingress();
    let budget = MemoryBudget::new(bytes.capacity());
    let mut queue = CommitScheduler::new(
        budget.clone(),
        QueueLimits {
            batches: 1,
            operations: 2,
        },
    )
    .unwrap();
    let outside = Buffer::admit(bytes.clone(), &MemoryBudget::new(bytes.capacity())).unwrap();
    assert!(queue.submit(planned(1, outside)).is_err());
    let id = queue
        .submit(planned(1, queue.buffer(bytes).unwrap()))
        .unwrap();
    let mut files = Files::default();
    assert!(matches!(
        queue.poll(&mut files, 10, usize::MAX).unwrap(),
        QueueProgress::Ready(_)
    ));
    queue.publication_failed(id).unwrap();
    assert!(queue.published(id).is_err());
    assert!(queue.poll(&mut files, 10, usize::MAX).is_err());
    assert_eq!(queue.recovered().unwrap(), vec![id]);
}

#[cfg(feature = "registration")]
#[test]
fn trunk_owns_one_optional_commit_lane() {
    use data_seeds::writer::QueueLimits;
    let taxonomy = trie_trunk_taxonomy::Trunk::new("writer").unwrap();
    let mut node = data_seeds::trunk::Trunk::open(taxonomy.bytes()).unwrap();
    assert!(node.commits().is_err());
    node = node
        .writer(
            MemoryBudget::new(1024),
            QueueLimits {
                batches: 2,
                operations: 8,
            },
        )
        .unwrap();
    assert_eq!(node.commits().unwrap().memory().limit(), 1024);
    assert_eq!(node.commits().unwrap().pending(), 0);
}
