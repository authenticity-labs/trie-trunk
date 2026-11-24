//! Filesystem adapter for the shared publication plan. All command/HTTP
//! adapters use this path; file operations never interpret Pills or Packs.
use super::{SummaryCommit, publication::Publication};
use crate::registration::Storage;
use crate::writer::{CommitScheduler, NativeFiles, QueueProgress, WriteBatch};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub fn ingest(
    owner: &mut super::OwnedIngress,
    spool: &mut File,
    branch: u16,
    pill: &[u8],
    now: u64,
) -> std::result::Result<u64, crate::Error> {
    use std::io::{Seek, SeekFrom, Write};
    owner.persist(branch, pill, now, |prefix, bytes| {
        (|| -> std::io::Result<()> {
            spool.seek(SeekFrom::End(0))?;
            spool.write_all(prefix)?;
            spool.write_all(bytes)?;
            spool.sync_all()
        })()
        .map_err(|e| crate::error(e.to_string()))
    })
}

fn suffix(prefix: &Path, extension: &str) -> PathBuf {
    let mut name = prefix.as_os_str().to_owned();
    name.push(extension);
    name.into()
}
fn file(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}
fn sync_directory(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

/// Callers serialize publication using their node's scheduler. Admission holds
/// the existing encoded buffers by Arc; it does not copy the Pack/index payload.
pub fn publish(
    prefix: &Path,
    storage: Storage,
    plan: &Publication,
    scheduler: &mut CommitScheduler,
) -> Result<()> {
    if scheduler.pending() != 0 {
        return Err("Publication scheduler is already occupied".into());
    }
    let pending = suffix(prefix, ".time.pending");
    let batch = publication_batch(plan, scheduler)?;
    let mut files = NativeFiles(HashMap::from([
        (1, file(&suffix(prefix, storage.suffix()))?),
        (2, file(&suffix(prefix, ".indx"))?),
        (3, file(&pending)?),
    ]));
    execute(batch, &mut files, scheduler, || {
        fs::rename(&pending, suffix(prefix, ".time"))?;
        sync_directory(prefix.parent().ok_or("Missing resource directory")?)?;
        Ok(())
    })
}

// Admit all buffers before opening or changing output files.
fn publication_batch(plan: &Publication, scheduler: &CommitScheduler) -> Result<WriteBatch> {
    let mut batch = WriteBatch::new(16);
    for (id, offset, length, bytes) in [
        (1, plan.info.offset, plan.info.length, &plan.tail),
        (
            2,
            plan.info.index,
            plan.info
                .index
                .checked_add(plan.index.len() as u64)
                .ok_or("Index length overflow")?,
            &plan.index,
        ),
    ] {
        batch
            .write(id, offset, scheduler.buffer(bytes.clone())?, 0..bytes.len())?
            .resize(id, length)?;
    }
    batch.barrier()?;
    batch
        .write(
            3,
            0,
            scheduler.buffer(plan.time.clone())?,
            0..plan.time.len(),
        )?
        .resize(3, plan.time.len() as u64)?;
    Ok(batch)
}

pub fn summary(prefix: &Path, plan: &SummaryCommit, scheduler: &mut CommitScheduler) -> Result<()> {
    if scheduler.pending() != 0 {
        return Err("Publication scheduler is already occupied".into());
    }
    plan.validate()?;
    let mut batch = WriteBatch::new(4);
    batch
        .write(
            1,
            plan.offset,
            scheduler.buffer(plan.bytes.clone())?,
            0..plan.bytes.len(),
        )?
        .resize(1, plan.length)?;
    let mut files = NativeFiles(HashMap::from([(1, file(&suffix(prefix, ".summ"))?)]));
    execute(batch, &mut files, scheduler, || {
        sync_directory(prefix.parent().ok_or("Missing resource directory")?)?;
        Ok(())
    })
}

fn execute(
    batch: WriteBatch,
    storage: &mut impl crate::writer::Storage,
    scheduler: &mut CommitScheduler,
    publish: impl FnOnce() -> Result<()>,
) -> Result<()> {
    // A synchronous filesystem adapter must not drain another caller's work
    // using its own file handles. Browser adapters drive this same queue async.
    if scheduler.pending() != 0 {
        return Err("Publication scheduler is already occupied".into());
    }
    let id = scheduler.submit(batch.start()?).map_err(|e| e.reason)?;
    loop {
        if let QueueProgress::Ready(ready) = scheduler.poll(storage, 16, 1 << 20)? {
            if ready != id {
                return Err("Unexpected publication identity".into());
            }
            break;
        }
    }
    if let Err(error) = publish() {
        scheduler.publication_failed(id)?;
        return Err(error);
    }
    scheduler.published(id)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryBudget, writer::QueueLimits};
    use std::{io, sync::Arc};

    struct Probe {
        pointers: [usize; 3],
        flushed: Vec<u64>,
        time: bool,
        fail: Option<u64>,
    }
    impl crate::writer::Storage for Probe {
        fn write_at(&mut self, id: u64, _: u64, bytes: &[u8]) -> io::Result<usize> {
            assert_eq!(
                bytes.as_ptr() as usize,
                self.pointers[id as usize - 1],
                "publication copied its encoded allocation"
            );
            if id == 3 {
                assert_eq!(
                    self.flushed,
                    [1, 2],
                    "Time was written before both dependencies were durable"
                );
                self.time = true;
            }
            Ok(bytes.len())
        }
        fn resize(&mut self, _: u64, _: u64) -> io::Result<()> {
            Ok(())
        }
        fn flush(&mut self, id: u64) -> io::Result<()> {
            if self.fail == Some(id) {
                return Err(io::Error::other("injected durability failure"));
            }
            self.flushed.push(id);
            Ok(())
        }
    }
    #[test]
    fn publication_retains_buffers_and_never_publishes_failed_dependencies() {
        let plan = Publication {
            tail: Arc::new(vec![1; 8]),
            index: Arc::new(vec![2; 8]),
            time: Arc::new(vec![3; 8]),
            info: super::super::publication::PublicationPlan {
                length: 8,
                ..Default::default()
            },
        };
        for fail in [None, Some(1), Some(2), Some(3)] {
            let memory = MemoryBudget::new(24);
            let mut scheduler = CommitScheduler::new(
                memory,
                QueueLimits {
                    batches: 1,
                    operations: 16,
                },
            )
            .unwrap();
            let batch = publication_batch(&plan, &scheduler).unwrap();
            let mut probe = Probe {
                pointers: [
                    plan.tail.as_ptr() as usize,
                    plan.index.as_ptr() as usize,
                    plan.time.as_ptr() as usize,
                ],
                flushed: vec![],
                time: false,
                fail,
            };
            let mut published = false;
            let result = execute(batch, &mut probe, &mut scheduler, || {
                published = true;
                Ok(())
            });
            assert_eq!(result.is_ok(), fail.is_none());
            assert_eq!(published, fail.is_none());
            if matches!(fail, Some(1 | 2)) {
                assert!(!probe.time);
            }
            if fail.is_none() {
                assert_eq!(probe.flushed, [1, 2, 3]);
                assert_eq!(scheduler.pending(), 0);
            }
        }
    }
    #[test]
    fn publication_budget_failure_does_not_create_files() {
        let prefix =
            std::env::temp_dir().join(format!("trunk-no-admission-{}", std::process::id()));
        let plan = Publication {
            tail: Arc::new(vec![0; 8]),
            index: Arc::new(vec![0; 8]),
            time: Arc::new(vec![0; 8]),
            info: super::super::publication::PublicationPlan {
                length: 8,
                ..Default::default()
            },
        };
        let mut scheduler = CommitScheduler::new(
            MemoryBudget::new(16),
            QueueLimits {
                batches: 1,
                operations: 16,
            },
        )
        .unwrap();
        assert!(publish(&prefix, Storage::Browse, &plan, &mut scheduler).is_err());
        for extension in [".beds", ".indx", ".time.pending"] {
            assert!(!suffix(&prefix, extension).exists());
        }
        assert_eq!(scheduler.pending(), 0);
    }
}
