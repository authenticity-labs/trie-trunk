//! Shared positional I/O execution. Placement and publication plans belong to
//! Trunk; adapters execute operations without inspecting their contents.
//!
//! The owner holds its exclusive writer lease for the entire batch. A Ready
//! result means these dependencies passed their barriers, not that the owner
//! has published a new availability snapshot. No implicit footer or CRC work.
use crate::{Error, MemoryBudget, MemoryLease};
use std::{collections::HashSet, io, ops::Range, sync::Arc};

pub type FileId = u64;
mod scheduler;
pub use scheduler::{CommitId, CommitScheduler, QueueLimits, QueueProgress, RejectedWrite};

/// Handles are opened/retained by the node's environment adapter. Short writes
/// are allowed; success must report the actual number of bytes written.
pub trait Storage {
    fn write_at(&mut self, file: FileId, offset: u64, bytes: &[u8]) -> io::Result<usize>;
    fn resize(&mut self, file: FileId, length: u64) -> io::Result<()>;
    fn flush(&mut self, file: FileId) -> io::Result<()>;
}

/// Admission charges the allocation retained by this buffer, not only a range
/// written from it. Cloning shares that charge. This does not account for an
/// allocation before it is handed to the writer or independent external owners.
#[derive(Clone)]
pub struct Buffer(Arc<Allocation>);
struct Allocation {
    bytes: Owner,
    _lease: MemoryLease,
}
enum Owner {
    Bytes(Arc<Vec<u8>>),
    Copy(Box<[u8]>),
    Pack(Arc<crate::SeedPackSnapshot>),
}
impl Buffer {
    pub fn admit(bytes: Arc<Vec<u8>>, budget: &MemoryBudget) -> Result<Self, Error> {
        let lease = budget.reserve(bytes.capacity())?;
        Ok(Self(Arc::new(Allocation {
            bytes: Owner::Bytes(bytes),
            _lease: lease,
        })))
    }
    /// Admit before copying a borrowed encoder result for asynchronous writes.
    /// The exact-size owner and its charge survive queue rejection and retries.
    pub fn copy(bytes: &[u8], budget: &MemoryBudget) -> Result<Self, Error> {
        let lease = budget.reserve(bytes.len())?;
        let mut copy = Vec::new();
        copy.try_reserve_exact(bytes.len()).map_err(|_| Error::Overflow)?;
        copy.extend_from_slice(bytes);
        Ok(Self(Arc::new(Allocation {
            bytes: Owner::Copy(copy.into_boxed_slice()),
            _lease: lease,
        })))
    }
    pub fn bytes(&self) -> &[u8] {
        match &self.0.bytes {
            Owner::Bytes(bytes) => bytes,
            Owner::Copy(bytes) => bytes,
            Owner::Pack(pack) => pack.allocation_bytes(),
        }
    }
    pub(crate) fn admit_pack(pack: Arc<crate::SeedPackSnapshot>, budget: &MemoryBudget) -> Result<Self, Error> {
        let lease = budget.reserve(pack.retained_bytes())?;
        Ok(Self(Arc::new(Allocation { bytes: Owner::Pack(pack), _lease: lease })))
    }
}

enum Operation {
    Write {
        file: FileId,
        offset: u64,
        buffer: Buffer,
        range: Range<usize>,
    },
    Resize {
        file: FileId,
        length: u64,
    },
    Flush(FileId),
}

/// Bounded plan: preserves insertion order (including overlapping writes).
/// Consecutive operations never require copying payloads into a combined buffer.
pub struct WriteBatch {
    operations: Vec<Operation>,
    dirty: Vec<FileId>,
    dirty_set: HashSet<FileId>,
    maximum: usize,
    #[cfg(feature = "registration")]
    metadata: Vec<crate::reader::Retention>,
}
impl WriteBatch {
    pub fn new(maximum_operations: usize) -> Self {
        Self {
            operations: Vec::new(),
            dirty: Vec::new(),
            dirty_set: HashSet::new(),
            maximum: maximum_operations,
            #[cfg(feature = "registration")]
            metadata: Vec::new(),
        }
    }
    fn room(&self, additional: usize) -> io::Result<()> {
        if self
            .operations
            .len()
            .checked_add(additional)
            .is_none_or(|n| n > self.maximum)
        {
            return Err(io::Error::other("write batch operation limit exceeded"));
        }
        Ok(())
    }
    fn mark(&mut self, file: FileId) {
        if self.dirty_set.insert(file) {
            self.dirty.push(file);
        }
    }
    pub fn write(
        &mut self,
        file: FileId,
        offset: u64,
        buffer: Buffer,
        range: Range<usize>,
    ) -> io::Result<&mut Self> {
        let bytes = buffer
            .bytes()
            .get(range.clone())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid write range"))?;
        offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "write address overflow"))?;
        if bytes.is_empty() {
            return Ok(self);
        }
        self.room(1)?;
        self.operations.push(Operation::Write {
            file,
            offset,
            buffer,
            range,
        });
        self.mark(file);
        Ok(self)
    }
    /// Only explicit length changes need resize. Appending bytes already grows
    /// the file; the executor does not add redundant truncate operations.
    pub fn resize(&mut self, file: FileId, length: u64) -> io::Result<&mut Self> {
        self.room(1)?;
        self.operations.push(Operation::Resize { file, length });
        self.mark(file);
        Ok(self)
    }
    /// Flush once per dirty file since the previous barrier. Used between
    /// payload/dependency writes and publication, never implicitly per Pack.
    pub fn barrier(&mut self) -> io::Result<&mut Self> {
        self.room(self.dirty.len())?;
        self.operations
            .extend(self.dirty.drain(..).map(Operation::Flush));
        self.dirty_set.clear();
        Ok(self)
    }
    pub fn start(mut self) -> io::Result<WriteCursor> {
        self.barrier()?;
        Ok(WriteCursor {
            operations: self.operations,
            operation: 0,
            written: 0,
            failed: false,
            started: false,
            #[cfg(feature = "registration")]
            metadata: self.metadata,
        })
    }
    /// Cache handoffs run only after the owner acknowledges publication.
    #[cfg(feature = "registration")]
    pub(crate) fn retain_metadata(&mut self, metadata: crate::reader::Retention) {
        self.metadata.push(metadata);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Progress {
    pub ready: bool,
    pub calls: usize,
    pub bytes: usize,
}

/// Cooperative execution. Yield a host task between polls in a browser; a
/// synchronous flush itself cannot be interrupted. Drop pending work only at
/// the storage owner's failure boundary, before publishing its availability.
pub struct WriteCursor {
    operations: Vec<Operation>,
    operation: usize,
    written: usize,
    failed: bool,
    started: bool,
    #[cfg(feature = "registration")]
    metadata: Vec<crate::reader::Retention>,
}
impl WriteCursor {
    pub fn poll(
        &mut self,
        storage: &mut impl Storage,
        maximum_calls: usize,
        maximum_bytes: usize,
    ) -> io::Result<Progress> {
        if self.failed {
            return Err(io::Error::other(
                "failed write batch requires owner recovery",
            ));
        }
        if maximum_calls == 0 || maximum_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "positive write slice limits required",
            ));
        }
        let result = self.poll_inner(storage, maximum_calls, maximum_bytes);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn poll_inner(
        &mut self,
        storage: &mut impl Storage,
        maximum_calls: usize,
        maximum_bytes: usize,
    ) -> io::Result<Progress> {
        self.started = true;
        let mut progress = Progress {
            ready: false,
            calls: 0,
            bytes: 0,
        };
        while self.operation < self.operations.len() && progress.calls < maximum_calls {
            let operation = &self.operations[self.operation];
            if matches!(operation, Operation::Write { .. }) && progress.bytes == maximum_bytes {
                break;
            }
            progress.calls += 1;
            match operation {
                Operation::Write {
                    file,
                    offset,
                    buffer,
                    range,
                } => {
                    let start = range.start + self.written;
                    let count = (range.end - start).min(maximum_bytes - progress.bytes);
                    let n = match storage.write_at(
                        *file,
                        offset + self.written as u64,
                        &buffer.bytes()[start..start + count],
                    ) {
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                        result => result?,
                    };
                    if n == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "storage write made no progress",
                        ));
                    }
                    if n > count {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "storage reported an oversized write",
                        ));
                    }
                    self.written += n;
                    progress.bytes += n;
                    if self.written < range.len() {
                        continue;
                    }
                }
                Operation::Resize { file, length } => storage.resize(*file, *length)?,
                Operation::Flush(file) => storage.flush(*file)?,
            }
            self.operation += 1;
            self.written = 0;
        }
        progress.ready = self.operation == self.operations.len();
        if progress.ready {
            self.operations = Vec::new();
            self.operation = 0;
        }
        Ok(progress)
    }
}

/// Native adapter uses retained file descriptors and platform positional writes;
/// no shared seek cursor is disturbed while readers use the same file.
#[cfg(any(unix, windows))]
pub struct NativeFiles(pub std::collections::HashMap<FileId, std::fs::File>);
#[cfg(any(unix, windows))]
impl Storage for NativeFiles {
    fn write_at(&mut self, file: FileId, offset: u64, bytes: &[u8]) -> io::Result<usize> {
        let file = self
            .0
            .get(&file)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown storage handle"))?;
        #[cfg(unix)]
        {
            std::os::unix::fs::FileExt::write_at(file, bytes, offset)
        }
        #[cfg(windows)]
        {
            std::os::windows::fs::FileExt::seek_write(file, bytes, offset)
        }
    }
    fn resize(&mut self, file: FileId, length: u64) -> io::Result<()> {
        self.0
            .get(&file)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown storage handle"))?
            .set_len(length)
    }
    fn flush(&mut self, file: FileId) -> io::Result<()> {
        self.0
            .get(&file)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown storage handle"))?
            .sync_data()
    }
}
