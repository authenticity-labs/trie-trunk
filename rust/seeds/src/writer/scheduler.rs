//! One ordered node-wide commit lane. The owner publishes its metadata between
//! Ready and published(); no following batch may pass that boundary early.
use super::{Buffer, Operation, Progress, Storage, WriteCursor};
use crate::MemoryBudget;
use std::{collections::VecDeque, fmt, io, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitId(u64);
impl CommitId {
    /// Opaque identity for an environment bridge. Queue state validates its use.
    pub fn from_raw(value: u64) -> Self {
        Self(value)
    }
    pub fn value(self) -> u64 {
        self.0
    }
}
#[derive(Clone, Copy, Debug)]
pub struct QueueLimits {
    pub batches: usize,
    pub operations: usize,
}
#[derive(Debug)]
pub enum QueueProgress {
    Idle,
    Writing {
        id: CommitId,
        progress: Progress,
    },
    /// Payload/dependency barriers passed. Availability is not published yet.
    Ready(CommitId),
}

/// Backpressure returns ownership, so the producer can retry without rebuilding
/// or losing its received payload. It must not continue collecting unboundedly.
pub struct RejectedWrite {
    pub reason: io::Error,
    cursor: WriteCursor,
}
impl RejectedWrite {
    pub fn into_cursor(self) -> WriteCursor {
        self.cursor
    }
}
impl fmt::Debug for RejectedWrite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reason.fmt(f)
    }
}
#[derive(PartialEq, Eq)]
enum State {
    Queued,
    Writing,
    Ready,
    Failed,
}
struct Entry {
    id: CommitId,
    cursor: WriteCursor,
    operations: usize,
    state: State,
}

pub struct CommitScheduler {
    memory: MemoryBudget,
    limits: QueueLimits,
    queue: VecDeque<Entry>,
    operations: usize,
    next: u64,
}
impl CommitScheduler {
    pub fn new(memory: MemoryBudget, limits: QueueLimits) -> io::Result<Self> {
        if limits.batches == 0 || limits.operations == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "positive commit queue limits required",
            ));
        }
        Ok(Self {
            memory,
            limits,
            queue: VecDeque::new(),
            operations: 0,
            next: 1,
        })
    }
    pub fn memory(&self) -> &MemoryBudget {
        &self.memory
    }
    pub fn pending(&self) -> usize {
        self.queue.len()
    }
    pub fn buffer(&self, bytes: Arc<Vec<u8>>) -> Result<Buffer, crate::Error> {
        Buffer::admit(bytes, &self.memory)
    }
    pub fn submit(&mut self, cursor: WriteCursor) -> Result<CommitId, RejectedWrite> {
        let reject = |reason: &'static str, cursor| {
            Err(RejectedWrite {
                reason: io::Error::other(reason),
                cursor,
            })
        };
        if cursor.started {
            return reject("submit a write batch before executing it", cursor);
        }
        if self.queue.front().is_some_and(|e| e.state == State::Failed) {
            return reject("commit queue requires owner recovery", cursor);
        }
        let count = cursor.operations.len();
        if self.queue.len() >= self.limits.batches
            || self
                .operations
                .checked_add(count)
                .is_none_or(|n| n > self.limits.operations)
        {
            return reject("commit queue is full", cursor);
        }
        if cursor.operations.iter().any(|op| {
            matches!(op,
            Operation::Write { buffer, .. } if !buffer.0._lease.belongs_to(&self.memory))
        }) {
            return reject("write buffer is outside the node memory budget", cursor);
        }
        let Some(next) = self.next.checked_add(1) else {
            return reject("commit identity exhausted", cursor);
        };
        let id = CommitId(self.next);
        self.next = next;
        self.operations += count;
        self.queue.push_back(Entry {
            id,
            cursor,
            operations: count,
            state: State::Queued,
        });
        Ok(id)
    }
    pub fn poll(
        &mut self,
        storage: &mut impl Storage,
        calls: usize,
        bytes: usize,
    ) -> io::Result<QueueProgress> {
        if calls == 0 || bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "positive write slice limits required",
            ));
        }
        let Some(entry) = self.queue.front_mut() else {
            return Ok(QueueProgress::Idle);
        };
        match entry.state {
            State::Ready => return Ok(QueueProgress::Ready(entry.id)),
            State::Failed => return Err(io::Error::other("commit queue requires owner recovery")),
            _ => {}
        }
        entry.state = State::Writing;
        match entry.cursor.poll(storage, calls, bytes) {
            Ok(progress) if progress.ready => {
                entry.state = State::Ready;
                Ok(QueueProgress::Ready(entry.id))
            }
            Ok(progress) => Ok(QueueProgress::Writing {
                id: entry.id,
                progress,
            }),
            Err(error) => {
                entry.state = State::Failed;
                Err(error)
            }
        }
    }
    /// Called only after the owner's existing metadata publication succeeds.
    pub fn published(&mut self, id: CommitId) -> io::Result<()> {
        match self.queue.front() {
            Some(entry) if entry.id == id && entry.state == State::Ready => {}
            _ => return Err(io::Error::other("commit is not awaiting publication")),
        }
        let entry = self.queue.pop_front().unwrap();
        self.operations -= entry.operations;
        #[cfg(feature = "registration")]
        for metadata in entry.cursor.metadata {
            // Durable publication already succeeded. A poisoned reader cache
            // remains an error on that reader, not a failed storage commit.
            let _ = metadata.publish();
        }
        Ok(())
    }
    /// Publication failure blocks later writes just as an I/O failure does.
    pub fn publication_failed(&mut self, id: CommitId) -> io::Result<()> {
        match self.queue.front_mut() {
            Some(entry) if entry.id == id && entry.state == State::Ready => {
                entry.state = State::Failed;
                Ok(())
            }
            _ => Err(io::Error::other("commit is not awaiting publication")),
        }
    }
    /// The owner restores its last published snapshot before calling this.
    /// Discard every dependent queued plan; none is silently replayed against
    /// recovered state. Returned identities let producers re-plan collection.
    pub fn recovered(&mut self) -> io::Result<Vec<CommitId>> {
        if !self
            .queue
            .front()
            .is_some_and(|entry| entry.state == State::Failed)
        {
            return Err(io::Error::other(
                "commit queue has no failed publication to recover",
            ));
        }
        let discarded = self.queue.drain(..).map(|entry| entry.id).collect();
        self.operations = 0;
        Ok(discarded)
    }
}
