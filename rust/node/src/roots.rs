//! Ordered, live-only delivery from the node's serialized durable write path.
use crate::{Node, Result};
use data_seeds::{MemoryBudget, MemoryLease};
use std::{
    collections::VecDeque,
    io::Write,
    os::unix::net::UnixStream,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

const SUBSCRIBERS: usize = 128;
const QUEUE: usize = 8;

pub struct Message {
    pub bytes: Vec<u8>,
    _lease: MemoryLease,
    _node_lease: MemoryLease,
}

struct Queue {
    wake: Option<UnixStream>,
    messages: VecDeque<Arc<Message>>,
}
struct Slot {
    branch: u16,
    digest: String,
    source: String,
    queue: Mutex<Queue>,
}

pub struct Subscription {
    slot: Arc<Slot>,
    ready: UnixStream,
}

impl Subscription {
    /// One byte signals one complete message. EOF requires resynchronization.
    pub fn readiness(&self) -> std::io::Result<UnixStream> {
        self.ready.try_clone()
    }
    pub fn next(&self) -> Result<Arc<Message>> {
        self.slot
            .queue
            .lock()
            .unwrap()
            .messages
            .pop_front()
            .ok_or_else(|| "No pending Roots message".into())
    }
}

pub(crate) struct Roots {
    slots: Mutex<Vec<Weak<Slot>>>,
    active: AtomicBool,
    budget: MemoryBudget,
    node_budget: MemoryBudget,
}

impl Roots {
    pub fn new(node_budget: MemoryBudget) -> Self {
        Self {
            slots: Mutex::default(),
            active: AtomicBool::new(false),
            budget: MemoryBudget::new((node_budget.limit() / 4).min(16 << 20)),
            node_budget,
        }
    }

    fn subscribe(&self, branch: u16, digest: String, source: String) -> Result<Subscription> {
        let mut slots = self.slots.lock().unwrap();
        slots.retain(|s| s.strong_count() != 0);
        if slots.len() >= SUBSCRIBERS {
            return Err("Roots subscriber limit reached".into());
        }
        let (wake, ready) = UnixStream::pair()?;
        wake.set_nonblocking(true)?;
        let slot = Arc::new(Slot {
            branch,
            digest,
            source,
            queue: Mutex::new(Queue {
                wake: Some(wake),
                messages: VecDeque::new(),
            }),
        });
        slots.push(Arc::downgrade(&slot));
        self.active.store(true, Ordering::Relaxed);
        Ok(Subscription { slot, ready })
    }

    pub fn refresh(&self, trie: &trie_trunk_taxonomy::Trunk) {
        if !self.active.load(Ordering::Relaxed) {
            return;
        }
        let mut slots = self.slots.lock().unwrap();
        slots.retain(|weak| {
            let Some(slot) = weak.upgrade() else {
                return false;
            };
            if trie.route(slot.branch).is_ok_and(|r| {
                r.digest == slot.digest && r.source().ok().as_ref() == Some(&slot.source)
            }) {
                return true;
            }
            slot.queue.lock().unwrap().wake.take();
            true
        });
        self.active.store(!slots.is_empty(), Ordering::Relaxed);
    }

    // The sender never performs network I/O. On overflow the receiver drains
    // its ordered prefix and sees EOF, so a dropped update cannot go unnoticed.
    pub fn publish(&self, branch: u16, data: &[u8], single: bool) {
        let mut message = None;
        if !self.active.load(Ordering::Relaxed) {
            return;
        }
        let mut slots = self.slots.lock().unwrap();
        slots.retain(|weak| {
            let Some(slot) = weak.upgrade() else {
                return false;
            };
            if slot.branch != branch {
                return true;
            }
            let mut queue = slot.queue.lock().unwrap();
            if queue.wake.is_none() {
                return true;
            }
            if queue.messages.len() < QUEUE {
                let message = message.get_or_insert_with(|| {
                    let len = data.len() + if single { 4 } else { 0 };
                    let lease = self.budget.reserve(len).ok()?;
                    let node_lease = self.node_budget.reserve(len).ok()?;
                    let mut bytes = Vec::with_capacity(len);
                    if single {
                        bytes.extend_from_slice(&(data.len() as u32).to_be_bytes());
                    }
                    bytes.extend_from_slice(data);
                    Some(Arc::new(Message {
                        bytes,
                        _lease: lease,
                        _node_lease: node_lease,
                    }))
                });
                if let Some(message) = message {
                    if queue
                        .wake
                        .as_mut()
                        .is_some_and(|wake| wake.write(&[1]).ok() == Some(1))
                    {
                        queue.messages.push_back(message.clone());
                        return true;
                    }
                }
            }
            queue.wake.take();
            true
        });
        self.active.store(!slots.is_empty(), Ordering::Relaxed);
    }
}

impl Node {
    pub fn subscribe(&self, branch: u16, digest: &str, source: &str) -> Result<Subscription> {
        let _writer = self.runtime.lock().map_err(|_| "Trunk writer poisoned")?;
        let route = self.trie().route(branch)?;
        if route.digest != digest || route.source()? != source {
            return Err("Subscription Definition changed".into());
        }
        route.storage()?;
        self.roots
            .subscribe(branch, route.digest, source.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn fanout_charges_once_and_a_gap_closes_the_stream() {
        let budget = MemoryBudget::new(32);
        let roots = Roots::new(budget.clone());
        let first = roots.subscribe(1, "d".into(), "src".into()).unwrap();
        let second = roots.subscribe(1, "d".into(), "src".into()).unwrap();
        roots.publish(1, &[7; 4], true);
        assert_eq!(budget.used(), 8);
        roots.publish(1, &[8], true); // No room for the second frame.
        for sub in [&first, &second] {
            let mut signals = Vec::new();
            sub.readiness().unwrap().read_to_end(&mut signals).unwrap();
            assert_eq!(signals, [1]);
            assert_eq!(sub.next().unwrap().bytes, [0, 0, 0, 4, 7, 7, 7, 7]);
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn lagging_receiver_gets_a_prefix_and_eof() {
        let roots = Roots::new(MemoryBudget::new(4096));
        let slow = roots.subscribe(1, "a".into(), "a".into()).unwrap();
        let fast = roots.subscribe(1, "a".into(), "a".into()).unwrap();
        let other = roots.subscribe(2, "b".into(), "b".into()).unwrap();
        let mut ready = fast.readiness().unwrap();
        for i in 0..QUEUE + 2 {
            roots.publish(1, &[i as u8], true);
            ready.read_exact(&mut [0]).unwrap();
            assert_eq!(fast.next().unwrap().bytes, [0, 0, 0, 1, i as u8]);
        }
        let mut signals = Vec::new();
        slow.readiness().unwrap().read_to_end(&mut signals).unwrap();
        assert_eq!(signals.len(), QUEUE);
        for i in 0..QUEUE {
            assert_eq!(slow.next().unwrap().bytes[4], i as u8);
        }
        assert!(other.next().is_err());
        assert_eq!(roots.budget.used(), 0);
    }
}
