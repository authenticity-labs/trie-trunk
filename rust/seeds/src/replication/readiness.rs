//! Reader readiness is independent of disk publication. One node owner admits
//! collection, pins immutable Packs for readers, and evicts only durable data.
use crate::{Error, MemoryBudget, MemoryLease, SeedPackSnapshot, error};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Queued,
    Ready,
}
struct Entry {
    reservation: Option<MemoryLease>,
    claim: u64,
    pack: Option<Arc<SeedPackSnapshot>>,
    durable: bool,
    submitted: bool,
    touched: u64,
}
#[derive(Clone)]
pub struct Readiness(Arc<Mutex<Owner>>);
struct Owner {
    budget: MemoryBudget,
    entries: HashMap<(String, u64), Entry>,
    duplicates: HashMap<(String, u64, u64), MemoryLease>,
    clock: u64,
}
impl Readiness {
    pub fn new(budget: MemoryBudget) -> Self {
        Self(Arc::new(Mutex::new(Owner {
            budget,
            entries: HashMap::new(),
            duplicates: HashMap::new(),
            clock: 0,
        })))
    }
    pub fn queue(&self, source: &str, address: u64) -> Result<State, Error> {
        let mut owner = self
            .0
            .lock()
            .map_err(|_| error("readiness owner poisoned"))?;
        let entry = owner
            .entries
            .entry((source.into(), address))
            .or_insert(Entry {
                reservation: None,
                claim: 0,
                pack: None,
                durable: false,
                submitted: false,
                touched: 0,
            });
        Ok(if entry.pack.is_some() {
            State::Ready
        } else {
            State::Queued
        })
    }
    /// Reserve before receiving/decoding the next Pack. A false result means
    /// queued for capacity, not failed. No payload is allocated by the queue.
    pub fn admit(
        &self,
        source: &str,
        address: u64,
        maximum: usize,
        claim: u64,
    ) -> Result<bool, Error> {
        let mut owner = self
            .0
            .lock()
            .map_err(|_| error("readiness owner poisoned"))?;
        if maximum == 0 || maximum > owner.budget.limit() {
            return Err(error("Pack admission exceeds resident quota"));
        }
        let key = (source.into(), address);
        let duplicate = (source.into(), address, claim);
        if owner.duplicates.contains_key(&duplicate) {
            return Ok(true);
        }
        if let Some(e) = owner.entries.get(&key) {
            if e.reservation.is_some() {
                return Ok(e.claim == claim);
            }
        }
        owner.entries.entry(key.clone()).or_insert(Entry {
            reservation: None,
            claim: 0,
            pack: None,
            durable: false,
            submitted: false,
            touched: 0,
        });
        if owner.entries[&key].pack.is_none() {
            owner.entries.get_mut(&key).unwrap().claim = claim;
        }
        while owner.budget.limit() - owner.budget.used() < maximum {
            let victim = owner
                .entries
                .iter()
                .filter(|(k, e)| {
                    e.durable
                        && !owner
                            .duplicates
                            .keys()
                            .any(|(s, a, _)| s == &k.0 && a == &k.1)
                        && e.pack.as_ref().is_some_and(|p| {
                            Arc::strong_count(p) == 1
                                && p.memory.as_ref().is_none_or(|m| Arc::strong_count(m) == 1)
                        })
                })
                .min_by_key(|(_, e)| e.touched)
                .map(|(k, _)| k.clone());
            let Some(victim) = victim else {
                return Ok(false);
            };
            owner.entries.remove(&victim);
        }
        // Other readers share this budget and may reserve concurrently.
        let Ok(lease) = owner.budget.reserve(maximum) else {
            return Ok(false);
        };
        if owner.entries.get(&key).is_some_and(|e| e.pack.is_some()) {
            owner.duplicates.insert(duplicate, lease);
            return Ok(true);
        }
        // Eviction may have retired this same address before recollection.
        let entry = owner.entries.entry(key).or_insert(Entry {
            reservation: None,
            claim,
            pack: None,
            durable: false,
            submitted: false,
            touched: 0,
        });
        entry.reservation = Some(lease);
        entry.claim = claim;
        Ok(true)
    }
    pub fn load(
        &self,
        source: &str,
        address: u64,
        pack: Arc<SeedPackSnapshot>,
        claim: u64,
    ) -> Result<(), Error> {
        if !pack.is_sealed() {
            return Err(error("only sealed Packs enter reader readiness"));
        }
        let mut owner = self
            .0
            .lock()
            .map_err(|_| error("readiness owner poisoned"))?;
        owner.clock = owner.clock.wrapping_add(1);
        let stamp = owner.clock;
        let duplicate = owner.duplicates.remove(&(source.into(), address, claim));
        let entry = owner
            .entries
            .get_mut(&(source.into(), address))
            .ok_or_else(|| error("Pack is not queued"))?;
        if let Some(existing) = &entry.pack {
            if duplicate.is_none() {
                return Err(error("duplicate Pack has no memory admission"));
            }
            if existing.stored_bytes() != pack.stored_bytes() {
                return Err(error("immutable Ready Pack changed"));
            }
            return Ok(());
        }
        if entry.claim != claim {
            return Err(error("Pack admission belongs to another collector"));
        }
        let mut lease = entry
            .reservation
            .take()
            .ok_or_else(|| error("Pack has no memory admission"))?;
        if let Err(e) = lease.shrink(pack.retained_bytes()) {
            entry.reservation = Some(lease);
            return Err(e);
        }
        entry.pack = Some(pack.with_memory(Arc::new(lease)));
        entry.touched = stamp;
        Ok(())
    }
    pub fn get(&self, source: &str, address: u64) -> Result<Option<Arc<SeedPackSnapshot>>, Error> {
        let mut owner = self
            .0
            .lock()
            .map_err(|_| error("readiness owner poisoned"))?;
        owner.clock = owner.clock.wrapping_add(1);
        let stamp = owner.clock;
        Ok(owner
            .entries
            .get_mut(&(source.into(), address))
            .and_then(|e| {
                e.touched = stamp;
                e.pack.clone()
            }))
    }
    pub fn state(&self, source: &str, address: u64) -> Result<Option<State>, Error> {
        let owner = self
            .0
            .lock()
            .map_err(|_| error("readiness owner poisoned"))?;
        Ok(owner.entries.get(&(source.into(), address)).map(|e| {
            if e.pack.is_some() {
                State::Ready
            } else {
                State::Queued
            }
        }))
    }
    /// Transient availability for planning. Never persist these bits: only the
    /// publication transaction can update durable Time coverage.
    pub fn coverage(&self, source: &str, address: u64, flags: u8) -> Result<u8, Error> {
        if self.state(source, address)? != Some(State::Ready) {
            return Ok(0);
        }
        Ok(crate::TimeTrie::collected_flags(flags)
            .map(|ready| ready & !flags)
            .unwrap_or(0))
    }
    pub fn published(&self, source: &str, addresses: &[u64]) -> Result<(), Error> {
        let mut owner = self
            .0
            .lock()
            .map_err(|_| error("readiness owner poisoned"))?;
        for address in addresses {
            if !owner
                .entries
                .get(&(source.into(), *address))
                .is_some_and(|e| e.pack.is_some())
            {
                return Err(error("published Pack is not Ready"));
            }
        }
        for address in addresses {
            owner
                .entries
                .get_mut(&(source.into(), *address))
                .unwrap()
                .durable = true;
        }
        Ok(())
    }
    pub fn submitted(&self, source: &str, address: u64) -> Result<(), Error> {
        let mut owner = self
            .0
            .lock()
            .map_err(|_| error("readiness owner poisoned"))?;
        let entry = owner
            .entries
            .get_mut(&(source.into(), address))
            .filter(|e| e.pack.is_some())
            .ok_or_else(|| error("Pack is not Ready"))?;
        entry.submitted = true;
        Ok(())
    }
    pub fn discard(&self, source: &str, address: u64, claim: u64) -> Result<(), Error> {
        let mut owner = self
            .0
            .lock()
            .map_err(|_| error("readiness owner poisoned"))?;
        let key = (source.into(), address);
        owner.duplicates.remove(&(source.into(), address, claim));
        if owner
            .entries
            .get(&key)
            .is_some_and(|e| e.claim == claim && !e.submitted && !e.durable)
        {
            owner.entries.remove(&key);
        }
        Ok(())
    }
    /// Cancellation releases only incomplete admissions. A failed publication
    /// must retain Ready data until retry or explicit node reset.
    pub fn cancel(&self, source: &str, claim: u64) -> Result<(), Error> {
        let mut owner = self
            .0
            .lock()
            .map_err(|_| error("readiness owner poisoned"))?;
        owner
            .entries
            .retain(|(s, _), e| s != source || e.claim != claim || e.pack.is_some());
        owner
            .duplicates
            .retain(|(s, _, c), _| s != source || *c != claim);
        Ok(())
    }
    pub fn clear(&self, source: &str) -> Result<(), Error> {
        let mut owner = self
            .0
            .lock()
            .map_err(|_| error("readiness owner poisoned"))?;
        owner.entries.retain(|(s, _), _| s != source);
        owner.duplicates.retain(|(s, _, _), _| s != source);
        Ok(())
    }
    pub fn used(&self) -> Result<usize, Error> {
        Ok(self
            .0
            .lock()
            .map_err(|_| error("readiness owner poisoned"))?
            .budget
            .used())
    }
}
