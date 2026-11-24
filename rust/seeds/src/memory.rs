//! Shared admission for explicitly charged allocations. This is not an RSS
//! meter: callers must reserve before allocation and retain the lease with it.
use crate::{Error, error};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

#[derive(Clone, Debug)]
pub struct MemoryBudget(Arc<State>);
#[derive(Debug)]
struct State { limit: usize, used: AtomicUsize }
#[derive(Debug)]
pub struct MemoryLease { budget: Option<MemoryBudget>, bytes: usize }
impl MemoryBudget {
    pub fn new(limit: usize) -> Self {
        Self(Arc::new(State { limit, used: AtomicUsize::new(0) }))
    }
    pub fn used(&self) -> usize { self.0.used.load(Ordering::Relaxed) }
    pub fn limit(&self) -> usize { self.0.limit }
    pub fn reserve(&self, bytes: usize) -> Result<MemoryLease, Error> {
        if bytes == 0 { return Ok(MemoryLease { budget: None, bytes: 0 }); }
        self.0.used.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
            used.checked_add(bytes).filter(|next| *next <= self.0.limit)
        }).map_err(|_| error("memory budget exhausted"))?;
        Ok(MemoryLease { budget: Some(self.clone()), bytes })
    }
}
impl Drop for MemoryLease {
    fn drop(&mut self) {
        if let Some(budget) = &self.budget { budget.0.used.fetch_sub(self.bytes, Ordering::Relaxed); }
    }
}
impl MemoryLease {
    pub(crate) fn shrink(&mut self, bytes: usize) -> Result<(), Error> {
        if bytes > self.bytes { return Err(error("allocation exceeds admitted memory")); }
        if let Some(budget) = &self.budget { budget.0.used.fetch_sub(self.bytes - bytes, Ordering::Relaxed); }
        self.bytes = bytes;
        Ok(())
    }
    pub(crate) fn belongs_to(&self, budget: &MemoryBudget) -> bool {
        self.budget.as_ref().is_none_or(|owner| Arc::ptr_eq(&owner.0, &budget.0))
    }
}
