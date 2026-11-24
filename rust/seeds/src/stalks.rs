//! Owned in-memory columns. A Stalk never borrows a source Pack's storage.
//! Admission precedes allocation; dropping a column returns its budget lease.
use crate::{Error, MemoryBudget, MemoryLease, error};
mod data;
pub use data::{Bytes, Chunk, Field, Values, State, Stalks, Growth};

pub struct Column<T: Copy> {
    values: Vec<T>,
    _memory: MemoryLease,
}
impl<T: Copy> Column<T> {
    pub fn new(capacity: usize, budget: &MemoryBudget) -> Result<Self, Error> {
        let bytes = capacity.checked_mul(std::mem::size_of::<T>()).ok_or(Error::Overflow)?;
        let memory = budget.reserve(bytes)?;
        let mut values = Vec::new();
        values.try_reserve_exact(capacity).map_err(|_| error("column allocation failed"))?;
        if values.capacity() != capacity && std::mem::size_of::<T>() != 0 {
            return Err(error("column allocation exceeded reservation"));
        }
        Ok(Self { values, _memory: memory })
    }
    pub fn push(&mut self, value: T) -> Result<(), Error> {
        if self.values.len() == self.values.capacity() { return Err(error("column capacity exceeded")); }
        self.values.push(value); Ok(())
    }
    pub fn values(&self) -> &[T] { &self.values }
    pub fn len(&self) -> usize { self.values.len() }
    pub fn is_empty(&self) -> bool { self.values.is_empty() }
    pub fn bytes(&self) -> usize { self.values.capacity()*std::mem::size_of::<T>() }
}
