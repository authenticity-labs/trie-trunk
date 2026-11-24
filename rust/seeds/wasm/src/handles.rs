//! Reusable opaque handles. A released token cannot address a replacement.
//! Exhausted generations retire the slot instead of wrapping into an old token.
const INDEX_BITS: usize = 20;
const INDEX_MASK: usize = (1 << INDEX_BITS) - 1;
const MAX_GENERATION: u32 = (1 << (32 - INDEX_BITS)) - 1;
struct Slot<T> { generation: u32, value: Option<T> }
pub struct Handles<T> { slots: Vec<Slot<T>>, free: Vec<usize> }
impl<T> Default for Handles<T> {
    fn default() -> Self { Self { slots: Vec::new(), free: Vec::new() } }
}
impl<T> Handles<T> {
    pub fn insert(&mut self, value: T) -> Result<usize, &'static str> {
        let index = if let Some(index) = self.free.pop() {
            self.slots[index].value = Some(value); index
        } else {
            // Exclude the all-ones index, so no token equals the ABI error value.
            if self.slots.len() >= INDEX_MASK { return Err("handle capacity exhausted"); }
            let index = self.slots.len();
            self.slots.push(Slot { generation: 0, value: Some(value) }); index
        };
        Ok(index | ((self.slots[index].generation as usize) << INDEX_BITS))
    }
    pub fn get(&self, token: usize) -> Option<&T> {
        if token > u32::MAX as usize { return None; }
        let slot = self.slots.get(token & INDEX_MASK)?;
        (slot.generation == (token >> INDEX_BITS) as u32).then_some(slot.value.as_ref()).flatten()
    }
    pub fn get_mut(&mut self, token: usize) -> Option<&mut T> {
        if token > u32::MAX as usize { return None; }
        let slot = self.slots.get_mut(token & INDEX_MASK)?;
        (slot.generation == (token >> INDEX_BITS) as u32).then_some(slot.value.as_mut()).flatten()
    }
    pub fn remove(&mut self, token: usize) {
        self.take(token);
    }
    pub fn take(&mut self, token: usize)->Option<T> {
        if token > u32::MAX as usize { return None; }
        let index = token & INDEX_MASK;
        let slot = self.slots.get_mut(index)?;
        if slot.generation != (token >> INDEX_BITS) as u32 { return None; }
        let value=slot.value.take()?;
        if slot.generation < MAX_GENERATION {
            slot.generation += 1; self.free.push(index);
        }
        Some(value)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reuse_rejects_stale_access_and_stale_release() {
        let mut table = Handles::default();
        let old = table.insert(10).unwrap(); table.remove(old);
        let new = table.insert(20).unwrap();
        assert_eq!(table.slots.len(),1); assert_ne!(old,new);
        assert!(table.get(old).is_none()); table.remove(old);
        assert_eq!(table.get(new),Some(&20));
        *table.get_mut(new).unwrap()=30; assert_eq!(table.get(new),Some(&30));
    }
    #[test]
    fn generation_exhaustion_never_revives_a_token() {
        let mut table = Handles::default();
        let first = table.insert(0).unwrap(); table.remove(first);
        for i in 1..=MAX_GENERATION { let id=table.insert(i).unwrap(); assert_ne!(id,first); table.remove(id); }
        let next=table.insert(7).unwrap(); assert_eq!(table.slots.len(),2);
        assert!(table.get(first).is_none()); assert_eq!(table.get(next),Some(&7));
    }
}
