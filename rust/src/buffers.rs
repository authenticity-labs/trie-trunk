//! Native byte ownership for the Go adapter. Buffers are mutable only while
//! uniquely owned. After sharing, Go may append only to an exclusively leased
//! unused suffix; published spans are immutable. Pins outlive readers.
use std::{
    cell::UnsafeCell,
    ffi::c_void,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
struct Pool {
    budget: Option<data_seeds::MemoryBudget>,
    limit: u64,
    used: AtomicU64,
    peak: AtomicU64,
}
impl Pool {
    fn reserve(&self, n: u64) -> bool {
        let mut old = self.used.load(Ordering::Relaxed);
        loop {
            let Some(next) = old.checked_add(n) else {
                return false;
            };
            if next > self.limit {
                return false;
            }
            match self
                .used
                .compare_exchange_weak(old, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => {
                    self.peak.fetch_max(next, Ordering::Relaxed);
                    return true;
                }
                Err(v) => old = v,
            }
        }
    }
}
struct Buffer {
    charge: Option<data_seeds::MemoryLease>,
    pool: Arc<Pool>,
    bytes: UnsafeCell<Box<[u8]>>,
    // Completed codec output transfers its Vec allocation without shrinking or
    // copying it. Such buffers are immutable, including unused Vec capacity.
    retained: Option<Vec<u8>>,
}
// SAFETY: the adapter serializes each allocation's writer lease. Sharing freezes
// allocation size and all published spans; only the disjoint unused suffix can
// be written. Readers never inspect that suffix. Release follows the last pin.
unsafe impl Sync for Buffer {}
impl Drop for Buffer {
    fn drop(&mut self) {
        self.pool
            .used
            .fetch_sub(self.retained.as_ref().map_or(self.bytes.get_mut().len(), Vec::capacity) as u64, Ordering::AcqRel);
    }
}
struct Arena {
    buffers: Mutex<Vec<Arc<Buffer>>>,
}
// Raw handles are boxed Arc values. Cloning creates a new independently owned
// handle. Go serializes mutation of each handle; pins can be dropped concurrently.
#[unsafe(no_mangle)]
pub extern "C" fn trunk_memory_new(limit: u64) -> *mut c_void {
    Box::into_raw(Box::new(Arc::new(Pool {
        budget: None,
        limit,
        used: AtomicU64::new(0),
        peak: AtomicU64::new(0),
    })))
    .cast()
}
pub(crate) fn shared_memory(budget:data_seeds::MemoryBudget)->*mut c_void {
    Box::into_raw(Box::new(Arc::new(Pool{limit:budget.limit() as u64,budget:Some(budget),used:AtomicU64::new(0),peak:AtomicU64::new(0)}))).cast()
}
fn charge(pool:&Pool,n:usize)->Result<Option<data_seeds::MemoryLease>,()> {
    pool.budget.as_ref().map(|b|b.reserve(n)).transpose().map_err(|_|())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_memory_free(p: *mut c_void) {
    if !p.is_null() {
        drop(unsafe { Box::from_raw(p.cast::<Arc<Pool>>()) })
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_memory_used(p: *const c_void) -> u64 {
    unsafe { &*p.cast::<Arc<Pool>>() }
        .used
        .load(Ordering::Acquire)
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_memory_peak(p: *const c_void) -> u64 {
    unsafe { &*p.cast::<Arc<Pool>>() }
        .peak
        .load(Ordering::Acquire)
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_buffer_new(p: *const c_void) -> *mut c_void {
    let pool = unsafe { &*p.cast::<Arc<Pool>>() }.clone();
    Box::into_raw(Box::new(Arc::new(Buffer {
        charge: None,
        pool,
        bytes: UnsafeCell::new(Box::new([])),
        retained: None,
    })))
    .cast()
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_buffer_resize(p: *mut c_void, len: usize) -> *mut u8 {
    let handle = unsafe { &mut *p.cast::<Arc<Buffer>>() };
    let Some(b) = Arc::get_mut(handle) else {
        return std::ptr::null_mut();
    };
    if b.retained.is_some() { return std::ptr::null_mut(); }
    let current = b.bytes.get_mut();
    if len <= current.len() {
        return current.as_mut_ptr();
    }
    if !b.pool.reserve(len as u64) {
        return std::ptr::null_mut();
    }
    let Ok(lease)=charge(&b.pool,len) else {
        b.pool.used.fetch_sub(len as u64,Ordering::AcqRel);
        return std::ptr::null_mut();
    };
    let mut bytes = Vec::new();
    if bytes.try_reserve_exact(len).is_err() {
        b.pool.used.fetch_sub(len as u64, Ordering::AcqRel);
        return std::ptr::null_mut();
    }
    bytes.resize(len, 0);
    bytes[..current.len()].copy_from_slice(current);
    b.pool
        .used
        .fetch_sub(current.len() as u64, Ordering::AcqRel);
    *current = bytes.into_boxed_slice();
    b.charge=lease;
    current.as_mut_ptr()
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_buffer_free(p: *mut c_void) {
    if !p.is_null() {
        drop(unsafe { Box::from_raw(p.cast::<Arc<Buffer>>()) })
    }
}
#[unsafe(no_mangle)]
pub extern "C" fn trunk_arena_new() -> *mut c_void {
    Box::into_raw(Box::new(Arc::new(Arena {
        buffers: Mutex::new(Vec::new()),
    })))
    .cast()
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_arena_pin(p: *const c_void) -> *mut c_void {
    Box::into_raw(Box::new(unsafe { &*p.cast::<Arc<Arena>>() }.clone())).cast()
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_arena_free(p: *mut c_void) {
    if !p.is_null() {
        drop(unsafe { Box::from_raw(p.cast::<Arc<Arena>>()) })
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_arena_adopt(a: *const c_void, b: *const c_void) -> bool {
    let arena = unsafe { &*a.cast::<Arc<Arena>>() };
    let Ok(mut buffers) = arena.buffers.lock() else {
        return false;
    };
    if buffers.try_reserve(1).is_err() {
        return false;
    }
    buffers.push(unsafe { &*b.cast::<Arc<Buffer>>() }.clone());
    true
}

// The returned handle owns the final compressed allocation. The Go adapter
// borrows a slice and may transfer ownership to an hour arena without a copy.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_buffer_seal(pool:*const c_void,input:*const u8,length:usize,algorithm:u32,level:u32)->*mut c_void {
    if pool.is_null() || input.is_null() || length==0 { return std::ptr::null_mut(); }
    let pool=unsafe { &*pool.cast::<Arc<Pool>>() }.clone();
    let raw=unsafe { std::slice::from_raw_parts(input,length) };
    let Ok(algorithm)=crate::seed_native::compression_algorithm(algorithm) else {return std::ptr::null_mut()};
    let mut output=Vec::new();
    if pil_codec::compression::seal(raw,algorithm,level,false,&mut output).is_err() || !pool.reserve(output.len() as u64) {return std::ptr::null_mut();}
    let Ok(lease)=charge(&pool,output.len()) else {pool.used.fetch_sub(output.len() as u64,Ordering::AcqRel);return std::ptr::null_mut()};
    Box::into_raw(Box::new(Arc::new(Buffer{pool,bytes:UnsafeCell::new(output.into_boxed_slice()),retained:None,charge:lease}))).cast()
}
/// Preserve the codec allocation and charge its retained capacity to the same
/// pool as ingress buffers. Failure returns ownership to the caller untouched.
pub(crate) unsafe fn adopt_output(pool: *const c_void, bytes: Vec<u8>) -> Result<*mut c_void, Vec<u8>> {
    if pool.is_null() { return Err(bytes); }
    let pool = unsafe { &*pool.cast::<Arc<Pool>>() }.clone();
    if !pool.reserve(bytes.capacity() as u64) { return Err(bytes); }
    let Ok(lease)=charge(&pool,bytes.capacity()) else {pool.used.fetch_sub(bytes.capacity() as u64,Ordering::AcqRel);return Err(bytes)};
    Ok(Box::into_raw(Box::new(Arc::new(Buffer {
        pool, bytes: UnsafeCell::new(Box::new([])), retained: Some(bytes), charge:lease,
    }))).cast())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_buffer_data(p:*const c_void)->*mut u8 {
    let b=unsafe { &*p.cast::<Arc<Buffer>>() };
    if let Some(bytes) = &b.retained { return bytes.as_ptr().cast_mut(); }
    unsafe { (&*b.bytes.get()).as_ptr().cast_mut() }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_buffer_len(p:*const c_void)->usize {
    let b=unsafe { &*p.cast::<Arc<Buffer>>() };
    if let Some(bytes) = &b.retained { return bytes.len(); }
    unsafe { (&*b.bytes.get()).len() }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn transport_and_node_allocations_share_admission() {
        let budget=data_seeds::MemoryBudget::new(128);
        let node=budget.reserve(96).unwrap();
        unsafe {
            let pool=shared_memory(budget.clone());
            let buffer=trunk_buffer_new(pool);
            assert!(trunk_buffer_resize(buffer,64).is_null());
            assert_eq!(budget.used(),96);
            drop(node);
            assert!(!trunk_buffer_resize(buffer,64).is_null());
            assert_eq!(budget.used(),64);
            let arena=trunk_arena_new();
            assert!(trunk_arena_adopt(arena,buffer));
            trunk_buffer_free(buffer);
            trunk_memory_free(pool);
            assert_eq!(budget.used(),64);
            trunk_arena_free(arena);
            assert_eq!(budget.used(),0);
        }
    }
    #[test]
    fn append_only_suffix_survives_pool_and_other_hour_release() {
        unsafe {
            let pool = trunk_memory_new(256);
            let buffer = trunk_buffer_new(pool);
            let bytes = trunk_buffer_resize(buffer, 128);
            *bytes = 41;
            let first = trunk_arena_new();
            let second = trunk_arena_new();
            assert!(trunk_arena_adopt(first, buffer));
            // Publishing a prefix does not forbid a disjoint suffix write.
            *bytes.add(64) = 42;
            assert!(trunk_arena_adopt(second, buffer));
            assert!(trunk_buffer_resize(buffer, 256).is_null());
            trunk_buffer_free(buffer);
            trunk_arena_free(first);
            trunk_memory_free(pool);
            assert_eq!(*bytes, 41);
            assert_eq!(*bytes.add(64), 42);
            trunk_arena_free(second);
        }
    }
    #[test]
    fn small_packs_use_the_codec_header_limit() {
        let document = data_seeds::doc::new("empty-pack")
            .field(data_seeds::doc::Field::new("value").r#type("uint").size(1))
            .sign().unwrap();
        let definition = data_seeds::SeedDefinition::prescribe(&document).unwrap();
        let mut pack = definition.fill(0).unwrap();
        pack.seal().unwrap();
        let plain = pack.into_bytes().unwrap();
        assert!(plain.len() < 16);
        unsafe {
            let pool = trunk_memory_new(4096);
            let output = trunk_buffer_seal(pool, plain.as_ptr(), plain.len(), 1, 1);
            assert!(!output.is_null());
            let compressed = std::slice::from_raw_parts(trunk_buffer_data(output), trunk_buffer_len(output));
            let mut expanded = Vec::new();
            assert!(pil_codec::compression::unseal(compressed, &mut expanded).unwrap());
            assert_eq!(expanded, plain);
            trunk_buffer_free(output);
            assert_eq!(trunk_memory_used(pool), 0);
            trunk_memory_free(pool);
        }
    }
    #[test]
    fn pins_budget_and_freezing() {
        unsafe {
            let p = trunk_memory_new(512);
            let b = trunk_buffer_new(p);
            let ptr = trunk_buffer_resize(b, 128);
            assert!(!ptr.is_null());
            *ptr = 42;
            let a = trunk_arena_new();
            assert!(trunk_arena_adopt(a, b));
            assert!(trunk_buffer_resize(b, 256).is_null());
            trunk_buffer_free(b);
            let pin = trunk_arena_pin(a);
            trunk_arena_free(a);
            assert_eq!(*ptr, 42);
            assert_eq!(trunk_memory_used(p), 128);
            trunk_arena_free(pin);
            assert_eq!(trunk_memory_used(p), 0);
            let b = trunk_buffer_new(p);
            assert!(trunk_buffer_resize(b, 513).is_null());
            assert_eq!(trunk_memory_used(p), 0);
            trunk_buffer_free(b);
            trunk_memory_free(p);
        }
    }
}
