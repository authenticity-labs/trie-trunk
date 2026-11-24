//! Native ownership adapter. Host code serializes calls and owns file handles;
//! the shared Seed library owns PIL admission, Pack construction and recovery.
use super::*;
use data_seeds::registration::ingress::OwnedIngress;
use data_seeds::registration::{IngressLimits, SealedHour};

type Owner = OwnedIngress;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_inspect(
    handle: *mut c_void,
    branch: u16,
    pill: SeedSlice,
    now: u64,
) -> *mut c_void {
    output((|| {
        let time = unsafe { owner(handle)? }.inspect(
            branch,
            unsafe { input(pill.data, pill.len)? },
            now,
        )?;
        serde_json::to_vec(&time).map_err(|e| invalid(e.to_string()))
    })())
}

#[cfg(unix)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_persist(
    handle: *mut c_void,
    branch: u16,
    pill: SeedSlice,
    now: u64,
    fd: i32,
) -> *mut c_void {
    use std::os::fd::FromRawFd;
    output((|| {
        if fd < 0 {
            return Err(invalid("invalid recovery file descriptor"));
        }
        // The transport retains its open file and writer lock across this
        // synchronous call. Borrow its descriptor without dup/close or a copy.
        let mut spool = std::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(fd) });
        let time = data_seeds::registration::ingress::files::ingest(
            unsafe { owner(handle)? },
            &mut spool,
            branch,
            unsafe { input(pill.data, pill.len)? },
            now,
        )?;
        serde_json::to_vec(&time).map_err(|e| invalid(e.to_string()))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_storage(name: SeedSlice) -> *mut c_void {
    output((|| {
        let name = std::str::from_utf8(unsafe { input(name.data, name.len)? })
            .map_err(|e| invalid(e.to_string()))?;
        let storage: data_seeds::registration::Storage =
            serde_json::from_value(serde_json::json!(name)).map_err(|e| invalid(e.to_string()))?;
        serde_json::to_vec(&serde_json::json!({"suffix":storage.suffix(), "alternative":storage.alternative().suffix(), "packed":storage == data_seeds::registration::Storage::Pack})).map_err(|e| invalid(e.to_string()))
    })())
}
struct Handle {
    value: Result<Owner, String>,
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_enabled(
    trunk: *const c_void,
    branch: u16,
) -> *mut c_void {
    output((|| {
        let trunk = unsafe { registered_trunk(trunk)? };
        let route = trunk.trie().route(branch).map_err(invalid)?;
        let definition = trunk.definition(route.definition)?;
        let enabled = (0..definition.prescription().definition().fields().len()).any(|i| {
            definition
                .annotations(i)
                .unwrap_or_default()
                .iter()
                .any(|a| a == "@time" || a.starts_with("@time("))
        });
        if enabled {
            data_seeds::IndexFields::from_definition(&definition)?;
        }
        Ok(if enabled {
            b"true".to_vec()
        } else {
            b"false".to_vec()
        })
    })())
}
unsafe fn owner<'a>(handle: *mut c_void) -> Result<&'a mut Owner, Error> {
    if handle.is_null() {
        return Err(invalid("missing ingress handle"));
    }
    let owner = unsafe { &mut *handle.cast::<Handle>() }
        .value
        .as_mut()
        .map_err(|e| invalid(e.as_str()))?;
    owner.healthy()?;
    Ok(owner)
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_open(
    trunk: *const c_void,
    branch: u16,
    bytes: usize,
    pill_bytes: usize,
    through: u64,
) -> *mut c_void {
    let value = (|| {
        if trunk.is_null() {
            return Err(invalid("missing registered trunk"));
        }
        let trunk = unsafe { registered_trunk(trunk)? };
        let mut owner = Owner::new(
            trunk.trie().branches().select([branch]).packs()?,
            branch,
            IngressLimits {
                bytes,
                pill_bytes,
                ..Default::default()
            },
            through,
        )?;
        owner.configure_summaries(&trunk, data_seeds::summaries::SummaryCalendar::new(0)?, &[])?;
        Ok(owner)
    })()
    .map_err(|e: Error| e.to_string());
    Box::into_raw(Box::new(Handle { value })).cast()
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_error(handle: *const c_void) -> SeedSlice {
    let error = unsafe { &*handle.cast::<Handle>() }
        .value
        .as_ref()
        .err()
        .map_or("", String::as_str);
    SeedSlice {
        data: error.as_ptr(),
        len: error.len(),
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_free(handle: *mut c_void) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle.cast::<Handle>()) });
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_load(
    handle: *mut c_void,
    branch: u16,
    pill: SeedSlice,
    now: u64,
    recovery: bool,
) -> *mut c_void {
    output((|| {
        let owner = unsafe { owner(handle)? };
        let pill = unsafe { input(pill.data, pill.len)? };
        let time = if recovery {
            owner.recover(branch, pill)?
        } else {
            Some(owner.load(branch, pill, now)?)
        };
        serde_json::to_vec(&time).map_err(|e| invalid(e.to_string()))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_snapshot(
    handle: *mut c_void,
    hour: u64,
) -> *mut c_void {
    output(unsafe { owner(handle) }.and_then(|o| o.snapshot(hour)))
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_hours(handle: *mut c_void) -> *mut c_void {
    output(unsafe { owner(handle) }.and_then(|o| {
        serde_json::to_vec(&o.hours().collect::<Vec<_>>()).map_err(|e| invalid(e.to_string()))
    }))
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_commits(handle: *mut c_void, now: u64) -> *mut c_void {
    output((|| {
        let plan = unsafe { owner(handle)? }.commits(now)?;
        serde_json::to_vec(&plan).map_err(|e| invalid(e.to_string()))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_published(
    handle: *mut c_void,
    hour: u64,
    through: u64,
) -> *mut c_void {
    output((|| {
        let live = unsafe { owner(handle)? };
        if hour < through && live.count(hour) > 0 {
            live.published(hour)?;
        }
        live.advance(through)?;
        Ok(Vec::new())
    })())
}
struct Sealing {
    value: Result<SealedHour, String>,
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_seal(
    handle: *mut c_void,
    hour: u64,
    now: u64,
) -> *mut c_void {
    let value = unsafe { owner(handle) }
        .and_then(|o| o.seal(hour, now))
        .map_err(|e| e.to_string());
    Box::into_raw(Box::new(Sealing { value })).cast()
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_seal_error(handle: *const c_void) -> SeedSlice {
    let error = unsafe { &*handle.cast::<Sealing>() }
        .value
        .as_ref()
        .err()
        .map_or("", String::as_str);
    SeedSlice {
        data: error.as_ptr(),
        len: error.len(),
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_seal_bytes(
    handle: *const c_void,
    index: bool,
) -> SeedSlice {
    let value = unsafe { &*handle.cast::<Sealing>() }.value.as_ref().ok();
    let bytes = value
        .map(|v| {
            if index {
                v.index.as_slice()
            } else {
                v.pack.as_slice()
            }
        })
        .unwrap_or_default();
    SeedSlice {
        data: bytes.as_ptr(),
        len: bytes.len(),
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_seal_free(handle: *mut c_void) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle.cast::<Sealing>()) });
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_spool_prefix(branch: u16, pill: SeedSlice) -> *mut c_void {
    output((|| {
        Ok(
            data_seeds::registration::ingress::recovery::prefix(branch, unsafe {
                input(pill.data, pill.len)?
            })?
            .to_vec(),
        )
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_spool_read(
    raw: SeedSlice,
    maximum: usize,
    branch: *mut u16,
    length: *mut usize,
) -> i32 {
    if branch.is_null() || length.is_null() {
        return -1;
    }
    let raw = match unsafe { input(raw.data, raw.len) } {
        Ok(raw) => raw,
        Err(_) => return -1,
    };
    let total = match data_seeds::registration::ingress::recovery::length(raw, maximum) {
        Ok(n) => n,
        Err(Error::Incomplete) => return 1,
        Err(_) => return -1,
    };
    unsafe {
        *length = total;
    }
    match data_seeds::registration::ingress::recovery::read(raw, maximum) {
        Ok((id, _)) => {
            unsafe {
                *branch = id;
            }
            0
        }
        Err(Error::Incomplete) => 1,
        Err(_) => -1,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_bootstrap(
    trunk: *const c_void,
    branch: u16,
    now: u64,
    generation: SeedSlice,
    kind: u32,
) -> *mut c_void {
    output((|| {
        if trunk.is_null() {
            return Err(invalid("missing trunk"));
        }
        let trunk = unsafe { registered_trunk(trunk)? };
        match kind {
            0 => {
                let generation: [u8; 16] = unsafe { input(generation.data, generation.len)? }
                    .try_into()
                    .map_err(|_| invalid("invalid generation"))?;
                let first = now.saturating_sub(IngressLimits::default().straggler_ms)
                    / data_seeds::TIME_HOUR_MS
                    * data_seeds::TIME_HOUR_MS;
                Ok(data_seeds::TimeTrie::create(first, generation, 0)?
                    .bytes()
                    .to_vec())
            }
            1 => Ok(data_seeds::packs::Writer::new(&[])?
                .commit()?
                .bytes
                .to_vec()),
            2 => {
                let mut leaves = trunk
                    .trie()
                    .branches()
                    .select([branch])
                    .leaves()?
                    .create()?;
                leaves.register(branch)?;
                leaves.publish()
            }
            _ => Err(invalid("invalid ingress bootstrap component")),
        }
    })())
}

use data_seeds::registration::ingress::publication::Publication;
pub use data_seeds::registration::ingress::publication::PublicationPlan;
struct PublicationHandle {
    value: Result<Publication, String>,
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_prepare(
    seal: *const c_void,
    time: SeedSlice,
    footer: SeedSlice,
    length: u64,
    packed: bool,
) -> *mut c_void {
    let value = (|| {
        if seal.is_null() {
            return Err(invalid("missing sealed hour"));
        }
        let sealed = unsafe { &*seal.cast::<Sealing>() }
            .value
            .as_ref()
            .map_err(invalid)?;
        Publication::prepare(
            sealed,
            unsafe { input(time.data, time.len)? },
            unsafe { input(footer.data, footer.len)? },
            length,
            if packed {
                data_seeds::registration::Storage::Pack
            } else {
                data_seeds::registration::Storage::Browse
            }
            .compression(),
        )
    })()
    .map_err(|e: Error| e.to_string());
    Box::into_raw(Box::new(PublicationHandle { value })).cast()
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_prepare_error(handle: *const c_void) -> SeedSlice {
    let error = unsafe { &*handle.cast::<PublicationHandle>() }
        .value
        .as_ref()
        .err()
        .map_or("", String::as_str);
    SeedSlice {
        data: error.as_ptr(),
        len: error.len(),
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_prepare_bytes(
    handle: *const c_void,
    kind: u32,
    info: *mut PublicationPlan,
) -> SeedSlice {
    let value = unsafe { &*handle.cast::<PublicationHandle>() }
        .value
        .as_ref()
        .ok();
    let bytes = if let Some(value) = value {
        if !info.is_null() {
            unsafe {
                std::ptr::copy_nonoverlapping(&value.info, info, 1);
            }
        }
        match kind {
            0 => value.tail.as_slice(),
            1 => value.index.as_slice(),
            2 => value.time.as_slice(),
            _ => &[],
        }
    } else {
        &[]
    };
    SeedSlice {
        data: bytes.as_ptr(),
        len: bytes.len(),
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_prepare_free(handle: *mut c_void) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle.cast::<PublicationHandle>()) });
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_checkpoint(handle: *const c_void) -> *mut c_void {
    output((|| {
        let plan = unsafe { &*handle.cast::<PublicationHandle>() }
            .value
            .as_ref()
            .map_err(invalid)?;
        plan.checkpoint()
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_checkpoint_open(raw: SeedSlice) -> *mut c_void {
    let value = (|| {
        let raw = unsafe { input(raw.data, raw.len)? };
        Publication::read(raw)
    })()
    .map_err(|e: Error| e.to_string());
    Box::into_raw(Box::new(PublicationHandle { value })).cast()
}

fn write_scheduler(budget: usize) -> Result<data_seeds::writer::CommitScheduler, Error> {
    data_seeds::writer::CommitScheduler::new(
        data_seeds::MemoryBudget::new(budget),
        data_seeds::writer::QueueLimits {
            batches: 1,
            operations: 16,
        },
    )
    .map_err(|e| invalid(e.to_string()))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_publish(
    handle: *const c_void,
    prefix: SeedSlice,
    packed: bool,
    budget: usize,
) -> *mut c_void {
    output((|| {
        if handle.is_null() {
            return Err(invalid("missing publication"));
        }
        let plan = unsafe { &*handle.cast::<PublicationHandle>() }
            .value
            .as_ref()
            .map_err(invalid)?;
        let prefix = std::str::from_utf8(unsafe { input(prefix.data, prefix.len)? })
            .map_err(|e| invalid(e.to_string()))?;
        let storage = if packed {
            data_seeds::registration::Storage::Pack
        } else {
            data_seeds::registration::Storage::Browse
        };
        data_seeds::registration::ingress::files::publish(
            std::path::Path::new(prefix),
            storage,
            plan,
            &mut write_scheduler(budget)?,
        )
        .map_err(|e| invalid(e.to_string()))?;
        Ok(Vec::new())
    })())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_complete(
    handle: *mut c_void,
    time: SeedSlice,
    now: u64,
) -> *mut c_void {
    output((|| {
        let live = unsafe { owner(handle)? };
        let timeline =
            data_seeds::TimeTrie::open(Arc::new(unsafe { input(time.data, time.len)? }.to_vec()))?;
        Ok(live
            .completion(&timeline, now)?
            .map(|time| time.bytes().to_vec())
            .unwrap_or_default())
    })())
}

#[unsafe(no_mangle)]
pub extern "C" fn trunk_seed_query_definition() -> *mut c_void {
    output(
        data_seeds::registration::query_events::definition()
            .and_then(|d| d.bytes().map(|b| b.to_vec())),
    )
}

/// Initialize calendar and persisted summary lineage before replaying input.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_summary_start(
    handle: *mut c_void,
    trunk: *const c_void,
    time: SeedSlice,
    rx: SeedSlice,
    footer: SeedSlice,
    length: u64,
) -> *mut c_void {
    output((|| {
        let owner = unsafe { owner(handle)? };
        let timeline =
            data_seeds::TimeTrie::open(Arc::new(unsafe { input(time.data, time.len)? }.to_vec()))?;
        let trunk = unsafe { registered_trunk(trunk)? };
        let Some(start) = owner.summaries_start(
            &trunk,
            &timeline,
            unsafe { input(rx.data, rx.len)? },
            unsafe { input(footer.data, footer.len)? },
            length,
        )?
        else {
            return Ok(Vec::new());
        };
        serde_json::to_vec(&serde_json::json!({"first":start.first,"file":data_seeds::registration::ingress::SUMMARY_FILE,"bootstrap":start.bootstrap})).map_err(|e|invalid(e.to_string()))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_summary_restore(
    handle: *mut c_void,
    bytes: SeedSlice,
) -> *mut c_void {
    output((|| {
        let owner = unsafe { owner(handle)? };
        owner.summary_restore(unsafe { input(bytes.data, bytes.len)? }.to_vec())?;
        Ok(Vec::new())
    })())
}

/// Return an opaque live PAC only when its registry context is already visible.
/// An ordinary accumulator update needs fresh bytes, not another JSON transfer
/// of unchanged RX and taxonomy artifacts. Empty means publication is required.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_summary_snapshot(
    handle: *mut c_void,
    trie: SeedSlice,
    day: u64,
) -> *mut c_void {
    output((|| {
        let owner = unsafe { owner(handle)? };
        Ok(owner
            .summary_snapshot(unsafe { input(trie.data, trie.len)? }, day)?
            .unwrap_or_default())
    })())
}

struct SummaryOutput {
    trie: Vec<u8>,
    rx: Vec<u8>,
    commit: data_seeds::registration::ingress::SummaryCommit,
}
struct SummaryHandle(Result<SummaryOutput, String>);
unsafe fn summary_output<'a>(handle: *const c_void) -> Result<&'a SummaryOutput, Error> {
    if handle.is_null() {
        return Err(invalid("missing summary publication"));
    }
    unsafe { &*handle.cast::<SummaryHandle>() }
        .0
        .as_ref()
        .map_err(invalid)
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_summary(
    handle: *mut c_void,
    trie: SeedSlice,
    day: u64,
    through: u64,
    footer: SeedSlice,
    length: u64,
    seal: bool,
) -> *mut c_void {
    let value = (|| -> Result<SummaryOutput, Error> {
        let owner = unsafe { owner(handle)? };
        let trie = trie_trunk_taxonomy::Trunk::open(unsafe { input(trie.data, trie.len)? })
            .map_err(invalid)?;
        let write = owner.summary_prepare(
            &trie,
            day,
            through,
            unsafe { input(footer.data, footer.len)? },
            length,
            seal,
        )?;
        Ok(SummaryOutput {
            trie: write.trie.bytes().to_vec(),
            rx: write.document.bytes()?.to_vec(),
            commit: write.commit(),
        })
    })()
    .map_err(|e| e.to_string());
    Box::into_raw(Box::new(SummaryHandle(value))).cast()
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_summary_error(handle: *const c_void) -> SeedSlice {
    let error = unsafe { &*handle.cast::<SummaryHandle>() }
        .0
        .as_ref()
        .err()
        .map_or("", String::as_str);
    SeedSlice {
        data: error.as_ptr(),
        len: error.len(),
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_summary_bytes(
    handle: *const c_void,
    kind: u32,
    offset: *mut u64,
    length: *mut u64,
) -> SeedSlice {
    let value = unsafe { summary_output(handle) };
    let bytes = if let Ok(value) = value {
        if !offset.is_null() {
            unsafe {
                *offset = value.commit.offset;
            }
        }
        if !length.is_null() {
            unsafe {
                *length = value.commit.length;
            }
        }
        match kind {
            0 => value.commit.bytes.as_slice(),
            1 => &value.trie,
            2 => &value.rx,
            3 => data_seeds::registration::ingress::SUMMARY_FILE.as_bytes(),
            _ => &[],
        }
    } else {
        &[]
    };
    SeedSlice {
        data: bytes.as_ptr(),
        len: bytes.len(),
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_summary_free(handle: *mut c_void) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle.cast::<SummaryHandle>()) });
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_summary_publish(
    handle: *const c_void,
    prefix: SeedSlice,
    budget: usize,
) -> *mut c_void {
    output((|| {
        let plan = &unsafe { summary_output(handle)? }.commit;
        let prefix = std::str::from_utf8(unsafe { input(prefix.data, prefix.len)? })
            .map_err(|e| invalid(e.to_string()))?;
        data_seeds::registration::ingress::files::summary(
            std::path::Path::new(prefix),
            plan,
            &mut write_scheduler(budget)?,
        )
        .map_err(|e| invalid(e.to_string()))?;
        Ok(Vec::new())
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingress_summary_published(
    handle: *mut c_void,
    day: u64,
) -> *mut c_void {
    output((|| {
        unsafe { owner(handle)? }.summary_published(day);
        Ok(Vec::new())
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_summary_checkpoint(handle: *const c_void) -> *mut c_void {
    output(unsafe { summary_output(handle) }.and_then(|v| v.commit.checkpoint()))
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_summary_restore(bytes: SeedSlice) -> *mut c_void {
    let value = (|| -> Result<SummaryOutput, Error> {
        let commit = data_seeds::registration::ingress::SummaryCommit::restore(
            unsafe { input(bytes.data, bytes.len)? },
            256 << 20,
        )?;
        Ok(SummaryOutput {
            trie: Vec::new(),
            rx: Vec::new(),
            commit,
        })
    })()
    .map_err(|e| e.to_string());
    Box::into_raw(Box::new(SummaryHandle(value))).cast()
}
