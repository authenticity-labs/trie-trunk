//! Thin transport ABI over the one Trunk owner. Handles pin snapshots and files;
//! they do not implement ingestion, publication, or query lifecycle themselves.
use crate::seed_native::{SeedSlice, input};
use std::{ffi::c_void, os::fd::IntoRawFd, path::Path, sync::Arc};
use trunk_node::{Node, Result};

enum Value {
    Node(Arc<Node>),
    Snapshot(trunk_node::publication::Snapshot),
    Registry(data_seeds::registration::Snapshot),
    Service {
        _service: trunk_node::service::Service,
    },
    Postgres { _service: trunk_node::postgres::Service },
    Number(u64),
    Logs(trunk_node::logs::Logs),
    Registration(Vec<u8>),
}
struct Failure {
    message: String,
    status: u16,
}
struct Handle(std::result::Result<Value, Failure>);
fn result(value: Result<Value>) -> *mut c_void {
    Box::into_raw(Box::new(Handle(value.map_err(|e| Failure {
        status: if e.is::<trunk_node::InputError>() {
            400
        } else {
            503
        },
        message: e.to_string(),
    }))))
    .cast()
}
unsafe fn value<'a>(ptr: *const c_void) -> Result<&'a Value> {
    if ptr.is_null() {
        return Err("Missing node handle".into());
    }
    unsafe { &*ptr.cast::<Handle>() }
        .0
        .as_ref()
        .map_err(|e| e.message.clone().into())
}
unsafe fn owner<'a>(ptr: *const c_void) -> Result<&'a Arc<Node>> {
    match unsafe { value(ptr)? } {
        Value::Node(node) => Ok(node),
        _ => Err("Expected node handle".into()),
    }
}
unsafe fn bytes<'a>(slice: SeedSlice) -> Result<&'a [u8]> {
    Ok(unsafe { input(slice.data, slice.len)? })
}
unsafe fn text<'a>(slice: SeedSlice) -> Result<&'a str> {
    Ok(std::str::from_utf8(unsafe { bytes(slice)? })?)
}
fn slice(bytes: &[u8]) -> SeedSlice {
    SeedSlice {
        data: bytes.as_ptr(),
        len: bytes.len(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_free(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(unsafe { Box::from_raw(ptr.cast::<Handle>()) });
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_error(ptr: *const c_void) -> SeedSlice {
    if ptr.is_null() {
        return slice(b"Missing node result");
    }
    slice(
        unsafe { &*ptr.cast::<Handle>() }
            .0
            .as_ref()
            .err()
            .map_or("", |error| error.message.as_str())
            .as_bytes(),
    )
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_status(ptr: *const c_void) -> u16 {
    if ptr.is_null() {
        return 503;
    }
    unsafe { &*ptr.cast::<Handle>() }
        .0
        .as_ref()
        .err()
        .map_or(0, |error| error.status)
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_open(
    root: SeedSlice,
    config: SeedSlice,
    memory: usize,
    clock: u64,
) -> *mut c_void {
    result((|| {
        Ok(Value::Node(Arc::new(Node::open_with(
            Path::new(unsafe { text(root)? }),
            serde_json::from_slice(unsafe { bytes(config)? })?,
            memory,
            clock,
        )?)))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_ingest(
    ptr: *const c_void,
    branch: u16,
    pill: SeedSlice,
    clock: u64,
) -> *mut c_void {
    result((|| {
        Ok(Value::Number(unsafe { owner(ptr)? }.ingest(
            branch,
            unsafe { bytes(pill)? },
            clock,
        )?))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_commit(ptr: *const c_void, clock: u64) -> *mut c_void {
    result((|| {
        Ok(Value::Number(unsafe { owner(ptr)? }.commit(clock)? as u64))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_logs(
    ptr: *const c_void,
    cache: SeedSlice,
    resources: SeedSlice,
) -> *mut c_void {
    result((|| {
        let resources = serde_json::from_slice::<Vec<String>>(unsafe { bytes(resources)? })?;
        Ok(Value::Logs(
            unsafe { owner(ptr)? }.logs(unsafe { text(cache)? }, &resources)?,
        ))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_log(ptr: *const c_void, observation: SeedSlice) -> *mut c_void {
    result((|| {
        let Value::Logs(logs) = (unsafe { value(ptr)? }) else {
            return Err("Expected resource observer".into());
        };
        logs.write(serde_json::from_slice(unsafe { bytes(observation)? })?)?;
        Ok(Value::Number(0))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_prepare(
    ptr: *const c_void,
    branch: u16,
    clock: u64,
) -> *mut c_void {
    result((|| {
        unsafe { owner(ptr)? }.prepare(branch, clock)?;
        Ok(Value::Number(0))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_register(
    ptr: *const c_void,
    request: SeedSlice,
    rx: SeedSlice,
) -> *mut c_void {
    result((|| {
        Ok(Value::Number(unsafe { owner(ptr)? }.register_request(
            &serde_json::from_slice(unsafe { bytes(request)? })?,
            unsafe { bytes(rx)? },
        )? as u64))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_registration(
    ptr: *const c_void,
    target: SeedSlice,
    rx: SeedSlice,
) -> *mut c_void {
    result((|| {
        let registered = unsafe { owner(ptr)? }.registration(
            &serde_json::from_slice(unsafe { bytes(target)? })?,
            unsafe { bytes(rx)? },
        )?;
        Ok(Value::Registration(serde_json::to_vec(&registered)?))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_publish(
    ptr: *const c_void,
    expected: SeedSlice,
    next: SeedSlice,
) -> *mut c_void {
    result((|| {
        unsafe { owner(ptr)? }.publish(unsafe { bytes(expected)? }, unsafe { bytes(next)? })?;
        Ok(Value::Number(0))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_registry(ptr: *const c_void) -> *mut c_void {
    result((|| Ok(Value::Registry(unsafe { owner(ptr)? }.trie())))())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_trie(ptr: *const c_void) -> *mut c_void {
    match unsafe { value(ptr) } {
        Ok(Value::Registry(s)) => crate::seed_native::shared_trunk(s.clone()),
        _ => std::ptr::null_mut(),
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_memory(ptr: *const c_void) -> *mut c_void {
    unsafe { owner(ptr) }.map_or(std::ptr::null_mut(), |n| {
        crate::buffers::shared_memory(n.memory())
    })
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_postgres(ptr: *const c_void, address: SeedSlice, password: SeedSlice) -> *mut c_void {
    result((|| Ok(Value::Postgres { _service: trunk_node::postgres::Service::start(
        unsafe { owner(ptr)? }.clone(), unsafe { text(address)? }, unsafe { text(password)? }.to_owned())? }))())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_serve(ptr: *const c_void, path: SeedSlice) -> *mut c_void {
    result((|| {
        Ok(Value::Service {
            _service: trunk_node::service::Service::start(
                unsafe { owner(ptr)? }.clone(),
                Path::new(unsafe { text(path)? }),
            )?,
        })
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_command(
    args: SeedSlice,
    input: i32,
    output: i32,
    diagnostic: i32,
    _terminal: bool,
) -> *mut c_void {
    use std::{
        fs::File,
        io::{BufReader, IsTerminal},
        mem::ManuallyDrop,
        os::fd::FromRawFd,
    };
    result((|| {
        if input < 0 || output < 0 || diagnostic < 0 {
            return Err("Missing command descriptor".into());
        }
        let args: Vec<String> = serde_json::from_slice(unsafe { bytes(args)? })?;
        let input = ManuallyDrop::new(unsafe { File::from_raw_fd(input) });
        let output = ManuallyDrop::new(unsafe { File::from_raw_fd(output) });
        let diagnostic = ManuallyDrop::new(unsafe { File::from_raw_fd(diagnostic) });
        trunk_node::command::run(
            &args,
            BufReader::new(&*input),
            &mut &*output,
            &mut &*diagnostic,
            output.is_terminal(),
        )?;
        Ok(Value::Number(0))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_snapshot(
    ptr: *const c_void,
    branch: u16,
    windows: SeedSlice,
    summaries: bool,
    known: u64,
    has_known: bool,
    clock: u64,
) -> *mut c_void {
    result((|| {
        Ok(Value::Snapshot(unsafe { owner(ptr)? }.publication(
            branch,
            &serde_json::from_slice::<Vec<(u64, u64)>>(unsafe { bytes(windows)? })?,
            summaries,
            has_known.then_some(known),
            clock,
        )?))
    })())
}
#[repr(C)]
#[derive(Default)]
pub struct SnapshotInfo {
    generation: u64,
    length: u64,
    summary_length: u64,
    index_length: u64,
    live: usize,
    summaries: usize,
    published: bool,
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_snapshot_info(ptr: *const c_void) -> SnapshotInfo {
    let Ok(Value::Snapshot(s)) = (unsafe { value(ptr) }) else {
        return SnapshotInfo::default();
    };
    SnapshotInfo {
        generation: s.generation,
        length: s.published.as_ref().map_or(0, |p| p.length),
        summary_length: s.published.as_ref().map_or(0, |p| p.summary_length),
        index_length: s.published.as_ref().map_or(0, |p| p.index_length),
        live: s.live.len(),
        summaries: s.summaries.len(),
        published: s.published.is_some(),
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_bytes(
    ptr: *const c_void,
    kind: u32,
    index: usize,
) -> SeedSlice {
    let raw = match unsafe { value(ptr) } {
        Ok(Value::Registry(r)) if kind == 0 => r.bytes(),
        Ok(Value::Registration(r)) if kind == 0 => r,
        Ok(Value::Snapshot(s)) => match kind {
            0 => s.registry.bytes(),
            1 => s
                .published
                .as_ref()
                .map_or(&[] as &[u8], |p| p.time.as_slice()),
            2 => s
                .published
                .as_ref()
                .map_or(&[] as &[u8], |p| p.footer.as_slice()),
            3 => s
                .published
                .as_ref()
                .map_or(&[] as &[u8], |p| p.summary_footer.as_slice()),
            4 => s.live.get(index).map_or(&[] as &[u8], Vec::as_slice),
            5 => s
                .summaries
                .get(index)
                .map_or(&[] as &[u8], |(_, b)| b.as_slice()),
            _ => &[],
        },
        _ => &[],
    };
    slice(raw)
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_number(ptr: *const c_void, index: usize) -> u64 {
    match unsafe { value(ptr) } {
        Ok(Value::Number(n)) => *n,
        Ok(Value::Snapshot(s)) => s.summaries.get(index).map_or(0, |(day, _)| *day),
        _ => 0,
    }
}
// A descriptor is cloned once per publication, never per streamed Pack. The
// transport owns the returned descriptor and closes it with its read lease.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_node_file(ptr: *const c_void, kind: u32) -> i32 {
    let Ok(Value::Snapshot(s)) = (unsafe { value(ptr) }) else {
        return -1;
    };
    let Some(p) = &s.published else { return -1 };
    let file = match kind {
        0 => p.packs.as_ref(),
        1 => p.index.as_ref(),
        2 => p.summaries.as_ref(),
        _ => None,
    };
    match file {
        None => -1,
        Some(file) => file.try_clone().map_or(-2, |f| f.into_raw_fd()),
    }
}
