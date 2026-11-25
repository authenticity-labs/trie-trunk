//! Native adapter for the Go collection prototype. Handles own their file descriptors.
use std::{io, os::fd::BorrowedFd, slice};
use trunk_uuid_index::collection::{Binding, HistoryLink, Index, Posting, Snapshot};

#[repr(C)]
pub struct Failure {
    errno: i32,
    message: [u8; 256],
}
impl Failure {
    fn set(&mut self, e: io::Error) {
        self.errno = e.raw_os_error().unwrap_or(0);
        let message = e.to_string();
        let n = message.len().min(self.message.len() - 1);
        self.message[..n].copy_from_slice(&message.as_bytes()[..n]);
        self.message[n] = 0;
    }
}
type Hook = extern "C" fn(usize, bool) -> bool;
type Visit = extern "C" fn(usize, *const u8, u32, u64) -> bool;
type Source = extern "C" fn(usize, u32, *const u8, u8, u64);
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tt_collection_open(fd: i32, error: &mut Failure) -> *mut Index {
    let fd = unsafe { BorrowedFd::borrow_raw(fd) };
    match fd
        .try_clone_to_owned()
        .and_then(|fd| Index::open(fd.into()))
    {
        Ok(idx) => Box::into_raw(Box::new(idx)),
        Err(e) => {
            error.set(e);
            std::ptr::null_mut()
        }
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tt_collection_close(idx: *mut Index) {
    drop(unsafe { Box::from_raw(idx) })
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_collection_pin(idx: &Index) -> *mut Snapshot {
    Box::into_raw(Box::new(idx.snapshot()))
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tt_collection_release(view: *mut Snapshot) {
    drop(unsafe { Box::from_raw(view) })
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_collection_sources(idx: &Index, ctx: usize, visit: Source) {
    for (k, w, t) in idx.snapshot().sources() {
        visit(ctx, k.minor, k.generation.as_ptr(), w, t)
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tt_collection_append(
    idx: &Index,
    k: Binding,
    width: u8,
    from: u64,
    through: u64,
    merge: bool,
    data: *const Posting,
    len: usize,
    ctx: usize,
    hook: Option<Hook>,
    error: &mut Failure,
) {
    let posts = if len == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(data, len) }
    };
    if let Err(e) = idx.update(
        k,
        width,
        (!merge).then_some(from),
        through,
        posts,
        |after| {
            if hook.is_some_and(|f| !f(ctx, after)) {
                return Err(io::Error::other("collection publication interrupted"));
            }
            Ok(())
        },
    ) {
        error.set(e)
    }
}
#[repr(C)]
pub struct LookupKey {
    id: [u8; 16],
}
#[repr(C)]
pub struct Lookup {
    address: u64,
    revision: u32,
    error: *mut Failure,
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_collection_get(
    idx: &Index,
    k: Binding,
    key: LookupKey,
    rev: u32,
    latest: bool,
) -> Lookup {
    let view = idx.snapshot();
    let found = if latest {
        view.latest(k, key.id)
    } else {
        view.get(k, key.id, rev)
            .map(|opt| opt.map(|addr| (rev, addr)))
    };
    let mut result = Lookup {
        address: 0,
        revision: 0,
        error: std::ptr::null_mut(),
    };
    match found {
        Ok(Some((r, a))) => {
            result.address = a;
            result.revision = r;
        }
        Ok(None) => {}
        Err(e) => {
            let mut error = Box::new(Failure {
                errno: 0,
                message: [0; 256],
            });
            error.set(e);
            result.error = Box::into_raw(error);
        }
    }
    result
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tt_collection_error_free(ptr: *mut Failure) {
    drop(unsafe { Box::from_raw(ptr) })
}
// A page holds at most 123 units of 32 bytes with locators at least 5 bytes
// wide. The caller reuses this fixed buffer, even for arbitrarily long histories.
const HISTORY_BATCH: usize = 768;
#[repr(C)]
pub struct HistoryRow {
    revision: u32,
    hour: u32,
    record: u32,
}
pub struct HistoryCursor {
    view: Snapshot,
    binding: Binding,
    key: [u8; 16],
    next: Option<HistoryLink>,
}
#[repr(C)]
pub struct BatchResult {
    len: usize,
    cursor: *mut HistoryCursor,
    error: *mut Failure,
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn tt_history_page(
    index: Option<&Index>,
    pinned: Option<&Snapshot>,
    binding: Binding,
    key: LookupKey,
    resume: *mut HistoryCursor,
    rows: &mut [HistoryRow; HISTORY_BATCH],
) -> BatchResult {
    // The input cursor is consumed on every call, including errors.
    let mut owned = (!resume.is_null()).then(|| unsafe { Box::from_raw(resume) });
    let mut first = None;
    let cursor = match &mut owned {
        Some(c) => c.as_mut(),
        None => {
            let view = match pinned {
                Some(v) => v.clone(),
                None => index.unwrap().snapshot(),
            };
            first.insert(HistoryCursor {
                view,
                binding,
                key: key.id,
                next: None,
            })
        }
    };
    let mut out = BatchResult {
        len: 0,
        cursor: std::ptr::null_mut(),
        error: std::ptr::null_mut(),
    };
    match cursor.view.history_page(
        cursor.binding,
        cursor.key,
        cursor.next.take(),
        |revision, addr| {
            rows[out.len] = HistoryRow {
                revision,
                hour: (addr >> 32) as u32,
                record: addr as u32,
            };
            out.len += 1;
        },
    ) {
        Ok(Some(next)) => {
            cursor.next = Some(next);
            out.cursor = Box::into_raw(match owned {
                Some(c) => c,
                None => Box::new(first.unwrap()),
            });
        }
        Ok(None) => {}
        Err(e) => {
            let mut error = Box::new(Failure {
                errno: 0,
                message: [0; 256],
            });
            error.set(e);
            out.error = Box::into_raw(error);
        }
    }
    out
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tt_history_close(cursor: *mut HistoryCursor) {
    drop(unsafe { Box::from_raw(cursor) })
}

#[unsafe(no_mangle)]
pub extern "C" fn tt_collection_each(
    view: &Snapshot,
    k: Binding,
    ctx: usize,
    visit: Visit,
    error: &mut Failure,
) {
    if let Err(e) = view.each(k, |id, r, v| {
        if visit(ctx, id.as_ptr(), r, v) {
            Ok(())
        } else {
            Err(io::Error::other("collection visitor stopped"))
        }
    }) {
        error.set(e);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn tt_collection_pages(index: &Index, k: Binding, counts: &mut [usize; 3]) {
    let (a, b, c) = index.snapshot().page_counts(k);
    *counts = [a, b, c];
}
