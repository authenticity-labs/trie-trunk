//! Native server adapter. DOC, typed ingress, PAC assembly and envelope reading
//! delegate to the same Rust libraries used by the Seed WASM adapter.
//! Input pointers are borrowed only for the duration of a call. Output handles
//! own their allocations until released; callers must serialize handle mutation.
use data_seeds::{SeedDefinition, doc};
use pil_codec::{Error, api::FieldBytes};
use std::{ffi::c_void, sync::Arc};
mod ingress;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SeedSlice {
    pub data: *const u8,
    pub len: usize,
}
#[repr(C)]
pub struct SeedValue {
    pub ordinal: usize,
    pub kind: u32,
    pub low: u64,
    pub high: u64,
    pub real: f64,
    pub bytes: SeedSlice,
}
#[repr(C)]
pub struct SeedExpansion {
    pub status: i32,
    pub output: *mut c_void,
}
struct Codec {
    definition: Result<Arc<SeedDefinition>, String>,
}
struct TrunkHandle {
    value: Result<Registry, String>,
}
enum Registry {
    Mutable(std::sync::Mutex<data_seeds::registration::Trunk>),
    Shared(data_seeds::registration::Snapshot),
}
pub(crate) fn shared_trunk(snapshot:data_seeds::registration::Snapshot) -> *mut c_void {
    Box::into_raw(Box::new(TrunkHandle{value:Ok(Registry::Shared(snapshot))})).cast()
}
enum Registered<'a> {
    Mutable(std::sync::MutexGuard<'a,data_seeds::registration::Trunk>),
    Shared(&'a data_seeds::registration::Trunk),
}
impl std::ops::Deref for Registered<'_> {
    type Target=data_seeds::registration::Trunk;
    fn deref(&self)->&Self::Target {match self {Self::Mutable(v)=>v,Self::Shared(v)=>v}}
}
impl Registered<'_> {
    fn prescribe(&mut self,id:u32,bytes:&[u8])->Result<Arc<SeedDefinition>,Error> {
        match self {Self::Mutable(t)=>t.prescribe(id,bytes),Self::Shared(_)=>Err(invalid("Published registry plans are immutable"))}
    }
    fn prescribe_file(&mut self,file:&str,bytes:&[u8])->Result<(),Error> {
        match self {Self::Mutable(t)=>{t.prescribe_file(file,bytes)?;Ok(())},Self::Shared(_)=>Ok(())}
    }
}
struct Timeline {
    value: Result<data_seeds::TimeTrie, String>,
    index_segments: std::sync::OnceLock<Result<Vec<(u64,u64)>, String>>,
}
struct SummaryIndex { value: Result<data_seeds::summaries::SummaryBoxReader<'static>, String> }
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_summary_index(footer: SeedSlice, length: u64) -> *mut c_void {
    let value = unsafe { input(footer.data, footer.len) }.and_then(|bytes|
        data_seeds::summaries::SummaryBoxReader::open_footer_owned(bytes.to_vec(), length)).map_err(|e| e.to_string());
    Box::into_raw(Box::new(SummaryIndex { value })).cast()
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_summary_index_error(handle: *const c_void) -> SeedSlice {
    let text = unsafe { &*handle.cast::<SummaryIndex>() }.value.as_ref().err().map_or("", String::as_str);
    SeedSlice { data: text.as_ptr(), len: text.len() }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_summary_index_free(handle: *mut c_void) {
    if !handle.is_null() { drop(unsafe { Box::from_raw(handle.cast::<SummaryIndex>()) }); }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_summary_spans(handle: *const c_void, time: *const c_void, ranges: SeedSlice) -> *mut c_void {
    output((|| {
        if handle.is_null() || time.is_null() { return Err(invalid("Missing summary or time index")); }
        let reader = unsafe { &*handle.cast::<SummaryIndex>() }.value.as_ref().map_err(invalid)?;
        let time = unsafe { &*time.cast::<Timeline>() }.value.as_ref().map_err(invalid)?;
        let calendar = time.summary_calendar()?;
        let first = calendar.day(time.header().first)?.start;
        let ranges: Vec<[u64; 2]> = serde_json::from_slice(unsafe { input(ranges.data, ranges.len)? }).map_err(|e| invalid(e.to_string()))?;
        let mut spans = Vec::new();
        for [from, to] in ranges {
            if from >= to || calendar.day(from)?.start != from || calendar.day(to)?.start != to { return Err(invalid("summary range must contain complete calendar days")); }
            let mut at = from;
            while at < to {
                if spans.len() == 1024 { return Err(invalid("summary range exceeds 1024 days")); }
                let ordinal = at.checked_sub(first).ok_or_else(|| invalid("summary day predates retained storage"))? / data_seeds::summaries::SUMMARY_DAY_MS;
                let extent = reader.storage_range(usize::try_from(ordinal).map_err(|_| Error::Overflow)?)?;
                spans.push(serde_json::json!({"day":at,"start":extent.start,"end":extent.end}));
                at = at.checked_add(data_seeds::summaries::SUMMARY_DAY_MS).ok_or(Error::Overflow)?;
            }
        }
        serde_json::to_vec(&spans).map_err(|e| invalid(e.to_string()))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_time_open(bytes: SeedSlice) -> *mut c_void {
    let value = unsafe { input(bytes.data, bytes.len) }
        .and_then(|bytes| data_seeds::TimeTrie::open(Arc::new(bytes.to_vec())))
        .map_err(|e| e.to_string());
    Box::into_raw(Box::new(Timeline { value, index_segments: Default::default() })).cast()
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_time_error(handle: *const c_void) -> SeedSlice {
    let timeline = unsafe { &*handle.cast::<Timeline>() };
    let text = timeline.value.as_ref().err().map_or("", String::as_str);
    SeedSlice { data: text.as_ptr(), len: text.len() }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_time_free(handle: *mut c_void) {
    if !handle.is_null() { drop(unsafe { Box::from_raw(handle.cast::<Timeline>()) }); }
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TimeLeaf {
    pub hour: u64, pub start: u64, pub end: u64,
    pub packs: u64, pub beds: u64, pub count: u64,
}
struct TimeSelection { leaves: Vec<TimeLeaf>, error: String }
#[repr(C)]
pub struct TimeLeaves { data: *const TimeLeaf, len: usize }
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_time_select(handle: *const c_void, from: u64, to: u64) -> *mut c_void {
    let selected = (|| {
        if handle.is_null() { return Err(invalid("Missing time trie")); }
        let timeline = unsafe { &*handle.cast::<Timeline>() }.value.as_ref().map_err(invalid)?;
        if from >= to || to - from > 65_536 * data_seeds::TIME_HOUR_MS {
            return Err(invalid("Invalid timeline selection"));
        }
        let range = timeline.range(from, to)?;
        let mut leaves = Vec::with_capacity(range.size_hint().0);
        for leaf in range {
            let leaf = leaf?;
            leaves.push(TimeLeaf {hour:leaf.hour,start:leaf.start,end:leaf.end,packs:leaf.packs,beds:leaf.beds,count:leaf.count});
        }
        Ok(leaves)
    })();
    let result = match selected {
        Ok(leaves) => TimeSelection {leaves,error:String::new()},
        Err(error) => TimeSelection {leaves:Vec::new(),error:error.to_string()},
    };
    Box::into_raw(Box::new(result)).cast()
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_time_leaves(handle: *const c_void) -> TimeLeaves {
    let selection = unsafe { &*handle.cast::<TimeSelection>() };
    TimeLeaves {data:selection.leaves.as_ptr(),len:selection.leaves.len()}
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_time_selection_error(handle: *const c_void) -> SeedSlice {
    let selection = unsafe { &*handle.cast::<TimeSelection>() };
    SeedSlice {data:selection.error.as_ptr(),len:selection.error.len()}
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_time_selection_free(handle: *mut c_void) {
    if !handle.is_null() {drop(unsafe {Box::from_raw(handle.cast::<TimeSelection>())});}
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_time_info(handle: *const c_void) -> *mut c_void {
    output((|| {
        if handle.is_null() {return Err(invalid("Missing time trie"));}
        let timeline = unsafe {&*handle.cast::<Timeline>()}.value.as_ref().map_err(invalid)?;
        serde_json::to_vec(timeline.header()).map_err(|e|invalid(e.to_string()))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_time_bytes(handle: *const c_void) -> SeedSlice {
    let bytes = unsafe {&*handle.cast::<Timeline>()}.value.as_ref().map_or(&[][..],|v|v.bytes());
    SeedSlice {data:bytes.as_ptr(),len:bytes.len()}
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_time_spans(handle: *const c_void, from: u64, to: u64) -> *mut c_void {
    output((|| {
        if handle.is_null() {return Err(invalid("Missing time trie"));}
        let timeline = unsafe {&*handle.cast::<Timeline>()}.value.as_ref().map_err(invalid)?;
        serde_json::to_vec(&timeline.spans(from,to)?).map_err(|e|invalid(e.to_string()))
    })())
}
/// Reuse the immutable Time snapshot's segment boundaries for index requests.
/// The host copies these byte spans without interpreting index rows.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_time_index_spans(handle: *const c_void, request: SeedSlice, length: u64) -> *mut c_void {
    output((|| {
        if handle.is_null() {return Err(invalid("Missing time trie"));}
        let owner=unsafe {&*handle.cast::<Timeline>()};
        let timeline=owner.value.as_ref().map_err(invalid)?;
        let segments=owner.index_segments.get_or_init(|| {
            let header=timeline.header();
            if header.first==header.sealed {return Ok(Vec::new());}
            timeline.range(header.first,header.sealed).map_err(|e|e.to_string())?
                .filter_map(|leaf|match leaf {Ok(leaf) if leaf.count==0=>None,other=>Some(other)})
                .map(|leaf|leaf.map(|leaf|(leaf.start,leaf.end)).map_err(|e|e.to_string())).collect()
        }).as_ref().map_err(invalid)?;
        let ranges:Vec<String>=serde_json::from_slice(unsafe{input(request.data,request.len)?}).map_err(|e|invalid(e.to_string()))?;
        if ranges.is_empty()||ranges.len()>128{return Err(invalid("provide 1–128 complete row ranges"));}
        let mut previous=0;let mut spans=Vec::<(u64,u64)>::new();
        for text in ranges {
            let (a,z)=text.split_once(',').ok_or_else(||invalid("invalid rows"))?;
            let a=a.parse::<u64>().map_err(|_|invalid("invalid rows"))?;
            let z=z.parse::<u64>().map_err(|_|invalid("invalid rows"))?;
            if a>=z||a<previous{return Err(invalid("invalid row order"));}
            let first=segments.binary_search_by_key(&a,|s|s.0).map_err(|_|invalid("rows must select complete sealed segments"))?;
            let last=segments.binary_search_by_key(&z,|s|s.1).map_err(|_|invalid("rows must select complete sealed segments"))?;
            if first>last{return Err(invalid("invalid row order"));}
            let start=a.checked_mul(6).ok_or_else(||invalid("index extent overflow"))?;
            let end=z.checked_mul(6).filter(|end|*end<=length).ok_or_else(||invalid("index extent exceeds file"))?;
            if let Some(last)=spans.last_mut().filter(|last|last.1==start){last.1=end;}else{spans.push((start,end));}
            previous=z;
        }
        serde_json::to_vec(&spans).map_err(|e|invalid(e.to_string()))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_trie_open(bytes: SeedSlice) -> *mut c_void {
    let value = unsafe { input(bytes.data, bytes.len) }
        .and_then(|b| data_seeds::registration::Trunk::open(b))
        .map(|t|Registry::Mutable(std::sync::Mutex::new(t))).map_err(|e| e.to_string());
    Box::into_raw(Box::new(TrunkHandle { value })).cast()
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_trie_error(handle: *const c_void) -> SeedSlice {
    let reader = unsafe { &*handle.cast::<TrunkHandle>() };
    let text = reader.value.as_ref().err().map_or("", String::as_str);
    SeedSlice { data: text.as_ptr(), len: text.len() }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_trie_free(handle: *mut c_void) {
    if !handle.is_null() { drop(unsafe { Box::from_raw(handle.cast::<TrunkHandle>()) }); }
}
unsafe fn registered_trunk<'a>(handle: *const c_void) -> Result<Registered<'a>, Error> {
    if handle.is_null() {return Err(invalid("Missing registered trie"));}
    match unsafe {&*handle.cast::<TrunkHandle>()}.value.as_ref().map_err(invalid)? {
        Registry::Mutable(t)=>t.lock().map(Registered::Mutable).map_err(|_|invalid("Registered trie lock poisoned")),
        Registry::Shared(s)=>Ok(Registered::Shared(s.registered())),
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_trie_definition_file(handle: *const c_void, id: u32) -> *mut c_void {
    output((|| {
        let reader = unsafe { registered_trunk(handle)? };
        Ok(reader.trie().definition(id).map_err(invalid)?.file.as_bytes().to_vec())
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_trie_prescribe(handle: *const c_void, id: u32, bytes: SeedSlice) -> *mut c_void {
    let definition = (|| {
        let mut trunk = unsafe { registered_trunk(handle)? };
        let bytes = unsafe { input(bytes.data, bytes.len)? };
        if bytes.is_empty() { trunk.definition(id) } else { trunk.prescribe(id, bytes) }
    })().map_err(|e: Error| e.to_string());
    Box::into_raw(Box::new(Codec { definition })).cast()
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_trie_route(handle: *const c_void, id: u16) -> *mut c_void {
    output((|| {
        let reader = unsafe { registered_trunk(handle)? };
        let route = reader.trie().route(id).map_err(invalid)?;
        let mut value = serde_json::to_value(&route).map_err(|e| invalid(e.to_string()))?;
        value["source"] = serde_json::Value::String(route.source().map_err(invalid)?);
        value["storage"] = serde_json::to_value(route.storage().map_err(invalid)?).map_err(|e| invalid(e.to_string()))?;
        serde_json::to_vec(&value).map_err(|e| invalid(e.to_string()))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_trie_routes(handle: *const c_void, cache: SeedSlice) -> *mut c_void {
    output((|| {
        let cache = std::str::from_utf8(unsafe { input(cache.data, cache.len)? })
            .map_err(|e| invalid(e.to_string()))?;
        let reader = unsafe { registered_trunk(handle)? };
        let trie = reader.trie();
        let mut routes = Vec::new();
        for resource in trie.definitions().map_err(invalid)?.resources() {
            let route = trie.route(resource.branch).map_err(invalid)?;
            if route.cache == cache {
                let mut value=serde_json::to_value(&route).map_err(|e|invalid(e.to_string()))?;
                value["source"]=serde_json::Value::String(route.source().map_err(invalid)?);
                value["storage"]=serde_json::to_value(route.storage().map_err(invalid)?).map_err(|e|invalid(e.to_string()))?;
                routes.push(value);
            }
        }
        serde_json::to_vec(&routes).map_err(|e| invalid(e.to_string()))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_trie_state_path(handle: *const c_void, source: SeedSlice, definition: SeedSlice, branch: u16) -> *mut c_void {
    output((|| {
        let source = std::str::from_utf8(unsafe { input(source.data, source.len)? }).map_err(|e| invalid(e.to_string()))?;
        let definition = std::str::from_utf8(unsafe { input(definition.data, definition.len)? }).map_err(|e| invalid(e.to_string()))?;
        let reader = unsafe { registered_trunk(handle)? };
        let route = if branch == 0 { None } else { Some(reader.trie().route(branch).map_err(invalid)?) };
        if let Some(route) = &route {
            if source != route.storage().map_err(invalid)?.directory && source != route.source().map_err(invalid)? {
                return Err(invalid("State selector does not match resource"));
            }
            if !definition.is_empty() && definition != route.digest { return Err(invalid("State selector Definition mismatch")); }
        }
        if let Some(route) = route { return Ok(route.leaf_path().map_err(invalid)?.into_bytes()); }
        Ok(reader.trie().state_path(source, (!definition.is_empty()).then_some(definition)).map_err(invalid)?.into_bytes())
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_trie_leaves(handle: *const c_void, path: SeedSlice, bytes: SeedSlice) -> *mut c_void {
    output((|| {
        let reader = unsafe { registered_trunk(handle)? };
        let path = std::str::from_utf8(unsafe { input(path.data, path.len)? }).map_err(|e| invalid(e.to_string()))?;
        let mut selected = None;
        for resource in reader.trie().definitions().map_err(invalid)?.resources() {
            if reader.trie().route(resource.branch).map_err(invalid)?.leaf_path().map_err(invalid)? == path {
                selected = Some(resource.branch); break;
            }
        }
        let branch = selected.ok_or_else(|| invalid("Unregistered leaf resource"))?;
        let roots = reader.trie().branches().select([branch]).leaves()?.definitions(unsafe { input(bytes.data, bytes.len)? })?;
        let roots:Vec<String> = roots.iter().map(|digest| digest.iter().map(|b| format!("{b:02x}")).collect()).collect();
        serde_json::to_vec(&roots).map_err(|e| invalid(e.to_string()))
    })())
}
// The host serializes commits for one type. Rust retains the published state;
// an unacknowledged tail never freezes the preceding version root in memory.
struct LeafWriter {
    value: Result<data_seeds::registration::LeafWriter, String>,
    pending: Option<data_seeds::LeafCommit>,
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_leaf_writer(handle: *const c_void, branch: u16, bytes: SeedSlice) -> *mut c_void {
    let value = (|| {
        let reader = unsafe { registered_trunk(handle)? };
        let scope = reader.trie().branches().select([branch]).leaves()?;
        let raw = unsafe { input(bytes.data, bytes.len)? };
        if raw.is_empty() { scope.create() } else { scope.resume(raw) }
    })().map_err(|e: Error| e.to_string());
    Box::into_raw(Box::new(LeafWriter {value, pending:None})).cast()
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_leaf_writer_error(handle: *const c_void) -> SeedSlice {
    let writer = unsafe { &*handle.cast::<LeafWriter>() };
    let text = writer.value.as_ref().err().map_or("", String::as_str);
    SeedSlice {data:text.as_ptr(),len:text.len()}
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_leaf_writer_free(handle: *mut c_void) {
    if !handle.is_null() { drop(unsafe { Box::from_raw(handle.cast::<LeafWriter>()) }); }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_leaf_prepare(handle: *mut c_void, branch: u16, first: usize, nodes: SeedSlice) -> *mut c_void {
    output((|| {
        if handle.is_null() { return Err(invalid("Missing leaf writer")); }
        let writer = unsafe { &mut *handle.cast::<LeafWriter>() };
        if writer.pending.is_some() { return Err(invalid("Leaf write already pending")); }
        let value = writer.value.as_ref().map_err(invalid)?;
        let mut trie = value.nodes(branch)?;
        let bytes = unsafe { input(nodes.data,nodes.len)? };
        if !bytes.is_empty() { trie.append_pil(first,bytes)?; }
        writer.pending = Some(value.prepare(branch,trie)?);
        Ok(Vec::new())
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_leaf_tail(handle: *const c_void, offset: *mut usize) -> SeedSlice {
    let writer = unsafe { &*handle.cast::<LeafWriter>() };
    if let Some(commit) = &writer.pending {
        if !offset.is_null() { unsafe { *offset=commit.offset(); } }
        SeedSlice {data:commit.bytes().as_ptr(),len:commit.bytes().len()}
    } else { SeedSlice {data:std::ptr::null(),len:0} }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_leaf_finish(handle: *mut c_void, written: bool) -> *mut c_void {
    output((|| {
        if handle.is_null() { return Err(invalid("Missing leaf writer")); }
        let writer = unsafe { &mut *handle.cast::<LeafWriter>() };
        let commit=writer.pending.take().ok_or_else(||invalid("No pending leaf write"))?;
        if written { writer.value.as_mut().map_err(|e|invalid(e.as_str()))?.accept(commit)?; }
        Ok(Vec::new())
    })())
}
/// Catalog modes: 0 returns file candidates for host stat/read; 1 returns the
/// existing inventory PIL; 2/3 return the cache directory as PIL/JSON.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_trie_catalog(handle: *const c_void, cache: SeedSlice, files: SeedSlice, mode: u32) -> *mut c_void {
    output((|| {
        let cache = std::str::from_utf8(unsafe { input(cache.data, cache.len)? }).map_err(|e| invalid(e.to_string()))?;
        let reader = unsafe { registered_trunk(handle)? };
        let mut catalog = reader.trie().catalog(cache).map_err(invalid)?;
        if mode == 0 { return serde_json::to_vec(&catalog).map_err(|e| invalid(e.to_string())); }
        let value = if mode == 1 {
            let files: Vec<trie_trunk_taxonomy::catalog::File> = serde_json::from_slice(unsafe { input(files.data, files.len)? }).map_err(|e| invalid(e.to_string()))?;
            catalog.files_present(&files).map_err(invalid)?;
            serde_json::to_value(catalog).map_err(|e| invalid(e.to_string()))?
        } else if mode == 2 || mode == 3 {
            serde_json::json!({"caches":catalog.sources,"collections":catalog.collections})
        } else { return Err(invalid("Unknown catalog operation")); };
        if mode == 3 { return serde_json::to_vec(&value).map_err(|e| invalid(e.to_string())); }
        pil_codec::object::encode_value(&pil_codec::json_view::native_json(&value)?)
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_trie_prescribe_file(handle: *const c_void, file: SeedSlice, bytes: SeedSlice) -> *mut c_void {
    output((|| {
        let file = std::str::from_utf8(unsafe { input(file.data, file.len)? })
            .map_err(|e| invalid(e.to_string()))?;
        let bytes = unsafe { input(bytes.data, bytes.len)? };
        let mut trunk = unsafe { registered_trunk(handle)? };
        // File publication and trie publication are separate. Check the exact
        // resource revision at this I/O boundary, including annotation edits.
        if !trunk.trie().definition_file(file).map_err(invalid)?.matches(bytes) {
            return Err(invalid("Definition resource changed during registration publication"));
        }
        trunk.prescribe_file(file, bytes)?;
        Ok(Vec::new())
    })())
}
struct Output {
    bytes: Vec<u8>,
    error: String,
}
pub(crate) fn invalid(message: impl Into<String>) -> Error {
    Error::Definition(message.into())
}
// Host policy chooses an algorithm, never PIL/PAC wire flags.
pub(crate) fn compression_algorithm(code: u32) -> Result<pil_codec::compression::Algorithm, Error> {
    match code {
        1 => Ok(pil_codec::compression::Algorithm::Zstd),
        2 => Ok(pil_codec::compression::Algorithm::Gzip),
        3 => Ok(pil_codec::compression::Algorithm::Brotli),
        _ => Err(invalid("unsupported Seed compression algorithm")),
    }
}
pub(crate) fn output(result: Result<Vec<u8>, Error>) -> *mut c_void {
    let result = match result {
        Ok(bytes) => Output {
            bytes,
            error: String::new(),
        },
        Err(e) => Output {
            bytes: vec![],
            error: e.to_string(),
        },
    };
    Box::into_raw(Box::new(result)).cast()
}
pub(crate) unsafe fn input<'a, T>(ptr: *const T, len: usize) -> Result<&'a [T], Error> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() || len > isize::MAX as usize / std::mem::size_of::<T>() {
        return Err(invalid("invalid native input slice"));
    }
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}
unsafe fn definition<'a>(handle: *const c_void) -> Result<&'a Arc<SeedDefinition>, Error> {
    if handle.is_null() {
        return Err(invalid("missing Seed Definition"));
    }
    unsafe { &*handle.cast::<Codec>() }
        .definition
        .as_ref()
        .map_err(invalid)
}

/// The input is the existing DOC JSON view, not a server-specific schema.
/// Author through DOC's builder, then sign; no record layout is built here.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_document(bytes: SeedSlice) -> *mut c_void {
    output((|| {
        let source: Vec<pil_codec::authoring_language::wire::Definition> =
            serde_json::from_slice(unsafe { input(bytes.data, bytes.len)? })
                .map_err(|e| invalid(e.to_string()))?;
        let first = source
            .first()
            .ok_or_else(|| invalid("empty Definition document"))?;
        let mut document = doc::new(&first.header.rx_name);
        for (i, member) in source.into_iter().enumerate() {
            if member.header.pil_version != 0 {
                return Err(invalid("unsupported PIL version"));
            }
            if i != 0 {
                document = document.definition(member.header.rx_name);
            }
            document = document
                .version(member.header.rx_major, member.header.rx_minor)
                .namespace(member.header.namespace);
            for field in member.fields {
                document = document.field(doc::Field::from_field(field));
            }
        }
        Ok(document.select(0).sign()?.bytes()?.to_vec())
    })())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_register(bytes: SeedSlice) -> *mut c_void {
    unsafe { trunk_seed_register_member(bytes, 0) }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_register_member(
    bytes: SeedSlice,
    member: usize,
) -> *mut c_void {
    let definition = unsafe { input(bytes.data, bytes.len) }
        .and_then(doc::read)
        .and_then(|document| SeedDefinition::prescribe_member(&document, member))
        .map_err(|e| e.to_string());
    Box::into_raw(Box::new(Codec { definition })).cast()
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_register_error(handle: *const c_void) -> SeedSlice {
    let codec = unsafe { &*handle.cast::<Codec>() };
    let text = codec.definition.as_ref().err().map_or("", String::as_str);
    SeedSlice {
        data: text.as_ptr(),
        len: text.len(),
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_register_free(handle: *mut c_void) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle.cast::<Codec>()) });
    }
}

unsafe fn set_values<'a>(
    seed: &mut data_seeds::Seed<'a>,
    values: &'a [SeedValue],
) -> Result<(), Error> {
    for value in values {
        let ordinal = value.ordinal;
        match value.kind {
            0 => {
                seed.set_absent(ordinal)?;
            }
            1 => {
                seed.set_null(ordinal)?;
            }
            2 => {
                // Already canonical field bytes, not a framed Pill.
                seed.set(
                    ordinal,
                    FieldBytes::Bytes(unsafe { input(value.bytes.data, value.bytes.len)? }),
                )?;
            }
            3 => {
                seed.set_uint(
                    ordinal,
                    (u128::from(value.high) << 64) | u128::from(value.low),
                )?;
            }
            4 => {
                seed.set_int(
                    ordinal,
                    ((u128::from(value.high) << 64) | u128::from(value.low)) as i128,
                )?;
            }
            5 => {
                seed.set_float(ordinal, value.real)?;
            }
            6 => {
                if value.low > 1 {
                    return Err(invalid("invalid native boolean"));
                }
                seed.set_bool(ordinal, value.low != 0)?;
            }
            7 => {
                let text =
                    std::str::from_utf8(unsafe { input(value.bytes.data, value.bytes.len)? })
                        .map_err(|e| invalid(e.to_string()))?;
                seed.set_text_string(ordinal, text)?;
            }
            8 => {
                seed.set_byte_string(ordinal, unsafe {
                    input(value.bytes.data, value.bytes.len)?
                })?;
            }
            9 => {
                // Native structured input bridge. The shared codec performs all
                // array/map/object/enum encoding using the registered shape.
                let bytes = unsafe { input(value.bytes.data, value.bytes.len)? };
                let mut cursor = std::io::Cursor::new(bytes);
                let value =
                    ciborium::from_reader(&mut cursor).map_err(|e| invalid(e.to_string()))?;
                if cursor.position() as usize != bytes.len() {
                    return Err(invalid("trailing native value"));
                }
                seed.set_native(ordinal, Some(value))?;
            }
            _ => return Err(invalid("unknown native setter")),
        }
    }
    Ok(())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_ingest(
    handle: *const c_void,
    values: *const SeedValue,
    count: usize,
    concise: bool,
) -> *mut c_void {
    output((|| {
        let definition = unsafe { definition(handle)? };
        let values = unsafe { input(values, count)? };
        let mut seed =
            definition.ingest(values, |values, seed| unsafe { set_values(seed, values) })?;
        if concise {
            seed.squeeze()?;
        }
        seed.cap()?;
        Ok(seed.bytes()?.into_owned())
    })())
}

/// Assemble capped ingress Pills through the existing shared Pack implementation.
/// Recovery/live snapshots retain their unsealed lifecycle; only sealed output
/// may be compressed or inserted into a Box.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_pack(
    handle: *const c_void,
    members: *const SeedSlice,
    count: usize,
    hour: u64,
    sealed: bool,
    algorithm: u32,
    level: u32,
) -> *mut c_void {
    output((|| {
        let definition = unsafe { definition(handle)? };
        let members = unsafe { input(members, count)? };
        let mut pack = definition.fill(count)?;
        pack.hour(hour)?;
        if sealed && algorithm == 0 {
            pack.header(data_seeds::integrity::HEADER, vec![0; 4])?;
        }
        for member in members {
            pack.load(unsafe { input(member.data, member.len)? })?;
        }
        if !sealed {
            if algorithm != 0 {
                return Err(invalid("compression requires a sealed Pack"));
            }
            return pack.snapshot();
        }
        pack.seal()?;
        let mut bytes = pack.into_bytes()?;
        if algorithm == 0 {
            data_seeds::integrity::stamp(&mut bytes)?;
            return Ok(bytes);
        }
        let algorithm = compression_algorithm(algorithm)?;
        let mut compressed = Vec::new();
        pil_codec::compression::seal(&bytes, algorithm, level, false, &mut compressed)
            .map_err(|e| invalid(e.to_string()))?;
        Ok(compressed)
    })())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_compress(bytes: SeedSlice, algorithm: u32, level: u32) -> *mut c_void {
    output((|| {
        let raw = unsafe { input(bytes.data, bytes.len)? };
        let mut compressed = Vec::new();
        pil_codec::compression::seal(raw, compression_algorithm(algorithm)?, level, false, &mut compressed)
            .map_err(|e| invalid(e.to_string()))?;
        Ok(compressed)
    })())
}

/// 0 borrows the original plain bytes without allocating an output handle.
/// 1 returns owned expanded bytes; -1 returns an error output handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_expand(bytes: SeedSlice) -> SeedExpansion {
    let decoded = (|| {
        let raw = unsafe { input(bytes.data, bytes.len)? };
        let mut expanded = Vec::new();
        if pil_codec::compression::unseal(raw, &mut expanded).map_err(|e| invalid(e.to_string()))? {
            Ok(Some(expanded))
        } else {
            Ok(None)
        }
    })();
    match decoded {
        Ok(None) => SeedExpansion { status: 0, output: std::ptr::null_mut() },
        Ok(Some(bytes)) => SeedExpansion { status: 1, output: output(Ok(bytes)) },
        Err(error) => SeedExpansion { status: -1, output: output(Err(error)) },
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_envelope(bytes: SeedSlice, size: *mut u64) -> i32 {
    if size.is_null() {
        return -1;
    }
    match unsafe { input(bytes.data, bytes.len) }.and_then(pil_codec::pac_envelope::stored_length) {
        Ok(n) => {
            unsafe { *size = n };
            0
        }
        Err(Error::Incomplete) => 1,
        Err(_) => -1,
    }
}

// Snapshot-owned dictionary. The HTTP adapter releases it with the pinned files.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_strings_open(
    footer: SeedSlice, length: u64, publication: *mut BoxPublication,
) -> *mut c_void {
    if publication.is_null() { return std::ptr::null_mut(); }
    let parsed = unsafe { input(footer.data, footer.len) }.and_then(|raw| {
        let container = pil_codec::r#box::Box::open_footer_owned(raw.to_vec(), length)?;
        let info = data_seeds::packs::Publication::from_container(&container)?;
        unsafe { *publication = BoxPublication { length: info.length, count: info.count }; }
        Ok(container)
    });
    parsed.map_or(std::ptr::null_mut(), |b| Box::into_raw(Box::new(b)).cast())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_strings_free(handle: *mut c_void) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle.cast::<pil_codec::r#box::Box<'static>>()) });
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_strings(handle: *const c_void, ids: *const u32, count: usize) -> *mut c_void {
    output((|| {
        if ids.is_null() || !(1..=256).contains(&count) { return Err(Error::Invalid("invalid string selection")); }
        let dir = unsafe { handle.cast::<pil_codec::r#box::Box<'static>>().as_ref() }.ok_or(Error::Incomplete)?;
        let ids = unsafe { std::slice::from_raw_parts(ids, count) };
        let entries = ids.iter().map(|&id| Ok((id, dir.string(id as usize)
            .ok_or(Error::Invalid("unknown BOX string ordinal"))?))).collect::<Result<Vec<_>,Error>>()?;
        serde_json::to_vec(&entries).map_err(|e| Error::Definition(e.to_string()))
    })())
}

#[repr(C)]
pub struct BoxPublication { length: u64, count: u64 }

/// Bounded footer discovery; no Time traversal or member reads.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_box_footer(tail: SeedSlice, length: u64, offset: *mut u64) -> i32 {
    if offset.is_null() { return -1; }
    match unsafe { input(tail.data, tail.len) }.and_then(|raw| pil_codec::r#box::Box::footer_range(raw, length)) {
        Ok(range) => { unsafe { *offset = range.start }; 0 }
        Err(_) => -1,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_packs_publication(footer: SeedSlice, length: u64, out: *mut BoxPublication) -> i32 {
    if out.is_null() { return -1; }
    match unsafe { input(footer.data, footer.len) }.and_then(|raw| data_seeds::packs::Publication::read(raw, length)) {
        Ok(value) => { unsafe { *out = BoxPublication { length: value.length, count: value.count } }; 0 }
        Err(_) => -1,
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_output_bytes(handle: *const c_void) -> SeedSlice {
    let result = unsafe { &*handle.cast::<Output>() };
    SeedSlice {
        data: result.bytes.as_ptr(),
        len: result.bytes.len(),
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_output_error(handle: *const c_void) -> SeedSlice {
    let result = unsafe { &*handle.cast::<Output>() };
    SeedSlice {
        data: result.error.as_ptr(),
        len: result.error.len(),
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_output_free(handle: *mut c_void) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle.cast::<Output>()) });
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_output_adopt(
    handle: *mut c_void,
    pool: *const c_void,
) -> *mut c_void {
    if handle.is_null() {
        return std::ptr::null_mut();
    }
    let result = unsafe { &mut *handle.cast::<Output>() };
    if !result.error.is_empty() {
        return std::ptr::null_mut();
    }
    match unsafe { crate::buffers::adopt_output(pool, std::mem::take(&mut result.bytes)) } {
        Ok(buffer) => buffer,
        Err(bytes) => {
            result.bytes = bytes;
            std::ptr::null_mut()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_seeds::{Compare, Predicate, Query, Sources};
    fn span(bytes: &[u8]) -> SeedSlice {
        SeedSlice {
            data: bytes.as_ptr(),
            len: bytes.len(),
        }
    }
    fn value(ordinal: usize, kind: u32, low: u64, bytes: &[u8]) -> SeedValue {
        SeedValue {
            ordinal,
            kind,
            low,
            high: 0,
            real: 0.,
            bytes: span(bytes),
        }
    }
    unsafe fn owned(handle: *mut c_void) -> Vec<u8> {
        let result = unsafe { Box::from_raw(handle.cast::<Output>()) };
        assert!(result.error.is_empty(), "{}", result.error);
        result.bytes
    }
    fn document() -> doc::Document {
        doc::new("native-ingress")
            .field(
                doc::Field::new("time")
                    .r#type("uint")
                    .size(6)
                    .annotation("@time"),
            )
            .field(
                doc::Field::new("id")
                    .r#type("bytes")
                    .size(10)
                    .annotation("@event"),
            )
            .field(doc::Field::new("duration").r#type("uint").size(4))
            .field(doc::Field::new("message").r#type("tstr"))
            .field(doc::Field::new("attributes").array("any").element_size(0))
            .sign()
            .unwrap()
    }
    #[test]
    fn existing_compression_adapter_borrows_plain_and_preserves_all_algorithms() {
        let def = SeedDefinition::prescribe(&document()).unwrap();
        let mut pack = def.fill(2).unwrap();
        for n in 0..2 {
            pack.load(def.ingest(n, |n, seed| {
                seed.set_uint("time", n)?
                    .set_text_string("message", if n == 0 { "first" } else { "second" })?;
                Ok(())
            }).unwrap()).unwrap();
        }
        pack.seal().unwrap();
        let plain = pack.into_bytes().unwrap();
        unsafe {
            let result = trunk_seed_expand(span(&plain));
            assert_eq!(result.status, 0);
            assert!(result.output.is_null(), "plain input must not allocate an output");
            for algorithm in 1..=3 {
                let encoded = owned(trunk_seed_compress(span(&plain), algorithm, 1));
                let result = trunk_seed_expand(span(&encoded));
                assert_eq!(result.status, 1);
                assert_eq!(owned(result.output), plain);
                let result = trunk_seed_expand(span(&encoded[..encoded.len()-1]));
                assert_eq!(result.status, -1);
                assert!(!result.output.is_null());
                assert!(trunk_seed_output_error(result.output).len > 0);
                trunk_seed_output_free(result.output);
            }
            let result = trunk_seed_expand(span(&[]));
            assert_eq!(result.status, -1);
            assert!(trunk_seed_output_error(result.output).len > 0);
            trunk_seed_output_free(result.output);
        }
    }
    #[test]
    fn native_ingress_to_pack_to_seed_query_uses_shared_codecs() {
        let doc = document();
        let json = serde_json::to_vec(doc.definitions()).unwrap();
        let raw = unsafe { owned(trunk_seed_document(span(&json))) };
        assert_eq!(doc.bytes().unwrap(), raw);
        let codec = unsafe { trunk_seed_register(span(&raw)) };
        assert_eq!(unsafe { trunk_seed_register_error(codec) }.len, 0);
        let def = SeedDefinition::read(&raw).unwrap();
        let hour = 3_600_000;
        let attributes = ciborium::Value::Array(vec![ciborium::Value::Map(vec![(
            ciborium::Value::Text("unknown".into()),
            ciborium::Value::Bool(true),
        )])]);
        let mut structured = vec![];
        ciborium::into_writer(&attributes, &mut structured).unwrap();
        let mut members = vec![];
        // All test records are created by typed ingress, never byte fixtures.
        for i in 0..3 {
            let id = [i as u8 + 1; 10];
            let fields = [
                value(0, 3, hour + i, &[]),
                value(1, 8, 0, &id),
                value(2, 3, 10_000 + i, &[]),
                value(3, 7, 0, b"received"),
                value(4, 9, 0, &structured),
            ];
            members.push(unsafe {
                owned(trunk_seed_ingest(
                    codec,
                    fields.as_ptr(),
                    fields.len(),
                    true,
                ))
            });
        }
        let spans: Vec<_> = members.iter().map(|v| span(v)).collect();
        for (sealed, algorithm) in [(false, 0), (true, 0), (true, 1), (true, 2), (true, 3)] {
            let bytes = unsafe {
                owned(trunk_seed_pack(
                    codec,
                    spans.as_ptr(),
                    spans.len(),
                    hour,
                    sealed,
                    algorithm,
                    1,
                ))
            };
            let mut length = 0;
            assert_eq!(
                unsafe { trunk_seed_envelope(span(&bytes[..1]), &mut length) },
                1
            );
            assert_eq!(
                unsafe { trunk_seed_envelope(span(&bytes[..bytes.len().min(27)]), &mut length) },
                0
            );
            assert_eq!(length, bytes.len() as u64);
            if sealed && algorithm == 0 {
                data_seeds::integrity::verify(&bytes).unwrap();
                let mut corrupt = bytes.clone();
                // Header metadata stays parseable; changing an application
                // value must be detected independently of record validation.
                let value = pil_codec::pac::application_header(&bytes, "seed.hour")
                    .unwrap()
                    .unwrap();
                let at = value.as_ptr() as usize - bytes.as_ptr() as usize;
                corrupt[at] ^= 1;
                assert!(
                    data_seeds::integrity::verify(&corrupt)
                        .unwrap_err()
                        .to_string()
                        .contains("mismatch")
                );
                let borrowed = pil_codec::pac::application_header(&bytes, "crc32c")
                    .unwrap()
                    .unwrap();
                let at = borrowed.as_ptr() as usize - bytes.as_ptr() as usize;
                assert_eq!(&bytes[at..at + 4], borrowed);
                assert!(
                    pil_codec::pac::application_header(&bytes, "not-present")
                        .unwrap()
                        .is_none()
                );
                for end in 0..9 {
                    assert!(pil_codec::pac::application_header(&bytes[..end], "crc32c").is_err());
                }
            }
            let pack = def.open(bytes).unwrap();
            assert_eq!(pack.is_sealed(), sealed);
            assert_eq!(pack.hour().unwrap(), Some(hour));
            let mut sources = Sources::new();
            sources.pack(pack);
            let mut cursor = sources
                .query(Query::new().filter(Predicate::compare("duration", Compare::Gt, 10_000)))
                .unwrap();
            let batch = cursor.fetch(10).unwrap();
            assert_eq!(batch.len(), 2);
            let objects = batch.objects().unwrap();
            assert_eq!(
                objects[0]
                    .iter()
                    .find(|(n, _)| n == "attributes")
                    .unwrap()
                    .1,
                attributes
            );
            assert_eq!(
                objects[1].iter().find(|(n, _)| n == "time").unwrap().1,
                ciborium::Value::Integer((hour + 2).into())
            );
        }
        unsafe {
            trunk_seed_register_free(codec);
        }
    }
    #[test]
    fn stored_empty_and_common_only_packs_keep_count_and_checksum() {
        let document = document();
        let definition = SeedDefinition::prescribe(&document).unwrap();
        let codec = unsafe { trunk_seed_register(span(document.bytes().unwrap())) };
        let fields = [value(2, 3, 42, &[])];
        let member = unsafe {
            owned(trunk_seed_ingest(
                codec,
                fields.as_ptr(),
                fields.len(),
                false,
            ))
        };
        for count in [0, 1, 100] {
            let members = vec![span(&member); count];
            let bytes = unsafe {
                owned(trunk_seed_pack(
                    codec,
                    members.as_ptr(),
                    count,
                    0,
                    true,
                    0,
                    0,
                ))
            };
            data_seeds::integrity::verify(&bytes).unwrap();
            let pack = definition.open_stored(bytes).unwrap();
            assert_eq!(pack.len(), count);
            let mut sources = data_seeds::Sources::new();
            sources.pack(pack);
            let mut cursor = sources
                .query(data_seeds::Query::default().limit(100))
                .unwrap();
            assert_eq!(cursor.fetch(100).unwrap().len(), count);
        }
        unsafe {
            trunk_seed_register_free(codec);
        }
    }
    #[test]
    fn compressed_ingress_cannot_bypass_expanded_receive_limit() {
        let document = document();
        let definition = SeedDefinition::prescribe(&document).unwrap();
        let codec = unsafe { trunk_seed_register(span(document.bytes().unwrap())) };
        let text = "received ".repeat(40_000);
        let fields = [value(3, 7, 0, text.as_bytes())];
        let member = unsafe {
            owned(trunk_seed_ingest(
                codec,
                fields.as_ptr(),
                fields.len(),
                false,
            ))
        };
        let members = [span(&member)];
        for algorithm in [1, 2, 3] {
            let bytes = unsafe {
                owned(trunk_seed_pack(
                    codec,
                    members.as_ptr(),
                    1,
                    0,
                    true,
                    algorithm,
                    1,
                ))
            };
            assert!(bytes.len() < 65_536);
            let mut expanded = Vec::new();
            let error = pil_codec::compression::unseal_with_limit(&bytes, 65_536, &mut expanded)
                .unwrap_err();
            assert!(error.to_string().contains("receive limit"));
            assert!(expanded.len() <= 65_537);
            pil_codec::compression::unseal_with_limit(&bytes, 1_000_000, &mut expanded).unwrap();
            let exact = expanded.len();
            pil_codec::compression::unseal_with_limit(&bytes, exact, &mut expanded).unwrap();
            assert!(
                pil_codec::compression::unseal_with_limit(&bytes, exact - 1, &mut expanded)
                    .is_err()
            );
            let end = bytes.len();
            let bytes = Arc::new(bytes);
            assert!(
                definition
                    .open_stored_range_limited(bytes.clone(), 0..end, 65_536)
                    .is_err()
            );
            let pack = definition
                .open_stored_range_limited(bytes, 0..end, exact)
                .unwrap();
            assert_eq!(pack.len(), 1);
        }
        unsafe {
            trunk_seed_register_free(codec);
        }
    }
    #[test]
    fn definition_document_rejects_resource_binding_fields() {
        let document = document();
        for (location, property) in [
            (0, "resource"),
            (0, "cddl"),
            (0, "adaptive_integers"),
            (1, "resource"),
            (1, "service"),
            (1, "timezone"),
            (2, "resource"),
            (2, "storage_offset"),
        ] {
            let mut source = serde_json::to_value(document.definitions()).unwrap();
            let target = match location {
                0 => &mut source[0],
                1 => &mut source[0]["header"],
                _ => &mut source[0]["fields"][0],
            };
            target.as_object_mut().unwrap().insert(
                property.into(),
                serde_json::json!({"service":"payment","timezone":"UTC"}),
            );
            let bytes = serde_json::to_vec(&source).unwrap();
            let result =
                unsafe { Box::from_raw(trunk_seed_document(span(&bytes)).cast::<Output>()) };
            assert!(result.error.contains("unknown field"));
            assert!(result.bytes.is_empty());
        }
    }
    #[test]
    fn native_errors_do_not_publish_output_and_setters_reject_type_mismatches() {
        let doc = document();
        let codec = unsafe { trunk_seed_register(span(doc.bytes().unwrap())) };
        for fields in [
            vec![value(2, 7, 0, b"not a number")],
            vec![value(900, 3, 1, &[])],
            vec![value(0, 99, 1, &[])],
        ] {
            let result = unsafe {
                Box::from_raw(
                    trunk_seed_ingest(codec, fields.as_ptr(), fields.len(), false).cast::<Output>(),
                )
            };
            assert!(!result.error.is_empty());
            assert!(result.bytes.is_empty());
        }
        let result = unsafe {
            Box::from_raw(
                trunk_seed_pack(codec, std::ptr::null(), 0, 0, false, 1, 1).cast::<Output>(),
            )
        };
        assert!(result.error.contains("sealed"));
        assert!(result.bytes.is_empty());
        unsafe {
            trunk_seed_register_free(codec);
        }
        let codec = unsafe { trunk_seed_register(span(&[0])) };
        assert_ne!(unsafe { trunk_seed_register_error(codec) }.len, 0);
        unsafe {
            trunk_seed_register_free(codec);
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_leaf_labels(handle: *const c_void, branch: u16, file: SeedSlice) -> *mut c_void {
    output((|| {
        let trunk = unsafe { registered_trunk(handle)? };
        let labels = trunk.trie().branches().select([branch]).leaves()?.labels(unsafe { input(file.data, file.len)? })?;
        serde_json::to_vec(&labels).map_err(|e| invalid(e.to_string()))
    })())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trunk_seed_leaf_label(handle: *const c_void, branch: u16, file: SeedSlice, leaf: u16, label: SeedSlice) -> *mut c_void {
    output((|| {
        let trunk = unsafe { registered_trunk(handle)? };
        let label = std::str::from_utf8(unsafe { input(label.data, label.len)? }).map_err(|e| invalid(e.to_string()))?;
        trunk.trie().branches().select([branch]).leaves()?.label(unsafe { input(file.data, file.len)? }, leaf, (!label.is_empty()).then_some(label))
    })())
}
