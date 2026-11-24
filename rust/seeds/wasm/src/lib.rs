//! Runtime adapter only. Definitions, queries, and codec operations stay in Rust.
mod handles;
use data_seeds::request::{context, query, predicate, required_text};
mod storage;
use handles::Handles;
use ciborium::Value as Data;
use data_seeds::{
    Cursor, Error, OwnedBatchReader, Packs, Predicate, Query, SeedDefinition,
    SeedPackSnapshot,
};
use pil_codec::{Array, Value, api::NativeValue};
use serde_json::Value as Json;
use std::{cell::RefCell, collections::VecDeque, sync::Arc};

#[unsafe(no_mangle)]
pub extern "C" fn seed_ports_sample(source_bytes: f64, sample_length:usize) -> u32 {
    run(|r| {
        if !source_bytes.is_finite() || source_bytes < sample_length as f64 || source_bytes.fract() != 0.0 || source_bytes > 9007199254740991.0 || sample_length > 4 << 20 || sample_length>r.input.len() || r.input.len()-sample_length>32768 {
            return Err(fail("Invalid source sample size"));
        }
        let profile=serde_json::from_slice(&r.input[sample_length..]).map_err(|e|fail(e.to_string()))?;
        let candidate = data_seeds::ports::Ports.sample_with(&r.input[..sample_length], data_seeds::ports::Limits::default(), Some(source_bytes as u64),profile)?;
        json_value(&serde_json::to_value(candidate).map_err(|e| fail(e.to_string()))?)?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn seed_ports_design() -> u32 {
    run(|r| {
        let draft: data_seeds::ports::design::Design = serde_json::from_slice(&r.input).map_err(|e|fail(e.to_string()))?;
        json_value(&draft.run()?)?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn seed_ports_review() -> u32 {
    run(|r| {
        let mapping = serde_json::from_slice(&r.input).map_err(|e| fail(e.to_string()))?;
        let review = data_seeds::ports::Ports.review(&mapping)?;
        json_value(&serde_json::to_value(review).map_err(|e| fail(e.to_string()))?)?;
        Ok(0)
    })
}

enum ReceiveDefinition {
    Single(Arc<SeedDefinition>),
    Registered(data_seeds::registration::Packs),
}
struct Receiver {
    definition: ReceiveDefinition,
    stream: Option<data_seeds::replication::PackStream>,
    pending: Option<data_seeds::replication::VerifiedPack>,
    missing: Vec<usize>,
    ready: VecDeque<Arc<SeedPackSnapshot>>,
    stored: bool,
    verify_write: bool,
    maximum: usize,
    chunk: Option<(Arc<Vec<u8>>,usize)>,
}

enum Store {
    Unbound(Packs),
    Registered(data_seeds::registration::Packs),
    Reader(data_seeds::reader::Reader),
}
impl Store {
    fn query(&self, query: Query) -> Result<Cursor, Error> {
        match self { Self::Unbound(store) => store.query(query), Self::Registered(store) => store.query(query), Self::Reader(reader) => reader.query(query) }
    }
    fn registered(&mut self) -> Result<&mut data_seeds::registration::Packs, Error> {
        match self { Self::Registered(store) => Ok(store), _ => Err(fail("Expected a registered PACKS scope")) }
    }
}
impl From<Packs> for Store {
    fn from(store: Packs) -> Self { Self::Unbound(store) }
}

#[link(wasm_import_module = "seed_host")]
unsafe extern "C" {
    fn absent();
    fn null();
    fn boolean(v: u32);
    fn number(v: f64);
    fn signed(v: i64);
    fn unsigned(v: u64);
    fn big_integer(p: *const u8, n: usize);
    fn text(p: *const u8, n: usize);
    fn bytes(p: *const u8, n: usize);
    fn array();
    fn object();
    fn map();
    fn end();
    fn key(p: *const u8, n: usize);
    fn shape();
    fn name(p: *const u8, n: usize);
    fn missing(i: usize);
    fn view(row: usize);
    fn record();
    fn field(i: usize);
}
#[derive(Default)]
struct Runtime {
    input: Vec<u8>,
    error: String,
    definitions: Handles<Arc<SeedDefinition>>,
    packs: Handles<Arc<SeedPackSnapshot>>,
    stores: Handles<Store>,
    cursors: Handles<Cursor>,
    batches: Handles<OwnedBatchReader>,
    staged_box: Option<Arc<Vec<u8>>>,
    receivers: Handles<Receiver>,
    leaves: Handles<Arc<data_seeds::LeafTrie>>,
    trunks: Handles<data_seeds::registration::Trunk>,
    times: Handles<Arc<data_seeds::TimeTrie>>,
    query_memory: Option<data_seeds::MemoryBudget>,
    resident_memory: Option<data_seeds::MemoryBudget>,
    readiness: Option<data_seeds::replication::Readiness>,
    read_owners: Handles<data_seeds::replication::Readiness>,
    replicas: Handles<data_seeds::replication::Replica>,
    writes: Handles<storage::PendingWrite>,
    collections: Handles<data_seeds::replication::Collection>,
    summaries: Handles<data_seeds::summaries::SummaryReader>,
    summary_plans: Handles<(usize, u16)>,
    summary_reports: Handles<data_seeds::summaries::SummaryReport>,
    summary_tables: Handles<data_seeds::summaries::SummaryTable>,
    growth: Handles<data_seeds::stalks::Growth>,
    stalks: Handles<data_seeds::stalks::Stalks>,
}
thread_local! {static R:RefCell<Runtime>=RefCell::new(Runtime::default());}
fn fail(s: impl Into<String>) -> Error {
    Error::Definition(s.into())
}
fn get<T>(v: &Handles<T>, i: usize) -> Result<&T, Error> {
    v.get(i).ok_or_else(|| fail("released or invalid handle"))
}
fn get_mut<T>(v: &mut Handles<T>, i: usize) -> Result<&mut T, Error> {
    v.get_mut(i).ok_or_else(|| fail("released or invalid handle"))
}
fn add<T>(v: &mut Handles<T>, t: T) -> Result<usize, Error> { v.insert(t).map_err(fail) }
fn run(f: impl FnOnce(&mut Runtime) -> Result<usize, Error>) -> u32 {
    R.with_borrow_mut(|r| match f(r) {
        Ok(v) => u32::try_from(v).unwrap_or(u32::MAX),
        Err(e) => {
            r.error = e.to_string();
            u32::MAX
        }
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_input(n: usize) -> *mut u8 {
    R.with_borrow_mut(|r| {
        if r.input
            .try_reserve(n.saturating_sub(r.input.len()))
            .is_err()
        {
            r.error = "input allocation failed".into();
            return std::ptr::null_mut();
        }
        r.input.resize(n, 0);
        r.input.as_mut_ptr()
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_error_ptr() -> *const u8 {
    R.with_borrow(|r| r.error.as_ptr())
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_error_len() -> usize {
    R.with_borrow(|r| r.error.len())
}
/// Control-resource values use the same Rust PIL object codec as the origin.
#[unsafe(no_mangle)]
pub extern "C" fn seed_control_read() -> u32 {
    run(|r| {
        data(pil_codec::object::decode_value(&r.input)?)?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_control_write() -> u32 {
    run(|r| {
        let value: Json = serde_json::from_slice(&r.input).map_err(|e| fail(e.to_string()))?;
        let value = pil_codec::json_view::native_json(&value)?;
        let encoded = pil_codec::object::encode_value(&value)?;
        unsafe { bytes(encoded.as_ptr(), encoded.len()); }
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_prescribe() -> u32 {
    run(|r| add(&mut r.definitions, SeedDefinition::read(&r.input)?))
}
/// Calendar selection and annotation interpretation stay in the shared Rust
/// library. The host only transfers the resulting control values.
#[unsafe(no_mangle)]
pub extern "C" fn seed_summary_window(from: u64, to: u64) -> u32 {
    run(|_| {
        let window = data_seeds::summaries::SummaryWindow::new(from..to)?;
        let covered = window.covered();
        json_value(&serde_json::json!({
            "requested": { "from": from, "to": to },
            "from": covered.start, "to": covered.end,
            "period": data_seeds::summaries::SUMMARY_PERIOD_MS,
            "periods": window.period_count(),
        }))?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_pack_size() -> u32 {
    run(|r| {
        let size = match pil_codec::pac_envelope::stored_length(&r.input) {
            Ok(size) => Some(size), Err(Error::Incomplete) => None, Err(e) => return Err(e),
        };
        json_value(&serde_json::json!(size))?; Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_pack_expand(maximum: usize) -> u32 {
    run(|r| {
        if r.input.len() > maximum { return Err(fail("Pack exceeds receive limit")); }
        let mut expanded = Vec::new();
        let changed = pil_codec::compression::unseal_with_limit(&r.input, maximum, &mut expanded).map_err(|e| fail(e.to_string()))?;
        let raw = if changed { expanded.as_slice() } else { r.input.as_slice() };
        if pil_codec::pac::stored_size(raw, maximum)? != raw.len() { return Err(fail("Pack extent differs from envelope")); }
        unsafe { bytes(raw.as_ptr(), raw.len()); } Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_summary_analytics(d: usize) -> u32 {
    run(|r| {
        let fields: Vec<_> = get(&r.definitions, d)?.summary_analytics().iter()
            .map(|v| serde_json::json!({"ordinal": v.ordinal(), "field": v.name(), "percentiles": v.percentiles()})).collect();
        json_value(&serde_json::json!(fields))?;
        Ok(0)
    })
}
/// The selected taxonomy branch supplies the day's historical event RX and annotations;
/// daily PACs supply their bound array dimensions.
#[unsafe(no_mangle)]
pub extern "C" fn seed_registered_summary_open(t: usize, branch: u16, first: u64) -> u32 {
    run(|r| {
        let reader = data_seeds::summaries::SummaryReader::registered(
            get(&r.trunks, t)?, branch, std::mem::take(&mut r.input), first)?;
        add(&mut r.summaries, reader)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_registered_summary_box_open(t: usize, branch: u16, first: u64) -> u32 {
    run(|r| {
        let reader = data_seeds::summaries::SummaryReader::registered_box(
            get(&r.trunks, t)?, branch, std::mem::take(&mut r.input), first)?;
        add(&mut r.summaries, reader)
    })
}
/// Pin the owning branch; each day resolves its historical selection there.
#[unsafe(no_mangle)]
pub extern "C" fn seed_trunk_summaries(t: usize, branch: u16) -> u32 {
    run(|r| {
        get(&r.trunks, t)?.trie().route(branch).map_err(Error::Definition)?;
        add(&mut r.summary_plans, (t, branch))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_summary_plan_open(h: usize, first: u64, pack: bool) -> u32 {
    run(|r| {
        let (trunk, branch) = get(&r.summary_plans, h)?;
        let reader = if pack {
            data_seeds::summaries::SummaryReader::registered(
                get(&r.trunks, *trunk)?, *branch, std::mem::take(&mut r.input), first)?
        } else {
            data_seeds::summaries::SummaryReader::registered_box(
                get(&r.trunks, *trunk)?, *branch, std::mem::take(&mut r.input), first)?
        };
        add(&mut r.summaries, reader)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_summary_info(h: usize) -> u32 {
    run(|r| {
        let reader=get(&r.summaries,h)?;let extent=reader.extent()?;
        json_value(&serde_json::json!({"day":extent.start,"from":extent.start,"to":extent.end,"sealed":reader.sealed(),"hours":reader.hour_counts()?.iter().map(|(time,count)|serde_json::json!({"time":time,"count":count})).collect::<Vec<_>>()}))?;Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_summary_rows(h:usize,from:u64,to:u64)->u32 {
    run(|r| {
        let mut rows=Vec::new();
        get_mut(&mut r.summaries,h)?.rows(&data_seeds::summaries::SummaryWindow::new(from..to)?,|time,count,object|{
            let fields=Data::Map(object.into_iter().map(|(k,v)|(Data::Text(k),v)).collect());
            rows.push(Data::Map(vec![("time".into(),time.into()),("count".into(),count.into()),("fields".into(),fields)]));Ok(())
        })?;
        data(Data::Array(rows))?;Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_summary_report()->u32 {
    run(|r| {let v:Json=serde_json::from_slice(&r.input).map_err(|e|fail(e.to_string()))?;
        let integer=|name:&str|v.get(name).and_then(Json::as_u64).ok_or_else(||fail(format!("Invalid report {name}")));
        let memory=r.query_memory.get_or_insert_with(||data_seeds::MemoryBudget::new(64*1024*1024)).clone();
        add(&mut r.summary_reports,data_seeds::summaries::SummaryReport::new(integer("from")?,integer("to")?,integer("step")?,v.get("metric").and_then(Json::as_str).unwrap_or(""))?.context(context(&v)?).memory_budget(memory)?)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_summary_report_read(h:usize,reader:usize)->u32 {
    run(|r| {let v:Json=serde_json::from_slice(&r.input).map_err(|e|fail(e.to_string()))?;
        let from=v.get("from").and_then(Json::as_u64).ok_or_else(||fail("Invalid report start"))?;
        let to=v.get("to").and_then(Json::as_u64).ok_or_else(||fail("Invalid report end"))?;
        let filter=v.get("where").map(predicate).transpose()?.unwrap_or(Predicate::All);
        get_mut(&mut r.summary_reports,h)?.consume(get_mut(&mut r.summaries,reader)?,required_text(&v,"source")?,from,to,&filter)?;Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_summary_report_result(h:usize)->u32 {
    run(|r|{json_value(&get(&r.summary_reports,h)?.finish()?)?;Ok(0)})
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_summary_table()->u32 {
    run(|r|{
        #[derive(serde::Deserialize)]
        struct Spec {plan:Json,coverage:Vec<data_seeds::summaries::Coverage>,from:u64,to:u64,#[serde(default)] context:Json}
        let spec:Spec=serde_json::from_slice(&r.input).map_err(|e|fail(e.to_string()))?;
        let memory=r.query_memory.get_or_insert_with(||data_seeds::MemoryBudget::new(64*1024*1024)).clone();
        add(&mut r.summary_tables,data_seeds::summaries::SummaryTable::new(&spec.plan,spec.coverage,spec.from,spec.to)?.context(context(&serde_json::json!({"context":spec.context}))?).memory_budget(memory))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_summary_table_read(h:usize,reader:usize)->u32 {
    run(|r|{let v:Json=serde_json::from_slice(&r.input).map_err(|e|fail(e.to_string()))?;
        get_mut(&mut r.summary_tables,h)?.read(get_mut(&mut r.summaries,reader)?,required_text(&v,"source")?,required_text(&v,"definition")?)?;Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_summary_table_ranges(h:usize)->u32 {
    run(|r| {json_value(&serde_json::json!(get_mut(&mut r.summary_tables,h)?.remaining()?))?;Ok(0)})
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_summary_table_batch(h:usize,b:usize)->u32 {
    run(|r| {get_mut(&mut r.summary_tables,h)?.events(get(&r.batches,b)?.batch())?;Ok(0)})
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_summary_table_result(h:usize)->u32 {
    run(|r|{json_value(&get_mut(&mut r.summary_tables,h)?.finish()?)?;Ok(0)})
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_stalks(cursor:usize,rows:usize)->u32 {
    run(|r| {
        let cursor=r.cursors.take(cursor).ok_or_else(||fail("released or invalid query"))?;
        let memory=r.query_memory.get_or_insert_with(||data_seeds::MemoryBudget::new(64*1024*1024)).clone();
        add(&mut r.growth,cursor.stalks(memory,rows))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_stalks_poll(h:usize,work:usize)->u32 {
    run(|r|{let g=get_mut(&mut r.growth,h)?;let done=g.poll(work)?;json_value(&serde_json::json!({"done":done,"rows":g.len(),"bytes":g.bytes()}))?;Ok(0)})
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_stalks_finish(h:usize)->u32 {
    run(|r|{
        if !get(&r.growth,h)?.is_complete() {return Err(fail("Stalk growth is incomplete or cancelled"));}
        let g=r.growth.take(h).ok_or_else(||fail("released Stalk growth"))?;
        add(&mut r.stalks,g.finish()?)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_stalks_info(h:usize)->u32 {
    run(|r|{let s=get(&r.stalks,h)?;json_value(&serde_json::json!({"rows":s.len(),"bytes":s.bytes(),"chunks":s.chunks().len()}))?;Ok(0)})
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_stalks_sum(h:usize)->u32 {
    run(|r|{let name=std::str::from_utf8(&r.input).map_err(|_|fail("invalid column name"))?;
        match get(&r.stalks,h)?.sum(name)? {data_seeds::Cell::Absent=>unsafe{absent()},data_seeds::Cell::Value(v)=>data(v)?};Ok(0)})
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_stalks_objects(h:usize,first:usize,count:usize)->u32 {
    run(|r|{
        if count>4096 {return Err(fail("Stalk result page exceeds 4096 rows"));}
        let s=get(&r.stalks,h)?;let until=first.checked_add(count).ok_or(Error::Overflow)?.min(s.len());
        if first>s.len() {return Err(fail("Stalk result page outside rows"));}
        unsafe{array();}
        for row in first..until {data(Data::Map(s.object(row)?.into_iter().map(|(k,v)|(Data::Text(k),v)).collect()))?;}
        unsafe{end();}Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_summary_query(h: usize) -> u32 {
    run(|r| {
        let spec: Json = serde_json::from_slice(&r.input).map_err(|e| fail(e.to_string()))?;
        let from = spec.get("from").and_then(Json::as_u64).ok_or_else(|| fail("Invalid summary start"))?;
        let to = spec.get("to").and_then(Json::as_u64).ok_or_else(|| fail("Invalid summary end"))?;
        let window = data_seeds::summaries::SummaryWindow::new(from..to)?;
        let summary = get_mut(&mut r.summaries, h)?;
        if let Some(filter) = spec.get("where") {
            let predicate = predicate(filter)?;
            let result = summary.preview_with(&window, &predicate, &context(&spec)?)?;
            let count = result.count.map(|n| serde_json::json!({"lower":n.lower,"upper":n.upper}));
            json_value(&serde_json::json!({
                "requested":{"from":from,"to":to},"from":window.covered().start,"to":window.covered().end,
                "count":count,"coverageComplete":result.coverage_complete,"predicateComplete":result.predicate_complete,
                "fields":result.fields,"pending":result.pending,
                "eligible":result.eligible.iter().map(|r|[r.start,r.end]).collect::<Vec<_>>(),
                "periods":result.periods.iter().map(|p|serde_json::json!({"time":p.time,"lower":p.count.lower,"upper":p.count.upper,"complete":p.complete})).collect::<Vec<_>>(),
            }))?;
            return Ok(0);
        }
        use data_seeds::summaries::{SummaryMeasure, SUMMARY_PERIOD_MS};
        let span = match spec.get("span") {
            Some(value) => value.as_u64().ok_or_else(|| fail("Invalid summary span"))?,
            None => SUMMARY_PERIOD_MS,
        };
        let requests = spec.get("percentiles").and_then(Json::as_array).ok_or_else(|| fail("Expected percentile requests"))?;
        let mut measures = Vec::new();
        if let Some(selected) = spec.get("measures") {
            for selected in selected.as_array().ok_or_else(|| fail("Expected summary measures"))? {
                let field = match selected.get("field") {
                    Some(Json::Null) => None,
                    Some(Json::String(name)) => Some(name.clone()),
                    _ => return Err(fail("Expected summary field or null for event count")),
                };
                let measure = SummaryMeasure { field, aggregate: required_text(selected, "aggregate")?.parse()? };
                if !measures.contains(&measure) { measures.push(measure); }
            }
        }
        let mut selections = Vec::with_capacity(requests.len());
        for request in requests {
            let field = match request.get("field") {
                Some(Json::Null) => None,
                Some(Json::String(name)) => Some(name.clone()),
                _ => return Err(fail("Expected summary field or null for event count")),
            };
            let aggregate = required_text(request, "aggregate")?.parse()?;
            let measure = SummaryMeasure { field, aggregate };
            let index = match measures.iter().position(|m| *m == measure) {
                Some(index) => index,
                None => { let index = measures.len(); measures.push(measure); index },
            };
            let n = request.get("n").and_then(Json::as_f64).ok_or_else(|| fail("Invalid percentile"))?;
            selections.push((index, n));
        }
        if measures.is_empty() { measures.push(SummaryMeasure::events()); }
        let series = summary.series(&window, span, &measures)?;
        let mut results = Vec::with_capacity(selections.len());
        for (index, n) in selections {
            let series = &series[index];
            results.push(Data::Map(vec![
                ("field".into(), series.measure.field.clone().map(Data::Text).unwrap_or(Data::Null)),
                ("aggregate".into(), series.measure.aggregate.as_str().into()),
                ("n".into(), Data::Float(n)),
                ("value".into(), series.perc(n)?.unwrap_or(Data::Null)),
                ("samples".into(), (series.samples() as u64).into()),
                ("intervals".into(), (series.points().len() as u64).into()),
            ]));
        }
        data(Data::Map(vec![
            ("requested".into(),Data::Map(vec![("from".into(),from.into()),("to".into(),to.into())])),
            ("from".into(),window.covered().start.into()),("to".into(),window.covered().end.into()),
            ("span".into(),span.into()),
            ("count".into(),series[0].count.into()),("percentiles".into(),Data::Array(results)),
            ("series".into(),Data::Array(series.iter().map(|series| Data::Map(vec![
                ("field".into(),series.measure.field.clone().map(Data::Text).unwrap_or(Data::Null)),
                ("aggregate".into(),series.measure.aggregate.as_str().into()),
                ("points".into(),Data::Array(series.points().iter().map(|point| Data::Map(vec![
                    ("from".into(),point.from.into()),("to".into(),point.to.into()),
                    ("value".into(),point.value.clone().unwrap_or(Data::Null)),
                ])).collect())),
            ])).collect())),
        ]))?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_trunk_open() -> u32 {
    run(|r| {
        let bytes = std::mem::take(&mut r.input);
        add(&mut r.trunks, data_seeds::registration::Trunk::open(bytes)?)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_trunk_time(t: usize, branch: u16) -> u32 {
    run(|r| {
        let bytes = Arc::new(std::mem::take(&mut r.input));
        let time = get(&r.trunks, t)?.trie().branches().select([branch]).time(bytes)?;
        add(&mut r.times, Arc::new(time))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_trunk_replica_time(t: usize, branch: u16, length: usize) -> u32 {
    run(|r| {
        let suffix = r.input.get(length..).ok_or_else(|| fail("invalid Time input length"))?;
        let ranges: Vec<[u64; 2]> = serde_json::from_slice(suffix).map_err(|e| fail(e.to_string()))?;
        r.input.truncate(length);
        let time = get(&r.trunks, t)?.trie().branches().select([branch]).time_replica(
            Arc::new(std::mem::take(&mut r.input)),
            ranges.into_iter().map(|[start, end]| start..end).collect(),
        )?;
        add(&mut r.times, Arc::new(time))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_time_info(t: usize) -> u32 {
    run(|r| {
        json_value(&serde_json::to_value(get(&r.times, t)?.header()).map_err(|e| fail(e.to_string()))?)?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_time_leaf(t: usize, ms: u64) -> u32 {
    run(|r| {
        json_value(&serde_json::to_value(get(&r.times, t)?.leaf(ms)?).map_err(|e| fail(e.to_string()))?)?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_time_range(t: usize, from: u64, to: u64) -> u32 {
    run(|r| {
        if from >= to || to - from > 65_536 * data_seeds::TIME_HOUR_MS {
            return Err(fail("Invalid timeline selection"));
        }
        let range = get(&r.times, t)?.range(from, to)?;
        unsafe { array(); }
        for leaf in range {
            json_value(&serde_json::to_value(leaf?).map_err(|e| fail(e.to_string()))?)?;
        }
        unsafe { end(); }
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_trunk_query(t: usize) -> u32 {
    run(|r| {
        let query: Json = serde_json::from_slice(&r.input).map_err(|e| fail(e.to_string()))?;
        let value = get(&r.trunks, t)?.trie().query(&query).map_err(fail)?;
        json_value(&value)?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_trunk_describe(t: usize, id: u32) -> u32 {
    run(|r| { json_value(get(&r.trunks,t)?.describe(id)?)?; Ok(0) })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_trunk_section(t: usize, section: u32) -> u32 {
    run(|r| {
        let trie = get(&r.trunks, t)?.trie();
        let data = trie.section(section).ok_or_else(|| fail("Unknown registry section"))?;
        unsafe { bytes(data.as_ptr(), data.len()); }
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_trunk_check_file(t: usize, name_length: usize) -> u32 {
    run(|r| {
        let name = std::str::from_utf8(r.input.get(..name_length).ok_or_else(|| fail("Invalid filename length"))?)
            .map_err(|e| fail(e.to_string()))?;
        if !get(&r.trunks, t)?.trie().definition_file(name).map_err(fail)?.matches(&r.input[name_length..]) {
            return Err(fail("Definition file does not match registered revision"));
        }
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_trunk_prescribe(t: usize, id: u32) -> u32 {
    run(|r| {
        let trunk = get_mut(&mut r.trunks, t)?;
        let definition = if r.input.is_empty() { trunk.definition(id)? } else { trunk.prescribe(id, &r.input)? };
        add(&mut r.definitions, definition)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_trunk_prescribe_file(t: usize, name_length: usize) -> u32 {
    run(|r| {
        let file = std::str::from_utf8(r.input.get(..name_length)
            .ok_or_else(|| fail("Invalid Definition filename boundary"))?)
            .map_err(|e| fail(e.to_string()))?;
        get_mut(&mut r.trunks, t)?.prescribe_file(file, &r.input[name_length..])?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_prescribe_version(rx_length: usize) -> u32 {
    run(|r| {
        let bytes = r.input.get(..rx_length).ok_or_else(|| fail("Invalid Definition input boundary"))?;
        let document = data_seeds::doc::read(bytes)?;
        let selector: Json = serde_json::from_slice(&r.input[rx_length..]).map_err(|e| fail(e.to_string()))?;
        let object = selector.as_object().ok_or_else(|| fail("Expected Definition selection"))?;
        if object.keys().any(|k| !matches!(k.as_str(), "namespace"|"name"|"major"|"minor")) {
            return Err(fail("Unknown Definition selection property"));
        }
        let namespace = match object.get("namespace") {
            None => "",
            Some(v) => v.as_str().ok_or_else(|| fail("Invalid Definition namespace"))?,
        };
        let name = selector["name"].as_str().ok_or_else(|| fail("Missing Definition name"))?;
        let version = |key: &str| selector[key].as_u64().and_then(|n|u32::try_from(n).ok())
            .ok_or_else(|| fail(format!("Invalid Definition {key}")));
        let member = document.member(namespace, name, version("major")?, version("minor")?)?;
        add(&mut r.definitions, SeedDefinition::prescribe_member(&document, member)?)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_open_pack(d: usize) -> u32 {
    run(|r| {
        let def = get(&r.definitions, d)?.clone();
        let bytes = std::mem::take(&mut r.input);
        add(&mut r.packs, def.open(bytes)?)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_open_stored_pack(d: usize, maximum: usize) -> u32 {
    run(|r| {
        let def = get(&r.definitions, d)?.clone();
        let bytes = std::mem::take(&mut r.input);
        let pack = if maximum == 0 {
            def.open_stored(bytes)?
        } else {
            let length = bytes.len();
            def.open_stored_range_limited(Arc::new(bytes), 0..length, maximum)?
        };
        add(&mut r.packs, pack)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_stored_length() -> u32 {
    run(|r| {
        let size = pil_codec::pac_envelope::stored_length(&r.input)?;
        usize::try_from(size).map_err(|_| Error::Overflow)
    })
}
/// Storage publication calls this once on received bytes, before writing them.
/// Opening Packs and materializing query results do not verify payload CRCs.
#[unsafe(no_mangle)]
pub extern "C" fn seed_before_write() -> u32 {
    run(|r| {
        data_seeds::integrity::before_write(&r.input)?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_pack_index(h: usize, hour: u64) -> u32 {
    run(|r| {
        let pack = get(&r.packs, h)?.clone();
        let fields = data_seeds::IndexFields::from_definition(pack.definition())?;
        let bytes = Arc::new(std::mem::take(&mut r.input));
        let end = bytes.len();
        let index = data_seeds::HourIndex::open(bytes, 0..end, hour, fields)?;
        add(&mut r.packs, pack.with_storage(Arc::new(index))?)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_trunk_leaf_definitions(t: usize, branch: u16) -> u32 {
    run(|r| {
        let roots = get(&r.trunks, t)?.trie().branches().select([branch]).leaves()?.definitions(&r.input)?;
        let roots: Vec<String> = roots.iter().map(|digest| digest.iter().map(|b| format!("{b:02x}")).collect()).collect();
        json_value(&serde_json::json!(roots))?; Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_trunk_leaf_read(t: usize, branch: u16) -> u32 {
    run(|r| {
        let trie = get(&r.trunks, t)?.trie().branches().select([branch]).leaves()?.read(&r.input)?;
        add(&mut r.leaves, trie)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_trunk_leaf_inspect(t: usize, branch: u16) -> u32 {
    run(|r| {
        let layout = get(&r.trunks, t)?.trie().branches().select([branch]).leaves()?.inspect(&r.input)?;
        let blocks: Vec<Json> = layout.blocks.into_iter().map(|b| serde_json::json!({
            "start":b.start,"count":b.count,"local":b.local,"length":b.length
        })).collect();
        json_value(&serde_json::json!({"nodesEnd":layout.nodes_end,"nodeCount":layout.node_count,"blocks":blocks}))?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_leaf_append(h: usize, first: usize) -> u32 {
    run(|r| {
        // Readers retain their immutable snapshot. Nodes share value bytes;
        // only the node directory is copied when a snapshot is still in use.
        let trie = get_mut(&mut r.leaves, h)?;
        Arc::make_mut(trie).append_pil(first, &r.input)?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_pack_leaf_index(h: usize, hour: u64, leaves: usize) -> u32 {
    run(|r| {
        let pack = get(&r.packs, h)?.clone();
        let trie = get(&r.leaves, leaves)?.clone();
        if !trie.matches_definition(pack.definition()) {
            return Err(fail("state trie uses a different Definition"));
        }
        let fields = data_seeds::IndexFields::from_definition(pack.definition())?;
        let bytes = Arc::new(std::mem::take(&mut r.input));
        let end = bytes.len();
        let index = data_seeds::HourIndex::open(bytes, 0..end, hour, fields)?;
        add(
            &mut r.packs,
            pack.with_storage(Arc::new(data_seeds::LeafIndex::new(index, trie)))?,
        )
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_pack_retain(h: usize) -> u32 {
    run(|r| {
        let pack = get(&r.packs, h)?.clone();
        add(&mut r.packs, pack)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_pack_info(h: usize) -> u32 {
    run(|r| {
        let pack = get(&r.packs, h)?;
        let hour = pack.hour()?;
        unsafe {
            object();
            key(b"length".as_ptr(), 6);
            number(pack.len() as f64);
            key(b"byteLength".as_ptr(), 10);
            number(pack.stored_bytes().len() as f64);
            key(b"sealed".as_ptr(), 6);
            boolean(pack.is_sealed() as u32);
            key(b"hour".as_ptr(), 4);
            if let Some(hour) = hour {
                integer(hour as i128)?;
            } else {
                null();
            }
            end();
        }
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_pack_bytes(h: usize) -> u32 {
    run(|r| {
        let value = get(&r.packs, h)?.stored_bytes();
        unsafe { bytes(value.as_ptr(), value.len()); }
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_receive(d: usize, maximum: usize) -> u32 {
    receive(d, maximum, false, false)
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_receive_stored(d: usize, maximum: usize) -> u32 {
    receive(d, maximum, true, false)
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_receive_for_write(d: usize, maximum: usize) -> u32 {
    receive(d, maximum, true, true)
}
fn receive(d: usize, maximum: usize, stored: bool, verify_write: bool) -> u32 {
    run(|r| {
        let definition = get(&r.definitions, d)?.clone();
        add(
            &mut r.receivers,
            Receiver {
                definition: ReceiveDefinition::Single(definition),
                stream: Some(data_seeds::replication::PackStream::new(maximum)),
                pending: None, missing: Vec::new(), ready: VecDeque::new(),
                stored,
                verify_write,
                maximum,
                chunk: None,
            },
        )
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_registered_receive(s: usize, maximum: usize, verify_write: bool) -> u32 {
    run(|r| {
        let scope = get_mut(&mut r.stores, s)?.registered()?.reader_scope();
        add(&mut r.receivers, Receiver {
            definition: ReceiveDefinition::Registered(scope),
            stream: Some(data_seeds::replication::PackStream::new(maximum)),
            pending: None, missing: Vec::new(), ready: VecDeque::new(), stored: true, verify_write, maximum,
            chunk: None,
        })
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_receive_feed(h:usize)->u32 {
    run(|r|{
        let receiver=get_mut(&mut r.receivers,h)?;
        if receiver.pending.is_some() || receiver.chunk.is_some() || r.input.len()>receiver.maximum {return Err(fail("receive chunk exceeds admission or is still pending"));}
        receiver.chunk=Some((Arc::new(std::mem::take(&mut r.input)),0));Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_receive_step(h:usize)->u32 {
    run(|r|{
        let receiver=get_mut(&mut r.receivers,h)?;
        if let Some(pending)=receiver.pending.take() {
            let ReceiveDefinition::Registered(scope)=&receiver.definition else {return Err(fail("invalid dictionary scope"));};
            if !receiver.missing.is_empty(){return Err(fail("unresolved BOX strings"));}
            receiver.ready.push_back(pending.resolve(scope,receiver.maximum)?);
            return Ok(1 | usize::from(receiver.chunk.is_some())<<1);
        }
        let (chunk,at)=receiver.chunk.take().ok_or_else(||fail("no receive chunk"))?;
        let (end,received)=receiver.stream.as_mut().ok_or_else(||fail("Seed stream is finished"))?.step(chunk.clone(),at)?;
        let more=end<chunk.len();
        if more {receiver.chunk=Some((chunk,end));}
        let completed=received.is_some();
        if let Some(pack)=received {
            let maximum=receiver.maximum;
            receiver.ready.push_back(match &receiver.definition {
                ReceiveDefinition::Registered(scope) if receiver.verify_write=>{
                    let plain=pack.plain_for_write(maximum)?;
                    receiver.missing=plain.missing(scope)?;
                    if !receiver.missing.is_empty(){
                        receiver.pending=Some(plain);
                        return Ok(4);
                    }
                    plain.resolve(scope,maximum)?
                },
                ReceiveDefinition::Single(d) if receiver.verify_write=>pack.open_stored_for_write(d,maximum)?,
                ReceiveDefinition::Registered(scope)=>pack.open_registered(scope,maximum)?,
                ReceiveDefinition::Single(d) if receiver.stored=>pack.open_stored_limited(d,maximum)?,
                ReceiveDefinition::Single(d)=>pack.open(d)?,
            });
        }
        Ok(usize::from(completed)|usize::from(more)<<1)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_registered_strings(s:usize)->u32 {
    run(|r|{
        let entries=serde_json::from_slice(&r.input).map_err(|e|fail(e.to_string()))?;
        get_mut(&mut r.stores,s)?.registered()?.strings(entries)?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_receive_missing(h:usize)->u32 {
    run(|r|{
        json_value(&serde_json::json!(&get(&r.receivers,h)?.missing))?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_receive_strings(h:usize)->u32 {
    run(|r|{
        let entries:Vec<(usize,String)>=serde_json::from_slice(&r.input).map_err(|e|fail(e.to_string()))?;
        let receiver=get_mut(&mut r.receivers,h)?;
        if receiver.pending.is_none() || !entries.iter().map(|e|e.0).eq(receiver.missing.iter().copied()){
            return Err(fail("response does not match requested BOX string ordinals"));
        }
        let ReceiveDefinition::Registered(scope)=&mut receiver.definition else {return Err(fail("expected registered scope"));};
        scope.strings(entries)?;
        receiver.missing.clear();
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_receive_load(h: usize) -> u32 {
    run(|r| {
        let chunk = Arc::new(std::mem::take(&mut r.input));
        let receiver = get_mut(&mut r.receivers, h)?;
        let stream = receiver
            .stream
            .as_mut()
            .ok_or_else(|| fail("Seed stream is finished"))?;
        let definition = &receiver.definition;
        let stored = receiver.stored;
        let verify_write = receiver.verify_write;
        let maximum = receiver.maximum;
        let ready = &mut receiver.ready;
        if let Err(error) = stream.load(chunk, |pack| {
            ready.push_back(match definition {
                ReceiveDefinition::Registered(scope) if verify_write => pack.open_registered_for_write(scope, maximum)?,
                ReceiveDefinition::Single(definition) if verify_write => pack.open_stored_for_write(definition, maximum)?,
                ReceiveDefinition::Registered(scope) => pack.open_registered(scope, maximum)?,
                ReceiveDefinition::Single(definition) if stored => pack.open_stored_limited(definition, maximum)?,
                ReceiveDefinition::Single(definition) => pack.open(definition)?,
            });
            Ok(())
        }) {
            ready.clear();
            return Err(error);
        }
        Ok(ready.len())
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_receive_next(h: usize) -> u32 {
    run(|r| {
        let pack = get_mut(&mut r.receivers, h)?
            .ready
            .pop_front()
            .ok_or_else(|| fail("No complete Seed Pack"))?;
        add(&mut r.packs, pack)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_receive_finish(h: usize) -> u32 {
    run(|r| {
        let receiver=get(&r.receivers,h)?;
        if receiver.pending.is_some() || receiver.chunk.is_some() {
            return Err(fail("Seed stream has unconsumed input"));
        }
        let stream = get_mut(&mut r.receivers, h)?
            .stream
            .take()
            .ok_or_else(|| fail("Seed stream is finished"))?;
        usize::try_from(stream.finish()?).map_err(|_| Error::Overflow)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_store() -> u32 {
    run(|r| add(&mut r.stores, Packs::new().into()))
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_trunk_packs(t: usize) -> u32 {
    run(|r| {
        let selected: Option<Vec<u16>> = serde_json::from_slice(&r.input).map_err(|e| fail(e.to_string()))?;
        let branches = get(&r.trunks, t)?.trie().branches();
        let scope = match selected { Some(ids) => branches.select(ids), None => branches };
        add(&mut r.stores, Store::Registered(scope.packs()?))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_registered_store_load(s: usize, branch: u32, maximum: usize) -> u32 {
    run(|r| {
        let branch = u16::try_from(branch).map_err(|_| fail("Invalid Branch ID"))?;
        let owner = Arc::new(std::mem::take(&mut r.input));
        let length = owner.len();
        get_mut(&mut r.stores, s)?.registered()?.load(branch, owner, 0..length, if maximum == 0 { usize::MAX } else { maximum })?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_registered_store_pack(s: usize, branch: u32, p: usize) -> u32 {
    run(|r| {
        let branch = u16::try_from(branch).map_err(|_| fail("Invalid Branch ID"))?;
        let pack = get(&r.packs, p)?.clone();
        get_mut(&mut r.stores, s)?.registered()?.pack(branch, pack)?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_registered_store_box(s: usize, branch: u32) -> u32 {
    run(|r| {
        let branch = u16::try_from(branch).map_err(|_| fail("Invalid Branch ID"))?;
        let owner = r.staged_box.take().ok_or_else(|| fail("missing Box input"))?;
        let addresses: Vec<String> = serde_json::from_slice(&r.input).map_err(|e| fail(e.to_string()))?;
        let addresses = addresses.iter().map(|address| address.parse::<u64>().map_err(|_| fail("Invalid Pack address")))
            .collect::<Result<Vec<_>, _>>()?;
        get_mut(&mut r.stores, s)?.registered()?.load_box(branch, owner, addresses)?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_open_summaries(first: usize, end: usize) -> u32 {
    run(|r| {
        let bytes = Arc::new(std::mem::take(&mut r.input));
        add(&mut r.stores, Packs::open_summary_box(bytes, first..end)?.into())
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_stage_box() -> u32 {
    run(|r| {
        r.staged_box = Some(Arc::new(std::mem::take(&mut r.input)));
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_drop_box() {
    R.with_borrow_mut(|r| r.staged_box = None);
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_open_box() -> u32 {
    run(|r| {
        let bytes = r
            .staged_box
            .take()
            .ok_or_else(|| fail("missing Box input"))?;
        let ids: Vec<usize> = serde_json::from_slice(&r.input).map_err(|e| fail(e.to_string()))?;
        let store = Packs::open_box(bytes, |ordinal| {
            let id = *ids
                .get(ordinal)
                .ok_or_else(|| fail("missing child Definition binding"))?;
            Ok(get(&r.definitions, id)?.clone())
        })?;
        add(&mut r.stores, store.into())
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_open_box_addresses() -> u32 {
    run(|r| {
        let bytes = r
            .staged_box
            .take()
            .ok_or_else(|| fail("missing Box input"))?;
        let bindings: Vec<(usize, String)> =
            serde_json::from_slice(&r.input).map_err(|e| fail(e.to_string()))?;
        let addresses: Vec<u64> = bindings
            .iter()
            .map(|(_, address)| {
                address
                    .parse::<u64>()
                    .map_err(|_| fail("invalid Pack address"))
            })
            .collect::<Result<_, _>>()?;
        let mut definitions = bindings.iter();
        let store = Packs::open_box_addresses(bytes, addresses, |_| {
            let (id, _) = definitions
                .next()
                .ok_or_else(|| fail("missing child Definition binding"))?;
            Ok(get(&r.definitions, *id)?.clone())
        })?;
        add(&mut r.stores, store.into())
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_store_pack(s: usize, p: usize) -> u32 {
    run(|r| {
        let p = get(&r.packs, p)?.clone();
        match get_mut(&mut r.stores, s)? {
            Store::Unbound(store) => { store.pack(p); },
            Store::Registered(_) => return Err(fail("Registered PACKS requires a Branch ID")),
            Store::Reader(_) => return Err(fail("read-only storage selection")),
        }
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_query(s: usize) -> u32 {
    run(|r| {
        let spec: Json = serde_json::from_slice(&r.input).map_err(|e| fail(e.to_string()))?;
        let memory = r.query_memory.get_or_insert_with(|| data_seeds::MemoryBudget::new(64 * 1024 * 1024)).clone();
        let cursor = get(&r.stores, s)?.query(query(&spec)?)?.memory_budget(memory)?;
        add(&mut r.cursors, cursor)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_query_readers() -> u32 {
    run(|r| {
        let spec: Json = serde_json::from_slice(&r.input).map_err(|e| fail(e.to_string()))?;
        let ids = spec.get("readers").and_then(Json::as_array).ok_or_else(|| fail("missing reader selections"))?;
        let mut readers:Vec<Box<dyn data_seeds::PackSource>>=Vec::with_capacity(ids.len());
        for selection in ids {
            let id=selection.get("id").and_then(Json::as_u64).and_then(|v|usize::try_from(v).ok()).ok_or_else(||fail("invalid query source"))?;
            match selection.get("kind").and_then(Json::as_u64) {
                Some(2)=>match get(&r.stores,id)? {Store::Reader(reader)=>readers.push(Box::new(reader.clone())),_=>return Err(fail("expected file reader"))},
                Some(1)=>{let mut source=data_seeds::Sources::new();source.pack(get(&r.packs,id)?.clone());readers.push(Box::new(source));},
                _=>return Err(fail("invalid query source kind")),
            }
        }
        let memory = r.query_memory.get_or_insert_with(|| data_seeds::MemoryBudget::new(64 * 1024 * 1024)).clone();
        let cursor = data_seeds::reader::Reader::query_sources(readers, query(&spec)?)?.memory_budget(memory)?;
        add(&mut r.cursors, cursor)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_fetch(c: usize, n: usize) -> u32 {
    run(|r| {
        let batch = get_mut(&mut r.cursors, c)?.fetch(n)?.into_reader();
        add(&mut r.batches, batch)
    })
}
/// Cumulative cursor counters; inspection does not advance the query.
#[unsafe(no_mangle)]
pub extern "C" fn seed_query_stats(c: usize) -> u32 {
    run(|r| {
        let cursor = get(&r.cursors, c)?;
        json_value(&serde_json::json!({"scanned":cursor.scanned,"matched":cursor.matched}))?;
        Ok(0)
    })
}
/// No result handle is allocated for a yielded slice with no matches.
#[unsafe(no_mangle)]
pub extern "C" fn seed_poll(c: usize, n: usize, work: usize) -> u32 {
    run(|r| {
        let slice = get_mut(&mut r.cursors, c)?.poll(n, work)?;
        let handle = if slice.batch.is_empty() { None } else {
            Some(add(&mut r.batches, slice.batch.into_reader())?)
        };
        json_value(&serde_json::json!({"batch":handle,"done":slice.done,"work":slice.work}))?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_batch_origins(b: usize) -> u32 {
    run(|r| {
        let batch = get(&r.batches, b)?.batch();
        unsafe { array(); }
        for row in 0..batch.len() {
            let (pack, ordinal, digest) = batch.origin(row)?;
            let digest: String = digest.iter().map(|b| format!("{b:02x}")).collect();
            json_value(&serde_json::json!({"pack":pack,"row":ordinal,"definition":digest}))?;
        }
        unsafe { end(); }
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_batch_len(b: usize) -> u32 {
    run(|r| Ok(get(&r.batches, b)?.batch().len()))
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_retained_bytes(b: usize) -> u32 {
    run(|r| Ok(get(&r.batches, b)?.batch().retained_pack_bytes()))
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_cancel(c: usize) -> u32 {
    run(|r| {
        get_mut(&mut r.cursors, c)?.abort();
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_release(kind: u32, h: usize) {
    R.with_borrow_mut(|r| match kind {
        0 => r.definitions.remove(h),
        1 => r.packs.remove(h),
        2 => r.stores.remove(h),
        3 => r.cursors.remove(h),
        4 => r.batches.remove(h),
        5 => r.receivers.remove(h),
        6 => r.leaves.remove(h),
        7 => r.trunks.remove(h),
        8 => r.times.remove(h),
        9 => r.replicas.remove(h),
        10 => r.writes.remove(h),
        11 => r.collections.remove(h),
        12 => r.summaries.remove(h),
        13 => r.summary_plans.remove(h),
        14 => r.summary_reports.remove(h),
        15 => r.growth.remove(h),
        16 => r.stalks.remove(h),
        17 => r.summary_tables.remove(h),
        18 => r.read_owners.remove(h),
        _ => {}
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_field(b: usize, row: usize, i: usize) -> u32 {
    run(|r| {
        emit(get_mut(&mut r.batches, b)?.view(row)?.native_at(i)?)?;
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_results(b: usize, lazy: u32) -> u32 {
    run(|r| {
        let reader = get_mut(&mut r.batches, b)?;
        let count = reader.batch().len();
        let mut previous = usize::MAX;
        let mut fields = 0;
        unsafe {
            array();
        }
        for row in 0..count {
            let v = reader.view(row)?;
            if v.shape_id() != previous {
                previous = v.shape_id();
                fields = 0;
                unsafe {
                    shape();
                    for f in v.fields() {
                        name(f.as_ptr(), f.len());
                        fields += 1;
                    }
                }
            }
            if lazy != 0 {
                for i in 0..fields {
                    if !v.is_present_at(i)? {
                        unsafe {
                            missing(i);
                        }
                    }
                }
                unsafe {
                    view(row);
                }
            } else {
                unsafe {
                    record();
                }
                for i in 0..fields {
                    let value = v.native_at(i)?;
                    if matches!(value, NativeValue::Scalar(Value::Absent)) {
                        continue;
                    }
                    unsafe {
                        field(i);
                    }
                    emit(value)?;
                }
                unsafe {
                    end();
                }
            }
        }
        unsafe {
            end();
        }
        Ok(count)
    })
}


fn emit(v: NativeValue<'_>) -> Result<(), Error> {
    match v {
        NativeValue::Scalar(v) => scalar(v),
        NativeValue::Composite(v) => data(v),
    }
}
// Host projection only. Avoid serde's arbitrary-precision Number transport
// representation when crossing from a Rust metadata result into host values.
fn json_value(v: &Json) -> Result<(), Error> {
    unsafe {
        match v {
            Json::Null => null(),
            Json::Bool(v) => boolean(*v as u32),
            Json::String(v) => text(v.as_ptr(),v.len()),
            Json::Number(v) => {
                if let Some(v)=v.as_u64() { emit_unsigned(v as u128)?; }
                else if let Some(v)=v.as_i64() { integer(v as i128)?; }
                else { number(v.as_f64().ok_or_else(||fail("Metadata number outside host range"))?); }
            }
            Json::Array(values) => { array(); for v in values { json_value(v)?; } end(); }
            Json::Object(values) => { object(); for (k,v) in values { key(k.as_ptr(),k.len());json_value(v)?; } end(); }
        }
    }
    Ok(())
}
fn emit_unsigned(v: u128) -> Result<(), Error> {
    unsafe {
        if v <= 9_007_199_254_740_991 {
            number(v as f64);
        } else if let Ok(n) = u64::try_from(v) {
            unsigned(n);
        } else {
            let text = v.to_string();
            big_integer(text.as_ptr(), text.len());
        }
    }
    Ok(())
}
fn integer(v: i128) -> Result<(), Error> {
    if v >= 0 {
        return emit_unsigned(v as u128);
    }
    unsafe {
        if v >= -9_007_199_254_740_991 {
            number(v as f64);
        } else if let Ok(n) = i64::try_from(v) {
            signed(n);
        } else {
            let text = v.to_string();
            big_integer(text.as_ptr(), text.len());
        }
    }
    Ok(())
}

fn scalar(v: Value<'_>) -> Result<(), Error> {
    unsafe {
        match v {
            Value::Absent => absent(),
            Value::Null => null(),
            Value::Bool(v) => boolean(v as u32),
            Value::I8(v) => number(v as f64),
            Value::I16(v) => number(v as f64),
            Value::I32(v) => number(v as f64),
            Value::I64(v)
            | Value::TimestampMs(v)
            | Value::TimestampUs(v)
            | Value::TimestampNs(v) => integer(v as i128)?,
            Value::U8(v) => number(v as f64),
            Value::U16(v) | Value::Ref16(v) => number(v as f64),
            Value::U24(v) | Value::U32(v) => number(v as f64),
            Value::U64(v) => integer(v as i128)?,
            Value::U128(v) => emit_unsigned(v)?,
            Value::I128(v) => integer(v)?,
            Value::F32(v) => number(v as f64),
            Value::F64(v) => number(v),
            Value::String(v) => text(v.as_ptr(), v.len()),
            Value::Bytes(v) => bytes(v.as_ptr(), v.len()),
            Value::Uuid(v) => bytes(v.as_ptr(), v.len()),
            Value::Object(v) => data(pil_codec::object::decode_value(v)?)?,
            Value::Array(v) => {
                array();
                match v {
                    Array::Items(v) => {
                        for &v in v {
                            scalar(v)?;
                        }
                    }
                    Array::Encoded(v) => {
                        for v in v.iter() {
                            scalar(v?)?;
                        }
                    }
                }
                end();
            }
            Value::Mapped(_) => return Err(fail("unresolved dictionary value")),
        }
    }
    Ok(())
}
fn data(v: Data) -> Result<(), Error> {
    unsafe {
        match v {
            Data::Null => null(),
            Data::Bool(v) => boolean(v as u32),
            Data::Integer(v) => integer(v.into())?,
            v @ Data::Tag(2, _) => emit_unsigned(pil_codec::unsigned_native(&v)?)?,
            v @ Data::Tag(3, _) => integer(pil_codec::signed_native(&v)?)?,
            Data::Float(v) => number(v),
            Data::Text(v) => text(v.as_ptr(), v.len()),
            Data::Bytes(v) => bytes(v.as_ptr(), v.len()),
            Data::Array(v) => {
                array();
                for v in v {
                    data(v)?;
                }
                end();
            }
            Data::Map(v) => {
                if v.iter().all(|(k, _)| matches!(k, Data::Text(_))) {
                    object();
                    for (k, v) in v {
                        let Data::Text(k) = k else { unreachable!() };
                        key(k.as_ptr(), k.len());
                        data(v)?;
                    }
                    end();
                } else {
                    map();
                    for (k, v) in v {
                        data(k)?;
                        data(v)?;
                    }
                    end();
                }
            }
            _ => return Err(fail("value has no host representation")),
        }
    }
    Ok(())
}

#[unsafe(no_mangle)]
pub extern "C" fn seed_query_event()->u32 {
 run(|r|{let event:data_seeds::registration::query_events::QueryEvent=serde_json::from_slice(&r.input).map_err(|e|fail(e.to_string()))?;
 let encoded=event.encode()?;unsafe{bytes(encoded.as_ptr(),encoded.len());}Ok(0)})
}

#[unsafe(no_mangle)]
pub extern "C" fn seed_trunk_leaf_labels(t: usize, branch: u16) -> u32 {
    run(|r| { let labels = get(&r.trunks,t)?.trie().branches().select([branch]).leaves()?.labels(&r.input)?;
        json_value(&serde_json::json!(labels))?; Ok(0) })
}
#[unsafe(no_mangle)]
pub extern "C" fn seed_trunk_leaf_label(t: usize, branch: u16, length: usize, leaf: u16) -> u32 {
    run(|r| {
        let (file,label) = r.input.split_at_checked(length).ok_or(Error::Incomplete)?;
        let label = std::str::from_utf8(label).map_err(|e| fail(e.to_string()))?;
        let next = get(&r.trunks,t)?.trie().branches().select([branch]).leaves()?.label(file,leaf,(!label.is_empty()).then_some(label))?;
        unsafe { bytes(next.as_ptr(),next.len()); } Ok(0)
    })
}
