//! Run with --release. This is an experiment, not a new storage format or
//! deployed query path. All fixture events enter the Rust summary accumulator.
#[path = "columns/access.rs"]
mod access;
use access::{Column, Path};
use data_seeds::{doc, Field, SeedDefinition, summaries::{SummaryBox, SummaryBoxReader}};
use pil_codec::api::{FieldBytes, NativeValue, Pack};
use std::{alloc::{GlobalAlloc, Layout, System}, hint::black_box, sync::atomic::{AtomicBool, AtomicUsize, Ordering}, time::Instant};
use serde_json::json;

struct Allocator;
static TRACK: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if TRACK.load(Ordering::Relaxed) { ALLOCS.fetch_add(1, Ordering::Relaxed); BYTES.fetch_add(layout.size(), Ordering::Relaxed); }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) { unsafe { System.dealloc(ptr, layout) } }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if TRACK.load(Ordering::Relaxed) { ALLOCS.fetch_add(1, Ordering::Relaxed); BYTES.fetch_add(size, Ordering::Relaxed); }
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static GLOBAL: Allocator = Allocator;
fn allocations<T>(run: impl FnOnce() -> T) -> (T, usize, usize) {
    ALLOCS.store(0, Ordering::Relaxed); BYTES.store(0, Ordering::Relaxed); TRACK.store(true, Ordering::Relaxed);
    let value = run(); TRACK.store(false, Ordering::Relaxed);
    (value, ALLOCS.load(Ordering::Relaxed), BYTES.load(Ordering::Relaxed))
}
fn measure<T>(mut run: impl FnMut() -> T, iterations: usize) -> serde_json::Value {
    for _ in 0..3 { black_box(run()); }
    let mut samples = Vec::new();
    for _ in 0..7 {
        let start = Instant::now();
        for _ in 0..iterations { black_box(run()); }
        samples.push(start.elapsed().as_secs_f64() * 1e6 / iterations as f64);
    }
    let mut sorted = samples.clone(); sorted.sort_by(f64::total_cmp);
    let (_, count, bytes) = allocations(|| black_box(run()));
    json!({"median_us":sorted[3],"samples_us":samples,"iterations":iterations,"allocations":count,"allocated_bytes":bytes})
}
fn fixture(width: usize, days: usize) -> Vec<u8> {
    let names = (0..width).map(|i| if i == 0 { "duration".into() } else { format!("metric_{i}") }).collect::<Vec<String>>();
    let mut doc = doc::new("strided-summary-benchmark");
    for name in &names { doc = doc.field(Field::new(name).r#type("float").size(8).annotation("#summ(numeric)")); }
    let doc = doc.sign().unwrap();
    let source = SeedDefinition::prescribe(&doc).unwrap();
    let plan = source.summaries(288).unwrap();
    let mut storage = SummaryBox::new(plan.layout().clone()).unwrap();
    for day in 0..days {
        let mut accumulator = plan.accumulator(day as u64 * 86_400_000).unwrap();
        let mut events = source.fill(288 * 4).unwrap();
        for period in 0..288 { for event in 0..4 {
            let time = day as u64 * 86_400_000 + period as u64 * 300_000 + event as u64;
            let seed = accumulator.ingest(time, (day, period, event), |(day, period, event), seed| {
                for (field, name) in names.iter().enumerate() {
                    seed.set_float(name.as_str(), ((day * 31 + period * 7 + event * 13 + field * 17) % 10007) as f64 + 0.25)?;
                }
                Ok(())
            }).unwrap();
            events.load(seed).unwrap();
        }}
        events.seal().unwrap();
        accumulator.complete(accumulator.end()).unwrap();
        storage.append(accumulator.seal().unwrap()).unwrap();
    }
    storage.commit().unwrap(); storage.bytes().unwrap().to_vec()
}
fn row_values(pack: &Pack<'_>, path: &Path, mut visit: impl FnMut(f64)) {
    let mut rows = pack.reader();
    let mut objects = path.prescriptions[1].decoder();
    let mut metrics = path.prescriptions[2].decoder();
    for i in 0..pack.len() {
        let row = rows.get(i).unwrap();
        let FieldBytes::Bytes(raw) = row.field(path.ordinals[0]).unwrap() else { panic!("missing fields") };
        let fields = objects.read(raw).unwrap();
        let FieldBytes::Bytes(raw) = fields.field(path.ordinals[1]).unwrap() else { panic!("missing metric") };
        let metric = metrics.read(raw).unwrap();
        let NativeValue::Scalar(pil_codec::Value::F64(value)) = metric.native_value(path.ordinals[2]).unwrap() else { panic!("not float") };
        visit(value);
    }
}
fn main() {
    let mut results = Vec::new();
    for width in [1, 8, 32] { for days in [1, 32] {
        let bytes = fixture(width, days);
        let storage = SummaryBoxReader::open(&bytes).unwrap();
        assert!(storage.is_uniform());
        let rx = storage.layout().definition().prescription();
        let path = Path::new(rx, &["fields", "duration", "sum"]).unwrap();
        let packs = (0..days).map(|i| storage.pack(i).unwrap()).collect::<Vec<_>>();
        let bind = || (0..days).map(|i| {
            let range = storage.storage_range(i).unwrap();
            Column::bind(&packs[i], &bytes[range.start as usize..range.end as usize], &path).unwrap()
        }).collect::<Vec<_>>();
        let columns = bind();
        let values = || { let mut values = Vec::with_capacity(days * 288); for column in &columns { for i in 0..column.len() { values.push(column.get(i).unwrap()); } } values };
        // Check every value, not only sums (which can hide reordered records).
        let materialized = values();
        let mut ordinal = 0;
        for pack in &packs { row_values(pack, &path, |value| { assert_eq!(value, materialized[ordinal]); ordinal += 1; }); }
        assert_eq!(ordinal, days * 288);
        for column in &columns { assert!(column.get(column.len()).is_err()); }
        let expected = materialized.iter().sum::<f64>();
        let mut runs = Vec::new();
        for passes in [1, 10, 100] {
            let row = || { let mut total = 0.0; for _ in 0..passes { for pack in black_box(&packs) { row_values(pack, &path, |v| total += v); } } total };
            let strided = || { let mut total = 0.0; for _ in 0..passes { for column in black_box(&columns) { total += column.sum().unwrap(); } } total };
            let contiguous = || { let mut total = 0.0; for _ in 0..passes { for &value in black_box(&materialized) { total += value; } } total };
            let copy_and_scan = || { let values = values(); let mut total = 0.0; for _ in 0..passes { for &value in black_box(&values) { total += value; } } total };
            for actual in [row(), strided(), contiguous(), copy_and_scan()] { assert_eq!(actual, expected * passes as f64); }
            let n = (200_000 / (days * 288 * passes)).clamp(2, 200);
            runs.push(json!({"passes":passes,"row":measure(row,n),"strided":measure(strided,n),"contiguous_scan_only":measure(contiguous,n),"copy_and_scan":measure(copy_and_scan,n)}));
        }
        results.push(json!({"fields":width,"days":days,"rows":days*288,"box_bytes":bytes.len(),"row_stride":columns[0].stride(),
            "column_descriptor_bytes":std::mem::size_of::<Column>()*columns.len(),"copied_column_bytes":materialized.capacity()*8,
            "definition_path_setup":measure(||Path::new(rx,&["fields","duration","sum"]).unwrap(),20),
            "pack_open":measure(||(0..days).map(|i|storage.pack(i).unwrap()).collect::<Vec<_>>(),20),
            "column_bind":measure(bind,20),"runs":runs}));
    }}
    println!("{}",serde_json::to_string_pretty(&json!({"scope":"Native Rust, release, resident immutable fixed-full summary Packs; typed RX scalar decoding; one sum slot; no I/O or report rendering","cases":results})).unwrap());
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn column_borrows_nested_values_and_checks_bounds() {
        let bytes = fixture(8, 2);
        let storage = SummaryBoxReader::open(&bytes).unwrap();
        let path = Path::new(storage.layout().definition().prescription(), &["fields", "metric_7", "sum"]).unwrap();
        for i in 0..2 {
            let pack = storage.pack(i).unwrap(); let range = storage.storage_range(i).unwrap();
            let column = Column::bind(&pack,&bytes[range.start as usize..range.end as usize],&path).unwrap();
            let mut n = 0;
            row_values(&pack,&path,|value|{ assert_eq!(column.get(n).unwrap(), value); n += 1; });
            assert_eq!(n,column.len()); assert!(column.get(n).is_err()); assert!(column.get(usize::MAX).is_err());
        }
        assert!(Path::new(storage.layout().definition().prescription(), &["fields", "missing", "sum"]).is_err());
    }
    #[test]
    fn column_rejects_truncated_and_unrelated_buffers() {
        let bytes = fixture(1, 1);
        let storage = SummaryBoxReader::open(&bytes).unwrap();
        let path = Path::new(storage.layout().definition().prescription(), &["fields", "duration", "sum"]).unwrap();
        let pack = storage.pack(0).unwrap();
        let range = storage.storage_range(0).unwrap();
        let raw = &bytes[range.start as usize..range.end as usize];
        assert!(Column::bind(&pack,&raw[..raw.len()-1],&path).is_err());
        // Identical bytes in a different allocation are not this Pack's owner.
        assert!(Column::bind(&pack,&raw.to_vec(),&path).is_err());
    }
    #[test]
    fn column_rejects_sparse_null_and_common_members() {
        let doc = doc::new("unsupported-column-layout")
            .field(Field::new("value").r#type("float").size(8)).sign().unwrap();
        let rx = doc.prescribe().unwrap();
        let path = Path::new(&rx,&["value"]).unwrap();
        for common in [false,true] {
            let mut pack = rx.fill(2).unwrap();
            pack.configure(data_seeds::PackOptions {fixed:true,common_values:common,..Default::default()}).unwrap();
            for _ in 0..2 {
                let mut pill = rx.dispense();
                if common { pill.set_float("value",3.0).unwrap(); } else { pill.set_null("value").unwrap(); }
                pack.load(pill).unwrap();
            }
            pack.seal().unwrap();
            let bytes = pack.bytes().unwrap();
            let opened = rx.open(bytes).unwrap();
            assert!(Column::bind(&opened,bytes,&path).is_err());
        }
    }
}
